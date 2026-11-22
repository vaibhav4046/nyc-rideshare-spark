package rideshare

import java.nio.file.{Files, Path, Paths}
import java.time.YearMonth

import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class PipelineSpec extends AnyFunSuite with SparkSuite {
  import Fixtures._

  private def tmp(prefix: String): Path = Files.createTempDirectory(prefix)

  private def zonesCsv(dir: Path): String = {
    val p = dir.resolve("zones.csv")
    Files.write(p, Seq(
      "\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"",
      "1,\"EWR\",\"Newark Airport\",\"EWR\"",
      "2,\"Queens\",\"Jamaica Bay\",\"Boro Zone\"",
      "3,\"Bronx\",\"Allerton/Pelham Gardens\",\"Boro Zone\"",
      "4,\"Manhattan\",\"Alphabet City\",\"Yellow Zone\"",
      "5,\"Staten Island\",\"Arden Heights\",\"Boro Zone\""
    ).mkString("\n").getBytes)
    p.toString
  }

  private def writeMonth(raw: Path, month: String, rows: Seq[RawTrip]): Unit =
    df(spark, rows: _*).coalesce(1).write.mode(SaveMode.Overwrite)
      .parquet(raw.resolve(s"staging_$month").toString)

  /** Spark writes a directory; the pipeline expects one file per month named like the TLC files. */
  private def publishRaw(raw: Path, month: String, rows: Seq[RawTrip]): Unit = {
    writeMonth(raw, month, rows)
    val staged = raw.resolve(s"staging_$month")
    val part = Files.list(staged).filter(_.getFileName.toString.endsWith(".parquet")).findFirst().get()
    Files.move(part, raw.resolve(s"fhvhv_tripdata_$month.parquet"))
    Files.walk(staged).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.delete(p))
  }

  test("monthOf and pending select only unpublished months unless full refresh") {
    assert(Incremental.monthOf("fhvhv_tripdata_2023-04.parquet").contains(YearMonth.of(2023, 4)))
    assert(Incremental.monthOf("yellow_tripdata_2023-04.parquet").isEmpty)
    val files = Seq(3, 1, 2).map(m => RawFile(YearMonth.of(2023, m), s"f$m", 1L))
    val processed = Set(YearMonth.of(2023, 1))
    assert(Incremental.pending(files, processed, fullRefresh = false).map(_.month.getMonthValue) == Seq(2, 3))
    assert(Incremental.pending(files, processed, fullRefresh = true).map(_.month.getMonthValue) == Seq(1, 2, 3))
  }

  test("end to end: ingest, partition, aggregate, then rerun incrementally") {
    val base = tmp("pipeline")
    val raw = Files.createDirectories(base.resolve("raw"))
    val out = base.resolve("out").toString
    val results = base.resolve("results").toString
    val zones = zonesCsv(base)
    val cfg = QualityConfig(minRows = 10)

    publishRaw(raw, "2023-01", validMonth("2023-01", 60) :+ RawTrip(trip_time = 0L) /* 1 reject < 2% */)
    publishRaw(raw, "2023-02", validMonth("2023-02", 40))
    Pipeline.run(spark, raw.toString, zones, out, results, fullRefresh = false, cfg)

    val trips = spark.read.parquet(Pipeline.tripsPath(out))
    assert(trips.count() == 100)
    assert(trips.select("pickup_date").distinct().count() == 10) // 5 days x 2 months
    assert(Incremental.readManifest(spark, out).map(_.month.toString) == Seq("2023-01", "2023-02"))
    assert(Files.exists(Paths.get(results, "run_metrics.json")))

    val daily = spark.read.parquet(s"$out/agg/daily_company")
    assert(daily.groupBy().sum("trips").head().getLong(0) == 100)
    val borough = spark.read.parquet(s"$out/agg/borough_hourly")
    assert(borough.select("pickup_borough").distinct().collect().map(_.getString(0)).toSet == Set("EWR", "Queens", "Bronx"))
    assert(Files.readAllLines(Paths.get(results, "top_routes.csv")).size() > 1)

    // Rerun with no new files: nothing is reprocessed.
    Pipeline.run(spark, raw.toString, zones, out, results, fullRefresh = false, cfg)
    assert(Files.exists(Paths.get(results, "run_metrics_noop.json")))
    assert(spark.read.parquet(Pipeline.tripsPath(out)).count() == 100)

    // A new month arrives: only it is processed; earlier partitions are untouched.
    publishRaw(raw, "2023-03", validMonth("2023-03", 30))
    Pipeline.run(spark, raw.toString, zones, out, results, fullRefresh = false, cfg)
    assert(spark.read.parquet(Pipeline.tripsPath(out)).count() == 130)
    assert(Files.readAllLines(Paths.get(results, "run_metrics_incremental.json")).toString.contains("\"2023-03\""))

    // Full refresh rewrites the same partitions idempotently (no duplicates).
    Pipeline.run(spark, raw.toString, zones, out, results, fullRefresh = true, cfg)
    assert(spark.read.parquet(Pipeline.tripsPath(out)).count() == 130)

    // An operator can hand-edit the manifest (here: drop March) to force that month to be reprocessed.
    val manifest = Paths.get(out, Incremental.ManifestName)
    val kept = Files.readAllLines(manifest).toArray.map(_.toString).filterNot(_.startsWith("2023-03"))
    Files.write(manifest, kept.map(_ + System.lineSeparator()).mkString.getBytes)
    assert(Incremental.readManifest(spark, out).map(_.month.toString) == Seq("2023-01", "2023-02"))
    Pipeline.run(spark, raw.toString, zones, out, results, fullRefresh = false, cfg)
    assert(Incremental.readManifest(spark, out).size == 3)
    assert(spark.read.parquet(Pipeline.tripsPath(out)).count() == 130)
  }

  test("a month that fails the quality gate stops the job and publishes nothing for it") {
    val base = tmp("pipeline-bad")
    val raw = Files.createDirectories(base.resolve("raw"))
    val out = base.resolve("out").toString
    val bad = validMonth("2023-05", 20) ++ (1 to 10).map(_ => RawTrip(trip_miles = -1.0,
      pickup_datetime = ts("2023-05-02 10:00:00"), dropoff_datetime = ts("2023-05-02 10:10:00")))
    publishRaw(raw, "2023-05", bad)

    val e = intercept[DataQualityException](
      Pipeline.run(spark, raw.toString, zonesCsv(base), out, base.resolve("results").toString, fullRefresh = false,
        QualityConfig(minRows = 10)))
    assert(e.getMessage.contains("reject_ratio"))
    assert(!Files.exists(Paths.get(Pipeline.tripsPath(out))))
    assert(Incremental.readManifest(spark, out).isEmpty)
  }

  test("broadcast and sort-merge joins give identical borough aggregates") {
    val base = tmp("join")
    val trips = Clean.curate(Clean.tag(Schema.conform(df(spark, validMonth("2023-01", 50): _*)), YearMonth.of(2023, 1)))
    val zones = Aggregations.zones(spark, zonesCsv(base))
    val a = Aggregations.boroughHourly(trips, zones, useBroadcast = true).collect().toSeq
    val b = Aggregations.boroughHourly(trips, zones, useBroadcast = false).collect().toSeq
    assert(a == b && a.nonEmpty)
  }

  test("parseArgs reads flags and key-value pairs") {
    val (cmd, opts) = Main.parseArgs(Array("run", "--raw", "r", "--full-refresh", "--out", "o"))
    assert(cmd == "run" && opts == Map("raw" -> "r", "full-refresh" -> "true", "out" -> "o"))
  }
}

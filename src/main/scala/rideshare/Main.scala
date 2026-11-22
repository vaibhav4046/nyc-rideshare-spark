package rideshare

import java.time.Instant

import scala.collection.immutable.ListMap

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Entry point.
 *
 *   run   --raw DIR --zones CSV --out DIR [--results DIR] [--full-refresh] [--max-reject-ratio 0.02]
 *   bench --raw DIR --zones CSV --out DIR [--results DIR] [--reps 3]
 */
object Main {

  def main(args: Array[String]): Unit = {
    val (cmd, opts) = parseArgs(args)
    def req(k: String): String = opts.getOrElse(k, sys.error(s"missing --$k"))
    val results = opts.getOrElse("results", "results")
    if (cmd != "run" && cmd != "bench") sys.error(s"unknown command '$cmd' (expected run | bench)")
    val spark = Spark.session(s"nyc-rideshare-$cmd")
    try {
      if (cmd == "run") {
        val cfg = QualityConfig(maxRejectRatio = opts.get("max-reject-ratio").map(_.toDouble).getOrElse(0.02))
        Pipeline.run(spark, req("raw"), req("zones"), req("out"), results, opts.contains("full-refresh"), cfg)
      } else Bench.run(spark, req("raw"), req("zones"), req("out"), results, opts.get("reps").map(_.toInt).getOrElse(3))
    } finally spark.stop()
  }

  private[rideshare] def parseArgs(args: Array[String]): (String, Map[String, String]) = {
    require(args.nonEmpty, "usage: run|bench --key value ...")
    def loop(rest: List[String], acc: Map[String, String]): Map[String, String] = rest match {
      case Nil => acc
      case k :: v :: tail if k.startsWith("--") && !v.startsWith("--") => loop(tail, acc + (k.drop(2) -> v))
      case k :: tail if k.startsWith("--") => loop(tail, acc + (k.drop(2) -> "true"))
      case x :: _ => sys.error(s"unexpected argument '$x'")
    }
    (args.head, loop(args.tail.toList, Map.empty))
  }
}

final case class MonthResult(file: RawFile, stats: MonthStats, checks: Seq[CheckResult],
                             dqSeconds: Double, writeSeconds: Double, seconds: Double) {
  def toJson: Map[String, Any] = ListMap(
    "month" -> file.month.toString,
    "input_bytes" -> file.bytes,
    "input_rows" -> stats.inputRows,
    "valid_rows" -> stats.validRows,
    "rejected_rows" -> stats.rejected,
    "rejects_by_reason" -> ListMap(Clean.Reasons.map(r => r -> stats.rejects(r)): _*),
    "dq_checks" -> checks.map(c => ListMap("check" -> c.name, "passed" -> c.passed, "detail" -> c.detail)),
    "dq_pass_seconds" -> dqSeconds,
    "write_seconds" -> writeSeconds,
    "month_seconds" -> seconds
  )
}

object Pipeline {

  def tripsPath(out: String): String = s"$out/trips"

  /** Wall time is measured from here, so it excludes JVM start-up and SparkSession creation. */
  def run(spark: SparkSession, raw: String, zonesCsv: String, out: String, results: String, fullRefresh: Boolean,
          cfg: QualityConfig): Unit = {
    val started = System.nanoTime()
    val startedAt = Instant.now().toString
    locally {
      val available = Incremental.listRaw(spark, raw)
      val manifest = Incremental.readManifest(spark, out)
      val todo = Incremental.pending(available, manifest.map(_.month).toSet, fullRefresh)
      println(s"[pipeline] ${available.size} source months, ${manifest.size} already published, processing ${todo.size}")

      val ingestStart = System.nanoTime()
      val done = todo.foldLeft((manifest, Vector.empty[MonthResult])) { case ((entries, acc), f) =>
        val r = ingestMonth(spark, f, out, cfg)
        val next = entries :+ ManifestEntry(f.month, f.path, f.bytes, r.stats.inputRows, r.stats.validRows, Instant.now().toString)
        Incremental.writeManifest(spark, out, next) // after every month, so a crash keeps finished months
        (next, acc :+ r)
      }._2
      val ingestSeconds = Report.seconds(ingestStart)

      val aggStart = System.nanoTime()
      val aggRows = aggregate(spark, zonesCsv, out, results)
      val aggSeconds = Report.seconds(aggStart)
      val total = Report.seconds(started)

      val inRows = done.map(_.stats.inputRows).sum
      val inBytes = done.map(_.file.bytes).sum
      val metrics = ListMap(
        "run_started_utc" -> startedAt,
        "spark_master" -> spark.sparkContext.master,
        "spark_version" -> spark.version,
        "full_refresh" -> fullRefresh,
        "months_processed" -> todo.map(_.month.toString),
        "months_skipped_already_published" ->
          available.map(_.month).filterNot(m => todo.exists(_.month == m)).map(_.toString),
        "input_bytes" -> inBytes,
        "input_gb" -> inBytes / 1e9,
        "input_rows" -> inRows,
        "valid_rows" -> done.map(_.stats.validRows).sum,
        "rejected_rows" -> done.map(_.stats.rejected).sum,
        "ingest_seconds" -> ingestSeconds,
        "aggregate_seconds" -> aggSeconds,
        "total_wall_seconds" -> total,
        "ingest_rows_per_second" -> (if (ingestSeconds > 0) inRows / ingestSeconds else 0.0),
        "ingest_mb_per_second" -> (if (ingestSeconds > 0) inBytes / 1e6 / ingestSeconds else 0.0),
        "aggregate_output_rows" -> aggRows,
        "machine" -> Report.machine,
        "months" -> done.map(_.toJson)
      )
      val name =
        if (todo.isEmpty) "run_metrics_noop.json"
        else if (fullRefresh || manifest.isEmpty) "run_metrics.json"
        else "run_metrics_incremental.json"
      Report.write(s"$results/$name", Report.json(metrics) + "\n")
      println(f"[pipeline] done in $total%.1f s, metrics -> $results/$name")
    }
  }

  private def ingestMonth(spark: SparkSession, f: RawFile, out: String, cfg: QualityConfig): MonthResult = {
    val t0 = System.nanoTime()
    val tagged = Clean.tag(Schema.conform(spark.read.parquet(f.path)), f.month)

    val stats = Quality.collectStats(tagged, f.month)
    val dqSeconds = Report.seconds(t0)
    val checks = Quality.enforce(stats, cfg) // throws DataQualityException -> job fails, nothing published

    val t1 = System.nanoTime()
    Clean.curate(tagged)
      .repartition(col("pickup_date"))
      .write.mode(SaveMode.Overwrite) // dynamic partition overwrite: only this month's dates are replaced
      .partitionBy("pickup_date")
      .parquet(tripsPath(out))
    val writeSeconds = Report.seconds(t1)

    val written = spark.read.parquet(tripsPath(out))
      .where(col("pickup_date").between(
        lit(java.sql.Date.valueOf(f.month.atDay(1))), lit(java.sql.Date.valueOf(f.month.atEndOfMonth()))))
      .count()
    Quality.reconcile(f.month, stats.validRows, written)
    val seconds = Report.seconds(t0)
    println(f"[pipeline] ${f.month} rows=${stats.inputRows} valid=${stats.validRows} rejected=${stats.rejected} in $seconds%.1f s")
    MonthResult(f, stats, checks, dqSeconds, writeSeconds, seconds)
  }

  /** Builds the analytical tables from the curated trips table. Returns total output rows. */
  def aggregate(spark: SparkSession, zonesCsv: String, out: String, results: String): Long = {
    val trips = spark.read.parquet(tripsPath(out))
    val zones = Aggregations.zones(spark, zonesCsv)
    val tables: Seq[(String, DataFrame)] = Seq(
      "daily_company" -> Aggregations.dailyCompany(trips),
      "borough_hourly" -> Aggregations.boroughHourly(trips, zones),
      "top_routes" -> Aggregations.topRoutes(trips, zones)
    )
    tables.map { case (name, df) =>
      df.write.mode(SaveMode.Overwrite).parquet(s"$out/agg/$name")
      val sortCol = if (name == "top_routes") desc("trips") else col(df.columns.head)
      val order = sortCol +: df.columns.tail.toSeq.map(col)
      Report.csv(spark.read.parquet(s"$out/agg/$name").orderBy(order: _*), s"$results/$name.csv")
    }.sum
  }
}

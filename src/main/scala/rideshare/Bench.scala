package rideshare

import scala.collection.immutable.ListMap

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Before/after timings for two optimisations, on the real curated table.
 *  1. Join strategy: sort-merge join (broadcast disabled) vs broadcast hash join for the zone lookup.
 *  2. Partition pruning: a one-week query over the raw monthly files vs the date-partitioned curated table.
 * Each variant runs once as warm-up (discarded), then `reps` times in alternating order; we report every
 * timing and the median.
 */
object Bench {

  private def median(xs: Seq[Double]): Double = {
    val s = xs.sorted
    if (s.size % 2 == 1) s(s.size / 2) else (s(s.size / 2 - 1) + s(s.size / 2)) / 2
  }

  private def timed[T](body: => T): (T, Double) = {
    val t = System.nanoTime()
    val r = body
    (r, Report.seconds(t))
  }

  private def joinOperator(df: DataFrame): String = {
    val plan = df.queryExecution.executedPlan.toString
    Seq("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin").find(plan.contains).getOrElse("unknown")
  }

  def run(spark: SparkSession, raw: String, zonesCsv: String, out: String, results: String, reps: Int): Unit = {
    locally {
      val join = joinBench(spark, zonesCsv, out, reps)
      Report.write(s"$results/bench_join.json", Report.json(join) + "\n")
      val pruning = pruningBench(spark, raw, out, reps)
      Report.write(s"$results/bench_pruning.json", Report.json(pruning) + "\n")
      println("[bench] " + Report.json(ListMap("join" -> join("median_seconds"), "pruning" -> pruning("median_seconds"))))
    }
  }

  private def joinBench(spark: SparkSession, zonesCsv: String, out: String, reps: Int): Map[String, Any] = {
    val trips = spark.read.parquet(Pipeline.tripsPath(out))
    val zones = Aggregations.zones(spark, zonesCsv)
    val defaultThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    def once(broadcastJoin: Boolean): (Double, String, Long) = {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", if (broadcastJoin) defaultThreshold else "-1")
      val df = Aggregations.boroughHourly(trips, zones, useBroadcast = broadcastJoin)
      val (rows, secs) = timed(df.collect().length.toLong)
      (secs, joinOperator(df), rows)
    }

    once(false); once(true) // warm-up: JIT, file cache
    val runs = (1 to reps).flatMap { i =>
      val order = if (i % 2 == 1) Seq(false, true) else Seq(true, false)
      order.map(b => b -> once(b))
    }
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", defaultThreshold)

    def summary(b: Boolean): Map[String, Any] = {
      val rs = runs.filter(_._1 == b).map(_._2)
      ListMap("join_operator" -> rs.head._2, "output_rows" -> rs.head._3,
        "seconds" -> rs.map(_._1), "median_seconds" -> median(rs.map(_._1)))
    }
    val before = summary(false)
    val after = summary(true)
    ListMap(
      "query" -> "borough_hourly: curated trips JOIN taxi zones (265 rows) GROUP BY borough, hour",
      "input_rows" -> trips.count(),
      "reps" -> reps,
      "before_sort_merge_join" -> before,
      "after_broadcast_hash_join" -> after,
      "median_seconds" -> ListMap("before" -> before("median_seconds"), "after" -> after("median_seconds")),
      "speedup" -> before("median_seconds").asInstanceOf[Double] / after("median_seconds").asInstanceOf[Double],
      "machine" -> Report.machine
    )
  }

  private def pruningBench(spark: SparkSession, raw: String, out: String, reps: Int): Map[String, Any] = {
    val from = java.sql.Date.valueOf("2023-03-06")
    val to = java.sql.Date.valueOf("2023-03-12")
    val rawFiles = Incremental.listRaw(spark, raw).map(_.path)

    // Before: same question answered from the raw monthly files (no date partitions to prune).
    def rawQuery: DataFrame = rawFiles.map(p => Schema.conform(spark.read.parquet(p))).reduce(_ unionByName _)
      .withColumn("pickup_date", to_date(col("pickup_datetime")))
      .where(col("pickup_date").between(lit(from), lit(to)))
      .groupBy(Schema.companyName(col("hvfhs_license_num")).as("company")).count()

    // After: curated table partitioned by pickup_date, so Spark only lists and reads 7 partitions.
    def curatedQuery: DataFrame = spark.read.parquet(Pipeline.tripsPath(out))
      .where(col("pickup_date").between(lit(from), lit(to)))
      .groupBy("company").count()

    def once(curated: Boolean): (Double, Long) = {
      val df = if (curated) curatedQuery else rawQuery
      val (rows, secs) = timed(df.collect().map(_.getLong(1)).sum)
      (secs, rows)
    }

    once(false); once(true)
    val runs = (1 to reps).flatMap { i =>
      val order = if (i % 2 == 1) Seq(false, true) else Seq(true, false)
      order.map(c => c -> once(c))
    }
    def summary(c: Boolean): Map[String, Any] = {
      val rs = runs.filter(_._1 == c).map(_._2)
      ListMap("trips_counted" -> rs.head._2, "seconds" -> rs.map(_._1), "median_seconds" -> median(rs.map(_._1)))
    }
    val before = summary(false)
    val after = summary(true)
    ListMap(
      "query" -> s"trips per company, pickup_date $from..$to",
      "note" -> "raw counts include rows later rejected by validation, so the two counts differ slightly",
      "reps" -> reps,
      "before_raw_monthly_files" -> before,
      "after_partitioned_curated" -> after,
      "median_seconds" -> ListMap("before" -> before("median_seconds"), "after" -> after("median_seconds")),
      "speedup" -> before("median_seconds").asInstanceOf[Double] / after("median_seconds").asInstanceOf[Double],
      "machine" -> Report.machine
    )
  }
}

package rideshare

import java.time.YearMonth

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Row-level validation. Each bad row gets exactly one reject reason (first rule that fails). */
object Clean {
  val MaxLocationId = 265 // taxi_zone_lookup.csv covers IDs 1..265
  val MaxTripSeconds: Long = 6L * 3600
  val MaxTripMiles = 250.0

  val ReasonCol = "reject_reason"

  val Reasons: Seq[String] =
    Seq("null_timestamp", "bad_location", "bad_duration", "bad_distance", "negative_money", "out_of_period")

  private def between(c: Column, lo: Any, hi: Any): Column = c.isNotNull && c >= lo && c <= hi

  def rejectReason(month: YearMonth): Column = {
    val start = java.sql.Timestamp.valueOf(month.atDay(1).atStartOfDay())
    val end = java.sql.Timestamp.valueOf(month.plusMonths(1).atDay(1).atStartOfDay())
    when(col("pickup_datetime").isNull || col("dropoff_datetime").isNull, "null_timestamp")
      .when(!between(col("PULocationID"), 1, MaxLocationId) || !between(col("DOLocationID"), 1, MaxLocationId), "bad_location")
      .when(col("dropoff_datetime") < col("pickup_datetime") || !between(col("trip_time"), 1L, MaxTripSeconds), "bad_duration")
      .when(!between(col("trip_miles"), 0.0, MaxTripMiles), "bad_distance")
      .when(col("base_passenger_fare").isNull || col("base_passenger_fare") < 0 ||
        col("driver_pay").isNull || col("driver_pay") < 0, "negative_money")
      .when(col("pickup_datetime") < lit(start) || col("pickup_datetime") >= lit(end), "out_of_period")
  }

  /** Conformed input plus a nullable reject_reason column. */
  def tag(conformed: DataFrame, month: YearMonth): DataFrame =
    conformed.withColumn(ReasonCol, rejectReason(month))

  private def money(name: String): Column = coalesce(col(name), lit(0.0))

  /** Valid rows only, with derived columns used by the curated table and aggregations. */
  def curate(tagged: DataFrame): DataFrame =
    tagged
      .where(col(ReasonCol).isNull)
      .drop(ReasonCol)
      .withColumn("company", Schema.companyName(col("hvfhs_license_num")))
      .withColumn("pickup_date", to_date(col("pickup_datetime")))
      .withColumn("pickup_hour", hour(col("pickup_datetime")))
      .withColumn("total_fare",
        money("base_passenger_fare") + money("tolls") + money("bcf") + money("sales_tax") +
          money("congestion_surcharge") + money("airport_fee"))
      .withColumn("tips", money("tips"))
}

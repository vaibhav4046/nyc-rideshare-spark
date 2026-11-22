package rideshare

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Aggregations {

  def zones(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").csv(path)
      .select(col("LocationID").cast("int").as("LocationID"), col("Borough"), col("Zone"))

  /** Trips, gross fares and driver pay per company per day. */
  def dailyCompany(trips: DataFrame): DataFrame =
    trips.groupBy("pickup_date", "company")
      .agg(
        count(lit(1)).as("trips"),
        round(sum("trip_miles"), 1).as("miles"),
        round(sum("total_fare"), 2).as("gross_fare_usd"),
        round(sum("driver_pay"), 2).as("driver_pay_usd"),
        round(sum("tips"), 2).as("tips_usd"),
        round(sum("driver_pay") / sum("base_passenger_fare"), 4).as("driver_pay_to_base_fare")
      )
      .orderBy("pickup_date", "company")

  /** Demand profile by pickup borough and hour of day. The zone table is tiny, so it is broadcast. */
  def boroughHourly(trips: DataFrame, zoneDf: DataFrame, useBroadcast: Boolean = true): DataFrame = {
    val z = zoneDf.select(col("LocationID").as("PULocationID"), col("Borough").as("pickup_borough"))
    trips.join(if (useBroadcast) broadcast(z) else z, Seq("PULocationID"))
      .groupBy("pickup_borough", "pickup_hour")
      .agg(
        count(lit(1)).as("trips"),
        round(avg("trip_miles"), 3).as("avg_miles"),
        round(avg(col("trip_time") / 60.0), 2).as("avg_minutes"),
        round(avg("total_fare"), 2).as("avg_fare_usd")
      )
      .orderBy("pickup_borough", "pickup_hour")
  }

  /** Busiest pickup-to-dropoff zone pairs. */
  def topRoutes(trips: DataFrame, zoneDf: DataFrame, n: Int = 25): DataFrame = {
    val pu = zoneDf.select(col("LocationID").as("PULocationID"), col("Zone").as("pickup_zone"), col("Borough").as("pickup_borough"))
    val dr = zoneDf.select(col("LocationID").as("DOLocationID"), col("Zone").as("dropoff_zone"), col("Borough").as("dropoff_borough"))
    trips.groupBy("PULocationID", "DOLocationID")
      .agg(
        count(lit(1)).as("trips"),
        round(avg("trip_miles"), 2).as("avg_miles"),
        round(avg(col("trip_time") / 60.0), 1).as("avg_minutes"),
        round(avg("total_fare"), 2).as("avg_fare_usd")
      )
      .orderBy(desc("trips"))
      .limit(n)
      .join(broadcast(pu), Seq("PULocationID"), "left")
      .join(broadcast(dr), Seq("DOLocationID"), "left")
      .select("pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough", "trips", "avg_miles", "avg_minutes", "avg_fare_usd")
      .orderBy(desc("trips"))
  }
}

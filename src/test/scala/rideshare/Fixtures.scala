package rideshare

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Raw-shaped TLC rows. IDs are Long and trip_time Long, as in the real monthly files, to exercise casting. */
final case class RawTrip(
  hvfhs_license_num: String = "HV0003",
  dispatching_base_num: String = "B03404",
  PULocationID: java.lang.Long = 132L,
  DOLocationID: java.lang.Long = 236L,
  request_datetime: Timestamp = Timestamp.valueOf("2023-01-10 08:55:00"),
  pickup_datetime: Timestamp = Timestamp.valueOf("2023-01-10 09:00:00"),
  dropoff_datetime: Timestamp = Timestamp.valueOf("2023-01-10 09:30:00"),
  trip_miles: java.lang.Double = 17.5,
  trip_time: java.lang.Long = 1800L,
  base_passenger_fare: java.lang.Double = 50.0,
  tolls: java.lang.Double = 6.55,
  bcf: java.lang.Double = 1.5,
  sales_tax: java.lang.Double = 4.4,
  congestion_surcharge: java.lang.Double = 2.75,
  airport_fee: java.lang.Double = null,
  tips: java.lang.Double = 5.0,
  driver_pay: java.lang.Double = 40.0,
  shared_match_flag: String = "N",
  wav_request_flag: String = "N"
)

object Fixtures {
  def ts(s: String): Timestamp = Timestamp.valueOf(s)

  def df(spark: SparkSession, rows: RawTrip*): DataFrame = {
    import spark.implicits._
    rows.toDF()
  }

  /** `n` valid trips spread across the first days of the given month. */
  def validMonth(month: String, n: Int, licence: String = "HV0003"): Seq[RawTrip] =
    (0 until n).map { i =>
      val day = 1 + i % 5
      val start = ts(f"$month-$day%02d ${8 + i % 10}%02d:00:00")
      RawTrip(
        hvfhs_license_num = if (i % 3 == 0) "HV0005" else licence,
        PULocationID = (1L + i % 3),
        DOLocationID = (4L + i % 2),
        pickup_datetime = start,
        request_datetime = new Timestamp(start.getTime - 60000L),
        dropoff_datetime = new Timestamp(start.getTime + 600000L),
        trip_time = 600L,
        trip_miles = 2.0,
        base_passenger_fare = 20.0,
        driver_pay = 15.0
      )
    }
}

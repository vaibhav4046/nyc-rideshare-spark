package rideshare

import java.time.YearMonth

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class CleanQualitySpec extends AnyFunSuite with SparkSuite {
  import Fixtures._

  private val jan = YearMonth.of(2023, 1)

  private def reasonOf(t: RawTrip): Option[String] =
    Option(Clean.tag(Schema.conform(df(spark, t)), jan).select(Clean.ReasonCol).head().getString(0))

  test("a well-formed trip passes validation") {
    assert(reasonOf(RawTrip()).isEmpty)
  }

  test("each broken field maps to its reject reason") {
    assert(reasonOf(RawTrip(pickup_datetime = null)).contains("null_timestamp"))
    assert(reasonOf(RawTrip(PULocationID = 0L)).contains("bad_location"))
    assert(reasonOf(RawTrip(DOLocationID = 266L)).contains("bad_location"))
    assert(reasonOf(RawTrip(trip_time = 0L)).contains("bad_duration"))
    assert(reasonOf(RawTrip(dropoff_datetime = ts("2023-01-10 08:00:00"))).contains("bad_duration"))
    assert(reasonOf(RawTrip(trip_miles = -1.0)).contains("bad_distance"))
    assert(reasonOf(RawTrip(trip_miles = 900.0)).contains("bad_distance"))
    assert(reasonOf(RawTrip(base_passenger_fare = -3.0)).contains("negative_money"))
    assert(reasonOf(RawTrip(driver_pay = null)).contains("negative_money"))
    assert(reasonOf(RawTrip(pickup_datetime = ts("2022-12-31 23:59:00"),
      dropoff_datetime = ts("2023-01-01 00:20:00"))).contains("out_of_period"))
  }

  test("conform fails fast and names every missing column") {
    val broken = df(spark, RawTrip()).drop("tips", "driver_pay")
    val e = intercept[DataQualityException](Schema.conform(broken))
    assert(e.getMessage.contains("tips") && e.getMessage.contains("driver_pay"))
  }

  test("conform casts drifting integer widths to the canonical schema") {
    val c = Schema.conform(df(spark, RawTrip()))
    assert(c.schema("PULocationID").dataType.typeName == "integer")
    assert(c.columns.toSeq == Schema.required.map(_._1))
  }

  test("curate keeps valid rows and derives company, date, hour and total fare") {
    val rows = Seq(RawTrip(), RawTrip(hvfhs_license_num = "HV0005"), RawTrip(trip_miles = -5.0))
    val cur = Clean.curate(Clean.tag(Schema.conform(df(spark, rows: _*)), jan)).orderBy("company").collect()
    assert(cur.length == 2)
    assert(cur.map(_.getAs[String]("company")).toSeq == Seq("Lyft", "Uber"))
    val r = cur.head
    assert(r.getAs[java.sql.Date]("pickup_date").toString == "2023-01-10")
    assert(r.getAs[Int]("pickup_hour") == 9)
    // 50 + 6.55 + 1.5 + 4.4 + 2.75 + null airport fee as 0
    assert(math.abs(r.getAs[Double]("total_fare") - 65.2) < 1e-9)
  }

  test("collectStats counts input, rejects per reason and unknown licences in one pass") {
    val rows = validMonth("2023-01", 20) ++ Seq(RawTrip(trip_time = 0L), RawTrip(PULocationID = 999L),
      RawTrip(hvfhs_license_num = "HV9999"))
    val stats = Quality.collectStats(Clean.tag(Schema.conform(df(spark, rows: _*)), jan), jan)
    assert(stats.inputRows == 23)
    assert(stats.rejects("bad_duration") == 1 && stats.rejects("bad_location") == 1)
    assert(stats.rejected == 2 && stats.validRows == 21)
    assert(stats.unknownCompany == 1)
  }

  test("quality gate passes clean stats and fails with every broken check listed") {
    val good = MonthStats(jan, 10000, Clean.Reasons.map(_ -> 0L).toMap.updated("bad_location", 10L), 0, 22.0)
    assert(Quality.enforce(good, QualityConfig()).forall(_.passed))

    val bad = MonthStats(jan, 500, Clean.Reasons.map(_ -> 0L).toMap.updated("bad_distance", 100L), 3, 400.0)
    val e = intercept[DataQualityException](Quality.enforce(bad, QualityConfig()))
    Seq("min_rows", "reject_ratio", "known_company", "avg_fare_in_range").foreach(c => assert(e.getMessage.contains(c)))
  }

  test("reconcile fails when written rows differ from validated rows") {
    Quality.reconcile(jan, 5, 5)
    intercept[DataQualityException](Quality.reconcile(jan, 5, 4))
  }

  test("company mapping covers the four published licences") {
    val names = df(spark, Seq("HV0002", "HV0003", "HV0004", "HV0005").map(l => RawTrip(hvfhs_license_num = l)): _*)
      .select(Schema.companyName(col("hvfhs_license_num"))).collect().map(_.getString(0)).toSet
    assert(names == Set("Juno", "Uber", "Via", "Lyft"))
  }
}

package rideshare

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Canonical subset of the TLC High Volume FHV schema. Monthly files drift in integer widths, so every column is cast. */
object Schema {
  val required: Seq[(String, DataType)] = Seq(
    "hvfhs_license_num" -> StringType,
    "PULocationID" -> IntegerType,
    "DOLocationID" -> IntegerType,
    "request_datetime" -> TimestampType,
    "pickup_datetime" -> TimestampType,
    "dropoff_datetime" -> TimestampType,
    "trip_miles" -> DoubleType,
    "trip_time" -> LongType,
    "base_passenger_fare" -> DoubleType,
    "tolls" -> DoubleType,
    "bcf" -> DoubleType,
    "sales_tax" -> DoubleType,
    "congestion_surcharge" -> DoubleType,
    "airport_fee" -> DoubleType,
    "tips" -> DoubleType,
    "driver_pay" -> DoubleType,
    "shared_match_flag" -> StringType,
    "wav_request_flag" -> StringType
  )

  /** HVFHS licence numbers published in the TLC data dictionary. */
  val companies: Map[String, String] = Map(
    "HV0002" -> "Juno",
    "HV0003" -> "Uber",
    "HV0004" -> "Via",
    "HV0005" -> "Lyft"
  )

  /** Fails fast with the full list of missing columns instead of a late AnalysisException. */
  def conform(raw: DataFrame): DataFrame = {
    val present = raw.columns.toSet
    val missing = required.map(_._1).filterNot(present.contains)
    if (missing.nonEmpty)
      throw new DataQualityException(s"Input is missing required columns: ${missing.mkString(", ")}")
    raw.select(required.map { case (name, tpe) => col(name).cast(tpe).as(name) }: _*)
  }

  def companyName(licence: Column): Column =
    companies.foldLeft(lit(null).cast(StringType)) { case (acc, (code, name)) =>
      when(licence === code, lit(name)).otherwise(acc)
    }
}

class DataQualityException(message: String) extends RuntimeException(message)

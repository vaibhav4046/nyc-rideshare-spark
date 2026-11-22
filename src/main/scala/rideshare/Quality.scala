package rideshare

import java.time.YearMonth

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

final case class QualityConfig(
  minRows: Long = 1000L,
  maxRejectRatio: Double = 0.02,
  maxUnknownCompanyRatio: Double = 0.0,
  avgFareRange: (Double, Double) = (5.0, 100.0)
)

final case class MonthStats(
  month: YearMonth,
  inputRows: Long,
  rejects: Map[String, Long],
  unknownCompany: Long,
  avgBaseFare: Double
) {
  def rejected: Long = rejects.values.sum
  def validRows: Long = inputRows - rejected
  def rejectRatio: Double = if (inputRows == 0) 0.0 else rejected.toDouble / inputRows
}

final case class CheckResult(name: String, passed: Boolean, detail: String)

/** Batch-level data-quality gate. Any failed check stops the job before data is published. */
object Quality {

  /** One pass over the tagged month: input count, rejects per reason, and stats on valid rows. */
  def collectStats(tagged: DataFrame, month: YearMonth): MonthStats = {
    val reason = col(Clean.ReasonCol)
    val valid = reason.isNull
    val aggs =
      Seq(count(lit(1)).as("input_rows")) ++
        Clean.Reasons.map(r => sum(when(reason === r, 1L).otherwise(0L)).as(r)) ++
        Seq(
          sum(when(valid && Schema.companyName(col("hvfhs_license_num")).isNull, 1L).otherwise(0L)).as("unknown_company"),
          avg(when(valid, col("base_passenger_fare"))).as("avg_base_fare")
        )
    val row = tagged.agg(aggs.head, aggs.tail: _*).head()
    def long(name: String): Long = Option(row.getAs[java.lang.Long](name)).map(_.longValue).getOrElse(0L)
    MonthStats(
      month = month,
      inputRows = long("input_rows"),
      rejects = Clean.Reasons.map(r => r -> long(r)).toMap,
      unknownCompany = long("unknown_company"),
      avgBaseFare = Option(row.getAs[java.lang.Double]("avg_base_fare")).map(_.doubleValue).getOrElse(Double.NaN)
    )
  }

  def evaluate(stats: MonthStats, cfg: QualityConfig): Seq[CheckResult] = {
    val valid = math.max(stats.validRows, 1L)
    val unknownRatio = stats.unknownCompany.toDouble / valid
    val (lo, hi) = cfg.avgFareRange
    Seq(
      CheckResult("min_rows", stats.inputRows >= cfg.minRows, s"input_rows=${stats.inputRows} min=${cfg.minRows}"),
      CheckResult("reject_ratio", stats.rejectRatio <= cfg.maxRejectRatio,
        f"reject_ratio=${stats.rejectRatio}%.5f max=${cfg.maxRejectRatio}%.5f"),
      CheckResult("known_company", unknownRatio <= cfg.maxUnknownCompanyRatio,
        s"unknown_company_rows=${stats.unknownCompany}"),
      CheckResult("avg_fare_in_range", !stats.avgBaseFare.isNaN && stats.avgBaseFare >= lo && stats.avgBaseFare <= hi,
        f"avg_base_fare=${stats.avgBaseFare}%.2f range=[$lo%.0f, $hi%.0f]")
    )
  }

  def enforce(stats: MonthStats, cfg: QualityConfig): Seq[CheckResult] = {
    val results = evaluate(stats, cfg)
    val failed = results.filterNot(_.passed)
    if (failed.nonEmpty)
      throw new DataQualityException(
        s"Data-quality gate failed for ${stats.month}: " + failed.map(f => s"${f.name} (${f.detail})").mkString("; "))
    results
  }

  /** Post-write reconciliation: the published partitions must hold exactly the rows that passed validation. */
  def reconcile(month: YearMonth, expected: Long, written: Long): Unit =
    if (expected != written)
      throw new DataQualityException(s"Row reconciliation failed for $month: expected $expected, wrote $written")
}

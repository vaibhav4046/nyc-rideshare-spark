package rideshare

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.time.YearMonth

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

final case class RawFile(month: YearMonth, path: String, bytes: Long)

final case class ManifestEntry(month: YearMonth, inputFile: String, inputBytes: Long, inputRows: Long,
                               validRows: Long, processedAtUtc: String) {
  def toLine: String = Seq(month, inputFile, inputBytes, inputRows, validRows, processedAtUtc).mkString(",")
}

/** Tracks which monthly source files are already published, so reruns only process new months. */
object Incremental {
  private val FilePattern = """fhvhv_tripdata_(\d{4}-\d{2})\.parquet""".r
  val ManifestName = "_manifest.csv"

  def monthOf(fileName: String): Option[YearMonth] = fileName match {
    case FilePattern(ym) => Some(YearMonth.parse(ym))
    case _ => None
  }

  def pending(available: Seq[RawFile], processed: Set[YearMonth], fullRefresh: Boolean): Seq[RawFile] =
    available.filter(f => fullRefresh || !processed.contains(f.month)).sortBy(_.month)

  def listRaw(spark: SparkSession, rawDir: String): Seq[RawFile] = {
    val dir = new Path(rawDir)
    val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(dir).toSeq.flatMap { st =>
      monthOf(st.getPath.getName).map(m => RawFile(m, st.getPath.toString, st.getLen))
    }.sortBy(_.month)
  }

  /**
   * The manifest is a small, human-editable text file, so skip Hadoop's .crc side files for it.
   * A fresh (uncached) instance keeps checksum verification on for the Parquet data itself.
   */
  private def manifestFs(spark: SparkSession, p: Path): FileSystem = {
    val fs = FileSystem.newInstance(p.toUri, spark.sparkContext.hadoopConfiguration)
    fs.setVerifyChecksum(false)
    fs.setWriteChecksum(false)
    fs
  }

  def readManifest(spark: SparkSession, outDir: String): Seq[ManifestEntry] = {
    val p = new Path(outDir, ManifestName)
    val fs = manifestFs(spark, p)
    try {
      if (!fs.exists(p)) Seq.empty
      else {
        val in = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))
        try Iterator.continually(in.readLine()).takeWhile(_ != null).filter(_.nonEmpty).map(parse).toList
        finally in.close()
      }
    } finally fs.close()
  }

  private def parse(line: String): ManifestEntry = {
    val f = line.split(",", -1)
    ManifestEntry(YearMonth.parse(f(0)), f(1), f(2).toLong, f(3).toLong, f(4).toLong, f(5))
  }

  /** Rewrites the manifest with one entry per month (latest run wins). */
  def writeManifest(spark: SparkSession, outDir: String, entries: Seq[ManifestEntry]): Unit = {
    val p = new Path(outDir, ManifestName)
    val fs = manifestFs(spark, p)
    val merged = entries.groupBy(_.month).map { case (_, es) => es.last }.toSeq.sortBy(_.month)
    try {
      val out = fs.create(p, true)
      try out.write(merged.map(_.toLine).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
      finally out.close()
    } finally fs.close()
  }
}

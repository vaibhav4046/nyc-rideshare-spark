package rideshare

import org.apache.spark.sql.SparkSession

object Spark {
  private val isWindows = System.getProperty("os.name", "").toLowerCase.contains("windows")

  /** Local session tuned for a shared laptop: bounded cores, UTC so stored wall-clock times are not shifted. */
  def session(appName: String, master: String = sys.env.getOrElse("SPARK_MASTER", "local[6]")): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTITIONS", "48"))
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.ui.enabled", "false")
    val withFs =
      if (isWindows) builder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
      else builder
    val spark = withFs.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

package rideshare

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame

/** Small helpers for writing results to the repo's results/ folder (driver-side, small data only). */
object Report {
  def write(path: String, content: String): Unit = {
    val p = Paths.get(path)
    Option(p.getParent).foreach(Files.createDirectories(_))
    Files.write(p, content.getBytes(StandardCharsets.UTF_8))
  }

  private def csvCell(v: Any): String = v match {
    case null => ""
    case s: String if s.exists(c => c == ',' || c == '"' || c == '\n') => "\"" + s.replace("\"", "\"\"") + "\""
    case other => other.toString
  }

  /** Collects a small DataFrame to the driver and writes a single CSV file. */
  def csv(df: DataFrame, path: String): Long = {
    val rows = df.collect()
    val lines = df.columns.mkString(",") +: rows.toSeq.map(_.toSeq.map(csvCell).mkString(","))
    write(path, lines.mkString("", "\n", "\n"))
    rows.length.toLong
  }

  private def escape(s: String): String = s.flatMap {
    case '"' => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case c if c < ' ' => f"\\u${c.toInt}%04x"
    case c => c.toString
  }

  /** Minimal JSON encoder for metrics: maps, sequences, strings, numbers, booleans. */
  def json(v: Any, indent: Int = 0): String = {
    val pad = "  " * (indent + 1)
    val end = "  " * indent
    v match {
      case null => "null"
      case s: String => "\"" + escape(s) + "\""
      case d: Double =>
        if (d.isNaN || d.isInfinite) "null"
        else BigDecimal(d).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      case n @ (_: Int | _: Long | _: BigDecimal) => n.toString
      case b: Boolean => b.toString
      case m: Map[_, _] if m.isEmpty => "{}"
      case m: Map[_, _] =>
        m.toSeq.map { case (k, x) => s"$pad${json(k.toString)}: ${json(x, indent + 1)}" }
          .mkString("{\n", ",\n", s"\n$end}")
      case s: Seq[_] if s.isEmpty => "[]"
      case s: Seq[_] => s.map(x => pad + json(x, indent + 1)).mkString("[\n", ",\n", s"\n$end]")
      case other => json(other.toString)
    }
  }

  def seconds(startNanos: Long): Double = (System.nanoTime() - startNanos) / 1e9

  def machine: Map[String, Any] = scala.collection.immutable.ListMap(
    "os" -> s"${System.getProperty("os.name")} ${System.getProperty("os.version")}",
    "jvm" -> s"${System.getProperty("java.vendor")} ${System.getProperty("java.version")}",
    "available_processors" -> Runtime.getRuntime.availableProcessors(),
    "driver_max_heap_gb" -> Runtime.getRuntime.maxMemory() / 1e9
  )
}

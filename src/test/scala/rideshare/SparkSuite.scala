package rideshare

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSuite extends BeforeAndAfterAll { self: Suite =>
  lazy val spark: SparkSession = Spark.session("tests", "local[2]")

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}

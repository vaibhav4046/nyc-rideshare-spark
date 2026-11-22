"""PySpark version of the Scala `borough_hourly` aggregation (Aggregations.boroughHourly).

Reads the curated, date-partitioned trips table written by the Scala pipeline, joins the
taxi zone lookup (broadcast) and computes trips / avg miles / avg minutes / avg fare per
pickup borough and hour of day.

    python pyspark/borough_hourly.py --trips OUT/trips --zones taxi_zone_lookup.csv --out results/borough_hourly_pyspark.csv
    python pyspark/borough_hourly.py --self-test

On Windows, pass --jar target/scala-2.12/classes so Spark can use the project's
BareLocalFileSystem instead of winutils.exe.
"""

import argparse
import csv
import json
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def borough_hourly(trips: DataFrame, zones: DataFrame) -> DataFrame:
    z = zones.select(F.col("LocationID").cast("int").alias("PULocationID"), F.col("Borough").alias("pickup_borough"))
    return (
        trips.join(F.broadcast(z), "PULocationID")
        .groupBy("pickup_borough", "pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("trip_miles"), 3).alias("avg_miles"),
            F.round(F.avg(F.col("trip_time") / 60.0), 2).alias("avg_minutes"),
            F.round(F.avg("total_fare"), 2).alias("avg_fare_usd"),
        )
        .orderBy("pickup_borough", "pickup_hour")
    )


def session(master: str, jar: str | None) -> SparkSession:
    b = (
        SparkSession.builder.appName("borough-hourly-pyspark")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.ui.enabled", "false")
    )
    if jar:
        b = b.config("spark.driver.extraClassPath", os.path.abspath(jar)).config(
            "spark.hadoop.fs.file.impl", "rideshare.BareLocalFileSystem"
        )
    return b.getOrCreate()


def self_test(spark: SparkSession) -> None:
    trips = spark.createDataFrame(
        [(1, 8, 2.0, 600, 20.0), (1, 8, 4.0, 1200, 30.0), (2, 9, 1.0, 300, 10.0), (99, 9, 1.0, 300, 10.0)],
        "PULocationID int, pickup_hour int, trip_miles double, trip_time long, total_fare double",
    )
    zones = spark.createDataFrame([("1", "Manhattan"), ("2", "Queens")], "LocationID string, Borough string")
    rows = [r.asDict() for r in borough_hourly(trips, zones).collect()]
    assert rows == [
        {"pickup_borough": "Manhattan", "pickup_hour": 8, "trips": 2, "avg_miles": 3.0, "avg_minutes": 15.0, "avg_fare_usd": 25.0},
        {"pickup_borough": "Queens", "pickup_hour": 9, "trips": 1, "avg_miles": 1.0, "avg_minutes": 5.0, "avg_fare_usd": 10.0},
    ], rows
    print("self-test passed")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trips")
    ap.add_argument("--zones")
    ap.add_argument("--out")
    ap.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[6]"))
    ap.add_argument("--jar", help="classpath entry holding rideshare.BareLocalFileSystem (Windows only)")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()

    spark = session(a.master, a.jar)
    spark.sparkContext.setLogLevel("WARN")
    try:
        if a.self_test:
            self_test(spark)
            return 0
        if not (a.trips and a.zones and a.out):
            ap.error("--trips, --zones and --out are required unless --self-test")
        zones = spark.read.option("header", "true").csv(a.zones)
        start = time.perf_counter()
        trips = spark.read.parquet(a.trips)
        rows = borough_hourly(trips, zones).collect()
        seconds = time.perf_counter() - start
        with open(a.out, "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["pickup_borough", "pickup_hour", "trips", "avg_miles", "avg_minutes", "avg_fare_usd"])
            w.writerows([list(r) for r in rows])
        print(json.dumps({"rows": len(rows), "seconds": round(seconds, 2)}))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

"""
Vehicle sensor data analysis (PySpark) - generic, all-columns version
----------------------------------------------------------------------
Reads parquet from S3 and aggregates ALL columns PER TRUCK (per VIN) at
four time resolutions: 1 minute, 15 minutes, 1 hour, 1 day.

You do NOT list columns. The script auto-detects them:
  - numeric columns  -> avg / min / max / last
  - text / metadata  -> last (most recent value inside the bucket)
  - keys + timestamp -> used for grouping, not aggregated

Each resolution is written to its own S3 prefix, partitioned by VIN so
every truck's output is separate.

Run on AWS Glue / EMR / standalone Spark:
    spark-submit vin_vehicle_aggregate.py
"""

from pyspark.sql import SparkSession, functions as F

# --------------------------------------------------------------------------- #
# Config - edit these
# --------------------------------------------------------------------------- #
INPUT_PATH  = "s3://your-bucket/sensor-data/"        # source parquet
OUTPUT_BASE = "s3://your-bucket/analysis-output/"    # results root

VIN_COL = "metaData_vin"
ID_COL  = "metaData_vehicleId"
TS_COL  = "window_start"     # timestamp used for bucketing (or first_reading_ts)

# resolution name -> Spark window size
BUCKETS = {
    "1min":  "1 minute",
    "15min": "15 minutes",
    "1hour": "1 hour",
    "1day":  "1 day",
}

# Spark numeric type prefixes
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")

spark = (
    SparkSession.builder
    .appName("vin-vehicle-aggregate")
    .getOrCreate()
)


# --------------------------------------------------------------------------- #
# Build aggregation expressions for every column automatically
# --------------------------------------------------------------------------- #
def build_agg_exprs(df):
    keys = {VIN_COL, ID_COL, TS_COL}
    exprs = []
    for name, dtype in df.dtypes:
        if name in keys:
            continue
        if dtype.startswith(NUMERIC_TYPES):
            exprs += [
                F.avg(name).alias(f"{name}_avg"),
                F.min(name).alias(f"{name}_min"),
                F.max(name).alias(f"{name}_max"),
                F.last(name, ignorenulls=True).alias(f"{name}_last"),
            ]
        else:
            # text / timestamp / boolean metadata: keep the latest value
            exprs.append(F.last(name, ignorenulls=True).alias(name))
    exprs.append(F.count(F.lit(1)).alias("record_count"))
    return exprs


def aggregate_per_truck(df, bucket, agg_exprs):
    """One row per (vin, vehicleId, time-bucket)."""
    return (
        df.groupBy(
            F.col(VIN_COL),
            F.col(ID_COL),
            F.window(F.col(TS_COL), bucket).alias("w"),
        )
        .agg(*agg_exprs)
        .withColumn("bucket_start", F.col("w.start"))
        .withColumn("bucket_end",   F.col("w.end"))
        .drop("w")
        .orderBy(VIN_COL, ID_COL, "bucket_start")
    )


def write_analysis(df, name):
    out = OUTPUT_BASE.rstrip("/") + "/" + name + "/"
    df.write.mode("overwrite").partitionBy(VIN_COL).parquet(out)
    print(f"[done] {name} -> {out}")


# --------------------------------------------------------------------------- #
def main():
    src = spark.read.parquet(INPUT_PATH)
    src = src.withColumn(TS_COL, F.col(TS_COL).cast("timestamp"))

    agg_exprs = build_agg_exprs(src)   # computed once from the schema

    for label, window_size in BUCKETS.items():
        result = aggregate_per_truck(src, window_size, agg_exprs)
        write_analysis(result, label)

    spark.stop()


if __name__ == "__main__":
    main()

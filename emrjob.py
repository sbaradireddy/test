# ============================================================
# compact_iot_parquet.py
# EMR Serverless Spark job: compact small parquet files into
# ~256 MB files, partitioned by date.
# ============================================================
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------- CONFIG ----------------
SRC = "s3://pske-stg-maintenance/projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
DST = "s3://pske-stg-maintenance/projects/ard-iot-data-compacted/ps.NA_PENSKE_TRIAL.measurements.results/"

TARGET_FILE_MB   = 256          # target parquet file size
SHUFFLE_PARTS    = 400          # tune based on total data volume
# ----------------------------------------

spark = (SparkSession.builder
    .appName("compact-penske-iot-parquet")
    # Critical settings for compaction:
    .config("spark.sql.parquet.mergeSchema", "true")           # different SchemaTypes
    .config("spark.sql.files.ignoreCorruptFiles", "true")      # don't die on one bad file
    .config("spark.sql.adaptive.enabled", "true")              # AQE: auto-coalesces partitions
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",
            str(TARGET_FILE_MB * 1024 * 1024))                 # tells AQE the target
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
    # S3 write performance:
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate())

print(f"Reading from: {SRC}")

# recursiveFileLookup walks every date/HHMMSS/SchemaType*.parquet
df = (spark.read
      .option("recursiveFileLookup", "true")
      .option("mergeSchema", "true")
      .parquet(SRC))

# Extract date from the input file path so we can re-partition by it.
# input_file_name() looks like .../20260520/054500/SchemaType104-...parquet
df = df.withColumn("_src_path", F.input_file_name())
df = df.withColumn(
    "event_date",
    F.regexp_extract(F.col("_src_path"), r"/(\d{8})/\d+/[^/]+\.parquet$", 1)
).drop("_src_path")

# Sanity check: any rows where we failed to extract a date?
bad = df.filter(F.col("event_date") == "").limit(1).count()
if bad:
    print("WARNING: some files didn't match the expected /YYYYMMDD/HHMMSS/ pattern.")

# Repartition by date so each day gets its own writers (avoids one huge shuffle).
# AQE + advisoryPartitionSizeInBytes will then size files near 256 MB.
df = df.repartition("event_date")

print(f"Writing to: {DST}")
(df.write
   .mode("overwrite")
   .partitionBy("event_date")
   .option("compression", "snappy")
   .parquet(DST))

print("Compaction complete.")
spark.stop()

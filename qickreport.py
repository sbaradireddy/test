%glue_version 4.0
%worker_type G.2X
%number_of_workers 10
%idle_timeout 60
%%configure
{
  "--conf": "spark.sql.parquet.mergeSchema=false --conf spark.driver.maxResultSize=4g"
}

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------- CONFIG ----------------
SRC = "s3://pske-stg-maintenance/projects/ard-iot-data-compacted/"   # your snappy parquet location
DST = "s3://pske-stg-maintenance/analytics/measurement_name_data/"   # CSV output
COL = "metadata_measurementconfigurationname"
# ----------------------------------------

# Read parquet but project ONLY the one column (columnar pushdown = fast)
df = (spark.read
      .option("mergeSchema", "false")
      .option("recursiveFileLookup", "true")
      .parquet(SRC)
      .select(COL))

print("Rows:", df.count())
df.show(20, truncate=False)

# Single CSV file output (coalesce(1)). The column is small, so this is safe.
(df.coalesce(1)
   .write
   .mode("overwrite")
   .option("header", "true")
   .csv(DST))

print("Written to:", DST)


total = df.count()

summary = (df.groupBy(COL)
           .agg(F.count("*").alias("row_count"))
           .withColumn("pct", F.round(F.col("row_count") / total * 100, 1))
           .orderBy(F.desc("row_count")))

# add rank
from pyspark.sql.window import Window
summary = summary.withColumn(
    "rank", F.row_number().over(Window.orderBy(F.desc("row_count"))))

summary.show(truncate=False)

(summary.coalesce(1)
   .write.mode("overwrite")
   .option("header", "true")
   .csv("s3://pske-stg-maintenance/analytics/measurement_name_summary/"))

print("Summary written.")

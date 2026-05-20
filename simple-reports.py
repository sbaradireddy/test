import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from collections import defaultdict
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BUCKET           = "pske-stg-maintenance"
SOURCE_PREFIX    = "projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUTPUT_PREFIX    = "projects/ard-iot-data/reporting/"
FILES_PER_SCHEMA = 5
RUN_TS           = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3               = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPER 1 — type resolution
# ══════════════════════════════════════════════════════════════════════════════
TYPE_RANK = {
    "bool":1,
    "int8":2,   "int16":3,   "int32":4,   "int64":5,
    "uint8":2,  "uint16":3,  "uint32":4,  "uint64":5,
    "float":6,  "float16":6, "float32":7, "float64":8, "double":8,
    "date32":9, "date64":9,
    "timestamp[ms]":10, "timestamp[us]":10, "timestamp[ns]":10,
    "string":11, "utf8":11,  "large_utf8":11,
}

def resolve_type(dtypes):
    unique = list(set(dtypes))
    if len(unique) == 1:
        return unique[0]
    return sorted(
        unique,
        key=lambda t: TYPE_RANK.get(t.lower().strip(), 99),
        reverse=True
    )[0]

def to_spark_type(d):
    d = d.lower().strip()
    if d == "bool":                                      return T.BooleanType()
    if d in ("int8","int16","int32","uint8","uint16"):   return T.IntegerType()
    if d in ("int64","uint32","uint64"):                 return T.LongType()
    if d in ("float","float16","float32"):               return T.FloatType()
    if d in ("float64","double"):                        return T.DoubleType()
    if "timestamp" in d:                                 return T.TimestampType()
    if d in ("date32","date64"):                         return T.DateType()
    return T.StringType()

# ══════════════════════════════════════════════════════════════════════════════
# HELPER 2 — safe column name (no dots/brackets for CSV/parquet headers)
# ══════════════════════════════════════════════════════════════════════════════
def safe(name):
    return (name
            .replace("[]",    "_arr")
            .replace("[key]", "_key")
            .replace("[value]","_val")
            .replace(".",     "_")
            .replace(" ",     "_")
            .lower())

# ══════════════════════════════════════════════════════════════════════════════
# HELPER 3 — PyArrow schema flattener (driver only, zero Spark)
# ══════════════════════════════════════════════════════════════════════════════
def flatten_arrow(schema, prefix=""):
    cols = []
    for field in schema:
        path  = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.type
        if pa.types.is_struct(dtype):
            cols.extend(flatten_arrow(dtype, prefix=path))
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            elem = dtype.value_type
            if pa.types.is_struct(elem):
                cols.extend(flatten_arrow(elem, prefix=f"{path}[]"))
            else:
                cols.append((f"{path}[]", str(elem)))
        elif pa.types.is_map(dtype):
            cols.append((f"{path}[key]",   str(dtype.key_type)))
            cols.append((f"{path}[value]", str(dtype.item_type)))
        else:
            cols.append((path, str(dtype)))
    return cols

# ══════════════════════════════════════════════════════════════════════════════
# HELPER 4 — Spark flatten + cast in one select()
# ══════════════════════════════════════════════════════════════════════════════
def spark_flat_cast(schema, unified_schema, prefix=""):
    exprs = []
    for field in schema.fields:
        path    = f"{prefix}.{field.name}" if prefix else field.name
        parts   = path.split(".")
        col_ref = F.col(".".join(f"`{p}`" for p in parts))
        dtype   = field.dataType
        if isinstance(dtype, T.StructType):
            exprs.extend(spark_flat_cast(dtype, unified_schema, prefix=path))
        elif isinstance(dtype, (T.ArrayType, T.MapType)):
            exprs.append(col_ref.cast("string").alias(safe(path)))
        else:
            target = to_spark_type(unified_schema.get(path, "string"))
            exprs.append(col_ref.cast(target).alias(safe(path)))
    return exprs

# ══════════════════════════════════════════════════════════════════════════════
# HELPER 5 — write DataFrame as single CSV to S3
# ══════════════════════════════════════════════════════════════════════════════
def write_csv(df, filename, desc=""):
    path = f"s3://{BUCKET}/{OUTPUT_PREFIX}csv/{filename}/"
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(path)
    print(f"  ✓ {desc} → {path}")
    return path

def write_parquet(df, folder):
    path = f"s3://{BUCKET}/{OUTPUT_PREFIX}parquet/{folder}/"
    (df.write.mode("overwrite")
       .partitionBy("_date_folder","_schema_type")
       .parquet(path))
    print(f"  ✓ parquet → {path}")
    return path

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — S3 listing (zero data read, just key names)
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print("STEP 1 — S3 listing")
print("━"*60)

schema_files     = defaultdict(list)
total_file_count = 0
paginator        = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        total_file_count += 1
        sname = key.split("/")[-1].split("-")[0]   # SchemaType1, SchemaType10 ...
        if len(schema_files[sname]) < FILES_PER_SCHEMA:
            schema_files[sname].append(key)

print(f"  Total files in S3 : {total_file_count:,}")
print(f"  SchemaTypes       : {sorted(schema_files.keys())}")
print(f"  Files to process  : {sum(len(v) for v in schema_files.values())}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PyArrow schema discovery (driver only — seconds not hours)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 2 — schema discovery (PyArrow, driver only)")
print("━"*60)

col_types   = defaultdict(list)   # original col name → [dtype, ...]
col_samples = defaultdict(list)   # original col name → [sample values]
col_schemas = defaultdict(set)    # original col name → {schema names}

for sname, keys in sorted(schema_files.items()):
    for key in keys:
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            pf  = pq.ParquetFile(BytesIO(raw))

            # read first 5 rows only — uses row-group metadata, very fast
            try:
                batch    = next(pf.iter_batches(batch_size=5))
                row_dict = batch.to_pydict()
            except StopIteration:
                row_dict = {}

            for col_name, dtype_str in flatten_arrow(pf.schema_arrow):
                col_types[col_name].append(dtype_str)
                col_schemas[col_name].add(sname)
                top  = col_name.split(".")[0].rstrip("[]")
                for v in row_dict.get(top, [])[:3]:
                    s = "" if v is None else str(v)[:120]
                    if s and s not in col_samples[col_name]:
                        col_samples[col_name].append(s)

        except Exception as e:
            print(f"  WARN: {key} → {e}")

print(f"  Unique columns found: {len(col_types):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — build unified schema (pure Python on driver, instant)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — unified schema")
print("━"*60)

unified_schema = {}   # original col name → resolved dtype string
schema_report  = []   # rows for CSV output

for col_name in sorted(col_types.keys()):
    dtypes   = col_types[col_name]
    unified  = resolve_type(dtypes)
    present  = sorted(col_schemas[col_name])
    samples  = (col_samples.get(col_name, []) + ["","",""])[:3]

    unified_schema[col_name] = unified
    schema_report.append((
        col_name,                              # original nested name
        safe(col_name),                        # flat safe name for CSV/parquet
        unified,                               # resolved dtype
        " | ".join(sorted(set(dtypes))),       # all raw dtypes seen
        ", ".join(present),                    # which schemas have this col
        len(present),                          # how many schemas
        str(len(present) == len(schema_files)),# True = in every schema
        samples[0], samples[1], samples[2],    # sample values
    ))

# write schema map CSV
schema_map_df = spark.createDataFrame(schema_report, [
    "original_column", "safe_column", "unified_dtype",
    "raw_dtypes", "present_in", "schema_count",
    "common_to_all", "sample_1", "sample_2", "sample_3",
])
write_csv(
    schema_map_df.orderBy(F.desc("schema_count"), "original_column"),
    f"schema_map_{RUN_TS}",
    "schema map"
)
print(f"  Total unified columns: {len(unified_schema):,}")

all_safe_cols = [safe(c) for c in sorted(unified_schema.keys())]

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Spark: read files, flatten, cast, align, union
#           only real Spark work — N small files per schema type
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — Spark read + flatten + cast + union")
print("━"*60)

all_dfs = []

for sname, keys in sorted(schema_files.items()):
    s3_paths = [f"s3://{BUCKET}/{k}" for k in keys]
    print(f"  Reading {sname} ({len(s3_paths)} files) ...")

    try:
        raw_df  = spark.read.parquet(*s3_paths)
        flat_df = raw_df.select(
            spark_flat_cast(raw_df.schema, unified_schema)
        )

        # add any missing unified columns as typed nulls
        for orig, dtype_str in unified_schema.items():
            s = safe(orig)
            if s not in flat_df.columns:
                flat_df = flat_df.withColumn(
                    s, F.lit(None).cast(to_spark_type(dtype_str))
                )

        # reporting metadata
        flat_df = (flat_df
            .withColumn("_schema_type", F.lit(sname))
            .withColumn("_date_folder",
                F.regexp_extract(F.input_file_name(), r"/(\d{8})/", 1))
            .withColumn("_mile_bucket",
                F.regexp_extract(F.input_file_name(), r"/(\d{6})/", 1))
        )

        all_dfs.append(
            flat_df.select(all_safe_cols
                + ["_schema_type","_date_folder","_mile_bucket"])
        )
        print(f"    rows: {flat_df.count():,}  cols: {len(flat_df.columns)}")

    except Exception as e:
        print(f"  ERROR {sname}: {e} — skipping")

# union all schema types in one shot
print("\n  Merging all SchemaTypes ...")
unified_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    all_dfs
)

# single dedup pass
unified_df = unified_df.dropDuplicates(all_safe_cols)
unified_df.cache()

total_rows = unified_df.count()
total_cols = len(unified_df.columns)
print(f"\n  ✓ Final rows : {total_rows:,}")
print(f"  ✓ Final cols : {total_cols}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — write all outputs to S3
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — writing to S3")
print("━"*60)

# 1 — unified data parquet (partitioned — fastest for QuickSight)
write_parquet(unified_df, "unified_vehicle_data")

# 2 — unified data CSV (flat single file)
write_csv(unified_df, f"unified_vehicle_data_{RUN_TS}", "unified data CSV")

# 3 — per-SchemaType summary (row count + column count per schema)
summary_df = (unified_df
    .groupBy("_schema_type")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("_date_folder").alias("date_count"),
        F.min("_date_folder").alias("earliest_date"),
        F.max("_date_folder").alias("latest_date"),
    )
    .orderBy("_schema_type")
)
write_csv(summary_df, f"schema_summary_{RUN_TS}", "schema summary")

# 4 — column fill rate (how populated each column is — useful for QuickSight)
fill_exprs = [
    (F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)) / F.count("*") * 100)
     .alias(c)
    for c in all_safe_cols
]
fill_row   = unified_df.agg(*fill_exprs).collect()[0]
fill_data  = [(c, round(fill_row[c] or 0.0, 1)) for c in all_safe_cols]
fill_df    = spark.createDataFrame(fill_data, ["column_name","fill_pct"])
write_csv(
    fill_df.orderBy(F.desc("fill_pct")),
    f"column_fill_rates_{RUN_TS}",
    "column fill rates"
)

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"""
{"━"*60}
  DONE
  SchemaTypes     : {len(schema_files)}
  Total S3 files  : {total_file_count:,}
  Files processed : {sum(len(v) for v in schema_files.values())}
  Unified columns : {total_cols}
  Clean rows      : {total_rows:,}

  S3 outputs:
  s3://{BUCKET}/{OUTPUT_PREFIX}parquet/unified_vehicle_data/
  s3://{BUCKET}/{OUTPUT_PREFIX}csv/schema_map_{RUN_TS}/
  s3://{BUCKET}/{OUTPUT_PREFIX}csv/unified_vehicle_data_{RUN_TS}/
  s3://{BUCKET}/{OUTPUT_PREFIX}csv/schema_summary_{RUN_TS}/
  s3://{BUCKET}/{OUTPUT_PREFIX}csv/column_fill_rates_{RUN_TS}/
{"━"*60}
""")

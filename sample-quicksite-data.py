import sys
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from collections import defaultdict
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

# ── init ──────────────────────────────────────────────────────────────────────
args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc    = SparkContext()
glue  = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(args["JOB_NAME"], args)

# ── config ─────────────────────────────────────────────────────────────────────
BUCKET         = "pske-stg-maintenance"
PREFIX         = "projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUTPUT_PARQUET = "s3://pske-stg-maintenance/projects/ard-iot-data/reporting/quicksight/"
OUTPUT_CSV     = "s3://pske-stg-maintenance/projects/ard-iot-data/reporting/csv-outputs/"
RUN_TS         = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
FILES_PER_SCHEMA = 3      # sample files per SchemaType to read
s3             = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — collect N files per SchemaType from S3 listing
#           runs on driver, zero data read
# ══════════════════════════════════════════════════════════════════════════════
print("Step 1 — S3 listing ...")

# schema_files = { "SchemaType1": ["s3://...", "s3://...", "s3://..."] }
schema_files  = defaultdict(list)
schema_keys   = defaultdict(list)   # raw keys for PyArrow reads

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        sname = key.split("/")[-1].split("-")[0]   # SchemaType1
        if len(schema_files[sname]) < FILES_PER_SCHEMA:
            schema_files[sname].append(f"s3://{BUCKET}/{key}")
            schema_keys[sname].append(key)

print(f"  SchemaTypes : {sorted(schema_files.keys())}")
print(f"  Files queued: {sum(len(v) for v in schema_files.values())}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PyArrow schema discovery + sample data
#           ENTIRELY on driver — zero Spark overhead
#           reads only row-group-0 of each file (few KB)
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 2 — schema discovery + sample rows (PyArrow, driver only) ...")

TYPE_PRIORITY = {
    "bool": 1,
    "int8": 2,  "int16": 3,  "int32": 4,  "int64": 5,
    "uint8": 2, "uint16": 3, "uint32": 4, "uint64": 5,
    "float": 6, "float16": 6, "float32": 7, "float64": 8, "double": 8,
    "date32": 9, "date64": 9,
    "timestamp[ms]": 10, "timestamp[us]": 10, "timestamp[ns]": 10,
    "string": 11, "utf8": 11, "large_utf8": 11,
}

def resolve_type(dtypes):
    unique = list(set(dtypes))
    if len(unique) == 1:
        return unique[0]
    return sorted(unique,
                  key=lambda t: TYPE_PRIORITY.get(t.lower(), 99),
                  reverse=True)[0]

def to_spark_type(dtype_str):
    d = dtype_str.lower()
    if d == "bool":                                    return T.BooleanType()
    if d in ("int8","int16","int32","uint8","uint16"): return T.IntegerType()
    if d in ("int64","uint32","uint64"):               return T.LongType()
    if d in ("float","float16","float32"):             return T.FloatType()
    if d in ("float64","double"):                      return T.DoubleType()
    if "timestamp" in d:                               return T.TimestampType()
    if d in ("date32","date64"):                       return T.DateType()
    return T.StringType()

def flatten_arrow_schema(schema, prefix=""):
    cols = []
    for field in schema:
        path  = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.type
        if pa.types.is_struct(dtype):
            cols.extend(flatten_arrow_schema(dtype, prefix=path))
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            elem = dtype.value_type
            if pa.types.is_struct(elem):
                cols.extend(flatten_arrow_schema(elem, prefix=f"{path}[]"))
            else:
                cols.append((f"{path}[]", str(elem)))
        elif pa.types.is_map(dtype):
            cols.append((f"{path}[key]",   str(dtype.key_type)))
            cols.append((f"{path}[value]", str(dtype.item_type)))
        else:
            cols.append((path, str(dtype)))
    return cols

def safe_str(v):
    if v is None: return ""
    if isinstance(v, (list, dict)): return str(v)
    return str(v)

# col_type_map[col]    = [dtype from each schema]
# col_samples[col]     = [sample values]
# schema_col_map[sname]= [col names] — which cols are in each schema
col_type_map   = defaultdict(list)
col_samples    = defaultdict(list)
schema_col_map = defaultdict(list)

for sname, keys in sorted(schema_keys.items()):
    print(f"  {sname} ({len(keys)} files) ...")
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()
        pf  = pq.ParquetFile(BytesIO(raw))

        # read first batch only — very fast, few rows
        try:
            batch    = next(pf.iter_batches(batch_size=5))
            row_dict = batch.to_pydict()
        except StopIteration:
            row_dict = {}

        flat_cols = flatten_arrow_schema(pf.schema_arrow)
        for col_name, dtype_str in flat_cols:
            col_type_map[col_name].append(dtype_str)
            schema_col_map[sname].append(col_name)
            # get up to 3 sample values from top-level key
            top = col_name.split(".")[0].rstrip("[]")
            vals = row_dict.get(top, [])
            for v in vals[:3]:
                s = safe_str(v)
                if s and s not in col_samples[col_name]:
                    col_samples[col_name].append(s)

print(f"  Unique columns discovered: {len(col_type_map):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — build unified schema (single pass on driver, instant)
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 3 — building unified schema ...")

unified_schema = {}   # col_name → resolved dtype string
schema_report  = []

for col_name in sorted(col_type_map.keys()):
    dtypes        = col_type_map[col_name]
    unified_dtype = resolve_type(dtypes)
    present_in    = sorted({
        sname for sname, cols in schema_col_map.items()
        if col_name in cols
    })
    samples = col_samples.get(col_name, [])[:3]
    # pad samples to 3
    while len(samples) < 3:
        samples.append("")

    unified_schema[col_name] = unified_dtype
    schema_report.append((
        col_name,
        unified_dtype,
        " | ".join(sorted(set(dtypes))),
        ", ".join(present_in),
        len(present_in),
        samples[0], samples[1], samples[2],
    ))

# write unified schema CSV instantly (small df, no Spark action on big data)
schema_df = spark.createDataFrame(
    schema_report,
    ["column_name", "unified_dtype", "raw_dtypes",
     "present_in_schemas", "schema_count",
     "sample_1", "sample_2", "sample_3"]
)
(schema_df
 .orderBy(F.desc("schema_count"), "column_name")
 .coalesce(1).write.mode("overwrite")
 .option("header", "true")
 .csv(f"{OUTPUT_CSV}unified_schema_{RUN_TS}/"))
print(f"  Unified schema CSV → {OUTPUT_CSV}unified_schema_{RUN_TS}/")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Spark: read sampled files, flatten, cast, align, union
#           Spark only used here — on small set of files
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 4 — reading sampled files with Spark ...")

all_unified_cols = list(unified_schema.keys())

def spark_flatten_cast(schema, unified_schema, prefix=""):
    """One select() flattens + casts everything — zero loops at runtime."""
    exprs = []
    for field in schema.fields:
        path    = f"{prefix}.{field.name}" if prefix else field.name
        parts   = path.split(".")
        col_ref = F.col(".".join(f"`{p}`" for p in parts))
        dtype   = field.dataType
        if isinstance(dtype, T.StructType):
            exprs.extend(spark_flatten_cast(dtype, unified_schema, prefix=path))
        elif isinstance(dtype, (T.ArrayType, T.MapType)):
            exprs.append(col_ref.cast("string").alias(path))
        else:
            target = to_spark_type(unified_schema.get(path, "string"))
            exprs.append(col_ref.cast(target).alias(path))
    return exprs

all_dfs = []

for sname, files in sorted(schema_files.items()):
    print(f"  {sname}: reading {len(files)} files ...")

    raw_df   = spark.read.parquet(*files)
    exprs    = spark_flatten_cast(raw_df.schema, unified_schema)
    flat_df  = raw_df.select(exprs)

    # add missing unified columns as typed nulls
    for col in all_unified_cols:
        if col not in flat_df.columns:
            flat_df = flat_df.withColumn(
                col, F.lit(None).cast(to_spark_type(unified_schema[col]))
            )

    # metadata columns
    flat_df = (flat_df
               .withColumn("_schema_type", F.lit(sname))
               .withColumn("_date_folder",
                   F.regexp_extract(F.input_file_name(), r"/(\d{8})/", 1)))

    all_dfs.append(flat_df.select(
        all_unified_cols + ["_schema_type", "_date_folder"]
    ))

# union all in one shot
print("  Unioning all SchemaTypes ...")
from functools import reduce
unified_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    all_dfs
)

# single dedup pass on all data columns
unified_df = unified_df.dropDuplicates(all_unified_cols)
unified_df.cache()

total_rows = unified_df.count()
print(f"  Final unified rows : {total_rows:,}")
print(f"  Final unified cols : {len(unified_df.columns)}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — write outputs
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 5 — writing outputs ...")

# Parquet — partitioned for QuickSight SPICE
(unified_df
 .write.mode("overwrite")
 .partitionBy("_date_folder", "_schema_type")
 .parquet(f"{OUTPUT_PARQUET}unified_data_{RUN_TS}/"))
print(f"  Parquet → {OUTPUT_PARQUET}unified_data_{RUN_TS}/")

# CSV — flat single file
(unified_df
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", "true")
 .csv(f"{OUTPUT_CSV}unified_data_{RUN_TS}/"))
print(f"  CSV     → {OUTPUT_CSV}unified_data_{RUN_TS}/")

print("\nDONE")
job.commit()

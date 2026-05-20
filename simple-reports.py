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
MIN_FILE_BYTES   = 100          # anything under 100 bytes is treated as empty
RUN_TS           = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3               = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
TYPE_RANK = {
    "bool":1,
    "int8":2,  "int16":3,  "int32":4,  "int64":5,
    "uint8":2, "uint16":3, "uint32":4, "uint64":5,
    "float":6, "float16":6,"float32":7,"float64":8,"double":8,
    "date32":9,"date64":9,
    "timestamp[ms]":10,"timestamp[us]":10,"timestamp[ns]":10,
    "string":11,"utf8":11,"large_utf8":11,
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
    if d == "bool":                                     return T.BooleanType()
    if d in ("int8","int16","int32","uint8","uint16"):  return T.IntegerType()
    if d in ("int64","uint32","uint64"):                return T.LongType()
    if d in ("float","float16","float32"):              return T.FloatType()
    if d in ("float64","double"):                       return T.DoubleType()
    if "timestamp" in d:                               return T.TimestampType()
    if d in ("date32","date64"):                        return T.DateType()
    return T.StringType()

def safe(name):
    return (name
            .replace("[]",     "_arr")
            .replace("[key]",  "_key")
            .replace("[value]","_val")
            .replace(".",      "_")
            .replace(" ",      "_")
            .lower())

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

def write_csv(df, filename, desc=""):
    path = f"s3://{BUCKET}/{OUTPUT_PREFIX}csv/{filename}/"
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(path)
    print(f"  ✓ {desc} → {path}")

def write_parquet(df, folder):
    path = f"s3://{BUCKET}/{OUTPUT_PREFIX}parquet/{folder}/"
    (df.write.mode("overwrite")
       .partitionBy("_date_folder","_schema_type")
       .parquet(path))
    print(f"  ✓ parquet → {path}")

# ══════════════════════════════════════════════════════════════════════════════
# KEY FIX — safe union that never operates on empty list
# ══════════════════════════════════════════════════════════════════════════════
def safe_union(dfs):
    """
    Union a list of DataFrames safely.
    Never calls reduce on empty list.
    Returns None if list is empty.
    """
    # remove any None entries first
    valid = [df for df in dfs if df is not None]

    if len(valid) == 0:
        return None
    if len(valid) == 1:
        return valid[0]

    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        valid
    )

# ══════════════════════════════════════════════════════════════════════════════
# KEY FIX — safe single-file Spark reader
# ══════════════════════════════════════════════════════════════════════════════
def safe_spark_read(paths):
    """
    Read parquet files one by one.
    Skip any that cause empty schema / empty dataset errors.
    Returns DataFrame or None — never raises.
    """
    good = []
    for path in paths:
        try:
            df = spark.read.parquet(path)
            # must have columns AND rows
            if len(df.columns) == 0:
                print(f"    SKIP (0 columns): {path.split('/')[-1]}")
                continue
            if df.limit(1).count() == 0:
                print(f"    SKIP (0 rows)   : {path.split('/')[-1]}")
                continue
            good.append(df)
        except Exception as e:
            print(f"    SKIP (error)    : {path.split('/')[-1]} → {e}")
            continue

    return safe_union(good)   # safe — handles empty list

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — S3 listing — skip zero/tiny files immediately
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print("STEP 1 — S3 listing")
print("━"*60)

schema_files     = defaultdict(list)
skipped_listing  = []
total_count      = 0
paginator        = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key  = obj["Key"]
        size = obj.get("Size", 0)
        if not key.endswith(".parquet"):
            continue
        total_count += 1
        if size < MIN_FILE_BYTES:
            skipped_listing.append((key, size, "below min size"))
            continue
        sname = key.split("/")[-1].split("-")[0]
        if len(schema_files[sname]) < FILES_PER_SCHEMA:
            schema_files[sname].append((key, size))

print(f"  Total parquet files : {total_count:,}")
print(f"  Skipped (too small) : {len(skipped_listing):,}")
print(f"  SchemaTypes found   : {sorted(schema_files.keys())}")
print(f"  Files queued        : {sum(len(v) for v in schema_files.values())}")

# ── hard stop if nothing found at all ────────────────────────────────────────
if not schema_files:
    print("\n  ERROR: No valid files found. Possible causes:")
    print("  1. SOURCE_PREFIX is wrong — check the path")
    print("  2. All files are under 100 bytes — lower MIN_FILE_BYTES")
    print("  3. No .parquet extension — check file names in S3")
    raise RuntimeError("Step 1 failed — 0 valid files found")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PyArrow schema discovery
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 2 — schema discovery (PyArrow, driver only)")
print("━"*60)

col_types    = defaultdict(list)
col_samples  = defaultdict(list)
col_schemas  = defaultdict(set)
good_keys    = defaultdict(list)   # only files that passed validation
skipped_step2 = []

for sname, key_size_list in sorted(schema_files.items()):
    print(f"  {sname}: checking {len(key_size_list)} files ...")

    for key, size in key_size_list:
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

            # guard: raw bytes too short to be valid parquet
            if len(raw) < MIN_FILE_BYTES:
                skipped_step2.append((sname, key, f"raw bytes={len(raw)}"))
                continue

            pf = pq.ParquetFile(BytesIO(raw))

            # guard: no columns
            if len(pf.schema_arrow) == 0:
                skipped_step2.append((sname, key, "0 columns in schema"))
                continue

            # guard: no rows
            if pf.metadata.num_rows == 0:
                skipped_step2.append((sname, key, "0 rows"))
                continue

            # read first 5 rows
            try:
                batch    = next(pf.iter_batches(batch_size=5))
                row_dict = batch.to_pydict()
            except StopIteration:
                row_dict = {}

            # collect schema + samples
            flat = flatten_arrow(pf.schema_arrow)
            if not flat:
                skipped_step2.append((sname, key, "flatten produced 0 cols"))
                continue

            for col_name, dtype_str in flat:
                col_types[col_name].append(dtype_str)
                col_schemas[col_name].add(sname)
                top = col_name.split(".")[0].rstrip("[]")
                for v in row_dict.get(top, [])[:3]:
                    s = "" if v is None else str(v)[:120]
                    if s and s not in col_samples[col_name]:
                        col_samples[col_name].append(s)

            good_keys[sname].append(f"s3://{BUCKET}/{key}")

        except Exception as e:
            skipped_step2.append((sname, key, str(e)))
            print(f"    SKIP: {key.split('/')[-1]} → {e}")

    valid_count   = len(good_keys.get(sname, []))
    skipped_count = len(key_size_list) - valid_count
    print(f"    valid={valid_count}  skipped={skipped_count}")

# write skip log
all_skipped = (
    [(k, str(sz), r) for k, sz, r in skipped_listing] +
    [(k, "", r)       for _, k, r  in skipped_step2]
)
if all_skipped:
    skip_df = spark.createDataFrame(all_skipped, ["s3_key","size","reason"])
    write_csv(skip_df, f"skipped_files_{RUN_TS}", "skipped files log")

# ── hard stop: no columns discovered at all ───────────────────────────────────
if not col_types:
    print("\n  ERROR: Schema discovery found 0 columns. Possible causes:")
    print("  1. All sampled files are empty or corrupt — see skipped_files CSV")
    print(f"  2. Try increasing FILES_PER_SCHEMA (currently {FILES_PER_SCHEMA})")
    print("  3. Files may not be standard parquet — verify format in S3")
    raise RuntimeError("Step 2 failed — 0 columns discovered")

if not good_keys:
    print("\n  ERROR: 0 files passed validation across all SchemaTypes")
    raise RuntimeError("Step 2 failed — no valid files")

print(f"\n  Unique columns : {len(col_types):,}")
print(f"  Valid files    : {sum(len(v) for v in good_keys.values())}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — unified schema (driver only, instant)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — unified schema")
print("━"*60)

unified_schema = {}
schema_report  = []

for col_name in sorted(col_types.keys()):
    dtypes  = col_types[col_name]
    unified = resolve_type(dtypes)
    present = sorted(col_schemas[col_name])
    samples = (col_samples.get(col_name, []) + ["","",""])[:3]

    unified_schema[col_name] = unified
    schema_report.append((
        col_name,
        safe(col_name),
        unified,
        " | ".join(sorted(set(dtypes))),
        ", ".join(present),
        len(present),
        str(len(present) == len(schema_files)),
        samples[0], samples[1], samples[2],
    ))

schema_map_df = spark.createDataFrame(schema_report, [
    "original_column","safe_column","unified_dtype",
    "raw_dtypes","present_in","schema_count",
    "common_to_all","sample_1","sample_2","sample_3",
])
write_csv(
    schema_map_df.orderBy(F.desc("schema_count"),"original_column"),
    f"schema_map_{RUN_TS}",
    "schema map"
)

all_safe_cols = [safe(c) for c in sorted(unified_schema.keys())]
print(f"  Unified columns: {len(unified_schema):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Spark read + flatten + cast
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — Spark read + flatten + cast")
print("━"*60)

all_dfs = []

for sname, paths in sorted(good_keys.items()):
    print(f"  {sname}: {len(paths)} files ...")

    # safe read — handles empty files, bad files, one-by-one fallback
    raw_df = safe_spark_read(paths)

    if raw_df is None:
        print(f"    SKIP {sname} — all files invalid after Spark read")
        continue

    try:
        # flatten + cast
        exprs = spark_flat_cast(raw_df.schema, unified_schema)
        if not exprs:
            print(f"    SKIP {sname} — spark_flat_cast produced 0 expressions")
            continue

        flat_df = raw_df.select(exprs)

        if len(flat_df.columns) == 0:
            print(f"    SKIP {sname} — 0 columns after flatten")
            continue

        # fill missing unified cols as typed nulls
        for orig, dtype_str in unified_schema.items():
            s = safe(orig)
            if s not in flat_df.columns:
                flat_df = flat_df.withColumn(
                    s, F.lit(None).cast(to_spark_type(dtype_str))
                )

        # metadata
        flat_df = (flat_df
            .withColumn("_schema_type", F.lit(sname))
            .withColumn("_date_folder",
                F.regexp_extract(F.input_file_name(), r"/(\d{8})/", 1))
            .withColumn("_mile_bucket",
                F.regexp_extract(F.input_file_name(), r"/(\d{6})/", 1))
        )

        # final column selection — guaranteed list is never empty here
        final_cols = all_safe_cols + ["_schema_type","_date_folder","_mile_bucket"]
        flat_df    = flat_df.select(final_cols)

        row_count = flat_df.count()
        if row_count == 0:
            print(f"    SKIP {sname} — 0 rows after flatten")
            continue

        print(f"    ✓ rows={row_count:,}  cols={len(flat_df.columns)}")
        all_dfs.append(flat_df)

    except Exception as e:
        print(f"    ERROR {sname}: {e}")
        continue

# ── hard stop with clear message ─────────────────────────────────────────────
if not all_dfs:
    print("\n  ERROR: all_dfs is empty — reduce() cannot run. Causes:")
    print("  1. All SchemaTypes were skipped — check output above for SKIP messages")
    print("  2. Spark could not read any file — check IAM permissions on S3")
    print("  3. All files empty after flatten — schema may have only nested types")
    print(f"  4. Try increasing FILES_PER_SCHEMA (currently {FILES_PER_SCHEMA})")
    raise RuntimeError(
        "Step 4 failed — 0 valid DataFrames. "
        "Check SKIP messages above and skipped_files CSV."
    )

# ── safe union — never reduce on empty list ───────────────────────────────────
print(f"\n  Unioning {len(all_dfs)} DataFrames ...")
unified_df = safe_union(all_dfs)   # handles 1, 2, or N dfs safely

unified_df = unified_df.dropDuplicates(all_safe_cols)
unified_df.cache()

total_rows = unified_df.count()
total_cols = len(unified_df.columns)
print(f"  ✓ Final rows : {total_rows:,}")
print(f"  ✓ Final cols : {total_cols}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — write all outputs
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — writing to S3")
print("━"*60)

write_parquet(unified_df, "unified_vehicle_data")
write_csv(unified_df,     f"unified_vehicle_data_{RUN_TS}", "unified data")

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

fill_exprs = [
    (F.sum(F.when(F.col(c).isNotNull(),1).otherwise(0)) / F.count("*") * 100)
     .alias(c)
    for c in all_safe_cols
]
fill_row  = unified_df.agg(*fill_exprs).collect()[0]
fill_data = [(c, round(fill_row[c] or 0.0, 1)) for c in all_safe_cols]
fill_df   = spark.createDataFrame(fill_data, ["column_name","fill_pct"])
write_csv(
    fill_df.orderBy(F.desc("fill_pct")),
    f"column_fill_rates_{RUN_TS}",
    "column fill rates"
)

print(f"""
{"━"*60}
  DONE
  Total S3 files   : {total_count:,}
  Skipped files    : {len(all_skipped):,}
  Files processed  : {sum(len(v) for v in good_keys.values())}
  SchemaTypes      : {len(all_dfs)}
  Unified columns  : {total_cols}
  Clean rows       : {total_rows:,}
{"━"*60}
""")

"""
Day 10 - Data Engineering Patterns in PySpark
Topics:
  1. UPSERT / MERGE  — insert new rows, update existing rows (Merge-on-Read in pure PySpark)
  2. CDC Processing  — Change Data Capture: apply I / U / D operations from a changelog
  3. SCD Type 2      — Slowly Changing Dimensions: keep full history with effective_from / effective_to
  4. Soft Delete     — mark rows as deleted instead of physically removing them
  5. Deduplication   — keep the latest version of a record using row_number

Data:
  employees_current.csv  — 10 existing employees (the "target" table)
  employees_incoming.csv — 6 incoming records (some existing, some new)
  orders_cdc.csv         — CDC changelog for orders (I=Insert, U=Update, D=Delete)
  products.csv           — products with soft-delete flags
"""

import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

os.environ['JAVA_HOME']      = 'C:/Program Files/DBeaver/jre'
os.environ['PYSPARK_PYTHON'] = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_date, current_timestamp,
    to_date, to_timestamp, row_number, max as spark_max
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Day10-DataEngineeringPatterns") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "day10-data-engineering-patterns/data"

# ─────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────

current_df = spark.read.csv(
    f"{DATA}/employees_current.csv", header=True, inferSchema=True
)
incoming_df = spark.read.csv(
    f"{DATA}/employees_incoming.csv", header=True, inferSchema=True
)
cdc_df = spark.read.csv(
    f"{DATA}/orders_cdc.csv", header=True, inferSchema=True
)
products_df = spark.read.csv(
    f"{DATA}/products.csv", header=True, inferSchema=True
)

print("=" * 60)
print("CURRENT EMPLOYEES (target table)")
print("=" * 60)
current_df.show()

print("INCOMING EMPLOYEES (source / staging)")
incoming_df.show()

print("CDC ORDERS LOG")
cdc_df.show(truncate=False)

print("PRODUCTS (with soft-delete flags)")
products_df.show(truncate=False)


# ─────────────────────────────────────────────────────────────
# SECTION 1: UPSERT — Insert OR Update (Merge-on-Read)
# ─────────────────────────────────────────────────────────────
# Logic:
#   - If emp_id exists in current  → UPDATE  (take values from incoming)
#   - If emp_id is new             → INSERT  (add the new row)
#   - If emp_id only in current    → KEEP AS-IS
#
# Pure PySpark approach (no Delta Lake):
#   Step 1: Split matched vs unmatched in current
#   Step 2: Replace matched rows with incoming values
#   Step 3: Union updated current + new inserts
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 1: UPSERT (Insert or Update)")
print("=" * 60)

# Step 1 — find which emp_ids are being updated
incoming_ids = incoming_df.select("emp_id")

# Rows in current that are NOT being updated → keep as-is
unchanged = current_df.join(incoming_ids, on="emp_id", how="left_anti")

# Step 2 — rows being updated: take values from incoming
#           incoming has no is_active / updated_at columns → add them
updated = incoming_df \
    .withColumn("is_active", lit(True)) \
    .withColumn("updated_at", current_date())

# Step 3 — union unchanged + updated (incoming already has new emp_ids too)
upsert_result = unchanged.unionByName(updated)

print("After UPSERT (unchanged + updated + new inserts):")
upsert_result.orderBy("emp_id").show()
print(f"Rows before: {current_df.count()}  |  After UPSERT: {upsert_result.count()}")


# ─────────────────────────────────────────────────────────────
# SECTION 2: UPSERT — only UPDATE matching rows, no inserts
# ─────────────────────────────────────────────────────────────
# Sometimes you only want to update fields of existing rows,
# not add new records. This shows the pattern.
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 2: UPDATE ONLY (no inserts)")
print("=" * 60)

# Alias both DataFrames to safely reference columns from each
curr = current_df.alias("curr")
inc  = incoming_df.alias("inc")

update_only = curr.join(inc, on="emp_id", how="left") \
    .select(
        col("curr.emp_id"),
        col("curr.emp_name"),
        # take incoming department if available, else keep current
        coalesce(col("inc.department"), col("curr.department")).alias("department"),
        # take incoming salary if available, else keep current
        coalesce(col("inc.salary"), col("curr.salary")).alias("salary"),
        col("curr.city"),
        col("curr.is_active"),
        # mark updated_at as today if record was touched
        when(col("inc.emp_id").isNotNull(), current_date())
            .otherwise(col("curr.updated_at")).alias("updated_at")
    )

print("After UPDATE ONLY (only existing rows, with selective field updates):")
update_only.orderBy("emp_id").show()


# ─────────────────────────────────────────────────────────────
# SECTION 3: CDC PROCESSING — Apply I / U / D operations
# ─────────────────────────────────────────────────────────────
# CDC (Change Data Capture) captures every DML operation on a
# source database and logs it with an operation code:
#   I = INSERT   (new row was added)
#   U = UPDATE   (existing row was changed)
#   D = DELETE   (row was removed)
#
# The log may contain multiple events per key. We need to apply
# them in order (by timestamp) to arrive at the final state.
#
# Steps:
#   1. Deduplicate: keep only the LATEST operation per order_id
#   2. Apply: rows with D are excluded; I/U rows form the final table
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 3: CDC PROCESSING (I / U / D)")
print("=" * 60)

print("Raw CDC log (all events in order):")
cdc_df.orderBy("order_id", "cdc_ts").show(truncate=False)

# Step 1 — for each order_id keep only the most recent event
w_cdc = Window.partitionBy("order_id").orderBy(col("cdc_ts").desc())

cdc_latest = cdc_df \
    .withColumn("rn", row_number().over(w_cdc)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("Latest event per order (after deduplication):")
cdc_latest.orderBy("order_id").show(truncate=False)

# Step 2 — exclude rows where final operation was DELETE
cdc_final = cdc_latest.filter(col("cdc_op") != "D").drop("cdc_op", "cdc_ts")

print("Final orders table (after applying I/U/D):")
cdc_final.orderBy("order_id").show()
print(f"Orders in CDC log: {cdc_df.select('order_id').distinct().count()}")
print(f"Orders surviving (not deleted): {cdc_final.count()}")


# ─────────────────────────────────────────────────────────────
# SECTION 4: SCD TYPE 2 — Slowly Changing Dimension
# ─────────────────────────────────────────────────────────────
# SCD Type 2 preserves the FULL HISTORY of changes.
# Each change creates a NEW row instead of overwriting.
# Rows have:
#   effective_from  — date when this version became active
#   effective_to    — date when this version was replaced
#                     (9999-12-31 = currently active)
#   is_current      — True / False flag for the active row
#
# Scenario:
#   - current_df is the existing SCD2 dimension (all rows have
#     effective_from = 2024-01-10 and effective_to = 9999-12-31)
#   - incoming_df has 6 records with changes (date 2024-06-01)
#   - For each changed record we:
#       a. Close the old row  (set effective_to = 2024-05-31, is_current=False)
#       b. Add a new row      (effective_from = 2024-06-01, effective_to = 9999-12-31)
#   - Unchanged and new records are handled as well
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 4: SCD TYPE 2 — Full History")
print("=" * 60)

CHANGE_DATE    = "2024-06-01"
FAR_FUTURE     = "9999-12-31"
PREV_DAY       = "2024-05-31"   # day before the change date

# Step 1 — Add SCD2 columns to current_df (simulate an existing SCD2 table)
scd2_existing = current_df \
    .withColumn("effective_from", to_date(lit("2024-01-10"))) \
    .withColumn("effective_to",   to_date(lit(FAR_FUTURE))) \
    .withColumn("is_current",     lit(True))

print("Existing SCD2 table (before changes):")
scd2_existing.show()

# Step 2 — Identify which current rows are being changed
changed_ids = incoming_df.select("emp_id")

# Rows being changed — close them off
rows_to_close = scd2_existing \
    .join(changed_ids, on="emp_id", how="inner") \
    .withColumn("effective_to", to_date(lit(PREV_DAY))) \
    .withColumn("is_current",   lit(False))

# Rows NOT being changed — keep as-is
rows_unchanged = scd2_existing \
    .join(changed_ids, on="emp_id", how="left_anti")

# Step 3 — Build new versions from incoming
new_versions = incoming_df \
    .withColumn("is_active",      lit(True)) \
    .withColumn("updated_at",     to_date(lit(CHANGE_DATE))) \
    .withColumn("effective_from", to_date(lit(CHANGE_DATE))) \
    .withColumn("effective_to",   to_date(lit(FAR_FUTURE))) \
    .withColumn("is_current",     lit(True))

# Step 4 — Union all three sets into the final SCD2 table
scd2_final = rows_unchanged \
    .unionByName(rows_to_close) \
    .unionByName(new_versions)

print("SCD2 table AFTER applying changes (full history preserved):")
scd2_final.orderBy("emp_id", "effective_from").show()

# Show only active (current) records
print("Active records only (is_current = True):")
scd2_final.filter(col("is_current") == True).orderBy("emp_id").show()

# Show history for a specific employee
print("Full history for E001 (Amit Sharma) and E003 (Ravi Kumar):")
scd2_final.filter(col("emp_id").isin("E001", "E003")) \
    .orderBy("emp_id", "effective_from").show()

print(f"Rows before SCD2: {current_df.count()}")
print(f"Rows after  SCD2: {scd2_final.count()} (includes closed + new versions)")


# ─────────────────────────────────────────────────────────────
# SECTION 5: SOFT DELETE
# ─────────────────────────────────────────────────────────────
# Instead of removing rows, mark them with a flag.
# Common columns: is_deleted (boolean), deleted_at (timestamp)
#
# Patterns:
#   a. Perform a soft delete
#   b. Query only active records (exclude deleted)
#   c. Query only deleted records (for audit / restore)
#   d. Restore a soft-deleted record
#   e. Hard purge — physically remove soft-deleted rows
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 5: SOFT DELETE")
print("=" * 60)

print("Products table (with existing soft deletes):")
products_df.show(truncate=False)

# a. Soft-delete a product that is currently active (e.g. P002)
ids_to_delete = ["P002", "P005"]

soft_deleted = products_df \
    .withColumn(
        "is_deleted",
        when(col("product_id").isin(ids_to_delete), lit(True))
            .otherwise(col("is_deleted"))
    ) \
    .withColumn(
        "deleted_at",
        when(col("product_id").isin(ids_to_delete), current_date().cast("string"))
            .otherwise(col("deleted_at"))
    )

print("After soft-deleting P002 and P005:")
soft_deleted.show(truncate=False)

# b. Query only ACTIVE products (the normal read path)
active_products = soft_deleted.filter(col("is_deleted") == False)
print("Active products only (is_deleted = false):")
active_products.show(truncate=False)

# c. Query DELETED products (audit / data recovery)
deleted_products = soft_deleted.filter(col("is_deleted") == True)
print("Deleted products only:")
deleted_products.show(truncate=False)

# d. Restore a soft-deleted product (P004 being reinstated)
restored = soft_deleted \
    .withColumn(
        "is_deleted",
        when(col("product_id") == "P004", lit(False))
            .otherwise(col("is_deleted"))
    ) \
    .withColumn(
        "deleted_at",
        when(col("product_id") == "P004", lit(None).cast("string"))
            .otherwise(col("deleted_at"))
    )

print("After restoring P004:")
restored.filter(col("product_id") == "P004").show(truncate=False)

# e. Hard purge — physically remove all soft-deleted rows
purged = soft_deleted.filter(col("is_deleted") == False)
print(f"After hard purge: {purged.count()} rows remain "
      f"(was {soft_deleted.count()}, removed {soft_deleted.count() - purged.count()} deleted rows)")
purged.show(truncate=False)


# ─────────────────────────────────────────────────────────────
# SECTION 6: DEDUPLICATION — Keep Latest Record per Key
# ─────────────────────────────────────────────────────────────
# Real ingestion pipelines can receive the same record multiple
# times (retry, duplicate feed). Deduplication ensures we keep
# only one row per business key — typically the most recent one.
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 6: DEDUPLICATION — Keep Latest per Key")
print("=" * 60)

# Simulate a raw landing table with duplicates
from pyspark.sql import Row

raw_data = [
    Row(emp_id="E001", emp_name="Amit Sharma",  salary=95000, loaded_at="2024-01-10 08:00:00"),
    Row(emp_id="E001", emp_name="Amit Sharma",  salary=95000, loaded_at="2024-01-10 08:05:00"),  # duplicate
    Row(emp_id="E002", emp_name="Priya Patel",  salary=72000, loaded_at="2024-01-10 09:00:00"),
    Row(emp_id="E001", emp_name="Amit Sharma",  salary=105000, loaded_at="2024-06-01 10:00:00"), # updated version
    Row(emp_id="E003", emp_name="Ravi Kumar",   salary=88000, loaded_at="2024-01-10 09:30:00"),
    Row(emp_id="E003", emp_name="Ravi Kumar",   salary=95000, loaded_at="2024-06-01 11:00:00"), # updated version
    Row(emp_id="E002", emp_name="Priya Patel",  salary=72000, loaded_at="2024-01-10 09:05:00"),  # duplicate
]

raw_df = spark.createDataFrame(raw_data)

print("Raw data with duplicates:")
raw_df.orderBy("emp_id", "loaded_at").show()

# Strategy 1 — row_number: keep the row with the latest loaded_at per emp_id
w_dedup = Window.partitionBy("emp_id").orderBy(col("loaded_at").desc())

deduped = raw_df \
    .withColumn("rn", row_number().over(w_dedup)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("After deduplication (latest row per emp_id):")
deduped.orderBy("emp_id").show()

# Strategy 2 — dropDuplicates: remove exact duplicates on specific columns
exact_dedup = raw_df.dropDuplicates(["emp_id", "salary", "loaded_at"])
print("After dropDuplicates on (emp_id, salary, loaded_at):")
exact_dedup.orderBy("emp_id", "loaded_at").show()

# Strategy 3 — keep MAX loaded_at per emp_id then re-join (groupBy approach)
max_ts = raw_df.groupBy("emp_id").agg(spark_max("loaded_at").alias("loaded_at"))
latest_join = raw_df.join(max_ts, on=["emp_id", "loaded_at"], how="inner")
print("After keep-max-timestamp approach:")
latest_join.orderBy("emp_id").show()


# ─────────────────────────────────────────────────────────────
# SECTION 7: COMBINED PATTERN — Upsert + SCD2 Summary
# ─────────────────────────────────────────────────────────────

print("=" * 60)
print("SECTION 7: PATTERN COMPARISON SUMMARY")
print("=" * 60)

patterns = [
    ("UPSERT",      "Overwrite matching rows, insert new ones",
     "left_anti for unchanged + union with incoming"),
    ("UPDATE ONLY", "Update fields of existing rows, no inserts",
     "left join current with incoming, coalesce fields"),
    ("CDC",         "Apply a stream of I/U/D ops to a table",
     "row_number to get latest op, filter out D rows"),
    ("SCD TYPE 2",  "Preserve full history of every change",
     "close old row (effective_to), insert new version"),
    ("SOFT DELETE", "Flag rows as deleted instead of removing",
     "is_deleted=True, filter on is_deleted=False for reads"),
    ("DEDUP",       "Eliminate duplicate records, keep latest",
     "row_number().over(partitionBy(key).orderBy(ts.desc()))"),
]

summary = spark.createDataFrame(
    patterns,
    ["Pattern", "Purpose", "PySpark Approach"]
)
summary.show(truncate=False)

spark.stop()
print("\nDay 10 complete.")

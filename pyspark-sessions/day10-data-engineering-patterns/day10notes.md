# Day 10 - Data Engineering Patterns

---

## Table of Contents
1. [UPSERT — Insert or Update](#1-upsert--insert-or-update)
2. [CDC Processing — I / U / D](#2-cdc-processing--i--u--d)
3. [SCD Type 2 — Full History](#3-scd-type-2--full-history)
4. [Soft Delete](#4-soft-delete)
5. [Deduplication — Keep Latest](#5-deduplication--keep-latest)
6. [Pattern Comparison](#6-pattern-comparison)
7. [Quick Reference](#7-quick-reference)

---

## 1. UPSERT — Insert or Update

### What it is
An UPSERT (also called **MERGE**) applies incoming data to a target table:
- If the key **already exists** → **UPDATE** the row with new values
- If the key is **new** → **INSERT** as a fresh row

### Pure PySpark approach (no Delta Lake)

```
Target table + Incoming batch
       ↓
left_anti join → rows NOT in incoming (unchanged) ─────────────────────────┐
                                                                            ↓
left_semi / inner join → rows IN incoming (to be updated) → use incoming ──→ unionByName → RESULT
                                                                            ↑
New rows in incoming (emp_ids not in target) ───────────────────────────────┘
```

```python
# Unchanged rows — exist in current but NOT in incoming
unchanged = current_df.join(incoming_ids, on="emp_id", how="left_anti")

# Updated + new rows — all rows from incoming
updated_and_new = incoming_df \
    .withColumn("is_active", lit(True)) \
    .withColumn("updated_at", current_date())

# Final result
result = unchanged.unionByName(updated_and_new)
```

### UPDATE ONLY (no inserts)
When you only want to update fields of existing rows and not add new records:

```python
curr.join(inc, on="emp_id", how="left") \
    .select(
        col("curr.emp_id"),
        coalesce(col("inc.salary"), col("curr.salary")).alias("salary"),
        when(col("inc.emp_id").isNotNull(), current_date())
            .otherwise(col("curr.updated_at")).alias("updated_at")
    )
```

`coalesce(incoming_val, current_val)` — takes incoming if not null, otherwise keeps current.

---

## 2. CDC Processing — I / U / D

### What is CDC
**Change Data Capture** records every database operation as an event in a log:

| Code | Operation | Meaning |
|---|---|---|
| `I` | INSERT | Row was added |
| `U` | UPDATE | Row was modified |
| `D` | DELETE | Row was removed |

The log grows continuously. Each key may appear multiple times. You need to apply them in timestamp order to get the final state.

### Two-step processing

**Step 1 — Deduplicate: keep latest event per key**
```python
w = Window.partitionBy("order_id").orderBy(col("cdc_ts").desc())

latest = cdc_df \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn")
```

**Step 2 — Apply: exclude rows whose final operation was DELETE**
```python
final_table = latest.filter(col("cdc_op") != "D").drop("cdc_op", "cdc_ts")
```

### Why deduplicate first?
A row may have received multiple updates. Taking only the latest event collapses all intermediate states into the correct final state before deciding whether to include or exclude the row.

### Full pattern
```
CDC log (multiple events per key, mixed I/U/D)
    ↓
row_number() partitionBy(key) orderBy(timestamp desc) → filter rn=1
    ↓
latest event per key
    ↓
filter cdc_op != 'D'
    ↓
Final state table
```

---

## 3. SCD Type 2 — Full History

### What is SCD
**Slowly Changing Dimension** — a dimension table whose rows change over time (e.g. employee department, salary, address).

| SCD Type | Behavior | History kept? |
|---|---|---|
| Type 1 | Overwrite the old value | No |
| **Type 2** | **Add a new row, close the old** | **Yes — full history** |
| Type 3 | Add a "previous value" column | Partial (1 version) |

### SCD2 columns

| Column | Purpose | Active row | Closed row |
|---|---|---|---|
| `effective_from` | When this version became active | e.g. `2024-06-01` | e.g. `2024-01-10` |
| `effective_to`   | When this version was replaced | `9999-12-31` | e.g. `2024-05-31` |
| `is_current`     | Quick flag for the active row | `True` | `False` |

### SCD2 algorithm (3-part union)

```
Existing SCD2 table
         ↓
┌────────────────────────────────────┐
│ rows NOT in incoming → keep as-is  │  (unchanged rows)
└────────────────────────────────────┘
         +
┌────────────────────────────────────────────────────────────────┐
│ rows IN incoming → close old version                           │
│   effective_to = change_date - 1 day   is_current = False      │
└────────────────────────────────────────────────────────────────┘
         +
┌────────────────────────────────────────────────────────────────┐
│ all incoming rows → new version                                │
│   effective_from = change_date   effective_to = 9999-12-31     │
│   is_current = True                                            │
└────────────────────────────────────────────────────────────────┘
         ↓
unionByName → final SCD2 table
```

### Code
```python
CHANGE_DATE = "2024-06-01"
FAR_FUTURE  = "9999-12-31"
PREV_DAY    = "2024-05-31"   # day before change

# 1. Close old versions of changed rows
rows_to_close = scd2_existing \
    .join(changed_ids, on="emp_id", how="inner") \
    .withColumn("effective_to", to_date(lit(PREV_DAY))) \
    .withColumn("is_current",   lit(False))

# 2. Keep unchanged rows
rows_unchanged = scd2_existing \
    .join(changed_ids, on="emp_id", how="left_anti")

# 3. New versions from incoming
new_versions = incoming_df \
    .withColumn("effective_from", to_date(lit(CHANGE_DATE))) \
    .withColumn("effective_to",   to_date(lit(FAR_FUTURE))) \
    .withColumn("is_current",     lit(True))

# 4. Combine
scd2_final = rows_unchanged.unionByName(rows_to_close).unionByName(new_versions)
```

### Querying SCD2

```python
# Current state (active rows only)
scd2.filter(col("is_current") == True)

# Point-in-time: what did the table look like on 2024-03-01?
target_date = to_date(lit("2024-03-01"))
scd2.filter(
    (col("effective_from") <= target_date) &
    (col("effective_to")   >= target_date)
)

# Full history for one employee
scd2.filter(col("emp_id") == "E001").orderBy("effective_from")
```

---

## 4. Soft Delete

### What is a soft delete
Instead of physically removing a row (`DELETE FROM table WHERE ...`), you set a flag:

```
is_deleted = True
deleted_at = <timestamp>
```

The row stays in the table. Application queries filter it out.

### Why soft delete?
- Audit trail — you know what was deleted and when
- Recovery — restore the row by setting `is_deleted = False`
- Referential integrity — foreign keys still resolve
- Regulatory compliance — some laws require keeping records

### Patterns

**Perform a soft delete**
```python
soft_deleted = df \
    .withColumn("is_deleted",
        when(col("product_id").isin(ids_to_delete), lit(True))
            .otherwise(col("is_deleted"))) \
    .withColumn("deleted_at",
        when(col("product_id").isin(ids_to_delete), current_date().cast("string"))
            .otherwise(col("deleted_at")))
```

**Read active records (standard query)**
```python
df.filter(col("is_deleted") == False)
```

**Read deleted records (audit)**
```python
df.filter(col("is_deleted") == True)
```

**Restore a soft-deleted row**
```python
df.withColumn("is_deleted",
        when(col("product_id") == restore_id, lit(False))
            .otherwise(col("is_deleted"))) \
  .withColumn("deleted_at",
        when(col("product_id") == restore_id, lit(None).cast("string"))
            .otherwise(col("deleted_at")))
```

**Hard purge — physically remove soft-deleted rows**
```python
purged = df.filter(col("is_deleted") == False)
# Write purged to replace the table
```

---

## 5. Deduplication — Keep Latest

### Why deduplication?
Data pipelines receive the same record multiple times due to:
- Retries after failure
- Duplicate feeds from upstream
- Multiple CDC events for the same key

### Strategy 1 — row_number (recommended for time-based latest)

```python
w = Window.partitionBy("emp_id").orderBy(col("loaded_at").desc())

deduped = df \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn")
```

Keeps the row with the **latest `loaded_at` per `emp_id`**.

### Strategy 2 — dropDuplicates (exact match)

```python
df.dropDuplicates(["emp_id", "salary", "loaded_at"])
```

Removes rows that are **identical** on the listed columns. Does NOT handle "same key, different value at different time" — use row_number for that.

### Strategy 3 — groupBy max + re-join

```python
max_ts = df.groupBy("emp_id").agg(spark_max("loaded_at").alias("loaded_at"))
latest = df.join(max_ts, on=["emp_id", "loaded_at"], how="inner")
```

Equivalent to Strategy 1 but uses groupBy instead of a window.

### When to use which

| Strategy | Use when |
|---|---|
| `row_number` | You need the latest row by timestamp, full row required |
| `dropDuplicates` | Exact byte-for-byte duplicates (retries, exact resends) |
| `groupBy + max join` | Same as row_number — useful if already grouping for other aggs |

---

## 6. Pattern Comparison

| Pattern | When to use | History kept? | Rows in output |
|---|---|---|---|
| **UPSERT** | Daily/hourly batch sync | No | Same count ± new inserts |
| **UPDATE ONLY** | Patch specific fields | No | Same count |
| **CDC** | Stream/log-based replication | Depends | All non-deleted rows |
| **SCD Type 2** | Dimension tables needing full history | Yes | More than input (old+new versions) |
| **Soft Delete** | Compliance, audit, recoverability | Yes | Same count (flag set) |
| **Dedup** | Raw landing zone cleanup | No | Fewer (duplicates removed) |

---

## 7. Quick Reference

### Join types used in DE patterns

| Pattern | Join type used | Why |
|---|---|---|
| UPSERT — unchanged rows | `left_anti` | Rows NOT matched by incoming |
| UPDATE ONLY | `left` join | Keep all current rows, NULLs where no incoming match |
| SCD2 — close old versions | `inner` join | Only rows being changed |
| SCD2 — keep unchanged | `left_anti` | Rows NOT being changed |
| CDC dedup | Window `row_number` | Latest per key without a join |

### Key functions

```python
from pyspark.sql.functions import (
    lit, when, coalesce, current_date, current_timestamp,
    to_date, row_number
)
from pyspark.sql.window import Window

# Conditional value assignment
when(condition, value).otherwise(other_value)

# First non-null — used for selective field updates
coalesce(col("incoming.salary"), col("current.salary"))

# Latest per key
Window.partitionBy("key").orderBy(col("ts").desc())
row_number().over(w)

# Convert string to date
to_date(lit("2024-06-01"))

# Placeholder "forever" date in SCD2
to_date(lit("9999-12-31"))
```

### SCD2 cheatsheet

```python
# Standard SCD2 columns to add
.withColumn("effective_from", to_date(lit(CHANGE_DATE)))
.withColumn("effective_to",   to_date(lit("9999-12-31")))
.withColumn("is_current",     lit(True))

# Close a row
.withColumn("effective_to", to_date(lit(PREV_DAY)))
.withColumn("is_current",   lit(False))

# Query current state
scd2.filter(col("is_current") == True)

# Point-in-time query
scd2.filter(
    (col("effective_from") <= target_date) &
    (col("effective_to")   >= target_date)
)
```

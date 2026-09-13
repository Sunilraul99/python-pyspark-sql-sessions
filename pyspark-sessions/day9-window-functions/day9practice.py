"""
Day 9 - Window Functions in PySpark
Topics:
  - What is a Window (partition + orderBy + frame)
  - Window frame types: ROWS vs RANGE
  - Frame boundaries: UNBOUNDED PRECEDING, CURRENT ROW, UNBOUNDED FOLLOWING, N PRECEDING/FOLLOWING

  Ranking functions:
    row_number, rank, dense_rank, ntile, percent_rank, cume_dist

  Analytic / navigation functions:
    lag, lead, first, last

  Aggregate window functions:
    sum, avg, min, max, count (with different frame boundaries)
    - Running total      (UNBOUNDED PRECEDING to CURRENT ROW)
    - Rolling average    (N PRECEDING to CURRENT ROW)
    - Full partition agg (UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING)

Data: 36 sales records across 5 salespersons, 4 regions, 4 categories,
      7 months (Jan-Jul 2024). Designed so every window output is distinct.
"""

import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

os.environ['JAVA_HOME']             = 'C:/Program Files/DBeaver/jre'
os.environ['PYSPARK_PYTHON']        = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

spark = SparkSession.builder \
    .appName("Day9 - Window Functions") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "day9-window-functions/data"

print("=" * 70)
print("Day 9 - Window Functions in PySpark")
print("=" * 70)

# ------------------------------------------------------------------
# Load dataset
# ------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

schema = StructType([
    StructField("sale_id",     StringType(),  False),
    StructField("salesperson", StringType(),  True),
    StructField("region",      StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("product",     StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("sale_date",   DateType(),    True),
    StructField("amount",      DoubleType(),  True),
])

df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .csv(f"{DATA}/sales.csv")

print("\n--- Full Dataset (36 rows) ---")
df.show(36, truncate=False)


# =============================================================
# SECTION 1 - WHAT IS A WINDOW FUNCTION?
# =============================================================
# groupBy().agg() compresses N rows into 1 row per group.
# Window function applies a calculation ACROSS rows related to
# the current row WITHOUT collapsing them into one row.
#
# Every row keeps its own value AND gets the window result as
# an extra column.
#
# A Window specification has 3 parts:
#   1. partitionBy("col")  - which group does this row belong to?
#                            (like GROUP BY in SQL)
#   2. orderBy("col")      - in what order are rows processed?
#                            (required for ranking and navigation funcs)
#   3. Frame (rowsBetween / rangeBetween)
#                          - which rows IN the partition to include?
#                            (required for aggregate window functions)
#
# Syntax:
#   from pyspark.sql.window import Window
#   from pyspark.sql.functions import rank, lag, sum, ...
#
#   w = Window.partitionBy("region").orderBy(col("amount").desc())
#   df.withColumn("rank", rank().over(w))

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank, ntile,
    percent_rank, cume_dist,
    lag, lead,
    first, last,
    sum as wsum, avg as wavg,
    min as wmin, max as wmax, count as wcount
)

print("\n" + "=" * 70)
print("SECTION 1 - Window Basics: rank vs groupBy")
print("=" * 70)

print("\n--- 1A: groupBy collapses rows (region -> 1 row per region) ---")
from pyspark.sql.functions import sum as spark_sum
df.groupBy("region") \
  .agg(spark_sum("amount").alias("total")) \
  .show()

print("\n--- 1B: Window function keeps all rows, adds total as extra column ---")
# Window without orderBy: entire partition is one group -> sum of partition
w_region = Window.partitionBy("region")
df.withColumn("region_total", wsum("amount").over(w_region)) \
  .select("sale_id", "salesperson", "region", "amount", "region_total") \
  .orderBy("region", "sale_id") \
  .show(20, truncate=False)
# Every row in "North" has the same region_total — no rows collapsed


# =============================================================
# SECTION 2 - WINDOW FRAME (rowsBetween / rangeBetween)
# =============================================================
# A frame defines WHICH rows in the partition to include in the
# calculation relative to the CURRENT ROW.
#
# Two frame types:
#   rowsBetween(start, end)   - counts by physical row position
#   rangeBetween(start, end)  - counts by value range of ORDER BY column
#
# Boundary constants (Window class):
#   Window.unboundedPreceding  = -infinity  (start of partition)
#   Window.currentRow          = 0          (current row)
#   Window.unboundedFollowing  = +infinity  (end of partition)
#
# Common frame patterns:
#   rowsBetween(unboundedPreceding, currentRow)     -> running total
#   rowsBetween(unboundedPreceding, unboundedFollowing) -> whole partition
#   rowsBetween(-2, 0)                              -> last 3 rows (2 before + current)
#   rowsBetween(-1, 1)                              -> 3-row sliding window
#   rowsBetween(0, unboundedFollowing)              -> current row to end

print("\n" + "=" * 70)
print("SECTION 2 - Window Frames: rowsBetween / rangeBetween")
print("=" * 70)

# Short aliases for cleaner code
UNBOUND_PRE  = Window.unboundedPreceding
UNBOUND_FOLL = Window.unboundedFollowing
CURR_ROW     = Window.currentRow

# Work on North region sales ordered by date for clear visualization
north = df.filter(col("region") == "North") \
          .select("sale_id", "salesperson", "sale_date", "amount") \
          .orderBy("sale_date")

print("\n--- North region sales (ordered by date) ---")
north.show(truncate=False)

# ------ 2A: UNBOUNDED PRECEDING to CURRENT ROW -> running total ------
print("\n--- 2A: rowsBetween(unboundedPreceding, currentRow) -> Running Total ---")
# From the start of the partition to the current row — cumulative sum
w_running = Window.partitionBy("region") \
                  .orderBy("sale_date") \
                  .rowsBetween(UNBOUND_PRE, CURR_ROW)

df.filter(col("region") == "North") \
  .withColumn("running_total", wsum("amount").over(w_running)) \
  .select("sale_id", "sale_date", "amount", "running_total") \
  .orderBy("sale_date") \
  .show(truncate=False)

# ------ 2B: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING -> whole partition ------
print("\n--- 2B: rowsBetween(unboundedPreceding, unboundedFollowing) -> Whole Partition Total ---")
# Includes ALL rows in the partition regardless of position
# Same value on every row in the partition
w_full = Window.partitionBy("region") \
               .rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)

df.filter(col("region") == "North") \
  .withColumn("partition_total", wsum("amount").over(w_full)) \
  .select("sale_id", "sale_date", "amount", "partition_total") \
  .orderBy("sale_date") \
  .show(truncate=False)

# ------ 2C: N PRECEDING to CURRENT ROW -> rolling window ------
print("\n--- 2C: rowsBetween(-2, currentRow) -> 3-row Rolling Average (current + 2 before) ---")
# -2 = 2 rows BEFORE current, 0 = currentRow -> window of last 3 rows
w_rolling = Window.partitionBy("region") \
                  .orderBy("sale_date") \
                  .rowsBetween(-2, CURR_ROW)

df.filter(col("region") == "North") \
  .withColumn("rolling_avg_3", spark_round(wavg("amount").over(w_rolling), 2)) \
  .select("sale_id", "sale_date", "amount", "rolling_avg_3") \
  .orderBy("sale_date") \
  .show(truncate=False)

# ------ 2D: -1 to 1 -> centred sliding window ------
print("\n--- 2D: rowsBetween(-1, 1) -> Centred 3-row Sliding Window ---")
# 1 row before + current row + 1 row after
w_centred = Window.partitionBy("region") \
                  .orderBy("sale_date") \
                  .rowsBetween(-1, 1)

df.filter(col("region") == "North") \
  .withColumn("centred_avg", spark_round(wavg("amount").over(w_centred), 2)) \
  .select("sale_id", "sale_date", "amount", "centred_avg") \
  .orderBy("sale_date") \
  .show(truncate=False)

# ------ 2E: CURRENT ROW to UNBOUNDED FOLLOWING -> suffix sum ------
print("\n--- 2E: rowsBetween(currentRow, unboundedFollowing) -> Suffix / Remaining Total ---")
# From current row to end of partition (how much revenue remains including this row)
w_suffix = Window.partitionBy("region") \
                 .orderBy("sale_date") \
                 .rowsBetween(CURR_ROW, UNBOUND_FOLL)

df.filter(col("region") == "North") \
  .withColumn("remaining_total", wsum("amount").over(w_suffix)) \
  .select("sale_id", "sale_date", "amount", "remaining_total") \
  .orderBy("sale_date") \
  .show(truncate=False)

# ------ 2F: rangeBetween vs rowsBetween ------
print("\n--- 2F: rangeBetween vs rowsBetween (important difference) ---")
# rangeBetween operates on the VALUE of the ORDER BY column, not row count.
# rangeBetween(-6, 0) on a date means: include rows where date is within
# 6 units (days) before current row's date.
# rowsBetween(-2, 0) means: include the 2 physically previous rows.
#
# For most use cases, rowsBetween is clearer and more predictable.
# rangeBetween is useful for time-based ranges or value-based windows.
print("    rowsBetween(-2, 0) -> exactly 2 previous rows + current")
print("    rangeBetween(-2, 0) -> rows where ORDER BY value is within 2 units")
print("    Use rowsBetween for most cases. rangeBetween for value-based logic.")


# =============================================================
# SECTION 3 - RANKING FUNCTIONS
# =============================================================
# Ranking functions require orderBy in the window spec.
# They do NOT require a frame (they work on the full ordered partition).
#
# row_number() -> 1, 2, 3 ... always unique, no ties
# rank()       -> 1, 1, 3 ... ties get same rank, next rank skips
# dense_rank() -> 1, 1, 2 ... ties get same rank, next rank does NOT skip
# ntile(n)     -> divides partition into n equal buckets (1..n)

print("\n" + "=" * 70)
print("SECTION 3 - Ranking Functions")
print("=" * 70)

w_rank = Window.partitionBy("region").orderBy(col("amount").desc())

print("\n--- 3A: row_number, rank, dense_rank side by side ---")
print("    Partition: region | Order: amount desc")
print("    Watch how ties (same amount) are handled differently\n")
df.withColumn("row_num",    row_number().over(w_rank)) \
  .withColumn("rank",       rank().over(w_rank)) \
  .withColumn("dense_rank", dense_rank().over(w_rank)) \
  .select("region", "salesperson", "sale_id", "amount",
          "row_num", "rank", "dense_rank") \
  .orderBy("region", "amount", "row_num") \
  .show(36, truncate=False)

print("    Tie explanation:")
print("    row_number -> always unique (1,2,3,4,5 ...)")
print("    rank       -> tied rows share rank, next rank SKIPS (1,2,2,4 ...)")
print("    dense_rank -> tied rows share rank, next rank does NOT skip (1,2,2,3 ...)")

# ------ 3B: Top-N per group using row_number ------
print("\n--- 3B: Top 2 sales per region (row_number + filter) ---")
# Classic pattern: rank within group, then filter where row_num <= N
df.withColumn("rn", row_number().over(w_rank)) \
  .filter(col("rn") <= 2) \
  .select("region", "salesperson", "sale_id", "amount", "rn") \
  .orderBy("region", "rn") \
  .show(truncate=False)

# ------ 3C: Top-N per group using rank (allows ties) ------
print("\n--- 3C: Top 2 per region using rank (ties both included) ---")
df.withColumn("rnk", rank().over(w_rank)) \
  .filter(col("rnk") <= 2) \
  .select("region", "salesperson", "sale_id", "amount", "rnk") \
  .orderBy("region", "rnk") \
  .show(truncate=False)

# ------ 3D: ntile — divide partition into equal buckets ------
print("\n--- 3D: ntile(3) — divide each region's sales into 3 equal tiers ---")
# ntile(3) -> bucket 1 = top third, 2 = middle, 3 = bottom third
w_ntile = Window.partitionBy("region").orderBy(col("amount").desc())
df.withColumn("tier", ntile(3).over(w_ntile)) \
  .select("region", "sale_id", "salesperson", "amount", "tier") \
  .orderBy("region", "tier", col("amount").desc()) \
  .show(36, truncate=False)

# ------ 3E: percent_rank ------
print("\n--- 3E: percent_rank — relative rank as percentage (0.0 to 1.0) ---")
# percent_rank = (rank - 1) / (total rows in partition - 1)
# First row = 0.0, last row = 1.0
w_pct = Window.partitionBy("region").orderBy("amount")
df.withColumn("pct_rank", spark_round(percent_rank().over(w_pct), 3)) \
  .select("region", "sale_id", "amount", "pct_rank") \
  .orderBy("region", "amount") \
  .show(20, truncate=False)

# ------ 3F: cume_dist ------
print("\n--- 3F: cume_dist — cumulative distribution (fraction of rows <= current) ---")
# cume_dist = number of rows with value <= current / total rows in partition
# Always > 0, last row = 1.0
df.withColumn("cume_dist", spark_round(cume_dist().over(w_pct), 3)) \
  .select("region", "sale_id", "amount", "cume_dist") \
  .orderBy("region", "amount") \
  .show(20, truncate=False)


# =============================================================
# SECTION 4 - LAG AND LEAD (Navigation Functions)
# =============================================================
# lag(col, n, default)  -> value from N rows BEFORE current row
# lead(col, n, default) -> value from N rows AFTER current row
#
# Syntax: lag("col", offset, default_if_null)
#
# Use cases:
#   - Compare current value with previous period (MoM, WoW)
#   - Calculate difference from last row
#   - Detect changes / state transitions
#
# Always requires orderBy in the window spec.

print("\n" + "=" * 70)
print("SECTION 4 - lag() and lead() — Navigation Functions")
print("=" * 70)

w_nav = Window.partitionBy("salesperson").orderBy("sale_date")

print("\n--- 4A: lag(1) — compare each sale to the previous sale ---")
df.withColumn("prev_amount", lag("amount", 1).over(w_nav)) \
  .withColumn("diff_from_prev",
              col("amount") - lag("amount", 1).over(w_nav)) \
  .select("salesperson", "sale_date", "amount", "prev_amount", "diff_from_prev") \
  .orderBy("salesperson", "sale_date") \
  .show(20, truncate=False)
# First sale per salesperson -> prev_amount = NULL (no previous row)

print("\n--- 4B: lag(1, 0) — default 0 instead of NULL for first row ---")
df.withColumn("prev_amount", lag("amount", 1, 0).over(w_nav)) \
  .select("salesperson", "sale_date", "amount", "prev_amount") \
  .orderBy("salesperson", "sale_date") \
  .show(10, truncate=False)

print("\n--- 4C: lag(2) — compare to 2 rows ago ---")
df.withColumn("amount_2_back", lag("amount", 2).over(w_nav)) \
  .select("salesperson", "sale_date", "amount", "amount_2_back") \
  .orderBy("salesperson", "sale_date") \
  .show(10, truncate=False)

print("\n--- 4D: lead(1) — peek at the NEXT row's value ---")
df.withColumn("next_amount", lead("amount", 1).over(w_nav)) \
  .withColumn("diff_to_next",
              lead("amount", 1).over(w_nav) - col("amount")) \
  .select("salesperson", "sale_date", "amount", "next_amount", "diff_to_next") \
  .orderBy("salesperson", "sale_date") \
  .show(10, truncate=False)
# Last sale per salesperson -> next_amount = NULL

print("\n--- 4E: lead(2) — value 2 rows ahead ---")
df.withColumn("amount_2_ahead", lead("amount", 2).over(w_nav)) \
  .select("salesperson", "sale_date", "amount", "amount_2_ahead") \
  .orderBy("salesperson", "sale_date") \
  .show(10, truncate=False)

print("\n--- 4F: Practical use — flag sales LOWER than previous sale ---")
df.withColumn("prev_amount", lag("amount", 1).over(w_nav)) \
  .withColumn("trend",
              when(lag("amount", 1).over(w_nav).isNull(), "FIRST")
              .when(col("amount") > lag("amount", 1).over(w_nav), "UP")
              .when(col("amount") < lag("amount", 1).over(w_nav), "DOWN")
              .otherwise("SAME")) \
  .select("salesperson", "sale_date", "amount", "trend") \
  .orderBy("salesperson", "sale_date") \
  .show(20, truncate=False)


# =============================================================
# SECTION 5 - FIRST AND LAST (Analytic Functions)
# =============================================================
# first(col, ignorenulls)  -> first value in the window frame
# last(col, ignorenulls)   -> last value in the window frame
#
# With frame = unboundedPreceding to unboundedFollowing:
#   first -> first in the partition
#   last  -> last in the partition
#
# With running frame (unboundedPreceding to currentRow):
#   first -> always the first row of partition (never changes)
#   last  -> grows — last seen so far (carry-forward pattern)

from pyspark.sql.functions import when

print("\n" + "=" * 70)
print("SECTION 5 - first() and last()")
print("=" * 70)

# first / last over full partition
w_full_ordered = Window.partitionBy("salesperson") \
                       .orderBy("sale_date") \
                       .rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)

print("\n--- 5A: first() and last() over full partition ---")
df.withColumn("first_sale_amt", first("amount").over(w_full_ordered)) \
  .withColumn("last_sale_amt",  last("amount").over(w_full_ordered)) \
  .select("salesperson", "sale_date", "amount", "first_sale_amt", "last_sale_amt") \
  .orderBy("salesperson", "sale_date") \
  .show(15, truncate=False)

# last() with running frame = carry-forward (fill from previous row)
w_running_ordered = Window.partitionBy("salesperson") \
                          .orderBy("sale_date") \
                          .rowsBetween(UNBOUND_PRE, CURR_ROW)

print("\n--- 5B: last() with running frame — carry-forward pattern ---")
print("    Each row shows the last (most recent) amount seen up to this row")
df.withColumn("last_so_far", last("amount").over(w_running_ordered)) \
  .select("salesperson", "sale_date", "amount", "last_so_far") \
  .orderBy("salesperson", "sale_date") \
  .show(10, truncate=False)


# =============================================================
# SECTION 6 - AGGREGATE WINDOW FUNCTIONS
# =============================================================
# Standard agg functions (sum, avg, min, max, count) work as
# window functions when combined with .over(windowSpec).
#
# The frame boundary determines WHICH rows are included:
#   - Running total     : unboundedPreceding -> currentRow
#   - Partition total   : unboundedPreceding -> unboundedFollowing
#   - Rolling N rows    : -N -> currentRow
#   - Centred window    : -N -> +N

print("\n" + "=" * 70)
print("SECTION 6 - Aggregate Window Functions (sum, avg, min, max, count)")
print("=" * 70)

w_agg_run   = Window.partitionBy("region").orderBy("sale_date") \
                    .rowsBetween(UNBOUND_PRE, CURR_ROW)
w_agg_full  = Window.partitionBy("region") \
                    .rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
w_agg_roll3 = Window.partitionBy("region").orderBy("sale_date") \
                    .rowsBetween(-2, CURR_ROW)

print("\n--- 6A: Running sum, running avg, running min, running max ---")
df.filter(col("region") == "North") \
  .withColumn("running_sum",   wsum("amount").over(w_agg_run)) \
  .withColumn("running_avg",   spark_round(wavg("amount").over(w_agg_run), 0)) \
  .withColumn("running_min",   wmin("amount").over(w_agg_run)) \
  .withColumn("running_max",   wmax("amount").over(w_agg_run)) \
  .select("sale_date", "amount",
          "running_sum", "running_avg", "running_min", "running_max") \
  .orderBy("sale_date") \
  .show(truncate=False)

print("\n--- 6B: Partition total (same value every row) vs running total ---")
df.filter(col("region") == "North") \
  .withColumn("running_sum",  wsum("amount").over(w_agg_run)) \
  .withColumn("partition_sum", wsum("amount").over(w_agg_full)) \
  .withColumn("pct_of_total",
              spark_round(
                  wsum("amount").over(w_agg_run) /
                  wsum("amount").over(w_agg_full) * 100, 1)) \
  .select("sale_date", "amount", "running_sum", "partition_sum", "pct_of_total") \
  .orderBy("sale_date") \
  .show(truncate=False)
# pct_of_total shows what % of region's total revenue each row has accumulated

print("\n--- 6C: 3-row rolling average ---")
df.filter(col("region") == "North") \
  .withColumn("rolling_avg_3", spark_round(wavg("amount").over(w_agg_roll3), 0)) \
  .select("sale_date", "amount", "rolling_avg_3") \
  .orderBy("sale_date") \
  .show(truncate=False)
# First row: avg of 1 row, second row: avg of 2 rows, third onwards: avg of 3

print("\n--- 6D: count() — running count and partition count ---")
df.filter(col("region") == "North") \
  .withColumn("running_count",  wcount("*").over(w_agg_run)) \
  .withColumn("partition_count", wcount("*").over(w_agg_full)) \
  .select("sale_date", "amount", "running_count", "partition_count") \
  .orderBy("sale_date") \
  .show(truncate=False)


# =============================================================
# SECTION 7 - WINDOW WITHOUT partitionBy
# =============================================================
# If you omit partitionBy(), the ENTIRE DataFrame is treated
# as one single partition. Use carefully — on large datasets
# this means all data goes to ONE executor (no parallelism).
#
# Useful for: global ranking, global running total across all rows.

print("\n" + "=" * 70)
print("SECTION 7 - Window Without partitionBy (Global Window)")
print("=" * 70)

w_global = Window.orderBy(col("amount").desc())
w_global_run = Window.orderBy("sale_date").rowsBetween(UNBOUND_PRE, CURR_ROW)

print("\n--- 7A: Global rank — rank all sales across all regions ---")
df.withColumn("global_rank", rank().over(w_global)) \
  .select("global_rank", "sale_id", "region", "salesperson", "amount") \
  .orderBy("global_rank") \
  .show(15, truncate=False)

print("\n--- 7B: Global running total across all rows ---")
df.withColumn("global_running_total", wsum("amount").over(w_global_run)) \
  .select("sale_date", "region", "salesperson", "amount", "global_running_total") \
  .orderBy("sale_date") \
  .show(15, truncate=False)


# =============================================================
# SECTION 8 - COMBINING MULTIPLE WINDOW SPECS
# =============================================================
# You can define and use multiple window specs in one withColumn chain.
# Each window spec is independent — different partitions or orders.

print("\n" + "=" * 70)
print("SECTION 8 - Multiple Window Specs in One Query")
print("=" * 70)

w_by_region      = Window.partitionBy("region").orderBy(col("amount").desc())
w_by_salesperson = Window.partitionBy("salesperson").orderBy("sale_date")
w_region_total   = Window.partitionBy("region").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
w_sp_running     = Window.partitionBy("salesperson").orderBy("sale_date") \
                         .rowsBetween(UNBOUND_PRE, CURR_ROW)

print("\n--- 8A: Rank within region + running total per salesperson in one query ---")
df.withColumn("rank_in_region",    rank().over(w_by_region)) \
  .withColumn("region_total",      wsum("amount").over(w_region_total)) \
  .withColumn("sp_running_total",  wsum("amount").over(w_sp_running)) \
  .withColumn("pct_of_region",
              spark_round(col("amount") / wsum("amount").over(w_region_total) * 100, 1)) \
  .select("region", "salesperson", "sale_date", "amount",
          "rank_in_region", "region_total", "sp_running_total", "pct_of_region") \
  .orderBy("region", "rank_in_region") \
  .show(20, truncate=False)


# =============================================================
# SECTION 9 - REAL-WORLD PATTERNS
# =============================================================

print("\n" + "=" * 70)
print("SECTION 9 - Real-World Patterns")
print("=" * 70)

# ------ 9A: Remove duplicates — keep latest row per salesperson ------
# row_number() ordered by date desc -> pick rn == 1 -> latest record
print("\n--- 9A: Deduplicate — keep most recent sale per salesperson ---")
w_dedup = Window.partitionBy("salesperson").orderBy(col("sale_date").desc())
df.withColumn("rn", row_number().over(w_dedup)) \
  .filter(col("rn") == 1) \
  .select("salesperson", "sale_date", "product", "amount") \
  .orderBy("salesperson") \
  .show(truncate=False)

# ------ 9B: Month-over-month change ------
print("\n--- 9B: Month-over-month revenue change per salesperson ---")
from pyspark.sql.functions import date_format, sum as spark_sum2

monthly = df.withColumn("month", date_format("sale_date", "yyyy-MM")) \
            .groupBy("salesperson", "month") \
            .agg(spark_sum2("amount").alias("monthly_revenue")) \
            .orderBy("salesperson", "month")

w_mom = Window.partitionBy("salesperson").orderBy("month")
monthly.withColumn("prev_month_rev", lag("monthly_revenue", 1).over(w_mom)) \
       .withColumn("mom_change",
                   col("monthly_revenue") - lag("monthly_revenue", 1).over(w_mom)) \
       .withColumn("mom_pct",
                   spark_round(
                       (col("monthly_revenue") - lag("monthly_revenue", 1).over(w_mom)) /
                       lag("monthly_revenue", 1).over(w_mom) * 100, 1)) \
       .show(20, truncate=False)

# ------ 9C: % contribution of each sale to region total ------
print("\n--- 9C: Each sale's % contribution to its region's total revenue ---")
w_rtotal = Window.partitionBy("region").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
df.withColumn("region_total", wsum("amount").over(w_rtotal)) \
  .withColumn("pct_contribution",
              spark_round(col("amount") / col("region_total") * 100, 2)) \
  .select("region", "sale_id", "salesperson", "amount", "region_total", "pct_contribution") \
  .orderBy("region", col("pct_contribution").desc()) \
  .show(20, truncate=False)

# ------ 9D: Flag if this sale is above/below personal average ------
print("\n--- 9D: Flag each sale as above or below salesperson's own average ---")
w_sp_avg = Window.partitionBy("salesperson").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
df.withColumn("sp_avg", spark_round(wavg("amount").over(w_sp_avg), 0)) \
  .withColumn("vs_avg",
              when(col("amount") > wavg("amount").over(w_sp_avg), "ABOVE AVG")
              .when(col("amount") < wavg("amount").over(w_sp_avg), "BELOW AVG")
              .otherwise("AT AVG")) \
  .select("salesperson", "sale_date", "amount", "sp_avg", "vs_avg") \
  .orderBy("salesperson", "sale_date") \
  .show(20, truncate=False)


# =============================================================
# WINDOW SPEC FRAME BOUNDARY REFERENCE
# =============================================================
# Window.unboundedPreceding  = start of partition (-inf)
# Window.currentRow          = current row (0)
# Window.unboundedFollowing  = end of partition (+inf)
# Integer N                  = N rows offset (+/-)
#
# Common frame patterns:
#
# rowsBetween(unboundedPreceding, currentRow)
#   -> running total / running avg (from start to current)
#
# rowsBetween(unboundedPreceding, unboundedFollowing)
#   -> full partition total (same value on every row)
#
# rowsBetween(-N, currentRow)
#   -> rolling window of last N+1 rows
#
# rowsBetween(-N, N)
#   -> centred sliding window (N before, current, N after)
#
# rowsBetween(currentRow, unboundedFollowing)
#   -> suffix sum (current to end)
#
# rangeBetween(unboundedPreceding, currentRow)
#   -> like running total but by VALUE not row count
#   -> all rows where orderBy col value <= current row's value

# =============================================================
# IMPORTS SUMMARY
# =============================================================
# from pyspark.sql.window import Window
#
# from pyspark.sql.functions import (
#     # Ranking
#     row_number, rank, dense_rank, ntile, percent_rank, cume_dist,
#     # Navigation
#     lag, lead, first, last,
#     # Aggregates (as window functions)
#     sum, avg, min, max, count,
#     # Helpers
#     col, when, round
# )
#
# Window spec building blocks:
#   Window.partitionBy("col")
#   Window.orderBy("col")
#   Window.rowsBetween(start, end)
#   Window.rangeBetween(start, end)
#   Window.unboundedPreceding
#   Window.currentRow
#   Window.unboundedFollowing

print("\n" + "=" * 70)
print("Day 9 Complete - Window Functions")
print("=" * 70)

spark.stop()
print("SparkSession stopped. Done.")

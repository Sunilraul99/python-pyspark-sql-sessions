# Day 9 - Window Functions

---

## Table of Contents
1. [What Is a Window Function?](#1-what-is-a-window-function)
2. [Window Spec — partitionBy, orderBy, Frame](#2-window-spec--partitionby-orderby-frame)
3. [Frame Boundaries](#3-frame-boundaries)
4. [Ranking Functions](#4-ranking-functions)
5. [lag() and lead()](#5-lag-and-lead)
6. [first() and last()](#6-first-and-last)
7. [Aggregate Window Functions](#7-aggregate-window-functions)
8. [Global Window (no partitionBy)](#8-global-window-no-partitionby)
9. [Real-World Patterns](#9-real-world-patterns)
10. [Quick Reference](#10-quick-reference)

---

## 1. What Is a Window Function?

### groupBy vs Window

| | `groupBy().agg()` | Window function |
|---|---|---|
| Input rows | N rows per group | N rows per group |
| Output rows | **1 row per group** | **N rows — same as input** |
| Use case | Summarise data | Add calculated column alongside existing rows |

A window function computes a result **across a set of rows related to the current row** without collapsing them into one output row. Every row keeps its own data and gets the calculated value as an extra column.

### Import
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, lag, sum, ...

w = Window.partitionBy("region").orderBy(col("amount").desc())
df.withColumn("rank", rank().over(w))
```

---

## 2. Window Spec — partitionBy, orderBy, Frame

A `WindowSpec` has up to 3 parts:

```python
w = Window \
    .partitionBy("region")          # 1. which group
    .orderBy(col("sale_date"))      # 2. row order within group
    .rowsBetween(-2, Window.currentRow)  # 3. which rows to include
```

| Part | Required for | Notes |
|---|---|---|
| `partitionBy` | Nothing (optional) | Without it, whole DataFrame = one partition |
| `orderBy` | Ranking functions, lag/lead | Required for ordered calculations |
| Frame | Aggregate window functions | Default frame depends on whether orderBy is present |

### Default frame behaviour
- **With orderBy, no frame specified:** `rangeBetween(unboundedPreceding, currentRow)` — running aggregate up to current value
- **Without orderBy, no frame specified:** `rowsBetween(unboundedPreceding, unboundedFollowing)` — whole partition

---

## 3. Frame Boundaries

### Constants
```python
Window.unboundedPreceding   # = start of partition (-∞)
Window.currentRow            # = current row (0)
Window.unboundedFollowing   # = end of partition (+∞)
# Integer N                 # = N rows before (negative) or after (positive)
```

### rowsBetween — by row count position
```python
Window.rowsBetween(start, end)
```

| Pattern | Frame | Result |
|---|---|---|
| `rowsBetween(UNBOUND_PRE, CURR_ROW)` | Start → current | Running total |
| `rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)` | Start → end | Whole partition |
| `rowsBetween(-2, CURR_ROW)` | 2 rows back → current | 3-row rolling window |
| `rowsBetween(-1, 1)` | 1 before → 1 after | Centred 3-row window |
| `rowsBetween(CURR_ROW, UNBOUND_FOLL)` | Current → end | Suffix / remaining total |
| `rowsBetween(-6, CURR_ROW)` | 6 rows back → current | 7-row rolling window |

### rangeBetween — by value of ORDER BY column
```python
Window.rangeBetween(start, end)
```
- Operates on the **value** of the `orderBy` column, not row position
- `rangeBetween(-7, 0)` on a date means: all rows where date is within 7 units of current
- For most use cases, `rowsBetween` is clearer and more predictable

### Quick alias pattern
```python
UNBOUND_PRE  = Window.unboundedPreceding
UNBOUND_FOLL = Window.unboundedFollowing
CURR_ROW     = Window.currentRow
```

---

## 4. Ranking Functions

All ranking functions require `orderBy` in the window spec. No frame needed.

### Import
```python
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, percent_rank, cume_dist
```

### row_number
```python
w = Window.partitionBy("region").orderBy(col("amount").desc())
df.withColumn("rn", row_number().over(w))
```
- Always assigns a **unique** sequential number: 1, 2, 3 ...
- Ties are broken arbitrarily (no guarantee which tied row gets which number)

### rank
```python
df.withColumn("rnk", rank().over(w))
```
- Tied rows get the **same rank**
- Next rank after a tie **skips** numbers: 1, 2, 2, **4**, 5 ...

### dense_rank
```python
df.withColumn("drnk", dense_rank().over(w))
```
- Tied rows get the **same rank**
- Next rank after a tie does **NOT skip**: 1, 2, 2, **3**, 4 ...

### Tie comparison

| Value | row_number | rank | dense_rank |
|---|---|---|---|
| 110000 | 1 | 1 | 1 |
| 84000 | 2 | 2 | 2 |
| 84000 | 3 | 2 | 2 |
| 62000 | 4 | 4 | 3 |

### ntile(n) — equal-sized buckets
```python
df.withColumn("tier", ntile(3).over(w))
```
Divides the partition into `n` equal groups. Returns bucket number 1..n.  
Top third = 1, middle = 2, bottom = 3 (when ordered desc).

### percent_rank — relative position as 0.0–1.0
```python
df.withColumn("pct", percent_rank().over(w))
```
Formula: `(rank - 1) / (total rows - 1)`. First row = 0.0, last row = 1.0.

### cume_dist — cumulative distribution
```python
df.withColumn("cd", cume_dist().over(w))
```
Fraction of rows with value ≤ current row's value. Always > 0, last row = 1.0.

### Top-N per group pattern
```python
# row_number: strictly top 2 (ties broken, only 2 rows per group)
df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 2)

# rank: top 2 ranks (ties both included — may return > 2 rows)
df.withColumn("rnk", rank().over(w)).filter(col("rnk") <= 2)
```

---

## 5. lag() and lead()

Navigate to a **different row** within the same partition.

```python
from pyspark.sql.functions import lag, lead
```

### lag — look BACK
```python
lag("col", offset=1, default=None)
```
Returns the value from `offset` rows **before** the current row.  
First `offset` rows in the partition → `None` (or `default` if provided).

```python
w = Window.partitionBy("salesperson").orderBy("sale_date")
df.withColumn("prev_amount", lag("amount", 1).over(w))
df.withColumn("prev_amount", lag("amount", 1, 0).over(w))   # default 0 instead of NULL
df.withColumn("two_back",    lag("amount", 2).over(w))
```

### lead — look FORWARD
```python
lead("col", offset=1, default=None)
```
Returns the value from `offset` rows **after** the current row.  
Last `offset` rows in the partition → `None`.

```python
df.withColumn("next_amount", lead("amount", 1).over(w))
df.withColumn("two_ahead",   lead("amount", 2).over(w))
```

### Common use cases
```python
# Period-over-period change
df.withColumn("prev", lag("amount", 1).over(w)) \
  .withColumn("change", col("amount") - col("prev"))

# Trend direction
df.withColumn("trend",
    when(lag("amount", 1).over(w).isNull(), "FIRST")
    .when(col("amount") > lag("amount", 1).over(w), "UP")
    .when(col("amount") < lag("amount", 1).over(w), "DOWN")
    .otherwise("SAME"))
```

---

## 6. first() and last()

```python
from pyspark.sql.functions import first, last
```

### Over full partition
```python
w_full = Window.partitionBy("salesperson") \
               .orderBy("sale_date") \
               .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn("first_sale", first("amount").over(w_full))
df.withColumn("last_sale",  last("amount").over(w_full))
```
- `first` → first row's value in the partition (same for all rows)
- `last` → last row's value in the partition (same for all rows)

### last() with running frame — carry-forward
```python
w_run = Window.partitionBy("salesperson").orderBy("sale_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("last_so_far", last("amount").over(w_run))
```
Each row shows the **most recent** value seen up to that row — useful for filling forward from a previous non-null value.

---

## 7. Aggregate Window Functions

`sum`, `avg`, `min`, `max`, `count` work as window functions with `.over(w)`.

```python
from pyspark.sql.functions import sum, avg, min, max, count
```

### Running total
```python
w = Window.partitionBy("region").orderBy("sale_date") \
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("amount").over(w))
```

### Partition total (same on every row)
```python
w = Window.partitionBy("region") \
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df.withColumn("region_total", sum("amount").over(w))
```

### Rolling N-row window
```python
w = Window.partitionBy("region").orderBy("sale_date").rowsBetween(-2, Window.currentRow)
df.withColumn("rolling_avg_3", avg("amount").over(w))   # last 3 rows
```

### % of total pattern
```python
w_run  = Window.partitionBy("region").orderBy("sale_date").rowsBetween(UNBOUND_PRE, CURR_ROW)
w_full = Window.partitionBy("region").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)

df.withColumn("pct_so_far",
    round(sum("amount").over(w_run) / sum("amount").over(w_full) * 100, 1))
```

### Suffix / remaining total
```python
w = Window.partitionBy("region").orderBy("sale_date") \
          .rowsBetween(Window.currentRow, Window.unboundedFollowing)
df.withColumn("remaining", sum("amount").over(w))
```

---

## 8. Global Window (no partitionBy)

When `partitionBy` is omitted, the **entire DataFrame** is treated as one partition.

```python
w_global = Window.orderBy(col("amount").desc())
df.withColumn("global_rank", rank().over(w_global))
```

**Warning:** All data goes to a single executor — no parallelism. Avoid on large datasets. Use only when you genuinely need a global ranking or global running total.

---

## 9. Real-World Patterns

### Deduplicate — keep latest row per group
```python
w = Window.partitionBy("salesperson").orderBy(col("sale_date").desc())
df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)
```

### Month-over-month change
```python
monthly = df.groupBy("salesperson", "month").agg(sum("amount").alias("rev"))
w = Window.partitionBy("salesperson").orderBy("month")
monthly.withColumn("prev_rev", lag("rev", 1).over(w)) \
       .withColumn("mom_change", col("rev") - col("prev_rev"))
```

### % contribution of each row to group total
```python
w_total = Window.partitionBy("region").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
df.withColumn("pct", round(col("amount") / sum("amount").over(w_total) * 100, 2))
```

### Flag above/below personal average
```python
w_avg = Window.partitionBy("salesperson").rowsBetween(UNBOUND_PRE, UNBOUND_FOLL)
df.withColumn("sp_avg", avg("amount").over(w_avg)) \
  .withColumn("vs_avg",
      when(col("amount") > col("sp_avg"), "ABOVE AVG").otherwise("BELOW AVG"))
```

---

## 10. Quick Reference

### Window function categories

| Category | Functions | Needs orderBy | Needs frame |
|---|---|---|---|
| Ranking | `row_number, rank, dense_rank, ntile, percent_rank, cume_dist` | Yes | No |
| Navigation | `lag, lead, first, last` | Yes (lag/lead) | Optional |
| Aggregate | `sum, avg, min, max, count` | Optional | Yes (for running/rolling) |

### Frame boundary constants

| Constant | Meaning |
|---|---|
| `Window.unboundedPreceding` | Start of partition |
| `Window.currentRow` | Current row |
| `Window.unboundedFollowing` | End of partition |
| `-N` (integer) | N rows before current |
| `+N` (integer) | N rows after current |

### Frame cheatsheet

| Goal | Frame |
|---|---|
| Running total | `rowsBetween(unboundedPreceding, currentRow)` |
| Whole partition | `rowsBetween(unboundedPreceding, unboundedFollowing)` |
| Last 3 rows | `rowsBetween(-2, currentRow)` |
| Last 7 rows | `rowsBetween(-6, currentRow)` |
| Centred 3-row | `rowsBetween(-1, 1)` |
| Current to end | `rowsBetween(currentRow, unboundedFollowing)` |

### Ranking comparison

| Value | row_number | rank | dense_rank |
|---|---|---|---|
| 84000 | 1 | 1 | 1 |
| 84000 | 2 | 1 | 1 |
| 62000 | 3 | 3 | 2 |
| 58000 | 4 | 4 | 3 |

### Top-N per group

```python
# Exactly N (no ties)
df.withColumn("rn", row_number().over(w)).filter(col("rn") <= N)

# Top N ranks (ties may give more than N rows)
df.withColumn("rnk", rank().over(w)).filter(col("rnk") <= N)

# Dedup — keep latest
df.withColumn("rn", row_number().over(
    Window.partitionBy("key").orderBy(col("date").desc())
)).filter(col("rn") == 1)
```

# Day 6 - GroupBy, Aggregations, Sorting, and Date Functions

---

## Table of Contents
1. [groupBy + Aggregations](#1-groupby--aggregations)
2. [Filter After groupBy — HAVING](#2-filter-after-groupby--having)
3. [orderBy / sort](#3-orderby--sort)
4. [Date Functions](#4-date-functions)
5. [Quick Reference](#5-quick-reference)

---

## 1. groupBy + Aggregations

### Syntax
```python
df.groupBy("col1", "col2") \
  .agg(func("col").alias("name"))
```

Group rows that share the same value, then apply an aggregation function to each group.

### Import
```python
from pyspark.sql.functions import (
    count, sum, avg, min, max,
    countDistinct, collect_list, collect_set
)
```

### count
```python
df.groupBy("category").agg(count("*").alias("total_sales"))
```
- `count("*")` — counts ALL rows including nulls
- `count("col")` — counts only non-null values in that column

### sum
```python
df.groupBy("region").agg(sum("total_amount").alias("total_revenue"))
```

### avg
```python
df.groupBy("salesperson").agg(round(avg("total_amount"), 2).alias("avg_sale"))
```

### min / max
```python
df.groupBy("category").agg(
    min("unit_price").alias("min_price"),
    max("unit_price").alias("max_price")
)
```

### Multiple aggregations in one call
```python
df.groupBy("category").agg(
    count("*").alias("num_sales"),
    sum("total_amount").alias("total_revenue"),
    round(avg("total_amount"), 2).alias("avg_revenue"),
    min("unit_price").alias("min_price"),
    max("unit_price").alias("max_price")
)
```
All computed in a single pass — more efficient than separate calls.

### countDistinct
```python
df.groupBy("region").agg(countDistinct("product").alias("unique_products"))
```
Counts distinct non-null values only.

### groupBy on multiple columns
```python
df.groupBy("region", "category").agg(count("*").alias("num_sales"))
```
Produces finer-grained buckets — one row per unique (region, category) pair.

### collect_list vs collect_set
```python
df.groupBy("salesperson").agg(collect_list("product").alias("all_products"))
df.groupBy("salesperson").agg(collect_set("category").alias("unique_categories"))
```
- `collect_list` — all values including duplicates, preserves order
- `collect_set` — unique values only, order not guaranteed

---

## 2. Filter After groupBy — HAVING

In SQL: `HAVING` filters on aggregated values.  
In PySpark: chain `.filter()` **after** `.agg()`.

```python
# Regions with total revenue > 100,000
df.groupBy("region") \
  .agg(sum("total_amount").alias("total_revenue")) \
  .filter(col("total_revenue") > 100000)
```

```python
# Salespersons with more than 5 sales
df.groupBy("salesperson") \
  .agg(count("*").alias("num_sales")) \
  .filter(col("num_sales") > 5)
```

**Rule:** You cannot filter on the aggregated alias inside `agg()`. Always chain `.filter()` after.

---

## 3. orderBy / sort

`orderBy()` and `sort()` are identical — use either.

### Import
```python
from pyspark.sql.functions import asc, desc
```

### Ascending (default)
```python
df.orderBy("total_amount")
df.orderBy(col("total_amount").asc())
```

### Descending
```python
df.orderBy(col("total_amount").desc())
```

### Multiple columns
```python
df.orderBy(col("region").asc(), col("total_amount").desc())
```
Primary sort: region A→Z. Within each region, secondary: total_amount high→low.

### Sort after aggregation
```python
df.groupBy("salesperson") \
  .agg(sum("total_amount").alias("total_revenue")) \
  .orderBy(col("total_revenue").desc())
```

### NULL handling in sort

| Method | Behaviour |
|---|---|
| `.asc()` (default) | NULLs sort **last** |
| `.desc()` (default) | NULLs sort **first** |
| `.asc_nulls_first()` | NULLs at top |
| `.asc_nulls_last()` | NULLs at bottom |
| `.desc_nulls_first()` | NULLs at top |
| `.desc_nulls_last()` | NULLs at bottom |

```python
df.orderBy(col("discount").desc_nulls_last())
```

---

## 4. Date Functions

### Import
```python
from pyspark.sql.functions import (
    current_date, datediff, date_add, date_sub,
    months_between, last_day, next_day,
    date_trunc, date_format,
    dayofweek, dayofyear, quarter, weekofyear,
    to_date, trunc,
    year, month, dayofmonth   # covered in Day 5
)
```

### current_date — today's date
```python
df.withColumn("today", current_date())
```

### datediff — days between two dates
```python
datediff(col("delivery_date"), col("sale_date"))   # positive when end > start
datediff(current_date(), col("sale_date"))          # days since sale
```

### date_add / date_sub — shift a date
```python
date_add(col("sale_date"), 7)    # add 7 days
date_sub(col("sale_date"), 3)    # subtract 3 days
```

### months_between — fractional months between dates
```python
round(months_between(current_date(), col("sale_date")), 1)
```

### last_day — last day of the month
```python
last_day(col("sale_date"))    # e.g. 2024-01-05 → 2024-01-31
```

### next_day — next occurrence of a weekday
```python
next_day(col("sale_date"), "Mon")   # next Monday after sale_date
```
Weekday names: `"Mon"`, `"Tue"`, `"Wed"`, `"Thu"`, `"Fri"`, `"Sat"`, `"Sun"`

### date_trunc — truncate to start of time unit
```python
date_trunc("month", col("sale_date"))   # → first day of month
date_trunc("year",  col("sale_date"))   # → first day of year
date_trunc("week",  col("sale_date"))   # → Monday of that week
```
Use this to group by month or year without string formatting.

### date_format — format date as string
```python
date_format(col("sale_date"), "dd-MM-yyyy")    # 05-01-2024
date_format(col("sale_date"), "MMM yyyy")      # Jan 2024
date_format(col("sale_date"), "EEEE")          # Monday
date_format(col("sale_date"), "MM/dd/yyyy")    # 01/05/2024
```

Common format tokens:

| Token | Meaning | Example |
|---|---|---|
| `yyyy` | 4-digit year | 2024 |
| `MM` | 2-digit month | 01 |
| `MMM` | Short month name | Jan |
| `MMMM` | Full month name | January |
| `dd` | 2-digit day | 05 |
| `EEE` | Short weekday | Mon |
| `EEEE` | Full weekday | Monday |

### dayofweek — day number (1=Sun … 7=Sat)
```python
dayofweek(col("sale_date"))   # 1=Sunday, 7=Saturday (Java convention)
```

### dayofyear — day number in year (1–365)
```python
dayofyear(col("sale_date"))
```

### quarter — quarter number (1–4)
```python
quarter(col("sale_date"))   # Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
```

### weekofyear — ISO week number (1–53)
```python
weekofyear(col("sale_date"))
```

### to_date — parse string → DateType
```python
to_date(col("date_str"), "dd-MM-yyyy")
to_date(col("date_str"), "dd/MM/yyyy")
```
Use when data arrives as a string column. Provide the pattern matching your string.

### trunc — truncate to month or year (older API)
```python
trunc(col("sale_date"), "month")   # → first day of month
trunc(col("sale_date"), "year")    # → first day of year
```
Similar to `date_trunc` but older — only supports `year`, `month`, `week`.

### Combine date functions with groupBy (monthly report pattern)
```python
df.groupBy(
    year(col("sale_date")).alias("year"),
    month(col("sale_date")).alias("month"),
    date_format(col("sale_date"), "MMM yyyy").alias("label")
) \
.agg(count("*").alias("sales"), sum("total_amount").alias("revenue")) \
.orderBy("year", "month")
```

---

## 5. Quick Reference

### Aggregation functions

| Function | Use |
|---|---|
| `count("*")` | Count all rows |
| `count("col")` | Count non-null values |
| `countDistinct("col")` | Count unique non-null values |
| `sum("col")` | Total |
| `avg("col")` | Mean |
| `min("col")` | Minimum |
| `max("col")` | Maximum |
| `collect_list("col")` | List of all values (with duplicates) |
| `collect_set("col")` | Set of unique values |

### orderBy cheatsheet

| Goal | Code |
|---|---|
| Ascending | `orderBy("col")` or `orderBy(col("x").asc())` |
| Descending | `orderBy(col("x").desc())` |
| Multi-column | `orderBy(col("a").asc(), col("b").desc())` |
| NULLs last | `orderBy(col("x").desc_nulls_last())` |

### Date function cheatsheet

| Goal | Function |
|---|---|
| Today | `current_date()` |
| Days between | `datediff(end, start)` |
| Add days | `date_add(col, n)` |
| Subtract days | `date_sub(col, n)` |
| Months between | `months_between(end, start)` |
| End of month | `last_day(col)` |
| Next weekday | `next_day(col, "Mon")` |
| Month start | `date_trunc("month", col)` |
| Format display | `date_format(col, "dd-MM-yyyy")` |
| Weekday name | `date_format(col, "EEEE")` |
| Day of week num | `dayofweek(col)` |
| Day of year | `dayofyear(col)` |
| Quarter | `quarter(col)` |
| Week number | `weekofyear(col)` |
| Parse string | `to_date(col, "dd/MM/yyyy")` |

# Day 8 - Advanced Joins

---

## Table of Contents
1. [Expression Join — Different Column Names](#1-expression-join--different-column-names)
2. [DataFrame Aliases](#2-dataframe-aliases)
3. [Duplicate Column Problem + Fixes](#3-duplicate-column-problem--fixes)
4. [Multiple Join Conditions](#4-multiple-join-conditions)
5. [Join on Transformed Columns](#5-join-on-transformed-columns)
6. [Self Join](#6-self-join)
7. [Three-Table and Four-Table Joins](#7-three-table-and-four-table-joins)
8. [Broadcast Join](#8-broadcast-join)
9. [Real-World Patterns](#9-real-world-patterns)
10. [Quick Reference](#10-quick-reference)

---

## 1. Expression Join — Different Column Names

### Day 7 recap — same column name
```python
emp.join(dept, on="dept_id", how="inner")   # works when both have "dept_id"
```

### Day 8 — different column names
When table A has `dept_code` and table B has `department_id`, `on="dept_code"` fails because `departments` has no column called `dept_code`.

**Use an expression instead:**

```python
# Style 1 — DataFrame["column"]
emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner")

# Style 2 — col("alias.column")  (preferred when DataFrames are aliased)
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner")
```

Both styles are equivalent. Style 2 requires the DataFrames to have an alias set (see Section 2).

---

## 2. DataFrame Aliases

Assign a short name to a DataFrame with `.alias("name")`. This lets you qualify column references as `col("alias.column")` to avoid ambiguity in multi-table joins.

```python
emp  = spark.read.csv(...).alias("emp")
dept = spark.read.csv(...).alias("dept")
loc  = spark.read.csv(...).alias("loc")
```

Then reference columns as:
```python
col("emp.emp_name")
col("dept.department_name")
col("loc.city")
```

**Always alias DataFrames before joining** — it is a production habit that prevents column name conflicts.

---

## 3. Duplicate Column Problem + Fixes

### The problem
Expression joins keep **both** key columns in the result — even though they hold the same value.

```python
raw = emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner")
raw.printSchema()
# Both "dept_code" and "department_id" appear -> ambiguity
```

### Fix A — drop() the redundant column
```python
emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner") \
   .drop(col("dept.department_id"))
```

### Fix B — select() with alias prefix (most production-friendly)
```python
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner") \
   .select(
       col("emp.emp_id").alias("emp_id"),
       col("emp.emp_name").alias("emp_name"),
       col("emp.dept_code").alias("dept_code"),
       col("dept.department_name").alias("dept_name"),
       col("emp.salary").alias("salary")
   )
```

### Fix C — rename before join, then use on=
```python
dept_renamed = dept.withColumnRenamed("department_id", "dept_code")
emp.join(dept_renamed, on="dept_code", how="inner")
# Now only ONE dept_code column in result
```

---

## 4. Multiple Join Conditions

Combine conditions in the join expression using `&` (AND) or `|` (OR).

### AND — both must be true
```python
emp.join(
    proj,
    (col("emp.dept_code") == col("proj.dept_code")) &
    (col("proj.status") == "active"),
    how="inner"
)
```
Row matches only if the department matches AND the project is active.

### Multiple AND conditions
```python
emp.join(
    proj,
    (col("emp.emp_id") == col("proj.lead_emp_id")) &
    (col("emp.dept_code") == col("proj.dept_code")),
    how="inner"
)
```

**Rule:** Wrap each condition in parentheses. Use `&` not Python's `and`.

---

## 5. Join on Transformed Columns

Apply functions **inside** the join expression to normalize values before comparing.  
Common use: real data has inconsistent casing or extra spaces.

```python
from pyspark.sql.functions import lower, trim

df1.join(
    df2,
    lower(trim(col("df1.key"))) == lower(col("df2.key")),
    how="left"
)
```

This transforms the key on both sides before matching — the stored data is not changed.

```python
# dirty_emp.dept_key = " DC01 " or "dc02"  -> trim + lower -> "dc01", "dc02"
# clean_dept.dept_id = "DC01"              -> lower -> "dc01"
# They now match
```

---

## 6. Self Join

A self join joins a table **to itself**. Used when a table has a column that references another row in the same table (e.g. `manager_id` → `emp_id`).

**Requirement:** You MUST alias the two copies differently. Otherwise Spark cannot tell them apart.

```python
emp_table = spark.read.csv(...)   # loaded once

emp_side = emp_table.alias("e")
mgr_side = emp_table.alias("mgr")

emp_side.join(
    mgr_side,
    col("e.manager_id") == col("mgr.emp_id"),
    how="left"                       # left join so managers with no manager still appear
) \
.select(
    col("e.emp_name").alias("employee"),
    col("mgr.emp_name").alias("manager")
)
```

`how="left"` keeps employees with `manager_id = NULL` (they are top-level managers).

---

## 7. Three-Table and Four-Table Joins

Chain `.join()` calls — each adds one more table.

```python
# Three tables
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(loc,  col("emp.location_id") == col("loc.loc_id"),        how="left") \
   .select(col("emp.emp_name"), col("dept.department_name"), col("loc.city"))
```

```python
# Four tables
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(loc,  col("emp.location_id") == col("loc.loc_id"),        how="left") \
   .join(proj, col("emp.emp_id") == col("proj.lead_emp_id"),        how="left") \
   .select(col("emp.emp_name"), col("dept.department_name"),
           col("loc.city"), col("proj.project_name"))
```

**Best practices for multi-table joins:**
- Always alias all DataFrames before joining
- Use `col("alias.column")` to reference columns unambiguously
- Use `how="left"` for lookup/dimension tables so all fact rows appear
- `.select()` at the end to pick only the columns you need

---

## 8. Broadcast Join

### Why use it
By default, Spark **shuffles** both DataFrames to co-locate matching rows across the cluster. For small lookup tables (< ~10MB), this shuffle is wasteful.

`broadcast()` instead sends the small DataFrame to **every executor**, so the large DataFrame never moves.

### Syntax
```python
from pyspark.sql.functions import broadcast

emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="inner")
```

### When to use
- The smaller table fits in memory on each worker (rough rule: < 10MB)
- Dimension/lookup tables: departments, locations, status codes, country lists
- Never broadcast the large fact table

### In multi-table joins
```python
emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(broadcast(loc),  col("emp.location_id") == col("loc.loc_id"),         how="left")
```

The query result is identical — only the execution plan changes (faster, less network traffic).

---

## 9. Real-World Patterns

### Star schema (fact + dimensions)
```python
# employees = fact table, departments + locations = dimension tables
emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(broadcast(loc),  col("emp.location_id") == col("loc.loc_id"),         how="left") \
   .select(col("emp.emp_id"), col("emp.emp_name"),
           col("dept.department_name"), col("loc.city"), col("emp.salary"))
```

### coalesce — fill NULLs from outer join
```python
from pyspark.sql.functions import coalesce, lit

emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="left") \
   .select(
       col("emp.emp_name"),
       coalesce(col("dept.department_name"), lit("Unknown")).alias("department"),
       coalesce(col("dept.annual_budget"),   lit(0)).alias("budget")
   )
```

### Join then groupBy
```python
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner") \
   .groupBy(col("dept.department_name")) \
   .agg(count("emp.emp_id").alias("headcount"),
        round(avg("emp.salary"), 0).alias("avg_salary"))
```

---

## 10. Quick Reference

### When to use which join syntax

| Scenario | Syntax |
|---|---|
| Same column name on both sides | `on="col_name"` |
| Different column names | `df1["col_a"] == df2["col_b"]` |
| With DataFrame aliases | `col("emp.dept_code") == col("dept.department_id")` |
| Multiple conditions | `(cond1) & (cond2)` inside join |
| Transformed key | `lower(trim(col("a.key"))) == lower(col("b.key"))` |
| Self join | Two aliases of the same DataFrame |
| Small lookup table | Wrap with `broadcast(small_df)` |

### Multi-table join template
```python
emp.alias("emp") \
   .join(broadcast(dept.alias("dept")),
         col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(broadcast(loc.alias("loc")),
         col("emp.location_id") == col("loc.loc_id"), how="left") \
   .join(proj.alias("proj"),
         col("emp.emp_id") == col("proj.lead_emp_id"), how="left") \
   .select(
       col("emp.emp_name"),
       col("dept.department_name"),
       col("loc.city"),
       col("proj.project_name")
   )
```

### Fixing duplicate columns after expression join

| Fix | Code |
|---|---|
| Drop one | `.drop(col("dept.department_id"))` |
| Select explicitly | `.select(col("emp.dept_code"), col("dept.department_name"), ...)` |
| Rename before join | `dept.withColumnRenamed("department_id", "dept_code")` then `on="dept_code"` |

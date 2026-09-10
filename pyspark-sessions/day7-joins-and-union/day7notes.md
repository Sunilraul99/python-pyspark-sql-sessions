# Day 7 - Joins and Union

---

## Table of Contents
1. [Join Basics](#1-join-basics)
2. [Inner Join](#2-inner-join)
3. [Left Join](#3-left-join-left-outer)
4. [Right Join](#4-right-join-right-outer)
5. [Full Outer Join](#5-full-outer-join)
6. [Left Semi Join](#6-left-semi-join)
7. [Left Anti Join](#7-left-anti-join)
8. [Cross Join](#8-cross-join)
9. [Duplicate Column Names](#9-duplicate-column-names-after-join)
10. [union / unionByName](#10-union--unionbyname)
11. [Quick Reference](#11-quick-reference)

---

## 1. Join Basics

### Syntax
```python
df1.join(df2, on="key_col",           how="inner")   # same column name
df1.join(df2, df1.col == df2.col,     how="inner")   # expression (different names)
df1.join(df2, col("a.x") == col("b.x"), how="inner") # alias-qualified
```

### Join types (how= values)

| `how=` | Returns |
|---|---|
| `"inner"` | Only rows matched on BOTH sides |
| `"left"` | ALL left rows + matched right (NULL if no match) |
| `"right"` | ALL right rows + matched left (NULL if no match) |
| `"full"` | ALL rows from BOTH sides |
| `"left_semi"` | Left rows that HAVE a match (no right columns) |
| `"left_anti"` | Left rows that have NO match (no right columns) |
| `crossJoin()` | Every left row × every right row (no key) |

---

## 2. Inner Join

Returns only rows where the join key exists in **both** tables. Unmatched rows on either side are dropped.

```python
emp.join(dept, on="dept_id", how="inner")
```

**Use when:** you only want records that have a match on both sides.

```
emp:  E01, E02, E03, E11(D99), E12(NULL)
dept: D01, D02, D03, D07, D08
inner result: E01, E02, E03 only
E11(D99 not in dept) and E12(NULL) are DROPPED
D07, D08 are DROPPED (no employees linked)
```

---

## 3. Left Join (Left Outer)

Returns **ALL rows from the left** table. Unmatched left rows get NULL for right-side columns. Right rows with no match are dropped.

```python
emp.join(dept, on="dept_id", how="left")
```

**Use when:** "I want all employees, and their dept info IF it exists."

### Find unmatched left rows (isNull trick)
```python
emp.join(dept, on="dept_id", how="left") \
   .filter(col("dept_name").isNull())
```
Rows with NULL right-side columns are the unmatched ones.

---

## 4. Right Join (Right Outer)

Returns **ALL rows from the right** table. Unmatched right rows get NULL for left-side columns. Left rows with no match are dropped.

```python
emp.join(dept, on="dept_id", how="right")
```

**Use when:** "I want all departments, with their employees IF any exist."

### Find departments with no employees
```python
emp.join(dept, on="dept_id", how="right") \
   .filter(col("emp_id").isNull())
```

---

## 5. Full Outer Join

Returns **ALL rows from both tables**. Matched rows have all columns filled. Unmatched rows on either side get NULL for the other side's columns.

```python
emp.join(dept, on="dept_id", how="full")
```

**Use when:** "I want everything — employees without depts AND depts without employees."

```
Result = inner rows + left-only rows + right-only rows
```

---

## 6. Left Semi Join

Returns only left rows that **have a match** in the right table.  
Right-side columns are **NOT** included in the result.

```python
emp.join(bonus, on="emp_id", how="left_semi")
```

**Use when:** "Give me employees who received a bonus" — but you don't need the bonus columns.

Think of it as: *filter LEFT using RIGHT as a lookup, keep only LEFT columns.*

---

## 7. Left Anti Join

Returns only left rows that have **NO match** in the right table.  
Exact opposite of left semi join. Right-side columns are **NOT** included.

```python
emp.join(bonus, on="emp_id", how="left_anti")
```

**Use when:** "Give me employees who did NOT receive a bonus."

---

## 8. Cross Join

Every row from the left is paired with **every row** from the right.  
No join key — all combinations are produced.

```python
df1.crossJoin(df2)
```

Result row count = `left_count × right_count`

**Use when:** generating all possible combinations (date × product matrix, scoring all pairs).  
**Warning:** Can produce very large results. Use carefully.

---

## 9. Duplicate Column Names After Join

### The problem
When joining on an **expression** (`df1.col == df2.col`), Spark keeps BOTH columns. This causes ambiguity.

```python
# Both dept_id columns appear — causes AnalysisException later
duped = emp.join(dept, emp["dept_id"] == dept["dept_id"], how="inner")
```

### Fix 1 — Join on string (merges into one column)
```python
emp.join(dept, on="dept_id", how="inner")   # single dept_id column
```
Use when column names are the same on both sides.

### Fix 2 — drop() the redundant column
```python
emp.join(dept, emp["dept_id"] == dept["dept_id"], how="inner") \
   .drop(col("dept.dept_id"))
```

### Fix 3 — select() with alias prefix
```python
emp.join(dept, emp["dept_id"] == dept["dept_id"], how="inner") \
   .select(
       emp["emp_id"],
       emp["emp_name"],
       emp["dept_id"],
       dept["dept_name"]
   )
```

### Fix 4 — rename before join
```python
dept_renamed = dept.withColumnRenamed("dept_id", "d_dept_id")
emp.join(dept_renamed, emp["dept_id"] == dept_renamed["d_dept_id"], how="inner")
```

---

## 10. union / unionByName

### union — stacks rows by POSITION
```python
df1.union(df2)
```
- Matches columns by **position** (not name)
- Both DataFrames must have the **same number of columns**
- Does NOT deduplicate — use `.distinct()` after if needed

### unionByName — stacks rows by NAME
```python
df1.unionByName(df2)
```
- Matches columns by **name** (order doesn't matter)
- Safer when column order may differ between sources

### allowMissingColumns — handle schema differences
```python
df1.unionByName(df2, allowMissingColumns=True)
```
- Columns missing from one DataFrame are filled with `NULL`
- Use when DataFrames have different sets of columns

### Deduplicate after union
```python
df1.union(df2).distinct()
```
`distinct()` removes exact duplicate rows (all columns must match).

---

## 11. Quick Reference

### Join type cheatsheet

| Type | `how=` | Left rows | Right rows | Right cols in result |
|---|---|---|---|---|
| Inner | `"inner"` | matched only | matched only | Yes |
| Left | `"left"` | ALL | matched only | Yes (NULL if no match) |
| Right | `"right"` | matched only | ALL | Yes (NULL if no match) |
| Full | `"full"` | ALL | ALL | Yes (NULL where no match) |
| Semi | `"left_semi"` | matched only | not kept | No |
| Anti | `"left_anti"` | unmatched only | not kept | No |
| Cross | `crossJoin()` | ALL × ALL | ALL × ALL | Yes |

### Find unmatched rows pattern
```python
# Left rows with no match in right
df1.join(df2, on="key", how="left").filter(col("right_col").isNull())

# Right rows with no match in left
df1.join(df2, on="key", how="right").filter(col("left_col").isNull())

# Cleaner version using anti join
df1.join(df2, on="key", how="left_anti")
```

### union cheatsheet

| Goal | Code |
|---|---|
| Stack by position | `df1.union(df2)` |
| Stack by name | `df1.unionByName(df2)` |
| Handle missing cols | `df1.unionByName(df2, allowMissingColumns=True)` |
| Deduplicate | `.union(df2).distinct()` |

"""
Day 7 - Joins and Union in PySpark
Topics:
  - Inner join
  - Left join  (left outer)
  - Right join (right outer)
  - Full outer join
  - Left semi join
  - Left anti join
  - Cross join
  - Duplicate column names after join + how to fix them
  - union() vs unionByName()

Datasets (all inside data/):
  employees.csv   - 12 employees, dept_id E01-E12 (some with unknown/null dept)
  departments.csv - 7 departments (D01-D08, D06 missing -> some employees unmatched)
  bonuses.csv     - 8 bonus records (some emp_ids not in employees -> anti/semi demo)
  employees_2023.csv - 5 employees from previous year (for union demo)

Join key: emp.dept_id = dept.dept_id
          emp.emp_id  = bonus.emp_id
"""

import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

os.environ['JAVA_HOME']             = 'C:/Program Files/DBeaver/jre'
os.environ['PYSPARK_PYTHON']        = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Day7 - Joins and Union") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "day7-joins-and-union/data"

print("=" * 65)
print("Day 7 - Joins and Union in PySpark")
print("=" * 65)


# ------------------------------------------------------------------
# Load all datasets
# ------------------------------------------------------------------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

emp_schema = StructType([
    StructField("emp_id",    StringType(),  False),
    StructField("emp_name",  StringType(),  True),
    StructField("dept_id",   StringType(),  True),
    StructField("city",      StringType(),  True),
    StructField("salary",    DoubleType(),  True),
    StructField("join_date", DateType(),    True),
])

dept_schema = StructType([
    StructField("dept_id",   StringType(),  False),
    StructField("dept_name", StringType(),  True),
    StructField("manager",   StringType(),  True),
    StructField("budget",    DoubleType(),  True),
])

bonus_schema = StructType([
    StructField("emp_id",       StringType(),  False),
    StructField("bonus_amount", DoubleType(),  True),
    StructField("bonus_year",   IntegerType(), True),
    StructField("reason",       StringType(),  True),
])

emp  = spark.read.schema(emp_schema).option("header", "true") \
           .option("dateFormat", "yyyy-MM-dd").csv(f"{DATA}/employees.csv")
dept = spark.read.schema(dept_schema).option("header", "true").csv(f"{DATA}/departments.csv")
bonus = spark.read.schema(bonus_schema).option("header", "true").csv(f"{DATA}/bonuses.csv")
emp_2023 = spark.read.schema(emp_schema).option("header", "true") \
               .option("dateFormat", "yyyy-MM-dd").csv(f"{DATA}/employees_2023.csv")

print("\n--- employees (12 rows) ---")
emp.show(truncate=False)

print("\n--- departments (7 rows, D06 missing) ---")
dept.show(truncate=False)

print("\n--- bonuses (8 rows, E13/E14 not in employees) ---")
bonus.show(truncate=False)

print("\n--- employees_2023 (5 rows, for union demo) ---")
emp_2023.show(truncate=False)


# =============================================================
# SECTION 1 - INNER JOIN
# =============================================================
# Returns only rows where the join key EXISTS in BOTH tables.
# Rows that don't match on either side are DROPPED.
#
# Syntax: df1.join(df2, on="key_col", how="inner")
#         df1.join(df2, df1.col == df2.col, how="inner")
#
# employees  has dept_id: D01-D06, D99, NULL
# departments has dept_id: D01-D05, D07, D08
#
# INNER JOIN keeps only D01, D02, D03, D04, D05 (mutual matches).
# E09 (D06), E11 (D99), E12 (NULL) are DROPPED — no match in dept.
# D07, D08 are DROPPED — no employees linked to them.

print("\n" + "=" * 65)
print("SECTION 1 - INNER JOIN")
print("=" * 65)

print("\n--- 1A: Inner join - employees matched with their department ---")
print("    Only employees WITH a matching dept_id in departments table")
print("    E09(D06), E11(D99), E12(NULL) dropped | D07,D08 dropped")

inner_df = emp.join(dept, on="dept_id", how="inner")
inner_df.show(truncate=False)
print(f"    Row count: {inner_df.count()} (out of 12 employees)")

# ------ 1B: Select specific columns after join ------
# After join, both tables' columns exist. Pick only what you need.
print("\n--- 1B: Inner join - select only needed columns ---")
emp.join(dept, on="dept_id", how="inner") \
   .select("emp_id", "emp_name", "dept_name", "city", "salary", "budget") \
   .show(truncate=False)


# =============================================================
# SECTION 2 - LEFT JOIN (Left Outer)
# =============================================================
# Returns ALL rows from the LEFT table.
# Rows in LEFT that match RIGHT get the right-side columns filled.
# Rows in LEFT with NO match get NULL for all right-side columns.
# Rows in RIGHT with NO match in LEFT are DROPPED.
#
# Use when: "I want all employees, and their dept info IF it exists."
# emp is LEFT -> all 12 employees appear.
# E09(D06), E11(D99), E12(NULL) -> dept columns are NULL.

print("\n" + "=" * 65)
print("SECTION 2 - LEFT JOIN (Left Outer)")
print("=" * 65)

print("\n--- 2A: Left join - ALL employees, dept info if available ---")
print("    All 12 employees kept | E09/E11/E12 get NULL dept columns")

left_df = emp.join(dept, on="dept_id", how="left")
left_df.select("emp_id", "emp_name", "dept_id", "dept_name", "city", "salary") \
       .show(truncate=False)
print(f"    Row count: {left_df.count()} (all 12 employees)")

# ------ 2B: Use left join to find employees WITHOUT a department ------
# After left join, employees with no dept have dept_name = NULL.
# Filter for those NULL rows to find "orphan" employees.
print("\n--- 2B: Find employees with NO matching department (isNull trick) ---")
emp.join(dept, on="dept_id", how="left") \
   .filter(col("dept_name").isNull()) \
   .select("emp_id", "emp_name", "dept_id", "city") \
   .show(truncate=False)


# =============================================================
# SECTION 3 - RIGHT JOIN (Right Outer)
# =============================================================
# Returns ALL rows from the RIGHT table.
# RIGHT rows with a match get left-side columns filled.
# RIGHT rows with NO match get NULL for all left-side columns.
# LEFT rows with NO match in RIGHT are DROPPED.
#
# Use when: "I want all departments, and which employees are in them."
# dept is RIGHT -> all 7 departments appear.
# D07, D08 have no employees -> emp columns are NULL.

print("\n" + "=" * 65)
print("SECTION 3 - RIGHT JOIN (Right Outer)")
print("=" * 65)

print("\n--- 3A: Right join - ALL departments, employee info if available ---")
print("    All 7 departments kept | D07,D08 get NULL employee columns")

right_df = emp.join(dept, on="dept_id", how="right")
right_df.select("dept_id", "dept_name", "emp_id", "emp_name", "salary") \
        .orderBy("dept_id") \
        .show(truncate=False)
print(f"    Row count: {right_df.count()} (all 7 depts, some with NULL emp)")

# ------ 3B: Find departments with NO employees ------
print("\n--- 3B: Departments with NO employees (isNull trick on right join) ---")
emp.join(dept, on="dept_id", how="right") \
   .filter(col("emp_id").isNull()) \
   .select("dept_id", "dept_name", "manager", "budget") \
   .show(truncate=False)


# =============================================================
# SECTION 4 - FULL OUTER JOIN
# =============================================================
# Returns ALL rows from BOTH tables.
# Matched rows get all columns filled.
# Unmatched left rows  -> right-side columns = NULL.
# Unmatched right rows -> left-side  columns = NULL.
#
# Use when: "I want everything — employees without depts AND
#            departments without employees."
# Result = inner join rows + left-only rows + right-only rows.

print("\n" + "=" * 65)
print("SECTION 4 - FULL OUTER JOIN")
print("=" * 65)

print("\n--- 4A: Full outer join - every employee AND every department ---")
print("    E09/E11/E12 appear with NULL dept | D07/D08 appear with NULL emp")

full_df = emp.join(dept, on="dept_id", how="full")
full_df.select("dept_id", "dept_name", "emp_id", "emp_name", "salary") \
       .orderBy("dept_id") \
       .show(20, truncate=False)
print(f"    Row count: {full_df.count()}")

# ------ 4B: Find ALL unmatched records from both sides ------
print("\n--- 4B: All unmatched records from BOTH sides ---")
emp.join(dept, on="dept_id", how="full") \
   .filter(col("emp_id").isNull() | col("dept_name").isNull()) \
   .select("dept_id", "dept_name", "emp_id", "emp_name") \
   .orderBy("dept_id") \
   .show(truncate=False)


# =============================================================
# SECTION 5 - LEFT SEMI JOIN
# =============================================================
# Returns only rows from the LEFT table that HAVE a match in RIGHT.
# RIGHT table columns are NOT included in the result.
#
# Think of it as: "Filter LEFT to keep only rows that exist in RIGHT."
# It's like INNER JOIN but you only get left-side columns.
#
# Use case: "Give me employees who received a bonus."
# emp LEFT SEMI JOIN bonus ON emp_id -> employees that appear in bonus.
# Result has only employee columns (no bonus columns).

print("\n" + "=" * 65)
print("SECTION 5 - LEFT SEMI JOIN")
print("=" * 65)

print("\n--- 5A: Semi join - employees who received a bonus ---")
print("    Only left-side (employee) columns returned")
print("    E04,E06,E08,E09,E11,E12 had no bonus -> excluded")

semi_df = emp.join(bonus, on="emp_id", how="left_semi")
semi_df.show(truncate=False)
print(f"    Row count: {semi_df.count()}")

print("\n--- 5B: Semi join - employees working in a known department ---")
print("    Equivalent to: employees whose dept_id exists in departments")
emp.join(dept, on="dept_id", how="left_semi") \
   .show(truncate=False)


# =============================================================
# SECTION 6 - LEFT ANTI JOIN
# =============================================================
# Returns only rows from the LEFT table that have NO match in RIGHT.
# Exact opposite of left semi join.
# RIGHT table columns are NOT included.
#
# Use case: "Give me employees who did NOT receive a bonus."
# emp LEFT ANTI JOIN bonus ON emp_id -> employees NOT in bonus.

print("\n" + "=" * 65)
print("SECTION 6 - LEFT ANTI JOIN")
print("=" * 65)

print("\n--- 6A: Anti join - employees who did NOT receive a bonus ---")
print("    Only employees with NO matching emp_id in bonuses table")

anti_df = emp.join(bonus, on="emp_id", how="left_anti")
anti_df.show(truncate=False)
print(f"    Row count: {anti_df.count()}")

print("\n--- 6B: Anti join - employees with NO matching department ---")
print("    Same result as the isNull trick in left join 2B, but cleaner")
emp.join(dept, on="dept_id", how="left_anti") \
   .show(truncate=False)


# =============================================================
# SECTION 7 - DUPLICATE COLUMN NAMES AFTER JOIN
# =============================================================
# When joining on an EXPRESSION (not a shared column name),
# both dept_id columns from emp and dept appear separately.
# This causes ambiguity when you try to use col("dept_id").
#
# Three ways to fix:
#   A. Join on a column name string (Spark merges into one column)
#   B. Rename before join with .withColumnRenamed()
#   C. Use table aliases + select with alias.column syntax

print("\n" + "=" * 65)
print("SECTION 7 - Duplicate Column Names After Join")
print("=" * 65)

print("\n--- 7A: The problem - join on expression creates TWO dept_id columns ---")
# Using emp["dept_id"] == dept["dept_id"] keeps BOTH columns
duped = emp.join(dept, emp["dept_id"] == dept["dept_id"], how="inner")
print("    Schema after expression join (notice two dept_id columns):")
duped.printSchema()
# This causes AnalysisException if you try col("dept_id") later

print("\n--- 7B: Fix 1 - join on string key (Spark merges into one column) ---")
# Passing the column name as a string automatically merges both sides
fixed1 = emp.join(dept, on="dept_id", how="inner")
print("    Schema after on='dept_id' (single dept_id column):")
fixed1.printSchema()

print("\n--- 7C: Fix 2 - withColumnRenamed before join ---")
# Rename the right-side column to avoid collision
dept_renamed = dept.withColumnRenamed("dept_id", "d_dept_id")
fixed2 = emp.join(dept_renamed, emp["dept_id"] == dept_renamed["d_dept_id"], how="inner")
fixed2.select("emp_id", "emp_name", "dept_id", "dept_name", "budget").show(5, truncate=False)

print("\n--- 7D: Fix 3 - use DataFrame references to disambiguate ---")
# Access columns by DataFrame reference instead of by name
duped.select(
    emp["emp_id"],
    emp["emp_name"],
    emp["dept_id"].alias("emp_dept_id"),
    dept["dept_name"],
    dept["budget"]
).show(5, truncate=False)


# =============================================================
# SECTION 8 - CROSS JOIN
# =============================================================
# Every row from LEFT is paired with EVERY row from RIGHT.
# Result rows = LEFT count × RIGHT count.
# NO join key — all combinations are produced.
#
# Use case: generate all possible employee-department pairings,
#           build a date/product matrix, Cartesian product for scoring.
# WARNING: Can be very large. 12 emp × 7 dept = 84 rows.

print("\n" + "=" * 65)
print("SECTION 8 - CROSS JOIN")
print("=" * 65)

print("\n--- 8A: Cross join - every employee paired with every department ---")
print(f"    {emp.count()} employees × {dept.count()} departments = {emp.count() * dept.count()} rows")
cross_df = emp.crossJoin(dept)
cross_df.select("emp_id", "emp_name", "dept_id", "dept_name") \
        .orderBy("emp_id", "dept_id") \
        .show(20, truncate=False)
print(f"    Total rows: {cross_df.count()}")


# =============================================================
# SECTION 9 - union() and unionByName()
# =============================================================
# union()        - stacks two DataFrames row by row.
#                  Matches columns BY POSITION (not by name).
#                  Schema must have the same number of columns.
#
# unionByName()  - matches columns BY NAME (order doesn't matter).
#                  Safer when column order may differ.
#                  Use allowMissingColumns=True to handle schema differences.
#
# Neither deduplicates rows. Use .distinct() after if needed.

print("\n" + "=" * 65)
print("SECTION 9 - union() and unionByName()")
print("=" * 65)

print("\n--- 9A: union() - stack employees_2024 on top of employees_2023 ---")
print("    Matches by POSITION - column order must match")
union_df = emp.union(emp_2023)
union_df.show(20, truncate=False)
print(f"    2024 rows: {emp.count()} | 2023 rows: {emp_2023.count()} | union: {union_df.count()}")

print("\n--- 9B: union() includes duplicates - employees appearing in both years ---")
print("    E01, E02, E03 appear in BOTH files -> appear twice in union")
union_df.groupBy("emp_id", "emp_name") \
        .count() \
        .filter(col("count") > 1) \
        .show(truncate=False)

print("\n--- 9C: union().distinct() - deduplicate after union ---")
# distinct() removes exact duplicate rows (all columns must match).
# Note: E01/E02/E03 have different salaries in 2023 vs 2024 -> NOT exact duplicates.
print("    .distinct() removes exact row duplicates (all columns must be equal)")
union_df.distinct().show(20, truncate=False)
print(f"    After distinct: {union_df.distinct().count()} rows")

print("\n--- 9D: unionByName() - safer: matches columns by name ---")
# Reorder columns in emp_2023 to demonstrate name-based matching
emp_2023_reordered = emp_2023.select("emp_name", "emp_id", "city", "dept_id", "salary", "join_date")
print("    emp_2023 column order intentionally shuffled:")
emp_2023_reordered.printSchema()

union_by_name = emp.unionByName(emp_2023_reordered)
union_by_name.show(20, truncate=False)
print("    unionByName matched columns correctly despite different order")

print("\n--- 9E: unionByName with allowMissingColumns ---")
# When one DataFrame has extra columns the other doesn't, use allowMissingColumns=True.
# Missing columns are filled with NULL.
emp_extra = emp.select("emp_id", "emp_name", "dept_id", "city", "salary", "join_date") \
               .withColumn("phone", col("emp_id"))  # extra column only in emp
emp_2023_basic = emp_2023.select("emp_id", "emp_name", "dept_id", "city", "salary", "join_date")

union_missing = emp_extra.unionByName(emp_2023_basic, allowMissingColumns=True)
union_missing.show(20, truncate=False)
print("    emp_2023 rows have NULL in 'phone' column (column missing from that file)")


# =============================================================
# JOIN TYPE QUICK REFERENCE
# =============================================================
# how=           What it returns
# -------        ---------------------------------------------------
# "inner"        Only matched rows from BOTH sides
# "left"         ALL left rows + matched right (NULL if no match)
# "right"        ALL right rows + matched left (NULL if no match)
# "full"         ALL rows from BOTH sides (NULL where no match)
# "left_semi"    Left rows that HAVE a match in right (no right cols)
# "left_anti"    Left rows that have NO match in right (no right cols)
# crossJoin()    Every left row × every right row (no key needed)

# =============================================================
# IMPORTS SUMMARY
# =============================================================
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StructType, StructField, ...
#
# Joins:
#   df1.join(df2, on="col",        how="inner/left/right/full/left_semi/left_anti")
#   df1.join(df2, df1.c == df2.c,  how=...)   -- expression join (watch for dup cols)
#   df1.crossJoin(df2)                         -- cartesian product
#
# Union:
#   df1.union(df2)                             -- by position
#   df1.unionByName(df2)                       -- by name
#   df1.unionByName(df2, allowMissingColumns=True)  -- handles schema differences
#   .distinct()                                -- deduplicate after union

print("\n" + "=" * 65)
print("Day 7 Complete - Joins and Union")
print("=" * 65)

spark.stop()
print("SparkSession stopped. Done.")

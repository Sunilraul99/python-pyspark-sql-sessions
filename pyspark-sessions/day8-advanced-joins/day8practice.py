"""
Day 8 - Advanced Joins in PySpark
Topics:
  - Join on expression (df1.col == df2.col) when column names differ
  - Join on same column name (on="col") — recap + when to use each
  - Handling duplicate/ambiguous columns after expression joins
  - Aliases (.alias()) on DataFrames to qualify column names
  - Multiple table joins (3-way, 4-way) chained
  - Self join (employee -> manager lookup)
  - Multiple join conditions (AND / OR in expression)
  - Joining on computed/transformed columns (lower, trim)
  - Broadcast join hint for performance

Datasets (all inside data/):
  employees.csv   - 10 employees, dept_code, location_id, manager_id
  departments.csv - 7 departments, department_id (different name from dept_code!)
  locations.csv   - 5 locations, loc_id (different name from location_id!)
  projects.csv    - 8 projects, lead_emp_id, dept_code

Key difference from Day 7:
  Day 7 -> employees and departments both had "dept_id" (same name) -> on="dept_id"
  Day 8 -> employees has "dept_code", departments has "department_id" (different names)
           -> must use expression: emp["dept_code"] == dept["department_id"]
"""

import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

os.environ['JAVA_HOME']             = 'C:/Program Files/DBeaver/jre'
os.environ['PYSPARK_PYTHON']        = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hariom\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, broadcast, coalesce, lit

spark = SparkSession.builder \
    .appName("Day8 - Advanced Joins") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA = "day8-advanced-joins/data"

print("=" * 65)
print("Day 8 - Advanced Joins in PySpark")
print("=" * 65)

# ------------------------------------------------------------------
# Load all datasets and assign DataFrame aliases
# ------------------------------------------------------------------
# .alias("e") gives the DataFrame a short name.
# After a join, use col("e.emp_name") to avoid column name ambiguity.
# Always alias DataFrames before joining — it is a production habit.

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

emp = spark.read.option("header", "true").csv(f"{DATA}/employees.csv") \
          .alias("emp")

dept = spark.read.option("header", "true").csv(f"{DATA}/departments.csv") \
           .alias("dept")

loc = spark.read.option("header", "true").csv(f"{DATA}/locations.csv") \
          .alias("loc")

proj = spark.read.option("header", "true").csv(f"{DATA}/projects.csv") \
           .alias("proj")

print("\n--- employees ---")
emp.show(truncate=False)

print("\n--- departments (note: key is 'department_id', NOT 'dept_code') ---")
dept.show(truncate=False)

print("\n--- locations (note: key is 'loc_id', NOT 'location_id') ---")
loc.show(truncate=False)

print("\n--- projects ---")
proj.show(truncate=False)


# =============================================================
# SECTION 1 - WHY COLUMN NAMES DIFFER IN REAL TABLES
# =============================================================
# In Day 7, both tables had "dept_id" -> on="dept_id" worked.
#
# Real-world tables often use different names for the same concept:
#   employees.dept_code    links to   departments.department_id
#   employees.location_id  links to   locations.loc_id
#
# When names differ, on="col" does NOT work — Spark won't know
# which column you mean. You MUST use an expression:
#
#   emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner")
#   emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner")
#
# These two forms are equivalent. The second (col with alias prefix)
# is preferred in multi-table joins to avoid ambiguity.

print("\n" + "=" * 65)
print("SECTION 1 - Expression Join: different column names")
print("=" * 65)

# ------ 1A: Wrong attempt - on="dept_code" fails ------
print("\n--- 1A: What happens with on='dept_code' when names differ ---")
print("    employees has 'dept_code', departments has 'department_id'")
print("    Using on='dept_code' would look for 'dept_code' in BOTH tables")
print("    departments has no 'dept_code' -> result would be empty or error")
print("    CORRECT approach: use an expression shown in 1B below\n")

# ------ 1B: Correct - expression join with DataFrame["col"] ------
# emp["dept_code"] == dept["department_id"]
# Left side  -> column from emp DataFrame
# Right side -> column from dept DataFrame
# Spark matches rows where these two values are equal.
print("\n--- 1B: Expression join - emp['dept_code'] == dept['department_id'] ---")
result_1b = emp.join(
    dept,
    emp["dept_code"] == dept["department_id"],
    how="inner"
)
result_1b.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("emp.dept_code"),
    col("dept.department_id"),
    col("dept.department_name"),
    col("emp.salary")
).show(truncate=False)

# ------ 1C: Same join using col("alias.column") style ------
# Equivalent to 1B. Preferred when DataFrame is aliased.
# col("emp.dept_code") reads: "the dept_code column from the DataFrame aliased as 'emp'"
print("\n--- 1C: Same join using col('alias.column') style ---")
emp.join(
    dept,
    col("emp.dept_code") == col("dept.department_id"),
    how="inner"
) \
.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("dept.department_name"),
    col("emp.salary")
) \
.show(truncate=False)


# =============================================================
# SECTION 2 - DUPLICATE COLUMNS PROBLEM WITH EXPRESSION JOIN
# =============================================================
# When you use an expression join (df1.col == df2.col),
# Spark keeps BOTH columns separately in the result.
# Both "dept_code" and "department_id" appear even though they
# hold the same value. This causes ambiguity and wasted columns.
#
# Three clean solutions:
#   A. drop() the duplicate after join
#   B. select() only the columns you need using alias prefix
#   C. withColumnRenamed() before join to unify names

print("\n" + "=" * 65)
print("SECTION 2 - Handling Duplicate Columns in Expression Joins")
print("=" * 65)

print("\n--- 2A: Problem - both dept_code AND department_id appear ---")
raw_join = emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner")
print("    Schema after expression join (notice both key columns):")
raw_join.printSchema()

print("\n--- 2B: Fix A - drop() the redundant column ---")
# Keep emp's dept_code, drop dept's department_id (same value, different name)
emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner") \
   .drop(col("dept.department_id")) \
   .select("emp_id", "emp_name", "dept_code", "department_name", "salary") \
   .show(truncate=False)

print("\n--- 2C: Fix B - select() only needed columns with alias prefix ---")
# Most production-friendly — you explicitly control what comes out
emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner") \
   .select(
       col("emp.emp_id").alias("emp_id"),
       col("emp.emp_name").alias("emp_name"),
       col("emp.dept_code").alias("dept_code"),
       col("dept.department_name").alias("dept_name"),
       col("emp.salary").alias("salary"),
       col("dept.annual_budget").alias("dept_budget")
   ) \
   .show(truncate=False)

print("\n--- 2D: Fix C - rename before join to unify key column names ---")
# Rename dept's key to match emp's key name -> then use on="dept_code"
dept_renamed = dept.withColumnRenamed("department_id", "dept_code")
emp.join(dept_renamed, on="dept_code", how="inner") \
   .select("emp_id", "emp_name", "dept_code", "department_name", "salary") \
   .show(truncate=False)


# =============================================================
# SECTION 3 - MULTIPLE JOIN CONDITIONS (AND / OR)
# =============================================================
# You can combine multiple conditions in a single join expression
# using & (AND) and | (OR).
#
# AND: row must satisfy BOTH conditions to match
# OR:  row matches if EITHER condition is true (rare in joins, used for fuzzy matching)

print("\n" + "=" * 65)
print("SECTION 3 - Multiple Join Conditions")
print("=" * 65)

print("\n--- 3A: AND condition - join on dept_code AND status='active' ---")
# Join employees to projects on BOTH dept_code AND status
# Only active projects from the employee's department are matched
emp.join(
    proj,
    (col("emp.dept_code") == col("proj.dept_code")) &
    (col("proj.status") == "active"),
    how="inner"
) \
.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("emp.dept_code"),
    col("proj.project_name"),
    col("proj.status")
) \
.show(truncate=False)

print("\n--- 3B: Multiple AND conditions ---")
# Projects led by someone in the same dept AND with budget > 500000
emp.join(
    proj,
    (col("emp.emp_id") == col("proj.lead_emp_id")) &
    (col("emp.dept_code") == col("proj.dept_code")),
    how="inner"
) \
.select(
    col("emp.emp_name").alias("project_lead"),
    col("emp.dept_code"),
    col("proj.project_name"),
    col("proj.budget"),
    col("proj.status")
) \
.show(truncate=False)


# =============================================================
# SECTION 4 - JOINING ON TRANSFORMED COLUMNS
# =============================================================
# Sometimes the key values don't match exactly due to case
# differences or extra spaces in real data.
# You can apply functions INSIDE the join expression.

print("\n" + "=" * 65)
print("SECTION 4 - Join on Transformed / Computed Columns")
print("=" * 65)

# Simulate dirty data: dept codes with mixed case and spaces
from pyspark.sql import Row

dirty_emp = spark.createDataFrame([
    Row(emp_id="E01", emp_name="Amit",  dept_key=" DC01 "),
    Row(emp_id="E02", emp_name="Priya", dept_key="dc02"),
    Row(emp_id="E03", emp_name="Ravi",  dept_key="DC03"),
    Row(emp_id="E04", emp_name="Sneha", dept_key="DC99"),   # no match
]).alias("dirty")

clean_dept = spark.createDataFrame([
    Row(dept_id="DC01", dept_name="Engineering"),
    Row(dept_id="DC02", dept_name="Marketing"),
    Row(dept_id="DC03", dept_name="Data Science"),
]).alias("clean")

print("\n--- 4A: Dirty data - spaces and mixed case in key column ---")
dirty_emp.show()

print("\n--- 4B: Direct join FAILS - ' DC01 ' != 'DC01', 'dc02' != 'DC02' ---")
direct = dirty_emp.join(clean_dept, dirty_emp["dept_key"] == clean_dept["dept_id"], how="left")
direct.select("emp_id", "emp_name", "dept_key", "dept_name").show()
# Amit (DC01 with spaces) and Priya (dc02 lowercase) -> NULL dept_name

print("\n--- 4C: Fix - apply trim() and lower() inside the join expression ---")
# Transform BOTH sides to a common format before comparing
dirty_emp.join(
    clean_dept,
    lower(trim(col("dirty.dept_key"))) == lower(col("clean.dept_id")),
    how="left"
) \
.select(
    col("dirty.emp_id"),
    col("dirty.emp_name"),
    col("dirty.dept_key").alias("raw_dept_key"),
    col("clean.dept_name")
) \
.show()


# =============================================================
# SECTION 5 - SELF JOIN (Employee -> Manager lookup)
# =============================================================
# A self join joins a table to itself.
# Use case: employees table has manager_id that points to emp_id
#           in the SAME table. To get the manager's name, join
#           the table to itself.
#
# MUST use two different aliases — otherwise Spark cannot
# distinguish which copy of the table is which.
#   emp_table.alias("e")       <- employee side
#   emp_table.alias("mgr")     <- manager side

print("\n" + "=" * 65)
print("SECTION 5 - Self Join (Employee -> Manager)")
print("=" * 65)

# Load without alias first, then alias during join
emp_raw = spark.read.option("header", "true").csv(f"{DATA}/employees.csv")

emp_self  = emp_raw.alias("e")
mgr_self  = emp_raw.alias("mgr")

print("\n--- 5A: Self join - each employee with their manager's name ---")
print("    e.manager_id == mgr.emp_id")
print("    left join so employees with no manager (NULL) still appear")

emp_self.join(
    mgr_self,
    col("e.manager_id") == col("mgr.emp_id"),
    how="left"
) \
.select(
    col("e.emp_id").alias("emp_id"),
    col("e.emp_name").alias("employee"),
    col("e.salary").alias("emp_salary"),
    col("mgr.emp_id").alias("manager_id"),
    col("mgr.emp_name").alias("manager_name")
) \
.orderBy("e.emp_id") \
.show(truncate=False)
# E07 and E08 have NULL manager_id -> manager_name = NULL (they are top-level)


# =============================================================
# SECTION 6 - THREE-TABLE JOIN
# =============================================================
# Chain .join() calls to join 3 or more tables.
# Each .join() adds one more table.
# Always alias ALL DataFrames before joining.
#
# Pattern:
#   df1.join(df2, condition, how)
#      .join(df3, condition, how)
#      .join(df4, condition, how)
#      .select(...)

print("\n" + "=" * 65)
print("SECTION 6 - Three-Table Join: employees + departments + locations")
print("=" * 65)

# employees.dept_code   -> departments.department_id  (different names)
# employees.location_id -> locations.loc_id           (different names)

print("\n--- 6A: Three-table join ---")
print("    emp.dept_code == dept.department_id  (different column names)")
print("    emp.location_id == loc.loc_id        (different column names)")

three_way = emp.join(
    dept,
    col("emp.dept_code") == col("dept.department_id"),
    how="left"
).join(
    loc,
    col("emp.location_id") == col("loc.loc_id"),
    how="left"
)

three_way.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("dept.department_name").alias("department"),
    col("loc.city").alias("city"),
    col("loc.state").alias("state"),
    col("emp.salary")
) \
.orderBy("emp.emp_id") \
.show(truncate=False)

print("\n--- 6B: Three-table join - filter and aggregate on result ---")
three_way.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("dept.department_name").alias("department"),
    col("loc.city").alias("city"),
    col("emp.salary")
) \
.filter(col("loc.city").isin("Mumbai", "Delhi")) \
.orderBy(col("emp.salary").desc()) \
.show(truncate=False)


# =============================================================
# SECTION 7 - FOUR-TABLE JOIN
# =============================================================
# Add a 4th table: employees + departments + locations + projects
# employees.emp_id -> projects.lead_emp_id  (different names)

print("\n" + "=" * 65)
print("SECTION 7 - Four-Table Join: emp + dept + loc + projects")
print("=" * 65)

print("\n--- 7A: Four-table join - full employee profile with project ---")
print("    Join 1: emp.dept_code == dept.department_id")
print("    Join 2: emp.location_id == loc.loc_id")
print("    Join 3: emp.emp_id == proj.lead_emp_id")

four_way = emp.join(
    dept,
    col("emp.dept_code") == col("dept.department_id"),
    how="left"
).join(
    loc,
    col("emp.location_id") == col("loc.loc_id"),
    how="left"
).join(
    proj,
    col("emp.emp_id") == col("proj.lead_emp_id"),
    how="left"
)

four_way.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("dept.department_name").alias("department"),
    col("loc.city").alias("city"),
    col("emp.salary"),
    col("proj.project_name"),
    col("proj.budget").alias("project_budget"),
    col("proj.status").alias("project_status")
) \
.orderBy("emp.emp_id") \
.show(truncate=False)

print("\n--- 7B: Four-table join - employees leading ACTIVE projects only ---")
four_way.filter(col("proj.status") == "active") \
.select(
    col("emp.emp_name").alias("lead"),
    col("dept.department_name").alias("dept"),
    col("loc.city").alias("city"),
    col("proj.project_name"),
    col("proj.budget")
) \
.orderBy(col("proj.budget").desc()) \
.show(truncate=False)


# =============================================================
# SECTION 8 - BROADCAST JOIN (Performance Hint)
# =============================================================
# When one DataFrame is small (fits in memory on each executor),
# wrap it in broadcast() to avoid shuffling the large DataFrame.
#
# Spark normally shuffles both DataFrames to co-locate matching keys.
# broadcast() sends the small DataFrame to EVERY executor instead,
# so the large DataFrame never moves.
#
# Rule of thumb: use broadcast() when one side is < 10MB (a few thousand rows).
# departments (7 rows) and locations (5 rows) are perfect candidates.

print("\n" + "=" * 65)
print("SECTION 8 - Broadcast Join Hint (Performance)")
print("=" * 65)

print("\n--- 8A: Without broadcast hint (default shuffle join) ---")
emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner") \
   .select(col("emp.emp_name"), col("dept.department_name")) \
   .show(truncate=False)

print("\n--- 8B: With broadcast() on the small table ---")
# broadcast(dept) tells Spark: don't shuffle dept, replicate it to all executors
# The query plan changes but the result is identical
emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="inner") \
   .select(col("emp.emp_name"), col("dept.department_name")) \
   .show(truncate=False)

print("\n--- 8C: Broadcast in a three-table join ---")
emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(broadcast(loc),  col("emp.location_id") == col("loc.loc_id"),         how="left") \
   .select(
       col("emp.emp_name"),
       col("dept.department_name"),
       col("loc.city")
   ) \
   .show(truncate=False)


# =============================================================
# SECTION 9 - COMMON REAL-WORLD PATTERNS
# =============================================================

print("\n" + "=" * 65)
print("SECTION 9 - Common Real-World Join Patterns")
print("=" * 65)

# ------ 9A: Enrich a fact table with dimension tables ------
# employees = fact table (has foreign keys)
# departments, locations = dimension tables (lookup tables)
# This is the classic star schema pattern in data warehousing.
print("\n--- 9A: Star schema pattern - enrich employees with all dimensions ---")
emp.join(broadcast(dept), col("emp.dept_code") == col("dept.department_id"), how="left") \
   .join(broadcast(loc),  col("emp.location_id") == col("loc.loc_id"),         how="left") \
   .select(
       col("emp.emp_id"),
       col("emp.emp_name"),
       col("dept.department_name"),
       col("loc.city"),
       col("loc.state"),
       col("emp.salary")
   ) \
   .orderBy("emp.emp_id") \
   .show(truncate=False)

# ------ 9B: Self join + regular join combined ------
# Get employee, their manager's name, and the department name all together
print("\n--- 9B: Self join + regular join in one chain ---")
emp_side = emp_raw.alias("e")
mgr_side = emp_raw.alias("mgr")

emp_side.join(
    mgr_side,
    col("e.manager_id") == col("mgr.emp_id"),
    how="left"
).join(
    dept,
    col("e.dept_code") == col("dept.department_id"),
    how="left"
) \
.select(
    col("e.emp_id"),
    col("e.emp_name").alias("employee"),
    col("mgr.emp_name").alias("reports_to"),
    col("dept.department_name").alias("department"),
    col("e.salary")
) \
.orderBy("e.emp_id") \
.show(truncate=False)

# ------ 9C: Join + groupBy to summarize per department ------
print("\n--- 9C: Join then groupBy - department headcount and avg salary ---")
from pyspark.sql.functions import count, avg, round as spark_round, sum as spark_sum

emp.join(
    dept,
    col("emp.dept_code") == col("dept.department_id"),
    how="inner"
) \
.groupBy(col("dept.department_name").alias("department")) \
.agg(
    count("emp.emp_id").alias("headcount"),
    spark_round(avg("emp.salary"), 0).alias("avg_salary"),
    spark_round(spark_sum("emp.salary"), 0).alias("total_salary_cost")
) \
.orderBy(col("avg_salary").desc()) \
.show(truncate=False)

# ------ 9D: coalesce() after outer join to fill NULLs ------
# After a left join, unmatched rows have NULL in right-side columns.
# Use coalesce(col, lit("default")) to replace NULLs with a default.
print("\n--- 9D: coalesce() to replace NULLs from outer join ---")
emp.join(
    dept,
    col("emp.dept_code") == col("dept.department_id"),
    how="left"
) \
.select(
    col("emp.emp_id"),
    col("emp.emp_name"),
    col("emp.dept_code"),
    coalesce(col("dept.department_name"), lit("Unknown Dept")).alias("department"),
    coalesce(col("dept.annual_budget"),   lit(0)).alias("dept_budget")
) \
.show(truncate=False)
# E09 (DC06) and DC06 doesn't exist in departments -> department = "Unknown Dept"


# =============================================================
# QUICK REFERENCE: WHEN TO USE WHICH JOIN SYNTAX
# =============================================================
#
# SAME column name on both sides:
#   emp.join(dept, on="dept_id", how="inner")
#   -> Spark merges both into ONE column (no duplicate)
#   -> Clean, recommended when names match
#
# DIFFERENT column names:
#   emp.join(dept, emp["dept_code"] == dept["department_id"], how="inner")
#   emp.join(dept, col("emp.dept_code") == col("dept.department_id"), how="inner")
#   -> Both key columns appear in result (duplicate problem)
#   -> Fix: .drop() one, or .select() only what you need
#
# MULTIPLE conditions:
#   emp.join(proj,
#       (col("emp.dept_code") == col("proj.dept_code")) &
#       (col("proj.status") == "active"),
#       how="inner")
#
# SELF JOIN (same table twice):
#   emp_raw.alias("e").join(emp_raw.alias("mgr"),
#       col("e.manager_id") == col("mgr.emp_id"), how="left")
#
# TRANSFORMED key (trim/lower before comparing):
#   df1.join(df2, lower(trim(col("df1.key"))) == lower(col("df2.key")))
#
# BROADCAST (small table hint):
#   df1.join(broadcast(small_df), condition, how)

print("\n" + "=" * 65)
print("Day 8 Complete - Advanced Joins")
print("=" * 65)

spark.stop()
print("SparkSession stopped. Done.")

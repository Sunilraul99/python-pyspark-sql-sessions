# Day 10 Problems — Data Engineering Patterns

Use the CSVs from `data/` for all problems.

---

## Section 1 — UPSERT

**Problem 1**
Load `employees_current.csv` and `employees_incoming.csv`.
Perform a full UPSERT:
- Rows in incoming that match an existing `emp_id` → update with incoming values
- Rows in incoming with a new `emp_id` → insert as new rows
- Rows in current not touched by incoming → keep as-is

How many rows are in the final result?

**Problem 2**
From the UPSERT result, show only the rows that were UPDATED (existed in both current and incoming).
Hint: inner join current with incoming on emp_id.

**Problem 3**
From the UPSERT result, show only the rows that were NEW INSERTS (existed only in incoming).
Hint: left_anti join incoming on current emp_ids.

**Problem 4**
Perform an UPDATE ONLY (no inserts):
Update `salary` and `department` for employees that appear in incoming.
For employees NOT in incoming, keep all values unchanged.
Add a column `was_updated` (True/False) indicating which rows were touched.

**Problem 5**
After the UPSERT in Problem 1, add a column `change_type` with values:
- `"UPDATED"` for rows that existed in current and were updated
- `"INSERTED"` for rows that are new
- `"UNCHANGED"` for rows that were not touched
Hint: use two left_anti and one inner join, then add a literal column to each, then union.

---

## Section 2 — CDC Processing

**Problem 6**
Load `orders_cdc.csv`.
Show the raw CDC log sorted by `order_id` then `cdc_ts`.
How many total events are there? How many distinct order_ids?

**Problem 7**
For each `order_id`, find the latest `cdc_op` (most recent event by `cdc_ts`).
Show order_id and its final operation code.

**Problem 8**
Apply the CDC log to produce the final orders table:
- Keep only the latest event per `order_id`
- Exclude orders whose final operation was `D` (Delete)
Show the final table sorted by `order_id`.

**Problem 9**
From the CDC log, find all orders that went through a DELETE operation at any point.
*(Hint: filter cdc_op == 'D' before deduplication — an order may have been deleted then reinserted)*

**Problem 10**
Count how many orders ended in each final state (I/U/D) after applying the CDC log.
Show: `final_op`, `count`.

**Problem 11**
From the CDC log, find the orders that changed status more than once.
*(Hint: count events per order_id — orders with count > 1 were updated at least once)*

---

## Section 3 — SCD Type 2

**Problem 12**
Load `employees_current.csv` as the existing SCD2 table.
Add the SCD2 columns: `effective_from = 2024-01-10`, `effective_to = 9999-12-31`, `is_current = True`.
Show the resulting SCD2 table.

**Problem 13**
Load `employees_incoming.csv` (change date: 2024-06-01).
Apply SCD Type 2:
- Close old versions of changed employees (effective_to = 2024-05-31, is_current = False)
- Add new versions from incoming (effective_from = 2024-06-01, effective_to = 9999-12-31, is_current = True)
- Keep unchanged employees as-is

How many total rows are in the final SCD2 table?

**Problem 14**
From the SCD2 table in Problem 13, show only active records (is_current = True).

**Problem 15**
From the SCD2 table, show the full history for employees E001 and E003.
Which fields changed between versions?

**Problem 16**
Write a point-in-time query: what did the employee table look like on `2024-03-15`?
Filter rows where `effective_from <= 2024-03-15 AND effective_to >= 2024-03-15`.

**Problem 17**
How many employees have more than one version in the SCD2 table?
*(Hint: groupBy emp_id, count rows, filter count > 1)*

---

## Section 4 — Soft Delete

**Problem 18**
Load `products.csv`.
How many products are currently soft-deleted (is_deleted = True)?
Show their product_id, product_name, and deleted_at.

**Problem 19**
Perform a soft delete on products P002 and P005.
Set is_deleted = True and deleted_at = today's date.
Show the updated table.

**Problem 20**
After the soft delete in Problem 19, show only active (non-deleted) products.
How many remain?

**Problem 21**
Restore product P004 (Undo its soft delete).
Set is_deleted = False and deleted_at = NULL.
Show the row for P004 before and after the restore.

**Problem 22**
Perform a hard purge — create a new DataFrame that physically removes all soft-deleted rows.
Show the final row count vs the original.

**Problem 23**
Add a column `status` to the products table:
- `"Active"` if is_deleted = False
- `"Deleted"` if is_deleted = True
Show all products with this status column.

---

## Section 5 — Deduplication

**Problem 24**
Create the following raw DataFrame with duplicates:

| emp_id | emp_name | salary | loaded_at |
|---|---|---|---|
| E001 | Amit | 95000 | 2024-01-10 08:00:00 |
| E001 | Amit | 95000 | 2024-01-10 08:05:00 |
| E001 | Amit | 105000 | 2024-06-01 10:00:00 |
| E002 | Priya | 72000 | 2024-01-10 09:00:00 |
| E002 | Priya | 72000 | 2024-01-10 09:05:00 |
| E003 | Ravi | 88000 | 2024-01-10 09:30:00 |
| E003 | Ravi | 95000 | 2024-06-01 11:00:00 |

Use `row_number()` to keep the latest row per `emp_id`. Show the result.

**Problem 25**
On the same DataFrame, use `dropDuplicates(["emp_id", "salary", "loaded_at"])` to remove exact duplicates.
How many rows remain? Is the result different from Problem 24?

**Problem 26**
On the same DataFrame, use the `groupBy + max(loaded_at) + re-join` approach to get the latest row per `emp_id`.
Verify the result matches Problem 24.

**Problem 27**
From the raw DataFrame in Problem 24, find emp_ids that have more than 2 records in the raw data.
*(These are the ones with the most duplication)*

---

## Section 6 — Combined Patterns

**Problem 28**
Combine Soft Delete + UPSERT:
- Load `employees_current.csv`
- Soft-delete employees E004 and E010 (set is_active = False, updated_at = today)
- Then UPSERT with `employees_incoming.csv`
Show the final table with is_active column.

**Problem 29**
Combine CDC + SCD2:
Apply the CDC log from `orders_cdc.csv` to get the final orders table (Section 2).
Then treat the final orders table as a slowly changing dimension:
Add `effective_from`, `effective_to`, `is_current` columns to the final result.
*(In practice this simulates building an SCD2 snapshot from a CDC feed)*

**Problem 30**
Full pipeline simulation:
1. Start with `employees_current.csv`
2. Deduplicate it (simulate: add 3 duplicate rows for E001 and E003, then dedup)
3. UPSERT with `employees_incoming.csv`
4. Soft-delete E002 and E008
5. Show the final table with only active employees (is_active = True)

---

## Section 7 — Delta Lake MERGE

**Problem 36**
Write the SparkSession configuration needed to enable Delta Lake extensions.
What two `.config()` keys must be set?

**Problem 37**
Write the Delta MERGE statement to perform a basic UPSERT on an `employees` Delta table.
Match on `emp_id`. Update all columns when matched, insert all when not matched.

**Problem 38**
Write a Delta MERGE that only updates `salary` and `department` for matching rows.
All other columns should remain unchanged.
*(Use `whenMatchedUpdate(set={...})` instead of `whenMatchedUpdateAll()`)*

**Problem 39**
Write a Delta MERGE that applies a CDC log (with `cdc_op` column) in one operation:
- `cdc_op = 'I'` → insert
- `cdc_op = 'U'` → update all columns
- `cdc_op = 'D'` → delete the row

**Problem 40**
Write a Delta MERGE for SCD Type 2 — Step 1 only:
Close old versions of changed rows by setting `effective_to = '2024-05-31'` and `is_current = false`.
The merge condition must match on `emp_id` AND `is_current = true` (to only close active rows).

**Problem 41**
Write a Delta MERGE to soft-delete products P002 and P005:
Set `is_deleted = true` and `deleted_at = current_date()` for matching product_ids.

**Problem 42**
Compare: what happens if two jobs run the pure PySpark UPSERT (union approach) at the same time
on the same output path? What happens with Delta MERGE? Why is Delta safer?

---

## Section 8 — Analytical Queries on Patterns

**Problem 43**
After the UPSERT from Problem 1, group by `department` and count employees per department.
Which department has the most employees?

**Problem 44**
After applying SCD2, find the average salary for active employees vs closed (historical) employees.
*(Group by is_current, aggregate avg of salary)*

**Problem 45**
From the CDC orders log, calculate the total `amount` of orders that are currently active (not deleted).
Also show total amount of deleted orders.

**Problem 46**
From `employees_current.csv`, add a column `salary_band`:
- `"Junior"` if salary < 70000
- `"Mid"` if salary 70000–89999
- `"Senior"` if salary >= 90000

Then soft-delete all `"Junior"` employees (is_active = False).
Show the count per salary_band after the soft delete.

**Problem 47**
After the SCD2 operation, write a query that shows for each employee:
- emp_id
- emp_name
- Number of versions in SCD2 table
- Latest effective_from date
- Current salary

*(Hint: groupBy emp_id, use max/count as aggregations, then filter for current version to get salary)*

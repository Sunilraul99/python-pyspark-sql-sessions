# Day 7 Problems — Joins and Union

Use the datasets from `data/`: `employees.csv`, `departments.csv`, `bonuses.csv`, `employees_2023.csv`.

---

## Section 1 — Inner Join

**Problem 1**
Join employees with departments. Show emp_id, emp_name, dept_name, city, salary.
How many employees have a matching department?

**Problem 2**
Join employees with bonuses. Show emp_name, city, salary, bonus_amount, reason.
Only employees who received a bonus should appear.

**Problem 3**
Join employees with departments. Show emp_name, dept_name, and budget.
Filter result to show only employees in departments with budget > 3,000,000.

---

## Section 2 — Left Join

**Problem 4**
Left join employees with departments.
Show all employees with their dept_name (NULL if no department found).

**Problem 5**
Using a left join, find employees who belong to a department that does NOT exist in the departments table.
*(Hint: filter for isNull on dept_name after left join)*

**Problem 6**
Left join employees with bonuses.
Show all employees with their bonus_amount (NULL if no bonus).
How many employees received no bonus?

---

## Section 3 — Right Join

**Problem 7**
Right join employees with departments.
Show all departments with their employee names (NULL if no employee).

**Problem 8**
Using a right join, find departments that have NO employees assigned.

---

## Section 4 — Full Outer Join

**Problem 9**
Full outer join employees and departments.
Show dept_id, dept_name, emp_id, emp_name. How many total rows?

**Problem 10**
From the full outer join result, find all records where either the employee has no department OR the department has no employees.

---

## Section 5 — Semi and Anti Join

**Problem 11**
Use a left semi join to find all employees who received a bonus.
Show only employee columns (no bonus columns).

**Problem 12**
Use a left anti join to find all employees who did NOT receive a bonus.

**Problem 13**
Use a left anti join to find all bonus records for emp_ids that do NOT exist in the employees table.
*(Hint: flip the join — bonus anti join employees)*

---

## Section 6 — Duplicate Columns

**Problem 14**
Join employees and departments using an expression (`emp["dept_id"] == dept["dept_id"]`).
Print the schema. Notice the duplicate dept_id columns.
Fix it by selecting emp["dept_id"] explicitly with an alias.

---

## Section 7 — Union

**Problem 15**
Union employees (2024) with employees_2023 using `union()`.
What is the total row count? How many emp_ids appear in both years?

**Problem 16**
Union both employee datasets using `unionByName()` after reordering columns in employees_2023.
Confirm the result is the same.

**Problem 17**
After unioning both datasets, use `groupBy("emp_id").count()` to find employees who appear more than once.
Are their salaries the same in both years? Why or why not?

**Problem 18**
Add a column `data_year` (2024 for current, 2023 for old) to each DataFrame BEFORE unioning.
Then union them and show the result. This helps track which year each row came from.

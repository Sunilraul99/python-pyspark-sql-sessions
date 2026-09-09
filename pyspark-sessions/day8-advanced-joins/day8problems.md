# Day 8 Problems — Advanced Joins

Use datasets from `data/`: `employees.csv`, `departments.csv`, `locations.csv`, `projects.csv`.

**Key column name differences (intentional):**
- `employees.dept_code` ↔ `departments.department_id`
- `employees.location_id` ↔ `locations.loc_id`
- `employees.emp_id` ↔ `projects.lead_emp_id`
- `employees.manager_id` ↔ `employees.emp_id` (self join)

---

## Section 1 — Expression Join (different column names)

**Problem 1**
Join employees with departments using an expression join.
Show emp_id, emp_name, dept_code, department_name.
Which employees have NO matching department? *(left join + isNull)*

**Problem 2**
Join employees with locations using an expression join (`location_id == loc_id`).
Show emp_name, city, state. How many employees are in Maharashtra?

**Problem 3**
Join projects with departments using dept_code.
Show project_name, department_name, budget. Sort by budget descending.

---

## Section 2 — Handling Duplicate Columns

**Problem 4**
Join employees and departments using an expression.
Print the schema — notice both `dept_code` and `department_id` appear.
Fix it using `.drop()` to remove `department_id` from the result.

**Problem 5**
Do the same join but fix the duplicate using `.select()` with `col("emp.dept_code")` alias syntax.

**Problem 6**
Fix the duplicate by renaming `department_id` to `dept_code` in the departments DataFrame BEFORE joining, then use `on="dept_code"`.

---

## Section 3 — Multiple Join Conditions

**Problem 7**
Join employees with projects where:
- `emp.dept_code == proj.dept_code` AND
- `proj.status == "active"`

Show emp_name, department, project_name. Only active projects from the employee's own department.

**Problem 8**
Join employees with projects where:
- `emp.emp_id == proj.lead_emp_id` AND
- `proj.budget > 500000`

Show the project lead's name, project name, and budget.

---

## Section 4 — Join on Transformed Columns

**Problem 9**
Create this dirty DataFrame:
```python
dirty = [(" DC01 ", "Amit"), ("dc03", "Ravi"), ("DC99", "Ghost")]
```
Join with departments using `lower(trim(dept_key)) == lower(department_id)`.
Show which employees matched and which didn't. *(left join)*

**Problem 10**
Given city values with inconsistent casing (`"mumbai"`, `"DELHI"`, `"Bangalore"`),
join with locations using `lower(city_col) == lower(loc.city)`.

---

## Section 5 — Self Join

**Problem 11**
Write a self join to show each employee with their manager's name.
Employees with no manager should show `NULL` for manager name.

**Problem 12**
From the self join result, find:
- All employees who report to `"Arjun Nair"`
- Employees with no manager (top-level)

**Problem 13**
Extend the self join: show employee name, manager name, and salary difference
(`manager_salary - emp_salary`). Who earns more than their manager?

---

## Section 6 — Three-Table Join

**Problem 14**
Join employees + departments + locations (three tables, all expression joins).
Show emp_name, department_name, city, state, salary.

**Problem 15**
From the three-table join, filter employees from "Maharashtra" state.
Group by department_name and show count and average salary.

**Problem 16**
Three-table join: employees + departments + projects.
Show each employee's name, department, and the project they lead (if any).
Employees with no project should still appear.

---

## Section 7 — Four-Table Join

**Problem 17**
Join all four tables: employees + departments + locations + projects.
Show: emp_name, department, city, project_name, project_budget.
Filter: only active projects.

**Problem 18**
From the four-table join, find departments where:
- Total project budget > 1,000,000
- All projects are active

---

## Section 8 — Broadcast Join

**Problem 19**
Rewrite the three-table join from Problem 14 using `broadcast()` on both
`departments` and `locations` (they are small tables).
The result should be identical — confirm row counts match.

**Problem 20**
Write a self join + department join + broadcast hint in a single chain.
Show: employee, manager, department_name, city.
Use broadcast on departments and locations.

---

## Section 9 — Real-World Patterns

**Problem 21**
After a left join of employees with departments, use `coalesce()` to fill:
- `department_name` → `"Unassigned"` if NULL
- `annual_budget` → `0` if NULL

**Problem 22**
Join employees + departments, then `groupBy` department to compute:
- headcount per department
- total salary cost per department
- average salary per department
Sort by total salary cost descending.

**Problem 23**
Full chain: self join (employee+manager) + departments + locations + broadcast hints.
Produce a complete employee directory:
`emp_id | employee | manager | department | city | salary`

**Problem 24**
Join employees to projects, then join that result to departments.
Find the department with the highest total project budget across all its active projects.

# Day 9 Problems — Window Functions

Use `sales.csv` from `data/` for all problems.

---

## Section 1 — Window Basics

**Problem 1**
Without using `groupBy`, add a column `region_total` showing total revenue of each row's region.
Every row in "North" should show the same total. *(partitionBy + rowsBetween full)*

**Problem 2**
Add a column `salesperson_total` showing total revenue per salesperson alongside each row.
Show sale_id, salesperson, amount, salesperson_total.

**Problem 3**
Compare: run `groupBy("region").agg(sum("amount"))` vs the window version from Problem 1.
How many rows does each produce? Why?

---

## Section 2 — Frame Boundaries

**Problem 4**
Add a `running_total` column — cumulative sum of amount per region ordered by sale_date.
*(unboundedPreceding → currentRow)*

**Problem 5**
Add a `remaining_total` column — sum from current row to end of region partition.
*(currentRow → unboundedFollowing)*

**Problem 6**
Add a `rolling_avg_3` column — average of current row + 2 previous rows per region.
*(rowsBetween(-2, currentRow))*

**Problem 7**
Add a `centred_avg` column — average of 1 row before + current + 1 row after per region.
*(rowsBetween(-1, 1))*

**Problem 8**
Add both `running_total` and `region_total` columns, then calculate `pct_accumulated` —
what % of the region's total revenue has been accumulated up to this row.
Filter to show only North region, ordered by sale_date.

**Problem 9**
Add a `running_min` and `running_max` column per salesperson ordered by sale_date.
As rows accumulate, min should only go down and max should only go up.

---

## Section 3 — Ranking Functions

**Problem 10**
Add `row_number`, `rank`, and `dense_rank` within each region, ordered by amount descending.
Show a case where rank and dense_rank differ from row_number (a tie exists in the data).

**Problem 11**
Find the **top 1 sale** per region (highest amount). Use `row_number()`.

**Problem 12**
Find the **top 2 sales** per category using `rank()`.
If there are ties at rank 2, include all tied rows.

**Problem 13**
Find the **bottom 1 sale** per salesperson (lowest amount) using `row_number` with `orderBy(amount.asc())`.

---

## Section 4 — lag() and lead()

**Problem 14**
For each salesperson, add a `prev_amount` column (amount from previous sale).
First sale per salesperson should show 0 (use default).

**Problem 15**
Calculate `amount_change` = current amount - previous sale amount per salesperson.
Flag rows as `"GROWTH"` if positive, `"DECLINE"` if negative, `"FIRST"` if no previous row.

**Problem 16**
Add `next_sale_amount` using `lead(1)` per salesperson ordered by sale_date.
Last sale per salesperson → NULL.

**Problem 17**
Calculate `gap_to_next` = next_amount - current_amount per salesperson.
Which salesperson has the largest single-sale improvement?

**Problem 18**
Using `lag(2)`, show the amount from 2 sales ago per salesperson.
How many rows have a non-null value for this column?

**Problem 19**
Detect where a salesperson's amount dropped compared to their previous sale AND
the previous sale also dropped compared to its previous sale (2 consecutive declines).

---

## Section 5 — first() and last()

**Problem 20**
For each salesperson, add `first_sale_amount` and `last_sale_amount` columns
showing their very first and most recent sale amounts. *(full partition frame)*

**Problem 21**
Using `last()` with a running frame, add `last_seen_amount` — the most recent
amount for each salesperson up to and including the current row.

**Problem 22**
Calculate `growth_since_first` = current amount - first sale amount per salesperson.
Which salesperson has grown the most from their first sale?

---

## Section 6 — Aggregate Window Functions

**Problem 23**
Add `running_count` — how many sales each salesperson has made up to and including
this row (ordered by sale_date). Last row per salesperson = their total sale count.

**Problem 24**
Add `running_avg` per region ordered by sale_date.
Show sale_date, amount, and running_avg side by side.

**Problem 25**
Add `5_row_rolling_sum` — sum of current + 4 previous rows per salesperson.
*(rowsBetween(-4, currentRow))*

**Problem 26**
Calculate each sale's `pct_of_salesperson_total` — what % of that salesperson's
total revenue does this single sale represent? Round to 2 decimal places.

**Problem 27**
Add `above_partition_avg` — a boolean flag (True/False) showing whether this sale's
amount is above the average amount for its region. *(partition avg = full frame)*

---

## Section 7 — Global Window

**Problem 28**
Rank ALL 36 sales globally by amount descending (no partitionBy). Show top 10.

**Problem 29**
Calculate the global running total across all rows ordered by sale_date.
The last row should equal the sum of all amounts.

---

## Section 8 — Real-World Patterns

**Problem 30**
Deduplicate the dataset — keep only the **most recent** sale per salesperson.
*(row_number + desc date + filter == 1)*

**Problem 31**
Build a monthly revenue table per salesperson (groupBy first).
Then add a `prev_month_revenue` and `mom_change` column using lag() on the result.

**Problem 32**
Find the salesperson with the **most consistent** sales — lowest standard deviation
of amount. *(Hint: use stddev() as an aggregate window function over full partition)*

**Problem 33**
Combine multiple window specs in one query:
- Rank within region (by amount desc)
- Running total per salesperson
- % of region total for each sale
Show: region, salesperson, sale_id, amount, rank_in_region, sp_running_total, pct_of_region

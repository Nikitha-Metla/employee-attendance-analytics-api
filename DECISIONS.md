# DECISIONS.md

1. **Indexes.** Employees: unique `emp_code` (identity, create race), `(department, emp_code)` for the sorted list, `(department, joined_on)` and `joined_on` for headcount. Logs: unique `(emp_code, date)` (one record a day, monthly and PATCH lookups), `(date desc, emp_code)` (list sort, month scans), `(status, date desc, emp_code)` and `(emp_code, punch_in desc)` for punch-out. I rejected an index on `late_minutes`: the analytics read a whole month anyway.
2. **Punch-in race.** Both requests compute the same date and insert. The unique `(emp_code, date)` index lets one insert win; the other gets a duplicate-key error and returns 409. There is no read before the write.
3. **Ties.** Ranking uses `$rank`, then keeps `rank <= limit`. Employees tied at the cutoff are both returned, so the list can be longer than `limit`.
4. **Headcount.** The summary starts from `employees` (joined on or before month end) and left-joins each person's logs, so someone with no logs still counts as one row.
5. **100x data.** Pre-aggregate daily or monthly totals per employee on write, and have analytics read those instead of raw logs.

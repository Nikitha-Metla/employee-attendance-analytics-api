# REVIEW.md

Defects found in the starter `app/main.py`, and what I did about each.

| # | Where | What is wrong | How you'd notice it | Fix |
|---|---|---|---|---|
| 1 | `health` | Always returns ok, never talks to MongoDB. | Stop MongoDB: `/health` still says 200, so the runner starts testing too early. | Ping MongoDB; 503 if it fails. |
| 2 | `MongoClient(...)` | No `tz_aware`, so datetimes come back naive; no server selection timeout. | `punch_out - end` raises `TypeError` (naive vs aware); requests hang when the DB is down. | `tz_aware=True`, 5 s timeout, all instants handled as aware UTC. |
| 3 | module level | No indexes at all. | `GET /attendance` on 100k docs is a COLLSCAN; "one record per day" is not enforced. | `ensure_indexes()` in the lifespan hook (idempotent), including two unique indexes. |
| 4 | `compute_late_minutes` | `replace(hour, minute)` on the punch-in's own (UTC) date and clock: shift start is treated as UTC, not IST, and overnight shifts are ignored. | Punch-in 09:35 IST (04:05 UTC) is compared with 09:30 UTC, giving a negative number, so 0. | Build shift start as IST on the attendance date (R1). |
| 5 | `compute_late_minutes` | `int(seconds/60)` then `> 10`: 09:40:30 gives 10 minutes, which is not `> 10`, so 0. R2 says strictly more than 10:00 is late. | Punch-in 09:40:01 expected 10, got 0. | Compare whole seconds `> 600`, then floor to minutes. |
| 6 | `compute_work_hours` | Python `round()` (float/banker's) instead of half-up; microseconds not truncated; `half_day` never derived. | 16218 s = 4.505 h gives 4.5 instead of 4.51, which also flips the half-day test. | `Decimal` with `ROUND_HALF_UP` on truncated seconds; `half_day` from the rounded value (R4, R5). |
| 7 | `compute_overtime` | No 30-minute minimum; no overnight handling (end date taken as the record date); naive punch_out minus aware end raises `TypeError`; never called. | Out 18:40 on a 18:30 shift reports 10 min instead of 0; overnight shift 22:00-06:00 gives large negative to 0. | Reimplemented: end on the next day for overnight, floor minutes, count only if >= 30 (R3). |
| 8 | `EmployeeIn` | No validation: any `emp_code`, bad shift strings, invalid `joined_on`, `shift_start == shift_end`, no length limits. | `POST /employees` with `shift_start: "25:99"` returns 201. | Patterns, lengths and validators; 422 on failure. |
| 9 | `create_employee` | Check-then-insert race (two parallel creates both pass the `find_one`). `created_at` is naive local `datetime.now()` and is returned as an ISO string, not epoch ms. | 20 parallel POSTs give several 201s; response `created_at` is a string. | Unique index on `emp_code` plus `DuplicateKeyError` to 409; UTC instant; ms in the response. |
| 10 | `list_employees` | `skip = page * page_size` skips the first page; `total` counts the whole collection, not the filter; no sort; no bounds on page/page_size; `if department:` ignores `?department=`. | `?page=1` omits the first 20 employees; `total` ignores the department filter. | `(page - 1) * page_size`, `count_documents(q)`, sort by `emp_code`, `Query(ge/le)`. |
| 11 | `punch_in` | Unknown employee: `emp` is `None`, `emp["shift_start"]` raises `TypeError` (500). | Punch in with an unknown `emp_code`. | 404. |
| 12 | `punch_in` | `punched_at` is accepted as float/string/seconds; `if body.punched_at` treats 0 as missing; `fromtimestamp(ms / 1000)` yields naive server-local time; no truncation to seconds. | `punched_at: 1783312500` (seconds) is accepted; result depends on the server's time zone. | Strict int in [1e11, 4.1e12] (422 otherwise); `from_ms` to aware UTC; truncate to seconds. |
| 13 | `punch_in` | `date` is the server-local date, not the IST date (R1), and ignores the overnight rule. | Punch-in 00:30 IST gets yesterday's date on a UTC server. | `attendance_date()` implements R1. |
| 14 | `punch_in` | Check-then-insert race: two simultaneous requests both pass `find_one` and both insert. | Two parallel punch-ins give two 201s (and duplicate documents). | Unique `(emp_code, date)` index; `DuplicateKeyError` to 409; no pre-read. |
| 15 | `PunchInIn.status` | Plain `str`: `ABSENT`, `LEAVE` or any text is accepted at punch-in. | `status: "banana"` returns 201. | `Literal["PRESENT","WFH","ON_DUTY"]`. |
| 16 | `punch_in` response | Leaks the internal `_id` as `id` and returns datetimes as ISO strings instead of epoch ms. | Response contains `"id"`. | `serialize_record()` with the contract's fields only. |
| 17 | `list_attendance` | Loads the whole matching set into memory, sorts and paginates in Python; sorts by `date` only (no `emp_code` tie-break). | Slow and memory-heavy at 100k docs; unstable order within a day. | Database-side sort `date desc, emp_code asc`, `skip`/`limit`, backed by indexes. |
| 18 | `list_attendance` | No validation: `page=0` gives a negative slice, huge `page_size`, malformed dates, `date_from > date_to`, unknown `status`. | `?page=0`, `?date_from=2026-02-30`. | `Query(ge/le)`, strict date parsing, `Literal` status, 422 on bad ranges. |
| 19 | `list_attendance` | Returns raw documents: `_id` leaked as `id`, ISO datetimes, `history`/`half_day` missing for legacy documents. | Legacy record has no `half_day` key. | `serialize_record()` defaults missing values (0, false, `[]`). |

## Examined and judged fine

- Endpoints are plain `def`, so FastAPI runs them in a thread pool and the blocking PyMongo calls do not block the event loop.
- `date` is stored as `YYYY-MM-DD`, so string `$gte`/`$lte`/sort comparisons order correctly.
- `load_dotenv()` does not override variables already set in the environment, so injected `MONGO_URI`/`MONGO_DB` win. I kept the localhost fallback for local development only.
- `doc.pop("_id")` after `insert_one` is the right idea (the driver mutates the dict); I replaced it with explicit serializers.

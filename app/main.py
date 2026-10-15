"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded for convenience; real environment variables win)

All code lives in this single file, in this order:
  1. configuration & indexes   2. time / rule helpers (R1-R5, R8)   3. models
  4. serialisation             5. query / pipeline builders          6. endpoints
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as Date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # does not override variables that are already set in the environment
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration & indexes
# --------------------------------------------------------------------------- #
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,  # datetimes come back as aware UTC, never naive
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: create_index with identical keys/options is a no-op."""
    global _indexes_ready
    specs = [
        (employees, [("emp_code", ASCENDING)], {"unique": True}),  # identity + create race
        (employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {}),  # list / filter / sort
        (employees, [("department", ASCENDING), ("joined_on", ASCENDING)], {}),  # headcount, trend
        (employees, [("joined_on", ASCENDING)], {}),  # headcount without department
        (logs, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True}),  # one record per day (race guard)
        (logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {}),  # list sort, month scans
        (logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {}),  # status filter
        (logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {}),  # punch-out lookup
    ]
    ok = True
    for coll, keys, opts in specs:
        try:
            coll.create_index(keys, **opts)
        except PyMongoError as exc:
            ok = False
            log.error("index %s on %s failed: %s", keys, coll.name, exc)
    _indexes_ready = ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # never block startup; /health reports readiness
        log.error("index creation failed at startup: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
def mongo_error_handler(_, exc: PyMongoError):
    log.error("mongo error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Helpers: time, rules R1-R5, rounding
# --------------------------------------------------------------------------- #
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000
PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
NON_PRESENCE = ["ABSENT", "LEAVE"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]  # rejects floats, strings, bools, seconds

DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


def unprocessable(msg: str, loc: tuple = ("body",)) -> RequestValidationError:
    """422 in FastAPI's standard body shape (detail is a list)."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg}])


def parse_date(s: str) -> Date:
    if not DATE_RE.match(s):
        raise ValueError("expected YYYY-MM-DD")
    return Date.fromisoformat(s)


def query_date(value: str, name: str, where: str = "query") -> Date:
    try:
        return parse_date(value)
    except ValueError:
        raise unprocessable(f"{name} must be a valid YYYY-MM-DD date", (where, name))


def month_bounds(month: str):
    """-> (first day, first day of next month, last day) as ISO strings."""
    if not MONTH_RE.match(month) or int(month[:4]) < 1:
        raise unprocessable("month must be YYYY-MM", ("query", "month"))
    y, m = int(month[:4]), int(month[5:])
    start = Date(y, m, 1)
    nxt = Date(y + 1, 1, 1) if m == 12 else Date(y, m + 1, 1)
    return start, nxt, nxt - timedelta(days=1)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def from_ms(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def now_ms() -> int:
    return to_ms(datetime.now(UTC))


def trunc_sec(ms: int) -> int:
    """R1: instants are truncated to whole seconds before any calculation."""
    return ms - ms % 1000


def hm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hm(shift_end) <= hm(shift_start)


def attendance_date(punch_in: datetime, emp: dict) -> Date:
    """R1: IST calendar date of the punch-in; early punches of an overnight shift belong to the previous day."""
    local = punch_in.astimezone(IST)
    d = local.date()
    if is_overnight(emp["shift_start"], emp["shift_end"]) and local.time().replace(microsecond=0) < hm(emp["shift_end"]):
        d -= timedelta(days=1)
    return d


def late_minutes_for(punch_in: datetime, d: Date, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; then whole minutes since shift_start."""
    start = datetime.combine(d, hm(shift_start), tzinfo=IST)
    secs = int((punch_in - start).total_seconds())
    return secs // 60 if secs > 600 else 0


def overtime_for(punch_out: datetime, d: Date, emp: dict) -> int:
    """R3: whole minutes from shift_end to punch_out, only if >= 30."""
    end_day = d + timedelta(days=1) if is_overnight(emp["shift_start"], emp["shift_end"]) else d
    end = datetime.combine(end_day, hm(emp["shift_end"]), tzinfo=IST)
    mins = int((punch_out - end).total_seconds()) // 60  # floor, also for negatives
    return mins if mins >= 30 else 0


def work_hours_for(punch_in: datetime, punch_out: datetime) -> Decimal:
    """R4: seconds / 3600, 2 decimals, half-up (Decimal, never float rounding)."""
    secs = int((punch_out - punch_in).total_seconds())
    return (Decimal(secs) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def derive(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], d: Date, emp: dict) -> dict:
    """All stored derived fields for a record, per R2-R5."""
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "work_hours": None,
        "late_minutes": late_minutes_for(punch_in, d, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        wh = work_hours_for(punch_in, punch_out)
        out["work_hours"] = float(wh)
        out["overtime_minutes"] = overtime_for(punch_out, d, emp)
        out["half_day"] = wh < Decimal("4.50")  # R5: uses the rounded value
    return out


def round_half_up(x, places: int) -> float:
    return float(Decimal(str(x)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def count_weekdays(a: Date, b: Date) -> int:
    """R7: Mon-Fri days in [a, b] (calendar arithmetic, at most 31 iterations)."""
    n, d = 0, a
    while d <= b:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


# --------------------------------------------------------------------------- #
# 3. Models
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM)
    shift_end: str = Field("18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        parse_date(v)
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_nulls(cls, data):
        if isinstance(data, dict):
            for k in ("status", "punch_in", "punch_out"):
                if k in data and data[k] is None:
                    raise ValueError(f"{k} may not be null")
        return data


# --------------------------------------------------------------------------- #
# 4. Serialisation (BSON -> API)
# --------------------------------------------------------------------------- #
def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc.get("joined_on"),
        "created_at": to_ms(doc.get("created_at")),
    }


def _history_value(key: str, v: Any) -> Any:
    return to_ms(v) if key in ("punch_in", "punch_out") else v


def serialize_record(doc: dict) -> dict:
    history = []
    for h in doc.get("history") or []:
        changes = {
            k: {"from": _history_value(k, c.get("from")), "to": _history_value(k, c.get("to"))}
            for k, c in (h.get("changes") or {}).items()
        }
        history.append({"at": to_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes})
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    emp.setdefault("shift_start", "09:30")
    emp.setdefault("shift_end", "18:30")
    return emp


# --------------------------------------------------------------------------- #
# 5. Query & pipeline builders (shared by endpoints and /admin/explain)
# --------------------------------------------------------------------------- #
ATTENDANCE_SORT = {"date": -1, "emp_code": 1}


def build_attendance_query(emp_code, date_from, date_to, status) -> dict:
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    df = query_date(date_from, "date_from") if date_from is not None else None
    dt = query_date(date_to, "date_to") if date_to is not None else None
    if df and dt and df > dt:
        raise unprocessable("date_from must not be after date_to", ("query", "date_from"))
    rng = {}
    if df:
        rng["$gte"] = df.isoformat()
    if dt:
        rng["$lte"] = dt.isoformat()
    if rng:
        q["date"] = rng
    if status is not None:
        q["status"] = status
    return q


# Expression fragments. `p` is a field-path prefix ("" or e.g. "logs.").
def _presence(p=""):
    return {"$in": [f"${p}status", PRESENCE]}


def _half(p=""):
    return {"$eq": [{"$ifNull": [f"${p}half_day", False]}, True]}


def _weekday(p=""):  # $dayOfWeek: 1 = Sunday ... 7 = Saturday  -> Mon-Fri = 2..6
    d = {"$dateFromString": {"dateString": f"${p}date", "format": "%Y-%m-%d"}}
    return {"$in": [{"$dayOfWeek": d}, [2, 3, 4, 5, 6]]}


def present_weight(p="", weekdays_only=True):
    """1 per present record, 0.5 for a half day, 0 otherwise (R5, R6, R7)."""
    conds = [_presence(p)] + ([_weekday(p)] if weekdays_only else [])
    return {"$cond": [{"$and": conds}, {"$cond": [_half(p), 0.5, 1]}, 0]}


def late_min(p=""):
    return {"$ifNull": [f"${p}late_minutes", 0]}


def is_late(p=""):
    return {"$gt": [late_min(p), 0]}


def one_if(cond):
    return {"$cond": [cond, 1, 0]}


def monthly_pipeline(emp_code: str, start: Date, nxt: Date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start.isoformat(), "$lt": nxt.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": present_weight()},
                "leave_days": {"$sum": one_if({"$eq": ["$status", "LEAVE"]})},
                "late_count": {"$sum": one_if(is_late())},
                "total_late_minutes": {"$sum": late_min()},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


def dept_summary_pipeline(start: Date, nxt: Date, end: Date, department: Optional[str]) -> list:
    """Starts from employees (so zero-log people are kept) and LEFT-joins each one's month of logs."""
    match: dict = {"joined_on": {"$lte": end.isoformat()}}  # R9
    if department is not None:
        match["department"] = department
    has_hours = {"$and": [_presence(), {"$isNumber": "$work_hours"}]}
    return [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start.isoformat(), "$lt": nxt.isoformat()}}},
                    {
                        "$group": {
                            "_id": None,
                            "present_days": {"$sum": present_weight()},
                            "hours_sum": {"$sum": {"$cond": [has_hours, "$work_hours", 0]}},
                            "hours_cnt": {"$sum": one_if(has_hours)},
                            "late_count": {"$sum": one_if(is_late())},
                            "late_minutes": {"$sum": late_min()},
                            "leave_count": {"$sum": one_if({"$eq": ["$status", "LEAVE"]})},
                            "on_duty_count": {"$sum": one_if({"$eq": ["$status", "ON_DUTY"]})},
                        }
                    },
                ],
                "as": "agg",
            }
        },
        {"$unwind": {"path": "$agg", "preserveNullAndEmptyArrays": True}},  # one row per employee, logs or not
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": {"$ifNull": ["$agg.present_days", 0]}},
                "hours_sum": {"$sum": {"$ifNull": ["$agg.hours_sum", 0]}},
                "hours_cnt": {"$sum": {"$ifNull": ["$agg.hours_cnt", 0]}},
                "late_count": {"$sum": {"$ifNull": ["$agg.late_count", 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$agg.late_minutes", 0]}},
                "leave_count": {"$sum": {"$ifNull": ["$agg.leave_count", 0]}},
                "on_duty_count": {"$sum": {"$ifNull": ["$agg.on_duty_count", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]


def leaderboard_pipeline(start: Date, nxt: Date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lt": nxt.isoformat()}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": late_min()}, "late_count": {"$sum": one_if(is_late())}}},
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees are dropped
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank only inside the department
    pipeline += [
        # $rank = standard competition ranking: 1, 2, 2, 4
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applied AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def trend_range(department: str, date_from: Optional[str], date_to: Optional[str]):
    d_from = query_date(date_from, "from") if date_from is not None else None
    d_to = query_date(date_to, "to") if date_to is not None else None
    if d_from is None:
        raise unprocessable("from is required", ("query", "from"))
    if d_to is None:
        raise unprocessable("to is required", ("query", "to"))
    if d_to < d_from:
        raise unprocessable("to must not be before from", ("query", "to"))
    if (d_to - d_from).days + 1 > 92:
        raise unprocessable("range must not exceed 92 days", ("query", "to"))
    return d_from, d_to


def trend_pipeline(department: str, d_from: Date, d_to: Date) -> list:
    """
    employees(department) -> $facet { join-date counts | per-day attendance sums } -> one row per calendar
    day generated with $range (gap filling, weekends included) -> $setWindowFields moving average.
    Rates are rounded half-up with integer arithmetic (Mongo's $round is half-to-even).
    """
    n_days = (d_to - d_from).days + 1
    from_dt = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)  # UTC midnight == calendar date
    f_s, t_s = d_from.isoformat(), d_to.isoformat()

    def by_day(field):  # value of `field` in the daily facet bucket for the current day string $$ds
        return {
            "$ifNull": [
                {
                    "$first": {
                        "$map": {
                            "input": {"$filter": {"input": "$daily", "as": "x", "cond": {"$eq": ["$$x._id", "$$ds"]}}},
                            "as": "y",
                            "in": f"$$y.{field}",
                        }
                    }
                },
                0,
            ]
        }

    day_row = {
        "$let": {
            "vars": {"dt": {"$dateAdd": {"startDate": from_dt, "unit": "day", "amount": "$$i"}}},
            "in": {
                "$let": {
                    "vars": {
                        "ds": {"$dateToString": {"format": "%Y-%m-%d", "date": "$$dt"}},
                        "dow": {"$dayOfWeek": "$$dt"},
                    },
                    "in": {
                        "date": "$$ds",
                        "is_working_day": {"$and": [{"$gte": ["$$dow", 2]}, {"$lte": ["$$dow", 6]}]},
                        "headcount": {
                            "$sum": {
                                "$map": {
                                    "input": {"$filter": {"input": "$joins", "as": "j", "cond": {"$lte": ["$$j._id", "$$ds"]}}},
                                    "as": "jj",
                                    "in": "$$jj.n",
                                }
                            }
                        },
                        "present_count": by_day("present"),
                        "late_count": by_day("late"),
                    },
                }
            },
        }
    }
    return [
        {"$match": {"department": department}},
        {
            "$facet": {
                "joins": [
                    {"$match": {"joined_on": {"$lte": t_s}}},
                    {"$group": {"_id": "$joined_on", "n": {"$sum": 1}}},
                ],
                "daily": [
                    {
                        "$lookup": {
                            "from": "attendance_logs",
                            "localField": "emp_code",
                            "foreignField": "emp_code",
                            "pipeline": [
                                {"$match": {"date": {"$gte": f_s, "$lte": t_s}}},
                                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
                            ],
                            "as": "logs",
                        }
                    },
                    {"$unwind": "$logs"},
                    {
                        "$group": {
                            "_id": "$logs.date",
                            "present": {"$sum": present_weight("logs.", weekdays_only=False)},
                            "late": {"$sum": one_if(is_late("logs."))},
                        }
                    },
                ],
            }
        },
        {"$project": {"_id": 0, "days": {"$map": {"input": {"$range": [0, n_days]}, "as": "i", "in": day_row}}}},
        {"$unwind": "$days"},
        {"$replaceRoot": {"newRoot": "$days"}},
        {"$sort": {"date": 1}},
        {
            "$addFields": {
                # rate in ten-thousandths, half-up: floor((20000*present + H) / (2H))
                "rate_q": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": [20000, "$present_count"]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "q_sum": {"$sum": "$rate_q", "window": {"documents": [-6, 0]}},
                    "q_cnt": {"$sum": {"$cond": [{"$isNumber": "$rate_q"}, 1, 0]}, "window": {"documents": [-6, 0]}},
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": {"$cond": [{"$isNumber": "$rate_q"}, {"$divide": ["$rate_q", 10000]}, None]},
                "moving_avg_7d": {
                    "$cond": [
                        {"$gt": ["$q_cnt", 0]},
                        {
                            "$divide": [
                                {
                                    "$floor": {
                                        "$divide": [
                                            {"$add": [{"$multiply": [2, "$q_sum"]}, "$q_cnt"]},
                                            {"$multiply": [2, "$q_cnt"]},
                                        ]
                                    }
                                },
                                10000,
                            ]
                        },
                        None,
                    ]
                },
            }
        },
    ]


# --------------------------------------------------------------------------- #
# 6. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "mongodb unavailable")
    if not _indexes_ready:
        try:
            ensure_indexes()
        except PyMongoError:
            pass
    return {"status": "ok"}


# ---- employees ------------------------------------------------------------ #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = from_ms(now_ms())
    try:
        employees.insert_one(doc)  # unique index decides the race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {} if department is None else {"department": department}
    total = employees.count_documents(q)
    cur = employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_employee(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# ---- attendance ------------------------------------------------------------ #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ms = trunc_sec(body.punched_at if body.punched_at is not None else now_ms())
    ts = from_ms(ms)
    d = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": d.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(body.status, ts, None, d, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    out_dt = from_ms(trunc_sec(body.punched_at if body.punched_at is not None else now_ms()))
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": out_dt}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    in_dt = rec["punch_in"]
    if out_dt <= in_dt or out_dt - in_dt > timedelta(hours=24):
        raise unprocessable("punched_at must be after punch_in and within 24 hours of it", ("body", "punched_at"))
    derived = derive(rec["status"], in_dt, out_dt, Date.fromisoformat(rec["date"]), emp)
    updated = logs.find_one_and_update(
        # compare-and-set: only close the record we read, and only if nobody closed/changed it meanwhile
        {"_id": rec["_id"], "punch_out": None, "punch_in": in_dt},
        {"$set": {"punch_out": out_dt, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return serialize_record(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = build_attendance_query(emp_code, date_from, date_to, status)
    total = logs.count_documents(q)
    cur = logs.find(q).sort(list(ATTENDANCE_SORT.items())).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_record(d) for d in cur], "total": total, "page": page, "page_size": page_size}


def plan_regularization(rec: dict, emp: dict, body: RegularizeIn) -> tuple[dict, dict]:
    """-> (fields to $set, changes for history). Raises 422 for rule violations."""
    old_status, old_in, old_out = rec["status"], rec.get("punch_in"), rec.get("punch_out")
    new_status = body.status or old_status
    d = Date.fromisoformat(rec["date"])

    if new_status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"punch times cannot be supplied with status {new_status}")
        new_in = new_out = None
    else:
        new_in = from_ms(trunc_sec(body.punch_in)) if body.punch_in is not None else old_in
        new_out = from_ms(trunc_sec(body.punch_out)) if body.punch_out is not None else old_out
        if new_in is None:
            raise unprocessable("a presence status requires a punch_in", ("body", "punch_in"))
        if attendance_date(new_in, emp) != d:
            raise unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        if new_out is not None and (new_out <= new_in or new_out - new_in > timedelta(hours=24)):
            raise unprocessable("punch_out must be after punch_in and within 24 hours", ("body", "punch_out"))

    if (new_status, to_ms(new_in), to_ms(new_out)) == (old_status, to_ms(old_in), to_ms(old_out)):
        raise unprocessable("the request changes nothing")

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derive(new_status, new_in, new_out, d, emp)}
    old = {
        "status": old_status,
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": int(rec.get("late_minutes") or 0),
        "overtime_minutes": int(rec.get("overtime_minutes") or 0),
        "half_day": bool(rec.get("half_day", False)),
    }
    changes = {k: {"from": old[k], "to": new[k]} for k in new if to_cmp(old[k]) != to_cmp(new[k])}
    return new, changes


def to_cmp(v):
    return to_ms(v) if isinstance(v, datetime) else v


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    query_date(date, "date", "path")
    emp = get_employee(emp_code)
    for _ in range(5):  # optimistic concurrency: retry if another edit slipped in between read and write
        rec = logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new, changes = plan_regularization(rec, emp, body)
        hist = rec.get("history")
        guard = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "history": {"$size": len(hist)} if hist is not None else None,
        }
        entry = {"at": datetime.now(UTC), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        updated = logs.find_one_and_update(
            guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
        )
        if updated is not None:
            return serialize_record(updated)
    raise HTTPException(409, "concurrent modification, please retry")


# ---- analytics --------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")):
    start, nxt, end = month_bounds(month)
    emp = get_employee(emp_code)
    rows = list(logs.aggregate(monthly_pipeline(emp_code, start, nxt)))
    agg = rows[0] if rows else {}
    try:
        joined = Date.fromisoformat(emp["joined_on"])
    except (KeyError, TypeError, ValueError):
        joined = start
    working_days = count_weekdays(max(start, joined), end)
    present = float(agg.get("present_days", 0))
    pct = round_half_up(Decimal(str(present)) * 100 / working_days, 2) if working_days else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present,
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(
    month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"), department: Optional[str] = None
):
    start, nxt, end = month_bounds(month)
    items = []
    for r in employees.aggregate(dept_summary_pipeline(start, nxt, end, department)):
        cnt = int(r["hours_cnt"])
        avg = None
        if cnt:  # mean over records; round(sum, 2) removes float noise, then exact Decimal half-up
            avg = round_half_up(Decimal(str(round(float(r["hours_sum"]), 2))) / cnt, 2)
        items.append(
            {
                "department": r["_id"],
                "headcount": int(r["headcount"]),
                "present_days": round_half_up(r["present_days"], 2),
                "avg_work_hours": avg,
                "late_count": int(r["late_count"]),
                "total_late_minutes": int(r["total_late_minutes"]),
                "leave_count": int(r["leave_count"]),
                "on_duty_count": int(r["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    start, nxt, _ = month_bounds(month)
    rows = logs.aggregate(leaderboard_pipeline(start, nxt, limit, department))
    items = [
        {
            "rank": int(r["rank"]),
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": int(r["total_late_minutes"]),
            "late_count": int(r["late_count"]),
        }
        for r in rows
    ]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, date_from: str = Query(..., alias="from"), date_to: str = Query(..., alias="to")):
    d_from, d_to = trend_range(department, date_from, date_to)
    if employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    items = []
    for r in employees.aggregate(trend_pipeline(department, d_from, d_to)):
        items.append(
            {
                "date": r["date"],
                "is_working_day": bool(r["is_working_day"]),
                "headcount": int(r["headcount"]),
                "present_count": float(r["present_count"]),
                "late_count": int(r["late_count"]),
                "attendance_rate": r.get("attendance_rate"),
                "moving_avg_7d": r.get("moving_avg_7d"),
            }
        )
    return {"department": department, "items": items}


# ---- admin: explain --------------------------------------------------------------- #
def _jsonable(doc: dict) -> dict:
    return json.loads(json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS))


def _require(value, name: str):
    if value is None:
        raise unprocessable(f"{name} is required", ("query", name))
    return value


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    # every branch builds the query with the SAME builder the real endpoint uses
    if endpoint == "attendance_list":
        q = build_attendance_query(emp_code, date_from, date_to, status)
        cmd = {"find": "attendance_logs", "filter": q, "sort": ATTENDANCE_SORT, "skip": (page - 1) * page_size, "limit": page_size}
        coll = "attendance_logs"
    else:
        if endpoint == "department_trend":
            dept = _require(department, "department")
            d_from, d_to = trend_range(dept, _require(from_, "from"), _require(to, "to"))
            cmd = {"aggregate": "employees", "pipeline": trend_pipeline(dept, d_from, d_to), "cursor": {}}
            coll = "employees"
        else:
            start, nxt, end = month_bounds(_require(month, "month"))
            if endpoint == "employee_monthly":
                cmd = {"aggregate": "attendance_logs", "pipeline": monthly_pipeline(_require(emp_code, "emp_code"), start, nxt), "cursor": {}}
                coll = "attendance_logs"
            elif endpoint == "department_summary":
                cmd = {"aggregate": "employees", "pipeline": dept_summary_pipeline(start, nxt, end, department), "cursor": {}}
                coll = "employees"
            else:  # late_leaderboard
                cmd = {"aggregate": "attendance_logs", "pipeline": leaderboard_pipeline(start, nxt, limit, department), "cursor": {}}
                coll = "attendance_logs"
    result = db.command({"explain": cmd, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": coll, "explain": _jsonable(result)}

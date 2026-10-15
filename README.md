# Employee Attendance & Analytics API

```bash
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```

Indexes are created at startup. All parts (A, B, C) are implemented in `app/main.py`.
Note: I could not run this against a live MongoDB 6/7 instance while writing it, so verify the three
aggregation pipelines (and `/admin/explain/*`) against your own data first.

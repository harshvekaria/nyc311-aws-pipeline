"""
Plans which days the ingest Lambda should fetch.

Event options:
    {"mode": "backfill"}                              -> every day from MIN_DATE to today
    {"mode": "backfill", "start": "2026-04-01", "end": "2026-04-30"}
    {} or {"mode": "incremental"}                     -> last LOOKBACK_DAYS days (default 7)

Why re-fetch the last 7 days on every incremental run? 311 records created a few
days ago are still being closed/updated. Re-pulling a short window catches those
status changes, and because each day is written to a fixed S3 key the re-run
simply overwrites (idempotent).

Returns {"dates": ["2026-04-01", ...]} for the Step Functions Map state.
"""
import datetime as dt
import os

MIN_DATE = dt.date.fromisoformat(os.environ.get("MIN_DATE", "2026-04-01"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))


def handler(event, context):
    event = event or {}
    today = dt.datetime.utcnow().date()
    mode = event.get("mode", "incremental")

    if mode == "backfill":
        start = dt.date.fromisoformat(event.get("start", MIN_DATE.isoformat()))
        end = dt.date.fromisoformat(event.get("end", today.isoformat()))
    else:
        start = today - dt.timedelta(days=LOOKBACK_DAYS)
        end = today

    start = max(start, MIN_DATE)  # never fetch before April 1, 2026
    if end < start:
        return {"dates": []}

    days = [(start + dt.timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
    return {"mode": mode, "dates": days, "count": len(days)}

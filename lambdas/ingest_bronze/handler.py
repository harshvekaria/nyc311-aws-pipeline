"""
Bronze ingestion Lambda.

Fetches ONE calendar day of NYC 311 service requests from the SODA3 query
endpoint and writes the raw rows to S3 as gzipped JSON Lines, plus a manifest.

Invocation event:
    {"date": "2026-04-01"}

Environment variables:
    BUCKET              S3 bucket of the data lake (e.g. nyc311-lake-harsh)
    APP_TOKEN_PARAM     SSM SecureString name holding the NYC Open Data app token
                        (default /nyc311/app_token)
    DATASET_ID          default erm2-nwe9
    DOMAIN              default data.cityofnewyork.us
    PAGE_SIZE           default 50000
    MIN_DATE            default 2026-04-01  (assignment rule: nothing earlier)

Only the Python standard library + boto3 (already in the Lambda runtime) are used,
so this function needs no packaging.
"""
import datetime as dt
import gzip
import io
import json
import os
import time
import urllib.error
import urllib.request

import boto3

S3 = boto3.client("s3")
SSM = boto3.client("ssm")

BUCKET = os.environ["BUCKET"]
APP_TOKEN_PARAM = os.environ.get("APP_TOKEN_PARAM", "/nyc311/app_token")
DATASET_ID = os.environ.get("DATASET_ID", "erm2-nwe9")
DOMAIN = os.environ.get("DOMAIN", "data.cityofnewyork.us")
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "50000"))
MIN_DATE = dt.date.fromisoformat(os.environ.get("MIN_DATE", "2026-04-01"))

ENDPOINT = f"https://{DOMAIN}/api/v3/views/{DATASET_ID}/query.json"
_token_cache = None


def _app_token() -> str:
    global _token_cache
    if _token_cache is None:
        _token_cache = SSM.get_parameter(Name=APP_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
    return _token_cache


def _post(body: dict, retries: int = 5) -> list:
    """POST a SoQL query to SODA3 with exponential backoff on 429/5xx."""
    data = json.dumps(body).encode("utf-8")
    for attempt in range(retries):
        req = urllib.request.Request(
            ENDPOINT,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-App-Token": _app_token(),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read())
                # SODA3 returns a JSON array of row objects
                if isinstance(payload, dict) and "data" in payload:
                    return payload["data"]
                return payload
        except urllib.error.HTTPError as e:
            msg = e.read()[:500]
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"SODA3 HTTP {e.code}: {msg!r}") from e
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    return []


def fetch_day(day: dt.date) -> list:
    start = f"{day.isoformat()}T00:00:00"
    end = f"{(day + dt.timedelta(days=1)).isoformat()}T00:00:00"
    # Stable ORDER BY is required for deterministic paging.
    soql = (
        "SELECT * "
        f"WHERE created_date >= '{start}' AND created_date < '{end}' "
        "ORDER BY unique_key"
    )
    rows, page = [], 1
    while True:
        batch = _post({
            "query": soql,
            "page": {"pageNumber": page, "pageSize": PAGE_SIZE},
            "includeSynthetic": False,
        })
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


def handler(event, context):
    day = dt.date.fromisoformat(event["date"])
    if day < MIN_DATE:
        raise ValueError(f"{day} is before the minimum allowed date {MIN_DATE}")

    started = time.time()
    rows = fetch_day(day)

    # Bronze = raw, untouched rows. One JSON object per line, gzipped.
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write((json.dumps(r, separators=(",", ":")) + "\n").encode("utf-8"))

    prefix = f"bronze/nyc311/created_date={day.isoformat()}"
    # Deterministic key -> re-running a day overwrites it (idempotent).
    S3.put_object(Bucket=BUCKET, Key=f"{prefix}/data.jsonl.gz", Body=buf.getvalue(),
                  ContentType="application/json", ContentEncoding="gzip")

    manifest = {
        "dataset_id": DATASET_ID,
        "endpoint": ENDPOINT,
        "created_date": day.isoformat(),
        "record_count": len(rows),
        "ingested_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "duration_seconds": round(time.time() - started, 2),
        "page_size": PAGE_SIZE,
    }
    S3.put_object(Bucket=BUCKET, Key=f"{prefix}/_manifest.json",
                  Body=json.dumps(manifest, indent=2).encode("utf-8"),
                  ContentType="application/json")
    print(json.dumps(manifest))
    return {"date": day.isoformat(), "record_count": len(rows)}

"""
Smoke test for the NYC 311 SODA3 endpoint (one day only, so it returns in seconds).
    export NYC_APP_TOKEN=xxxxxxxx
    python3 scripts/test_soda.py
"""
import json, os, time, urllib.request

URL = "https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/query.json"
TOKEN = os.environ["NYC_APP_TOKEN"]
DAY = "created_date >= '2026-04-01T00:00:00' AND created_date < '2026-04-02T00:00:00'"


def soql(query, page_size=5, timeout=180):
    body = json.dumps({"query": query, "page": {"pageNumber": 1, "pageSize": page_size}}).encode()
    req = urllib.request.Request(URL, data=body, method="POST",
                                 headers={"Content-Type": "application/json", "X-App-Token": TOKEN})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out = json.loads(r.read())
    print(f"  ({time.time() - t:.1f}s)")
    return out


print("Columns:", sorted(soql(f"SELECT * WHERE {DAY}", 1)[0].keys()))
print("Count for 2026-04-01:", soql(f"SELECT count(*) AS n WHERE {DAY}"))
print("Rows returned for a full day:", len(soql(f"SELECT * WHERE {DAY} ORDER BY unique_key", 50000)))

"""
Dashboard API (API Gateway HTTP API -> this Lambda -> RDS PostgreSQL).

Routes
    GET /filters     -> available districts, complaint types, agencies, date bounds
    GET /dashboard   -> every dataset the dashboard needs, for one filter selection
    GET /export      -> the matching gold_daily_fact rows as a CSV download (max 50,000 rows)
        query params for /dashboard and /export (all optional):
            districts=1,5,33        council districts
            types=Noise - Residential|Illegal Parking   (pipe-separated)
            start=2026-04-01        inclusive
            end=2026-10-31          inclusive

All filters are passed as bound parameters (never string-formatted into SQL).
Runs in the VPC; connects as the read-only role dashboard_ro.
"""
import csv
import datetime as dt
import io
import json
import os
import ssl

import pg8000.native as pg

_conn = None
CORS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Content-Type": "application/json",
    "Cache-Control": "max-age=60",
}


def conn():
    global _conn
    if _conn is None:
        _conn = pg.Connection(
            user=os.environ.get("DB_USER", "dashboard_ro"),
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", "5432")),
            database=os.environ.get("DB_NAME", "nyc311"),
            ssl_context=ssl.create_default_context(cafile=os.path.join(os.path.dirname(os.path.abspath(__file__)), "rds-ca.pem")),
            timeout=30,
        )
    return _conn


def q(sql, **params):
    """Run a query and return a list of dicts. Reconnect once if the connection went stale."""
    global _conn
    for attempt in range(2):
        try:
            c = conn()
            rows = c.run(sql, **params)
            cols = [col["name"] for col in c.columns]
            return [dict(zip(cols, r)) for r in rows]
        except (pg.InterfaceError, OSError):
            _conn = None
            if attempt:
                raise


def _json_default(o):
    if isinstance(o, (dt.date, dt.datetime)):
        return o.isoformat()
    return float(o)


def respond(status, body):
    return {"statusCode": status, "headers": CORS, "body": json.dumps(body, default=_json_default)}


EXPORT_MAX_ROWS = 50000  # keeps the response under Lambda's 6 MB limit


def export_csv(districts, types, start, end):
    """Detail rows for the current filters, as CSV text. Raises ValueError if too many rows."""
    w, p = where(districts, types)
    rows = q(f"""
        SELECT created_day, council_district, borough, complaint_type, agency,
               request_count, closed_count, open_count,
               round((sum_resolution_hours / nullif(closed_count, 0))::numeric, 2) AS avg_resolution_hours
        FROM gold.gold_daily_fact WHERE {w}
        ORDER BY created_day, council_district, complaint_type, agency
        LIMIT {EXPORT_MAX_ROWS + 1}""", start=start, end=end, **p)
    if len(rows) > EXPORT_MAX_ROWS:
        raise ValueError(f"More than {EXPORT_MAX_ROWS:,} rows match. Narrow the date range, "
                         "districts or complaint types and try again.")
    buf = io.StringIO()
    cols = ["created_day", "council_district", "borough", "complaint_type", "agency",
            "request_count", "closed_count", "open_count", "avg_resolution_hours"]
    writer = csv.DictWriter(buf, fieldnames=cols)
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if r[k] is None else r[k]) for k in cols})
    return buf.getvalue(), len(rows)


def parse_filters(qs):
    qs = qs or {}
    districts = [int(x) for x in qs.get("districts", "").split(",") if x.strip().isdigit()]
    types = [t.strip() for t in qs.get("types", "").split("|") if t.strip()]
    start = dt.date.fromisoformat(qs["start"]) if qs.get("start") else dt.date(2026, 4, 1)
    end = dt.date.fromisoformat(qs["end"]) if qs.get("end") else dt.date.today()
    return districts, types, start, end


def where(districts, types, include_district=True, include_type=True):
    """Builds a WHERE clause from fixed fragments + bound params only."""
    parts, params = ["created_day BETWEEN :start AND :end"], {}
    if include_district and districts:
        parts.append("council_district = ANY(CAST(:districts AS smallint[]))")
        params["districts"] = districts
    if include_type and types:
        parts.append("complaint_type = ANY(CAST(:ctypes AS text[]))")
        params["ctypes"] = types
    return " AND ".join(parts), params


def filters():
    return {
        "districts": q("""SELECT council_district AS id, coalesce(max(borough) FILTER (WHERE borough <> 'UNSPECIFIED'), 'UNSPECIFIED') AS borough, sum(request_count) AS n
                          FROM gold.gold_daily_fact WHERE council_district > 0
                          GROUP BY 1 ORDER BY 1"""),
        "complaint_types": q("""SELECT complaint_type AS name, sum(request_count) AS n
                                FROM gold.gold_daily_fact GROUP BY 1 ORDER BY 2 DESC"""),
        "date_bounds": q("SELECT min(created_day) AS min, max(created_day) AS max FROM gold.gold_daily_fact")[0],
        "last_loaded": q("SELECT max(loaded_at) AS ts FROM gold.load_audit")[0]["ts"],
    }


def dashboard(districts, types, start, end):
    base = {"start": start, "end": end}
    w_all, p_all = where(districts, types)
    w_city, p_city = where(districts, types, include_district=False)   # citywide benchmark

    kpis = q(f"""
        SELECT sum(request_count) AS total, sum(closed_count) AS closed, sum(open_count) AS open,
               round((sum(sum_resolution_hours) / nullif(sum(closed_count), 0))::numeric, 1) AS avg_resolution_hours
        FROM gold.gold_daily_fact WHERE {w_all}""", **base, **p_all)[0]

    # Per-district stats with the same date/type filter for ALL districts:
    # feeds the map, the district bar chart and the bottleneck chart.
    by_district = q(f"""
        WITH d AS (
            SELECT council_district, coalesce(max(borough) FILTER (WHERE borough <> 'UNSPECIFIED'), 'UNSPECIFIED') AS borough,
                   sum(request_count) AS total, sum(closed_count) AS closed, sum(open_count) AS open,
                   sum(sum_resolution_hours) / nullif(sum(closed_count), 0) AS avg_hours
            FROM gold.gold_daily_fact
            WHERE {w_city} AND council_district > 0
            GROUP BY 1
        ), city AS (
            SELECT sum(open)::float / nullif(sum(closed), 0) AS city_ratio FROM d
        )
        SELECT d.*, round(avg_hours::numeric, 1) AS avg_resolution_hours,
               round((open::float / nullif(closed, 0))::numeric, 4) AS open_to_closed_ratio,
               round(city.city_ratio::numeric, 4) AS city_ratio,
               (open::float / nullif(closed, 0)) > city.city_ratio AS is_bottleneck
        FROM d CROSS JOIN city ORDER BY council_district""", **base, **p_city)
    for r in by_district:
        r.pop("avg_hours", None)

    # Top complaint types in the selection vs their share citywide (same dates).
    w_sel, p_sel = where(districts, [], include_type=False)
    w_cw, p_cw = where([], [], include_district=False, include_type=False)
    complaints = q(f"""
        WITH sel AS (SELECT complaint_type, sum(request_count) AS n FROM gold.gold_daily_fact
                     WHERE {w_sel} GROUP BY 1),
             cw  AS (SELECT complaint_type, sum(request_count) AS n FROM gold.gold_daily_fact
                     WHERE {w_cw} GROUP BY 1)
        SELECT sel.complaint_type, sel.n AS selection_count,
               round((100.0 * sel.n / sum(sel.n) OVER ())::numeric, 2) AS selection_share_pct,
               round((100.0 * cw.n / (SELECT sum(n) FROM cw))::numeric, 2) AS citywide_share_pct
        FROM sel JOIN cw USING (complaint_type)
        ORDER BY sel.n DESC LIMIT 15""", **base, **p_sel, **p_cw)

    agencies = q(f"""
        SELECT agency, sum(request_count) AS total, sum(closed_count) AS closed,
               round((sum(sum_resolution_hours) / nullif(sum(closed_count), 0))::numeric, 1) AS avg_resolution_hours
        FROM gold.gold_daily_fact WHERE {w_all}
        GROUP BY 1 HAVING sum(closed_count) >= 20
        ORDER BY avg_resolution_hours DESC NULLS LAST""", **base, **p_all)

    daily = q(f"""
        SELECT created_day AS day, sum(request_count) AS n, sum(closed_count) AS closed
        FROM gold.gold_daily_fact WHERE {w_all} GROUP BY 1 ORDER BY 1""", **base, **p_all)

    weekly = q(f"""
        SELECT date_trunc('week', created_day)::date AS week, sum(request_count) AS n
        FROM gold.gold_daily_fact WHERE {w_all} GROUP BY 1 ORDER BY 1""", **base, **p_all)

    return {
        "filters": {"districts": districts, "types": types, "start": start, "end": end},
        "kpis": kpis, "by_district": by_district, "complaints": complaints,
        "agencies": agencies, "daily": daily, "weekly": weekly,
    }


def handler(event, context):
    path = event.get("rawPath") or event.get("path", "")
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method == "OPTIONS":
        return respond(204, {})
    try:
        if path.endswith("/filters"):
            return respond(200, filters())
        if path.endswith("/dashboard"):
            return respond(200, dashboard(*parse_filters(event.get("queryStringParameters"))))
        if path.endswith("/export"):
            body, n = export_csv(*parse_filters(event.get("queryStringParameters")))
            headers = {**CORS, "Content-Type": "text/csv; charset=utf-8", "X-Row-Count": str(n),
                       "Access-Control-Expose-Headers": "X-Row-Count",
                       "Content-Disposition": 'attachment; filename="nyc311_export.csv"'}
            return {"statusCode": 200, "headers": headers, "body": body}
        return respond(404, {"error": "not found"})
    except ValueError as e:
        return respond(400, {"error": str(e)})
    except Exception as e:  # noqa: BLE001
        print("ERROR", repr(e))
        return respond(500, {"error": "internal error"})

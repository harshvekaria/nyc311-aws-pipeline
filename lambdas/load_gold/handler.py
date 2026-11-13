"""
Loads Gold CSVs from S3 into RDS PostgreSQL.

Runs INSIDE the VPC (same as RDS) and reaches S3 through the free S3 gateway
endpoint, so no NAT gateway is needed.

For every table it:
    1. finds the single part-*.csv the Gold Glue job wrote
    2. in ONE transaction: TRUNCATE + COPY FROM STDIN
       -> the dashboard never sees a half-loaded table
    3. records the row count in gold.load_audit

Environment variables:
    BUCKET, DB_HOST, DB_NAME (default nyc311), DB_USER, DB_PASSWORD, DB_PORT (5432)
    RO_PASSWORD   password to set for the read-only dashboard_ro role
Packaging: needs the pg8000 layer (see scripts/build_layer.sh).
"""
import io
import json
import os
import ssl
import pathlib

import boto3
import pg8000.native as pg

S3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
TABLES = [
    "gold_daily_fact",
    "gold_district_performance",
    "gold_complaint_distribution",
    "gold_agency_efficiency",
    "gold_temporal_trends",
    "gold_bottlenecks",
]
SCHEMA_SQL = (pathlib.Path(__file__).parent / "postgres_schema.sql").read_text()


def connect():
    return pg.Connection(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "nyc311"),
        ssl_context=ssl.create_default_context(cafile=os.path.join(os.path.dirname(os.path.abspath(__file__)), "rds-ca.pem")),
        timeout=60,
    )


def latest_csv_key(table: str) -> str:
    prefix = f"gold/csv/{table}/"
    resp = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".csv")]
    if len(keys) != 1:
        raise RuntimeError(f"Expected exactly 1 CSV under {prefix}, found {len(keys)}")
    return keys[0]


def handler(event, context):
    conn = connect()
    try:
        conn.run(SCHEMA_SQL)  # idempotent DDL
        if os.environ.get("RO_PASSWORD"):
            conn.run(f"ALTER ROLE dashboard_ro PASSWORD {pg.literal(os.environ['RO_PASSWORD'])}")

        results = {}
        for table in TABLES:
            key = latest_csv_key(table)
            body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            header = body.split(b"\n", 1)[0].decode("utf-8").strip()
            cols = ", ".join(pg.identifier(c) for c in header.split(","))

            conn.run("BEGIN")
            try:
                conn.run(f"TRUNCATE gold.{table}")
                conn.run(f"COPY gold.{table} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                         stream=io.BytesIO(body))
                n = conn.run(f"SELECT count(*) FROM gold.{table}")[0][0]
                conn.run("INSERT INTO gold.load_audit (table_name, row_count) VALUES (:t, :n)", t=table, n=n)
                conn.run("COMMIT")
            except Exception:
                conn.run("ROLLBACK")
                raise
            results[table] = n
        conn.run("ANALYZE")  # refresh planner statistics after a full reload
        print(json.dumps(results))
        return {"loaded": results}
    finally:
        conn.close()

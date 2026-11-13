-- Serving layer schema for RDS PostgreSQL.
-- Applied automatically (idempotently) by the nyc311-load-gold Lambda.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.gold_daily_fact (
    created_day           date        NOT NULL,
    council_district      smallint    NOT NULL,   -- 0 = unknown
    borough               text,
    complaint_type        text        NOT NULL,
    agency                text        NOT NULL,
    request_count         integer     NOT NULL,
    closed_count          integer     NOT NULL,
    open_count            integer     NOT NULL,
    sum_resolution_hours  double precision
);
-- Every dashboard query filters by a date range, optionally district/type.
CREATE INDEX IF NOT EXISTS ix_fact_day           ON gold.gold_daily_fact (created_day);
CREATE INDEX IF NOT EXISTS ix_fact_district_day  ON gold.gold_daily_fact (council_district, created_day);
CREATE INDEX IF NOT EXISTS ix_fact_type_day      ON gold.gold_daily_fact (complaint_type, created_day);
CREATE INDEX IF NOT EXISTS ix_fact_agency        ON gold.gold_daily_fact (agency);

CREATE TABLE IF NOT EXISTS gold.gold_district_performance (
    council_district        smallint PRIMARY KEY,
    borough                 text,
    total_requests          integer,
    closed_requests         integer,
    open_requests           integer,
    avg_resolution_hours    double precision,
    median_resolution_hours double precision,
    p90_resolution_hours    double precision,
    pct_closed              double precision
);

CREATE TABLE IF NOT EXISTS gold.gold_complaint_distribution (
    complaint_type      text,
    council_district    smallint,
    district_count      integer,
    district_share_pct  double precision,
    district_rank       integer,
    citywide_count      integer,
    citywide_share_pct  double precision,
    citywide_rank       integer,
    over_index          double precision,
    PRIMARY KEY (council_district, complaint_type)
);
CREATE INDEX IF NOT EXISTS ix_cd_rank ON gold.gold_complaint_distribution (council_district, district_rank);

CREATE TABLE IF NOT EXISTS gold.gold_agency_efficiency (
    agency                  text PRIMARY KEY,
    agency_name             text,
    total_requests          integer,
    closed_requests         integer,
    avg_resolution_hours    double precision,
    median_resolution_hours double precision,
    p90_resolution_hours    double precision,
    pct_closed_within_24h   double precision,
    pct_closed              double precision
);

CREATE TABLE IF NOT EXISTS gold.gold_temporal_trends (
    period_start    date,
    request_count   integer,
    grain           text,
    rolling_7d_avg  double precision,
    is_top5_peak    boolean,
    PRIMARY KEY (grain, period_start)
);

CREATE TABLE IF NOT EXISTS gold.gold_bottlenecks (
    council_district           smallint PRIMARY KEY,
    borough                    text,
    open_requests              integer,
    closed_requests            integer,
    total_requests             integer,
    open_to_closed_ratio       double precision,
    city_open_to_closed_ratio  double precision,
    ratio_vs_city              double precision,
    is_bottleneck              boolean
);

-- Bookkeeping: when was each table last refreshed?
CREATE TABLE IF NOT EXISTS gold.load_audit (
    table_name  text,
    row_count   integer,
    loaded_at   timestamptz DEFAULT now()
);

-- Read-only role for the API Lambda (least privilege).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
        CREATE ROLE dashboard_ro LOGIN;  -- password set by the loader Lambda from RO_PASSWORD
    END IF;
END $$;
GRANT USAGE ON SCHEMA gold TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO dashboard_ro;

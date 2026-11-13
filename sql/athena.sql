-- Athena: query the lake directly. Run in the Athena query editor (workgroup primary,
-- query result location s3://nyc311-lake-<you>/athena-results/).  Replace <you>.

CREATE DATABASE IF NOT EXISTS nyc311;

-- Silver, with PARTITION PROJECTION: Athena computes partitions from the
-- template, so no crawler or MSCK REPAIR is ever needed as months are added.
CREATE EXTERNAL TABLE IF NOT EXISTS nyc311.silver_requests (
  unique_key bigint, created_ts timestamp, closed_ts timestamp, resolution_updated_ts timestamp,
  agency_name string, complaint_type string, descriptor string, location_type string,
  incident_zip string, city string, status string,
  resolution_description string, community_board string, council_district int,
  police_precinct int, borough string, channel string, latitude double, longitude double,
  agency string, dq_bad_district boolean, dq_bad_coords boolean, dq_closed_before_created boolean,
  dq_status_mismatch boolean, is_closed boolean, resolution_hours double, dq_zero_resolution boolean,
  created_date date, created_week date, created_hour int, created_dow string, `_silver_loaded_at` string
)
PARTITIONED BY (created_year int, created_month string)
STORED AS PARQUET
LOCATION 's3://nyc311-lake-<you>/silver/nyc311/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.created_year.type'='integer', 'projection.created_year.range'='2026,2030',
  'projection.created_month.type'='enum',
  'projection.created_month.values'='01,02,03,04,05,06,07,08,09,10,11,12',
  'storage.location.template'='s3://nyc311-lake-<you>/silver/nyc311/created_year=${created_year}/created_month=${created_month}/'
);

-- 1. Volume per month (partition pruning: only the scanned months are read)
SELECT created_year, created_month, count(*) AS requests
FROM nyc311.silver_requests GROUP BY 1, 2 ORDER BY 1, 2;

-- 2. Prove the temporal rule: nothing before 2026-04-01
SELECT min(created_ts) AS earliest, max(created_ts) AS latest FROM nyc311.silver_requests;

-- 3. Prove uniqueness after dedupe (should return 0 rows)
SELECT unique_key, count(*) FROM nyc311.silver_requests GROUP BY 1 HAVING count(*) > 1 LIMIT 10;

-- 4. Data-quality flag counts
SELECT sum(cast(dq_bad_district AS int)) AS bad_district,
       sum(cast(dq_bad_coords AS int)) AS bad_coords,
       sum(cast(dq_closed_before_created AS int)) AS closed_before_created,
       sum(cast(dq_status_mismatch AS int)) AS status_mismatch,
       count_if(council_district IS NULL) AS no_district
FROM nyc311.silver_requests;

-- 5. Standardisation check: no two labels that differ only by case/spacing
SELECT upper(trim(complaint_type)) k, count(DISTINCT complaint_type) variants
FROM nyc311.silver_requests GROUP BY 1 HAVING count(DISTINCT complaint_type) > 1;

-- 6. Partition pruning demo: compare "Data scanned" of these two queries
SELECT count(*) FROM nyc311.silver_requests WHERE created_year = 2026 AND created_month = '06';
SELECT count(*) FROM nyc311.silver_requests WHERE created_date BETWEEN date '2026-06-01' AND date '2026-06-30';

-- 7. Reconcile Gold to Silver (the totals must match)
CREATE EXTERNAL TABLE IF NOT EXISTS nyc311.gold_daily_fact (
  created_day date, council_district int, borough string, complaint_type string, agency string,
  request_count bigint, closed_count bigint, open_count bigint, sum_resolution_hours double)
STORED AS PARQUET LOCATION 's3://nyc311-lake-<you>/gold/parquet/gold_daily_fact/';
SELECT (SELECT sum(request_count) FROM nyc311.gold_daily_fact) AS gold_total,
       (SELECT count(*) FROM nyc311.silver_requests) AS silver_total;

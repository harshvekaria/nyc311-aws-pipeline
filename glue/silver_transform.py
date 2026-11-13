"""
Glue 5.0 (Spark 3.5) job: Bronze JSON Lines -> Silver partitioned Parquet.

Job parameters (Glue console -> Job details -> Advanced -> Job parameters):
    --BUCKET        nyc311-lake-<you>
    --MIN_DATE      2026-04-01   (optional)

Outputs:
    s3://BUCKET/silver/nyc311/created_year=YYYY/created_month=MM/*.parquet
    s3://BUCKET/quarantine/nyc311/run_ts=.../*.parquet   (rows that failed hard rules)
    s3://BUCKET/dq_reports/silver/run_ts=.../dq_report.json

Run locally for testing:
    spark-submit glue/silver_transform.py --local --BUCKET ./lake
"""
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------- arguments
LOCAL = "--local" in sys.argv


def _arg(name, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default


if LOCAL:
    BUCKET = _arg("BUCKET", "./lake")
    ROOT = BUCKET.rstrip("/")
    spark = SparkSession.builder.appName("nyc311-silver-local").getOrCreate()
    job = None
else:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
    BUCKET = args["BUCKET"]
    ROOT = f"s3://{BUCKET}"
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

MIN_DATE = _arg("MIN_DATE", "2026-04-01")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BRONZE = f"{ROOT}/bronze/nyc311/"
SILVER = f"{ROOT}/silver/nyc311/"
QUARANTINE = f"{ROOT}/quarantine/nyc311/run_ts={RUN_TS}/"
DQ_REPORT = f"{ROOT}/dq_reports/silver/run_ts={RUN_TS}/"

# ---------------------------------------------------------------- 1. schema
# Read everything as string first: Bronze is raw, and the API returns all
# values as JSON strings. We then cast explicitly so the Silver schema is
# identical on every run, even if a day's file is missing a column.
EXPECTED_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip",
    "incident_address", "city", "status", "resolution_description",
    "resolution_action_updated_date", "community_board", "council_district",
    "police_precinct", "borough", "open_data_channel_type", "latitude", "longitude",
]
raw_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in EXPECTED_COLUMNS])

raw = (
    spark.read.schema(raw_schema)
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.jsonl.gz")
    .json(BRONZE)
    .withColumn("_source_file", F.input_file_name())
)
raw_count = raw.count()

# ---------------------------------------------------------------- 2. helpers
def clean_str(col):
    """trim, collapse internal whitespace, empty/'N/A'/'UNSPECIFIED' -> null"""
    c = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(F.upper(c).isin("", "N/A", "NA", "NULL", "NONE", "UNSPECIFIED", "UNKNOWN"), None).otherwise(c)


def to_ts(col):
    # SODA floating timestamps look like 2026-04-01T08:15:00.000
    return F.coalesce(
        F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ss"),
    )


# Agency acronyms are already consistent; we normalise case and map the few
# known aliases so the dashboard never shows the same agency twice.
AGENCY_ALIASES = {"NYPD ": "NYPD", "DEPT OF TRANSPORTATION": "DOT", "DEPARTMENT OF TRANSPORTATION": "DOT"}
agency_map = F.create_map(*[F.lit(x) for kv in AGENCY_ALIASES.items() for x in kv])

# ---------------------------------------------------------------- 3. typed + standardised
typed = (
    raw.select(
        F.col("unique_key").cast("long").alias("unique_key"),
        to_ts("created_date").alias("created_ts"),
        to_ts("closed_date").alias("closed_ts"),
        to_ts("resolution_action_updated_date").alias("resolution_updated_ts"),
        F.upper(clean_str("agency")).alias("agency_raw"),
        clean_str("agency_name").alias("agency_name"),
        clean_str("complaint_type").alias("complaint_type"),
        clean_str("descriptor").alias("descriptor"),
        clean_str("location_type").alias("location_type"),
        F.regexp_extract(F.col("incident_zip"), r"(\d{5})", 1).alias("incident_zip"),
        F.upper(clean_str("city")).alias("city"),
        F.initcap(F.lower(clean_str("status"))).alias("status"),
        clean_str("resolution_description").alias("resolution_description"),
        clean_str("community_board").alias("community_board"),
        F.regexp_extract(F.col("council_district"), r"(\d+)", 1).cast("int").alias("council_district"),
        F.regexp_extract(F.col("police_precinct"), r"(\d+)", 1).cast("int").alias("police_precinct"),
        F.upper(clean_str("borough")).alias("borough"),
        F.upper(clean_str("open_data_channel_type")).alias("channel"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        "_source_file",
    )
    .withColumn("agency", F.coalesce(agency_map[F.col("agency_raw")], F.col("agency_raw")))
    .drop("agency_raw")
    .withColumn("incident_zip", F.when(F.col("incident_zip") == "", None).otherwise(F.col("incident_zip")))
)

# Complaint-type standardisation: "NOISE - RESIDENTIAL", "Noise - Residential "
# and "noise - residential" are the same category. Group on an upper-cased key
# and publish the most frequent original spelling as the canonical label, so
# labels stay readable ("HEAT/HOT WATER" doesn't become "Heat/hot Water").
typed = typed.withColumn("_ct_key", F.upper("complaint_type"))
label_counts = typed.filter(F.col("_ct_key").isNotNull()).groupBy("_ct_key", "complaint_type").count()
canonical = (
    label_counts
    .withColumn("_r", F.row_number().over(Window.partitionBy("_ct_key").orderBy(F.desc("count"), "complaint_type")))
    .filter("_r = 1")
    .select("_ct_key", F.col("complaint_type").alias("_ct_label"))
)
typed = (typed.join(F.broadcast(canonical), "_ct_key", "left")
         .withColumn("complaint_type", F.col("_ct_label"))
         .drop("_ct_key", "_ct_label"))

# ---------------------------------------------------------------- 4. validation rules
# HARD rules -> row goes to quarantine. SOFT rules -> value is nulled + flagged.
hard_fail = (
    F.when(F.col("unique_key").isNull(), "missing_unique_key")
    .when(F.col("created_ts").isNull(), "missing_or_bad_created_date")
    .when(F.col("created_ts") < F.lit(MIN_DATE).cast("timestamp"), "created_before_min_date")
    .when(F.col("complaint_type").isNull(), "missing_complaint_type")
    .when(F.col("agency").isNull(), "missing_agency")
)
typed = typed.withColumn("_reject_reason", hard_fail)

quarantine = typed.filter(F.col("_reject_reason").isNotNull())
valid = typed.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")

# Dedupe: the same unique_key can appear twice if a day was re-ingested or
# (rarely) the API returns a record on two pages. Keep the most recently updated.
w = Window.partitionBy("unique_key").orderBy(
    F.col("resolution_updated_ts").desc_nulls_last(), F.col("closed_ts").desc_nulls_last(), F.col("_source_file").desc()
)
before_dedupe = valid.count()
valid = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
after_dedupe = valid.count()

NYC_LAT = (40.47, 40.93)
NYC_LON = (-74.27, -73.68)
silver = (
    valid
    # soft: council districts are 1..51
    .withColumn("dq_bad_district", F.col("council_district").isNotNull() & ~F.col("council_district").between(1, 51))
    .withColumn("council_district", F.when(F.col("dq_bad_district"), None).otherwise(F.col("council_district")))
    # soft: coordinates must fall inside the NYC bounding box
    .withColumn("dq_bad_coords", F.col("latitude").isNotNull() & ~(
        F.col("latitude").between(*NYC_LAT) & F.col("longitude").between(*NYC_LON)))
    .withColumn("latitude", F.when(F.col("dq_bad_coords"), None).otherwise(F.col("latitude")))
    .withColumn("longitude", F.when(F.col("dq_bad_coords"), None).otherwise(F.col("longitude")))
    # soft: a closed date earlier than the created date is impossible
    .withColumn("dq_closed_before_created", F.col("closed_ts").isNotNull() & (F.col("closed_ts") < F.col("created_ts")))
    # soft: status says Closed but no closed date (or vice versa)
    .withColumn("dq_status_mismatch",
                ((F.col("status") == "Closed") & F.col("closed_ts").isNull())
                | ((F.col("status") != "Closed") & F.col("closed_ts").isNotNull()))
    .withColumn("borough", F.coalesce(F.col("borough"), F.lit("UNSPECIFIED")))
    .withColumn("status", F.coalesce(F.col("status"), F.lit("Unknown")))
)

# ---------------------------------------------------------------- 5. derived metrics
silver = (
    silver
    # A case counts as closed when it has a valid closed timestamp.
    .withColumn("is_closed", F.col("closed_ts").isNotNull() & ~F.col("dq_closed_before_created"))
    .withColumn(
        "resolution_hours",
        F.when(F.col("is_closed"),
               F.round((F.unix_timestamp("closed_ts") - F.unix_timestamp("created_ts")) / 3600.0, 2)),
    )
    # Records closed in the same second they were opened are usually
    # auto-closed duplicates/referrals; keep them but flag them.
    .withColumn("dq_zero_resolution", F.col("resolution_hours") == 0)
    .withColumn("created_date", F.to_date("created_ts"))
    .withColumn("created_week", F.date_trunc("week", F.col("created_ts")).cast("date"))
    .withColumn("created_hour", F.hour("created_ts"))
    .withColumn("created_dow", F.date_format("created_ts", "E"))
    .withColumn("created_year", F.year("created_ts"))
    .withColumn("created_month", F.date_format("created_ts", "MM"))
    .withColumn("_silver_loaded_at", F.lit(RUN_TS))
    .drop("_source_file")
)

# ---------------------------------------------------------------- 6. write
# Partition by year/month: the dashboard and Athena almost always filter on
# a date range, so month partitions give pruning with ~250k rows (~10-20 MB)
# per partition. Partitioning by day would create ~225 tiny partitions and by
# district x day ~11k, the classic small-files problem. Inside each partition
# rows are sorted by district + complaint type so Parquet row-group min/max
# statistics let engines skip row groups for district/type filters too.
(
    silver.repartition("created_year", "created_month")
    .sortWithinPartitions("council_district", "complaint_type", "created_ts")
    .write.mode("overwrite")
    .partitionBy("created_year", "created_month")
    .option("compression", "snappy")
    .parquet(SILVER)
)

q_count = quarantine.count()
if q_count:
    quarantine.write.mode("overwrite").parquet(QUARANTINE)

# ---------------------------------------------------------------- 7. data-quality report
dq = silver.agg(
    F.count("*").alias("silver_rows"),
    F.sum(F.col("dq_bad_district").cast("int")).alias("bad_district_nulled"),
    F.sum(F.col("council_district").isNull().cast("int")).alias("null_district"),
    F.sum(F.col("dq_bad_coords").cast("int")).alias("bad_coords_nulled"),
    F.sum(F.col("dq_closed_before_created").cast("int")).alias("closed_before_created"),
    F.sum(F.col("dq_status_mismatch").cast("int")).alias("status_mismatch"),
    F.sum(F.col("dq_zero_resolution").cast("int")).alias("zero_resolution_time"),
    F.sum(F.col("is_closed").cast("int")).alias("closed_rows"),
    F.min("created_ts").cast("string").alias("min_created"),
    F.max("created_ts").cast("string").alias("max_created"),
).first().asDict()

reasons = {r["_reject_reason"]: r["count"] for r in quarantine.groupBy("_reject_reason").count().collect()}
report = {
    "run_ts": RUN_TS,
    "bronze_rows": raw_count,
    "quarantined_rows": q_count,
    "quarantine_reasons": reasons,
    "duplicates_removed": before_dedupe - after_dedupe,
    **dq,
}
print(json.dumps(report, indent=2, default=str))
spark.createDataFrame([(json.dumps(report, default=str),)], ["value"]).coalesce(1).write.mode("overwrite").text(DQ_REPORT)

# Fail loudly if something is badly wrong rather than publishing bad data.
if raw_count and (q_count / raw_count) > 0.05:
    raise RuntimeError(f"Quarantine rate {q_count / raw_count:.1%} exceeds 5% threshold")

if job:
    job.commit()

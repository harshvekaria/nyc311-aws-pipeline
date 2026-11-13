"""
Glue 5.0 (Spark 3.5) job: Silver Parquet -> Gold business tables.

Each Gold table is written twice:
    s3://BUCKET/gold/parquet/<table>/   -> for Athena / ad-hoc analysis
    s3://BUCKET/gold/csv/<table>/       -> single CSV, loaded into PostgreSQL

Tables
    gold_daily_fact           day x district x complaint_type x agency cube (drives the dashboard filters)
    gold_district_performance avg/median resolution time + volume per council district
    gold_complaint_distribution complaint ranks citywide vs per district
    gold_agency_efficiency    response time stats per agency
    gold_temporal_trends      daily + weekly volume with 7-day rolling average
    gold_bottlenecks          districts whose open:closed ratio exceeds the city average
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

LOCAL = "--local" in sys.argv


def _arg(name, default=None):
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if LOCAL:
    ROOT = _arg("BUCKET", "./lake").rstrip("/")
    spark = SparkSession.builder.appName("nyc311-gold-local").getOrCreate()
    job = None
else:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
    ROOT = f"s3://{args['BUCKET']}"
    glue_ctx = GlueContext(SparkContext.getOrCreate())
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "America/New_York")
silver = spark.read.parquet(f"{ROOT}/silver/nyc311/")

# Unknown district is kept as 0 so totals always reconcile with Silver.
s = silver.withColumn("council_district", F.coalesce("council_district", F.lit(0)))
closed = F.col("is_closed")


def write(df, name):
    df.write.mode("overwrite").parquet(f"{ROOT}/gold/parquet/{name}/")
    (df.coalesce(1).write.mode("overwrite")
       .option("header", True)
       .option("escape", '"')          # Postgres CSV escapes quotes by doubling them
       .option("nullValue", "")
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
       .option("dateFormat", "yyyy-MM-dd")
       .csv(f"{ROOT}/gold/csv/{name}/"))
    print(f"wrote {name}: {df.count()} rows")


# 1. Fact cube --------------------------------------------------------------
# Stores additive measures only (counts and sums). Any slice the dashboard
# asks for can then compute a correct weighted average:
#     avg_resolution_hours = SUM(sum_resolution_hours) / SUM(closed_count)
fact = (
    s.groupBy(F.col("created_date").alias("created_day"), "council_district", "borough",
              "complaint_type", "agency")
    .agg(
        F.count("*").alias("request_count"),
        F.sum(closed.cast("int")).alias("closed_count"),
        F.sum((~closed).cast("int")).alias("open_count"),
        F.round(F.sum("resolution_hours"), 2).alias("sum_resolution_hours"),
    )
)
write(fact, "gold_daily_fact")

# 2. District performance ----------------------------------------------------
district = (
    s.groupBy("council_district")
    .agg(
        F.first("borough", ignorenulls=True).alias("borough"),
        F.count("*").alias("total_requests"),
        F.sum(closed.cast("int")).alias("closed_requests"),
        F.sum((~closed).cast("int")).alias("open_requests"),
        F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.5), 2).alias("median_resolution_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.9), 2).alias("p90_resolution_hours"),
    )
    .withColumn("pct_closed", F.round(F.col("closed_requests") / F.col("total_requests") * 100, 2))
)
write(district, "gold_district_performance")

# 3. Complaint distribution: citywide rank vs rank inside each district ------
city_counts = s.groupBy("complaint_type").agg(F.count("*").alias("citywide_count"))
city_total = s.count()
city_counts = (city_counts
               .withColumn("citywide_share_pct", F.round(F.col("citywide_count") / city_total * 100, 3))
               .withColumn("citywide_rank", F.dense_rank().over(Window.orderBy(F.desc("citywide_count")))))
dist_counts = s.groupBy("council_district", "complaint_type").agg(F.count("*").alias("district_count"))
dist_tot = Window.partitionBy("council_district")
complaints = (
    dist_counts
    .withColumn("district_share_pct",
                F.round(F.col("district_count") / F.sum("district_count").over(dist_tot) * 100, 3))
    .withColumn("district_rank", F.dense_rank().over(dist_tot.orderBy(F.desc("district_count"))))
    .join(city_counts, "complaint_type")
    # >1 means this complaint is over-represented in the district vs the city
    .withColumn("over_index", F.round(F.col("district_share_pct") / F.col("citywide_share_pct"), 3))
)
write(complaints, "gold_complaint_distribution")

# 4. Agency efficiency -------------------------------------------------------
agency = (
    s.groupBy("agency")
    .agg(
        F.first("agency_name", ignorenulls=True).alias("agency_name"),
        F.count("*").alias("total_requests"),
        F.sum(closed.cast("int")).alias("closed_requests"),
        F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.5), 2).alias("median_resolution_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.9), 2).alias("p90_resolution_hours"),
        F.round(F.avg((F.col("resolution_hours") <= 24).cast("int")) * 100, 2).alias("pct_closed_within_24h"),
    )
    .withColumn("pct_closed", F.round(F.col("closed_requests") / F.col("total_requests") * 100, 2))
)
write(agency, "gold_agency_efficiency")

# 5. Temporal trends ---------------------------------------------------------
daily = s.groupBy(F.col("created_date").alias("period_start")).agg(F.count("*").alias("request_count"))
daily = (daily.withColumn("grain", F.lit("day"))
         .withColumn("rolling_7d_avg",
                     F.round(F.avg("request_count").over(Window.orderBy("period_start").rowsBetween(-6, 0)), 1)))
weekly = (s.groupBy(F.col("created_week").alias("period_start")).agg(F.count("*").alias("request_count"))
          .withColumn("grain", F.lit("week")).withColumn("rolling_7d_avg", F.lit(None).cast("double")))
temporal = daily.unionByName(weekly)
peak = Window.partitionBy("grain").orderBy(F.desc("request_count"))
temporal = temporal.withColumn("is_top5_peak", F.row_number().over(peak) <= 5)
write(temporal, "gold_temporal_trends")

# 6. Bottlenecks -------------------------------------------------------------
city_open = s.filter(~closed).count()
city_closed = s.filter(closed).count()
city_ratio = city_open / city_closed if city_closed else None
bottlenecks = (
    district.filter("council_district <> 0")
    .select("council_district", "borough", "open_requests", "closed_requests", "total_requests")
    .withColumn("open_to_closed_ratio", F.round(F.col("open_requests") / F.col("closed_requests"), 4))
    .withColumn("city_open_to_closed_ratio", F.lit(round(city_ratio, 4) if city_ratio else None))
    .withColumn("ratio_vs_city", F.round(F.col("open_to_closed_ratio") / F.col("city_open_to_closed_ratio"), 3))
    .withColumn("is_bottleneck", F.col("open_to_closed_ratio") > F.col("city_open_to_closed_ratio"))
)
write(bottlenecks, "gold_bottlenecks")

if job:
    job.commit()

# Databricks notebook source
# MAGIC %md
# MAGIC Config and helpers

# COMMAND ----------

from pyspark.sql import functions as F
import datetime

CATALOG   = "dbw_insurevista_dev"
SILVER    = f"{CATALOG}.hollard_silver"
GOLD      = f"{CATALOG}.hollard_gold"
GOLD_PATH = "abfss://gold@hollardlake.dfs.core.windows.net"


def sk(*cols):
    """Stable surrogate key: same business key → same BIGINT on every rebuild."""
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols])


def with_unknown(df, key, text_cols):
    """Append the 'Unknown' member row (key = -1)."""
    row = {key: -1, **{c: "Unknown" for c in text_cols}}
    unk = spark.createDataFrame([row])
    unk = unk.select([F.col(c).cast(df.schema[c].dataType).alias(c) for c in row])
    return df.unionByName(unk, allowMissingColumns=True)


def write_gold(df, table):
    delta_path  = f"{GOLD_PATH}/delta/{table}/"
    export_path = f"{GOLD_PATH}/export/{table}/"
    (df.withColumn("_gold_loaded_ts", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD}.{table} USING DELTA LOCATION '{delta_path}'")
    # Clean, single-version Parquet snapshot for Snowflake
    spark.table(f"{GOLD}.{table}").write.mode("overwrite").parquet(export_path)
    print(f"{table:<16} {spark.table(f'{GOLD}.{table}').count():>9,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC dim_date, with the Australian financial year and public holidays

# COMMAND ----------

def easter_sunday(y):
    """Anonymous Gregorian algorithm."""
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime.date(y, month, day)

holidays = []
for y in range(2010, 2031):
    e = easter_sunday(y)
    holidays += [
        (datetime.date(y, 1, 1),   "New Year's Day"),
        (datetime.date(y, 1, 26),  "Australia Day"),
        (e - datetime.timedelta(days=2), "Good Friday"),
        (e + datetime.timedelta(days=1), "Easter Monday"),
        (datetime.date(y, 4, 25),  "Anzac Day"),
        (datetime.date(y, 12, 25), "Christmas Day"),
        (datetime.date(y, 12, 26), "Boxing Day"),
    ]

# Collapse dates that are two holidays at once (e.g. Anzac Day = Easter Monday in 2011)
hol = (spark.createDataFrame(holidays, "full_date DATE, holiday_name STRING")
         .groupBy("full_date").agg(F.concat_ws(" / ", F.collect_list("holiday_name")).alias("holiday_name")))

dates = spark.sql("SELECT explode(sequence(DATE'2010-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS full_date")
fy_year = F.when(F.month("full_date") >= 7, F.year("full_date") + 1).otherwise(F.year("full_date"))

dim_date = (dates.join(hol, "full_date", "left")
    .select(F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            "full_date",
            F.dayofmonth("full_date").alias("day_of_month"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.month("full_date").alias("month_number"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.date_format("full_date", "yyyy-MM").alias("year_month"),
            F.quarter("full_date").alias("quarter"),
            F.year("full_date").alias("year"),
            F.concat(F.lit("FY"), fy_year.cast("string")).alias("financial_year_au"),   # FY2024 = Jul 2023–Jun 2024
            (F.floor(((F.month("full_date") + 5) % 12) / 3) + 1).cast("int").alias("fy_quarter"),  # Jul–Sep = Q1   
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
            F.col("holiday_name").isNotNull().alias("is_public_holiday_au"),
            "holiday_name"))

dim_date = with_unknown(dim_date, "date_key", ["day_name", "month_name", "financial_year_au"])
write_gold(dim_date, "dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC dim_lob and dim_broker

# COMMAND ----------

lob = (spark.table(f"{SILVER}.policies")
         .groupBy("line_of_business")
         .agg(F.concat_ws(", ", F.array_sort(F.collect_set("product_name"))).alias("product_list")))

dim_lob = lob.select(
    sk("line_of_business").alias("lob_key"),
    F.col("line_of_business").alias("lob_name"),
    F.when(F.col("line_of_business") == "Health", "Health")
     .when(F.col("line_of_business") == "Life", "Life")
     .otherwise("General Insurance").alias("lob_category"),
    "product_list")
write_gold(with_unknown(dim_lob, "lob_key", ["lob_name", "lob_category"]), "dim_lob")

dim_broker = spark.table(f"{SILVER}.brokers").select(
    sk("broker_id").alias("broker_key"),
    "broker_id", "broker_name", "broker_group", "state",
    "commission_rate_pct", "afsl_number", "is_active", "onboarded_date")
write_gold(with_unknown(dim_broker, "broker_key", ["broker_id", "broker_name", "broker_group", "state"]), "dim_broker")

# COMMAND ----------

# MAGIC %md
# MAGIC quick sanity cell

# COMMAND ----------

display(spark.sql(f"""
SELECT full_date, day_name, financial_year_au, fy_quarter, holiday_name
FROM {GOLD}.dim_date
WHERE full_date IN (DATE'2023-06-30', DATE'2023-07-01', DATE'2024-03-29', DATE'2024-04-25')
ORDER BY full_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC dim_customer

# COMMAND ----------

from pyspark.sql import Window

cust = spark.table(f"{SILVER}.customers")
pol  = spark.table(f"{SILVER}.policies")
pol_cust = pol.select("policy_id", "customer_id")

policy_stats = pol.groupBy("customer_id").agg(
    F.count("*").alias("total_policies"),
    F.sum(F.when(F.col("policy_status") == "Active", 1).otherwise(0)).alias("active_policies"))

ltv = (spark.table(f"{SILVER}.premium_transactions").join(pol_cust, "policy_id")
         .groupBy("customer_id")
         .agg(F.sum("amount_received_aud").cast("decimal(14,2)").alias("lifetime_value_aud")))

claim_stats = (spark.table(f"{SILVER}.claims").join(pol_cust, "policy_id")
                 .groupBy("customer_id").agg(F.count("*").alias("claim_count")))

complaints = (spark.table(f"{SILVER}.customer_complaints_log").groupBy("customer_id").agg(
    F.count("*").alias("complaint_count"),
    F.round(F.avg("nps_post_resolution"), 1).alias("nps_avg"),
    F.sum(F.when(F.col("severity") == "Escalated to AFCA", 1).otherwise(0)).alias("afca_escalations")))

natcat = (spark.table(f"{SILVER}.catastrophe_risk_scores")
            .select("postcode", F.col("overall_natcat_index").alias("natcat_risk_index")))

dc = (cust.join(policy_stats, "customer_id", "left")
          .join(ltv,          "customer_id", "left")
          .join(claim_stats,  "customer_id", "left")
          .join(complaints,   "customer_id", "left")
          .join(natcat,       "postcode",    "left"))

for c in ["total_policies", "active_policies", "claim_count", "complaint_count", "afca_escalations"]:
    dc = dc.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
dc = dc.withColumn("lifetime_value_aud", F.coalesce(F.col("lifetime_value_aud"), F.lit(0).cast("decimal(14,2)")))

# Illustrative risk tier: percentile of (50% claims + 50% NatCat)
all_rows = Window.partitionBy()
dc = dc.withColumn("_risk_score",
        0.5 * (F.col("claim_count") / F.max("claim_count").over(all_rows))
      + 0.5 * (F.coalesce(F.col("natcat_risk_index"), F.lit(0)) / 10))
dc = dc.withColumn("_pct", F.percent_rank().over(Window.orderBy("_risk_score")))
dc = dc.withColumn("risk_tier", F.when(F.col("_pct") >= 0.8, "High")
                                 .when(F.col("_pct") >= 0.5, "Medium")
                                 .otherwise("Low"))

dim_customer = dc.select(
    sk("customer_id", "valid_from").alias("customer_key"),
    "customer_id",
    F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
    "date_of_birth", "gender", "state", "suburb", "postcode", "preferred_channel",
    "is_multi_policy", "customer_since",
    "total_policies", "active_policies", "claim_count", "lifetime_value_aud",
    "complaint_count", "afca_escalations", "nps_avg",
    "natcat_risk_index", "risk_tier",
    "valid_from", "valid_to", "is_current")

dim_customer = with_unknown(dim_customer, "customer_key",
                            ["customer_id", "full_name", "state", "suburb", "postcode", "risk_tier"])
# The Unknown member must survive "WHERE is_current" filters in BI views
dim_customer = dim_customer.withColumn("is_current",
                    F.when(F.col("customer_key") == -1, F.lit(True)).otherwise(F.col("is_current")))

write_gold(dim_customer, "dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC dim_policy

# COMMAND ----------

dim_policy = spark.table(f"{SILVER}.policies").select(
    sk("policy_id").alias("policy_key"),
    "policy_id",
    sk("line_of_business").alias("lob_key"),          # same hash as dim_lob → joins directly
    "line_of_business", "product_name",
    "premium_aud", "sum_insured_aud", "excess_aud",
    "effective_date", "expiry_date", "policy_status",
    "distribution_channel", "state", "underwriter_id",
    # Motor enrichment
    "vehicle_make", "vehicle_model", "vehicle_year", "body_type", "usage_type", "agreed_value_aud",
    # Property enrichment
    "property_type", "construction_type", "flood_zone",
    F.col("bushfire_attack_level").alias("bushfire_level"),
    "distance_to_coast_km",
    # NatCat enrichment
    "natcat_risk_index", "bushfire_risk_score", "flood_risk_score", "cyclone_risk_score",
    "climate_trend_indicator")

write_gold(with_unknown(dim_policy, "policy_key",
                        ["policy_id", "line_of_business", "product_name", "policy_status"]), "dim_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity checks

# COMMAND ----------

display(spark.sql(f"""
SELECT risk_tier, COUNT(*) AS customers,
       ROUND(AVG(claim_count), 1) AS avg_claims,
       ROUND(AVG(natcat_risk_index), 1) AS avg_natcat
FROM {GOLD}.dim_customer
WHERE is_current AND customer_key <> -1
GROUP BY risk_tier ORDER BY avg_natcat DESC
"""))

display(spark.sql(f"""
SELECT customer_key, customer_id, suburb, postcode, natcat_risk_index, risk_tier,
       valid_from, valid_to, is_current
FROM {GOLD}.dim_customer
WHERE customer_id = 'HL-CUS-00001'
ORDER BY valid_from
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Helpers for the facts

# COMMAND ----------

from pyspark.sql import Window

def dkey(c):
    """Date → yyyyMMdd INT key; missing date → -1 (Unknown)."""
    return F.coalesce(F.date_format(F.col(c), "yyyyMMdd").cast("int"), F.lit(-1))

def fkey(c):
    """Business key → surrogate key; NULL business key → -1 (Unknown)."""
    return F.when(F.col(c).isNotNull(), sk(c)).otherwise(F.lit(-1).cast("bigint"))

cust_versions = (spark.table(f"{GOLD}.dim_customer")
                   .filter("customer_key <> -1")
                   .select("customer_id", "customer_key", "valid_from", "valid_to"))

def point_in_time_customer(df, event_date_col, grain_col):
    """Pick the customer version valid on the event date.
    If none covers it, fall back to the earliest version and flag it."""
    in_range = (F.col(event_date_col) >= F.col("valid_from")) & (F.col(event_date_col) < F.col("valid_to"))
    w = Window.partitionBy(grain_col).orderBy(F.when(in_range, 0).otherwise(1),
                                              F.col("valid_from").asc_nulls_last())
    return (df.join(cust_versions, "customer_id", "left")
              .withColumn("_pit_match", F.coalesce(in_range, F.lit(False)))
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1")
              .drop("_rn", "valid_from", "valid_to"))

# Policy attributes every fact needs
pol = spark.table(f"{SILVER}.policies").select("policy_id", "customer_id", "broker_id", "line_of_business")

# COMMAND ----------

# MAGIC %md
# MAGIC fact_claims

# COMMAND ----------

fc = (spark.table(f"{SILVER}.claims")
        .join(pol, "policy_id", "left"))
fc = point_in_time_customer(fc, "lodgement_date", "claim_id")

pit = fc.groupBy("_pit_match").count().collect()
print("claims customer match:", {r["_pit_match"]: r["count"] for r in pit})

fact_claims = fc.select(
    sk("claim_id").alias("claim_key"),
    fkey("policy_id").alias("policy_key"),
    F.coalesce(F.col("customer_key"), F.lit(-1).cast("bigint")).alias("customer_key"),
    fkey("broker_id").alias("broker_key"),
    fkey("line_of_business").alias("lob_key"),
    dkey("lodgement_date").alias("date_key"),                 # primary date = lodgement
    dkey("incident_date").alias("incident_date_key"),         # role-playing dates
    dkey("settlement_date").alias("settlement_date_key"),
    "claim_id", "claim_type", "claim_status", "catastrophe_code",   # degenerate dimensions
    "claim_amount_aud", "approved_amount_aud",
    F.datediff("settlement_date", "lodgement_date").alias("days_to_settle"),   # NULL while still open
    "is_fraud_flagged")

write_gold(fact_claims, "fact_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC fact_premiums

# COMMAND ----------

fp = (spark.table(f"{SILVER}.premium_transactions")
        .join(pol, "policy_id", "left"))
fp = point_in_time_customer(fp, "transaction_date", "transaction_id")

pit = fp.groupBy("_pit_match").count().collect()
print("premiums customer match:", {r["_pit_match"]: r["count"] for r in pit})

fact_premiums = fp.select(
    sk("transaction_id").alias("premium_key"),
    fkey("policy_id").alias("policy_key"),
    F.coalesce(F.col("customer_key"), F.lit(-1).cast("bigint")).alias("customer_key"),
    fkey("broker_id").alias("broker_key"),
    fkey("line_of_business").alias("lob_key"),
    dkey("transaction_date").alias("date_key"),
    "transaction_id", "payment_method", "payment_status", "instalment_number",
    "amount_due_aud", "amount_received_aud", "gst_aud", "stamp_duty_aud",
    F.when(F.col("amount_due_aud") != 0,
           F.round(F.col("amount_received_aud") / F.col("amount_due_aud"), 4)).alias("collection_rate"))

write_gold(fact_premiums, "fact_premiums")

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the star schema

# COMMAND ----------

# 1. Every foreign key must exist in its dimension (0 orphans = star schema is intact)
display(spark.sql(f"""
SELECT 'claims.policy_key'   AS fk, COUNT(*) AS orphans FROM {GOLD}.fact_claims f   LEFT ANTI JOIN {GOLD}.dim_policy   d ON f.policy_key   = d.policy_key
UNION ALL SELECT 'claims.customer_key',   COUNT(*) FROM {GOLD}.fact_claims f   LEFT ANTI JOIN {GOLD}.dim_customer d ON f.customer_key = d.customer_key
UNION ALL SELECT 'claims.broker_key',     COUNT(*) FROM {GOLD}.fact_claims f   LEFT ANTI JOIN {GOLD}.dim_broker   d ON f.broker_key   = d.broker_key
UNION ALL SELECT 'claims.lob_key',        COUNT(*) FROM {GOLD}.fact_claims f   LEFT ANTI JOIN {GOLD}.dim_lob      d ON f.lob_key      = d.lob_key
UNION ALL SELECT 'claims.date_key',       COUNT(*) FROM {GOLD}.fact_claims f   LEFT ANTI JOIN {GOLD}.dim_date     d ON f.date_key     = d.date_key
UNION ALL SELECT 'premiums.policy_key',   COUNT(*) FROM {GOLD}.fact_premiums f LEFT ANTI JOIN {GOLD}.dim_policy   d ON f.policy_key   = d.policy_key
UNION ALL SELECT 'premiums.customer_key', COUNT(*) FROM {GOLD}.fact_premiums f LEFT ANTI JOIN {GOLD}.dim_customer d ON f.customer_key = d.customer_key
UNION ALL SELECT 'premiums.broker_key',   COUNT(*) FROM {GOLD}.fact_premiums f LEFT ANTI JOIN {GOLD}.dim_broker   d ON f.broker_key   = d.broker_key
UNION ALL SELECT 'premiums.date_key',     COUNT(*) FROM {GOLD}.fact_premiums f LEFT ANTI JOIN {GOLD}.dim_date     d ON f.date_key     = d.date_key
"""))

# 2. Totals must reconcile with Silver: Gold must not gain or lose money
display(spark.sql(f"""
SELECT 'approved claims $' AS measure,
       (SELECT SUM(approved_amount_aud) FROM {SILVER}.claims)     AS silver,
       (SELECT SUM(approved_amount_aud) FROM {GOLD}.fact_claims)  AS gold
UNION ALL
SELECT 'premium received $',
       (SELECT SUM(amount_received_aud) FROM {SILVER}.premium_transactions),
       (SELECT SUM(amount_received_aud) FROM {GOLD}.fact_premiums)
"""))

# 3. Grace's claims: which address version does each one point to?
display(spark.sql(f"""
SELECT f.claim_id, d.full_date AS lodged, c.suburb, c.valid_from, c.valid_to
FROM {GOLD}.fact_claims f
JOIN {GOLD}.dim_date d     ON f.date_key = d.date_key
JOIN {GOLD}.dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_id = 'HL-CUS-00001'
ORDER BY lodged
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC fact_complaints

# COMMAND ----------

fcx = point_in_time_customer(spark.table(f"{SILVER}.customer_complaints_log"),
                             "complaint_date", "complaint_id")

fact_complaints = fcx.select(
    sk("complaint_id").alias("complaint_key"),
    F.coalesce(F.col("customer_key"), F.lit(-1).cast("bigint")).alias("customer_key"),
    dkey("complaint_date").alias("date_key"),
    dkey("resolution_date").alias("resolution_date_key"),       # role-playing date
    "complaint_id", "related_claim_id",                          # degenerate dimensions
    "category", "severity", "resolution_outcome",
    "nps_post_resolution",
    F.datediff("resolution_date", "complaint_date").alias("days_to_resolve"),
    (F.col("severity") == "Escalated to IDR").alias("is_idr_escalation"),
    (F.col("severity") == "Escalated to AFCA").alias("is_afca_escalation"))

write_gold(fact_complaints, "fact_complaints")

# COMMAND ----------

# MAGIC %md
# MAGIC fact_partner_sales

# COMMAND ----------

fps = (spark.table(f"{SILVER}.partner_brand_sales")
         .join(pol.select("policy_id", "broker_id", "line_of_business"), "policy_id", "left"))
fps = point_in_time_customer(fps, "sale_date", "sale_id")

fact_partner_sales = fps.select(
    sk("sale_id").alias("sale_key"),
    fkey("policy_id").alias("policy_key"),
    F.coalesce(F.col("customer_key"), F.lit(-1).cast("bigint")).alias("customer_key"),
    fkey("broker_id").alias("broker_key"),
    fkey("line_of_business").alias("lob_key"),
    dkey("sale_date").alias("date_key"),
    "sale_id", "partner_brand", "sale_channel", "campaign_code",
    "premium_aud", "acquisition_cost_aud",
    F.when(F.col("premium_aud") != 0,
           F.round(F.col("acquisition_cost_aud") / F.col("premium_aud"), 4)).alias("acquisition_cost_ratio"))

write_gold(fact_partner_sales, "fact_partner_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Validate

# COMMAND ----------

display(spark.sql(f"""
SELECT 'complaints.customer_key' AS fk, COUNT(*) AS orphans FROM {GOLD}.fact_complaints f    LEFT ANTI JOIN {GOLD}.dim_customer d ON f.customer_key = d.customer_key
UNION ALL SELECT 'complaints.date_key',       COUNT(*) FROM {GOLD}.fact_complaints f    LEFT ANTI JOIN {GOLD}.dim_date d     ON f.date_key     = d.date_key
UNION ALL SELECT 'partner.policy_key',        COUNT(*) FROM {GOLD}.fact_partner_sales f LEFT ANTI JOIN {GOLD}.dim_policy d   ON f.policy_key   = d.policy_key
UNION ALL SELECT 'partner.customer_key',      COUNT(*) FROM {GOLD}.fact_partner_sales f LEFT ANTI JOIN {GOLD}.dim_customer d ON f.customer_key = d.customer_key
UNION ALL SELECT 'partner.broker_key',        COUNT(*) FROM {GOLD}.fact_partner_sales f LEFT ANTI JOIN {GOLD}.dim_broker d   ON f.broker_key   = d.broker_key
UNION ALL SELECT 'partner.lob_key',           COUNT(*) FROM {GOLD}.fact_partner_sales f LEFT ANTI JOIN {GOLD}.dim_lob d      ON f.lob_key      = d.lob_key
UNION ALL SELECT 'partner.date_key',          COUNT(*) FROM {GOLD}.fact_partner_sales f LEFT ANTI JOIN {GOLD}.dim_date d     ON f.date_key     = d.date_key
"""))

display(spark.sql(f"""
SELECT 'partner premium $' AS measure,
       (SELECT SUM(premium_aud) FROM {SILVER}.partner_brand_sales) AS silver,
       (SELECT SUM(premium_aud) FROM {GOLD}.fact_partner_sales)    AS gold
UNION ALL
SELECT 'AFCA escalations',
       (SELECT COUNT(*) FROM {SILVER}.customer_complaints_log WHERE severity = 'Escalated to AFCA'),
       (SELECT SUM(CAST(is_afca_escalation AS INT)) FROM {GOLD}.fact_complaints)
"""))
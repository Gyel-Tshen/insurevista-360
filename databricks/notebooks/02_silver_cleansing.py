# Databricks notebook source
# MAGIC %md
# MAGIC Config and helper functions

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG     = "dbw_insurevista_dev"
BRONZE      = f"{CATALOG}.hollard_bronze"
SILVER      = f"{CATALOG}.hollard_silver"
SILVER_PATH = "abfss://silver@hollardlake.dfs.core.windows.net/delta"


def latest_per_key(df, keys):
    """Dedup: keep the newest version of each record.
    Orders by the snapshot date in the file path (yyyy/MM/dd), then _ingestion_ts."""
    date_str = F.regexp_extract("_source_file", r"/(\d{4}/\d{2}/\d{2})/", 1)
    snapshot = F.when(date_str != "", F.to_date(date_str, "yyyy/MM/dd"))
    w = Window.partitionBy(*keys).orderBy(F.col("_snapshot_date").desc_nulls_last(),
                                          F.col("_ingestion_ts").desc())
    return (df.withColumn("_snapshot_date", snapshot)
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))


def normalise(df, col, allowed, fixes=None):
    """Map any case/spacing variant (plus known misspellings) to one canonical value.
    Unrecognised values become NULL; the original is kept in _<col>_raw for quarantine."""
    mapping = {v.lower(): v for v in allowed}
    mapping.update({k.lower(): v for k, v in (fixes or {}).items()})
    key = F.lower(F.trim(F.col(col)))
    expr = None
    for k, v in mapping.items():
        expr = F.when(key == k, F.lit(v)) if expr is None else expr.when(key == k, F.lit(v))
    return df.withColumn(f"_{col}_raw", F.col(col)).withColumn(col, expr)


def write_silver(df, table):
    path = f"{SILVER_PATH}/{table}/"
    (df.withColumn("_silver_loaded_ts", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER}.{table} USING DELTA LOCATION '{path}'")
    print(f"{table:<32} {spark.table(f'{SILVER}.{table}').count():>9,} rows")


def write_quarantine(df_bad, table):
    """Always written, even with 0 rows, so the quarantine table exists for monitoring."""
    write_silver(df_bad.withColumn("_quarantined_ts", F.current_timestamp()), f"quarantine_{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver brokers

# COMMAND ----------

br = latest_per_key(spark.table(f"{BRONZE}.brokers"), ["broker_id"])

br_bad = (br.filter(F.col("broker_id").isNull())
            .withColumn("_dq_reason", F.lit("null broker_id")))

br_ok = (br.filter(F.col("broker_id").isNotNull())
           .select("broker_id", "broker_name", "broker_group", "afsl_number", "state",
                   F.col("commission_rate_pct").cast("decimal(4,2)").alias("commission_rate_pct"),
                   F.col("is_active").cast("boolean").alias("is_active"),
                   F.col("onboarded_date").cast("date").alias("onboarded_date"),
                   "_snapshot_date"))

write_silver(br_ok, "brokers")
write_quarantine(br_bad, "brokers")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver catastrophe risk scores

# COMMAND ----------

SCORES = ["bushfire_risk_score", "flood_risk_score", "cyclone_risk_score", "storm_risk_score",
          "earthquake_risk_score", "overall_natcat_index"]

cat = (spark.table(f"{BRONZE}.catastrophe_risk_scores")
         .withColumn("postcode", F.lpad(F.trim("postcode"), 4, "0"))      # protect NT leading zeros
         .withColumn("assessment_date", F.expr("try_cast(assessment_date AS DATE)")))

# Keep the latest assessment per postcode
w = Window.partitionBy("postcode").orderBy(F.col("assessment_date").desc_nulls_last(),
                                           F.col("_ingestion_ts").desc())
cat = cat.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

for s in SCORES:
    cat = cat.withColumn(s, F.col(s).cast("double"))

# DQ rule: every score must be within 0–10 (per the RiskFrontiers spec in the design doc)
out_of_range = " OR ".join([f"{s} < 0 OR {s} > 10" for s in SCORES])
cat_bad = cat.filter(f"postcode IS NULL OR {out_of_range}").withColumn("_dq_reason", F.lit("null postcode or score outside 0-10"))
cat_ok  = cat.filter(f"NOT (postcode IS NULL OR {out_of_range})")

write_silver(cat_ok.select("postcode", "state", "assessment_date", *SCORES,
                           F.col("historical_claims_density").cast("double").alias("historical_claims_density"),
                           "climate_trend_indicator", "data_provider", "model_version"),
             "catastrophe_risk_scores")
write_quarantine(cat_bad, "catastrophe_risk_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC Build the clean customer source

# COMMAND ----------

cu = latest_per_key(spark.table(f"{BRONZE}.customers"), ["customer_id"])
cu = normalise(cu, "state", ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"])
cu = normalise(cu, "preferred_channel", ["Email", "Phone", "App", "Broker"])

# DQ: missing key or unrecognised state → quarantine
cu_bad = (cu.filter(F.col("customer_id").isNull() | F.col("state").isNull())
            .withColumn("_dq_reason", F.lit("null customer_id or invalid state")))
write_quarantine(cu_bad, "customers")

SCD_COLS = ["state", "suburb", "postcode", "preferred_channel"]
T1_COLS  = ["first_name", "last_name", "date_of_birth", "gender", "email", "mobile", "is_multi_policy"]

def row_hash(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), 256)

src = (cu.filter(F.col("customer_id").isNotNull() & F.col("state").isNotNull())
         .select("customer_id",
                 F.trim("first_name").alias("first_name"),
                 F.trim("last_name").alias("last_name"),
                 F.col("date_of_birth").cast("date").alias("date_of_birth"),
                 F.trim("gender").alias("gender"),
                 F.lower(F.trim("email")).alias("email"),
                 F.trim(F.col("mobile").cast("string")).alias("mobile"),
                 "state",
                 F.trim("suburb").alias("suburb"),
                 F.lpad(F.trim(F.col("postcode").cast("string")), 4, "0").alias("postcode"),
                 F.col("customer_since").cast("date").alias("customer_since"),
                 F.coalesce("preferred_channel", F.lit("Unknown")).alias("preferred_channel"),
                 F.col("is_multi_policy").cast("boolean").alias("is_multi_policy"),
                 F.coalesce("_snapshot_date", F.current_date()).alias("_snapshot_date"))
         .withColumn("scd_hash", row_hash(SCD_COLS))
         .withColumn("t1_hash",  row_hash(T1_COLS)))

src.createOrReplaceTempView("customers_src")
print("source customers:", src.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Create the target table (first run only)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.customers (
    customer_id STRING, first_name STRING, last_name STRING, date_of_birth DATE, gender STRING,
    email STRING, mobile STRING, state STRING, suburb STRING, postcode STRING,
    customer_since DATE, preferred_channel STRING, is_multi_policy BOOLEAN,
    scd_hash STRING, t1_hash STRING,
    valid_from DATE, valid_to DATE, is_current BOOLEAN,
    _silver_loaded_ts TIMESTAMP)
USING DELTA
LOCATION '{SILVER_PATH}/customers/'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC The SCD2 MERGE

# COMMAND ----------

merge_result = spark.sql(f"""
MERGE INTO {SILVER}.customers t
USING (
    -- every source row, keyed normally: matches its current row (or is brand new)
    SELECT customer_id AS merge_key, * FROM customers_src
    UNION ALL
    -- a second copy of rows whose tracked attributes CHANGED, with a NULL key:
    -- it can never match, so it falls through to INSERT as the new version
    SELECT CAST(NULL AS STRING) AS merge_key, s.*
    FROM customers_src s
    JOIN {SILVER}.customers c
      ON s.customer_id = c.customer_id AND c.is_current
    WHERE s.scd_hash <> c.scd_hash
) s
ON t.customer_id = s.merge_key AND t.is_current

-- SCD2 change: close the old version
WHEN MATCHED AND t.scd_hash <> s.scd_hash THEN UPDATE SET
    t.is_current = false,
    t.valid_to   = s._snapshot_date,
    t._silver_loaded_ts = current_timestamp()

-- Type 1 change only: overwrite in place
WHEN MATCHED AND t.t1_hash <> s.t1_hash THEN UPDATE SET
    t.first_name = s.first_name, t.last_name = s.last_name, t.date_of_birth = s.date_of_birth,
    t.gender = s.gender, t.email = s.email, t.mobile = s.mobile,
    t.is_multi_policy = s.is_multi_policy, t.t1_hash = s.t1_hash,
    t._silver_loaded_ts = current_timestamp()

-- New customer, or new version of a changed customer
WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, date_of_birth, gender, email, mobile,
    state, suburb, postcode, customer_since, preferred_channel, is_multi_policy,
    scd_hash, t1_hash, valid_from, valid_to, is_current, _silver_loaded_ts)
VALUES (
    s.customer_id, s.first_name, s.last_name, s.date_of_birth, s.gender, s.email, s.mobile,
    s.state, s.suburb, s.postcode, s.customer_since, s.preferred_channel, s.is_multi_policy,
    s.scd_hash, s.t1_hash,
    CASE WHEN s.merge_key IS NULL THEN s._snapshot_date              -- changed: new version starts today
         ELSE COALESCE(s.customer_since, s._snapshot_date) END,      -- brand new: starts when they joined
    DATE'9999-12-31', true, current_timestamp())
""")
display(merge_result)

display(spark.sql(f"""
SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
       COUNT(DISTINCT customer_id) AS customers
FROM {SILVER}.customers
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Leading zeros survived.

# COMMAND ----------


display(spark.sql(f"""
SELECT state, postcode, COUNT(*) AS customers
FROM {SILVER}.customers
WHERE state = 'NT'
GROUP BY ALL ORDER BY postcode LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Cleanse and validate policies

# COMMAND ----------

po = latest_per_key(spark.table(f"{BRONZE}.policies"), ["policy_id"])
po = normalise(po, "policy_status", ["Active", "Lapsed", "Cancelled", "Renewed"])
po = normalise(po, "line_of_business", ["Motor", "Home", "Commercial", "Farm", "Pet", "Travel", "Health", "Life"])
po = normalise(po, "state", ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"])
po = normalise(po, "distribution_channel", ["Broker", "Direct Online", "Partner Brand", "Call Centre"])

# Referential integrity against SILVER (already-validated) parents
cust_ok   = (spark.table(f"{SILVER}.customers").filter("is_current")
               .select("customer_id", "postcode").withColumn("_cust_ok", F.lit(True)))
broker_ok = spark.table(f"{SILVER}.brokers").select("broker_id").withColumn("_broker_ok", F.lit(True))

po = po.join(cust_ok, "customer_id", "left").join(broker_ok, "broker_id", "left")

# Every rule that fails adds its reason; a clean row ends up with an empty string
po = po.withColumn("_dq_reason", F.concat_ws("; ",
    F.when(F.col("policy_id").isNull(),                                   F.lit("null policy_id")),
    F.when(F.col("_cust_ok").isNull(),                                    F.lit("customer not found")),
    F.when(F.col("broker_id").isNotNull() & F.col("_broker_ok").isNull(), F.lit("broker not found")),
    F.when(F.col("policy_status").isNull(),                               F.lit("invalid policy_status")),
    F.when(F.col("line_of_business").isNull(),                            F.lit("invalid line_of_business")),
    F.when(F.col("expiry_date") < F.col("effective_date"),                F.lit("expiry before effective")),
    F.when(F.col("premium_aud") < 0,                                      F.lit("negative premium"))))

write_quarantine(po.filter("_dq_reason <> ''"), "policies")

po_ok = (po.filter("_dq_reason = ''")
           .select("policy_id", "customer_id", "line_of_business", "product_name",
                   F.col("premium_aud").cast("decimal(12,2)").alias("premium_aud"),
                   F.col("sum_insured_aud").cast("decimal(15,2)").alias("sum_insured_aud"),
                   F.col("excess_aud").cast("decimal(10,2)").alias("excess_aud"),
                   F.col("effective_date").cast("date").alias("effective_date"),
                   F.col("expiry_date").cast("date").alias("expiry_date"),
                   "policy_status", "underwriter_id", "broker_id", "state",
                   "distribution_channel", "postcode", "_snapshot_date"))
print("valid policies:", po_ok.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Enrich and write

# COMMAND ----------

# Motor: one row per policy, CSV strings → proper types (try_cast = NULL instead of crashing on bad values)
mv = (latest_per_key(spark.table(f"{BRONZE}.motor_vehicle_register"), ["policy_id"])
        .select("policy_id", "rego_state", "rego_number", "vehicle_make", "vehicle_model",
                F.expr("try_cast(vehicle_year AS INT)").alias("vehicle_year"),
                "body_type", "usage_type",
                F.expr("try_cast(agreed_value_aud AS DECIMAL(12,2))").alias("agreed_value_aud"),
                "parking_type",
                F.expr("try_cast(annual_km AS INT)").alias("annual_km")))

# Property: one row per policy
pr = (latest_per_key(spark.table(f"{BRONZE}.property_risk_details"), ["policy_id"])
        .select("policy_id", "property_type", "construction_type", "roof_type",
                F.expr("try_cast(year_built AS INT)").alias("year_built"),
                F.expr("try_cast(floor_area_sqm AS DOUBLE)").alias("floor_area_sqm"),
                F.expr("try_cast(sum_insured_building_aud AS DECIMAL(15,2))").alias("sum_insured_building_aud"),
                F.expr("try_cast(sum_insured_contents_aud AS DECIMAL(15,2))").alias("sum_insured_contents_aud"),
                F.when(F.lower(F.trim("flood_zone")) == "yes", True)
                 .when(F.lower(F.trim("flood_zone")) == "no", False).alias("flood_zone"),
                "bushfire_attack_level",
                F.expr("try_cast(distance_to_coast_km AS DOUBLE)").alias("distance_to_coast_km")))

# NatCat scores, joined on the customer's current postcode
cat = (spark.table(f"{SILVER}.catastrophe_risk_scores")
         .select("postcode",
                 F.col("overall_natcat_index").alias("natcat_risk_index"),
                 "bushfire_risk_score", "flood_risk_score", "cyclone_risk_score",
                 "storm_risk_score", "climate_trend_indicator"))

enriched = (po_ok.join(mv,  "policy_id", "left")
                 .join(pr,  "policy_id", "left")
                 .join(cat, "postcode",  "left"))

# Guard: enrichment must never change the number of policies
before, after = po_ok.count(), enriched.count()
assert before == after, f"Join fan-out! {before:,} policies became {after:,} rows"
print(f"fan-out check passed: {after:,} = {before:,}")

write_silver(enriched, "policies")

# COMMAND ----------

# MAGIC %md
# MAGIC Enrichment coverage report

# COMMAND ----------

display(spark.sql(f"""
SELECT line_of_business,
       COUNT(*)                 AS policies,
       COUNT(vehicle_make)      AS with_vehicle,
       COUNT(property_type)     AS with_property,
       COUNT(natcat_risk_index) AS with_natcat
FROM {SILVER}.policies
GROUP BY ALL ORDER BY policies DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Two helpers

# COMMAND ----------

def add_exists_flag(df, ref_table, ref_key, df_key, flag):
    """LEFT JOIN against a Silver parent; flag is NULL when the key isn't found."""
    ref = (spark.table(f"{SILVER}.{ref_table}")
             .select(F.col(ref_key).alias(df_key)).distinct()
             .withColumn(flag, F.lit(True)))
    return df.join(ref, df_key, "left")


def finalise(df, table, hard_rules, warn_rules, cols):
    """Hard rules → quarantine. Warnings → kept, but noted in _dq_warning."""
    reason = F.concat_ws("; ", *[F.when(cond, F.lit(msg)) for msg, cond in hard_rules])
    warns  = F.concat_ws("; ", *[F.when(cond, F.lit(msg)) for msg, cond in warn_rules])
    df = (df.withColumn("_dq_reason", reason)
            .withColumn("_dq_warning", F.when(warns != "", warns)))
    write_quarantine(df.filter("_dq_reason <> ''"), table)
    write_silver(df.filter("_dq_reason = ''").select(*cols, "_dq_warning"), table)

# COMMAND ----------

# MAGIC %md
# MAGIC Claims

# COMMAND ----------

cl = latest_per_key(spark.table(f"{BRONZE}.claims"), ["claim_id"])
cl = normalise(cl, "claim_status",
               ["Lodged", "Under Assessment", "Approved", "Partially Approved", "Denied", "Withdrawn"])
cl = (cl.withColumn("claim_amount_aud",    F.col("claim_amount_aud").cast("decimal(12,2)"))
        .withColumn("approved_amount_aud", F.col("approved_amount_aud").cast("decimal(12,2)"))
        .withColumn("lodgement_date",      F.col("lodgement_date").cast("date"))
        .withColumn("incident_date",       F.col("incident_date").cast("date"))
        .withColumn("settlement_date",     F.col("settlement_date").cast("date"))
        .withColumn("is_fraud_flagged",    F.col("is_fraud_flagged").cast("boolean"))
        .withColumn("catastrophe_code",    F.when(F.trim("catastrophe_code") != "", F.trim("catastrophe_code"))))
cl = add_exists_flag(cl, "policies", "policy_id", "policy_id", "_policy_ok")

finalise(cl, "claims",
    hard_rules=[
        ("null claim_id",            F.col("claim_id").isNull()),
        ("policy not found",         F.col("_policy_ok").isNull()),
        ("invalid claim_status",     F.col("claim_status").isNull()),
        ("incident after lodgement", F.col("incident_date") > F.col("lodgement_date")),
        ("settled before lodged",    F.col("settlement_date") < F.col("lodgement_date")),
        ("negative amount",          (F.col("claim_amount_aud") < 0) | (F.col("approved_amount_aud") < 0))],
    warn_rules=[
        ("approved exceeds claimed",   F.col("approved_amount_aud") > F.col("claim_amount_aud")),
        ("denied but approved > 0",    (F.col("claim_status") == "Denied") & (F.col("approved_amount_aud") > 0))],
    cols=["claim_id", "policy_id", "lodgement_date", "incident_date", "claim_type",
          "claim_amount_aud", "approved_amount_aud", "claim_status", "assessor_id",
          "settlement_date", "is_fraud_flagged", "catastrophe_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC Premium transactions

# COMMAND ----------

pt = latest_per_key(spark.table(f"{BRONZE}.premium_transactions"), ["transaction_id"])
pt = normalise(pt, "payment_status", ["Paid", "Partial", "Overdue", "Dishonoured"],
               fixes={"Dishnoured": "Dishonoured"})          # the misspelling from the design doc
pt = normalise(pt, "payment_method", ["BPAY", "Credit Card", "Direct Debit", "Bank Transfer"])
pt = (pt.withColumn("transaction_date",    F.col("transaction_date").cast("date"))
        .withColumn("amount_due_aud",      F.col("amount_due_aud").cast("decimal(12,2)"))
        .withColumn("amount_received_aud", F.col("amount_received_aud").cast("decimal(12,2)"))
        .withColumn("gst_aud",             F.col("gst_aud").cast("decimal(10,2)"))
        .withColumn("stamp_duty_aud",      F.col("stamp_duty_aud").cast("decimal(10,2)"))
        .withColumn("instalment_number",   F.col("instalment_number").cast("int")))
pt = add_exists_flag(pt, "policies", "policy_id", "policy_id", "_policy_ok")

finalise(pt, "premium_transactions",
    hard_rules=[
        ("null transaction_id",     F.col("transaction_id").isNull()),
        ("policy not found",        F.col("_policy_ok").isNull()),
        ("invalid payment_status",  F.col("payment_status").isNull()),
        ("invalid payment_method",  F.col("payment_method").isNull()),
        ("negative amount",         (F.col("amount_due_aud") < 0) | (F.col("amount_received_aud") < 0)),
        ("instalment outside 1-12", ~F.col("instalment_number").between(1, 12))],
    warn_rules=[
        ("received exceeds due",    F.col("amount_received_aud") > F.col("amount_due_aud")),
        ("Paid but received < due", (F.col("payment_status") == "Paid") &
                                    (F.col("amount_received_aud") < F.col("amount_due_aud")))],
    cols=["transaction_id", "policy_id", "transaction_date", "amount_due_aud", "amount_received_aud",
          "gst_aud", "stamp_duty_aud", "payment_method", "payment_status", "instalment_number"])

# COMMAND ----------

# MAGIC %md
# MAGIC Complaints

# COMMAND ----------

co = latest_per_key(spark.table(f"{BRONZE}.customer_complaints_log"), ["complaint_id"])
co = normalise(co, "severity", ["Low", "Medium", "High", "Escalated to IDR", "Escalated to AFCA"])
co = (co.withColumn("complaint_date",      F.expr("try_cast(complaint_date AS DATE)"))
        .withColumn("resolution_date",     F.expr("try_cast(resolution_date AS DATE)"))
        .withColumn("nps_post_resolution", F.expr("try_cast(nps_post_resolution AS INT)"))
        .withColumn("related_claim_id",    F.when(F.trim("related_claim_id") != "", F.trim("related_claim_id"))))
co = add_exists_flag(co, "customers", "customer_id", "customer_id",      "_cust_ok")
co = add_exists_flag(co, "claims",    "claim_id",    "related_claim_id", "_claim_ok")

finalise(co, "customer_complaints_log",
    hard_rules=[
        ("null complaint_id",       F.col("complaint_id").isNull()),
        ("customer not found",      F.col("_cust_ok").isNull()),
        ("related claim not found", F.col("related_claim_id").isNotNull() & F.col("_claim_ok").isNull()),
        ("invalid severity",        F.col("severity").isNull()),
        ("bad complaint_date",      F.col("complaint_date").isNull()),
        ("resolved before lodged",  F.col("resolution_date") < F.col("complaint_date")),
        ("NPS outside 0-10",        ~F.col("nps_post_resolution").between(0, 10))],
    warn_rules=[
        ("resolved but no NPS",     F.col("resolution_date").isNotNull() & F.col("nps_post_resolution").isNull())],
    cols=["complaint_id", "customer_id", "complaint_date", "category", "severity", "related_claim_id",
          "resolution_date", "resolution_outcome", "nps_post_resolution"])

# COMMAND ----------

# MAGIC %md
# MAGIC Partner brand sales

# COMMAND ----------

ps = latest_per_key(spark.table(f"{BRONZE}.partner_brand_sales"), ["sale_id"])
ps = normalise(ps, "sale_channel", ["Website", "App", "Call Centre", "Comparison Site"])
ps = (ps.withColumn("sale_date",            F.expr("try_cast(sale_date AS DATE)"))
        .withColumn("premium_aud",          F.expr("try_cast(premium_aud AS DECIMAL(12,2))"))
        .withColumn("acquisition_cost_aud", F.expr("try_cast(acquisition_cost_aud AS DECIMAL(12,2))"))
        # 'NONE' / 'N/A' / blank are placeholders for "no campaign" → real NULL
        .withColumn("campaign_code", F.when(~F.upper(F.trim("campaign_code")).isin("NONE", "N/A", ""),
                                            F.trim("campaign_code"))))
ps = add_exists_flag(ps, "policies",  "policy_id",   "policy_id",   "_policy_ok")
ps = add_exists_flag(ps, "customers", "customer_id", "customer_id", "_cust_ok")

finalise(ps, "partner_brand_sales",
    hard_rules=[
        ("null sale_id",          F.col("sale_id").isNull()),
        ("policy not found",      F.col("_policy_ok").isNull()),
        ("customer not found",    F.col("_cust_ok").isNull()),
        ("invalid sale_channel",  F.col("sale_channel").isNull()),
        ("bad sale_date",         F.col("sale_date").isNull()),
        ("negative amount",       (F.col("premium_aud") < 0) | (F.col("acquisition_cost_aud") < 0))],
    warn_rules=[
        ("acquisition cost exceeds premium", F.col("acquisition_cost_aud") > F.col("premium_aud"))],
    cols=["sale_id", "partner_brand", "policy_id", "customer_id", "sale_date", "sale_channel",
          "campaign_code", "premium_aud", "acquisition_cost_aud"])

# COMMAND ----------

# MAGIC %md
# MAGIC Silver scorecard

# COMMAND ----------

display(spark.sql(f"""
SELECT 'claims' AS tbl, COUNT(*) AS rows, COUNT(_dq_warning) AS warnings FROM {SILVER}.claims
UNION ALL SELECT 'premium_transactions', COUNT(*), COUNT(_dq_warning) FROM {SILVER}.premium_transactions
UNION ALL SELECT 'customer_complaints_log', COUNT(*), COUNT(_dq_warning) FROM {SILVER}.customer_complaints_log
UNION ALL SELECT 'partner_brand_sales', COUNT(*), COUNT(_dq_warning) FROM {SILVER}.partner_brand_sales
UNION ALL SELECT 'partner_sales: NULL campaign', COUNT(*) - COUNT(campaign_code), NULL FROM {SILVER}.partner_brand_sales
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

display(spark.sql(f"""
SELECT customer_id, suburb, postcode, state, email, valid_from, valid_to, is_current
FROM {SILVER}.customers
WHERE customer_id IN ('HL-CUS-00001', 'HL-CUS-00002')
ORDER BY customer_id, valid_from
"""))
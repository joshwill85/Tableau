# Databricks Notebook (PySpark)
# ============================================================
# Tableau repo (table_assets) → ROW-LEVEL mapping of usage of eciscor_prod.npdataset.*
#
# Output row grain:
#   (npdataset table) × (source_type: workbook|datasource) × (datasource) × (workbook) × (dashboard)
#
# Writes to: eciscor_prod.cdo.npdatasetmetrics_tableau
# Incremental strategy:
#   - partitioned by snapshot_date
#   - overwrite ONLY today's partition (idempotent)
#
# IMPORTANT:
#   - Identifies npdataset assets by table_assets.full_name containing 'npdataset'
#   - Does NOT rely on table_schema (per your note)
#   - Does NOT use system.access.table_lineage / statement_id / query history
#   - Does NOT do any row-count comparisons

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import List, Optional, Dict

from pyspark.sql import DataFrame, functions as F, types as T


# =========================
# CONFIG
# =========================
CATALOG = "eciscor_prod"
TABLEAU_META_SCHEMA = "ec_tableau_meta"  # UC schema holding Tableau repo tables (renamed from Postgres 'public')

NP_CATALOG = "eciscor_prod"
NP_SCHEMA = "npdataset"

TARGET_SCHEMA = "cdo"
TARGET_TABLE  = "npdatasetmetrics_tableau"  # eciscor_prod.cdo.npdatasetmetrics_tableau

SNAPSHOT_DATE = date.today()

FAIL_JOB_IF_EMPTY = True

# One-time: drop + recreate the output Delta table (use to "clean up" schema)
RECREATE_TABLE = False

# Keep rows where datasource is linked to npdataset but has no downstream workbooks (workbook_id null)
INCLUDE_UNUSED_DATASOURCES = True

DEBUG_SAMPLES = True


# =========================
# Helpers
# =========================
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def fqn(schema: str, table: str, catalog: str = CATALOG) -> str:
    return f"{catalog}.{schema}.{table}"

def target_fqn() -> str:
    return f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

def assert_exists(table_fqn: str) -> None:
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(f"Missing required table: {table_fqn}")

def ensure_schema(catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def _find_duplicate_columns(cols: List[str]) -> List[str]:
    seen, dups = set(), []
    for c in cols:
        if c in seen and c not in dups:
            dups.append(c)
        seen.add(c)
    return dups

def _schema_map(schema: T.StructType) -> Dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in schema.fields}

def log_schema_diff(df: DataFrame, target: str) -> None:
    if not spark.catalog.tableExists(target):
        log("Target table does not exist yet; schema diff skipped.")
        return
    df_map = _schema_map(df.schema)
    tgt_map = _schema_map(spark.table(target).schema)

    mismatched = []
    for k, tgt_t in tgt_map.items():
        if k in df_map and df_map[k] != tgt_t:
            mismatched.append((k, df_map[k], tgt_t))

    if mismatched:
        log("Schema type mismatches vs target (df_type -> target_type):")
        for k, a, b in mismatched[:100]:
            print(f"  - {k}: {a} -> {b}")
    else:
        log("No type mismatches vs target for existing columns.")

def conform_to_existing_delta_schema(df: DataFrame, target: str) -> DataFrame:
    """
    Cast df columns to match existing Delta table types to avoid merge conflicts.
    Also add any missing target columns as NULL (cast to target type).
    """
    if not spark.catalog.tableExists(target):
        return df

    tgt_schema = spark.table(target).schema
    tgt_types = {f.name: f.dataType for f in tgt_schema.fields}
    df_cols = set(df.columns)

    out = df
    for name, dtype in tgt_types.items():
        if name in df_cols:
            out = out.withColumn(name, F.col(name).cast(dtype))
        else:
            out = out.withColumn(name, F.lit(None).cast(dtype))

    ordered = [f.name for f in tgt_schema.fields] + [c for c in df.columns if c not in tgt_types]
    out = out.select(*ordered)

    dups = _find_duplicate_columns(out.columns)
    if dups:
        raise RuntimeError(f"Duplicate column names after conforming schema: {dups}")

    return out


# =========================
# 0) Diagnostics
# =========================
def diagnose_inputs() -> None:
    log("=== DIAGNOSTICS: required tables ===")
    required = [
        "table_assets",
        "table_asset_sources",
        "workbooks",
        "views",
        "projects",
        "datasources",
        "data_connections",
    ]
    for t in required:
        full = fqn(TABLEAU_META_SCHEMA, t)
        assert_exists(full)
        log(f"Found: {full}")

    np_tables = spark.sql(f"""
      SELECT lower(table_name) AS table_name_lc
      FROM {NP_CATALOG}.information_schema.tables
      WHERE lower(table_schema) = lower('{NP_SCHEMA}')
    """)
    np_cnt = np_tables.count()
    log(f"NP tables in {NP_CATALOG}.{NP_SCHEMA}: {np_cnt:,}")
    if np_cnt == 0:
        raise RuntimeError(f"No tables found in {NP_CATALOG}.{NP_SCHEMA} (check perms or schema name).")

    ta = spark.table(fqn(TABLEAU_META_SCHEMA, "table_assets"))
    tas = spark.table(fqn(TABLEAU_META_SCHEMA, "table_asset_sources"))

    # Match by full_name (not table_schema)
    ta_match = (
        ta.withColumn("full_name_lc", F.lower(F.coalesce(F.col("full_name"), F.lit(""))))
          .where(F.col("full_name_lc").like(f"%{NP_SCHEMA.lower()}%"))
    )
    if "is_tombstoned" in ta.columns:
        ta_match = ta_match.where(~F.coalesce(F.col("is_tombstoned"), F.lit(False)))

    match_cnt = ta_match.count()
    log(f"table_assets where full_name contains '{NP_SCHEMA}': {match_cnt:,}")

    if DEBUG_SAMPLES and match_cnt > 0:
        cols = ["site_id", "id", "name", "full_name"]
        if "table_schema" in ta.columns:
            cols.insert(3, "table_schema")
        display(ta_match.select(*cols).limit(30))

    if "table_asset_id" not in tas.columns:
        raise RuntimeError("Expected table_asset_sources.table_asset_id not found.")

    tas_match = tas.join(
        ta_match.select(
            F.col("site_id").cast("long").alias("s_site_id"),
            F.col("id").cast("long").alias("s_table_asset_id"),
        ),
        (F.col("site_id").cast("long") == F.col("s_site_id")) & (F.col("table_asset_id").cast("long") == F.col("s_table_asset_id")),
        "inner",
    )

    log("source_type distribution among those assets:")
    display(
        tas_match.groupBy(F.lower(F.col("source_type")).alias("source_type"))
                 .count()
                 .orderBy(F.desc("count"))
    )


# =========================
# 1) Build ROW-LEVEL mapping
# =========================
def build_rowlevel_mapping() -> DataFrame:
    ta = spark.table(fqn(TABLEAU_META_SCHEMA, "table_assets"))
    tas = spark.table(fqn(TABLEAU_META_SCHEMA, "table_asset_sources"))

    wb = spark.table(fqn(TABLEAU_META_SCHEMA, "workbooks"))
    vw = spark.table(fqn(TABLEAU_META_SCHEMA, "views"))
    pr = spark.table(fqn(TABLEAU_META_SCHEMA, "projects"))
    ds = spark.table(fqn(TABLEAU_META_SCHEMA, "datasources"))
    dc = spark.table(fqn(TABLEAU_META_SCHEMA, "data_connections"))

    # Canonical NP tables list (to validate parsing)
    np_tables = spark.sql(f"""
      SELECT table_name, lower(table_name) AS table_name_lc
      FROM {NP_CATALOG}.information_schema.tables
      WHERE lower(table_schema) = lower('{NP_SCHEMA}')
    """)

    # 1) Filter assets by full_name containing npdataset
    ta_f = (
        ta.withColumn("full_name_lc", F.lower(F.coalesce(F.col("full_name"), F.lit(""))))
          .where(F.col("full_name_lc").like(f"%{NP_SCHEMA.lower()}%"))
    )
    if "is_tombstoned" in ta.columns:
        ta_f = ta_f.where(~F.coalesce(F.col("is_tombstoned"), F.lit(False)))

    # 2) Parse table name from full_name:
    #    Normalize by removing []"` and lowercasing, then extract token after "npdataset."
    full_clean = F.lower(F.regexp_replace(F.coalesce(F.col("full_name"), F.lit("")), r'[$begin:math:display$$end:math:display$"`]', ""))
    parsed = F.regexp_extract(full_clean, r"npdataset\.([a-z0-9_]+)", 1)

    ta_parsed = (
        ta_f
        .withColumn("parsed_table_name", F.when(F.length(parsed) > 0, parsed).otherwise(F.lower(F.col("name"))))
        .withColumn("parsed_table_name_lc", F.lower(F.col("parsed_table_name")))
    )

    # 3) Validate parsed table against actual npdataset tables (avoids false positives)
    ta_np = (
        ta_parsed.join(np_tables, ta_parsed.parsed_table_name_lc == np_tables.table_name_lc, "inner")
                 .select(
                     F.col("site_id").cast("long").alias("site_id"),
                     F.col("id").cast("long").alias("table_asset_id"),
                     F.lit(NP_SCHEMA).alias("np_table_schema"),
                     F.col("table_name").alias("np_table_name"),  # canonical
                     F.col("full_name").alias("table_full_name"),
                     F.col("name").alias("asset_name"),
                     F.col("parsed_table_name").alias("parsed_table_name"),
                     (F.col("table_schema") if "table_schema" in ta.columns else F.lit(None)).alias("asset_table_schema"),
                 )
    )

    ta_np_cnt = ta_np.count()
    log(f"Assets resolved to actual {NP_SCHEMA} tables: {ta_np_cnt:,}")
    if ta_np_cnt == 0:
        display(ta_f.select("full_name").where(F.col("full_name").isNotNull()).limit(50))
        raise RuntimeError("No assets resolved to actual npdataset tables; adjust full_name parsing regex.")

    # 4) Join to sources
    if "table_asset_id" not in tas.columns:
        raise RuntimeError("Expected table_asset_sources.table_asset_id not found.")

    sources = (
        tas.select(
            F.col("site_id").cast("long").alias("site_id"),
            F.col("table_asset_id").cast("long").alias("table_asset_id"),
            F.lower(F.col("source_type")).alias("source_type"),
            F.col("source_id").cast("long").alias("source_id"),
        )
        .join(ta_np, on=["site_id", "table_asset_id"], how="inner")
    )

    # Dimensions
    workbooks = (
        wb.where(~F.coalesce(F.col("is_deleted"), F.lit(False)))
          .select(
              F.col("site_id").cast("long").alias("site_id"),
              F.col("id").cast("long").alias("workbook_id"),
              F.col("name").alias("workbook_name"),
              F.col("repository_url").alias("workbook_repo_url"),
              F.col("project_id").alias("project_id"),
              F.col("luid").alias("workbook_luid"),
          )
    )

    projects = pr.select(
        F.col("site_id").cast("long").alias("site_id"),
        F.col("id").alias("project_id"),
        F.col("name").alias("project_name"),
    )

    dashboards = (
        vw.where((F.lower(F.col("sheettype")) == F.lit("dashboard")) & (~F.coalesce(F.col("is_deleted"), F.lit(False))))
          .select(
              F.col("site_id").cast("long").alias("site_id"),
              F.col("workbook_id").cast("long").alias("workbook_id"),
              F.col("id").cast("long").alias("dashboard_id"),
              F.col("name").alias("dashboard_name"),
              F.col("repository_url").alias("dashboard_repo_url"),
              F.col("luid").alias("dashboard_luid"),
          )
    )

    datasources = ds.select(
        F.col("site_id").cast("long").alias("site_id"),
        F.col("id").cast("long").alias("datasource_id"),
        F.col("name").alias("datasource_name"),
        F.col("repository_url").alias("datasource_repo_url"),
        F.col("luid").alias("datasource_luid"),
    )

    # Consumers: workbooks that use a published datasource (via workbook-owned data_connections)
    consumers = (
        dc.where((F.lower(F.col("owner_type")) == F.lit("workbook")) & F.col("datasource_id").isNotNull())
          .select(
              F.col("site_id").cast("long").alias("site_id"),
              F.col("datasource_id").cast("long").alias("datasource_id"),
              F.col("owner_id").cast("long").alias("workbook_id"),
          )
          .distinct()
    )

    # Connection mode for datasource (live/extract/mixed)
    ds_mode = (
        dc.where(F.lower(F.col("owner_type")) == F.lit("datasource"))
          .select(
              F.col("site_id").cast("long").alias("site_id"),
              F.col("owner_id").cast("long").alias("datasource_id"),
              F.coalesce(F.col("has_extract"), F.lit(False)).alias("has_extract"),
          )
          .groupBy("site_id", "datasource_id")
          .agg(
              F.count("*").alias("num_connections"),
              F.sum(F.col("has_extract").cast("int")).alias("num_extract_connections"),
          )
          .withColumn(
              "connection_mode",
              F.when(F.col("num_connections").isNull(), F.lit("unknown"))
               .when(F.col("num_extract_connections") == 0, F.lit("live"))
               .when(F.col("num_extract_connections") == F.col("num_connections"), F.lit("extract"))
               .otherwise(F.lit("mixed"))
          )
          .select("site_id", "datasource_id", "connection_mode")
    )

    # Path A: workbook sources
    wb_path = (
        sources.where(F.col("source_type") == F.lit("workbook"))
               .withColumn("link_path", F.lit("table_asset_source_workbook"))
               .withColumnRenamed("source_id", "workbook_id")
               .join(workbooks, on=["site_id", "workbook_id"], how="left")
               .join(projects, on=["site_id", "project_id"], how="left")
               .join(dashboards, on=["site_id", "workbook_id"], how="left")
               .select(
                   "site_id","table_asset_id","np_table_schema","np_table_name","table_full_name",
                   "asset_name","parsed_table_name","asset_table_schema",
                   "link_path",
                   F.lit("workbook").alias("source_type"),
                   F.lit(None).cast("long").alias("datasource_id"),
                   F.lit(None).cast("string").alias("datasource_name"),
                   F.lit(None).cast("string").alias("datasource_repo_url"),
                   F.lit(None).cast("string").alias("datasource_luid"),
                   F.lit(None).cast("string").alias("connection_mode"),
                   "project_name",
                   "workbook_id","workbook_name","workbook_repo_url","workbook_luid",
                   "dashboard_id","dashboard_name","dashboard_repo_url","dashboard_luid",
               )
    )

    # Path B: datasource sources → consumers → workbooks → dashboards
    ds_path_base = (
        sources.where(F.col("source_type") == F.lit("datasource"))
               .withColumn("link_path", F.lit("table_asset_source_datasource"))
               .withColumnRenamed("source_id", "datasource_id")
               .join(datasources, on=["site_id", "datasource_id"], how="left")
               .join(ds_mode, on=["site_id", "datasource_id"], how="left")
               .join(consumers, on=["site_id", "datasource_id"], how="left")
               .join(workbooks, on=["site_id", "workbook_id"], how="left")
               .join(projects, on=["site_id", "project_id"], how="left")
               .join(dashboards, on=["site_id", "workbook_id"], how="left")
    )

    if not INCLUDE_UNUSED_DATASOURCES:
        ds_path_base = ds_path_base.where(F.col("workbook_id").isNotNull())

    ds_path = ds_path_base.select(
        "site_id","table_asset_id","np_table_schema","np_table_name","table_full_name",
        "asset_name","parsed_table_name","asset_table_schema",
        "link_path",
        F.lit("datasource").alias("source_type"),
        "datasource_id","datasource_name","datasource_repo_url","datasource_luid","connection_mode",
        "project_name",
        "workbook_id","workbook_name","workbook_repo_url","workbook_luid",
        "dashboard_id","dashboard_name","dashboard_repo_url","dashboard_luid",
    )

    out = wb_path.unionByName(ds_path, allowMissingColumns=True)

    out = out.withColumn("uc_table_full_name", F.concat(F.lit(f"{NP_CATALOG}.{NP_SCHEMA}."), F.col("np_table_name")))

    dups = _find_duplicate_columns(out.columns)
    if dups:
        raise RuntimeError(f"Duplicate column names produced in output: {dups}")

    return out


# =========================
# 2) Write to Delta (daily partition, idempotent)
# =========================
def write_daily_partition(df: DataFrame, snapshot_date: date) -> str:
    ensure_schema(CATALOG, TARGET_SCHEMA)
    tgt = target_fqn()
    snap_str = snapshot_date.isoformat()

    if RECREATE_TABLE and spark.catalog.tableExists(tgt):
        log(f"RECREATE_TABLE=True → Dropping existing table: {tgt}")
        spark.sql(f"DROP TABLE {tgt}")

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    df_out = (
        df.withColumn("snapshot_date", F.lit(snap_str).cast("date"))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("ingest_run_id", F.lit(str(uuid.uuid4())))
          .withColumn(
              "row_key",
              F.sha2(
                  F.concat_ws(
                      "||",
                      F.coalesce(F.col("site_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("table_asset_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("source_type").cast("string"), F.lit("")),
                      F.coalesce(F.col("datasource_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("workbook_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("dashboard_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("uc_table_full_name").cast("string"), F.lit("")),
                  ),
                  256,
              )
          )
    )

    # Prevent Delta merge conflicts by conforming types if table already exists
    log_schema_diff(df_out, tgt)
    df_out = conform_to_existing_delta_schema(df_out, tgt)

    if not spark.catalog.tableExists(tgt):
        log(f"Creating table {tgt}")
        (df_out.write.format("delta")
             .mode("overwrite")
             .partitionBy("snapshot_date")
             .saveAsTable(tgt))
    else:
        log(f"Overwriting partition snapshot_date={snap_str} in {tgt} (idempotent)")
        (df_out.write.format("delta")
             .mode("overwrite")
             .option("replaceWhere", f"snapshot_date = '{snap_str}'")
             .option("mergeSchema", "true")
             .saveAsTable(tgt))

    return tgt


# =========================
# RUN
# =========================
log("=== STEP 0: Diagnostics ===")
diagnose_inputs()

log("=== STEP 1: Build row-level mapping ===")
df_metrics = build_rowlevel_mapping()
cnt = df_metrics.count()
log(f"df_metrics rows: {cnt:,}")

if cnt == 0 and FAIL_JOB_IF_EMPTY:
    raise RuntimeError("df_metrics is empty — failing job to avoid writing empty partition.")

if DEBUG_SAMPLES:
    display(df_metrics.orderBy("np_table_name", "datasource_name", "workbook_name", "dashboard_name").limit(200))

log("=== STEP 2: Write output ===")
tgt = write_daily_partition(df_metrics, snapshot_date=SNAPSHOT_DATE)
log(f"Wrote: {tgt}")

df_today = spark.table(tgt).where(F.col("snapshot_date") == F.lit(SNAPSHOT_DATE.isoformat()).cast("date"))
log(f"Rows written for {SNAPSHOT_DATE.isoformat()}: {df_today.count():,}")
display(df_today.orderBy("np_table_name", "datasource_name", "workbook_name", "dashboard_name").limit(200))

log("Job complete.")

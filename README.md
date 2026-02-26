# Databricks (PySpark) — Tableau Repo (PUBLIC-only) → row-level npdataset usage → Delta table (daily incremental partitions)
#
# What you get (ROW LEVEL):
#   One row per (matched npdataset table) × (published datasource OR workbook_direct) × (downstream workbook) × (dashboard)
#   + metadata (luid, repository_url, extract vs live/mixed)
#
# Incremental strategy that won’t miss due to retries:
#   - Table is PARTITIONED by snapshot_date
#   - Each run OVERWRITES ONLY today’s partition (idempotent)
#   - Schedule as a daily job (or more frequent); re-runs won’t duplicate data
#
# Why you got 0 rows previously:
#   Most common causes:
#     1) data_connections.tablename is blank/null for your Databricks connections
#     2) tablename doesn’t contain schema-qualified names (needs unqualified matching)
#     3) you filtered to only_matches=True and there were simply no matches
#
# This notebook block adds DIAGNOSTICS + LOGGING so you can see exactly where the mismatch is.

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional, List

from pyspark.sql import DataFrame, functions as F


# -----------------------------
# CONFIG (placeholders for reuse)
# -----------------------------
CATALOG = "eciscor_prod"
TABLEAU_SCHEMA = "ec_tableau_meta"           # UC schema containing Tableau repo tables (you renamed "public" -> this)
NP_CATALOG = "eciscor_prod"                  # where npdataset lives
NP_SCHEMA = "npdataset"                      # schema containing the tables you care about

TARGET_SCHEMA = "cdo"
TARGET_TABLE = "npdatasetmetrics_tableau"    # eciscor_prod.cdo.npdatasetmetrics_tableau

# Matching knobs
MATCH_UNQUALIFIED_TABLE_NAMES = False        # flip True only if tablename often stores just "my_table"
ONLY_MATCHES = True                          # write only rows that match npdataset
FAIL_JOB_IF_EMPTY = True                     # fail the job if output is empty (prevents writing an empty day)


# -----------------------------
# Helpers (logging + diagnostics)
# -----------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def table_fqn(schema: str, table: str, catalog: str = CATALOG) -> str:
    return f"{catalog}.{schema}.{table}"

def safe_count(df: DataFrame, name: str, sample_cols: Optional[List[str]] = None) -> int:
    c = df.count()
    log(f"{name}: {c:,} rows")
    if c > 0:
        if sample_cols:
            display(df.select(*sample_cols).limit(20))
        else:
            display(df.limit(20))
    return c

def assert_table_exists(fqn: str) -> None:
    if not spark.catalog.tableExists(fqn):
        raise RuntimeError(f"Required table not found: {fqn}")

def describe_if_exists(fqn: str) -> None:
    if spark.catalog.tableExists(fqn):
        log(f"DESCRIBE {fqn}")
        display(spark.sql(f"DESCRIBE TABLE {fqn}"))
    else:
        log(f"DESCRIBE skipped (missing): {fqn}")

def run_diagnostics(
    *,
    catalog: str,
    tableau_schema: str,
    np_catalog: str,
    np_schema: str,
) -> None:
    """
    Prints diagnostics to explain why matches might be 0.
    """
    log("=== DIAGNOSTICS START ===")

    # Required Tableau tables (public repo ingested into UC schema)
    dc_fqn = table_fqn(tableau_schema, "data_connections", catalog)
    ds_fqn = table_fqn(tableau_schema, "datasources", catalog)
    wb_fqn = table_fqn(tableau_schema, "workbooks", catalog)
    vw_fqn = table_fqn(tableau_schema, "views", catalog)
    pr_fqn = table_fqn(tableau_schema, "projects", catalog)

    for f in [dc_fqn, ds_fqn, wb_fqn, vw_fqn, pr_fqn]:
        assert_table_exists(f)
        log(f"Found: {f}")

    # NP tables count
    np_tables_df = spark.sql(f"""
        SELECT lower(table_name) AS table_name
        FROM {np_catalog}.information_schema.tables
        WHERE lower(table_schema) = lower('{np_schema}')
    """)
    np_cnt = np_tables_df.count()
    log(f"np_tables in {np_catalog}.{np_schema}: {np_cnt:,}")
    if np_cnt == 0:
        log("!!! No tables found in npdataset. Check NP_CATALOG/NP_SCHEMA or permissions.")
        log("=== DIAGNOSTICS END ===")
        return

    # Basic data_connections profiling
    dc = spark.table(dc_fqn)
    log("data_connections schema (columns):")
    print(dc.columns)

    safe_count(dc, "data_connections (raw)", sample_cols=["site_id", "owner_type", "owner_id", "datasource_id", "has_extract", "tablename"])

    # How many tablename are blank?
    dc_tab = dc.select(
        F.lower(F.col("owner_type")).alias("owner_type_lc"),
        F.col("tablename").alias("tablename")
    )
    blanks = dc_tab.where((F.col("tablename").isNull()) | (F.trim(F.col("tablename")) == "")).count()
    total = dc_tab.count()
    log(f"data_connections.tablename blank/null: {blanks:,} / {total:,} ({(blanks/total*100):.1f}%)")

    # Owner type distribution
    log("Owner type distribution:")
    display(dc_tab.groupBy("owner_type_lc").count().orderBy(F.desc("count")))

    # Quick “does anything even mention npdataset?” scan
    log("Quick scan: tablename contains 'npdataset' (case-insensitive)")
    contains_npd = dc.where(F.lower(F.coalesce(F.col("tablename"), F.lit(""))).like("%npdataset%"))
    safe_count(contains_npd, "Rows where tablename LIKE '%npdataset%'", sample_cols=["owner_type", "owner_id", "datasource_id", "has_extract", "tablename"])

    # If 0, show a sample of non-empty tablename values to see what it looks like
    if contains_npd.count() == 0:
        log("No tablename contains 'npdataset'. Showing sample of distinct non-empty tablename strings:")
        sample_tblnames = (
            dc.where((F.col("tablename").isNotNull()) & (F.trim(F.col("tablename")) != ""))
              .select(F.lower(F.col("tablename")).alias("tablename_lc"))
              .distinct()
              .limit(50)
        )
        display(sample_tblnames)

    log("=== DIAGNOSTICS END ===")


# -----------------------------
# Core build (ROW LEVEL matches)
# -----------------------------
def build_tableau_npdataset_usage_public_only_rowlevel(
    *,
    catalog: str = CATALOG,
    tableau_schema: str = TABLEAU_SCHEMA,
    np_catalog: str = NP_CATALOG,
    np_schema: str = NP_SCHEMA,
    match_unqualified_table_names: bool = MATCH_UNQUALIFIED_TABLE_NAMES,
    only_matches: bool = ONLY_MATCHES,
) -> DataFrame:
    """
    Returns ROW-LEVEL matches:
      - one row per matched npdataset table × (datasource/workbook_direct) × workbook × dashboard
    """
    dc_fqn = table_fqn(tableau_schema, "data_connections", catalog)
    ds_fqn = table_fqn(tableau_schema, "datasources", catalog)
    wb_fqn = table_fqn(tableau_schema, "workbooks", catalog)
    vw_fqn = table_fqn(tableau_schema, "views", catalog)
    pr_fqn = table_fqn(tableau_schema, "projects", catalog)
    is_tables_fqn = f"{np_catalog}.information_schema.tables"

    # Optional unqualified match clause (danger: false positives if table names collide across schemas)
    unqualified_clause = "OR tablename_norm = nt.table_name" if match_unqualified_table_names else ""

    # If only_matches False, we still only produce “match rows” in this rowlevel function.
    # (If you want all datasources with uses_npdataset=false rows, that’s a different output shape.)
    where_only_matches = ""  # rowlevel hits are inherently "matches"

    sql = f"""
WITH np_tables AS (
  SELECT lower(table_name) AS table_name
  FROM {is_tables_fqn}
  WHERE lower(table_schema) = lower('{np_schema}')
),

dc_norm AS (
  SELECT
    site_id,
    id AS data_connection_id,
    lower(owner_type) AS owner_type_lc,
    owner_id,
    datasource_id,
    has_extract,
    -- normalize: strip common quoting and lower
    lower(
      replace(
        replace(
          replace(
            replace(coalesce(tablename, ''), '`', ''),
          '"', ''),
        '[', ''),
      ']', '')
    ) AS tablename_norm
  FROM {dc_fqn}
),

-- -----------------------------
-- A) Published datasources: match npdataset tables via datasource-owned connections
-- -----------------------------
published_ds_hits AS (
  SELECT
    p.site_id,
    p.data_connection_id,
    p.owner_id AS datasource_id,
    p.has_extract,
    p.tablename_norm,
    nt.table_name AS matched_table
  FROM dc_norm p
  JOIN np_tables nt
    ON p.owner_type_lc = 'datasource'
   AND (
        p.tablename_norm LIKE concat('%', lower('{np_schema}'), '.', nt.table_name, '%')
     OR p.tablename_norm LIKE concat('%', lower('{np_catalog}'), '.', lower('{np_schema}'), '.', nt.table_name, '%')
     {unqualified_clause}
   )
),

published_ds_mode AS (
  SELECT
    site_id,
    datasource_id,
    count(*) AS num_connections,
    sum(CASE WHEN has_extract THEN 1 ELSE 0 END) AS num_extract_connections
  FROM dc_norm
  WHERE owner_type_lc = 'datasource'
  GROUP BY 1,2
),

-- Workbooks that consume a published datasource (workbook-owned connections with datasource_id set)
published_ds_consumers AS (
  SELECT DISTINCT
    site_id,
    datasource_id,
    owner_id AS workbook_id
  FROM dc_norm
  WHERE owner_type_lc = 'workbook'
    AND datasource_id IS NOT NULL
),

datasources_dim AS (
  SELECT
    site_id,
    id   AS datasource_id,
    name AS datasource_name,
    repository_url AS datasource_repo_url,
    luid AS datasource_luid
  FROM {ds_fqn}
),

workbooks_dim AS (
  SELECT
    site_id,
    id   AS workbook_id,
    name AS workbook_name,
    repository_url AS workbook_repo_url,
    project_id,
    luid AS workbook_luid
  FROM {wb_fqn}
  WHERE coalesce(is_deleted, false) = false
),

projects_dim AS (
  SELECT
    site_id,
    id   AS project_id,
    name AS project_name
  FROM {pr_fqn}
),

dashboards_dim AS (
  SELECT
    site_id,
    workbook_id,
    id   AS dashboard_id,
    name AS dashboard_name,
    repository_url AS dashboard_repo_url,
    luid AS dashboard_luid
  FROM {vw_fqn}
  WHERE lower(sheettype) = 'dashboard'
    AND coalesce(is_deleted, false) = false
),

published_rows AS (
  SELECT
    h.site_id,
    'published_datasource' AS datasource_kind,

    ds.datasource_id,
    ds.datasource_name,
    ds.datasource_repo_url,
    ds.datasource_luid,

    true AS uses_npdataset,
    h.matched_table,
    h.data_connection_id,
    h.has_extract,
    h.tablename_norm AS matched_from_tablename,

    CASE
      WHEN m.num_connections IS NULL THEN 'unknown'
      WHEN m.num_extract_connections = 0 THEN 'live'
      WHEN m.num_extract_connections = m.num_connections THEN 'extract'
      ELSE 'mixed'
    END AS connection_mode,

    pr.project_name,

    wb.workbook_id,
    wb.workbook_name,
    wb.workbook_repo_url,
    wb.workbook_luid,

    db.dashboard_id,
    db.dashboard_name,
    db.dashboard_repo_url,
    db.dashboard_luid

  FROM published_ds_hits h
  LEFT JOIN datasources_dim ds
    ON ds.site_id = h.site_id AND ds.datasource_id = h.datasource_id
  LEFT JOIN published_ds_mode m
    ON m.site_id = h.site_id AND m.datasource_id = h.datasource_id
  LEFT JOIN published_ds_consumers c
    ON c.site_id = h.site_id AND c.datasource_id = h.datasource_id
  LEFT JOIN workbooks_dim wb
    ON wb.site_id = c.site_id AND wb.workbook_id = c.workbook_id
  LEFT JOIN projects_dim pr
    ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
  LEFT JOIN dashboards_dim db
    ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id
),

-- -----------------------------
-- B) Workbook-direct connections (no published datasource): match npdataset via workbook-owned connections where datasource_id is null
-- -----------------------------
workbook_direct_hits AS (
  SELECT
    w.site_id,
    w.data_connection_id,
    w.owner_id AS workbook_id,
    w.has_extract,
    w.tablename_norm,
    nt.table_name AS matched_table
  FROM dc_norm w
  JOIN np_tables nt
    ON w.owner_type_lc = 'workbook'
   AND w.datasource_id IS NULL
   AND (
        w.tablename_norm LIKE concat('%', lower('{np_schema}'), '.', nt.table_name, '%')
     OR w.tablename_norm LIKE concat('%', lower('{np_catalog}'), '.', lower('{np_schema}'), '.', nt.table_name, '%')
     {unqualified_clause}
   )
),

workbook_direct_mode AS (
  SELECT
    site_id,
    owner_id AS workbook_id,
    count(*) AS num_connections,
    sum(CASE WHEN has_extract THEN 1 ELSE 0 END) AS num_extract_connections
  FROM dc_norm
  WHERE owner_type_lc = 'workbook' AND datasource_id IS NULL
  GROUP BY 1,2
),

workbook_direct_rows AS (
  SELECT
    h.site_id,
    'workbook_direct' AS datasource_kind,

    CAST(NULL AS BIGINT) AS datasource_id,
    CAST(NULL AS STRING) AS datasource_name,
    CAST(NULL AS STRING) AS datasource_repo_url,
    CAST(NULL AS STRING) AS datasource_luid,

    true AS uses_npdataset,
    h.matched_table,
    h.data_connection_id,
    h.has_extract,
    h.tablename_norm AS matched_from_tablename,

    CASE
      WHEN m.num_connections IS NULL THEN 'unknown'
      WHEN m.num_extract_connections = 0 THEN 'live'
      WHEN m.num_extract_connections = m.num_connections THEN 'extract'
      ELSE 'mixed'
    END AS connection_mode,

    pr.project_name,

    wb.workbook_id,
    wb.workbook_name,
    wb.workbook_repo_url,
    wb.workbook_luid,

    db.dashboard_id,
    db.dashboard_name,
    db.dashboard_repo_url,
    db.dashboard_luid

  FROM workbook_direct_hits h
  JOIN workbooks_dim wb
    ON wb.site_id = h.site_id AND wb.workbook_id = h.workbook_id
  LEFT JOIN workbook_direct_mode m
    ON m.site_id = h.site_id AND m.workbook_id = h.workbook_id
  LEFT JOIN projects_dim pr
    ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
  LEFT JOIN dashboards_dim db
    ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id
)

SELECT * FROM published_rows
UNION ALL
SELECT * FROM workbook_direct_rows
{where_only_matches}
ORDER BY
  datasource_kind, datasource_name, workbook_name, dashboard_name, matched_table
"""
    df = spark.sql(sql)

    if only_matches:
        # rowlevel output is already only matches; keep for symmetry
        return df
    else:
        return df


# -----------------------------
# Write (daily incremental partitions)
# -----------------------------
def write_daily_partition(
    df: DataFrame,
    *,
    target_catalog: str = CATALOG,
    target_schema: str = TARGET_SCHEMA,
    target_table: str = TARGET_TABLE,
    snapshot_date: Optional[date] = None,
    full_refresh: bool = False,
) -> str:
    """
    Writes df to Delta table partitioned by snapshot_date.
      - full_refresh/create: overwrite entire table
      - daily run: overwrite ONLY the snapshot_date partition (replaceWhere) — idempotent

    NOTE: This is still "row-level"; snapshot_date is just an ingestion partition to support incremental loads.
    """
    snapshot_date = snapshot_date or date.today()
    snapshot_date_str = snapshot_date.isoformat()

    target_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

    df_out = (
        df
        .withColumn("snapshot_date", F.lit(snapshot_date_str).cast("date"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(str(uuid.uuid4())))
        # stable key for de-duping within a day if you ever want it
        .withColumn(
            "row_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("site_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("datasource_kind").cast("string"), F.lit("")),
                    F.coalesce(F.col("datasource_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("workbook_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("dashboard_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("matched_table").cast("string"), F.lit("")),
                ),
                256
            )
        )
    )

    table_exists = spark.catalog.tableExists(target_fqn)
    if full_refresh or not table_exists:
        log(f"Writing FULL REFRESH to {target_fqn} (partitioned by snapshot_date)")
        (df_out.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .saveAsTable(target_fqn))
    else:
        log(f"Writing DAILY PARTITION to {target_fqn} for snapshot_date={snapshot_date_str} (idempotent overwrite)")
        (df_out.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'")
            .saveAsTable(target_fqn))

    return target_fqn


# -----------------------------
# RUN: Diagnose → Build → Write
# -----------------------------
log("Starting Tableau→npdataset metrics job")

# 1) Diagnose (this will show why you might be getting 0 rows)
run_diagnostics(
    catalog=CATALOG,
    tableau_schema=TABLEAU_SCHEMA,
    np_catalog=NP_CATALOG,
    np_schema=NP_SCHEMA,
)

# 2) Build row-level matches
log("Building row-level dataset (PUBLIC-only matching via data_connections.tablename)")
df_metrics = build_tableau_npdataset_usage_public_only_rowlevel(
    catalog=CATALOG,
    tableau_schema=TABLEAU_SCHEMA,
    np_catalog=NP_CATALOG,
    np_schema=NP_SCHEMA,
    match_unqualified_table_names=MATCH_UNQUALIFIED_TABLE_NAMES,
    only_matches=ONLY_MATCHES,
)

metrics_cnt = df_metrics.count()
log(f"Built df_metrics: {metrics_cnt:,} rows")

if metrics_cnt == 0:
    log("!!! df_metrics is empty.")
    log("Next steps to pinpoint cause:")
    log("  - Check diagnostic output: do any data_connections.tablename values contain 'npdataset'?")
    log("  - If not, your environment likely doesn’t store physical table names in public.data_connections.tablename")
    log("  - If tablename has only unqualified names, set MATCH_UNQUALIFIED_TABLE_NAMES=True (risk: false positives)")
    if FAIL_JOB_IF_EMPTY:
        raise RuntimeError("df_metrics is empty — failing job to prevent writing an empty partition.")
else:
    display(df_metrics.limit(50))

# 3) Write to eciscor_prod.cdo.npdatasetmetrics_tableau (daily incremental partition)
target_fqn = write_daily_partition(
    df_metrics,
    target_catalog=CATALOG,
    target_schema=TARGET_SCHEMA,
    target_table=TARGET_TABLE,
    snapshot_date=None,     # today
    full_refresh=False,     # set True once for initial create/replace
)

log(f"Write complete: {target_fqn}")

# 4) Quick verification for today's partition
log("Verifying today's partition")
today_str = date.today().isoformat()
df_today = spark.table(target_fqn).where(F.col("snapshot_date") == F.lit(today_str).cast("date"))
log(f"Rows written today: {df_today.count():,}")
display(df_today.orderBy(F.col("datasource_kind"), F.col("datasource_name"), F.col("workbook_name"), F.col("matched_table")).limit(100))

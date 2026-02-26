# Databricks (PySpark) — Tableau repo (PUBLIC-ONLY) → npdataset usage → write daily snapshot
#
# What this produces (row-level):
#   - published datasource OR workbook_direct
#   - whether it (best-effort) references eciscor_prod.npdataset.* based on Tableau repo:
#       eciscor_prod.ec_tableau_meta.data_connections.tablename
#   - downstream workbooks + dashboards (for published datasources)
#
# Incremental strategy (safe / “don’t miss data”):
#   - Write a DAILY SNAPSHOT partitioned by snapshot_date
#   - Each run OVERWRITES ONLY that day’s partition (idempotent; reruns won’t duplicate)
#   - This guarantees you never “miss” a day due to retries/re-runs.
#   - Note: This is *design-time metadata*, not event history; if you need intraday changes, run more frequently.

from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, functions as F


def build_tableau_npdataset_usage_public_only(
    *,
    catalog: str = "eciscor_prod",
    tableau_schema: str = "ec_tableau_meta",
    np_catalog: Optional[str] = None,
    np_schema: str = "npdataset",
    match_unqualified_table_names: bool = False,
    only_matches: bool = True,
) -> DataFrame:
    """
    Build a row-level DataFrame from Tableau Server Postgres repository tables (PUBLIC-ONLY approach).

    Returns columns:
      datasource_kind, datasource_id, datasource_name, datasource_repo_url, datasource_luid,
      uses_npdataset, matched_table_count, matched_tables,
      connection_mode,
      project_name,
      workbook_id, workbook_name, workbook_repo_url, workbook_luid,
      dashboard_id, dashboard_name, dashboard_repo_url, dashboard_luid
    """
    np_catalog = np_catalog or catalog

    # Fully-qualified UC table names
    dc_fqn = f"{catalog}.{tableau_schema}.data_connections"
    ds_fqn = f"{catalog}.{tableau_schema}.datasources"
    wb_fqn = f"{catalog}.{tableau_schema}.workbooks"
    vw_fqn = f"{catalog}.{tableau_schema}.views"
    pr_fqn = f"{catalog}.{tableau_schema}.projects"
    is_tables_fqn = f"{np_catalog}.information_schema.tables"

    # Optional unqualified match clause
    unqualified_clause = "OR tablename_norm = nt.table_name" if match_unqualified_table_names else ""

    # If only_matches=True, filter to matching published datasources only
    published_filter = "WHERE nr.datasource_id IS NOT NULL" if only_matches else ""

    sql = f"""
WITH np_tables AS (
  SELECT lower(table_name) AS table_name
  FROM {is_tables_fqn}
  WHERE lower(table_schema) = lower('{np_schema}')
),

-- Normalize connection tablename strings for matching
dc_norm AS (
  SELECT
    site_id,
    id AS data_connection_id,
    lower(owner_type) AS owner_type_lc,
    owner_id,
    datasource_id,
    has_extract,
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
-- A) PUBLISHED DATASOURCES
-- -----------------------------
published_ds_connections AS (
  SELECT
    site_id,
    data_connection_id,
    owner_id AS datasource_id,     -- owner_type='datasource' => published datasource id
    has_extract,
    tablename_norm
  FROM dc_norm
  WHERE owner_type_lc = 'datasource'
),

published_ds_np_hits AS (
  SELECT
    p.site_id,
    p.datasource_id,
    nt.table_name AS matched_table
  FROM published_ds_connections p
  JOIN np_tables nt
    ON  p.tablename_norm LIKE concat('%', lower('{np_schema}'), '.', nt.table_name, '%')
     OR p.tablename_norm LIKE concat('%', lower('{np_catalog}'), '.', lower('{np_schema}'), '.', nt.table_name, '%')
     {unqualified_clause}
),

published_ds_np_rollup AS (
  SELECT
    site_id,
    datasource_id,
    count(DISTINCT matched_table) AS matched_table_count,
    concat_ws(', ', sort_array(collect_set(matched_table))) AS matched_tables
  FROM published_ds_np_hits
  GROUP BY 1,2
),

published_ds_mode AS (
  SELECT
    site_id,
    datasource_id,
    count(*) AS num_connections,
    sum(CASE WHEN has_extract THEN 1 ELSE 0 END) AS num_extract_connections
  FROM published_ds_connections
  GROUP BY 1,2
),

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
    'published_datasource' AS datasource_kind,
    ds.datasource_id,
    ds.datasource_name,
    ds.datasource_repo_url,
    ds.datasource_luid,

    CASE WHEN nr.datasource_id IS NOT NULL THEN true ELSE false END AS uses_npdataset,
    coalesce(nr.matched_table_count, 0) AS matched_table_count,
    coalesce(nr.matched_tables, '')     AS matched_tables,

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

  FROM datasources_dim ds
  LEFT JOIN published_ds_np_rollup nr
    ON nr.site_id = ds.site_id AND nr.datasource_id = ds.datasource_id
  LEFT JOIN published_ds_mode m
    ON m.site_id = ds.site_id AND m.datasource_id = ds.datasource_id
  LEFT JOIN published_ds_consumers c
    ON c.site_id = ds.site_id AND c.datasource_id = ds.datasource_id
  LEFT JOIN workbooks_dim wb
    ON wb.site_id = c.site_id AND wb.workbook_id = c.workbook_id
  LEFT JOIN projects_dim pr
    ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
  LEFT JOIN dashboards_dim db
    ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id

  {published_filter}
),

-- -----------------------------
-- B) WORKBOOK-DIRECT CONNECTIONS (no published datasource)
-- -----------------------------
workbook_direct_connections AS (
  SELECT
    site_id,
    data_connection_id,
    owner_id AS workbook_id,
    has_extract,
    tablename_norm
  FROM dc_norm
  WHERE owner_type_lc = 'workbook'
    AND datasource_id IS NULL
),

workbook_direct_np_hits AS (
  SELECT
    w.site_id,
    w.workbook_id,
    nt.table_name AS matched_table
  FROM workbook_direct_connections w
  JOIN np_tables nt
    ON  w.tablename_norm LIKE concat('%', lower('{np_schema}'), '.', nt.table_name, '%')
     OR w.tablename_norm LIKE concat('%', lower('{np_catalog}'), '.', lower('{np_schema}'), '.', nt.table_name, '%')
     {unqualified_clause}
),

workbook_direct_rollup AS (
  SELECT
    site_id,
    workbook_id,
    count(DISTINCT matched_table) AS matched_table_count,
    concat_ws(', ', sort_array(collect_set(matched_table))) AS matched_tables
  FROM workbook_direct_np_hits
  GROUP BY 1,2
),

workbook_direct_mode AS (
  SELECT
    site_id,
    workbook_id,
    count(*) AS num_connections,
    sum(CASE WHEN has_extract THEN 1 ELSE 0 END) AS num_extract_connections
  FROM workbook_direct_connections
  GROUP BY 1,2
),

workbook_direct_rows AS (
  SELECT
    'workbook_direct' AS datasource_kind,
    CAST(NULL AS BIGINT) AS datasource_id,
    CAST(NULL AS STRING) AS datasource_name,
    CAST(NULL AS STRING) AS datasource_repo_url,
    CAST(NULL AS STRING) AS datasource_luid,

    true AS uses_npdataset,
    r.matched_table_count,
    r.matched_tables,

    CASE
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

  FROM workbook_direct_rollup r
  JOIN workbooks_dim wb
    ON wb.site_id = r.site_id AND wb.workbook_id = r.workbook_id
  LEFT JOIN workbook_direct_mode m
    ON m.site_id = r.site_id AND m.workbook_id = r.workbook_id
  LEFT JOIN projects_dim pr
    ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
  LEFT JOIN dashboards_dim db
    ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id
)

SELECT * FROM published_rows
UNION ALL
SELECT * FROM workbook_direct_rows
ORDER BY
  uses_npdataset DESC,
  datasource_kind,
  datasource_name,
  workbook_name,
  dashboard_name
"""
    return spark.sql(sql)


def write_daily_snapshot(
    df: DataFrame,
    *,
    target_catalog: str = "eciscor_prod",
    target_schema: str = "cdo",
    target_table: str = "npdatasetmetrics_tableau",
    snapshot_date: Optional[date] = None,
    full_refresh: bool = False,
) -> str:
    """
    Writes df to a Delta table partitioned by snapshot_date.

    - First run: creates the table (or full_refresh=True overwrites it)
    - Daily runs: overwrite ONLY the snapshot_date partition (replaceWhere) -> idempotent

    Returns the fully qualified table name.
    """
    snapshot_date = snapshot_date or date.today()
    snapshot_date_str = snapshot_date.isoformat()

    target_fqn = f"{target_catalog}.{target_schema}.{target_table}"

    # Add ingestion columns + a stable row hash (useful for diffs/auditing)
    # Note: This hash is based on business columns, not on timestamps.
    business_cols = [c for c in df.columns]  # everything from the query is "business" for this snapshot
    df_out = (
        df
        .withColumn("snapshot_date", F.lit(snapshot_date_str).cast("date"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(str(uuid.uuid4())))
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]), 256)
        )
    )

    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

    table_exists = spark.catalog.tableExists(target_fqn)

    if full_refresh or not table_exists:
        # Full load / create table
        (df_out.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .saveAsTable(target_fqn))
    else:
        # Incremental daily: replace only today's partition (safe on retries)
        (df_out.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'")
            .saveAsTable(target_fqn))

    return target_fqn


# -------------------------
# JOB ENTRYPOINT (daily)
# -------------------------
# 1) Build snapshot
df_metrics = build_tableau_npdataset_usage_public_only(
    catalog="eciscor_prod",
    tableau_schema="ec_tableau_meta",
    np_schema="npdataset",
    match_unqualified_table_names=False,  # set True only if necessary
    only_matches=True,                   # set False if you want all published datasources (including 0 matches)
)

# 2) Write snapshot to eciscor_prod.cdo.npdatasetmetrics_tableau
target = write_daily_snapshot(
    df_metrics,
    target_catalog="eciscor_prod",
    target_schema="cdo",
    target_table="npdatasetmetrics_tableau",
    snapshot_date=None,   # default: today
    full_refresh=False,   # set True for your one-time initial “full load” if desired
)

print(f"Wrote snapshot to {target}")
display(spark.table(target).orderBy(F.col("snapshot_date").desc()))

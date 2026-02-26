from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F

# =========================
# CONFIG (easy placeholders)
# =========================
CATALOG = "eciscor_prod"
TABLEAU_SCHEMA = "ec_tableau_meta"

NP_CATALOG = "eciscor_prod"
NP_SCHEMA = "npdataset"

TARGET_SCHEMA = "cdo"
TARGET_TABLE = "npdatasetmetrics_tableau"   # -> eciscor_prod.cdo.npdatasetmetrics_tableau

DAYS_RUNTIME_WINDOW = 30                    # runtime lookback for Tableau queries
FAIL_JOB_IF_EMPTY = True                    # fail instead of writing empty partition

# If you have many npdataset tables and ANALYZE is too heavy daily, set to False and rely on existing stats
RUN_ANALYZE_FOR_TABLE_STATS = False         # set True if you want to force-refresh stats nightly

# =========================
# Logging helpers
# =========================
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def fq(schema: str, table: str, catalog: str = CATALOG) -> str:
    return f"{catalog}.{schema}.{table}"

def assert_exists(table_fqn: str) -> None:
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(f"Missing required table: {table_fqn}")

# =========================
# 1) Build ROW-LEVEL mapping from Tableau table_assets
# =========================
def build_rowlevel_table_assets_mapping() -> DataFrame:
    """
    Output rows are:
      (npdataset table_asset) × (source workbook OR source datasource->consumer workbook) × (dashboard)
    """
    ta_fqn  = fq(TABLEAU_SCHEMA, "table_assets")
    tas_fqn = fq(TABLEAU_SCHEMA, "table_asset_sources")
    wb_fqn  = fq(TABLEAU_SCHEMA, "workbooks")
    vw_fqn  = fq(TABLEAU_SCHEMA, "views")
    pr_fqn  = fq(TABLEAU_SCHEMA, "projects")
    ds_fqn  = fq(TABLEAU_SCHEMA, "datasources")
    dc_fqn  = fq(TABLEAU_SCHEMA, "data_connections")

    for t in [ta_fqn, tas_fqn, wb_fqn, vw_fqn, pr_fqn, ds_fqn, dc_fqn]:
        assert_exists(t)

    ta  = spark.table(ta_fqn)
    tas = spark.table(tas_fqn)

    # filter to npdataset
    if "table_schema" in ta.columns:
        ta_np = ta.where(F.lower(F.col("table_schema")) == F.lit(NP_SCHEMA.lower()))
    else:
        ta_np = ta.where(F.lower(F.coalesce(F.col("full_name"), F.lit(""))).like(f"%{NP_SCHEMA.lower()}%"))

    if "is_tombstoned" in ta.columns:
        ta_np = ta_np.where(~F.coalesce(F.col("is_tombstoned"), F.lit(False)))

    ta_np = ta_np.select(
        F.col("site_id").alias("site_id"),
        F.col("id").cast("long").alias("table_asset_id"),
        F.col("name").alias("np_table_name"),
        (F.col("table_schema") if "table_schema" in ta.columns else F.lit(NP_SCHEMA)).alias("np_table_schema"),
        (F.col("full_name") if "full_name" in ta.columns else F.lit(None)).alias("table_full_name"),
    )

    src = (
        tas.select(
            F.col("site_id"),
            F.col("table_asset_id").cast("long"),
            F.lower(F.col("source_type")).alias("source_type"),
            F.col("source_id").cast("long").alias("source_id"),
        )
        .join(ta_np, on=["site_id", "table_asset_id"], how="inner")
    )

    # dims
    workbooks = (
        spark.table(wb_fqn)
        .where(~F.coalesce(F.col("is_deleted"), F.lit(False)))
        .select(
            "site_id",
            F.col("id").cast("long").alias("workbook_id"),
            F.col("name").alias("workbook_name"),
            F.col("repository_url").alias("workbook_repo_url"),
            F.col("project_id").alias("project_id"),
            F.col("luid").alias("workbook_luid"),
        )
    )

    projects = spark.table(pr_fqn).select(
        "site_id",
        F.col("id").alias("project_id"),
        F.col("name").alias("project_name"),
    )

    dashboards = (
        spark.table(vw_fqn)
        .where((F.lower(F.col("sheettype")) == F.lit("dashboard")) & (~F.coalesce(F.col("is_deleted"), F.lit(False))))
        .select(
            "site_id",
            F.col("workbook_id").cast("long").alias("workbook_id"),
            F.col("id").cast("long").alias("dashboard_id"),
            F.col("name").alias("dashboard_name"),
            F.col("repository_url").alias("dashboard_repo_url"),
            F.col("luid").alias("dashboard_luid"),
        )
    )

    datasources = spark.table(ds_fqn).select(
        "site_id",
        F.col("id").cast("long").alias("datasource_id"),
        F.col("name").alias("datasource_name"),
        F.col("repository_url").alias("datasource_repo_url"),
        F.col("luid").alias("datasource_luid"),
    )

    dc = spark.table(dc_fqn)

    # consumers of published datasource (doesn't require tablename)
    consumers = (
        dc.where((F.lower(F.col("owner_type")) == F.lit("workbook")) & F.col("datasource_id").isNotNull())
          .select(
              "site_id",
              F.col("datasource_id").cast("long").alias("datasource_id"),
              F.col("owner_id").cast("long").alias("workbook_id"),
          )
          .distinct()
    )

    # connection mode for datasource (live/extract/mixed)
    ds_mode = (
        dc.where(F.lower(F.col("owner_type")) == F.lit("datasource"))
          .select(
              "site_id",
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

    # Path A: source is workbook
    wb_path = (
        src.where(F.col("source_type") == F.lit("workbook"))
           .withColumnRenamed("source_id", "workbook_id")
           .withColumn("link_path", F.lit("table_asset_source_workbook"))
           .join(workbooks, on=["site_id", "workbook_id"], how="left")
           .join(projects, on=["site_id", "project_id"], how="left")
           .join(dashboards, on=["site_id", "workbook_id"], how="left")
           .select(
               "site_id", "table_asset_id", "np_table_schema", "np_table_name", "table_full_name",
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

    # Path B: source is published datasource -> consumer workbooks
    ds_path = (
        src.where(F.col("source_type") == F.lit("datasource"))
           .withColumnRenamed("source_id", "datasource_id")
           .withColumn("link_path", F.lit("table_asset_source_datasource"))
           .join(datasources, on=["site_id", "datasource_id"], how="left")
           .join(ds_mode, on=["site_id", "datasource_id"], how="left")
           .join(consumers, on=["site_id", "datasource_id"], how="left")
           .join(workbooks, on=["site_id", "workbook_id"], how="left")
           .join(projects, on=["site_id", "project_id"], how="left")
           .join(dashboards, on=["site_id", "workbook_id"], how="left")
           .select(
               "site_id", "table_asset_id", "np_table_schema", "np_table_name", "table_full_name",
               "link_path",
               F.lit("datasource").alias("source_type"),
               "datasource_id","datasource_name","datasource_repo_url","datasource_luid",
               "connection_mode",
               "project_name",
               "workbook_id","workbook_name","workbook_repo_url","workbook_luid",
               "dashboard_id","dashboard_name","dashboard_repo_url","dashboard_luid",
           )
    )

    out = wb_path.unionByName(ds_path, allowMissingColumns=True)

    # Add UC full name for the underlying table (what we’ll join to lineage/query metrics)
    out = out.withColumn("uc_table_full_name", F.concat(F.lit(f"{NP_CATALOG}.{NP_SCHEMA}."), F.col("np_table_name")))

    return out

# =========================
# 2) Enrichment: Databricks table stats (num_rows, size_in_bytes)
#    Uses DESCRIBE TABLE EXTENDED ... AS JSON (DBR 16.2+)  [oai_citation:5‡Databricks Documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)
# =========================
def get_uc_table_stats(table_full_names: List[str]) -> DataFrame:
    """
    Returns: uc_table_full_name, uc_num_rows, uc_size_in_bytes, uc_stats_collected_at
    """
    stats_rows = []
    for full_name in sorted(set(table_full_names)):
        try:
            if RUN_ANALYZE_FOR_TABLE_STATS:
                # ANALYZE TABLE collects numRows and sizeInBytes  [oai_citation:6‡Databricks Documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-compute-statistics?utm_source=chatgpt.com)
                spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS")

            ddf = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name} AS JSON")  #  [oai_citation:7‡Databricks Documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)
            col0 = ddf.columns[0]
            js = ddf.select(col0).first()[0]
            payload = json.loads(js)

            stats = payload.get("statistics", {}) or {}
            stats_rows.append({
                "uc_table_full_name": full_name,
                "uc_num_rows": stats.get("num_rows"),
                "uc_size_in_bytes": stats.get("size_in_bytes"),
                "uc_stats_collected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            })
        except Exception as e:
            # keep going; just log
            log(f"[WARN] table stats failed for {full_name}: {type(e).__name__}: {e}")
            stats_rows.append({
                "uc_table_full_name": full_name,
                "uc_num_rows": None,
                "uc_size_in_bytes": None,
                "uc_stats_collected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            })

    return spark.createDataFrame(stats_rows)

# =========================
# 3) Enrichment: Tableau extract size (bytes) (optional proxy)
#    public.extracts has datasource_id/workbook_id + size bytes  [oai_citation:8‡Tableau](https://tableau.github.io/tableau-data-dictionary/2024.2/data_dictionary.htm)
# =========================
def add_tableau_extract_sizes(df: DataFrame) -> DataFrame:
    extracts_fqn = fq(TABLEAU_SCHEMA, "extracts")
    if not spark.catalog.tableExists(extracts_fqn):
        log(f"[INFO] {extracts_fqn} not found; skipping extract size enrichment.")
        return df.withColumn("tableau_extract_size_bytes", F.lit(None).cast("long")) \
                 .withColumn("tableau_extract_updated_at", F.lit(None).cast("timestamp"))

    ex = spark.table(extracts_fqn).select(
        "site_id",
        F.col("datasource_id").cast("long").alias("ex_datasource_id"),
        F.col("workbook_id").cast("long").alias("ex_workbook_id"),
        F.col("size").cast("long").alias("extract_size_bytes"),
        F.col("updated_at").alias("extract_updated_at"),
    )

    # Join twice: once by datasource_id, once by workbook_id, then coalesce
    df1 = df.join(
        ex,
        on=[df.site_id == ex.site_id, df.datasource_id == ex.ex_datasource_id],
        how="left"
    ).drop(ex.site_id)

    df1 = df1.withColumnRenamed("extract_size_bytes", "ds_extract_size_bytes") \
             .withColumnRenamed("extract_updated_at", "ds_extract_updated_at")

    df2 = df1.join(
        ex,
        on=[df1.site_id == ex.site_id, df1.workbook_id == ex.ex_workbook_id],
        how="left"
    ).drop(ex.site_id)

    df2 = df2.withColumnRenamed("extract_size_bytes", "wb_extract_size_bytes") \
             .withColumnRenamed("extract_updated_at", "wb_extract_updated_at")

    return (
        df2
        .withColumn("tableau_extract_size_bytes", F.coalesce(F.col("ds_extract_size_bytes"), F.col("wb_extract_size_bytes")))
        .withColumn("tableau_extract_updated_at", F.coalesce(F.col("ds_extract_updated_at"), F.col("wb_extract_updated_at")))
        .drop("ex_datasource_id","ex_workbook_id","ds_extract_size_bytes","ds_extract_updated_at","wb_extract_size_bytes","wb_extract_updated_at")
    )

# =========================
# 4) Enrichment: Runtime “rows read” from Tableau queries (Databricks system tables)
#    query.history has read_rows/produced_rows/read_bytes  [oai_citation:9‡Databricks Documentation](https://docs.databricks.com/aws/en/admin/system-tables/query-history)
#    table_lineage.statement_id joins to query history  [oai_citation:10‡Databricks Documentation](https://docs.databricks.com/aws/en/admin/system-tables/lineage?utm_source=chatgpt.com)
# =========================
def build_runtime_table_read_metrics(days: int = DAYS_RUNTIME_WINDOW) -> DataFrame:
    """
    Returns per-table runtime metrics (across ALL Tableau queries in the window):
      uc_table_full_name, tableau_stmt_count, tableau_max_read_rows, tableau_max_produced_rows, tableau_max_read_bytes
    """
    q = spark.sql(f"""
      SELECT statement_id, start_time, client_application, read_rows, produced_rows, read_bytes
      FROM system.query.history
      WHERE start_time >= current_timestamp() - INTERVAL {days} DAYS
        AND lower(client_application) = 'tableau'
    """)
    l = spark.sql(f"""
      SELECT statement_id, source_table_full_name
      FROM system.access.table_lineage
      WHERE event_date >= current_date() - INTERVAL {days} DAYS
        AND statement_id IS NOT NULL
        AND source_table_full_name IS NOT NULL
    """)

    joined = (
        q.join(l, on="statement_id", how="inner")
         .withColumn("uc_table_full_name", F.col("source_table_full_name"))
         .drop("source_table_full_name")
    )

    return (
        joined.groupBy("uc_table_full_name")
              .agg(
                  F.countDistinct("statement_id").alias("tableau_stmt_count"),
                  F.max("read_rows").alias("tableau_max_read_rows"),
                  F.max("produced_rows").alias("tableau_max_produced_rows"),
                  F.max("read_bytes").alias("tableau_max_read_bytes"),
              )
    )

# =========================
# 5) Write (daily partition, idempotent overwrite)
# =========================
def write_daily_partition(df: DataFrame, snapshot_date: Optional[date] = None) -> str:
    snapshot_date = snapshot_date or date.today()
    snap_str = snapshot_date.isoformat()

    target_fqn = f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

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
                      F.coalesce(F.col("datasource_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("workbook_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("dashboard_id").cast("string"), F.lit("")),
                      F.coalesce(F.col("uc_table_full_name").cast("string"), F.lit("")),
                  ),
                  256,
              )
          )
    )

    # allow schema evolution (new columns)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if not spark.catalog.tableExists(target_fqn):
        log(f"Creating table {target_fqn}")
        (df_out.write.format("delta")
             .mode("overwrite")
             .partitionBy("snapshot_date")
             .saveAsTable(target_fqn))
    else:
        log(f"Overwriting partition snapshot_date={snap_str} in {target_fqn}")
        (df_out.write.format("delta")
             .mode("overwrite")
             .option("replaceWhere", f"snapshot_date = '{snap_str}'")
             .saveAsTable(target_fqn))

    return target_fqn

# =========================
# RUN
# =========================
log("Building row-level mapping from Tableau table_assets...")
df_map = build_rowlevel_table_assets_mapping()
map_cnt = df_map.count()
log(f"df_map rows: {map_cnt:,}")

if map_cnt == 0 and FAIL_JOB_IF_EMPTY:
    raise RuntimeError("No rows produced from table_assets mapping. Check table_assets/table_asset_sources population.")

log("Enriching with UC table stats (num_rows, size_in_bytes)...")
table_list = [r["uc_table_full_name"] for r in df_map.select("uc_table_full_name").distinct().collect()]
df_stats = get_uc_table_stats(table_list)

df_enriched = df_map.join(df_stats, on="uc_table_full_name", how="left")

log("Enriching with Tableau extract size bytes (optional proxy)...")
df_enriched = add_tableau_extract_sizes(df_enriched)

log("Enriching with runtime read metrics from Tableau queries (last N days)...")
df_runtime = build_runtime_table_read_metrics(days=DAYS_RUNTIME_WINDOW)
df_enriched = df_enriched.join(df_runtime, on="uc_table_full_name", how="left")

# Heuristic flags:
df_enriched = (
    df_enriched
    .withColumn(
        "suspected_full_table_read_runtime",
        F.when(
            (F.col("uc_num_rows").isNotNull()) & (F.col("tableau_max_read_rows").isNotNull()) &
            (F.col("uc_num_rows") > 0) &
            ((F.col("tableau_max_read_rows") / F.col("uc_num_rows")) >= F.lit(0.95)),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "suspected_full_table_extract_by_size",
        F.when(
            (F.col("uc_size_in_bytes").isNotNull()) & (F.col("tableau_extract_size_bytes").isNotNull()) &
            (F.col("uc_size_in_bytes") > 0) &
            ((F.col("tableau_extract_size_bytes") / F.col("uc_size_in_bytes")) >= F.lit(0.90)),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
)

final_cnt = df_enriched.count()
log(f"df_enriched rows: {final_cnt:,}")

if final_cnt == 0 and FAIL_JOB_IF_EMPTY:
    raise RuntimeError("Final dataset is empty; refusing to write empty partition.")

target = write_daily_partition(df_enriched, snapshot_date=None)
log(f"Wrote: {target}")

display(
    spark.table(target)
         .where(F.col("snapshot_date") == F.lit(date.today().isoformat()).cast("date"))
         .orderBy("np_table_name", "datasource_name", "workbook_name", "dashboard_name")
         .limit(200)
)

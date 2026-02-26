# Databricks Notebook (PySpark)
# ============================================================
# Tableau repo (table_assets) → ROW-LEVEL usage of eciscor_prod.npdataset.*
# + optional enrichments:
#   - UC table stats (num_rows, size_in_bytes) (best-effort; may be null if stats not collected)
#   - Tableau extract size bytes (proxy)
#   - Databricks runtime metrics from Tableau queries (read_rows/read_bytes via system tables)
#
# Writes to: eciscor_prod.cdo.npdatasetmetrics_tableau
# Incremental strategy:
#   - partitioned by snapshot_date
#   - each run overwrites ONLY today's partition (idempotent; reruns won’t duplicate)
#
# Fixes included:
#   - Explicit schema for df_stats (prevents “cannot determine type after inferring”)
#   - Conforms df_out to existing Delta table schema (prevents “Delta failed to merge fields workbook_id and workbook_id”)

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import List, Optional, Dict, Tuple

from pyspark.sql import DataFrame, functions as F, types as T


# =========================
# CONFIG (placeholders)
# =========================
CATALOG = "eciscor_prod"

# Tableau repo tables live here (you renamed Postgres 'public' to this UC schema)
TABLEAU_SCHEMA = "ec_tableau_meta"

# NPDataset location
NP_CATALOG = "eciscor_prod"
NP_SCHEMA = "npdataset"

# Output
TARGET_SCHEMA = "cdo"
TARGET_TABLE = "npdatasetmetrics_tableau"  # -> eciscor_prod.cdo.npdatasetmetrics_tableau

# Runtime enrichment window
DAYS_RUNTIME_WINDOW = 30

# Job behavior
FAIL_JOB_IF_EMPTY = True
RUN_ANALYZE_FOR_TABLE_STATS = False  # set True only if you want to force stats collection (can be heavy)


# =========================
# Helpers
# =========================
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def fqn(schema: str, table: str, catalog: str = CATALOG) -> str:
    return f"{catalog}.{schema}.{table}"

def assert_exists(table_fqn: str) -> None:
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(f"Missing required table: {table_fqn}")

def show_df(df: DataFrame, label: str, cols: Optional[List[str]] = None, n: int = 25) -> int:
    c = df.count()
    log(f"{label}: {c:,} rows")
    if c > 0:
        display(df.select(*cols).limit(n) if cols else df.limit(n))
    return c

def ensure_schema(catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def target_fqn() -> str:
    return f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

def _schema_map(schema: T.StructType) -> Dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in schema.fields}

def _find_duplicate_columns(cols: List[str]) -> List[str]:
    seen = set()
    dups = []
    for c in cols:
        if c in seen and c not in dups:
            dups.append(c)
        seen.add(c)
    return dups

def conform_to_existing_delta_schema(df: DataFrame, target: str) -> DataFrame:
    """
    If target Delta table exists, cast columns in df to match existing types to avoid merge conflicts.
    Also add any missing columns from target as nulls (cast to the target type).
    Keeps any *new* columns in df as-is (schema evolution), but avoids type conflicts for existing fields.
    """
    if not spark.catalog.tableExists(target):
        return df

    tgt_schema = spark.table(target).schema
    tgt_types = {f.name: f.dataType for f in tgt_schema.fields}

    df_cols = set(df.columns)

    # Cast existing target columns
    out = df
    for name, dtype in tgt_types.items():
        if name in df_cols:
            out = out.withColumn(name, F.col(name).cast(dtype))
        else:
            out = out.withColumn(name, F.lit(None).cast(dtype))

    # Reorder: target columns first, then any new columns
    ordered = [f.name for f in tgt_schema.fields] + [c for c in df.columns if c not in tgt_types]
    out = out.select(*ordered)

    # Sanity: duplicates
    dups = _find_duplicate_columns(out.columns)
    if dups:
        raise RuntimeError(f"DataFrame has duplicate column names after conforming: {dups}")

    return out

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
        for k, a, b in mismatched[:50]:
            print(f"  - {k}: {a} -> {b}")
    else:
        log("No type mismatches vs target for existing columns.")


# =========================
# 0) Diagnostics (table_assets + sources)
# =========================
def diagnose_table_assets() -> None:
    ta_fqn = fqn(TABLEAU_SCHEMA, "table_assets")
    tas_fqn = fqn(TABLEAU_SCHEMA, "table_asset_sources")

    wb_fqn = fqn(TABLEAU_SCHEMA, "workbooks")
    vw_fqn = fqn(TABLEAU_SCHEMA, "views")
    ds_fqn = fqn(TABLEAU_SCHEMA, "datasources")
    dc_fqn = fqn(TABLEAU_SCHEMA, "data_connections")
    pr_fqn = fqn(TABLEAU_SCHEMA, "projects")

    for t in [ta_fqn, tas_fqn, wb_fqn, vw_fqn, ds_fqn, dc_fqn, pr_fqn]:
        assert_exists(t)

    ta = spark.table(ta_fqn)
    tas = spark.table(tas_fqn)

    log("=== table_assets columns ===")
    print(ta.columns)
    log("=== table_asset_sources columns ===")
    print(tas.columns)

    # Filter to npdataset assets
    if "table_schema" in ta.columns:
        ta_np = ta.where(F.lower(F.col("table_schema")) == F.lit(NP_SCHEMA.lower()))
    else:
        ta_np = ta.where(F.lower(F.coalesce(F.col("full_name"), F.lit(""))).like(f"%{NP_SCHEMA.lower()}%"))

    if "is_tombstoned" in ta.columns:
        ta_np = ta_np.where(~F.coalesce(F.col("is_tombstoned"), F.lit(False)))

    cols = ["site_id", "id", "name"]
    if "table_schema" in ta.columns:
        cols += ["table_schema"]
    if "full_name" in ta.columns:
        cols += ["full_name"]

    show_df(ta_np.select(*cols), f"table_assets filtered to {NP_SCHEMA}", n=25)

    if "table_asset_id" not in tas.columns:
        raise RuntimeError("Expected column table_asset_id missing from table_asset_sources.")

    # source_type distribution
    tas_np = tas.join(
        ta_np.select(
            F.col("site_id").alias("s_site_id"),
            F.col("id").cast("long").alias("s_table_asset_id")
        ),
        (tas.site_id == F.col("s_site_id")) & (F.col("table_asset_id").cast("long") == F.col("s_table_asset_id")),
        "inner"
    )
    log("=== source_type distribution for npdataset table assets ===")
    display(
        tas_np.groupBy(F.lower(F.col("source_type")).alias("source_type"))
              .count()
              .orderBy(F.desc("count"))
    )


# =========================
# 1) Build ROW-LEVEL mapping (table_assets graph)
# =========================
def build_rowlevel_mapping_from_table_assets() -> DataFrame:
    """
    Row-level output:
      (npdataset table asset) × (source: workbook OR datasource) × (consumer workbook) × (dashboard)
    """
    ta_fqn = fqn(TABLEAU_SCHEMA, "table_assets")
    tas_fqn = fqn(TABLEAU_SCHEMA, "table_asset_sources")
    wb_fqn = fqn(TABLEAU_SCHEMA, "workbooks")
    vw_fqn = fqn(TABLEAU_SCHEMA, "views")
    pr_fqn = fqn(TABLEAU_SCHEMA, "projects")
    ds_fqn = fqn(TABLEAU_SCHEMA, "datasources")
    dc_fqn = fqn(TABLEAU_SCHEMA, "data_connections")

    for t in [ta_fqn, tas_fqn, wb_fqn, vw_fqn, pr_fqn, ds_fqn, dc_fqn]:
        assert_exists(t)

    ta = spark.table(ta_fqn)
    tas = spark.table(tas_fqn)

    # Filter to npdataset assets
    if "table_schema" in ta.columns:
        ta_np = ta.where(F.lower(F.col("table_schema")) == F.lit(NP_SCHEMA.lower()))
        table_schema_col = F.col("table_schema")
    else:
        ta_np = ta.where(F.lower(F.coalesce(F.col("full_name"), F.lit(""))).like(f"%{NP_SCHEMA.lower()}%"))
        table_schema_col = F.lit(NP_SCHEMA)

    if "is_tombstoned" in ta.columns:
        ta_np = ta_np.where(~F.coalesce(F.col("is_tombstoned"), F.lit(False)))

    ta_np_sel = ta_np.select(
        F.col("site_id").cast("long").alias("site_id"),
        F.col("id").cast("long").alias("table_asset_id"),
        F.col("name").alias("np_table_name"),
        table_schema_col.alias("np_table_schema"),
        (F.col("full_name") if "full_name" in ta.columns else F.lit(None)).alias("table_full_name"),
    )

    if "table_asset_id" not in tas.columns:
        raise RuntimeError("Expected column table_asset_id missing from table_asset_sources.")

    sources = (
        tas.select(
            F.col("site_id").cast("long").alias("site_id"),
            F.col("table_asset_id").cast("long").alias("table_asset_id"),
            F.lower(F.col("source_type")).alias("source_type"),
            F.col("source_id").cast("long").alias("source_id"),
        )
        .join(ta_np_sel, on=["site_id", "table_asset_id"], how="inner")
    )

    # Dimensions
    workbooks = (
        spark.table(wb_fqn)
        .where(~F.coalesce(F.col("is_deleted"), F.lit(False)))
        .select(
            F.col("site_id").cast("long").alias("site_id"),
            F.col("id").cast("long").alias("workbook_id"),
            F.col("name").alias("workbook_name"),
            F.col("repository_url").alias("workbook_repo_url"),
            F.col("project_id").alias("project_id"),
            F.col("luid").alias("workbook_luid"),
        )
    )

    projects = spark.table(pr_fqn).select(
        F.col("site_id").cast("long").alias("site_id"),
        F.col("id").alias("project_id"),
        F.col("name").alias("project_name"),
    )

    dashboards = (
        spark.table(vw_fqn)
        .where((F.lower(F.col("sheettype")) == F.lit("dashboard")) & (~F.coalesce(F.col("is_deleted"), F.lit(False))))
        .select(
            F.col("site_id").cast("long").alias("site_id"),
            F.col("workbook_id").cast("long").alias("workbook_id"),
            F.col("id").cast("long").alias("dashboard_id"),
            F.col("name").alias("dashboard_name"),
            F.col("repository_url").alias("dashboard_repo_url"),
            F.col("luid").alias("dashboard_luid"),
        )
    )

    datasources = spark.table(ds_fqn).select(
        F.col("site_id").cast("long").alias("site_id"),
        F.col("id").cast("long").alias("datasource_id"),
        F.col("name").alias("datasource_name"),
        F.col("repository_url").alias("datasource_repo_url"),
        F.col("luid").alias("datasource_luid"),
    )

    dc = spark.table(dc_fqn)

    # Consumers of a published datasource (workbook-owned connections referencing datasource_id)
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

    # Path 1: workbook sources
    wb_path = (
        sources.where(F.col("source_type") == F.lit("workbook"))
               .withColumn("link_path", F.lit("table_asset_source_workbook"))
               .withColumnRenamed("source_id", "workbook_id")
               .join(workbooks, on=["site_id", "workbook_id"], how="left")
               .join(projects, on=["site_id", "project_id"], how="left")
               .join(dashboards, on=["site_id", "workbook_id"], how="left")
               .select(
                   "site_id",
                   "table_asset_id",
                   "np_table_schema",
                   "np_table_name",
                   "table_full_name",
                   "link_path",
                   F.lit("workbook").alias("source_type"),
                   F.lit(None).cast("long").alias("datasource_id"),
                   F.lit(None).cast("string").alias("datasource_name"),
                   F.lit(None).cast("string").alias("datasource_repo_url"),
                   F.lit(None).cast("string").alias("datasource_luid"),
                   F.lit(None).cast("string").alias("connection_mode"),
                   "project_name",
                   "workbook_id",
                   "workbook_name",
                   "workbook_repo_url",
                   "workbook_luid",
                   "dashboard_id",
                   "dashboard_name",
                   "dashboard_repo_url",
                   "dashboard_luid",
               )
    )

    # Path 2: datasource sources → consumer workbooks → dashboards
    ds_path = (
        sources.where(F.col("source_type") == F.lit("datasource"))
               .withColumn("link_path", F.lit("table_asset_source_datasource"))
               .withColumnRenamed("source_id", "datasource_id")
               .join(datasources, on=["site_id", "datasource_id"], how="left")
               .join(ds_mode, on=["site_id", "datasource_id"], how="left")
               .join(consumers, on=["site_id", "datasource_id"], how="left")
               .join(workbooks, on=["site_id", "workbook_id"], how="left")
               .join(projects, on=["site_id", "project_id"], how="left")
               .join(dashboards, on=["site_id", "workbook_id"], how="left")
               .select(
                   "site_id",
                   "table_asset_id",
                   "np_table_schema",
                   "np_table_name",
                   "table_full_name",
                   "link_path",
                   F.lit("datasource").alias("source_type"),
                   "datasource_id",
                   "datasource_name",
                   "datasource_repo_url",
                   "datasource_luid",
                   "connection_mode",
                   "project_name",
                   "workbook_id",
                   "workbook_name",
                   "workbook_repo_url",
                   "workbook_luid",
                   "dashboard_id",
                   "dashboard_name",
                   "dashboard_repo_url",
                   "dashboard_luid",
               )
    )

    out = wb_path.unionByName(ds_path, allowMissingColumns=True)

    # UC full name of the underlying databricks table
    out = out.withColumn(
        "uc_table_full_name",
        F.concat(F.lit(f"{NP_CATALOG}.{NP_SCHEMA}."), F.col("np_table_name"))
    )

    # Sanity: duplicates
    dups = _find_duplicate_columns(out.columns)
    if dups:
        raise RuntimeError(f"Row-level mapping produced duplicate column names: {dups}")

    return out


# =========================
# 2) UC Table Stats Enrichment (explicit schema; no inference)
# =========================
UC_STATS_SCHEMA = T.StructType([
    T.StructField("uc_table_full_name", T.StringType(), False),
    T.StructField("uc_num_rows", T.LongType(), True),
    T.StructField("uc_size_in_bytes", T.LongType(), True),
    T.StructField("uc_stats_collected_at", T.TimestampType(), True),
    T.StructField("uc_stats_source", T.StringType(), True),
])

def get_uc_table_stats(table_full_names: List[str]) -> DataFrame:
    if not table_full_names:
        return spark.createDataFrame([], schema=UC_STATS_SCHEMA)

    rows = []
    now_ts = datetime.utcnow()

    for full_name in sorted(set(table_full_names)):
        uc_num_rows = None
        uc_size_in_bytes = None
        src = None

        if RUN_ANALYZE_FOR_TABLE_STATS:
            try:
                spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS")
            except Exception as e:
                log(f"[WARN] ANALYZE failed for {full_name}: {type(e).__name__}: {e}")

        # Try JSON describe
        try:
            ddf = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name} AS JSON")
            js = ddf.select(ddf.columns[0]).first()[0]
            payload = json.loads(js)
            stats = payload.get("statistics") or {}
            if stats.get("num_rows") is not None:
                uc_num_rows = int(stats["num_rows"])
            if stats.get("size_in_bytes") is not None:
                uc_size_in_bytes = int(stats["size_in_bytes"])
            src = "describe_table_extended_as_json"
        except Exception:
            pass

        # Fallback parse
        if src is None:
            try:
                ext = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}")
                cols = ext.columns
                if len(cols) >= 2:
                    stats_row = (
                        ext.where(F.lower(F.col(cols[0])) == F.lit("statistics"))
                           .select(F.col(cols[1]).cast("string").alias("stats_str"))
                           .limit(1)
                           .collect()
                    )
                    if stats_row:
                        s = stats_row[0]["stats_str"] or ""
                        import re
                        m1 = re.search(r"numRows\s*=\s*([0-9]+)", s)
                        m2 = re.search(r"sizeInBytes\s*=\s*([0-9]+)", s)
                        if m1:
                            uc_num_rows = int(m1.group(1))
                        if m2:
                            uc_size_in_bytes = int(m2.group(1))
                        src = "describe_table_extended_parse"
            except Exception:
                pass

        # Fallback describe detail for size
        if src is None:
            try:
                det = spark.sql(f"DESCRIBE DETAIL {full_name}")
                row = det.limit(1).collect()[0].asDict()
                if row.get("sizeInBytes") is not None:
                    uc_size_in_bytes = int(row["sizeInBytes"])
                src = "describe_detail"
            except Exception as e:
                log(f"[WARN] stats failed for {full_name}: {type(e).__name__}: {e}")
                src = "none"

        rows.append((full_name, uc_num_rows, uc_size_in_bytes, now_ts, src))

    return spark.createDataFrame(rows, schema=UC_STATS_SCHEMA)


# =========================
# 3) Tableau Extract Size Enrichment (optional proxy)
# =========================
EXTRACT_SCHEMA = T.StructType([
    T.StructField("site_id", T.LongType(), True),
    T.StructField("datasource_id", T.LongType(), True),
    T.StructField("workbook_id", T.LongType(), True),
    T.StructField("tableau_extract_size_bytes", T.LongType(), True),
    T.StructField("tableau_extract_updated_at", T.TimestampType(), True),
])

def get_tableau_extract_sizes() -> DataFrame:
    extracts_fqn = fqn(TABLEAU_SCHEMA, "extracts")
    if not spark.catalog.tableExists(extracts_fqn):
        log(f"[INFO] {extracts_fqn} not found; skipping extract size enrichment.")
        return spark.createDataFrame([], schema=EXTRACT_SCHEMA)

    ex = spark.table(extracts_fqn)
    if not all(c in ex.columns for c in ["size", "updated_at", "site_id"]):
        log(f"[WARN] extracts table exists but missing expected columns. Found: {ex.columns}")
        return spark.createDataFrame([], schema=EXTRACT_SCHEMA)

    out = ex.select(
        F.col("site_id").cast("long").alias("site_id"),
        (F.col("datasource_id").cast("long") if "datasource_id" in ex.columns else F.lit(None).cast("long")).alias("datasource_id"),
        (F.col("workbook_id").cast("long") if "workbook_id" in ex.columns else F.lit(None).cast("long")).alias("workbook_id"),
        F.col("size").cast("long").alias("tableau_extract_size_bytes"),
        F.col("updated_at").cast("timestamp").alias("tableau_extract_updated_at"),
    )
    return out


# =========================
# 4) Runtime Enrichment from Databricks system tables (safe, fixed schema)
# =========================
RUNTIME_SCHEMA = T.StructType([
    T.StructField("uc_table_full_name", T.StringType(), False),
    T.StructField("runtime_window_days", T.IntegerType(), True),
    T.StructField("tableau_stmt_count", T.LongType(), True),
    T.StructField("tableau_sum_read_rows", T.LongType(), True),
    T.StructField("tableau_max_read_rows", T.LongType(), True),
    T.StructField("tableau_sum_produced_rows", T.LongType(), True),
    T.StructField("tableau_max_produced_rows", T.LongType(), True),
    T.StructField("tableau_sum_read_bytes", T.LongType(), True),
    T.StructField("tableau_max_read_bytes", T.LongType(), True),
])

def get_runtime_table_read_metrics(days: int = DAYS_RUNTIME_WINDOW) -> DataFrame:
    try:
        q = spark.sql(f"""
          SELECT statement_id, read_rows, produced_rows, read_bytes
          FROM system.query.history
          WHERE start_time >= current_timestamp() - INTERVAL {days} DAYS
            AND lower(coalesce(client_application,'')) LIKE '%tableau%'
        """)
        l = spark.sql(f"""
          SELECT statement_id, source_table_full_name
          FROM system.access.table_lineage
          WHERE event_date >= current_date() - INTERVAL {days} DAYS
            AND statement_id IS NOT NULL
            AND source_table_full_name IS NOT NULL
        """)

        joined = q.join(l, on="statement_id", how="inner") \
                  .withColumn("uc_table_full_name", F.col("source_table_full_name")) \
                  .drop("source_table_full_name")

        agg = joined.groupBy("uc_table_full_name").agg(
            F.countDistinct("statement_id").alias("tableau_stmt_count"),
            F.sum("read_rows").alias("tableau_sum_read_rows"),
            F.max("read_rows").alias("tableau_max_read_rows"),
            F.sum("produced_rows").alias("tableau_sum_produced_rows"),
            F.max("produced_rows").alias("tableau_max_produced_rows"),
            F.sum("read_bytes").alias("tableau_sum_read_bytes"),
            F.max("read_bytes").alias("tableau_max_read_bytes"),
        ).withColumn("runtime_window_days", F.lit(int(days)))

        return agg.select([f.name for f in RUNTIME_SCHEMA.fields])

    except Exception as e:
        log(f"[WARN] runtime enrichment unavailable: {type(e).__name__}: {e}")
        return spark.createDataFrame([], schema=RUNTIME_SCHEMA)


# =========================
# 5) Write (daily partition, idempotent overwrite) — with schema conformance
# =========================
def write_daily_partition(df: DataFrame, snapshot_date: Optional[date] = None) -> str:
    snapshot_date = snapshot_date or date.today()
    snap_str = snapshot_date.isoformat()

    target = target_fqn()
    ensure_schema(CATALOG, TARGET_SCHEMA)

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Add ingestion columns
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

    # Diagnose & fix schema conflicts against existing Delta table
    log_schema_diff(df_out, target)
    df_out = conform_to_existing_delta_schema(df_out, target)

    # If table doesn't exist, create it; else overwrite only today's partition (idempotent)
    if not spark.catalog.tableExists(target):
        log(f"Creating table {target}")
        (df_out.write.format("delta")
             .mode("overwrite")
             .partitionBy("snapshot_date")
             .saveAsTable(target))
    else:
        log(f"Overwriting partition snapshot_date={snap_str} in {target} (idempotent)")
        (df_out.write.format("delta")
             .mode("overwrite")
             .option("replaceWhere", f"snapshot_date = '{snap_str}'")
             .option("mergeSchema", "true")
             .saveAsTable(target))

    return target


# =========================
# RUN NOTEBOOK
# =========================
log("=== STEP 0: Diagnose table_assets population ===")
diagnose_table_assets()

log("=== STEP 1: Build ROW-LEVEL mapping from table_assets ===")
df_map = build_rowlevel_mapping_from_table_assets()
map_cnt = df_map.count()
log(f"df_map rows: {map_cnt:,}")
if map_cnt == 0 and FAIL_JOB_IF_EMPTY:
    raise RuntimeError("df_map is empty. Check table_assets/table_asset_sources population and schema filter.")
display(df_map.limit(50))

log("=== STEP 2: UC table stats (explicit schema; fixes 'cannot determine type') ===")
table_list = [r["uc_table_full_name"] for r in df_map.select("uc_table_full_name").distinct().collect()]
log(f"Distinct UC tables to stat: {len(table_list)}")
log(f"Sample tables: {table_list[:10]}")
df_stats = get_uc_table_stats(table_list)
display(df_stats.limit(25))

log("=== STEP 3: Enrich mapping with UC stats ===")
df_enriched = df_map.join(df_stats, on="uc_table_full_name", how="left")

log("=== STEP 4: Tableau extract size bytes (optional proxy) ===")
df_extracts = get_tableau_extract_sizes()

if df_extracts.count() > 0:
    ds_ex = df_extracts.select(
        "site_id",
        F.col("datasource_id").alias("ds_id"),
        "tableau_extract_size_bytes",
        "tableau_extract_updated_at",
    )
    wb_ex = df_extracts.select(
        "site_id",
        F.col("workbook_id").alias("wb_id"),
        "tableau_extract_size_bytes",
        "tableau_extract_updated_at",
    )

    # join by datasource_id, then by workbook_id; coalesce
    df_enriched = (
        df_enriched
        .join(ds_ex, (df_enriched.site_id == ds_ex.site_id) & (df_enriched.datasource_id == ds_ex.ds_id), "left")
        .withColumnRenamed("tableau_extract_size_bytes", "ds_extract_size_bytes")
        .withColumnRenamed("tableau_extract_updated_at", "ds_extract_updated_at")
        .drop(ds_ex.site_id).drop("ds_id")
        .join(wb_ex, (df_enriched.site_id == wb_ex.site_id) & (df_enriched.workbook_id == wb_ex.wb_id), "left")
        .withColumnRenamed("tableau_extract_size_bytes", "wb_extract_size_bytes")
        .withColumnRenamed("tableau_extract_updated_at", "wb_extract_updated_at")
        .drop(wb_ex.site_id).drop("wb_id")
        .withColumn("tableau_extract_size_bytes", F.coalesce(F.col("ds_extract_size_bytes"), F.col("wb_extract_size_bytes")))
        .withColumn("tableau_extract_updated_at", F.coalesce(F.col("ds_extract_updated_at"), F.col("wb_extract_updated_at")))
        .drop("ds_extract_size_bytes","ds_extract_updated_at","wb_extract_size_bytes","wb_extract_updated_at")
    )
else:
    df_enriched = df_enriched.withColumn("tableau_extract_size_bytes", F.lit(None).cast("long")) \
                             .withColumn("tableau_extract_updated_at", F.lit(None).cast("timestamp"))

log("=== STEP 5: Databricks runtime metrics for Tableau queries (optional) ===")
df_runtime = get_runtime_table_read_metrics(days=DAYS_RUNTIME_WINDOW)
df_enriched = df_enriched.join(df_runtime, on="uc_table_full_name", how="left")

# Ratios / flags to spot “pulling whole table”
df_enriched = (
    df_enriched
    .withColumn(
        "ratio_max_read_to_table_rows",
        F.when(
            (F.col("uc_num_rows").isNotNull()) & (F.col("tableau_max_read_rows").isNotNull()) & (F.col("uc_num_rows") > 0),
            F.col("tableau_max_read_rows") / F.col("uc_num_rows")
        )
    )
    .withColumn(
        "suspected_full_table_read_runtime",
        F.when(F.col("ratio_max_read_to_table_rows") >= F.lit(0.95), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "ratio_extract_bytes_to_table_bytes",
        F.when(
            (F.col("uc_size_in_bytes").isNotNull()) & (F.col("tableau_extract_size_bytes").isNotNull()) & (F.col("uc_size_in_bytes") > 0),
            F.col("tableau_extract_size_bytes") / F.col("uc_size_in_bytes")
        )
    )
    .withColumn(
        "suspected_full_table_extract_by_size",
        F.when(F.col("ratio_extract_bytes_to_table_bytes") >= F.lit(0.90), F.lit(True)).otherwise(F.lit(False))
    )
)

final_cnt = df_enriched.count()
log(f"df_enriched rows: {final_cnt:,}")
if final_cnt == 0 and FAIL_JOB_IF_EMPTY:
    raise RuntimeError("Final dataset is empty; refusing to write empty partition.")

display(df_enriched.limit(100))

log("=== STEP 6: Write to Delta table (daily partition) ===")
target = write_daily_partition(df_enriched, snapshot_date=None)
log(f"Wrote: {target}")

today = date.today().isoformat()
df_today = spark.table(target).where(F.col("snapshot_date") == F.lit(today).cast("date"))
log(f"Rows in today's partition: {df_today.count():,}")
display(df_today.orderBy("np_table_name", "datasource_name", "workbook_name", "dashboard_name").limit(200))

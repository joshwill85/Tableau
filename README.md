You can absolutely do this two ways:

1. **Repository-only (design-time):** “Which Tableau datasource/workbook/dashboard *is configured to use* `npdataset` (directly or via an intermediate table)?”
2. **Repository + Databricks runtime lineage (best, row-level):** “Which *actual Databricks queries* (from Tableau) touched `npdataset`, and can we attribute them back to a specific workbook/view?”

Below are **both options** (public-only vs public+recommendations), and then the **Databricks lineage join** approach for direct vs indirect.

---

# Part 1 — Build a row-level “Tableau datasource → workbook/dashboard → referenced tables” dataset

## Option A — **Public schema only** (most common / you likely have this)

This relies on `public.data_connections.tablename` plus ownership fields and extract flag. The data dictionary documents:

* `tablename`, `owner_type`, `owner_id`, `datasource_id`, `has_extract` in `public.data_connections` ([Tableau Open Source][1])
* `public.workbooks` includes `luid` and workbook metadata ([Tableau Open Source][1])
* `public.views` includes `sheettype` and `luid` ([Tableau Open Source][1])

### What it gives you

Row-level mapping of:

* **published datasource** OR **workbook-owned (embedded) connection**
* “best-effort” table reference (whatever Tableau stored in `tablename`)
* all **workbooks/dashboards** that use a published datasource (via `data_connections.datasource_id`)

### SQL (Databricks SQL / UC)

Set one placeholder:

* `TABLEAU_CATALOG` = the UC catalog for the Tableau Postgres repo (foreign catalog or ingested catalog)

```sql
-- OPTION A: PUBLIC-ONLY TABLE REFERENCES
-- Replace TABLEAU_CATALOG with your Tableau repo catalog (the schema should be literally "public").

WITH np_tables AS (
  SELECT lower(table_name) AS table_name
  FROM eciscor_prod.information_schema.tables
  WHERE lower(table_schema) = 'npdataset'
),

-- 1) Published datasource connections (owner_type='Datasource')
published_ds_connections AS (
  SELECT
    dc.site_id,
    dc.id                AS data_connection_id,
    dc.owner_id          AS datasource_id,        -- published datasource id
    lower(dc.tablename)  AS tablename_lc,
    dc.has_extract,
    dc.dbclass,
    dc.dbname,
    dc.server,
    dc.port
  FROM TABLEAU_CATALOG.public.data_connections dc
  WHERE lower(dc.owner_type) = 'datasource'
),

-- 2) Workbooks that consume a published datasource (owner_type='Workbook' and datasource_id set)
published_ds_consumers AS (
  SELECT DISTINCT
    dc.site_id,
    dc.datasource_id     AS datasource_id,
    dc.owner_id          AS workbook_id
  FROM TABLEAU_CATALOG.public.data_connections dc
  WHERE lower(dc.owner_type) = 'workbook'
    AND dc.datasource_id IS NOT NULL
),

-- 3) Does the published datasource reference npdataset tables (best-effort via tablename)?
published_ds_np_hits AS (
  SELECT
    p.site_id,
    p.datasource_id,
    nt.table_name AS matched_table
  FROM published_ds_connections p
  JOIN np_tables nt
    ON p.tablename_lc LIKE concat('%npdataset.', nt.table_name, '%')
    OR p.tablename_lc LIKE concat('%eciscor_prod.npdataset.', nt.table_name, '%')
    OR p.tablename_lc = nt.table_name
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

datasources_dim AS (
  SELECT site_id, id AS datasource_id, name AS datasource_name, repository_url, luid
  FROM TABLEAU_CATALOG.public.datasources
),

workbooks_dim AS (
  SELECT site_id, id AS workbook_id, name AS workbook_name, repository_url, project_id, luid
  FROM TABLEAU_CATALOG.public.workbooks
  WHERE coalesce(is_deleted,false) = false
),

projects_dim AS (
  SELECT site_id, id AS project_id, name AS project_name
  FROM TABLEAU_CATALOG.public.projects
),

dashboards_dim AS (
  SELECT site_id, workbook_id, id AS view_id, name AS dashboard_name, repository_url, luid
  FROM TABLEAU_CATALOG.public.views
  WHERE lower(sheettype) = 'dashboard'
    AND coalesce(is_deleted,false) = false
)

SELECT
  -- Published datasource identity
  'published_datasource' AS datasource_kind,
  ds.datasource_id,
  ds.datasource_name,
  ds.repository_url AS datasource_repo_url,

  -- npdataset match (design-time best effort)
  coalesce(nr.matched_table_count, 0) AS matched_table_count,
  coalesce(nr.matched_tables, '')     AS matched_tables,

  -- extract vs live (at least one connection has_extract)
  CASE WHEN max(CASE WHEN p.has_extract THEN 1 ELSE 0 END) OVER (PARTITION BY ds.site_id, ds.datasource_id) = 1
       THEN 'extract_or_mixed' ELSE 'live' END AS connection_mode,

  -- downstream usage
  pr.project_name,
  wb.workbook_id,
  wb.workbook_name,
  wb.repository_url AS workbook_repo_url,
  db.dashboard_name,
  db.repository_url AS dashboard_repo_url

FROM datasources_dim ds
LEFT JOIN published_ds_np_rollup nr
  ON nr.site_id = ds.site_id AND nr.datasource_id = ds.datasource_id
LEFT JOIN published_ds_connections p
  ON p.site_id = ds.site_id AND p.datasource_id = ds.datasource_id
LEFT JOIN published_ds_consumers c
  ON c.site_id = ds.site_id AND c.datasource_id = ds.datasource_id
LEFT JOIN workbooks_dim wb
  ON wb.site_id = c.site_id AND wb.workbook_id = c.workbook_id
LEFT JOIN projects_dim pr
  ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
LEFT JOIN dashboards_dim db
  ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id

-- Uncomment if you only want datasources that match npdataset
-- WHERE nr.datasource_id IS NOT NULL

ORDER BY
  matched_table_count DESC,
  ds.datasource_name,
  wb.workbook_name,
  db.dashboard_name;
```

### Reality check

This works **only as well as `public.data_connections.tablename` is populated** (custom SQL often won’t be fully represented there). `public` is the “base repo,” but Tableau also has a **`recommendations` schema** that explicitly stores `custom_sql` + parsed “tables used in a connection.” ([Tableau Open Source][1])

---

## Option B — **Public + recommendations** (more accurate, includes custom SQL)

Your data dictionary (2025.3) clearly shows `recommendations.connection_tables` with:

* `custom_sql` and `table_value`
* `type` = `TABLE` or `CUSTOM_SQL` ([Tableau Open Source][1])

This is the best “repo-only” way to detect `npdataset` usage when authors used Custom SQL.

### Quick “do I have it?” check

```sql
SHOW TABLES IN TABLEAU_CATALOG.recommendations LIKE 'connection_tables';
```

### SQL (row-level, includes custom SQL hits)

This produces rows at the level of: **(published/embedded datasource, referenced table/custom_sql, workbook, dashboard)**

```sql
-- OPTION B: RECOMMENDATIONS + PUBLIC (better coverage, includes custom SQL)
-- Requires TABLEAU_CATALOG.recommendations.* tables to exist.

WITH np_tables AS (
  SELECT lower(table_name) AS table_name
  FROM eciscor_prod.information_schema.tables
  WHERE lower(table_schema) = 'npdataset'
),

-- recommendations.connection_tables has table_value + custom_sql + type :contentReference[oaicite:5]{index=5}
rec_table_refs AS (
  SELECT
    rc.site_id,
    rc.data_connection_id,                 -- FK back to public.data_connections (per rec schema design)
    rct.type                               AS ref_type,
    lower(rct.table_value)                 AS table_value_lc,
    cast(rct.custom_sql AS string)         AS custom_sql_str
  FROM TABLEAU_CATALOG.recommendations.connections rc
  JOIN TABLEAU_CATALOG.recommendations.connection_tables rct
    ON rct.rec_connections_id = rc.id
),

-- Match against known npdataset table names (works for TABLE refs and for custom_sql text)
np_hits AS (
  SELECT
    r.site_id,
    r.data_connection_id,
    nt.table_name AS matched_table,
    r.ref_type,
    r.table_value_lc,
    r.custom_sql_str
  FROM rec_table_refs r
  JOIN np_tables nt
    ON r.table_value_lc LIKE concat('%npdataset.', nt.table_name, '%')
    OR r.table_value_lc LIKE concat('%eciscor_prod.npdataset.', nt.table_name, '%')
    OR lower(coalesce(r.custom_sql_str,'')) LIKE concat('%npdataset.', nt.table_name, '%')
    OR lower(coalesce(r.custom_sql_str,'')) LIKE concat('%eciscor_prod.npdataset.', nt.table_name, '%')
),

dc AS (
  SELECT
    site_id,
    id AS data_connection_id,
    owner_type,
    owner_id,
    datasource_id,
    has_extract,
    tablename,
    query_tagging_enabled
  FROM TABLEAU_CATALOG.public.data_connections
),

datasources_dim AS (
  SELECT site_id, id AS datasource_id, name AS datasource_name, repository_url, luid
  FROM TABLEAU_CATALOG.public.datasources
),

workbooks_dim AS (
  SELECT site_id, id AS workbook_id, name AS workbook_name, repository_url, project_id, luid
  FROM TABLEAU_CATALOG.public.workbooks
  WHERE coalesce(is_deleted,false) = false
),

projects_dim AS (
  SELECT site_id, id AS project_id, name AS project_name
  FROM TABLEAU_CATALOG.public.projects
),

dashboards_dim AS (
  SELECT site_id, workbook_id, id AS view_id, name AS dashboard_name, repository_url, luid
  FROM TABLEAU_CATALOG.public.views
  WHERE lower(sheettype) = 'dashboard'
    AND coalesce(is_deleted,false) = false
),

published_ds_consumers AS (
  SELECT DISTINCT
    site_id,
    datasource_id,
    owner_id AS workbook_id
  FROM dc
  WHERE lower(owner_type) = 'workbook'
    AND datasource_id IS NOT NULL
)

SELECT
  -- The hit is at the CONNECTION level; resolve whether it is a published datasource or embedded workbook connection
  lower(dc.owner_type) AS connection_owner_type,
  dc.owner_id          AS connection_owner_id,
  dc.datasource_id     AS published_datasource_id,

  ds.datasource_name,
  ds.repository_url AS datasource_repo_url,

  nh.ref_type,
  nh.matched_table,
  nh.table_value_lc,
  nh.custom_sql_str,

  CASE WHEN dc.has_extract THEN 'extract' ELSE 'live' END AS connection_mode,
  dc.query_tagging_enabled,

  pr.project_name,
  wb.workbook_name,
  wb.repository_url AS workbook_repo_url,
  db.dashboard_name,
  db.repository_url AS dashboard_repo_url

FROM np_hits nh
JOIN dc
  ON dc.site_id = nh.site_id AND dc.data_connection_id = nh.data_connection_id
LEFT JOIN datasources_dim ds
  ON ds.site_id = dc.site_id AND ds.datasource_id = dc.datasource_id
LEFT JOIN published_ds_consumers c
  ON c.site_id = dc.site_id AND c.datasource_id = dc.datasource_id
LEFT JOIN workbooks_dim wb
  ON wb.site_id = c.site_id AND wb.workbook_id = c.workbook_id
LEFT JOIN projects_dim pr
  ON pr.site_id = wb.site_id AND pr.project_id = wb.project_id
LEFT JOIN dashboards_dim db
  ON db.site_id = wb.site_id AND db.workbook_id = wb.workbook_id

ORDER BY
  ds.datasource_name, wb.workbook_name, db.dashboard_name, nh.matched_table;
```

---

# Part 2 — Use Databricks lineage to classify “direct vs indirect” npdataset usage (row-level)

What you’re trying to separate is:

* **Direct:** Tableau queries include `eciscor_prod.npdataset.*` as **source tables**.
* **Indirect:** Tableau queries hit some other table(s) (silver/gold), and those tables are **derived from** `npdataset` (via ETL).

Unity Catalog gives you the exact building blocks:

* `system.access.table_lineage` includes `source_table_full_name` and `target_table_full_name` (3-part names) ([Databricks Documentation][2])
* It also tells you read vs write patterns (target null vs not null) ([Databricks Documentation][2])
* `statement_id` is a foreign key to `system.query.history` for SQL warehouse queries ([Databricks Documentation][2])
* `system.query.history` has:

  * `client_application` (can literally be “Tableau”)
  * `statement_text`
  * `query_tags` ([Databricks Documentation][3])

### What “others” typically do (and what I recommend)

**Enable Tableau Query Tagging** so every database query includes a comment that starts with
`/* "tableau-query-origins": "..." */` containing workbook/dashboard/view LUIDs. ([Tableau Help][4])
Then you can join those LUIDs right back to `public.workbooks.luid` and `public.views.luid`. ([Tableau Open Source][1])

That gives you **runtime, statement-level truth**.

---

## 2A) Build an “npdataset → derived tables” graph (transitive)

Databricks now supports recursive CTEs, which is perfect for lineage traversal. ([Databricks][5])

This produces row-level “paths”:

* `np_root_table_full_name` → `derived_table_full_name` (with `depth`)

```sql
-- BUILD DERIVED-TABLE CLOSURE FROM NPDATASET (up to depth 5)
WITH RECURSIVE edges AS (
  SELECT DISTINCT
    source_table_full_name AS src,
    target_table_full_name AS tgt
  FROM system.access.table_lineage
  WHERE event_date >= current_date() - INTERVAL 365 DAYS
    AND source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL   -- read+write edges
),
seed AS (
  SELECT concat('eciscor_prod.npdataset.', table_name) AS np_root
  FROM eciscor_prod.information_schema.tables
  WHERE lower(table_schema) = 'npdataset'
),
closure AS (
  SELECT
    np_root,
    np_root AS table_full_name,
    0 AS depth
  FROM seed

  UNION ALL

  SELECT
    c.np_root,
    e.tgt AS table_full_name,
    c.depth + 1 AS depth
  FROM closure c
  JOIN edges e
    ON e.src = c.table_full_name
  WHERE c.depth < 5
)
SELECT *
FROM closure;
```

---

## 2B) Join Tableau “referenced tables” to that closure to label direct vs indirect (design-time)

Take **Option A or B** above, normalize each Tableau reference into a UC 3-part table name (best-effort), then join to `closure`.

Row-level output example fields:

* workbook/dashboard/datasource
* `tableau_ref_uc_full_name`
* `direct_reads_npdataset`
* `indirect_via_derived_table`
* `np_root_table_full_name`
* `derivation_depth`

```sql
-- DESIGN-TIME CLASSIFICATION (works even without Tableau query-tagging)
-- Assumes you already have a CTE/table called tableau_refs with:
--   site_id, datasource_name, workbook_name, dashboard_name, ref_schema, ref_table

WITH RECURSIVE edges AS (
  SELECT DISTINCT source_table_full_name AS src, target_table_full_name AS tgt
  FROM system.access.table_lineage
  WHERE event_date >= current_date() - INTERVAL 365 DAYS
    AND source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
),
seed AS (
  SELECT concat('eciscor_prod.npdataset.', table_name) AS np_root
  FROM eciscor_prod.information_schema.tables
  WHERE lower(table_schema) = 'npdataset'
),
closure AS (
  SELECT np_root, np_root AS table_full_name, 0 AS depth
  FROM seed
  UNION ALL
  SELECT c.np_root, e.tgt, c.depth + 1
  FROM closure c
  JOIN edges e ON e.src = c.table_full_name
  WHERE c.depth < 5
),

-- Example "tableau_refs" using PUBLIC ONLY (swap in the output of Option A/Option B as needed)
tableau_refs AS (
  SELECT
    dc.site_id,
    ds.name AS datasource_name,
    wb.name AS workbook_name,
    v.name  AS dashboard_name,
    -- best-effort parse of schema.table from public.data_connections.tablename
    lower(split_part(regexp_replace(coalesce(dc.tablename,''), '[`"\\[\\]]', ''), '.', 1)) AS ref_schema,
    lower(split_part(regexp_replace(coalesce(dc.tablename,''), '[`"\\[\\]]', ''), '.', 2)) AS ref_table
  FROM TABLEAU_CATALOG.public.data_connections dc
  LEFT JOIN TABLEAU_CATALOG.public.datasources ds
    ON lower(dc.owner_type) = 'datasource' AND ds.id = dc.owner_id AND ds.site_id = dc.site_id
  LEFT JOIN TABLEAU_CATALOG.public.workbooks wb
    ON wb.id = dc.owner_id AND wb.site_id = dc.site_id
  LEFT JOIN TABLEAU_CATALOG.public.views v
    ON v.workbook_id = wb.id AND v.site_id = wb.site_id AND lower(v.sheettype) = 'dashboard'
  WHERE dc.tablename IS NOT NULL AND trim(dc.tablename) <> ''
),

tableau_refs_norm AS (
  SELECT
    *,
    concat('eciscor_prod.', ref_schema, '.', ref_table) AS tableau_ref_uc_full_name
  FROM tableau_refs
  WHERE ref_schema IS NOT NULL AND ref_table IS NOT NULL
)

SELECT
  tr.site_id,
  tr.datasource_name,
  tr.workbook_name,
  tr.dashboard_name,

  tr.tableau_ref_uc_full_name,

  -- direct if it’s an npdataset table
  CASE WHEN tr.ref_schema = 'npdataset' THEN true ELSE false END AS direct_reads_npdataset,

  -- indirect if it’s not npdataset but appears in the derived closure
  CASE WHEN tr.ref_schema <> 'npdataset' AND c.table_full_name IS NOT NULL THEN true ELSE false END AS indirect_via_derived_table,

  c.np_root   AS np_root_table_full_name,
  c.depth     AS derivation_depth

FROM tableau_refs_norm tr
LEFT JOIN closure c
  ON c.table_full_name = tr.tableau_ref_uc_full_name

ORDER BY
  tr.datasource_name, tr.workbook_name, tr.dashboard_name, tr.tableau_ref_uc_full_name, derivation_depth;
```

---

# Part 3 — Best version: row-level **runtime** linkage (Tableau → Databricks statement_id → lineage)

If you enable Tableau query tagging, Tableau appends a comment string beginning with:
`/* "tableau-query-origins": "..." */` ([Tableau Help][4])

Databricks can surface that because `system.query.history` includes `statement_text` and `client_application` (can be Tableau). ([Databricks Documentation][3])
Then you join to `system.access.table_lineage` via `statement_id`. ([Databricks Documentation][2])

### Runtime row-level query (statement → workbook/view via LUID → lineage rows)

This is “ground truth” and lets you split “direct vs indirect” by what the query actually read.

```sql
-- RUNTIME ROW-LEVEL: Tableau queries in Databricks -> lineage rows -> (optionally) map back to Tableau content via query-tagging comment

WITH q AS (
  SELECT
    statement_id,
    executed_by,
    executed_as,
    client_application,
    statement_text,
    query_tags,
    execution_duration_ms,
    total_duration_ms,
    compute.warehouse_id AS warehouse_id,
    start_time
  FROM system.query.history
  WHERE lower(client_application) = 'tableau'      -- client_application can be "Tableau" :contentReference[oaicite:16]{index=16}
    AND start_time >= current_timestamp() - INTERVAL 30 DAYS
),

-- Extract the query-origins payload if present
q_tagged AS (
  SELECT
    *,
    regexp_extract(statement_text, 'tableau-query-origins":\\s*"([^"]+)"', 1) AS tableau_query_origins_raw
  FROM q
),

-- Lineage rows for those statements
l AS (
  SELECT
    statement_id,
    event_time,
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type,
    created_by
  FROM system.access.table_lineage
  WHERE statement_id IS NOT NULL
    AND event_date >= current_date() - INTERVAL 30 DAYS
)

SELECT
  qt.start_time,
  qt.statement_id,
  qt.executed_by,
  qt.warehouse_id,
  qt.execution_duration_ms,
  qt.total_duration_ms,
  qt.query_tags,

  qt.tableau_query_origins_raw,   -- inspect this; it contains LUIDs when tagging is enabled :contentReference[oaicite:17]{index=17}

  l.event_time,
  l.source_table_full_name,
  l.target_table_full_name,

  -- direct if the SOURCE of the statement is npdataset
  CASE WHEN l.source_table_full_name LIKE 'eciscor_prod.npdataset.%' THEN true ELSE false END AS direct_read,

  -- this helps you separate read vs write patterns in the lineage rows :contentReference[oaicite:18]{index=18}
  CASE
    WHEN l.source_type IS NOT NULL AND l.target_type IS NULL THEN 'read_only'
    WHEN l.source_type IS NULL AND l.target_type IS NOT NULL THEN 'write_only'
    WHEN l.source_type IS NOT NULL AND l.target_type IS NOT NULL THEN 'read_and_write'
    ELSE 'unknown'
  END AS lineage_event_kind

FROM q_tagged qt
JOIN l
  ON l.statement_id = qt.statement_id
ORDER BY qt.start_time DESC, qt.statement_id, l.event_time;
```

### Next step (if query tagging is enabled)

Once you see what UUIDs are in `tableau_query_origins_raw`, you can:

* extract `workbook_luid` / `view_luid` from it, and
* join to:

  * `TABLEAU_CATALOG.public.workbooks.luid` ([Tableau Open Source][1])
  * `TABLEAU_CATALOG.public.views.luid` (then use `sheettype='dashboard'`) ([Tableau Open Source][1])

That gives you the “holy grail” row:

> statement_id → exact Tableau workbook/dashboard → exact UC source tables touched

---

## What I need from you (to make this copy/paste perfect)

You don’t need to explain anything — just tell me these two identifiers:

1. The UC name where Tableau repo lives (the `TABLEAU_CATALOG` you use), and confirm you can see `TABLEAU_CATALOG.public.data_connections`.
2. Whether you also have `TABLEAU_CATALOG.recommendations.connection_tables`.

Then I’ll rewrite the final row-level “direct vs indirect” query so it’s **one single SQL** you can drop into a view.

[1]: https://tableau.github.io/tableau-data-dictionary/2025.3/data_dictionary.htm "tableau.github.io"
[2]: https://docs.databricks.com/aws/en/admin/system-tables/lineage "Lineage system tables reference | Databricks on AWS"
[3]: https://docs.databricks.com/aws/en/admin/system-tables/query-history "Query history system table reference | Databricks on AWS"
[4]: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_how_to_query_tagging.htm "Understand Workbook Impact on Your Database Using Query Tagging - Tableau"
[5]: https://www.databricks.com/blog/introducing-recursive-common-table-expressions-databricks?utm_source=chatgpt.com "Introducing Recursive Common Table Expressions"

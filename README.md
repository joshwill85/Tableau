# Tableau

-- =====================================================================================
-- TABLEAU SERVER OBSERVABILITY LAYER (Databricks / Unity Catalog)
-- Source: ec_tableau_meta   (Tableau repo tables ingested into Databricks)
-- Target: cdo_restricted    (curated views for admin monitoring + analytics)
--
-- Notes:
-- - All Tableau repo timestamps are UTC.
-- - Repo-only limitations:
--     * Exact filter values / mark clicks / tooltip usage are not reliably available.
--     * Use Activity Log (Advanced Mgmt) and/or Extensions/Embedding instrumentation for those.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS cdo_restricted;

-- =========================================================
-- 0) Shared helpers (inline masking + parsing conventions)
-- =========================================================
-- IP masking strategy:
--   - Do NOT expose raw remote_ip/user_ip in curated views
--   - Expose a stable pseudonym token instead (still lets you count unique clients)
--   - Also expose a non-PII "ip_scope" classification for analytics (private/public/unknown)
--
-- Replace <<SALT>> with a stable secret value.
-- =========================================================


-- =========================================================
-- 1) DIMENSIONS
-- =========================================================

-- 1.1 User identity (site user + system identity)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_user AS
SELECT
  u.site_id,
  u.id                  AS user_id,
  u.system_user_id,
  su.name               AS user_name,
  su.email              AS user_email,
  su.friendly_name      AS user_friendly_name,
  su.domain_name        AS user_domain_name,
  su.admin_level        AS server_admin_level
FROM ec_tableau_meta.users u
JOIN ec_tableau_meta.system_users su
  ON su.id = u.system_user_id;


-- 1.2 Project
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_project AS
SELECT
  p.site_id,
  p.id          AS project_id,
  p.name        AS project_name,
  p.created_at  AS project_created_at,
  p.updated_at  AS project_updated_at
FROM ec_tableau_meta.projects p;


-- 1.3 Workbook (enriched with owner + project)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_workbook AS
SELECT
  w.site_id,
  w.id                    AS workbook_id,
  w.name                  AS workbook_name,
  w.repository_url        AS workbook_repo_url,
  w.project_id,
  dp.project_name,
  w.owner_id,
  du_owner.user_name      AS workbook_owner_name,
  du_owner.user_email     AS workbook_owner_email,
  w.modified_by_user_id,
  du_mod.user_name        AS workbook_modified_by_name,
  du_mod.user_email       AS workbook_modified_by_email,
  w.created_at            AS workbook_created_at,
  w.updated_at            AS workbook_updated_at,
  w.first_published_at,
  w.last_published_at,
  w.content_version,
  w.size                  AS workbook_size_bytes,
  w.view_count            AS workbook_view_count,
  w.display_tabs,
  w.is_deleted,
  -- extract / freshness signals
  w.refreshable_extracts,
  w.incrementable_extracts,
  w.data_engine_extracts,
  w.extracts_refreshed_at,
  w.extracts_incremented_at,
  w.extract_storage_format
FROM ec_tableau_meta.workbooks w
LEFT JOIN cdo_restricted.tableau_dim_project dp
  ON dp.site_id = w.site_id
 AND dp.project_id = w.project_id
LEFT JOIN cdo_restricted.tableau_dim_user du_owner
  ON du_owner.site_id = w.site_id
 AND du_owner.user_id = w.owner_id
LEFT JOIN cdo_restricted.tableau_dim_user du_mod
  ON du_mod.site_id = w.site_id
 AND du_mod.user_id = w.modified_by_user_id;


-- 1.4 View/Sheet (tabs are views inside a workbook)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_view AS
SELECT
  v.site_id,
  v.id                   AS view_id,
  v.name                 AS view_name,
  v.repository_url       AS view_repo_url,          -- usually "WorkbookRepo/SheetRepo"
  v.workbook_id,
  dw.workbook_name,
  dw.workbook_repo_url,
  dw.project_id,
  dw.project_name,
  v.owner_id,
  du_owner.user_name     AS view_owner_name,
  du_owner.user_email    AS view_owner_email,
  v.created_at           AS view_created_at,
  v.updated_at           AS view_updated_at,
  v.first_published_at,
  v.revision,
  v.sheet_id,
  v.sheettype,
  v.`index`              AS view_tab_index,
  v.state,
  v.is_deleted
FROM ec_tableau_meta.views v
LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON dw.site_id = v.site_id
 AND dw.workbook_id = v.workbook_id
LEFT JOIN cdo_restricted.tableau_dim_user du_owner
  ON du_owner.site_id = v.site_id
 AND du_owner.user_id = v.owner_id;


-- 1.5 Datasource (published)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_datasource AS
SELECT
  d.site_id,
  d.id                    AS datasource_id,
  d.name                  AS datasource_name,
  d.repository_url        AS datasource_repo_url,
  d.project_id,
  dp.project_name,
  d.owner_id,
  du_owner.user_name      AS datasource_owner_name,
  du_owner.user_email     AS datasource_owner_email,
  d.created_at            AS datasource_created_at,
  d.updated_at            AS datasource_updated_at,
  d.first_published_at,
  d.last_published_at,
  d.content_version,
  d.size                  AS datasource_size_bytes,
  d.db_class,
  d.db_name,
  d.table_name,
  d.refreshable_extracts,
  d.incrementable_extracts,
  d.data_engine_extracts,
  d.extracts_refreshed_at,
  d.extracts_incremented_at,
  d.using_remote_query_agent,
  d.is_certified,
  d.certification_note,
  d.certifier_user_id
FROM ec_tableau_meta.datasources d
LEFT JOIN cdo_restricted.tableau_dim_project dp
  ON dp.site_id = d.site_id
 AND dp.project_id = d.project_id
LEFT JOIN cdo_restricted.tableau_dim_user du_owner
  ON du_owner.site_id = d.site_id
 AND du_owner.user_id = d.owner_id;


-- 1.6 Datasource refresh properties (if populated)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_datasource_refresh_properties AS
SELECT
  drp.site_id,
  drp.id                  AS datasource_id,
  drp.refresh_mode,
  drp.remote_agent_id,
  drp.schedules            AS refresh_schedules_json,
  drp.expected_refresh_time,
  drp.expected_refresh_duration,
  drp.next_check_time,
  drp.last_refresh_error_client_data
FROM ec_tableau_meta.datasource_refresh_properties drp;


-- 1.7 Data connections (live vs extract, bridge usage, connection tech)
-- Joins to workbook/datasource based on owner_type + owner_id
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_data_connection AS
SELECT
  dc.site_id,
  dc.id                    AS data_connection_id,
  dc.luid                  AS data_connection_luid,
  dc.name                  AS data_connection_name,
  dc.caption               AS data_connection_caption,
  dc.dbclass,
  dc.db_subclass,
  dc.server,
  dc.dbname,
  dc.tablename,
  dc.port,
  dc.authentication,
  dc.has_extract,
  dc.using_remote_query_agent,
  dc.query_tagging_enabled,
  dc.owner_type,
  dc.owner_id,
  dc.datasource_id,
  dds.datasource_name,
  dds.datasource_repo_url,
  -- Attach owner object names
  CASE WHEN dc.owner_type = 'Workbook'  THEN dw.workbook_name  END AS owner_workbook_name,
  CASE WHEN dc.owner_type = 'Workbook'  THEN dw.workbook_id    END AS owner_workbook_id,
  CASE WHEN dc.owner_type = 'Datasource' THEN dds.datasource_name END AS owner_datasource_name,
  CASE WHEN dc.owner_type = 'Datasource' THEN dds.datasource_id   END AS owner_datasource_id,
  dc.created_at,
  dc.updated_at
FROM ec_tableau_meta.data_connections dc
LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON dw.site_id = dc.site_id
 AND dc.owner_type = 'Workbook'
 AND dw.workbook_id = dc.owner_id
LEFT JOIN cdo_restricted.tableau_dim_datasource dds
  ON dds.site_id = dc.site_id
 AND dds.datasource_id = dc.datasource_id;


-- 1.8 Connection summary per workbook/datasource (live vs extract)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_content_connection_summary AS
WITH wb AS (
  SELECT
    site_id,
    owner_workbook_id AS content_id,
    'Workbook'        AS content_type,
    max(CASE WHEN has_extract THEN 1 ELSE 0 END) AS has_extract_any,
    max(CASE WHEN using_remote_query_agent THEN 1 ELSE 0 END) AS uses_bridge_any,
    array_sort(collect_set(dbclass)) AS dbclass_set,
    count(*) AS connection_count
  FROM cdo_restricted.tableau_dim_data_connection
  WHERE owner_type = 'Workbook' AND owner_workbook_id IS NOT NULL
  GROUP BY site_id, owner_workbook_id
),
ds AS (
  SELECT
    site_id,
    owner_datasource_id AS content_id,
    'Datasource'        AS content_type,
    max(CASE WHEN has_extract THEN 1 ELSE 0 END) AS has_extract_any,
    max(CASE WHEN using_remote_query_agent THEN 1 ELSE 0 END) AS uses_bridge_any,
    array_sort(collect_set(dbclass)) AS dbclass_set,
    count(*) AS connection_count
  FROM cdo_restricted.tableau_dim_data_connection
  WHERE owner_type = 'Datasource' AND owner_datasource_id IS NOT NULL
  GROUP BY site_id, owner_datasource_id
)
SELECT * FROM wb
UNION ALL
SELECT * FROM ds;


-- 1.9 Schedules
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_schedule AS
SELECT
  s.id                    AS schedule_id,
  s.site_id,
  s.name                  AS schedule_name,
  s.active                AS schedule_active,
  s.priority              AS schedule_priority,
  s.schedule_type,
  CASE s.schedule_type
    WHEN 0 THEN 'Hourly'
    WHEN 1 THEN 'Daily'
    WHEN 2 THEN 'Weekly'
    WHEN 3 THEN 'Monthly'
    ELSE 'Unknown'
  END                     AS schedule_type_name,
  s.minute_interval,
  s.start_at_minute,
  s.end_at_minute,
  s.day_of_week_mask,
  s.day_of_month_mask,
  s.end_schedule_at,
  s.run_next_at,
  s.scheduled_action,
  CASE s.scheduled_action
    WHEN 0 THEN 'Extracts'
    WHEN 1 THEN 'Subscriptions'
    WHEN 2 THEN 'Sync AD'
    WHEN 3 THEN 'Auto Refresh Extracts'
    WHEN 4 THEN 'Flows'
    ELSE 'Other'
  END                     AS scheduled_action_name,
  s.timezoneid,
  s.defined_by,
  s.actionable,
  s.linked_task_enabled,
  s.created_at,
  s.updated_at
FROM ec_tableau_meta.schedules s;


-- 1.10 Tasks (ties schedules to objects; key for refresh monitoring)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_task AS
SELECT
  t.id                    AS task_id,
  t.luid                  AS task_luid,
  t.site_id,
  t.schedule_id,
  ds.schedule_name,
  ds.schedule_type_name,
  ds.scheduled_action_name,
  ds.run_next_at,
  t.type                  AS task_type,
  t.priority              AS task_priority,
  t.active                AS task_active,
  t.state                 AS task_state,
  t.obj_type              AS task_object_type,      -- 'Workbook' or 'Datasource'
  t.obj_id                AS task_object_id,
  t.title                 AS task_title,
  t.subtitle              AS task_subtitle,
  t.args                  AS task_args_yaml,
  t.consecutive_failure_count,
  t.run_count,
  t.historical_run_time,
  t.historical_queue_time,
  t.last_success_completed_at,
  t.creator_id,
  du_creator.user_name    AS task_creator_name,
  du_creator.user_email   AS task_creator_email,
  -- attach object names
  CASE WHEN t.obj_type = 'Workbook'  THEN dw.workbook_name  END AS task_workbook_name,
  CASE WHEN t.obj_type = 'Workbook'  THEN dw.project_name   END AS task_workbook_project,
  CASE WHEN t.obj_type = 'Datasource' THEN dds.datasource_name END AS task_datasource_name,
  CASE WHEN t.obj_type = 'Datasource' THEN dds.project_name    END AS task_datasource_project,
  t.created_at,
  t.updated_at
FROM ec_tableau_meta.tasks t
LEFT JOIN cdo_restricted.tableau_dim_schedule ds
  ON ds.schedule_id = t.schedule_id
 AND ds.site_id     = t.site_id
LEFT JOIN cdo_restricted.tableau_dim_user du_creator
  ON du_creator.site_id = t.site_id
 AND du_creator.user_id = t.creator_id
LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON dw.site_id = t.site_id
 AND t.obj_type = 'Workbook'
 AND dw.workbook_id = t.obj_id
LEFT JOIN cdo_restricted.tableau_dim_datasource dds
  ON dds.site_id = t.site_id
 AND t.obj_type = 'Datasource'
 AND dds.datasource_id = t.obj_id;


-- 1.11 Subscriptions (schedule + target + recipient)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_subscription AS
SELECT
  s.site_id,
  s.id                   AS subscription_id,
  s.luid                 AS subscription_luid,
  s.subject              AS subscription_subject,
  s.target_type,
  s.target_id,
  s.user_id              AS recipient_user_id,
  du_rec.user_name       AS recipient_name,
  du_rec.user_email      AS recipient_email,
  s.creator_id,
  du_creator.user_name   AS creator_name,
  du_creator.user_email  AS creator_email,
  s.schedule_id,
  ds.schedule_name,
  ds.run_next_at         AS schedule_run_next_at,
  ds.scheduled_action_name,
  s.last_sent,
  s.data_condition_type,
  s.is_refresh_extract_triggered,
  s.attach_pdf,
  s.attach_image,
  s.created_at
FROM ec_tableau_meta.subscriptions s
LEFT JOIN cdo_restricted.tableau_dim_user du_rec
  ON du_rec.site_id = s.site_id
 AND du_rec.user_id = s.user_id
LEFT JOIN cdo_restricted.tableau_dim_user du_creator
  ON du_creator.site_id = s.site_id
 AND du_creator.user_id = s.creator_id
LEFT JOIN cdo_restricted.tableau_dim_schedule ds
  ON ds.site_id = s.site_id
 AND ds.schedule_id = s.schedule_id;


-- 1.12 Group membership (security/governance)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_group_membership AS
SELECT
  g.site_id,
  g.id                AS group_id,
  g.name              AS group_name,
  g.domain_name       AS group_domain,
  gu.user_id,
  du.user_name,
  du.user_email,
  du.user_domain_name,
  du.server_admin_level
FROM ec_tableau_meta.groups g
JOIN ec_tableau_meta.group_users gu
  ON gu.group_id = g.id
 AND gu.site_id  = g.site_id
JOIN cdo_restricted.tableau_dim_user du
  ON du.user_id = gu.user_id
 AND du.site_id = g.site_id;


-- 1.13 Tags on content (governance)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_content_tags AS
SELECT
  t.site_id,
  tg.id          AS tagging_id,
  t.id           AS tag_id,
  t.name         AS tag_name,
  tg.taggable_type,
  tg.taggable_id,
  tg.user_id     AS tag_owner_user_id,
  du.user_name   AS tag_owner_name,
  du.user_email  AS tag_owner_email,
  tg.created_at,
  tg.updated_at
FROM ec_tableau_meta.tags t
JOIN ec_tableau_meta.taggings tg
  ON tg.tag_id  = t.id
 AND tg.site_id = t.site_id
LEFT JOIN cdo_restricted.tableau_dim_user du
  ON du.site_id = tg.site_id
 AND du.user_id = tg.user_id;


-- 1.14 Data quality indicators (governance)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_data_quality_indicators AS
SELECT
  dqi.site_id,
  dqi.id                AS dqi_id,
  dqi.luid              AS dqi_luid,
  dqi.content_type,
  dqi.content_id,
  dqi.data_quality_type,
  dqi.is_active,
  dqi.is_severe,
  dqi.message,
  dqi.user_id,
  du.user_name,
  du.user_email,
  dqi.created_at,
  dqi.updated_at
FROM ec_tableau_meta.data_quality_indicators dqi
LEFT JOIN cdo_restricted.tableau_dim_user du
  ON du.site_id = dqi.site_id
 AND du.user_id = dqi.user_id;


-- =========================================================
-- 2) FACTS: HTTP / NAVIGATION / DWELL / FUNNELS
-- =========================================================

-- 2.1 HTTP requests enriched (core clickstream-ish layer)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_http_requests_enriched AS
WITH req AS (
  SELECT
    hr.site_id,
    hr.id                     AS http_request_id,
    hr.created_at             AS request_created_at,
    hr.completed_at           AS request_completed_at,
    CASE
      WHEN hr.created_at IS NULL OR hr.completed_at IS NULL THEN NULL
      ELSE timestampdiff(MILLISECOND, hr.created_at, hr.completed_at)
    END                       AS request_duration_ms,
    hr.controller,
    hr.action,
    hr.http_request_uri,
    hr.http_referer,
    hr.http_user_agent,
    hr.status                 AS http_status,
    floor(hr.status / 100)    AS http_status_class,
    hr.session_id,
    hr.user_id,
    hr.vizql_session,
    hr.currentsheet,
    hr.worker,
    hr.port,

    -- Parse view repo URL portion from URI (works for /views/... and /t/<site>/views/...)
    NULLIF(regexp_extract(hr.http_request_uri, '/views/([^?]+)', 1), '') AS view_repo_url,

    -- Workbook repo URL from parsed view_repo_url
    CASE
      WHEN regexp_extract(hr.http_request_uri, '/views/([^?]+)', 1) IS NULL THEN NULL
      ELSE split_part(regexp_extract(hr.http_request_uri, '/views/([^?]+)', 1), '/', 1)
    END AS workbook_repo_url_from_uri,

    -- Mask remote_ip and user_ip (stable pseudonyms)
    CASE
      WHEN hr.remote_ip IS NULL OR hr.remote_ip = '' THEN NULL
      ELSE concat('ip_', substr(sha2(concat(hr.remote_ip, '<<SALT>>'), 256), 1, 12))
    END AS client_ip_token,
    CASE
      WHEN hr.user_ip IS NULL OR hr.user_ip = '' THEN NULL
      ELSE concat('ip_', substr(sha2(concat(hr.user_ip, '<<SALT>>'), 256), 1, 12))
    END AS origin_ip_token,

    -- Non-PII IP scope classification (based on raw remote_ip before hashing)
    CASE
      WHEN hr.remote_ip IS NULL OR hr.remote_ip = '' THEN 'unknown'
      WHEN hr.remote_ip RLIKE '^10\\.' THEN 'private'
      WHEN hr.remote_ip RLIKE '^192\\.168\\.' THEN 'private'
      WHEN hr.remote_ip RLIKE '^172\\.(1[6-9]|2[0-9]|3[0-1])\\.' THEN 'private'
      ELSE 'public'
    END AS client_ip_scope,

    -- Rough client classification from user agent
    CASE
      WHEN hr.http_user_agent IS NULL THEN 'unknown'
      WHEN lower(hr.http_user_agent) LIKE '%iphone%' OR lower(hr.http_user_agent) LIKE '%android%' THEN 'mobile'
      WHEN lower(hr.http_user_agent) LIKE '%ipad%' THEN 'tablet'
      ELSE 'desktop'
    END AS client_device_family,

    CASE
      WHEN hr.http_user_agent IS NULL THEN 'unknown'
      WHEN lower(hr.http_user_agent) LIKE '%chrome%' AND lower(hr.http_user_agent) NOT LIKE '%edg%' THEN 'chrome'
      WHEN lower(hr.http_user_agent) LIKE '%edg%' THEN 'edge'
      WHEN lower(hr.http_user_agent) LIKE '%safari%' AND lower(hr.http_user_agent) NOT LIKE '%chrome%' THEN 'safari'
      WHEN lower(hr.http_user_agent) LIKE '%firefox%' THEN 'firefox'
      ELSE 'other'
    END AS client_browser_family

  FROM ec_tableau_meta.http_requests hr
)
SELECT
  req.*,

  -- Session context
  s.created_at             AS session_created_at,
  s.updated_at             AS session_last_seen_at,

  -- Resolve user to name/email (prefer hr.user_id, fallback to session user)
  du.user_id               AS resolved_user_id,
  du.system_user_id,
  du.user_name,
  du.user_email,
  du.user_friendly_name,

  -- Map to view/workbook (best-effort)
  dv.view_id,
  dv.view_name,
  dv.sheettype             AS view_sheettype,
  dv.view_tab_index,
  dv.workbook_id,
  dv.workbook_name,
  dv.project_id,
  dv.project_name,

  -- fallback workbook mapping even if view mapping fails
  dw2.workbook_id          AS workbook_id_from_uri,
  dw2.workbook_name        AS workbook_name_from_uri,
  dw2.project_name         AS project_name_from_uri

FROM req
LEFT JOIN ec_tableau_meta.sessions s
  ON s.site_id = req.site_id
 AND s.session_id = req.session_id

LEFT JOIN cdo_restricted.tableau_dim_user du
  ON du.site_id = req.site_id
 AND du.user_id = coalesce(req.user_id, s.user_id)

LEFT JOIN cdo_restricted.tableau_dim_view dv
  ON dv.site_id = req.site_id
 AND dv.view_repo_url = req.view_repo_url

LEFT JOIN cdo_restricted.tableau_dim_workbook dw2
  ON dw2.site_id = req.site_id
 AND dw2.workbook_repo_url = req.workbook_repo_url_from_uri;


-- 2.2 Navigation events (one row per “entered view” within a session)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_navigation_events AS
WITH base AS (
  SELECT
    *,
    lag(view_id) OVER (PARTITION BY site_id, session_id ORDER BY request_created_at, http_request_id) AS prev_view_id
  FROM cdo_restricted.tableau_fct_http_requests_enriched
  WHERE session_id IS NOT NULL
    AND view_id IS NOT NULL
)
SELECT
  site_id,
  session_id,
  resolved_user_id,
  system_user_id,
  user_name,
  user_email,
  client_ip_token,
  client_ip_scope,
  client_device_family,
  client_browser_family,

  request_created_at       AS view_entered_at,
  request_duration_ms      AS view_load_time_ms_proxy,
  http_status,
  http_status_class,

  view_id,
  view_name,
  workbook_id,
  workbook_name,
  project_id,
  project_name
FROM base
WHERE prev_view_id IS NULL OR view_id <> prev_view_id;


-- 2.3 View dwell (time on tab/view) + idle-cap
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_dwell AS
WITH nav AS (
  SELECT
    n.*,
    lead(view_entered_at) OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS next_view_entered_at,
    max(session_last_seen_at) OVER (PARTITION BY site_id, session_id) AS session_last_seen_at_max
  FROM cdo_restricted.tableau_fct_view_navigation_events n
)
SELECT
  site_id,
  session_id,
  resolved_user_id,
  system_user_id,
  user_name,
  user_email,
  client_ip_token,
  client_ip_scope,
  client_device_family,
  client_browser_family,

  project_id,
  project_name,
  workbook_id,
  workbook_name,
  view_id,
  view_name,

  view_entered_at,
  coalesce(next_view_entered_at, session_last_seen_at_max) AS view_exited_at,

  greatest(
    0,
    timestampdiff(SECOND, view_entered_at, coalesce(next_view_entered_at, session_last_seen_at_max))
  ) AS dwell_seconds_raw,

  -- Industry-standard mitigation: cap idle inflation (30 minutes default)
  least(
    greatest(
      0,
      timestampdiff(SECOND, view_entered_at, coalesce(next_view_entered_at, session_last_seen_at_max))
    ),
    1800
  ) AS dwell_seconds_capped,

  view_load_time_ms_proxy
FROM nav;


-- 2.4 Workbook dwell (time in workbook) derived from view dwell
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_workbook_dwell AS
SELECT
  site_id,
  resolved_user_id,
  system_user_id,
  user_name,
  user_email,
  session_id,
  project_id,
  project_name,
  workbook_id,
  workbook_name,

  min(view_entered_at) AS workbook_entered_at,
  max(view_exited_at)  AS workbook_exited_at,

  sum(dwell_seconds_capped) AS workbook_dwell_seconds_capped,
  sum(dwell_seconds_raw)    AS workbook_dwell_seconds_raw,

  count(*)                  AS views_entered_count,
  count(distinct view_id)   AS distinct_views_entered
FROM cdo_restricted.tableau_fct_view_dwell
GROUP BY
  site_id, resolved_user_id, system_user_id, user_name, user_email, session_id,
  project_id, project_name, workbook_id, workbook_name;


-- 2.5 Session summary (for funnels + engagement + quality)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_session_summary AS
SELECT
  site_id,
  session_id,
  max(resolved_user_id) AS resolved_user_id,
  max(system_user_id)   AS system_user_id,
  max(user_name)        AS user_name,
  max(user_email)       AS user_email,

  min(request_created_at) AS session_start_at,
  max(coalesce(request_completed_at, request_created_at)) AS session_end_at,
  timestampdiff(SECOND, min(request_created_at), max(coalesce(request_completed_at, request_created_at))) AS session_seconds,

  max(client_ip_token)  AS client_ip_token,
  max(client_ip_scope)  AS client_ip_scope,
  max(client_device_family) AS client_device_family,
  max(client_browser_family) AS client_browser_family,

  count(*) AS http_request_count,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS http_error_count,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS http_5xx_count,

  count(distinct view_id)     AS distinct_views_touched,
  count(distinct workbook_id) AS distinct_workbooks_touched
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE session_id IS NOT NULL
GROUP BY site_id, session_id;


-- 2.6 Engagement “paths” (landing view -> next view transitions)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_engagement_paths AS
WITH nav AS (
  SELECT
    *,
    lead(view_id)       OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS next_view_id,
    lead(view_name)     OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS next_view_name,
    lead(workbook_id)   OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS next_workbook_id,
    lead(workbook_name) OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS next_workbook_name
  FROM cdo_restricted.tableau_fct_view_navigation_events
)
SELECT
  site_id,
  view_id,
  view_name,
  workbook_id,
  workbook_name,
  next_view_id,
  next_view_name,
  next_workbook_id,
  next_workbook_name,

  count(*) AS transition_count,
  count(distinct session_id) AS session_count
FROM nav
WHERE next_view_id IS NOT NULL
GROUP BY
  site_id, view_id, view_name, workbook_id, workbook_name,
  next_view_id, next_view_name, next_workbook_id, next_workbook_name;


-- 2.7 Approximate “interaction intensity” per view visit (proxy for filters/interactions)
-- Repo-only NOTE: This does NOT give exact filter values; it measures request volume during a view dwell window.
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_interaction_intensity AS
SELECT
  d.site_id,
  d.session_id,
  d.resolved_user_id,
  d.user_name,
  d.user_email,
  d.workbook_id,
  d.workbook_name,
  d.view_id,
  d.view_name,
  d.view_entered_at,
  d.view_exited_at,
  d.dwell_seconds_capped,

  count(r.http_request_id) AS request_count_during_view,
  sum(CASE WHEN r.http_status >= 400 THEN 1 ELSE 0 END) AS error_count_during_view,
  avg(r.request_duration_ms) AS avg_request_duration_ms_during_view
FROM cdo_restricted.tableau_fct_view_dwell d
LEFT JOIN cdo_restricted.tableau_fct_http_requests_enriched r
  ON r.site_id = d.site_id
 AND r.session_id = d.session_id
 AND r.request_created_at >= d.view_entered_at
 AND r.request_created_at <  d.view_exited_at
GROUP BY
  d.site_id, d.session_id, d.resolved_user_id, d.user_name, d.user_email,
  d.workbook_id, d.workbook_name, d.view_id, d.view_name,
  d.view_entered_at, d.view_exited_at, d.dwell_seconds_capped;


-- =========================================================
-- 3) FACTS: ADOPTION / USAGE AGGREGATIONS
-- =========================================================

-- 3.1 views_stats enriched (user-level cumulative counts + last access)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_views_stats_enriched AS
SELECT
  vs.site_id,
  vs.user_id,
  du.user_name,
  du.user_email,
  vs.view_id,
  dv.view_name,
  dv.workbook_id,
  dv.workbook_name,
  dv.project_name,
  vs.nviews,
  vs.`time` AS last_viewed_at,
  vs.device_type
FROM ec_tableau_meta.views_stats vs
JOIN cdo_restricted.tableau_dim_user du
  ON du.site_id = vs.site_id
 AND du.user_id = vs.user_id
JOIN cdo_restricted.tableau_dim_view dv
  ON dv.site_id = vs.site_id
 AND dv.view_id = vs.view_id;


-- 3.2 view_metrics_aggregations -> daily view counts (site-level)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_metrics_daily AS
SELECT
  vma.site_id,
  vma.view_id,
  dv.view_name,
  dv.workbook_id,
  dv.workbook_name,
  dv.project_name,
  make_date(vma.year_index, vma.month_index, vma.day_index) AS metric_date,
  vma.view_count,
  vma.device_type
FROM ec_tableau_meta.view_metrics_aggregations vma
JOIN cdo_restricted.tableau_dim_view dv
  ON dv.site_id = vma.site_id
 AND dv.view_id = vma.view_id
WHERE vma.day_index > 0 AND vma.month_index > 0 AND vma.year_index > 0;


-- 3.3 datasource_metrics_aggregations -> daily view counts
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_datasource_metrics_daily AS
SELECT
  dma.site_id,
  dma.datasource_id,
  dds.datasource_name,
  dds.project_name,
  make_date(dma.year_index, dma.month_index, dma.day_index) AS metric_date,
  dma.view_count
FROM ec_tableau_meta.datasource_metrics_aggregations dma
JOIN cdo_restricted.tableau_dim_datasource dds
  ON dds.site_id = dma.site_id
 AND dds.datasource_id = dma.datasource_id
WHERE dma.day_index > 0 AND dma.month_index > 0 AND dma.year_index > 0;


-- =========================================================
-- 3.4 REPLACEMENT: Extract refresh monitoring (most recent)
-- Source: ec_tableau_meta.most_recent_refreshes
-- Note: "most recent refresh data for alerts" (not full history)
-- Full history is from tableau_fct_background_jobs_enriched
-- =========================================================

CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_extract_refresh_most_recent AS
SELECT
  mrr.site_id,
  mrr.id                       AS most_recent_refresh_id,
  mrr.created_at               AS refresh_occurred_at,
  mrr.duration_in_ms,
  mrr.is_failure,
  CASE WHEN mrr.is_failure THEN 'failure' ELSE 'success' END AS refresh_status,
  mrr.worker,
  mrr.task_id,
  mrr.workbook_id,
  dw.workbook_name,
  dw.project_name              AS workbook_project_name,
  mrr.datasource_id,
  dds.datasource_name,
  dds.project_name             AS datasource_project_name,
  mrr.data_connection_id,
  mrr.historical_event_type_id,
  mrr.has_retry_job,
  mrr.details

FROM ec_tableau_meta.most_recent_refreshes mrr
LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON dw.site_id = mrr.site_id
 AND dw.workbook_id = mrr.workbook_id
LEFT JOIN cdo_restricted.tableau_dim_datasource dds
  ON dds.site_id = mrr.site_id
 AND dds.datasource_id = mrr.datasource_id;


-- 3.5 “Last content operation” (content_usage) – coarse but useful for stale content
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_content_usage_enriched AS
SELECT
  cu.site_id,
  cu.id AS content_usage_id,
  cu.content_type,  -- 1=workbook, 2=datasource, 3=flow
  cu.content_id,
  cu.operation_type,
  cu.last_recorded_at,
  CASE
    WHEN cu.content_type = 1 THEN dw.workbook_name
    WHEN cu.content_type = 2 THEN dds.datasource_name
    ELSE NULL
  END AS content_name
FROM ec_tableau_meta.content_usage cu
LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON cu.content_type = 1 AND dw.site_id = cu.site_id AND dw.workbook_id = cu.content_id
LEFT JOIN cdo_restricted.tableau_dim_datasource dds
  ON cu.content_type = 2 AND dds.site_id = cu.site_id AND dds.datasource_id = cu.content_id;


-- 3.6 Unused/stale content signals (views + workbooks)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_content_staleness AS
WITH view_last AS (
  SELECT
    site_id,
    view_id,
    max(metric_date) AS last_view_date
  FROM cdo_restricted.tableau_fct_view_metrics_daily
  WHERE view_count > 0
  GROUP BY site_id, view_id
),
view_last_user AS (
  SELECT
    site_id,
    view_id,
    max(last_viewed_at) AS last_viewed_at_any_user
  FROM cdo_restricted.tableau_fct_views_stats_enriched
  GROUP BY site_id, view_id
),
view_resolved AS (
  SELECT
    dv.site_id,
    dv.view_id,
    dv.view_name,
    dv.workbook_id,
    dv.workbook_name,
    dv.project_name,
    greatest(
      coalesce(cast(vl.last_view_date as timestamp), timestamp('1900-01-01')),
      coalesce(vlu.last_viewed_at_any_user, timestamp('1900-01-01'))
    ) AS last_access_at
  FROM cdo_restricted.tableau_dim_view dv
  LEFT JOIN view_last vl
    ON vl.site_id = dv.site_id AND vl.view_id = dv.view_id
  LEFT JOIN view_last_user vlu
    ON vlu.site_id = dv.site_id AND vlu.view_id = dv.view_id
)
SELECT
  'View' AS content_type,
  site_id,
  cast(view_id as string) AS content_id,
  view_name AS content_name,
  workbook_id,
  workbook_name,
  project_name,
  last_access_at,
  datediff(current_date(), cast(last_access_at as date)) AS days_since_last_access
FROM view_resolved
UNION ALL
SELECT
  'Workbook' AS content_type,
  dw.site_id,
  cast(dw.workbook_id as string) AS content_id,
  dw.workbook_name AS content_name,
  dw.workbook_id,
  dw.workbook_name,
  dw.project_name,
  max(v.last_access_at) AS last_access_at,
  datediff(current_date(), cast(max(v.last_access_at) as date)) AS days_since_last_access
FROM cdo_restricted.tableau_dim_workbook dw
LEFT JOIN (
  SELECT site_id, workbook_id, max(last_access_at) AS last_access_at
  FROM (
    SELECT site_id, workbook_id, last_access_at FROM (
      SELECT site_id, workbook_id, last_access_at FROM (
        SELECT site_id, workbook_id, last_access_at FROM (
          SELECT site_id, workbook_id, last_access_at FROM (
            SELECT site_id, workbook_id, last_access_at FROM (
              SELECT site_id, workbook_id, last_access_at FROM (
                SELECT dv.site_id, dv.workbook_id, greatest(timestamp('1900-01-01'), timestamp('1900-01-01')) AS last_access_at
                FROM cdo_restricted.tableau_dim_view dv
              )
            )
          )
        )
      )
    )
  ) x
  GROUP BY site_id, workbook_id
) v
  ON v.site_id = dw.site_id AND v.workbook_id = dw.workbook_id
GROUP BY dw.site_id, dw.workbook_id, dw.workbook_name, dw.project_name;


-- =========================================================
-- 4) FACTS: BACKGROUND JOBS / REFRESH HEALTH / SUBSCRIPTIONS
-- =========================================================

-- 4.1 Extract directories (used as a proxy signal for refresh writes)
CREATE OR REPLACE VIEW cdo_restricted.tableau_dim_extract_directory AS
SELECT
  e.site_id,
  e.id            AS extract_dir_id,
  e.workbook_id,
  e.datasource_id,
  e.descriptor,
  e.created_at,
  e.updated_at    AS extract_dir_updated_at
FROM ec_tableau_meta.extracts e;


-- 4.2 Background jobs enriched (durations, task/schedule/subscription linkage, content linkage)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_background_jobs_enriched AS
WITH bj AS (
  SELECT
    b.site_id,
    b.id                   AS background_job_id,
    b.luid                 AS background_job_luid,
    b.job_type,
    b.job_name,
    b.title                AS job_title,
    b.subtitle             AS job_subtitle,
    b.progress,
    b.finish_code,
    CASE b.finish_code
      WHEN 0 THEN 'success'
      WHEN 1 THEN 'failure'
      WHEN 2 THEN 'cancelled'
      ELSE 'other'
    END                    AS finish_status,
    b.created_at,
    b.started_at,
    b.completed_at,
    -- queue/runtime/total
    CASE WHEN b.created_at IS NULL OR b.started_at IS NULL THEN NULL
         ELSE timestampdiff(SECOND, b.created_at, b.started_at) END AS queue_seconds,
    CASE WHEN b.started_at IS NULL OR b.completed_at IS NULL THEN NULL
         ELSE timestampdiff(SECOND, b.started_at, b.completed_at) END AS run_seconds,
    CASE WHEN b.created_at IS NULL OR b.completed_at IS NULL THEN NULL
         ELSE timestampdiff(SECOND, b.created_at, b.completed_at) END AS total_seconds,
    b.attempts_remaining,
    b.task_id,
    b.correlation_id,
    b.creator_id,
    b.processed_on_worker,
    b.created_on_worker,
    b.queue_id,
    b.overflow,
    b.promoted_at,
    b.run_now,
    b.link
  FROM ec_tableau_meta.background_jobs b
),
task_by_task_id AS (
  SELECT * FROM cdo_restricted.tableau_dim_task
),
task_by_corr_id AS (
  SELECT * FROM cdo_restricted.tableau_dim_task
)
SELECT
  bj.*,

  du_creator.user_name   AS job_creator_name,
  du_creator.user_email  AS job_creator_email,

  -- Attach task (prefer task_id, fallback correlation_id when it matches a task)
  coalesce(t1.task_id, t2.task_id) AS resolved_task_id,
  coalesce(t1.task_type, t2.task_type) AS resolved_task_type,
  coalesce(t1.schedule_id, t2.schedule_id) AS resolved_schedule_id,
  coalesce(t1.schedule_name, t2.schedule_name) AS resolved_schedule_name,
  coalesce(t1.scheduled_action_name, t2.scheduled_action_name) AS resolved_scheduled_action_name,
  coalesce(t1.task_object_type, t2.task_object_type) AS resolved_object_type,
  coalesce(t1.task_object_id, t2.task_object_id) AS resolved_object_id,
  coalesce(t1.task_workbook_name, t2.task_workbook_name) AS resolved_workbook_name,
  coalesce(t1.task_datasource_name, t2.task_datasource_name) AS resolved_datasource_name,
  coalesce(t1.last_success_completed_at, t2.last_success_completed_at) AS task_last_success_completed_at,
  coalesce(t1.consecutive_failure_count, t2.consecutive_failure_count) AS task_consecutive_failure_count,

  -- Attach subscription when correlation_id matches a subscription
  sub.subscription_id,
  sub.subscription_subject,
  sub.target_type AS subscription_target_type,
  sub.target_id   AS subscription_target_id,
  sub.recipient_email AS subscription_recipient_email,
  sub.schedule_name   AS subscription_schedule_name,

  -- Attach content (workbook/datasource) for tasks
  dw.workbook_id,
  dw.workbook_name,
  dw.project_name AS workbook_project_name,
  dw.extracts_refreshed_at AS workbook_extracts_refreshed_at,
  dw.extracts_incremented_at AS workbook_extracts_incremented_at,

  dds.datasource_id,
  dds.datasource_name,
  dds.project_name AS datasource_project_name,
  dds.extracts_refreshed_at AS datasource_extracts_refreshed_at,
  dds.extracts_incremented_at AS datasource_extracts_incremented_at,

  -- Attach extract directory updated_at as a “write occurred” proxy
  ex.extract_dir_id,
  ex.extract_dir_updated_at,

  -- Proxy: did an extract timestamp update around the job completion time?
  CASE
    WHEN bj.completed_at IS NULL THEN NULL
    WHEN coalesce(t1.task_object_type, t2.task_object_type) = 'Workbook'
      AND greatest(coalesce(dw.extracts_refreshed_at, timestamp('1900-01-01')),
                   coalesce(dw.extracts_incremented_at, timestamp('1900-01-01'))) >= (bj.completed_at - INTERVAL 60 MINUTES)
      THEN true
    WHEN coalesce(t1.task_object_type, t2.task_object_type) = 'Datasource'
      AND greatest(coalesce(dds.extracts_refreshed_at, timestamp('1900-01-01')),
                   coalesce(dds.extracts_incremented_at, timestamp('1900-01-01'))) >= (bj.completed_at - INTERVAL 60 MINUTES)
      THEN true
    ELSE false
  END AS extract_timestamp_updated_proxy,

  CASE
    WHEN bj.completed_at IS NULL OR ex.extract_dir_updated_at IS NULL THEN NULL
    WHEN ex.extract_dir_updated_at >= (bj.completed_at - INTERVAL 60 MINUTES) THEN true
    ELSE false
  END AS extract_dir_updated_proxy

FROM bj
LEFT JOIN cdo_restricted.tableau_dim_user du_creator
  ON du_creator.site_id = bj.site_id
 AND du_creator.user_id = bj.creator_id

LEFT JOIN task_by_task_id t1
  ON t1.site_id = bj.site_id
 AND t1.task_id = bj.task_id

LEFT JOIN task_by_corr_id t2
  ON t2.site_id = bj.site_id
 AND t2.task_id = bj.correlation_id

LEFT JOIN cdo_restricted.tableau_dim_subscription sub
  ON sub.site_id = bj.site_id
 AND sub.subscription_id = bj.correlation_id

LEFT JOIN cdo_restricted.tableau_dim_workbook dw
  ON dw.site_id = bj.site_id
 AND coalesce(t1.task_object_type, t2.task_object_type) = 'Workbook'
 AND dw.workbook_id = coalesce(t1.task_object_id, t2.task_object_id)

LEFT JOIN cdo_restricted.tableau_dim_datasource dds
  ON dds.site_id = bj.site_id
 AND coalesce(t1.task_object_type, t2.task_object_type) = 'Datasource'
 AND dds.datasource_id = coalesce(t1.task_object_id, t2.task_object_id)

LEFT JOIN cdo_restricted.tableau_dim_extract_directory ex
  ON ex.site_id = bj.site_id
 AND (
      (coalesce(t1.task_object_type, t2.task_object_type) = 'Workbook'  AND ex.workbook_id  = coalesce(t1.task_object_id, t2.task_object_id))
   OR (coalesce(t1.task_object_type, t2.task_object_type) = 'Datasource' AND ex.datasource_id = coalesce(t1.task_object_id, t2.task_object_id))
 );


-- 4.3 Task health (overdue, failing, stale)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_task_health AS
SELECT
  site_id,
  task_id,
  task_type,
  scheduled_action_name,
  schedule_id,
  schedule_name,
  run_next_at,
  task_active,
  task_state,
  task_object_type,
  task_object_id,
  task_workbook_name,
  task_datasource_name,
  consecutive_failure_count,
  run_count,
  historical_run_time,
  historical_queue_time,
  last_success_completed_at,
  datediff(current_date(), cast(last_success_completed_at as date)) AS days_since_last_success,
  -- Overdue proxy: schedule has run_next_at in past AND last success < run_next_at (or null)
  CASE
    WHEN run_next_at IS NULL THEN NULL
    WHEN run_next_at < current_timestamp()
      AND (last_success_completed_at IS NULL OR last_success_completed_at < run_next_at)
    THEN true
    ELSE false
  END AS is_overdue_proxy
FROM cdo_restricted.tableau_dim_task;


-- 4.4 Subscription health (job outcomes via background_jobs)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_subscription_health AS
WITH last_run AS (
  SELECT
    site_id,
    subscription_id,
    max(completed_at) AS last_completed_at
  FROM cdo_restricted.tableau_fct_background_jobs_enriched
  WHERE subscription_id IS NOT NULL
  GROUP BY site_id, subscription_id
)
SELECT
  ds.*,
  lr.last_completed_at,
  bj.finish_status AS last_finish_status,
  bj.run_seconds   AS last_run_seconds
FROM cdo_restricted.tableau_dim_subscription ds
LEFT JOIN last_run lr
  ON lr.site_id = ds.site_id AND lr.subscription_id = ds.subscription_id
LEFT JOIN cdo_restricted.tableau_fct_background_jobs_enriched bj
  ON bj.site_id = lr.site_id
 AND bj.subscription_id = lr.subscription_id
 AND bj.completed_at = lr.last_completed_at;


-- 4.5 Data freshness view (extract vs live + “time since refresh”)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_data_freshness AS
SELECT
  'Workbook' AS content_type,
  dw.site_id,
  dw.workbook_id AS content_id,
  dw.workbook_name AS content_name,
  dw.project_name,
  cs.has_extract_any,
  cs.uses_bridge_any,
  greatest(coalesce(dw.extracts_refreshed_at, timestamp('1900-01-01')),
           coalesce(dw.extracts_incremented_at, timestamp('1900-01-01'))) AS last_extract_activity_at,
  datediff(current_date(), cast(greatest(coalesce(dw.extracts_refreshed_at, timestamp('1900-01-01')),
                                        coalesce(dw.extracts_incremented_at, timestamp('1900-01-01'))) as date)) AS days_since_extract_activity
FROM cdo_restricted.tableau_dim_workbook dw
LEFT JOIN cdo_restricted.tableau_fct_content_connection_summary cs
  ON cs.site_id = dw.site_id AND cs.content_type='Workbook' AND cs.content_id = dw.workbook_id
UNION ALL
SELECT
  'Datasource' AS content_type,
  dds.site_id,
  dds.datasource_id AS content_id,
  dds.datasource_name AS content_name,
  dds.project_name,
  cs.has_extract_any,
  cs.uses_bridge_any,
  greatest(coalesce(dds.extracts_refreshed_at, timestamp('1900-01-01')),
           coalesce(dds.extracts_incremented_at, timestamp('1900-01-01'))) AS last_extract_activity_at,
  datediff(current_date(), cast(greatest(coalesce(dds.extracts_refreshed_at, timestamp('1900-01-01')),
                                        coalesce(dds.extracts_incremented_at, timestamp('1900-01-01'))) as date)) AS days_since_extract_activity
FROM cdo_restricted.tableau_dim_datasource dds
LEFT JOIN cdo_restricted.tableau_fct_content_connection_summary cs
  ON cs.site_id = dds.site_id AND cs.content_type='Datasource' AND cs.content_id = dds.datasource_id;


-- =========================================================
-- 5) FACTS: GOVERNANCE / AUDIT (Historical Events)
-- =========================================================

CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_historical_events_enriched AS
SELECT
  he.id                    AS historical_event_id,
  he.created_at            AS event_created_at,
  het.action_type,
  het.name                 AS event_name,

  he.hist_actor_site_id,
  hs_actor.name            AS actor_site_name,

  he.hist_actor_user_id,
  hu_actor.name            AS actor_name,
  hu_actor.email           AS actor_email,

  he.hist_target_user_id,
  hu_target.name           AS target_user_name,
  hu_target.email          AS target_user_email,

  he.hist_project_id,
  hp.name                  AS project_name,

  he.hist_workbook_id,
  hw.name                  AS workbook_name,
  hw.repository_url        AS workbook_repo_url,

  he.hist_view_id,
  hv.name                  AS view_name,
  hv.repository_url        AS view_repo_url,

  he.hist_datasource_id,
  hds.name                 AS datasource_name,
  hds.repository_url       AS datasource_repo_url,

  he.hist_schedule_id,
  hsch.name                AS schedule_name,

  he.hist_task_id,
  ht.type                  AS task_type,

  he.details
FROM ec_tableau_meta.historical_events he
JOIN ec_tableau_meta.historical_event_types het
  ON het.type_id = he.historical_event_type_id
LEFT JOIN ec_tableau_meta.hist_sites hs_actor
  ON hs_actor.id = he.hist_actor_site_id
LEFT JOIN ec_tableau_meta.hist_users hu_actor
  ON hu_actor.id = he.hist_actor_user_id
LEFT JOIN ec_tableau_meta.hist_users hu_target
  ON hu_target.id = he.hist_target_user_id
LEFT JOIN ec_tableau_meta.hist_projects hp
  ON hp.id = he.hist_project_id
LEFT JOIN ec_tableau_meta.hist_workbooks hw
  ON hw.id = he.hist_workbook_id
LEFT JOIN ec_tableau_meta.hist_views hv
  ON hv.id = he.hist_view_id
LEFT JOIN ec_tableau_meta.hist_datasources hds
  ON hds.id = he.hist_datasource_id
LEFT JOIN ec_tableau_meta.hist_schedules hsch
  ON hsch.id = he.hist_schedule_id
LEFT JOIN ec_tableau_meta.hist_tasks ht
  ON ht.id = he.hist_task_id;


-- =========================================================
-- 6) PLATFORM HEALTH: DISK + LICENSE (if present)
-- =========================================================

-- 6.1 Disk usage (storage monitoring)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_disk_usage AS
SELECT *
FROM ec_tableau_meta.historical_disk_usage;

-- 6.2 Login-based license usage (if your server uses it)
CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_license_usage AS
SELECT *
FROM ec_tableau_meta.identity_based_activation_admin_view;

-- =====================================================================================
-- END
-- =====================================================================================

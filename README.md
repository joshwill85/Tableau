/* =====================================================================================
DATABRICKS AI/BI DASHBOARD: "Tableau Admin Observability" (Multi-tab)

PARAMETERS (create in dashboard, reused across all datasets):
- p_date        (Date Range) default: 2025-01-01 → Today
- p_site_id     (String)     default: 'All'
- p_project     (String)     default: 'All'
- p_workbook    (String)     default: 'All'
- p_view        (String)     default: 'All'   -- uses display label "Workbook / View"
- p_user_email  (String)     default: 'All'
- p_group       (String)     default: 'All'
- p_device      (String)     default: 'All'   -- desktop/mobile/tablet/unknown
- p_browser     (String)     default: 'All'   -- chrome/edge/safari/firefox/other/unknown
- p_stale_days  (Numeric)    default: 7
- p_session_id  (String)     default: 'All'   -- drilldown tab

NOTE: Query-based parameter lists should be dedicated datasets not used by visuals (Databricks guidance).
NOTE: If you implement archiving, point these datasets to your *_hist / UNION views instead of cdo_restricted.*.
===================================================================================== */


/* =====================================================================================
SECTION 0 — QUERY-BASED PARAMETER LIST DATASETS (use ONLY for filter widgets)
Create each as its own dataset, then in Canvas create a filter widget that uses:
- Fields: this dataset’s single column (e.g., project_name)
- Parameters: the target parameter (e.g., :p_project)
===================================================================================== */

-- DATASET: PARAM_SITE_ID_LIST  (Filter widget list for p_site_id)
SELECT site_id
FROM (
  SELECT 'All' AS site_id
  UNION ALL
  SELECT DISTINCT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE site_id IS NOT NULL
) t
ORDER BY CASE WHEN site_id = 'All' THEN 0 ELSE 1 END, site_id;

-- DATASET: PARAM_PROJECT_LIST  (Filter widget list for p_project)
SELECT project_name
FROM (
  SELECT 'All' AS project_name
  UNION ALL
  SELECT DISTINCT project_name
  FROM cdo_restricted.tableau_dim_workbook
  WHERE project_name IS NOT NULL
) t
ORDER BY CASE WHEN project_name = 'All' THEN 0 ELSE 1 END, project_name;

-- DATASET: PARAM_WORKBOOK_LIST (Filter widget list for p_workbook; optionally depends on p_project)
SELECT workbook_name
FROM (
  SELECT 'All' AS workbook_name
  UNION ALL
  SELECT DISTINCT workbook_name
  FROM cdo_restricted.tableau_dim_workbook
  WHERE workbook_name IS NOT NULL
    AND (:p_project = 'All' OR project_name = :p_project)
) t
ORDER BY CASE WHEN workbook_name = 'All' THEN 0 ELSE 1 END, workbook_name;

-- DATASET: PARAM_VIEW_LIST (Filter widget list for p_view; display label "Workbook / View")
SELECT view_display
FROM (
  SELECT 'All' AS view_display
  UNION ALL
  SELECT DISTINCT concat_ws(' / ', workbook_name, view_name) AS view_display
  FROM cdo_restricted.tableau_dim_view
  WHERE workbook_name IS NOT NULL AND view_name IS NOT NULL
    AND (:p_workbook = 'All' OR workbook_name = :p_workbook)
) t
ORDER BY CASE WHEN view_display = 'All' THEN 0 ELSE 1 END, view_display;

-- DATASET: PARAM_USER_EMAIL_LIST (Filter widget list for p_user_email; scoped to date range)
SELECT user_email
FROM (
  SELECT 'All' AS user_email
  UNION ALL
  SELECT user_email
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE cast(session_start_at as date) BETWEEN :p_date.min AND :p_date.max
    AND user_email IS NOT NULL
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  GROUP BY user_email
) t
ORDER BY CASE WHEN user_email = 'All' THEN 0 ELSE 1 END, user_email;

-- DATASET: PARAM_GROUP_LIST (Filter widget list for p_group)
SELECT group_name
FROM (
  SELECT 'All' AS group_name
  UNION ALL
  SELECT DISTINCT group_name
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name IS NOT NULL
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
) t
ORDER BY CASE WHEN group_name = 'All' THEN 0 ELSE 1 END, group_name;

-- DATASET: PARAM_DEVICE_LIST (Filter widget list for p_device)
SELECT device
FROM (
  SELECT 'All' AS device
  UNION ALL
  SELECT DISTINCT client_device_family AS device
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE client_device_family IS NOT NULL
) t
ORDER BY CASE WHEN device = 'All' THEN 0 ELSE 1 END, device;

-- DATASET: PARAM_BROWSER_LIST (Filter widget list for p_browser)
SELECT browser
FROM (
  SELECT 'All' AS browser
  UNION ALL
  SELECT DISTINCT client_browser_family AS browser
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE client_browser_family IS NOT NULL
) t
ORDER BY CASE WHEN browser = 'All' THEN 0 ELSE 1 END, browser;

-- DATASET: PARAM_SESSION_ID_LIST (Filter widget list for p_session_id; scoped to date range and user)
SELECT session_id
FROM (
  SELECT 'All' AS session_id
  UNION ALL
  SELECT session_id
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE cast(session_start_at as date) BETWEEN :p_date.min AND :p_date.max
    AND session_id IS NOT NULL
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
  GROUP BY session_id
) t
ORDER BY CASE WHEN session_id = 'All' THEN 0 ELSE 1 END, session_id;


/* =====================================================================================
TAB 1 — EXECUTIVE OVERVIEW (60-second health check)
===================================================================================== */

-- DATASET: T1_KPI_TILES  (Visualization: KPI Tiles / single-row table)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
sess_agg AS (
  SELECT
    count(distinct user_email) AS active_users,
    count(distinct session_id) AS sessions,
    sum(http_request_count)    AS http_requests,
    round(100.0 * (sum(http_error_count) / nullif(sum(http_request_count),0)), 2) AS http_error_rate_pct
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE cast(session_start_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_device     = 'All' OR client_device_family = :p_device)
    AND (:p_browser    = 'All' OR client_browser_family = :p_browser)
),
jobs_agg AS (
  SELECT
    sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) AS failed_jobs,
    sum(CASE WHEN finish_status='success' THEN 1 ELSE 0 END) AS successful_jobs,
    round(100.0 * (sum(CASE WHEN finish_status='success' THEN 1 ELSE 0 END) / nullif(count(*),0)), 2) AS job_success_rate_pct
  FROM cdo_restricted.tableau_fct_background_jobs_enriched
  WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
tasks_agg AS (
  SELECT sum(CASE WHEN is_overdue_proxy THEN 1 ELSE 0 END) AS overdue_tasks
  FROM cdo_restricted.tableau_fct_task_health
  WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
fresh_agg AS (
  SELECT sum(CASE WHEN has_extract_any = 1 AND days_since_extract_activity > :p_stale_days THEN 1 ELSE 0 END) AS stale_extract_items
  FROM cdo_restricted.tableau_fct_data_freshness
  WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project = 'All' OR project_name = :p_project)
)
SELECT
  s.active_users,
  s.sessions,
  s.http_requests,
  s.http_error_rate_pct,
  j.failed_jobs,
  j.successful_jobs,
  j.job_success_rate_pct,
  t.overdue_tasks,
  f.stale_extract_items
FROM sess_agg s
CROSS JOIN jobs_agg j
CROSS JOIN tasks_agg t
CROSS JOIN fresh_agg f;

-- DATASET: T1_DAILY_TRENDS  (Visualization: Line chart; X=day, Y=dau/sessions/error_rate)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
)
SELECT
  cast(session_start_at as date) AS day,
  count(distinct user_email)     AS dau,
  count(distinct session_id)     AS sessions,
  round(100.0 * (sum(http_error_count) / nullif(sum(http_request_count),0)), 2) AS http_error_rate_pct
FROM cdo_restricted.tableau_fct_session_summary
WHERE cast(session_start_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_user_email = 'All' OR user_email = :p_user_email)
  AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
  AND (:p_device     = 'All' OR client_device_family = :p_device)
  AND (:p_browser    = 'All' OR client_browser_family = :p_browser)
GROUP BY cast(session_start_at as date)
ORDER BY day;

-- DATASET: T1_TOP_WORKBOOKS  (Visualization: Bar chart; Y=unique_users)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
base AS (
  SELECT
    site_id, user_email, project_name, workbook_name,
    -- view_name might be "Workbook/View" in your env; keep leaf for display
    CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END AS view_tab,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display,
    view_entered_at
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
    AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
    AND (:p_view       = 'All' OR view_display = :p_view)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  count(distinct user_email) AS unique_users,
  count(*) AS view_enters
FROM base
GROUP BY project_name, workbook_name
ORDER BY unique_users DESC, view_enters DESC
LIMIT 25;

-- DATASET: T1_OVERDUE_FAILING_TASKS  (Visualization: Table; action list)
SELECT
  scheduled_action_name AS action,
  schedule_name         AS schedule,
  task_object_type      AS object_type,
  coalesce(task_workbook_name, task_datasource_name) AS object_name,
  consecutive_failure_count AS consecutive_failures,
  last_success_completed_at  AS last_success,
  days_since_last_success    AS days_since_success,
  run_next_at                AS next_run,
  is_overdue_proxy           AS overdue_proxy
FROM cdo_restricted.tableau_fct_task_health
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_workbook = 'All' OR task_workbook_name = :p_workbook)
  AND (is_overdue_proxy = true OR consecutive_failure_count > 0)
ORDER BY is_overdue_proxy DESC, consecutive_failure_count DESC, days_since_last_success DESC
LIMIT 200;

-- DATASET: T1_STALE_EXTRACTS_RISK_LIST  (Visualization: Table)
SELECT
  project_name        AS project,
  content_type,
  content_name,
  days_since_extract_activity AS days_since_refresh,
  uses_bridge_any     AS uses_bridge,
  has_extract_any     AS has_extract
FROM cdo_restricted.tableau_fct_data_freshness
WHERE (:p_site_id   = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_project   = 'All' OR project_name = :p_project)
  AND has_extract_any = 1
  AND days_since_extract_activity > :p_stale_days
ORDER BY days_since_extract_activity DESC, project_name, content_name
LIMIT 500;


/* =====================================================================================
TAB 2 — ADOPTION & CONTENT PORTFOLIO
===================================================================================== */

-- DATASET: T2_TOP_VIEWS  (Visualization: Bar chart; Y=unique_users or view_enters)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
base AS (
  SELECT
    site_id, user_email, project_name, workbook_name, view_name, view_entered_at,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
    AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  view_display AS view,
  count(*) AS view_enters,
  count(distinct user_email) AS unique_users
FROM base
WHERE (:p_view = 'All' OR view_display = :p_view)
GROUP BY project_name, workbook_name, view_display
ORDER BY unique_users DESC, view_enters DESC
LIMIT 50;

-- DATASET: T2_STALE_UNUSED_VIEWS  (Visualization: Table; last access + days since)
WITH last_from_metrics AS (
  SELECT site_id, view_id, max(metric_date) AS last_view_date
  FROM cdo_restricted.tableau_fct_view_metrics_daily
  WHERE view_count > 0
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  GROUP BY site_id, view_id
),
last_from_user_stats AS (
  SELECT site_id, view_id, max(last_viewed_at) AS last_viewed_at_any_user
  FROM cdo_restricted.tableau_fct_views_stats_enriched
  WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  GROUP BY site_id, view_id
),
base AS (
  SELECT
    dv.site_id,
    dv.project_name,
    dv.workbook_name,
    dv.view_id,
    dv.view_name,
    concat_ws(' / ', dv.workbook_name, dv.view_name) AS view_display,
    greatest(
      coalesce(cast(lm.last_view_date as timestamp), timestamp('1900-01-01')),
      coalesce(ls.last_viewed_at_any_user,            timestamp('1900-01-01'))
    ) AS last_access_at
  FROM cdo_restricted.tableau_dim_view dv
  LEFT JOIN last_from_metrics lm
    ON lm.site_id = dv.site_id AND lm.view_id = dv.view_id
  LEFT JOIN last_from_user_stats ls
    ON ls.site_id = dv.site_id AND ls.view_id = dv.view_id
  WHERE (:p_site_id  = 'All' OR cast(dv.site_id as string) = :p_site_id)
    AND (:p_project  = 'All' OR dv.project_name = :p_project)
    AND (:p_workbook = 'All' OR dv.workbook_name = :p_workbook)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  view_display AS view,
  last_access_at,
  datediff(current_date(), cast(last_access_at as date)) AS days_since_last_access
FROM base
WHERE (:p_view = 'All' OR view_display = :p_view)
  AND datediff(current_date(), cast(last_access_at as date)) >= :p_stale_days
ORDER BY days_since_last_access DESC, project, workbook, view
LIMIT 500;

-- DATASET: T2_NEW_CONTENT_TREND  (Visualization: Line chart; new workbooks/views/datasources per day)
WITH wb AS (
  SELECT cast(workbook_created_at as date) AS day, count(*) AS new_workbooks
  FROM cdo_restricted.tableau_dim_workbook
  WHERE cast(workbook_created_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project = 'All' OR project_name = :p_project)
  GROUP BY cast(workbook_created_at as date)
),
vw AS (
  SELECT cast(view_created_at as date) AS day, count(*) AS new_views
  FROM cdo_restricted.tableau_dim_view
  WHERE cast(view_created_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project = 'All' OR project_name = :p_project)
  GROUP BY cast(view_created_at as date)
),
ds AS (
  SELECT cast(datasource_created_at as date) AS day, count(*) AS new_datasources
  FROM cdo_restricted.tableau_dim_datasource
  WHERE cast(datasource_created_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project = 'All' OR project_name = :p_project)
  GROUP BY cast(datasource_created_at as date)
)
SELECT
  coalesce(wb.day, vw.day, ds.day) AS day,
  coalesce(new_workbooks, 0) AS new_workbooks,
  coalesce(new_views, 0)     AS new_views,
  coalesce(new_datasources, 0) AS new_datasources
FROM wb
FULL OUTER JOIN vw ON vw.day = wb.day
FULL OUTER JOIN ds ON ds.day = coalesce(wb.day, vw.day)
ORDER BY day;

-- DATASET: T2_BUS_FACTOR_OWNERSHIP  (Visualization: Table or bar; “ownership concentration”)
WITH base AS (
  SELECT
    workbook_owner_email AS owner_email,
    count(*) AS workbooks_owned
  FROM cdo_restricted.tableau_dim_workbook
  WHERE is_deleted = false
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project = 'All' OR project_name = :p_project)
  GROUP BY workbook_owner_email
),
tot AS (SELECT sum(workbooks_owned) AS total_workbooks FROM base)
SELECT
  owner_email,
  workbooks_owned,
  round(100.0 * workbooks_owned / nullif((SELECT total_workbooks FROM tot),0), 2) AS pct_of_workbooks
FROM base
ORDER BY workbooks_owned DESC
LIMIT 50;

-- DATASET: T2_ACTIVE_USERS_BY_GROUP  (Visualization: Bar chart; groups by sessions/users)
WITH base AS (
  SELECT
    gm.group_name,
    ss.user_email,
    ss.session_id
  FROM cdo_restricted.tableau_fct_session_summary ss
  JOIN cdo_restricted.tableau_dim_group_membership gm
    ON gm.user_email = ss.user_email
   AND gm.site_id = ss.site_id
  WHERE cast(ss.session_start_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(ss.site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR ss.user_email = :p_user_email)
)
SELECT
  group_name AS group,
  count(distinct user_email) AS active_users,
  count(distinct session_id) AS sessions
FROM base
WHERE (:p_group = 'All' OR group_name = :p_group)
GROUP BY group_name
ORDER BY active_users DESC, sessions DESC
LIMIT 50;


/* =====================================================================================
TAB 3 — ENGAGEMENT & NAVIGATION (Funnels)
===================================================================================== */

-- DATASET: T3_LANDING_VIEWS  (Visualization: Bar chart; top landing views)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
ranked AS (
  SELECT
    site_id,
    session_id,
    user_email,
    project_name,
    workbook_name,
    view_name,
    view_key,
    view_entered_at,
    row_number() OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS rn,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
    AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
)
SELECT
  workbook_name AS workbook,
  view_display  AS landing_view,
  count(*) AS sessions_landed,
  count(distinct user_email) AS unique_users
FROM ranked
WHERE rn = 1
  AND (:p_view = 'All' OR view_display = :p_view)
GROUP BY workbook_name, view_display
ORDER BY sessions_landed DESC
LIMIT 50;

-- DATASET: T3_TOP_TRANSITIONS  (Visualization: Table; (from_view -> to_view) for Sankey or bar)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
nav AS (
  SELECT
    site_id, session_id, user_email, workbook_name, project_name, view_key, view_name, view_entered_at,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
    AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
),
paths AS (
  SELECT
    workbook_name,
    view_display AS from_view,
    lead(view_display) OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS to_view
  FROM nav
)
SELECT
  workbook_name AS workbook,
  from_view,
  to_view,
  count(*) AS transitions
FROM paths
WHERE to_view IS NOT NULL
  AND (:p_view = 'All' OR from_view = :p_view)
GROUP BY workbook_name, from_view, to_view
ORDER BY transitions DESC
LIMIT 200;

-- DATASET: T3_BOUNCE_RATE_BY_WORKBOOK  (Visualization: Bar chart; bounce_rate_pct)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
per_session AS (
  SELECT
    workbook_name,
    session_id,
    count(distinct view_key) AS distinct_views
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
  GROUP BY workbook_name, session_id
)
SELECT
  workbook_name AS workbook,
  count(*) AS sessions,
  sum(CASE WHEN distinct_views = 1 THEN 1 ELSE 0 END) AS bounce_sessions,
  round(100.0 * sum(CASE WHEN distinct_views = 1 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS bounce_rate_pct
FROM per_session
WHERE (:p_workbook = 'All' OR workbook_name = :p_workbook)
GROUP BY workbook_name
ORDER BY bounce_rate_pct DESC, sessions DESC
LIMIT 50;


/* =====================================================================================
TAB 4 — TIME SPENT (Inferred) + DRILLDOWN
===================================================================================== */

-- DATASET: T4_TOP_VIEWS_BY_DWELL  (Visualization: Bar chart; total_dwell_hours)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
base AS (
  SELECT
    site_id, user_email, project_name, workbook_name, view_name, view_entered_at, dwell_seconds_capped,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display
  FROM cdo_restricted.tableau_fct_view_dwell
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
    AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
    AND (:p_project    = 'All' OR project_name = :p_project)
    AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  view_display  AS view,
  round(sum(dwell_seconds_capped)/3600.0, 2) AS total_dwell_hours,
  count(distinct user_email) AS unique_users,
  count(*) AS view_visits
FROM base
WHERE (:p_view = 'All' OR view_display = :p_view)
GROUP BY project_name, workbook_name, view_display
ORDER BY total_dwell_hours DESC
LIMIT 50;

-- DATASET: T4_TOP_WORKBOOKS_BY_DWELL  (Visualization: Bar chart)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  round(sum(workbook_dwell_seconds_capped)/3600.0, 2) AS total_dwell_hours,
  count(distinct user_email) AS unique_users,
  count(*) AS sessions_in_workbook
FROM cdo_restricted.tableau_fct_workbook_dwell
WHERE cast(workbook_entered_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_user_email = 'All' OR user_email = :p_user_email)
  AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
  AND (:p_project    = 'All' OR project_name = :p_project)
  AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
GROUP BY project_name, workbook_name
ORDER BY total_dwell_hours DESC
LIMIT 50;

-- DATASET: T4_DWELL_HISTOGRAM  (Visualization: Histogram; bin=30s)
WITH base AS (
  SELECT dwell_seconds_capped
  FROM cdo_restricted.tableau_fct_view_dwell
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
),
binned AS (
  SELECT floor(dwell_seconds_capped / 30) * 30 AS dwell_bin_start_sec
  FROM base
)
SELECT
  dwell_bin_start_sec AS dwell_seconds_bin_start,
  count(*) AS visits
FROM binned
GROUP BY dwell_bin_start_sec
ORDER BY dwell_bin_start_sec;

-- DATASET: T4_SESSION_TIMELINE_DRILLDOWN  (Visualization: Table; filter by p_session_id)
SELECT
  session_id,
  user_email AS user,
  project_name AS project,
  workbook_name AS workbook,
  view_name AS view,
  view_entered_at,
  view_exited_at,
  dwell_seconds_capped AS dwell_seconds,
  view_load_time_ms_proxy AS load_ms_proxy
FROM cdo_restricted.tableau_fct_view_dwell
WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_session_id = 'All' OR session_id = :p_session_id)
  AND (:p_user_email = 'All' OR user_email = :p_user_email)
ORDER BY view_entered_at DESC
LIMIT 2000;

-- DATASET: T4_INTERACTION_INTENSITY_PROXY  (Visualization: Scatter; X=dwell_seconds, Y=request_count)
WITH gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  WHERE group_name = :p_group
    AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
)
SELECT
  workbook_name AS workbook,
  view_name AS view,
  dwell_seconds_capped AS dwell_seconds,
  request_count_during_view AS requests_during_view,
  error_count_during_view AS errors_during_view,
  avg_request_duration_ms_during_view AS avg_request_ms
FROM cdo_restricted.tableau_fct_view_interaction_intensity
WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id    = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_user_email = 'All' OR user_email = :p_user_email)
  AND (:p_group      = 'All' OR user_email IN (SELECT user_email FROM gm))
  AND (:p_workbook   = 'All' OR workbook_name = :p_workbook)
LIMIT 50000;

-- DATASET: T4_TOOLTIP_USAGE (Visualization: Bar/Line; tooltip events)
SELECT
  cast(request_created_at as date) AS day,
  coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
  coalesce(view_name, currentsheet, 'Unknown') AS view,
  count(*) AS tooltip_events
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE cast(request_created_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_user_email = 'All' OR user_email = :p_user_email)
  AND (:p_device  = 'All' OR client_device_family = :p_device)
  AND (:p_browser = 'All' OR client_browser_family = :p_browser)
  AND action LIKE '%render-tooltip-server%'
GROUP BY cast(request_created_at as date),
         coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
         coalesce(view_name, currentsheet, 'Unknown')
ORDER BY day DESC
LIMIT 5000;


/* =====================================================================================
TAB 5 — PERFORMANCE (Repo proxy + errors)
===================================================================================== */

-- DATASET: T5_SLOWEST_VIEWS_P95 (Visualization: Bar; p95_load_ms_proxy)
WITH base AS (
  SELECT
    project_name,
    workbook_name,
    concat_ws(' / ', workbook_name, CASE WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1) ELSE view_name END) AS view_display,
    view_load_time_ms_proxy
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN :p_date.min AND :p_date.max
    AND view_load_time_ms_proxy IS NOT NULL
    AND (:p_site_id  = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project  = 'All' OR project_name = :p_project)
    AND (:p_workbook = 'All' OR workbook_name = :p_workbook)
)
SELECT
  project_name AS project,
  workbook_name AS workbook,
  view_display  AS view,
  percentile_approx(view_load_time_ms_proxy, 0.95) AS p95_load_ms_proxy,
  percentile_approx(view_load_time_ms_proxy, 0.50) AS p50_load_ms_proxy,
  count(*) AS view_enters
FROM base
WHERE (:p_view = 'All' OR view_display = :p_view)
GROUP BY project_name, workbook_name, view_display
HAVING count(*) >= 25
ORDER BY p95_load_ms_proxy DESC
LIMIT 50;

-- DATASET: T5_ERROR_HOTSPOTS_BY_VIEW (Visualization: Table)
WITH base AS (
  SELECT
    cast(request_created_at as date) AS day,
    coalesce(project_name, 'Unknown') AS project,
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    coalesce(view_name, currentsheet, 'Unknown') AS view,
    http_status
  FROM cdo_restricted.tableau_fct_http_requests_enriched
  WHERE cast(request_created_at as date) BETWEEN :p_date.min AND :p_date.max
    AND (:p_site_id  = 'All' OR cast(site_id as string) = :p_site_id)
    AND (:p_project  = 'All' OR project_name = :p_project)
    AND (:p_workbook = 'All' OR coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') = :p_workbook)
    AND (:p_user_email = 'All' OR user_email = :p_user_email)
)
SELECT
  project,
  workbook,
  view,
  count(*) AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS errors_5xx,
  round(100.0 * sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS error_rate_pct
FROM base
GROUP BY project, workbook, view
HAVING count(*) >= 100
ORDER BY error_rate_pct DESC, errors_5xx DESC
LIMIT 200;

-- DATASET: T5_ERROR_BY_CONTROLLER_ACTION (Visualization: Table; triage)
SELECT
  controller,
  action,
  count(*) AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS errors_5xx
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE cast(request_created_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
GROUP BY controller, action
ORDER BY errors_5xx DESC, errors DESC
LIMIT 200;

-- DATASET: T5_CLIENT_SEGMENTATION (Visualization: Stacked bar; errors by device/browser)
SELECT
  client_device_family AS device,
  client_browser_family AS browser,
  count(*) AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE cast(request_created_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
GROUP BY client_device_family, client_browser_family
ORDER BY requests DESC
LIMIT 200;


/* =====================================================================================
TAB 6 — RELIABILITY (Jobs/Schedules/Tasks) + “Did it write?” proxies
===================================================================================== */

-- DATASET: T6_JOB_SUCCESS_TREND (Visualization: Line; success_rate + avg queue/run)
SELECT
  cast(coalesce(completed_at, created_at) as date) AS day,
  job_type,
  count(*) AS jobs,
  sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) AS failures,
  round(100.0 * (1 - (sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) / nullif(count(*),0))), 2) AS success_rate_pct,
  round(avg(queue_seconds), 1) AS avg_queue_sec,
  round(avg(run_seconds), 1)   AS avg_run_sec
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
GROUP BY cast(coalesce(completed_at, created_at) as date), job_type
ORDER BY day, job_type;

-- DATASET: T6_JOBS_BY_SCHEDULE_FREQ (Visualization: Bar; schedule run frequency)
SELECT
  resolved_schedule_name AS schedule,
  resolved_scheduled_action_name AS action_type,
  count(*) AS jobs
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND resolved_schedule_name IS NOT NULL
GROUP BY resolved_schedule_name, resolved_scheduled_action_name
ORDER BY jobs DESC
LIMIT 100;

-- DATASET: T6_RECENT_FAILED_JOBS (Visualization: Table; “what is failing right now”)
SELECT
  completed_at,
  job_type,
  job_title,
  finish_status,
  resolved_schedule_name AS schedule,
  resolved_scheduled_action_name AS action,
  resolved_object_type AS object_type,
  coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
  run_seconds,
  queue_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND finish_status = 'failure'
ORDER BY coalesce(completed_at, created_at) DESC
LIMIT 300;

-- DATASET: T6_REFRESH_SUCCESS_NO_WRITE_PROXY (Visualization: Table; “success but maybe no data updated”)
SELECT
  completed_at,
  job_type,
  resolved_scheduled_action_name AS action,
  coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
  finish_status,
  extract_timestamp_updated_proxy,
  extract_dir_updated_proxy
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND finish_status = 'success'
  AND (extract_timestamp_updated_proxy = false AND extract_dir_updated_proxy = false)
ORDER BY completed_at DESC
LIMIT 300;

-- DATASET: T6_TASK_HEALTH_ACTION_LIST (Visualization: Table; overdue + failing tasks)
SELECT
  scheduled_action_name AS action,
  schedule_name AS schedule,
  task_object_type AS object_type,
  coalesce(task_workbook_name, task_datasource_name) AS object_name,
  consecutive_failure_count AS consecutive_failures,
  last_success_completed_at AS last_success,
  days_since_last_success AS days_since_success,
  run_next_at AS next_run,
  is_overdue_proxy
FROM cdo_restricted.tableau_fct_task_health
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (is_overdue_proxy = true OR consecutive_failure_count > 0)
ORDER BY is_overdue_proxy DESC, consecutive_failures DESC, days_since_success DESC
LIMIT 500;

-- DATASET: T6_MOST_RECENT_REFRESH_ALERTS (Visualization: Table; latest refresh outcomes)
SELECT
  refresh_occurred_at,
  refresh_status,
  duration_in_ms,
  workbook_name,
  datasource_name,
  details
FROM cdo_restricted.tableau_fct_extract_refresh_most_recent
WHERE cast(refresh_occurred_at as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
ORDER BY refresh_occurred_at DESC
LIMIT 300;


/* =====================================================================================
TAB 7 — DATA FRESHNESS + LIVE VS EXTRACT + BRIDGE
===================================================================================== */

-- DATASET: T7_STALE_EXTRACTS (Visualization: Heatmap/Table)
SELECT
  project_name AS project,
  content_type,
  content_name,
  days_since_extract_activity AS days_since_refresh,
  uses_bridge_any AS uses_bridge
FROM cdo_restricted.tableau_fct_data_freshness
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_project = 'All' OR project_name = :p_project)
  AND has_extract_any = 1
ORDER BY days_since_extract_activity DESC
LIMIT 1000;

-- DATASET: T7_SLA_COMPLIANCE_BUCKETS (Visualization: Bar; bucket counts)
SELECT
  project_name AS project,
  sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity <= 1 THEN 1 ELSE 0 END) AS refreshed_1d,
  sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity BETWEEN 2 AND 2 THEN 1 ELSE 0 END) AS refreshed_2d,
  sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity BETWEEN 3 AND 7 THEN 1 ELSE 0 END) AS refreshed_3_7d,
  sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity > 7 THEN 1 ELSE 0 END) AS stale_over_7d,
  sum(CASE WHEN has_extract_any=0 THEN 1 ELSE 0 END) AS live_content
FROM cdo_restricted.tableau_fct_data_freshness
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_project = 'All' OR project_name = :p_project)
GROUP BY project_name
ORDER BY stale_over_7d DESC, live_content DESC;

-- DATASET: T7_LIVE_VS_EXTRACT_BY_PROJECT (Visualization: Stacked bar)
SELECT
  project_name AS project,
  sum(CASE WHEN has_extract_any = 1 THEN 1 ELSE 0 END) AS extract_content_count,
  sum(CASE WHEN has_extract_any = 0 THEN 1 ELSE 0 END) AS live_content_count,
  sum(CASE WHEN uses_bridge_any = 1 THEN 1 ELSE 0 END) AS bridge_content_count
FROM cdo_restricted.tableau_fct_data_freshness
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_project = 'All' OR project_name = :p_project)
GROUP BY project_name
ORDER BY live_content_count DESC, extract_content_count DESC;


/* =====================================================================================
TAB 8 — SUBSCRIPTIONS
===================================================================================== */

-- DATASET: T8_SUBSCRIPTION_HEALTH (Visualization: Table)
SELECT
  subscription_subject AS subscription,
  recipient_email      AS recipient,
  schedule_name        AS schedule,
  last_completed_at    AS last_run_completed_at,
  last_finish_status   AS last_status,
  last_run_seconds     AS last_runtime_sec
FROM cdo_restricted.tableau_fct_subscription_health
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
ORDER BY last_run_completed_at DESC NULLS LAST
LIMIT 1000;

-- DATASET: T8_RECENT_SUBSCRIPTION_FAILURES (Visualization: Table)
SELECT
  completed_at,
  subscription_subject,
  subscription_recipient_email,
  finish_status,
  run_seconds,
  queue_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND subscription_id IS NOT NULL
  AND finish_status = 'failure'
ORDER BY completed_at DESC
LIMIT 300;


/* =====================================================================================
TAB 9 — GOVERNANCE & AUDIT
===================================================================================== */

-- DATASET: T9_AUDIT_EVENTS_BY_DAY (Visualization: Stacked bar; events by action_type)
SELECT
  cast(event_created_at as date) AS day,
  action_type,
  count(*) AS events
FROM cdo_restricted.tableau_fct_historical_events_enriched
WHERE cast(event_created_at as date) BETWEEN :p_date.min AND :p_date.max
GROUP BY cast(event_created_at as date), action_type
ORDER BY day, action_type;

-- DATASET: T9_RECENT_AUDIT_EVENTS (Visualization: Table; evidence drill list)
SELECT
  event_created_at,
  action_type,
  event_name,
  actor_name,
  actor_email,
  project_name,
  workbook_name,
  view_name,
  datasource_name,
  schedule_name,
  details
FROM cdo_restricted.tableau_fct_historical_events_enriched
WHERE cast(event_created_at as date) BETWEEN :p_date.min AND :p_date.max
ORDER BY event_created_at DESC
LIMIT 1000;

-- DATASET: T9_GROUP_MEMBERSHIP_COUNTS (Visualization: Bar)
SELECT
  group_name AS group,
  count(distinct user_email) AS members
FROM cdo_restricted.tableau_dim_group_membership
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
  AND (:p_group   = 'All' OR group_name = :p_group)
GROUP BY group_name
ORDER BY members DESC
LIMIT 200;

-- DATASET: T9_TAG_COVERAGE (Visualization: Table; tags by content type)
SELECT
  taggable_type AS content_type,
  tag_name,
  count(*) AS taggings
FROM cdo_restricted.tableau_dim_content_tags
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
GROUP BY taggable_type, tag_name
ORDER BY taggings DESC
LIMIT 300;

-- DATASET: T9_DATA_QUALITY_INDICATORS (Visualization: Table)
SELECT
  content_type,
  content_id,
  data_quality_type,
  is_active,
  is_severe,
  message,
  user_email AS set_by,
  created_at,
  updated_at
FROM cdo_restricted.tableau_fct_data_quality_indicators
WHERE (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
ORDER BY updated_at DESC
LIMIT 1000;


/* =====================================================================================
TAB 10 — CAPACITY & LICENSE (Optional but standard)
===================================================================================== */

-- DATASET: T10_DISK_USAGE_TREND (Visualization: Line; pct used by worker/path)
SELECT
  cast(record_timestamp as date) AS day,
  worker_id,
  path,
  total_space_bytes,
  used_space_bytes,
  round(100.0 * used_space_bytes / nullif(total_space_bytes,0), 2) AS pct_used,
  state
FROM cdo_restricted.tableau_fct_disk_usage
WHERE cast(record_timestamp as date) BETWEEN :p_date.min AND :p_date.max
ORDER BY day DESC
LIMIT 5000;

-- DATASET: T10_LARGEST_CONTENT (Visualization: Table; top sizes)
SELECT
  'Workbook' AS content_type,
  project_name,
  workbook_name AS content_name,
  workbook_owner_email AS owner_email,
  workbook_size_bytes AS size_bytes
FROM cdo_restricted.tableau_dim_workbook
WHERE is_deleted = false
  AND workbook_size_bytes IS NOT NULL
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
UNION ALL
SELECT
  'Datasource' AS content_type,
  project_name,
  datasource_name AS content_name,
  datasource_owner_email AS owner_email,
  datasource_size_bytes AS size_bytes
FROM cdo_restricted.tableau_dim_datasource
WHERE datasource_size_bytes IS NOT NULL
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
ORDER BY size_bytes DESC
LIMIT 200;

-- DATASET: T10_LICENSE_USAGE (Visualization: Table or bar; NOTE retention often <= 183 days in practice)
SELECT
  cast(date_last_used as date) AS last_used_day,
  site_name,
  user_role,
  product_type,
  server_email,
  server_user_name,
  host_name,
  product_version,
  date_last_used,
  date_last_updated
FROM cdo_restricted.tableau_fct_license_usage
WHERE cast(date_last_used as date) BETWEEN :p_date.min AND :p_date.max
  AND (:p_site_id = 'All' OR cast(site_id as string) = :p_site_id)
ORDER BY date_last_used DESC
LIMIT 2000;

-- DATASET: T10_GAPS_AND_NEXT_STEPS (Visualization: Small table)
SELECT 'Exact filter field + value applied' AS missing_signal,
       'Not reliably in Tableau repository. Use Activity Log (Advanced Management) and/or instrument via Extensions/Embedding to log filter-change values.' AS how_to_get_it
UNION ALL
SELECT 'Mark-level clicks (what marks were clicked)',
       'Not in repository. Instrument mark selection events via Extensions/Embedding and store in lakehouse.'
UNION ALL
SELECT 'Tooltip hover usage',
       'Not a first-class repo event; you CAN proxy some tooltip rendering via render-tooltip-server requests; full fidelity requires instrumentation.';

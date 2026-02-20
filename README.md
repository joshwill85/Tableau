BLOCK 1 — DATASET CHECKLIST (what you will create)

CHOICE LIST (dropdown) datasets:
- PARAM_SITE_ID_LIST           -> binds p_site_id
- PARAM_PROJECT_LIST           -> binds p_project
- PARAM_WORKBOOK_LIST          -> binds p_workbook (cascades on p_project)
- PARAM_VIEW_LIST              -> binds p_view (cascades on p_workbook) label = "Workbook / View"
- PARAM_USER_EMAIL_LIST        -> binds p_user_email (scoped by p_date + p_site_id)
- PARAM_GROUP_LIST             -> binds p_group (optional cascade on p_site_id)
- PARAM_DEVICE_LIST            -> binds p_device
- PARAM_BROWSER_LIST           -> binds p_browser
- PARAM_SESSION_ID_LIST        -> binds p_session_id (scoped by p_date + p_site_id + p_user_email)

ANALYTICS datasets (core insights; each powers one or more visuals):
TAB 1 Executive:
- DS_EXEC_KPIS (tiles)
- DS_DAILY_ADOPTION_TREND (line)
- DS_TOP_WORKBOOKS (bar)
- DS_TOP_VIEWS (bar)
- DS_OVERDUE_FAILING_TASKS (table)
- DS_STALE_EXTRACTS_RISK_LIST (table)
- DS_RECENT_FAILED_JOBS (table)

TAB 2 Adoption & Portfolio:
- DS_CONTENT_GROWTH_TREND (line)
- DS_STALE_UNUSED_VIEWS (table)
- DS_BUS_FACTOR_OWNERSHIP (bar/table)
- DS_ACTIVE_USERS_BY_GROUP (bar)

TAB 3 Engagement & Navigation:
- DS_LANDING_VIEWS (bar)
- DS_TOP_TRANSITIONS (table / sankey-ready)
- DS_BOUNCE_RATE_BY_WORKBOOK (bar)
- DS_RETURNING_USERS (tile)

TAB 4 Time Spent:
- DS_DWELL_BY_VIEW (bar)
- DS_DWELL_BY_WORKBOOK (bar)
- DS_SESSION_TIMELINE_DRILLDOWN (table)
- DS_TOOLTIP_USAGE (line/table)
- DS_CONFUSION_CANDIDATES (table)

TAB 5 Performance:
- DS_SLOW_VIEWS_P95 (bar)
- DS_ERROR_HOTSPOTS_BY_VIEW (table)
- DS_ERROR_BY_CONTROLLER_ACTION (table)
- DS_CLIENT_SEGMENTATION (stacked bar)
- DS_TOP_CLIENT_TOKENS (table)

TAB 6 Reliability (Jobs/Schedules):
- DS_JOB_SUCCESS_TREND (line)
- DS_JOBS_BY_SCHEDULE_FREQ (bar)
- DS_LONGEST_JOBS (table)
- DS_QUEUE_TIME_HOTSPOTS (table)
- DS_REFRESH_SUCCESS_NO_WRITE_PROXY (table)
- DS_MOST_RECENT_REFRESH_ALERTS (table)
- DS_TASK_HEALTH_ACTION_LIST (table)

TAB 7 Freshness:
- DS_STALE_EXTRACTS (table/heatmap)
- DS_SLA_BUCKETS (stacked bar)
- DS_LIVE_VS_EXTRACT_BY_PROJECT (stacked bar)
- DS_BRIDGE_USAGE_INVENTORY (table)

TAB 8 Subscriptions:
- DS_SUBSCRIPTION_HEALTH (table)
- DS_RECENT_SUBSCRIPTION_FAILURES (table)
- DS_TOP_SUBSCRIPTION_RECIPIENTS (bar/table)

TAB 9 Governance & Audit:
- DS_AUDIT_EVENTS_BY_DAY (stacked bar)
- DS_RECENT_AUDIT_EVENTS (table)
- DS_TOP_ACTORS (bar)
- DS_GROUP_MEMBERSHIP_COUNTS (bar)
- DS_GROUP_MEMBERSHIP_DETAIL (table)
- DS_TAG_COVERAGE (table)
- DS_DATA_QUALITY_INDICATORS (table)
- DS_CERTIFIED_DATASOURCES (table)

TAB 10 Capacity & License:
- DS_DISK_USAGE_TREND (line)
- DS_STORAGE_BY_PROJECT (bar)
- DS_LARGEST_CONTENT (table)
- DS_LICENSE_USAGE (table)

TAB 11 Retention & Archiving:
- DS_RETENTION_WINDOWS_ACTUAL (table: min/max dates per source table)
- DS_RETENTION_GUIDE (static table: what to archive + suggested cadence)


  /* BLOCK 2 — CHOICE LIST (dropdown) DATASETS
Create EACH as its own dataset: Data → Add dataset → Create from SQL */

-- DATASET: PARAM_SITE_ID_LIST  (binds p_site_id)
SELECT site_id
FROM (
  SELECT 'All' AS site_id
  UNION ALL
  SELECT DISTINCT cast(site_id as string)
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE site_id IS NOT NULL
) t
ORDER BY CASE WHEN site_id='All' THEN 0 ELSE 1 END, site_id;

-- DATASET: PARAM_PROJECT_LIST  (binds p_project)
SELECT project_name
FROM (
  SELECT 'All' AS project_name
  UNION ALL
  SELECT DISTINCT project_name
  FROM cdo_restricted.tableau_dim_workbook
  WHERE project_name IS NOT NULL
) t
ORDER BY CASE WHEN project_name='All' THEN 0 ELSE 1 END, project_name;

-- DATASET: PARAM_WORKBOOK_LIST (binds p_workbook; cascades on p_project)
WITH p AS (SELECT coalesce(nullif(:p_project,''), 'All') AS project)
SELECT workbook_name
FROM (
  SELECT 'All' AS workbook_name
  UNION ALL
  SELECT DISTINCT workbook_name
  FROM cdo_restricted.tableau_dim_workbook
  CROSS JOIN p
  WHERE workbook_name IS NOT NULL
    AND (p.project='All' OR project_name=p.project)
) t
ORDER BY CASE WHEN workbook_name='All' THEN 0 ELSE 1 END, workbook_name;

-- DATASET: PARAM_VIEW_LIST (binds p_view; cascades on p_workbook; label "Workbook / View")
WITH p AS (SELECT coalesce(nullif(:p_workbook,''), 'All') AS workbook)
SELECT view_display
FROM (
  SELECT 'All' AS view_display
  UNION ALL
  SELECT DISTINCT concat_ws(' / ', workbook_name, view_name) AS view_display
  FROM cdo_restricted.tableau_dim_view
  CROSS JOIN p
  WHERE workbook_name IS NOT NULL AND view_name IS NOT NULL
    AND (p.workbook='All' OR workbook_name=p.workbook)
) t
ORDER BY CASE WHEN view_display='All' THEN 0 ELSE 1 END, view_display;

-- DATASET: PARAM_USER_EMAIL_LIST (binds p_user_email; scoped by p_date + p_site_id)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_id
)
SELECT user_email
FROM (
  SELECT 'All' AS user_email
  UNION ALL
  SELECT user_email
  FROM cdo_restricted.tableau_fct_session_summary
  CROSS JOIN p
  WHERE cast(session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND user_email IS NOT NULL
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY user_email
) t
ORDER BY CASE WHEN user_email='All' THEN 0 ELSE 1 END, user_email
LIMIT 2000;

-- DATASET: PARAM_GROUP_LIST (binds p_group; optional cascade on p_site_id)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''), 'All') AS site_id)
SELECT group_name
FROM (
  SELECT 'All' AS group_name
  UNION ALL
  SELECT DISTINCT group_name
  FROM cdo_restricted.tableau_dim_group_membership
  CROSS JOIN p
  WHERE group_name IS NOT NULL
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
) t
ORDER BY CASE WHEN group_name='All' THEN 0 ELSE 1 END, group_name;

-- DATASET: PARAM_DEVICE_LIST (binds p_device)
SELECT device
FROM (
  SELECT 'All' AS device
  UNION ALL
  SELECT DISTINCT client_device_family
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE client_device_family IS NOT NULL
) t
ORDER BY CASE WHEN device='All' THEN 0 ELSE 1 END, device;

-- DATASET: PARAM_BROWSER_LIST (binds p_browser)
SELECT browser
FROM (
  SELECT 'All' AS browser
  UNION ALL
  SELECT DISTINCT client_browser_family
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE client_browser_family IS NOT NULL
) t
ORDER BY CASE WHEN browser='All' THEN 0 ELSE 1 END, browser;

-- DATASET: PARAM_SESSION_ID_LIST (binds p_session_id; scoped by p_date + p_site_id + p_user_email)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_id,
    coalesce(nullif(:p_user_email,''), 'All') AS user_email
)
SELECT session_id
FROM (
  SELECT 'All' AS session_id
  UNION ALL
  SELECT session_id
  FROM cdo_restricted.tableau_fct_session_summary
  CROSS JOIN p
  WHERE cast(session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND session_id IS NOT NULL
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.user_email='All' OR user_email=p.user_email)
  GROUP BY session_id
) t
ORDER BY CASE WHEN session_id='All' THEN 0 ELSE 1 END, session_id
LIMIT 2000;

/* BLOCK 3 — ANALYTICS DATASETS (the insights)
Create EACH as its own dataset: Data → Add dataset → Create from SQL
All use shared parameters: p_date, p_site_id, p_project, p_workbook, p_view, p_user_email, p_group, p_device, p_browser, p_stale_days, p_session_id */

/* ---------------- TAB 1: EXECUTIVE OVERVIEW ---------------- */

-- DATASET: DS_EXEC_KPIS (Visual: tiles/counters)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_id,
    coalesce(nullif(:p_user_email,''), 'All') AS user_email,
    coalesce(nullif(:p_group,''), 'All')      AS grp,
    coalesce(nullif(:p_device,''), 'All')     AS device,
    coalesce(nullif(:p_browser,''), 'All')    AS browser,
    coalesce(nullif(:p_project,''), 'All')    AS project,
    coalesce(:p_stale_days, 7)                AS stale_days
),
gm AS (
  SELECT DISTINCT user_email
  FROM cdo_restricted.tableau_dim_group_membership
  CROSS JOIN p
  WHERE p.grp <> 'All'
    AND group_name=p.grp
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
),
sess AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_session_summary
  CROSS JOIN p
  WHERE cast(session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.user_email='All' OR user_email=p.user_email)
    AND (p.device='All' OR client_device_family=p.device)
    AND (p.browser='All' OR client_browser_family=p.browser)
    AND (p.grp='All' OR user_email IN (SELECT user_email FROM gm))
),
nav_sess AS (
  SELECT
    site_id, session_id,
    count(distinct view_key) AS distinct_views
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.user_email='All' OR user_email=p.user_email)
    AND (p.grp='All' OR user_email IN (SELECT user_email FROM gm))
  GROUP BY site_id, session_id
),
jobs AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_background_jobs_enriched
  CROSS JOIN p
  WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
),
tasks AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_task_health
  CROSS JOIN p
  WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
),
fresh AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_data_freshness
  CROSS JOIN p
  WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.project='All' OR project_name=p.project)
)
SELECT
  count(distinct sess.user_email) AS active_users,
  count(distinct sess.session_id) AS sessions,
  round(avg(sess.session_seconds),1) AS avg_session_seconds,
  sum(sess.http_request_count) AS http_requests,
  round(100.0 * sum(sess.http_error_count) / nullif(sum(sess.http_request_count),0), 2) AS http_error_rate_pct,
  round(100.0 * sum(CASE WHEN nav_sess.distinct_views=1 THEN 1 ELSE 0 END) / nullif(count(nav_sess.session_id),0), 2) AS bounce_rate_pct,
  sum(CASE WHEN jobs.finish_status='failure' THEN 1 ELSE 0 END) AS failed_jobs,
  round(100.0 * sum(CASE WHEN jobs.finish_status='success' THEN 1 ELSE 0 END) / nullif(count(jobs.background_job_id),0), 2) AS job_success_rate_pct,
  sum(CASE WHEN tasks.is_overdue_proxy THEN 1 ELSE 0 END) AS overdue_tasks,
  sum(CASE WHEN fresh.has_extract_any=1 AND fresh.days_since_extract_activity > p.stale_days THEN 1 ELSE 0 END) AS stale_extract_items
FROM sess
LEFT JOIN nav_sess ON nav_sess.site_id=sess.site_id AND nav_sess.session_id=sess.session_id
CROSS JOIN (SELECT * FROM p) p
CROSS JOIN jobs
CROSS JOIN tasks
CROSS JOIN fresh
LIMIT 1;

-- DATASET: DS_DAILY_ADOPTION_TREND (Visual: line; day vs dau/sessions/error%)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_id,
    coalesce(nullif(:p_user_email,''), 'All') AS user_email,
    coalesce(nullif(:p_device,''), 'All')     AS device,
    coalesce(nullif(:p_browser,''), 'All')    AS browser
)
SELECT
  cast(session_start_at as date) AS day,
  count(distinct user_email) AS dau,
  count(distinct session_id) AS sessions,
  round(100.0 * sum(http_error_count) / nullif(sum(http_request_count),0), 2) AS http_error_rate_pct
FROM cdo_restricted.tableau_fct_session_summary
CROSS JOIN p
WHERE cast(session_start_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.user_email='All' OR user_email=p.user_email)
  AND (p.device='All' OR client_device_family=p.device)
  AND (p.browser='All' OR client_browser_family=p.browser)
GROUP BY cast(session_start_at as date)
ORDER BY day;

-- DATASET: DS_TOP_WORKBOOKS (Visual: bar; workbook vs unique_users)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''), 'All')    AS site_id,
         coalesce(nullif(:p_project,''), 'All')    AS project,
         coalesce(nullif(:p_workbook,''), 'All')   AS workbook,
         coalesce(nullif(:p_view,''), 'All')       AS view_display,
         coalesce(nullif(:p_user_email,''), 'All') AS user_email
),
base AS (
  SELECT
    project_name,
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    user_email,
    concat_ws(
      ' / ',
      coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
      CASE
        WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1)
        WHEN currentsheet LIKE '%/%' THEN regexp_extract(currentsheet, '^[^/]+/(.+)$', 1)
        ELSE coalesce(view_name, currentsheet, 'Unknown')
      END
    ) AS view_display
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.user_email='All' OR user_email=p.user_email)
    AND (p.project='All' OR project_name=p.project)
    AND (p.workbook='All' OR coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown')=p.workbook)
)
SELECT project_name AS project,
       workbook,
       count(distinct user_email) AS unique_users,
       count(*) AS view_enters
FROM base
CROSS JOIN p
WHERE (p.view_display='All' OR base.view_display=p.view_display)
GROUP BY project_name, workbook
ORDER BY unique_users DESC, view_enters DESC
LIMIT 25;

-- DATASET: DS_TOP_VIEWS (Visual: bar; view_display vs unique_users)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''), 'All')    AS site_id,
         coalesce(nullif(:p_project,''), 'All')    AS project,
         coalesce(nullif(:p_workbook,''), 'All')   AS workbook,
         coalesce(nullif(:p_view,''), 'All')       AS view_display
),
base AS (
  SELECT
    project_name,
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    user_email,
    concat_ws(
      ' / ',
      coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
      CASE
        WHEN view_name LIKE '%/%' THEN regexp_extract(view_name, '^[^/]+/(.+)$', 1)
        WHEN currentsheet LIKE '%/%' THEN regexp_extract(currentsheet, '^[^/]+/(.+)$', 1)
        ELSE coalesce(view_name, currentsheet, 'Unknown')
      END
    ) AS view_display
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.project='All' OR project_name=p.project)
    AND (p.workbook='All' OR coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown')=p.workbook)
)
SELECT project_name AS project,
       workbook,
       view_display AS view,
       count(*) AS view_enters,
       count(distinct user_email) AS unique_users
FROM base
CROSS JOIN p
WHERE (p.view_display='All' OR base.view_display=p.view_display)
GROUP BY project_name, workbook, view_display
ORDER BY unique_users DESC, view_enters DESC
LIMIT 25;

-- DATASET: DS_OVERDUE_FAILING_TASKS (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT
  scheduled_action_name AS action,
  schedule_name,
  task_object_type,
  coalesce(task_workbook_name, task_datasource_name) AS object_name,
  consecutive_failure_count,
  last_success_completed_at,
  days_since_last_success,
  run_next_at,
  is_overdue_proxy
FROM cdo_restricted.tableau_fct_task_health
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (is_overdue_proxy=true OR consecutive_failure_count>0)
ORDER BY is_overdue_proxy DESC, consecutive_failure_count DESC, days_since_last_success DESC
LIMIT 500;

-- DATASET: DS_STALE_EXTRACTS_RISK_LIST (Visual: table)
WITH p AS (
  SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
         coalesce(nullif(:p_project,''),'All') AS project,
         coalesce(:p_stale_days,7) AS stale_days
)
SELECT
  project_name,
  content_type,
  content_name,
  days_since_extract_activity AS days_since_refresh,
  uses_bridge_any,
  has_extract_any
FROM cdo_restricted.tableau_fct_data_freshness
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.project='All' OR project_name=p.project)
  AND has_extract_any=1
  AND days_since_extract_activity > p.stale_days
ORDER BY days_since_extract_activity DESC
LIMIT 1000;

-- DATASET: DS_RECENT_FAILED_JOBS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  coalesce(completed_at, created_at) AS job_time,
  job_type,
  job_title,
  finish_status,
  resolved_schedule_name,
  resolved_scheduled_action_name,
  resolved_object_type,
  coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
  queue_seconds,
  run_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND finish_status='failure'
ORDER BY job_time DESC
LIMIT 300;

/* ---------------- TAB 2: ADOPTION & PORTFOLIO ---------------- */

-- DATASET: DS_CONTENT_GROWTH_TREND (Visual: line; day vs new_workbooks/new_views/new_datasources)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
wb AS (
  SELECT cast(workbook_created_at as date) AS day, count(*) AS new_workbooks
  FROM cdo_restricted.tableau_dim_workbook
  CROSS JOIN p
  WHERE cast(workbook_created_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY cast(workbook_created_at as date)
),
vw AS (
  SELECT cast(view_created_at as date) AS day, count(*) AS new_views
  FROM cdo_restricted.tableau_dim_view
  CROSS JOIN p
  WHERE cast(view_created_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY cast(view_created_at as date)
),
ds AS (
  SELECT cast(datasource_created_at as date) AS day, count(*) AS new_datasources
  FROM cdo_restricted.tableau_dim_datasource
  CROSS JOIN p
  WHERE cast(datasource_created_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY cast(datasource_created_at as date)
)
SELECT coalesce(wb.day, vw.day, ds.day) AS day,
       coalesce(new_workbooks,0) AS new_workbooks,
       coalesce(new_views,0) AS new_views,
       coalesce(new_datasources,0) AS new_datasources
FROM wb
FULL OUTER JOIN vw ON vw.day=wb.day
FULL OUTER JOIN ds ON ds.day=coalesce(wb.day, vw.day)
ORDER BY day;

-- DATASET: DS_STALE_UNUSED_VIEWS (Visual: table)
WITH p AS (
  SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
         coalesce(nullif(:p_project,''),'All') AS project,
         coalesce(nullif(:p_workbook,''),'All') AS workbook,
         coalesce(:p_stale_days,7) AS stale_days
),
last_metrics AS (
  SELECT site_id, view_id, max(metric_date) AS last_view_date
  FROM cdo_restricted.tableau_fct_view_metrics_daily
  CROSS JOIN p
  WHERE view_count>0
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY site_id, view_id
),
last_stats AS (
  SELECT site_id, view_id, max(last_viewed_at) AS last_viewed_at_any_user
  FROM cdo_restricted.tableau_fct_views_stats_enriched
  CROSS JOIN p
  WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY site_id, view_id
),
base AS (
  SELECT
    dv.project_name,
    dv.workbook_name,
    concat_ws(' / ', dv.workbook_name, dv.view_name) AS view_display,
    greatest(
      coalesce(cast(lm.last_view_date as timestamp), timestamp('1900-01-01')),
      coalesce(ls.last_viewed_at_any_user,           timestamp('1900-01-01'))
    ) AS last_access_at
  FROM cdo_restricted.tableau_dim_view dv
  LEFT JOIN last_metrics lm ON lm.site_id=dv.site_id AND lm.view_id=dv.view_id
  LEFT JOIN last_stats   ls ON ls.site_id=dv.site_id AND ls.view_id=dv.view_id
  CROSS JOIN p
  WHERE (p.site_id='All' OR cast(dv.site_id as string)=p.site_id)
    AND (p.project='All' OR dv.project_name=p.project)
    AND (p.workbook='All' OR dv.workbook_name=p.workbook)
)
SELECT
  project_name,
  workbook_name,
  view_display,
  last_access_at,
  datediff(current_date(), cast(last_access_at as date)) AS days_since_last_access
FROM base
CROSS JOIN p
WHERE datediff(current_date(), cast(last_access_at as date)) >= p.stale_days
ORDER BY days_since_last_access DESC
LIMIT 1000;

-- DATASET: DS_BUS_FACTOR_OWNERSHIP (Visual: bar/table)
WITH p AS (
  SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
         coalesce(nullif(:p_project,''),'All') AS project
),
wb AS (
  SELECT workbook_owner_email AS owner_email, count(*) AS items_owned
  FROM cdo_restricted.tableau_dim_workbook
  CROSS JOIN p
  WHERE is_deleted=false
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND (p.project='All' OR project_name=p.project)
  GROUP BY workbook_owner_email
),
tot AS (SELECT sum(items_owned) AS total_items FROM wb)
SELECT
  owner_email,
  items_owned,
  round(100.0 * items_owned / nullif((SELECT total_items FROM tot),0), 2) AS pct_of_workbooks
FROM wb
ORDER BY items_owned DESC
LIMIT 50;

-- DATASET: DS_ACTIVE_USERS_BY_GROUP (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
base AS (
  SELECT gm.group_name, ss.user_email, ss.session_id
  FROM cdo_restricted.tableau_fct_session_summary ss
  JOIN cdo_restricted.tableau_dim_group_membership gm
    ON gm.site_id=ss.site_id AND gm.user_email=ss.user_email
  CROSS JOIN p
  WHERE cast(ss.session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(ss.site_id as string)=p.site_id)
)
SELECT
  group_name,
  count(distinct user_email) AS active_users,
  count(distinct session_id) AS sessions
FROM base
GROUP BY group_name
ORDER BY active_users DESC
LIMIT 100;

/* ---------------- TAB 3: ENGAGEMENT & NAVIGATION ---------------- */

-- DATASET: DS_LANDING_VIEWS (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
ranked AS (
  SELECT
    site_id, session_id, user_email,
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    concat_ws(
      ' / ',
      coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
      CASE WHEN currentsheet LIKE '%/%' THEN regexp_extract(currentsheet,'^[^/]+/(.+)$',1) ELSE coalesce(view_name,currentsheet,'Unknown') END
    ) AS view_display,
    view_entered_at,
    row_number() OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS rn
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
)
SELECT
  workbook,
  view_display AS landing_view,
  count(*) AS sessions_landed,
  count(distinct user_email) AS unique_users
FROM ranked
WHERE rn=1
GROUP BY workbook, view_display
ORDER BY sessions_landed DESC
LIMIT 50;

-- DATASET: DS_TOP_TRANSITIONS (Visual: table / sankey-ready)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
nav AS (
  SELECT
    site_id, session_id,
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    concat_ws(
      ' / ',
      coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
      CASE WHEN currentsheet LIKE '%/%' THEN regexp_extract(currentsheet,'^[^/]+/(.+)$',1) ELSE coalesce(view_name,currentsheet,'Unknown') END
    ) AS view_display,
    view_entered_at
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
),
paths AS (
  SELECT workbook,
         view_display AS from_view,
         lead(view_display) OVER (PARTITION BY site_id, session_id ORDER BY view_entered_at) AS to_view
  FROM nav
)
SELECT workbook, from_view, to_view, count(*) AS transitions
FROM paths
WHERE to_view IS NOT NULL
GROUP BY workbook, from_view, to_view
ORDER BY transitions DESC
LIMIT 200;

-- DATASET: DS_BOUNCE_RATE_BY_WORKBOOK (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
per_sess AS (
  SELECT
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
    session_id,
    count(distinct view_key) AS distinct_views
  FROM cdo_restricted.tableau_fct_view_navigation_events
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'), session_id
)
SELECT
  workbook,
  count(*) AS sessions,
  round(100.0 * sum(CASE WHEN distinct_views=1 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS bounce_rate_pct
FROM per_sess
GROUP BY workbook
ORDER BY bounce_rate_pct DESC
LIMIT 50;

-- DATASET: DS_RETURNING_USERS (Visual: tile)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
per_user AS (
  SELECT user_email, count(distinct session_id) AS sessions
  FROM cdo_restricted.tableau_fct_session_summary
  CROSS JOIN p
  WHERE cast(session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
    AND user_email IS NOT NULL
  GROUP BY user_email
)
SELECT
  count(*) AS users_in_range,
  sum(CASE WHEN sessions>1 THEN 1 ELSE 0 END) AS returning_users,
  round(100.0 * sum(CASE WHEN sessions>1 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS returning_users_pct
FROM per_user;

/* ---------------- TAB 4: TIME SPENT ---------------- */

-- DATASET: DS_DWELL_BY_VIEW (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  project_name,
  workbook_name,
  concat_ws(' / ', workbook_name, view_name) AS view_display,
  round(sum(dwell_seconds_capped)/3600.0, 2) AS total_dwell_hours,
  count(distinct user_email) AS unique_users,
  count(*) AS visits
FROM cdo_restricted.tableau_fct_view_dwell
CROSS JOIN p
WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY project_name, workbook_name, concat_ws(' / ', workbook_name, view_name)
ORDER BY total_dwell_hours DESC
LIMIT 50;

-- DATASET: DS_DWELL_BY_WORKBOOK (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  project_name,
  workbook_name,
  round(sum(workbook_dwell_seconds_capped)/3600.0, 2) AS total_dwell_hours,
  count(distinct user_email) AS unique_users,
  count(*) AS workbook_visits
FROM cdo_restricted.tableau_fct_workbook_dwell
CROSS JOIN p
WHERE cast(workbook_entered_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY project_name, workbook_name
ORDER BY total_dwell_hours DESC
LIMIT 50;

-- DATASET: DS_SESSION_TIMELINE_DRILLDOWN (Visual: table; uses p_session_id)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id,
         coalesce(nullif(:p_session_id,''),'All')  AS session_id
)
SELECT
  session_id,
  user_email,
  project_name,
  workbook_name,
  view_name,
  view_entered_at,
  view_exited_at,
  dwell_seconds_capped,
  view_load_time_ms_proxy
FROM cdo_restricted.tableau_fct_view_dwell
CROSS JOIN p
WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.session_id='All' OR session_id=p.session_id)
ORDER BY view_entered_at DESC
LIMIT 2000;

-- DATASET: DS_TOOLTIP_USAGE (Visual: line/table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  cast(request_created_at as date) AS day,
  coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
  coalesce(view_name, currentsheet, 'Unknown') AS view,
  count(*) AS tooltip_events
FROM cdo_restricted.tableau_fct_http_requests_enriched
CROSS JOIN p
WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND action LIKE '%render-tooltip-server%'
GROUP BY cast(request_created_at as date),
         coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
         coalesce(view_name, currentsheet, 'Unknown')
ORDER BY day DESC
LIMIT 5000;

-- DATASET: DS_CONFUSION_CANDIDATES (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
),
dwell AS (
  SELECT workbook_name, view_name,
         sum(dwell_seconds_capped) AS dwell_seconds,
         count(*) AS visits
  FROM cdo_restricted.tableau_fct_view_dwell
  CROSS JOIN p
  WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY workbook_name, view_name
),
errs AS (
  SELECT
    coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook_name,
    coalesce(view_name, currentsheet, 'Unknown') AS view_name,
    count(*) AS requests,
    sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors
  FROM cdo_restricted.tableau_fct_http_requests_enriched
  CROSS JOIN p
  WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
    AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  GROUP BY coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
           coalesce(view_name, currentsheet, 'Unknown')
)
SELECT
  d.workbook_name,
  d.view_name,
  round(d.dwell_seconds/3600.0,2) AS dwell_hours,
  d.visits,
  e.requests,
  e.errors,
  round(100.0 * e.errors / nullif(e.requests,0), 2) AS error_rate_pct
FROM dwell d
LEFT JOIN errs e ON e.workbook_name=d.workbook_name AND e.view_name=d.view_name
WHERE d.visits >= 25
ORDER BY dwell_hours DESC, error_rate_pct DESC
LIMIT 200;

/* ---------------- TAB 5: PERFORMANCE ---------------- */

-- DATASET: DS_SLOW_VIEWS_P95 (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  coalesce(project_name,'Unknown') AS project,
  coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
  coalesce(view_name, currentsheet, 'Unknown') AS view,
  percentile_approx(view_load_time_ms_proxy, 0.95) AS p95_load_ms_proxy,
  percentile_approx(view_load_time_ms_proxy, 0.50) AS p50_load_ms_proxy,
  count(*) AS view_enters
FROM cdo_restricted.tableau_fct_view_navigation_events
CROSS JOIN p
WHERE cast(view_entered_at as date) BETWEEN p.dmin AND p.dmax
  AND view_load_time_ms_proxy IS NOT NULL
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY coalesce(project_name,'Unknown'),
         coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
         coalesce(view_name, currentsheet, 'Unknown')
HAVING count(*) >= 25
ORDER BY p95_load_ms_proxy DESC
LIMIT 50;

-- DATASET: DS_ERROR_HOTSPOTS_BY_VIEW (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT
  coalesce(project_name,'Unknown') AS project,
  coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown') AS workbook,
  coalesce(view_name, currentsheet, 'Unknown') AS view,
  count(*) AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS errors_5xx,
  round(100.0 * sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS error_rate_pct
FROM cdo_restricted.tableau_fct_http_requests_enriched
CROSS JOIN p
WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY coalesce(project_name,'Unknown'),
         coalesce(workbook_name, split_part(currentsheet,'/',1), 'Unknown'),
         coalesce(view_name, currentsheet, 'Unknown')
HAVING count(*) >= 100
ORDER BY error_rate_pct DESC
LIMIT 200;

-- DATASET: DS_ERROR_BY_CONTROLLER_ACTION (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT controller, action,
       count(*) AS requests,
       sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors,
       sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END) AS errors_5xx
FROM cdo_restricted.tableau_fct_http_requests_enriched
CROSS JOIN p
WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY controller, action
ORDER BY errors_5xx DESC, errors DESC
LIMIT 200;

-- DATASET: DS_CLIENT_SEGMENTATION (Visual: stacked bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT client_device_family AS device,
       client_browser_family AS browser,
       count(*) AS requests,
       sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors
FROM cdo_restricted.tableau_fct_http_requests_enriched
CROSS JOIN p
WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY client_device_family, client_browser_family
ORDER BY requests DESC
LIMIT 200;

-- DATASET: DS_TOP_CLIENT_TOKENS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT client_ip_scope,
       client_ip_token,
       count(*) AS requests,
       sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS errors
FROM cdo_restricted.tableau_fct_http_requests_enriched
CROSS JOIN p
WHERE cast(request_created_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND client_ip_token IS NOT NULL
GROUP BY client_ip_scope, client_ip_token
ORDER BY requests DESC
LIMIT 200;

/* ---------------- TAB 6: RELIABILITY (JOBS/SCHEDULES) ---------------- */

-- DATASET: DS_JOB_SUCCESS_TREND (Visual: line)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT cast(coalesce(completed_at, created_at) as date) AS day,
       job_type,
       count(*) AS jobs,
       sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) AS failures,
       round(100.0 * (1 - sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END)/nullif(count(*),0)), 2) AS success_rate_pct,
       round(avg(queue_seconds),1) AS avg_queue_sec,
       round(avg(run_seconds),1) AS avg_run_sec
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY cast(coalesce(completed_at, created_at) as date), job_type
ORDER BY day, job_type;

-- DATASET: DS_JOBS_BY_SCHEDULE_FREQ (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT resolved_schedule_name AS schedule,
       resolved_scheduled_action_name AS action_type,
       count(*) AS jobs
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND resolved_schedule_name IS NOT NULL
GROUP BY resolved_schedule_name, resolved_scheduled_action_name
ORDER BY jobs DESC
LIMIT 200;

-- DATASET: DS_LONGEST_JOBS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT coalesce(completed_at, created_at) AS job_time,
       job_type,
       finish_status,
       resolved_schedule_name,
       coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
       queue_seconds,
       run_seconds,
       total_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY run_seconds DESC NULLS LAST
LIMIT 200;

-- DATASET: DS_QUEUE_TIME_HOTSPOTS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT coalesce(completed_at, created_at) AS job_time,
       job_type,
       finish_status,
       resolved_schedule_name,
       coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
       queue_seconds,
       run_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY queue_seconds DESC NULLS LAST
LIMIT 200;

-- DATASET: DS_REFRESH_SUCCESS_NO_WRITE_PROXY (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT completed_at,
       job_type,
       resolved_scheduled_action_name AS action_type,
       coalesce(resolved_workbook_name, resolved_datasource_name) AS object_name,
       finish_status,
       extract_timestamp_updated_proxy,
       extract_dir_updated_proxy
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND finish_status='success'
  AND (extract_timestamp_updated_proxy=false AND extract_dir_updated_proxy=false)
ORDER BY completed_at DESC
LIMIT 300;

-- DATASET: DS_MOST_RECENT_REFRESH_ALERTS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT refresh_occurred_at,
       refresh_status,
       duration_in_ms,
       workbook_name,
       datasource_name,
       details
FROM cdo_restricted.tableau_fct_extract_refresh_most_recent
CROSS JOIN p
WHERE cast(refresh_occurred_at as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY refresh_occurred_at DESC
LIMIT 300;

-- DATASET: DS_TASK_HEALTH_ACTION_LIST (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT scheduled_action_name,
       schedule_name,
       task_object_type,
       coalesce(task_workbook_name, task_datasource_name) AS object_name,
       consecutive_failure_count,
       last_success_completed_at,
       days_since_last_success,
       run_next_at,
       is_overdue_proxy
FROM cdo_restricted.tableau_fct_task_health
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (is_overdue_proxy=true OR consecutive_failure_count>0)
ORDER BY is_overdue_proxy DESC, consecutive_failure_count DESC
LIMIT 500;

/* ---------------- TAB 7: FRESHNESS ---------------- */

-- DATASET: DS_STALE_EXTRACTS (Visual: table/heatmap)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
                  coalesce(nullif(:p_project,''),'All') AS project)
SELECT project_name,
       content_type,
       content_name,
       days_since_extract_activity,
       uses_bridge_any
FROM cdo_restricted.tableau_fct_data_freshness
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.project='All' OR project_name=p.project)
  AND has_extract_any=1
ORDER BY days_since_extract_activity DESC
LIMIT 2000;

-- DATASET: DS_SLA_BUCKETS (Visual: stacked bar)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
                  coalesce(nullif(:p_project,''),'All') AS project)
SELECT project_name,
       sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity <= 1 THEN 1 ELSE 0 END) AS refreshed_1d,
       sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity BETWEEN 2 AND 2 THEN 1 ELSE 0 END) AS refreshed_2d,
       sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity BETWEEN 3 AND 7 THEN 1 ELSE 0 END) AS refreshed_3_7d,
       sum(CASE WHEN has_extract_any=1 AND days_since_extract_activity > 7 THEN 1 ELSE 0 END) AS stale_over_7d,
       sum(CASE WHEN has_extract_any=0 THEN 1 ELSE 0 END) AS live_content
FROM cdo_restricted.tableau_fct_data_freshness
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.project='All' OR project_name=p.project)
GROUP BY project_name
ORDER BY stale_over_7d DESC;

-- DATASET: DS_LIVE_VS_EXTRACT_BY_PROJECT (Visual: stacked bar)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
                  coalesce(nullif(:p_project,''),'All') AS project)
SELECT project_name,
       sum(CASE WHEN has_extract_any=1 THEN 1 ELSE 0 END) AS extract_content_count,
       sum(CASE WHEN has_extract_any=0 THEN 1 ELSE 0 END) AS live_content_count,
       sum(CASE WHEN uses_bridge_any=1 THEN 1 ELSE 0 END) AS bridge_content_count
FROM cdo_restricted.tableau_fct_data_freshness
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.project='All' OR project_name=p.project)
GROUP BY project_name
ORDER BY live_content_count DESC;

-- DATASET: DS_BRIDGE_USAGE_INVENTORY (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT project_name,
       content_type,
       content_name,
       has_extract_any,
       uses_bridge_any,
       connection_count,
       dbclass_set
FROM cdo_restricted.tableau_fct_content_connection_summary
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND uses_bridge_any=1
ORDER BY connection_count DESC
LIMIT 1000;

/* ---------------- TAB 8: SUBSCRIPTIONS ---------------- */

-- DATASET: DS_SUBSCRIPTION_HEALTH (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT subscription_subject,
       recipient_email,
       schedule_name,
       last_completed_at,
       last_finish_status,
       last_run_seconds
FROM cdo_restricted.tableau_fct_subscription_health
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY last_completed_at DESC NULLS LAST
LIMIT 2000;

-- DATASET: DS_RECENT_SUBSCRIPTION_FAILURES (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT completed_at,
       subscription_subject,
       subscription_recipient_email AS recipient,
       finish_status,
       run_seconds,
       queue_seconds
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND subscription_id IS NOT NULL
  AND finish_status='failure'
ORDER BY completed_at DESC
LIMIT 300;

-- DATASET: DS_TOP_SUBSCRIPTION_RECIPIENTS (Visual: bar/table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT subscription_recipient_email AS recipient,
       count(*) AS subscription_jobs,
       sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) AS failures
FROM cdo_restricted.tableau_fct_background_jobs_enriched
CROSS JOIN p
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND subscription_id IS NOT NULL
GROUP BY subscription_recipient_email
ORDER BY subscription_jobs DESC
LIMIT 100;

/* ---------------- TAB 9: GOVERNANCE & AUDIT ---------------- */

-- DATASET: DS_AUDIT_EVENTS_BY_DAY (Visual: stacked bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax
)
SELECT cast(event_created_at as date) AS day,
       action_type,
       count(*) AS events
FROM cdo_restricted.tableau_fct_historical_events_enriched
CROSS JOIN p
WHERE cast(event_created_at as date) BETWEEN p.dmin AND p.dmax
GROUP BY cast(event_created_at as date), action_type
ORDER BY day, action_type;

-- DATASET: DS_RECENT_AUDIT_EVENTS (Visual: table)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax
)
SELECT event_created_at,
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
CROSS JOIN p
WHERE cast(event_created_at as date) BETWEEN p.dmin AND p.dmax
ORDER BY event_created_at DESC
LIMIT 2000;

-- DATASET: DS_TOP_ACTORS (Visual: bar)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax
)
SELECT actor_email, actor_name, count(*) AS events
FROM cdo_restricted.tableau_fct_historical_events_enriched
CROSS JOIN p
WHERE cast(event_created_at as date) BETWEEN p.dmin AND p.dmax
  AND actor_email IS NOT NULL
GROUP BY actor_email, actor_name
ORDER BY events DESC
LIMIT 100;

-- DATASET: DS_GROUP_MEMBERSHIP_COUNTS (Visual: bar)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
                  coalesce(nullif(:p_group,''),'All') AS grp)
SELECT group_name,
       count(distinct user_email) AS members
FROM cdo_restricted.tableau_dim_group_membership
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
  AND (p.grp='All' OR group_name=p.grp)
GROUP BY group_name
ORDER BY members DESC
LIMIT 500;

-- DATASET: DS_GROUP_MEMBERSHIP_DETAIL (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id,
                  coalesce(nullif(:p_group,''),'All') AS grp)
SELECT group_name,
       user_email,
       user_name,
       server_admin_level
FROM cdo_restricted.tableau_dim_group_membership
CROSS JOIN p
WHERE p.grp <> 'All'
  AND group_name=p.grp
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY user_email
LIMIT 10000;

-- DATASET: DS_TAG_COVERAGE (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT taggable_type AS content_type,
       tag_name,
       count(*) AS taggings
FROM cdo_restricted.tableau_dim_content_tags
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
GROUP BY taggable_type, tag_name
ORDER BY taggings DESC
LIMIT 1000;

-- DATASET: DS_DATA_QUALITY_INDICATORS (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT content_type,
       content_id,
       data_quality_type,
       is_active,
       is_severe,
       message,
       user_email AS set_by,
       created_at,
       updated_at
FROM cdo_restricted.tableau_fct_data_quality_indicators
CROSS JOIN p
WHERE (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY updated_at DESC
LIMIT 2000;

-- DATASET: DS_CERTIFIED_DATASOURCES (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT project_name,
       datasource_name,
       datasource_owner_email AS owner_email,
       certifier_user_id,
       certification_note,
       extracts_refreshed_at,
       extracts_incremented_at
FROM cdo_restricted.tableau_dim_datasource
CROSS JOIN p
WHERE is_certified=true
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY project_name, datasource_name
LIMIT 2000;

/* ---------------- TAB 10: CAPACITY & LICENSE ---------------- */

-- DATASET: DS_DISK_USAGE_TREND (Visual: line)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax
)
SELECT cast(record_timestamp as date) AS day,
       worker_id,
       path,
       total_space_bytes,
       used_space_bytes,
       round(100.0 * used_space_bytes / nullif(total_space_bytes,0), 2) AS pct_used,
       state
FROM cdo_restricted.tableau_fct_disk_usage
CROSS JOIN p
WHERE cast(record_timestamp as date) BETWEEN p.dmin AND p.dmax
ORDER BY day DESC
LIMIT 10000;

-- DATASET: DS_STORAGE_BY_PROJECT (Visual: bar)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT project_name,
       sum(coalesce(workbook_size_bytes,0)) AS total_workbook_bytes,
       sum(coalesce(datasource_size_bytes,0)) AS total_datasource_bytes,
       sum(coalesce(workbook_size_bytes,0)) + sum(coalesce(datasource_size_bytes,0)) AS total_content_bytes
FROM (
  SELECT site_id, project_name, workbook_size_bytes, cast(null as bigint) AS datasource_size_bytes
  FROM cdo_restricted.tableau_dim_workbook
  WHERE is_deleted=false
  UNION ALL
  SELECT site_id, project_name, cast(null as bigint), datasource_size_bytes
  FROM cdo_restricted.tableau_dim_datasource
) x
CROSS JOIN p
WHERE (p.site_id='All' OR cast(x.site_id as string)=p.site_id)
GROUP BY project_name
ORDER BY total_content_bytes DESC
LIMIT 500;

-- DATASET: DS_LARGEST_CONTENT (Visual: table)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''),'All') AS site_id)
SELECT * FROM (
  SELECT 'Workbook' AS content_type, project_name, workbook_name AS content_name,
         workbook_owner_email AS owner_email, workbook_size_bytes AS size_bytes, site_id
  FROM cdo_restricted.tableau_dim_workbook
  WHERE is_deleted=false AND workbook_size_bytes IS NOT NULL
  UNION ALL
  SELECT 'Datasource', project_name, datasource_name,
         datasource_owner_email, datasource_size_bytes, site_id
  FROM cdo_restricted.tableau_dim_datasource
  WHERE datasource_size_bytes IS NOT NULL
) t
CROSS JOIN p
WHERE (p.site_id='All' OR cast(t.site_id as string)=p.site_id)
ORDER BY size_bytes DESC
LIMIT 500;

-- DATASET: DS_LICENSE_USAGE (Visual: table; only if enabled)
WITH p AS (
  SELECT coalesce(:p_date.min, date('2025-01-01')) AS dmin,
         coalesce(:p_date.max, current_date())     AS dmax,
         coalesce(nullif(:p_site_id,''),'All')     AS site_id
)
SELECT cast(date_last_used as date) AS last_used_day,
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
CROSS JOIN p
WHERE cast(date_last_used as date) BETWEEN p.dmin AND p.dmax
  AND (p.site_id='All' OR cast(site_id as string)=p.site_id)
ORDER BY date_last_used DESC
LIMIT 5000;

/* ---------------- TAB 11: RETENTION & ARCHIVING ---------------- */

-- DATASET: DS_RETENTION_WINDOWS_ACTUAL (Visual: table)
-- Shows your *actual* retention right now (min/max timestamps) so you know which tables have short windows.
SELECT 'http_requests' AS table_name,
       min(cast(created_at as timestamp)) AS min_ts,
       max(cast(created_at as timestamp)) AS max_ts
FROM ec_tableau_meta.http_requests
UNION ALL
SELECT 'historical_events',
       min(cast(created_at as timestamp)),
       max(cast(created_at as timestamp))
FROM ec_tableau_meta.historical_events
UNION ALL
SELECT 'background_jobs',
       min(cast(coalesce(completed_at, created_at) as timestamp)),
       max(cast(coalesce(completed_at, created_at) as timestamp))
FROM ec_tableau_meta.background_jobs
UNION ALL
SELECT 'most_recent_refreshes',
       min(cast(created_at as timestamp)),
       max(cast(created_at as timestamp))
FROM ec_tableau_meta.most_recent_refreshes;

-- DATASET: DS_RETENTION_GUIDE (Visual: table; static)
SELECT 'http_requests' AS source_table,
       'Often short retention (commonly ~days). Must archive for 13+ months.' AS why,
       'Archive every 5–15 minutes (or at least daily) to Delta history.' AS cadence
UNION ALL
SELECT 'historical_events',
       'Often ~months retention. Must archive for a full year of audit.',
       'Archive daily (append).'
UNION ALL
SELECT 'background_jobs',
       'Job history retention varies and is often limited. Must archive for year trends.',
       'Archive hourly or daily.'
UNION ALL
SELECT 'most_recent_refreshes',
       'Not history; it’s “most recent only.” Must snapshot to build history.',
       'Snapshot daily to a history table.';

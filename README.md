-- =====================================================================================
-- DATBRICKS DASHBOARD QUERY PACK: "Tableau Admin Observability"
-- Source views (curated): cdo_restricted.*
--
-- Global Dashboard Filters (create these in Dashboard UI as text/date parameters):
--   p_start_date   (text/date) default: 2025-01-01
--   p_end_date     (text/date) default: 2025-12-31 (or today)
--   p_site_id      (text)      default: ALL
--   p_project      (text)      default: ALL
--   p_workbook     (text)      default: ALL
--   p_view         (text)      default: ALL
--   p_user_email   (text)      default: ALL
--   p_group        (text)      default: ALL
--   p_device       (text)      default: ALL   (desktop/mobile/tablet)
--   p_browser      (text)      default: ALL   (chrome/edge/safari/firefox/other)
--   p_stale_days   (number)    default: 7
--   p_session_id   (text)      default: ALL   (for drilldown tab)
--
-- Parameter pattern used below:
--   WITH p AS (SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
--                     coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date)
--
-- IMPORTANT (retention):
-- If you haven’t implemented archiving, Tableau repo-derived facts may only cover days/weeks.
-- These queries will still work, but show only retained history. Once archived, point these
-- queries to your *_hist / union views.
-- =====================================================================================


-- =====================================================================================
-- TAB 1 — EXECUTIVE OVERVIEW (60-second check)
-- =====================================================================================

-- [VISUAL] KPI tiles (table or split into counters)
WITH p AS (
  SELECT
    coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
    coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
),
sess AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_session_summary
  WHERE cast(session_start_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
    AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
    AND ('{{p_user_email}}' = 'ALL' OR user_email = '{{p_user_email}}')
    AND ('{{p_device}}' = 'ALL' OR client_device_family = '{{p_device}}')
    AND ('{{p_browser}}' = 'ALL' OR client_browser_family = '{{p_browser}}')
),
jobs AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_background_jobs_enriched
  WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
    AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
),
tasks AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_task_health
  WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
),
fresh AS (
  SELECT *
  FROM cdo_restricted.tableau_fct_data_freshness
  WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
)
SELECT
  -- Adoption
  count(distinct user_email)                                              AS active_users,
  count(distinct session_id)                                              AS sessions,
  sum(http_request_count)                                                 AS http_requests,
  round(100.0 * (sum(http_error_count) / nullif(sum(http_request_count),0)), 2) AS http_error_rate_pct,

  -- Reliability
  sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END)                AS failed_jobs,
  sum(CASE WHEN finish_status='success' THEN 1 ELSE 0 END)                AS successful_jobs,
  round(100.0 * (sum(CASE WHEN finish_status='success' THEN 1 ELSE 0 END) / nullif(count(*),0)), 2) AS job_success_rate_pct,

  -- Schedule health (proxy)
  sum(CASE WHEN is_overdue_proxy THEN 1 ELSE 0 END)                       AS overdue_tasks,

  -- Data freshness
  sum(CASE WHEN has_extract_any = 1 AND days_since_extract_activity > {{p_stale_days}} THEN 1 ELSE 0 END) AS stale_extract_items
FROM sess
CROSS JOIN (SELECT * FROM jobs) j
CROSS JOIN (SELECT * FROM tasks) t
CROSS JOIN (SELECT * FROM fresh) f
LIMIT 1;


-- [VISUAL] Line chart: DAU + Sessions + Error Rate over time (daily)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  cast(session_start_at as date)                                 AS day,
  count(distinct user_email)                                     AS dau,
  count(distinct session_id)                                     AS sessions,
  round(100.0 * (sum(http_error_count) / nullif(sum(http_request_count),0)), 2) AS http_error_rate_pct
FROM cdo_restricted.tableau_fct_session_summary
WHERE cast(session_start_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_user_email}}' = 'ALL' OR user_email = '{{p_user_email}}')
  AND ('{{p_device}}' = 'ALL' OR client_device_family = '{{p_device}}')
  AND ('{{p_browser}}' = 'ALL' OR client_browser_family = '{{p_browser}}')
GROUP BY cast(session_start_at as date)
ORDER BY day;


-- [VISUAL] Bar chart: Top Workbooks by Unique Users
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  workbook_name                                                   AS workbook,
  project_name                                                    AS project,
  count(distinct user_email)                                      AS unique_users,
  count(*)                                                       AS view_enters
FROM cdo_restricted.tableau_fct_view_navigation_events
WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
  AND ('{{p_user_email}}' = 'ALL' OR user_email = '{{p_user_email}}')
GROUP BY workbook_name, project_name
ORDER BY unique_users DESC, view_enters DESC
LIMIT 25;


-- [VISUAL] Table: Overdue / failing tasks (action list)
SELECT
  scheduled_action_name                                            AS action,
  schedule_name                                                    AS schedule,
  task_object_type                                                 AS object_type,
  coalesce(task_workbook_name, task_datasource_name)                AS object_name,
  consecutive_failure_count                                         AS consecutive_failures,
  last_success_completed_at                                         AS last_success,
  days_since_last_success                                           AS days_since_success,
  run_next_at                                                      AS next_run,
  is_overdue_proxy                                                 AS overdue_proxy
FROM cdo_restricted.tableau_fct_task_health
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND (is_overdue_proxy = true OR consecutive_failure_count > 0)
ORDER BY is_overdue_proxy DESC, consecutive_failure_count DESC, days_since_last_success DESC
LIMIT 200;


-- [VISUAL] Table/Bar: Stale extracts by project (risk list)
SELECT
  project_name                                                     AS project,
  content_type                                                     AS content_type,
  content_name                                                     AS content_name,
  days_since_extract_activity                                      AS days_since_refresh,
  uses_bridge_any                                                  AS uses_bridge,
  has_extract_any                                                  AS has_extract
FROM cdo_restricted.tableau_fct_data_freshness
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND has_extract_any = 1
  AND days_since_extract_activity > {{p_stale_days}}
ORDER BY days_since_extract_activity DESC, project_name, content_name
LIMIT 500;


-- =====================================================================================
-- TAB 2 — ADOPTION & CONTENT PORTFOLIO
-- =====================================================================================

-- [VISUAL] Bar chart: Top Views by View Enters
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  project_name                                                    AS project,
  workbook_name                                                   AS workbook,
  view_name                                                       AS view,
  count(*)                                                        AS view_enters,
  count(distinct user_email)                                      AS unique_users
FROM cdo_restricted.tableau_fct_view_navigation_events
WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
GROUP BY project_name, workbook_name, view_name
ORDER BY unique_users DESC, view_enters DESC
LIMIT 50;


-- [VISUAL] Table: Stale/Unused Views (last access computed from metrics + stats)
WITH last_from_metrics AS (
  SELECT
    site_id, view_id, max(metric_date) AS last_view_date
  FROM cdo_restricted.tableau_fct_view_metrics_daily
  WHERE view_count > 0
  GROUP BY site_id, view_id
),
last_from_user_stats AS (
  SELECT
    site_id, view_id, max(last_viewed_at) AS last_viewed_at_any_user
  FROM cdo_restricted.tableau_fct_views_stats_enriched
  GROUP BY site_id, view_id
)
SELECT
  dv.project_name                                                 AS project,
  dv.workbook_name                                                AS workbook,
  dv.view_name                                                    AS view,
  greatest(
    coalesce(cast(lm.last_view_date as timestamp), timestamp('1900-01-01')),
    coalesce(ls.last_viewed_at_any_user,            timestamp('1900-01-01'))
  )                                                               AS last_access_at,
  datediff(current_date(), cast(greatest(
    coalesce(cast(lm.last_view_date as timestamp), timestamp('1900-01-01')),
    coalesce(ls.last_viewed_at_any_user,            timestamp('1900-01-01'))
  ) as date))                                                     AS days_since_last_access
FROM cdo_restricted.tableau_dim_view dv
LEFT JOIN last_from_metrics lm
  ON lm.site_id = dv.site_id AND lm.view_id = dv.view_id
LEFT JOIN last_from_user_stats ls
  ON ls.site_id = dv.site_id AND ls.view_id = dv.view_id
WHERE ('{{p_site_id}}' = 'ALL' OR cast(dv.site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR dv.project_name = '{{p_project}}')
  AND ('{{p_workbook}}' = 'ALL' OR dv.workbook_name = '{{p_workbook}}')
  AND datediff(current_date(), cast(greatest(
        coalesce(cast(lm.last_view_date as timestamp), timestamp('1900-01-01')),
        coalesce(ls.last_viewed_at_any_user,            timestamp('1900-01-01'))
      ) as date)) >= {{p_stale_days}}
ORDER BY days_since_last_access DESC, project, workbook, view
LIMIT 500;


-- [VISUAL] Line chart: New content created over time (workbooks/views/datasources)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
),
wb AS (
  SELECT cast(workbook_created_at as date) AS day, count(*) AS new_workbooks
  FROM cdo_restricted.tableau_dim_workbook
  WHERE cast(workbook_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  GROUP BY cast(workbook_created_at as date)
),
vw AS (
  SELECT cast(view_created_at as date) AS day, count(*) AS new_views
  FROM cdo_restricted.tableau_dim_view
  WHERE cast(view_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  GROUP BY cast(view_created_at as date)
),
ds AS (
  SELECT cast(datasource_created_at as date) AS day, count(*) AS new_datasources
  FROM cdo_restricted.tableau_dim_datasource
  WHERE cast(datasource_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  GROUP BY cast(datasource_created_at as date)
)
SELECT
  coalesce(wb.day, vw.day, ds.day) AS day,
  coalesce(new_workbooks, 0)      AS new_workbooks,
  coalesce(new_views, 0)          AS new_views,
  coalesce(new_datasources, 0)    AS new_datasources
FROM wb
FULL OUTER JOIN vw ON vw.day = wb.day
FULL OUTER JOIN ds ON ds.day = coalesce(wb.day, vw.day)
ORDER BY day;


-- =====================================================================================
-- TAB 3 — ENGAGEMENT & NAVIGATION (FUNNELS)
-- =====================================================================================

-- [VISUAL] Bar chart: Landing Views (first view in session)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
),
first_view AS (
  SELECT
    site_id, session_id,
    min_by(view_name, view_entered_at)       AS landing_view,
    min_by(workbook_name, view_entered_at)   AS landing_workbook,
    min(view_entered_at)                     AS landing_time,
    max(user_email)                          AS user_email
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
    AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  GROUP BY site_id, session_id
)
SELECT
  landing_workbook                                              AS workbook,
  landing_view                                                  AS view,
  count(*)                                                      AS sessions_landed,
  count(distinct user_email)                                    AS unique_users
FROM first_view
WHERE ('{{p_user_email}}' = 'ALL' OR user_email = '{{p_user_email}}')
GROUP BY landing_workbook, landing_view
ORDER BY sessions_landed DESC
LIMIT 50;


-- [VISUAL] Table (or bar): Top transitions (View -> Next View)
-- If you want a Sankey later, this is the dataset.
SELECT
  workbook_name                                                 AS workbook,
  view_name                                                     AS from_view,
  next_view_name                                                AS to_view,
  transition_count                                              AS transitions,
  session_count                                                 AS sessions
FROM cdo_restricted.tableau_fct_engagement_paths
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
ORDER BY transitions DESC
LIMIT 200;


-- [VISUAL] Bar chart: Bounce rate by workbook (sessions with only 1 distinct view)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
),
per_session AS (
  SELECT
    workbook_name,
    session_id,
    count(distinct view_id) AS distinct_views
  FROM cdo_restricted.tableau_fct_view_navigation_events
  WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
    AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  GROUP BY workbook_name, session_id
)
SELECT
  workbook_name                                                 AS workbook,
  count(*)                                                      AS sessions,
  sum(CASE WHEN distinct_views = 1 THEN 1 ELSE 0 END)           AS bounce_sessions,
  round(100.0 * sum(CASE WHEN distinct_views = 1 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS bounce_rate_pct
FROM per_session
GROUP BY workbook_name
ORDER BY bounce_rate_pct DESC, sessions DESC
LIMIT 50;


-- =====================================================================================
-- TAB 4 — TIME SPENT (INFERRED) + DRILLDOWN
-- =====================================================================================

-- [VISUAL] Bar chart: Top Views by total dwell (capped)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  project_name                                                   AS project,
  workbook_name                                                  AS workbook,
  view_name                                                      AS view,
  round(sum(dwell_seconds_capped)/3600.0, 2)                      AS total_dwell_hours,
  count(distinct user_email)                                     AS unique_users,
  count(*)                                                       AS view_visits
FROM cdo_restricted.tableau_fct_view_dwell
WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
GROUP BY project_name, workbook_name, view_name
ORDER BY total_dwell_hours DESC
LIMIT 50;


-- [VISUAL] Histogram: Dwell distribution (seconds, capped)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
),
binned AS (
  SELECT
    floor(dwell_seconds_capped / 30) * 30                         AS dwell_bin_start_sec
  FROM cdo_restricted.tableau_fct_view_dwell
  WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
    AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
)
SELECT
  dwell_bin_start_sec                                            AS dwell_seconds_bin_start,
  count(*)                                                       AS visits
FROM binned
GROUP BY dwell_bin_start_sec
ORDER BY dwell_bin_start_sec;


-- [VISUAL] Drilldown Table: Session timeline (sequence of views + dwell)
-- Use dashboard filter p_session_id. Default ALL shows latest sessions.
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  session_id,
  user_email                                                     AS user,
  workbook_name                                                  AS workbook,
  view_name                                                      AS view,
  view_entered_at,
  view_exited_at,
  dwell_seconds_capped                                           AS dwell_seconds,
  view_load_time_ms_proxy                                        AS load_ms_proxy
FROM cdo_restricted.tableau_fct_view_dwell
WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_session_id}}' = 'ALL' OR session_id = '{{p_session_id}}')
ORDER BY view_entered_at DESC
LIMIT 1000;


-- =====================================================================================
-- TAB 5 — PERFORMANCE (REPO PROXY) + ERROR HOTSPOTS
-- =====================================================================================

-- [VISUAL] Bar chart: Slowest views by p95 load proxy (ms)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  project_name                                                   AS project,
  workbook_name                                                  AS workbook,
  view_name                                                      AS view,
  percentile_approx(view_load_time_ms_proxy, 0.95)               AS p95_load_ms_proxy,
  percentile_approx(view_load_time_ms_proxy, 0.50)               AS p50_load_ms_proxy,
  count(*)                                                       AS view_enters
FROM cdo_restricted.tableau_fct_view_navigation_events
WHERE cast(view_entered_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND view_load_time_ms_proxy IS NOT NULL
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
GROUP BY project_name, workbook_name, view_name
HAVING count(*) >= 25
ORDER BY p95_load_ms_proxy DESC
LIMIT 50;


-- [VISUAL] Table: HTTP error hotspots by view/workbook
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  project_name                                                   AS project,
  workbook_name                                                  AS workbook,
  view_name                                                      AS view,
  count(*)                                                       AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END)            AS errors,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END)            AS errors_5xx,
  round(100.0 * sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) / nullif(count(*),0), 2) AS error_rate_pct
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE cast(request_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
GROUP BY project_name, workbook_name, view_name
HAVING count(*) >= 100
ORDER BY error_rate_pct DESC, errors_5xx DESC
LIMIT 100;


-- [VISUAL] Table: Controller/Action error breakdown (for triage)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  controller,
  action,
  count(*)                                                       AS requests,
  sum(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END)            AS errors,
  sum(CASE WHEN http_status >= 500 THEN 1 ELSE 0 END)            AS errors_5xx
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE cast(request_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
GROUP BY controller, action
ORDER BY errors_5xx DESC, errors DESC
LIMIT 200;


-- =====================================================================================
-- TAB 6 — RELIABILITY (REFRESH/SUBS/JOBS) + “DID IT WRITE?” PROXIES
-- =====================================================================================

-- [VISUAL] Line chart: Job success rate over time
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  cast(coalesce(completed_at, created_at) as date)                AS day,
  job_type,
  count(*)                                                       AS jobs,
  sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END)        AS failures,
  round(100.0 * (1 - (sum(CASE WHEN finish_status='failure' THEN 1 ELSE 0 END) / nullif(count(*),0))), 2) AS success_rate_pct,
  round(avg(queue_seconds), 1)                                   AS avg_queue_sec,
  round(avg(run_seconds), 1)                                     AS avg_run_sec
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
GROUP BY cast(coalesce(completed_at, created_at) as date), job_type
ORDER BY day, job_type;


-- [VISUAL] Table: Top failing tasks (go fix these)
SELECT
  resolved_scheduled_action_name                                  AS action,
  resolved_schedule_name                                          AS schedule,
  resolved_object_type                                            AS object_type,
  coalesce(resolved_workbook_name, resolved_datasource_name)      AS object_name,
  task_consecutive_failure_count                                  AS consecutive_failures,
  task_last_success_completed_at                                  AS last_success,
  finish_status                                                   AS last_finish_status,
  completed_at                                                    AS last_completed_at,
  run_seconds                                                     AS last_run_sec,
  queue_seconds                                                   AS last_queue_sec
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND finish_status = 'failure'
ORDER BY completed_at DESC
LIMIT 200;


-- [VISUAL] Table: “Refresh succeeded but may not have written new extract data” (proxy)
-- This is the closest we can get repo-only to “did new data actually ingest?”
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  job_type,
  resolved_scheduled_action_name                                  AS action,
  coalesce(resolved_workbook_name, resolved_datasource_name)      AS object_name,
  completed_at,
  finish_status,
  extract_timestamp_updated_proxy,
  extract_dir_updated_proxy
FROM cdo_restricted.tableau_fct_background_jobs_enriched
WHERE cast(coalesce(completed_at, created_at) as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND finish_status = 'success'
  AND (extract_timestamp_updated_proxy = false AND extract_dir_updated_proxy = false)
ORDER BY completed_at DESC
LIMIT 200;


-- =====================================================================================
-- TAB 7 — DATA FRESHNESS + LIVE VS EXTRACT + BRIDGE
-- =====================================================================================

-- [VISUAL] Heatmap/Table: Stale extracts by project + content
SELECT
  project_name                                                    AS project,
  content_type,
  content_name,
  days_since_extract_activity                                     AS days_since_refresh,
  uses_bridge_any                                                 AS uses_bridge
FROM cdo_restricted.tableau_fct_data_freshness
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND has_extract_any = 1
ORDER BY days_since_extract_activity DESC
LIMIT 500;


-- [VISUAL] Bar chart: Live vs Extract inventory by project (count)
SELECT
  project_name                                                    AS project,
  sum(CASE WHEN has_extract_any = 1 THEN 1 ELSE 0 END)            AS extract_content_count,
  sum(CASE WHEN has_extract_any = 0 THEN 1 ELSE 0 END)            AS live_content_count,
  sum(CASE WHEN uses_bridge_any = 1 THEN 1 ELSE 0 END)            AS bridge_content_count
FROM cdo_restricted.tableau_fct_data_freshness
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
GROUP BY project_name
ORDER BY live_content_count DESC, extract_content_count DESC;


-- =====================================================================================
-- TAB 8 — SUBSCRIPTIONS
-- =====================================================================================

-- [VISUAL] Table: Subscription delivery health
SELECT
  subscription_subject                                            AS subscription,
  recipient_email                                                 AS recipient,
  scheduled_action_name                                           AS schedule_action,
  schedule_name                                                   AS schedule,
  last_completed_at                                               AS last_run_completed_at,
  last_finish_status                                              AS last_status,
  last_run_seconds                                                AS last_runtime_sec
FROM cdo_restricted.tableau_fct_subscription_health
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
ORDER BY last_run_completed_at DESC NULLS LAST
LIMIT 500;


-- =====================================================================================
-- TAB 9 — GOVERNANCE & AUDIT
-- =====================================================================================

-- [VISUAL] Stacked bar: Event volume over time by action_type
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  cast(event_created_at as date)                                  AS day,
  action_type,
  count(*)                                                       AS events
FROM cdo_restricted.tableau_fct_historical_events_enriched
WHERE cast(event_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
GROUP BY cast(event_created_at as date), action_type
ORDER BY day, action_type;


-- [VISUAL] Table: Recent governance events (drillable evidence)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT
  event_created_at,
  action_type,
  event_name,
  actor_name                                                     AS actor,
  actor_email                                                    AS actor_email,
  project_name,
  workbook_name,
  view_name,
  datasource_name,
  schedule_name,
  details
FROM cdo_restricted.tableau_fct_historical_events_enriched
WHERE cast(event_created_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
ORDER BY event_created_at DESC
LIMIT 500;


-- [VISUAL] Table: Group membership (security review)
SELECT
  group_name                                                     AS group,
  count(distinct user_email)                                     AS members
FROM cdo_restricted.tableau_dim_group_membership
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
  AND ('{{p_group}}' = 'ALL' OR group_name = '{{p_group}}')
GROUP BY group_name
ORDER BY members DESC;


-- [VISUAL] Table: Data Quality Indicators (governance / trust signals)
SELECT
  content_type,
  content_id,
  data_quality_type,
  is_active,
  is_severe,
  message,
  user_email                                                     AS set_by,
  created_at,
  updated_at
FROM cdo_restricted.tableau_fct_data_quality_indicators
WHERE ('{{p_site_id}}' = 'ALL' OR cast(site_id as string) = '{{p_site_id}}')
ORDER BY updated_at DESC
LIMIT 500;


-- =====================================================================================
-- TAB 10 — “WHAT WE CAN’T SEE” (GAPS) + NEXT STEPS (optional tile/table)
-- =====================================================================================

-- [VISUAL] Table: Repo limitations + how to get the missing signals
SELECT 'Exact filter field + value applied' AS missing_signal,
       'Not reliably available in Tableau repo; use Advanced Management Activity Log and/or instrument via Extensions/Embedding to log filter-change events with values.' AS how_to_get_it
UNION ALL
SELECT 'Mark-level clicks / what marks they clicked', 
       'Not in repo; instrument mark selection events via Extensions/Embedding API and write to lakehouse.'
UNION ALL
SELECT 'Tooltip hover usage',
       'Not a first-class event; only measurable if instrumented (embedded) or redesigned into an explicit logged action.';


-- =====================================================================================
-- FILTER DROPDOWN DATASETS (use these to power dashboard selectors)
-- =====================================================================================

-- [FILTER] Project list
SELECT distinct project_name AS project
FROM cdo_restricted.tableau_dim_workbook
WHERE project_name IS NOT NULL
ORDER BY project;

-- [FILTER] Workbook list (optionally filtered by project)
SELECT distinct workbook_name AS workbook
FROM cdo_restricted.tableau_dim_workbook
WHERE workbook_name IS NOT NULL
  AND ('{{p_project}}' = 'ALL' OR project_name = '{{p_project}}')
ORDER BY workbook;

-- [FILTER] View list (optionally filtered by workbook)
SELECT distinct view_name AS view
FROM cdo_restricted.tableau_dim_view
WHERE view_name IS NOT NULL
  AND ('{{p_workbook}}' = 'ALL' OR workbook_name = '{{p_workbook}}')
ORDER BY view;

-- [FILTER] User list (top N active in range)
WITH p AS (
  SELECT coalesce(to_date('{{p_start_date}}'), date('2025-01-01')) AS start_date,
         coalesce(to_date('{{p_end_date}}'), current_date())      AS end_date
)
SELECT user_email
FROM cdo_restricted.tableau_fct_session_summary
WHERE cast(session_start_at as date) BETWEEN (SELECT start_date FROM p) AND (SELECT end_date FROM p)
  AND user_email IS NOT NULL
GROUP BY user_email
ORDER BY count(*) DESC
LIMIT 500;

-- [FILTER] Session list (recent sessions)
SELECT distinct session_id
FROM cdo_restricted.tableau_fct_session_summary
WHERE session_id IS NOT NULL
ORDER BY session_id DESC
LIMIT 500;

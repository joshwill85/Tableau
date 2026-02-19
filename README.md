CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_navigation_events AS
WITH base AS (
  SELECT
    r.*,

    -- Parse the /views path portion
    NULLIF(regexp_extract(r.http_request_uri, '/views/([^?]+)', 1), '') AS views_path,

    -- Split into workbook + view content URL segments (the typical /views/<wb>/<view>)
    NULLIF(split_part(regexp_extract(r.http_request_uri, '/views/([^?]+)', 1), '/', 1), '') AS workbook_url,
    NULLIF(split_part(regexp_extract(r.http_request_uri, '/views/([^?]+)', 1), '/', 2), '') AS view_url
  FROM cdo_restricted.tableau_fct_http_requests_enriched r
  WHERE r.session_id IS NOT NULL
),
mapped AS (
  SELECT
    b.*,

    dv.view_id      AS view_id_by_url,
    dv.view_name    AS view_name_by_url,
    dv.workbook_id  AS workbook_id_by_url,
    dv.workbook_name AS workbook_name_by_url,
    dv.project_id   AS project_id_by_url,
    dv.project_name AS project_name_by_url

  FROM base b
  LEFT JOIN cdo_restricted.tableau_dim_view dv
    ON dv.site_id = b.site_id
   AND b.workbook_url IS NOT NULL
   AND lower(dv.workbook_repo_url) = lower(b.workbook_url)
   AND (
        -- primary: /views/<wb>/<view> matches view_repo_url segment
        (b.view_url IS NOT NULL AND lower(dv.view_repo_url) = lower(b.view_url))

        -- fallback: currentsheet matches the view name or view repo url
        OR (b.currentsheet IS NOT NULL AND lower(dv.view_name) = lower(b.currentsheet))
        OR (b.currentsheet IS NOT NULL AND lower(dv.view_repo_url) = lower(b.currentsheet))
   )
),
resolved AS (
  SELECT
    *,

    -- prefer already-resolved IDs from http_requests_enriched, fallback to URL-mapped values
    coalesce(view_id, view_id_by_url) AS view_id_resolved,
    coalesce(view_name, view_name_by_url) AS view_name_resolved,

    coalesce(workbook_id, workbook_id_by_url, workbook_id_from_uri) AS workbook_id_resolved,
    coalesce(workbook_name, workbook_name_by_url, workbook_name_from_uri) AS workbook_name_resolved,

    coalesce(project_id, project_id_by_url) AS project_id_resolved,
    coalesce(project_name, project_name_by_url, project_name_from_uri) AS project_name_resolved
  FROM mapped
),
nav AS (
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

    request_created_at,
    request_duration_ms,
    http_status,
    http_status_class,

    view_id_resolved,
    view_name_resolved,
    workbook_id_resolved,
    workbook_name_resolved,
    project_id_resolved,
    project_name_resolved,

    controller,
    action,
    http_request_uri,
    currentsheet,

    lag(view_id_resolved) OVER (
      PARTITION BY site_id, session_id
      ORDER BY request_created_at, http_request_id
    ) AS prev_view_id
  FROM resolved
  WHERE view_id_resolved IS NOT NULL
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

  request_created_at AS view_entered_at,
  request_duration_ms AS view_load_time_ms_proxy,
  http_status,
  http_status_class,

  view_id_resolved AS view_id,
  view_name_resolved AS view_name,
  workbook_id_resolved AS workbook_id,
  workbook_name_resolved AS workbook_name,
  project_id_resolved AS project_id,
  project_name_resolved AS project_name,

  -- keep these for debugging / calibration (optional)
  controller,
  action,
  http_request_uri,
  currentsheet
FROM nav
WHERE prev_view_id IS NULL OR view_id_resolved <> prev_view_id;

CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_view_navigation_events AS
WITH base AS (
  SELECT
    r.site_id,
    r.http_request_id,
    r.session_id,
    r.request_created_at,
    r.request_duration_ms,
    r.http_status,
    r.http_status_class,

    r.resolved_user_id,
    r.system_user_id,
    r.user_name,
    r.user_email,
    r.client_ip_token,
    r.client_ip_scope,
    r.client_device_family,
    r.client_browser_family,

    r.controller,
    r.action,
    r.http_request_uri,
    r.currentsheet,

    -- Normalize currentsheet
    lower(trim(r.currentsheet)) AS sheet_norm,

    -- Parse workbook + view from currentsheet when it has workbook/view format
    CASE
      WHEN r.currentsheet LIKE '%/%' THEN lower(split_part(trim(r.currentsheet), '/', 1))
      ELSE NULL
    END AS sheet_workbook_url,

    CASE
      WHEN r.currentsheet LIKE '%/%' THEN lower(regexp_extract(trim(r.currentsheet), '^[^/]+/(.+)$', 1))
      ELSE lower(trim(r.currentsheet))
    END AS sheet_view_url

  FROM cdo_restricted.tableau_fct_http_requests_enriched r
  WHERE r.session_id IS NOT NULL
    AND r.currentsheet IS NOT NULL
    AND r.currentsheet <> ''
),
mapped AS (
  SELECT
    b.*,

    -- Map to real Tableau metadata when possible (handles both repo_url formats)
    dv.view_id,
    dv.view_name,
    dv.workbook_id,
    dv.workbook_name,
    dv.project_id,
    dv.project_name,

    -- How we mapped (debug/quality)
    CASE
      WHEN dv.view_id IS NOT NULL THEN
        CASE
          WHEN lower(dv.view_repo_url) = b.sheet_norm THEN 'repo_url_match_full'
          WHEN b.sheet_workbook_url IS NOT NULL
           AND lower(dv.workbook_repo_url) = b.sheet_workbook_url
           AND lower(dv.view_repo_url)     = b.sheet_view_url THEN 'repo_url_match_split'
          ELSE 'repo_url_match_other'
        END
      ELSE 'unmapped_currentsheet'
    END AS view_mapping_method

  FROM base b
  LEFT JOIN cdo_restricted.tableau_dim_view dv
    ON dv.site_id = b.site_id
   AND (
        -- Case 1: views.repository_url already equals "workbook/view"
        lower(dv.view_repo_url) = b.sheet_norm

        -- Case 2: views.repository_url is only "view" and workbook repo is separate
        OR (
            b.sheet_workbook_url IS NOT NULL
            AND lower(dv.workbook_repo_url) = b.sheet_workbook_url
            AND lower(dv.view_repo_url)     = b.sheet_view_url
           )
       )
),
resolved AS (
  SELECT
    *,
    -- Stable view key even if the metadata join fails
    coalesce(cast(view_id as string), concat('sheet:', sheet_norm)) AS view_key,

    -- Friendly names even if join fails
    coalesce(view_name, currentsheet) AS view_name_resolved,
    workbook_id  AS workbook_id_resolved,
    workbook_name AS workbook_name_resolved,
    project_id   AS project_id_resolved,
    project_name AS project_name_resolved,

    lag(coalesce(cast(view_id as string), concat('sheet:', sheet_norm))) OVER (
      PARTITION BY site_id, session_id
      ORDER BY request_created_at, http_request_id
    ) AS prev_view_key
  FROM mapped
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

  project_id_resolved      AS project_id,
  project_name_resolved    AS project_name,
  workbook_id_resolved     AS workbook_id,
  workbook_name_resolved   AS workbook_name,
  view_id                  AS view_id,
  view_name_resolved       AS view_name,

  view_key,
  view_mapping_method,

  -- keep for calibration / debugging
  controller,
  action,
  currentsheet,
  http_request_uri
FROM resolved
WHERE prev_view_key IS NULL OR view_key <> prev_view_key;

CREATE OR REPLACE VIEW cdo_restricted.tableau_fct_tooltip_events AS
SELECT
  site_id,
  session_id,
  request_created_at AS tooltip_at,
  resolved_user_id,
  user_name,
  user_email,
  workbook_name,
  view_name,
  currentsheet,
  controller,
  action,
  http_request_uri
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE action LIKE '%render-tooltip-server%';

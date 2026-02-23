/* =====================================================================================
BLOCK 2 — CHOICE LIST (dropdown) DATASETS — UPDATED FOR SITE NAME
Create EACH as its own dataset: Data → Add dataset → Create from SQL
===================================================================================== */

-- DATASET: PARAM_SITE_ID_LIST  (binds p_site_id)
-- NOW returns Site Name, not numeric id.
SELECT site_id
FROM (
  SELECT 'All' AS site_id
  UNION ALL
  SELECT DISTINCT site_name AS site_id
  FROM cdo_restricted.tableau_dim_site
  WHERE site_name IS NOT NULL
) t
ORDER BY CASE WHEN site_id='All' THEN 0 ELSE 1 END, site_id;


-- DATASET: PARAM_PROJECT_LIST  (binds p_project) — cascades on p_site_id
WITH p AS (
  SELECT coalesce(nullif(:p_site_id,''), 'All') AS site_pick
),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT project_name
FROM (
  SELECT 'All' AS project_name
  UNION ALL
  SELECT DISTINCT w.project_name
  FROM cdo_restricted.tableau_dim_workbook w
  CROSS JOIN p
  WHERE w.project_name IS NOT NULL
    AND (
      p.site_pick='All'
      OR cast(w.site_id as string)=p.site_pick
      OR cast(w.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN project_name='All' THEN 0 ELSE 1 END, project_name;


-- DATASET: PARAM_WORKBOOK_LIST (binds p_workbook; cascades on p_site_id + p_project)
WITH p AS (
  SELECT
    coalesce(nullif(:p_site_id,''), 'All') AS site_pick,
    coalesce(nullif(:p_project,''), 'All') AS project
),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT workbook_name
FROM (
  SELECT 'All' AS workbook_name
  UNION ALL
  SELECT DISTINCT w.workbook_name
  FROM cdo_restricted.tableau_dim_workbook w
  CROSS JOIN p
  WHERE w.workbook_name IS NOT NULL
    AND (p.project='All' OR w.project_name=p.project)
    AND (
      p.site_pick='All'
      OR cast(w.site_id as string)=p.site_pick
      OR cast(w.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN workbook_name='All' THEN 0 ELSE 1 END, workbook_name;


-- DATASET: PARAM_VIEW_LIST (binds p_view; cascades on p_site_id + p_workbook; label "Workbook / View")
WITH p AS (
  SELECT
    coalesce(nullif(:p_site_id,''), 'All') AS site_pick,
    coalesce(nullif(:p_workbook,''), 'All') AS workbook
),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT view_display
FROM (
  SELECT 'All' AS view_display
  UNION ALL
  SELECT DISTINCT concat_ws(' / ', v.workbook_name, v.view_name) AS view_display
  FROM cdo_restricted.tableau_dim_view v
  CROSS JOIN p
  WHERE v.workbook_name IS NOT NULL AND v.view_name IS NOT NULL
    AND (p.workbook='All' OR v.workbook_name=p.workbook)
    AND (
      p.site_pick='All'
      OR cast(v.site_id as string)=p.site_pick
      OR cast(v.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN view_display='All' THEN 0 ELSE 1 END, view_display;


-- DATASET: PARAM_USER_EMAIL_LIST (binds p_user_email; scoped by p_date + p_site_id)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_pick
),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT user_email
FROM (
  SELECT 'All' AS user_email
  UNION ALL
  SELECT ss.user_email
  FROM cdo_restricted.tableau_fct_session_summary ss
  CROSS JOIN p
  WHERE cast(ss.session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND ss.user_email IS NOT NULL
    AND (
      p.site_pick='All'
      OR cast(ss.site_id as string)=p.site_pick
      OR cast(ss.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
  GROUP BY ss.user_email
) t
ORDER BY CASE WHEN user_email='All' THEN 0 ELSE 1 END, user_email
LIMIT 2000;


-- DATASET: PARAM_GROUP_LIST (binds p_group; optional cascade on p_site_id)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''), 'All') AS site_pick),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT group_name
FROM (
  SELECT 'All' AS group_name
  UNION ALL
  SELECT DISTINCT gm.group_name
  FROM cdo_restricted.tableau_dim_group_membership gm
  CROSS JOIN p
  WHERE gm.group_name IS NOT NULL
    AND (
      p.site_pick='All'
      OR cast(gm.site_id as string)=p.site_pick
      OR cast(gm.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN group_name='All' THEN 0 ELSE 1 END, group_name;


-- DATASET: PARAM_DEVICE_LIST (binds p_device; optional cascade on p_site_id)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''), 'All') AS site_pick),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT device
FROM (
  SELECT 'All' AS device
  UNION ALL
  SELECT DISTINCT ss.client_device_family AS device
  FROM cdo_restricted.tableau_fct_session_summary ss
  CROSS JOIN p
  WHERE ss.client_device_family IS NOT NULL
    AND (
      p.site_pick='All'
      OR cast(ss.site_id as string)=p.site_pick
      OR cast(ss.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN device='All' THEN 0 ELSE 1 END, device;


-- DATASET: PARAM_BROWSER_LIST (binds p_browser; optional cascade on p_site_id)
WITH p AS (SELECT coalesce(nullif(:p_site_id,''), 'All') AS site_pick),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT browser
FROM (
  SELECT 'All' AS browser
  UNION ALL
  SELECT DISTINCT ss.client_browser_family AS browser
  FROM cdo_restricted.tableau_fct_session_summary ss
  CROSS JOIN p
  WHERE ss.client_browser_family IS NOT NULL
    AND (
      p.site_pick='All'
      OR cast(ss.site_id as string)=p.site_pick
      OR cast(ss.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
) t
ORDER BY CASE WHEN browser='All' THEN 0 ELSE 1 END, browser;


-- DATASET: PARAM_SESSION_ID_LIST (binds p_session_id; scoped by p_date + p_site_id + p_user_email)
WITH p AS (
  SELECT
    coalesce(:p_date.min, date('2025-01-01')) AS dmin,
    coalesce(:p_date.max, current_date())     AS dmax,
    coalesce(nullif(:p_site_id,''), 'All')    AS site_pick,
    coalesce(nullif(:p_user_email,''), 'All') AS user_email
),
selected_site_ids AS (
  SELECT cast(site_id as string) AS site_id
  FROM cdo_restricted.tableau_dim_site
  CROSS JOIN p
  WHERE p.site_pick <> 'All'
    AND site_name = p.site_pick
)
SELECT session_id
FROM (
  SELECT 'All' AS session_id
  UNION ALL
  SELECT ss.session_id
  FROM cdo_restricted.tableau_fct_session_summary ss
  CROSS JOIN p
  WHERE cast(ss.session_start_at as date) BETWEEN p.dmin AND p.dmax
    AND ss.session_id IS NOT NULL
    AND (p.user_email='All' OR ss.user_email=p.user_email)
    AND (
      p.site_pick='All'
      OR cast(ss.site_id as string)=p.site_pick
      OR cast(ss.site_id as string) IN (SELECT site_id FROM selected_site_ids)
    )
  GROUP BY ss.session_id
) t
ORDER BY CASE WHEN session_id='All' THEN 0 ELSE 1 END, session_id
LIMIT 2000;

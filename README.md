-- A) Do we ever see /views/ paths or is it mostly /vizql/?
SELECT
  count(*) AS total_rows,
  sum(CASE WHEN http_request_uri LIKE '%/views/%' THEN 1 ELSE 0 END)  AS rows_with_views_path,
  sum(CASE WHEN http_request_uri LIKE '%/vizql/%' THEN 1 ELSE 0 END)  AS rows_with_vizql_path,
  sum(CASE WHEN currentsheet IS NOT NULL AND currentsheet <> '' THEN 1 ELSE 0 END) AS rows_with_currentsheet
FROM cdo_restricted.tableau_fct_http_requests_enriched;

-- B) What does currentsheet look like (top values)?
SELECT
  currentsheet,
  count(*) AS n
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE currentsheet IS NOT NULL AND currentsheet <> ''
GROUP BY currentsheet
ORDER BY n DESC
LIMIT 50;

-- C) What controller/action combos show up when currentsheet is present?
SELECT
  controller,
  action,
  count(*) AS n
FROM cdo_restricted.tableau_fct_http_requests_enriched
WHERE currentsheet IS NOT NULL AND currentsheet <> ''
GROUP BY controller, action
ORDER BY n DESC
LIMIT 50;

-- =====================================================================================
-- NOTE: These are PASSTHROUGH VIEWS (not physical copies).
-- They let you grant access to cdo_restricted while keeping ec_tableau_meta locked down.
-- For true “copies” + 13-month history, use the archive Delta tables/jobs (separate cell).
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS cdo_restricted;

-- =====================================================================================
-- 1) SOURCE PASSTHROUGH VIEWS (one per ec_tableau_meta table we reference)
-- Naming convention: cdo_restricted.tableau_src_<table_name>
-- =====================================================================================

-- Core identity / content
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_users                         AS SELECT * FROM ec_tableau_meta.users;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_system_users                  AS SELECT * FROM ec_tableau_meta.system_users;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_projects                      AS SELECT * FROM ec_tableau_meta.projects;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_workbooks                     AS SELECT * FROM ec_tableau_meta.workbooks;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_views                         AS SELECT * FROM ec_tableau_meta.views;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_datasources                   AS SELECT * FROM ec_tableau_meta.datasources;

-- Connections / refresh metadata
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_data_connections              AS SELECT * FROM ec_tableau_meta.data_connections;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_datasource_refresh_properties AS SELECT * FROM ec_tableau_meta.datasource_refresh_properties;

-- Usage / traffic
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_sessions                      AS SELECT * FROM ec_tableau_meta.sessions;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_http_requests                 AS SELECT * FROM ec_tableau_meta.http_requests;

-- Schedules / tasks / subscriptions
-- NOTE: schedules is commonly server-scoped (no site_id); site context comes from tasks/subscriptions/jobs.
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_schedules                     AS SELECT * FROM ec_tableau_meta.schedules;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_tasks                         AS SELECT * FROM ec_tableau_meta.tasks;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_subscriptions                 AS SELECT * FROM ec_tableau_meta.subscriptions;

-- Groups / membership
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_groups                        AS SELECT * FROM ec_tableau_meta.groups;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_group_users                   AS SELECT * FROM ec_tableau_meta.group_users;

-- Tags / governance markers
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_tags                          AS SELECT * FROM ec_tableau_meta.tags;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_taggings                      AS SELECT * FROM ec_tableau_meta.taggings;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_data_quality_indicators       AS SELECT * FROM ec_tableau_meta.data_quality_indicators;

-- Adoption aggregates
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_view_metrics_aggregations     AS SELECT * FROM ec_tableau_meta.view_metrics_aggregations;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_datasource_metrics_aggregations AS SELECT * FROM ec_tableau_meta.datasource_metrics_aggregations;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_content_usage                 AS SELECT * FROM ec_tableau_meta.content_usage;

-- Extract metadata
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_extracts                      AS SELECT * FROM ec_tableau_meta.extracts;

-- Jobs / operations
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_background_jobs               AS SELECT * FROM ec_tableau_meta.background_jobs;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_most_recent_refreshes         AS SELECT * FROM ec_tableau_meta.most_recent_refreshes;

-- Audit (historical events + type)
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_historical_events             AS SELECT * FROM ec_tableau_meta.historical_events;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_historical_event_types        AS SELECT * FROM ec_tableau_meta.historical_event_types;

-- Hist dimensions (point-in-time copies used by historical_events)
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_sites                    AS SELECT * FROM ec_tableau_meta.hist_sites;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_users                    AS SELECT * FROM ec_tableau_meta.hist_users;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_projects                 AS SELECT * FROM ec_tableau_meta.hist_projects;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_workbooks                AS SELECT * FROM ec_tableau_meta.hist_workbooks;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_views                    AS SELECT * FROM ec_tableau_meta.hist_views;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_datasources              AS SELECT * FROM ec_tableau_meta.hist_datasources;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_schedules                AS SELECT * FROM ec_tableau_meta.hist_schedules;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_hist_tasks                    AS SELECT * FROM ec_tableau_meta.hist_tasks;

-- Capacity / license (optional but common)
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_historical_disk_usage         AS SELECT * FROM ec_tableau_meta.historical_disk_usage;
CREATE OR REPLACE VIEW cdo_restricted.tableau_src_identity_based_activation_admin_view AS SELECT * FROM ec_tableau_meta.identity_based_activation_admin_view;

-- =====================================================================================
-- 2) VIEW REGISTRY (single place to see everything we built)
--    Includes: curated views (analytics layer) + source passthrough views (this cell)
-- =====================================================================================

CREATE OR REPLACE VIEW cdo_restricted.tableau_view_registry AS
SELECT * FROM VALUES
-- ---------------------------
-- Curated Dimensions
-- ---------------------------
('tableau_dim_user','curated','User identity (site user + system identity)'),
('tableau_dim_project','curated','Projects'),
('tableau_dim_workbook','curated','Workbooks + owner/project + extract freshness fields'),
('tableau_dim_view','curated','Views/Sheets (tabs) + workbook/project'),
('tableau_dim_datasource','curated','Datasources + owner/project + extract freshness fields'),
('tableau_dim_datasource_refresh_properties','curated','Datasource refresh properties'),
('tableau_dim_data_connection','curated','Connection inventory (live vs extract, bridge, dbclass)'),
('tableau_fct_content_connection_summary','curated','Live vs extract/bridge summary per workbook/datasource'),
('tableau_dim_schedule','curated','Schedules (server-scoped; no site_id)'),
('tableau_dim_task','curated','Tasks (site-scoped schedule bindings to content)'),
('tableau_dim_subscription','curated','Subscriptions + schedule + recipients'),
('tableau_dim_group_membership','curated','Group membership (security review)'),
('tableau_dim_content_tags','curated','Tags applied to content'),

-- ---------------------------
-- Curated Facts / Analytics
-- ---------------------------
('tableau_fct_http_requests_enriched','curated','Request telemetry joined to user/session/content + masked IP tokens'),
('tableau_fct_view_navigation_events','curated','View/tab navigation events (view changes per session)'),
('tableau_fct_view_dwell','curated','Inferred time on view/tab (idle-capped)'),
('tableau_fct_workbook_dwell','curated','Inferred time in workbook (sum of view dwell)'),
('tableau_fct_session_summary','curated','Session KPIs: duration, request/error counts, unique views/workbooks'),
('tableau_fct_engagement_paths','curated','Landing-to-next transitions (paths/funnels)'),
('tableau_fct_view_interaction_intensity','curated','Request volume during a view visit (interaction proxy)'),
('tableau_fct_views_stats_enriched','curated','User-level cumulative view stats + last view time'),
('tableau_fct_view_metrics_daily','curated','Daily view counts (site-level)'),
('tableau_fct_datasource_metrics_daily','curated','Daily datasource counts (site-level)'),
('tableau_fct_content_usage_enriched','curated','Content usage (coarse last-recorded operations)'),
('tableau_fct_content_staleness','curated','Staleness/unused content indicators'),
('tableau_dim_extract_directory','curated','Extract directory metadata (write proxy)'),
('tableau_fct_background_jobs_enriched','curated','Job history + queue/run durations + task/schedule/sub linkage + write proxies'),
('tableau_fct_task_health','curated','Task health (overdue proxy, failures, days since success)'),
('tableau_fct_subscription_health','curated','Subscription last run health (via background jobs)'),
('tableau_fct_data_freshness','curated','Extract freshness + live/extract + bridge indicators'),
('tableau_fct_historical_events_enriched','curated','Audit trail enriched with hist_* dimensions'),
('tableau_fct_extract_refresh_most_recent','curated','Most recent refresh entries for alerting'),
('tableau_fct_data_quality_indicators','curated','DQ warnings/cert indicators'),
('tableau_fct_disk_usage','curated','Disk usage history (capacity)'),
('tableau_fct_license_usage','curated','License usage (if enabled)'),

-- ---------------------------
-- Optional (repo-only proxy)
-- ---------------------------
('tableau_fct_manual_refresh_proxy','optional','Approx manual refresh detection via URI/controller/action patterns (calibrate)'),

-- ---------------------------
-- Source Passthrough Views
-- ---------------------------
('tableau_src_users','source_passthrough','Raw users table'),
('tableau_src_system_users','source_passthrough','Raw system_users table'),
('tableau_src_projects','source_passthrough','Raw projects table'),
('tableau_src_workbooks','source_passthrough','Raw workbooks table'),
('tableau_src_views','source_passthrough','Raw views table'),
('tableau_src_datasources','source_passthrough','Raw datasources table'),
('tableau_src_data_connections','source_passthrough','Raw data_connections table'),
('tableau_src_datasource_refresh_properties','source_passthrough','Raw datasource_refresh_properties table'),
('tableau_src_sessions','source_passthrough','Raw sessions table'),
('tableau_src_http_requests','source_passthrough','Raw http_requests table (contains IP columns)'),
('tableau_src_schedules','source_passthrough','Raw schedules table (server-scoped)'),
('tableau_src_tasks','source_passthrough','Raw tasks table'),
('tableau_src_subscriptions','source_passthrough','Raw subscriptions table'),
('tableau_src_groups','source_passthrough','Raw groups table'),
('tableau_src_group_users','source_passthrough','Raw group_users table'),
('tableau_src_tags','source_passthrough','Raw tags table'),
('tableau_src_taggings','source_passthrough','Raw taggings table'),
('tableau_src_data_quality_indicators','source_passthrough','Raw data_quality_indicators table'),
('tableau_src_view_metrics_aggregations','source_passthrough','Raw view_metrics_aggregations table'),
('tableau_src_datasource_metrics_aggregations','source_passthrough','Raw datasource_metrics_aggregations table'),
('tableau_src_content_usage','source_passthrough','Raw content_usage table'),
('tableau_src_extracts','source_passthrough','Raw extracts table'),
('tableau_src_background_jobs','source_passthrough','Raw background_jobs table'),
('tableau_src_most_recent_refreshes','source_passthrough','Raw most_recent_refreshes table'),
('tableau_src_historical_events','source_passthrough','Raw historical_events table'),
('tableau_src_historical_event_types','source_passthrough','Raw historical_event_types table'),
('tableau_src_hist_sites','source_passthrough','Raw hist_sites table'),
('tableau_src_hist_users','source_passthrough','Raw hist_users table'),
('tableau_src_hist_projects','source_passthrough','Raw hist_projects table'),
('tableau_src_hist_workbooks','source_passthrough','Raw hist_workbooks table'),
('tableau_src_hist_views','source_passthrough','Raw hist_views table'),
('tableau_src_hist_datasources','source_passthrough','Raw hist_datasources table'),
('tableau_src_hist_schedules','source_passthrough','Raw hist_schedules table'),
('tableau_src_hist_tasks','source_passthrough','Raw hist_tasks table'),
('tableau_src_historical_disk_usage','source_passthrough','Raw historical_disk_usage table'),
('tableau_src_identity_based_activation_admin_view','source_passthrough','Raw identity_based_activation_admin_view')
AS t(view_name, category, description);


-- =====================================================================================
-- 3) Quick list of views in the schema (optional convenience)
-- =====================================================================================
SHOW VIEWS IN cdo_restricted;

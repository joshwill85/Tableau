EPIC USER STORY — “Tableau Server Observability + Adoption Dashboard (Industry Standard)”

As a Tableau Platform Owner (serving Server Admins, Site Admins, Data Owners, Security/Audit, and Leadership),
I want a single, trusted Tableau “Admin Observability” workbook (with multiple tabs) backed by curated Unity Catalog views,
so that we can (1) understand adoption and engagement, (2) monitor performance and reliability, (3) validate data freshness,
(4) enforce governance/security, and (5) plan capacity — with at least 13 months of history (e.g., 2025-01-01 → current).

--------------------------------------------
A) PRIMARY USERS / MODES
--------------------------------------------
1) Leadership mode (read-only):
   - “Is Tableau healthy?” “Are people using it?” “Where are the risks?” “What’s trending up/down?”
2) Admin/SRE mode (actionable):
   - “What’s failing?” “What’s slow?” “What’s overdue?” “Who/what is driving load?” “What changed?”
3) Data owner mode:
   - “Are my extracts fresh?” “Are subscriptions working?” “Do users hit errors?” “What’s the adoption of my assets?”
4) Security/Audit mode:
   - “What changed (permissions/ownership/groups)?” “Who accessed what?” “Evidence for investigations.”

--------------------------------------------
B) DASHBOARD END PRODUCT (ONE WORKBOOK, MULTIPLE TABS)
--------------------------------------------
Tab 1 — Executive Overview (60-second health check)
KPIs (tiles + trends):
- Active users (DAU/WAU/MAU), top projects/workbooks/views
- Error rate (4xx/5xx), sessions, request volume
- Refresh/subscription success rate, top failures today
- Overdue schedules/tasks (proxy), consecutive failures
- Data freshness (stale extracts by project), live vs extract mix
- Capacity: disk usage trend + top growing content (optional)

Tab 2 — Adoption & Content Portfolio
- Active users over time (by site/project/role/group)
- Top content (views/workbooks/datasources) by usage & unique users
- Unused/stale content: “no access in 30/60/90/180+ days”
- Ownership distribution (bus factor), content growth, publish frequency

Tab 3 — Engagement & Navigation (Funnels)
- Landing view → next view transitions (top paths)
- Repeat sessions / returning users
- “Bounce sessions” (sessions with only 1 view entered)
- “Common journeys” (workbook-level paths)

Tab 4 — Time-in-View / Time-in-Workbook (Inferred)
- Median / p90 dwell time by view/workbook
- Drilldown: user → session → sequence of views with dwell
- Outliers (very long dwell; capped dwell) to identify confusion vs stickiness
- Caveat messaging: “dwell inferred from navigation; idle capped”

Tab 5 — Performance (Repository proxy + errors)
- p50/p95 “view_load_time_ms_proxy” by view/workbook (from request duration)
- Error hotspots (5xx by workbook/view/controller/action)
- Client segmentation (browser/device family)
- NOTE: If Activity Log becomes available, this tab upgrades to “true view load times”

Tab 6 — Reliability & Backgrounder (Jobs/Schedules)
- Job success rate (extract refreshes, subscriptions, flows), queue time vs run time
- Top failing tasks (by object, schedule, owner)
- Overdue tasks proxy (next_run passed + no success)
- “Did refresh write?” proxy (extract timestamps / extract dir updated)

Tab 7 — Data Freshness & Live vs Extract Risk
- Extract freshness SLA coverage (days since refresh) by project/content owner
- Live connections inventory & Bridge usage flags
- “High-risk” content: live + high usage + slow + errors

Tab 8 — Subscriptions / Delivery Health
- Subscription failures, runtimes, last completed time
- High-volume subscriptions (load drivers)
- “Most recent subscription run” status (if available)

Tab 9 — Governance & Audit
- Timeline of changes: ownership changes, permission changes, publishes, deletes
- Group membership & access review support (who is in what group)
- Tags / certifications / data quality indicators coverage & changes

Tab 10 — Capacity & License (optional but common)
- Disk usage trend
- License usage / login-based activation (if present)
- Storage by workbook/datasource size & growth (where possible)

--------------------------------------------
C) GLOBAL FILTERS / “SEE DATA DIFFERENT WAYS”
--------------------------------------------
Every tab supports:
- Date range + time grain (day/week/month)
- Site / project / workbook / view / datasource
- User (name/email), owner (name/email), group, admin level, domain
- Client segmentation (device family, browser family, IP scope, unique client token)
- Status filters (success/failure, finish_status, HTTP status class)
- Top-N sliders (Top 10/25/100)
- Drillthrough actions:
  Exec overview → (click KPI) → underlying detailed tab pre-filtered
  Any chart → “Show sessions” → session timeline detail table

--------------------------------------------
D) KPI / INSIGHT CATALOG (WHAT WE CAN GLEAN FROM OUR VIEWS)
--------------------------------------------
ADOPTION
- DAU/WAU/MAU (unique user_email) via session_summary / navigation_events
- Top views/workbooks/projects (counts of view_entered events; unique users)
- Unused content (days_since_last_access) via content_staleness
- Content growth: new workbooks/views/datasources (created_at trends)
- Ownership risk: % content owned by top 1/5/10 owners (bus factor)

ENGAGEMENT / FUNNELS
- Landing view distribution (first view per session)
- Path analysis: view → next view transitions (engagement_paths)
- Bounce rate: sessions with 1 distinct view
- Repeat usage: sessions per user per week/month

TIME SPENT (INFERRED)
- Dwell per view (dwell_seconds_capped)
- Dwell per workbook (workbook_dwell_seconds_capped)
- “Confusion candidates”: high dwell + high error + high request intensity
- “Sticky content”: high dwell + high repeat sessions + low error

PERFORMANCE (REPO PROXY)
- View load proxy p95: view_load_time_ms_proxy by view/workbook
- Error rate hotspots: http 4xx/5xx by view/workbook/controller/action
- “Heavy interaction”: high request_count_during_view (interaction_intensity proxy)

RELIABILITY / BACKGROUNDER
- Success rate by job_type / schedule / task / object
- Queue time vs runtime (resource contention)
- Overdue tasks proxy + consecutive failure count
- Refresh “write occurred” proxy (extract timestamp / extract dir updated)
- “Most recent refresh” alerting status (most_recent_refreshes)

DATA FRESHNESS
- Days since last extract activity by workbook/datasource
- SLA compliance (e.g., refreshed within last 1/2/7 days)
- Live vs extract footprint + Bridge usage flags

SUBSCRIPTIONS
- Last run status, last completed time, runtime
- Failure hotspots by subscribed content + owner

GOVERNANCE / AUDIT
- Who changed what, when (historical_events enriched)
- Ownership changes, publish activity, deletes, schedule/task changes
- Tags, certifications, data quality indicators coverage and changes
- Group membership inventory (who has access pathways)

CAPACITY / LICENSE (IF AVAILABLE)
- Disk utilization trend
- License utilization trends (login-based activation view if present)

--------------------------------------------
E) SOLID “NO” AREAS (REQUIRES ADDITIONAL TELEMETRY)
--------------------------------------------
Repo-only cannot reliably deliver:
1) Exact filter fields + values users applied
2) Exact mark-level clicks (“what they clicked on” in the viz)
3) Tooltip hover usage

How to get those:
- Tableau Advanced Management Activity Log (server/Cloud dependent) for richer interaction events
- OR instrument: Tableau Extensions API / Embedding API events → write events to our lakehouse
  (this is the industry-standard solution for true UI analytics)

--------------------------------------------
F) ARCHIVING & RETENTION REQUIREMENTS (13+ MONTHS OF HISTORY)
--------------------------------------------
Goal: keep at least 13 months of history (e.g., 2025-01-01 → current), even when Tableau’s repository purges data.

We will implement an ARCHIVE LAYER (Delta tables) that is append-only:
- cdo_restricted_archive.tableau_http_requests_hist
- cdo_restricted_archive.tableau_background_jobs_hist
- cdo_restricted_archive.tableau_historical_events_hist
- cdo_restricted_archive.tableau_most_recent_refreshes_hist   (because “most recent only”)
- cdo_restricted_archive.tableau_most_recent_subscription_runs_hist (most recent only)
- Optional daily snapshots (SCD-ish) for: users, groups, workbooks, views, datasources, projects, schedules, tasks

Archiving jobs (minimum frequencies to avoid gaps):
1) http_requests: every 5–15 minutes (minimum daily; but 7-day purge risk is real)
2) background_jobs (and/or background task equivalents): at least daily; ideally hourly
3) historical_events: daily (because default retention ~6 months; we need >12 months)
4) most_recent_refreshes / most_recent_subscription_runs: daily snapshot (they overwrite / represent only latest)
5) dims (workbooks/views/datasources/users/groups/tasks/schedules): daily snapshot is fine

Partitioning / performance standard:
- Partition archive fact tables by event_date (derived from created_at / completed_at)
- Use incremental loads via high-water mark (max id or max timestamp)
- Validate pipeline completeness:
  - “last ingested timestamp” per table
  - row count deltas per run
  - anomaly alerts when ingestion drops to 0 unexpectedly

--------------------------------------------
G) LIST OF NEW VIEWS CREATED IN cdo_restricted
--------------------------------------------
Dimensions
- tableau_dim_user
- tableau_dim_project
- tableau_dim_workbook
- tableau_dim_view
- tableau_dim_datasource
- tableau_dim_datasource_refresh_properties
- tableau_dim_data_connection
- tableau_dim_schedule  (server-scoped; no site_id)
- tableau_dim_task
- tableau_dim_subscription
- tableau_dim_group_membership
- tableau_dim_content_tags

Facts / Analytics
- tableau_fct_http_requests_enriched
- tableau_fct_view_navigation_events
- tableau_fct_view_dwell
- tableau_fct_workbook_dwell
- tableau_fct_session_summary
- tableau_fct_engagement_paths
- tableau_fct_view_interaction_intensity
- tableau_fct_views_stats_enriched
- tableau_fct_view_metrics_daily
- tableau_fct_datasource_metrics_daily
- tableau_fct_content_connection_summary
- tableau_fct_content_usage_enriched
- tableau_fct_content_staleness
- tableau_dim_extract_directory
- tableau_fct_background_jobs_enriched
- tableau_fct_task_health
- tableau_fct_subscription_health
- tableau_fct_data_freshness
- tableau_fct_historical_events_enriched
- tableau_fct_extract_refresh_most_recent
- tableau_fct_data_quality_indicators
- tableau_fct_disk_usage
- tableau_fct_license_usage

Optional (repo-only proxy; needs calibration)
- tableau_fct_manual_refresh_proxy

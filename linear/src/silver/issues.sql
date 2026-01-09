CREATE OR REPLACE MATERIALIZED VIEW issues AS
select
  id,
  identifier,
  title,
  description,
  priority,
  project_id,
  project_name,
  team_id,
  team_name,
  team_key,
  assignee_id,
  assignee_name,
  state_id,
  state_name,
  CAST(convert_timezone('UTC', 'America/Edmonton', created_at) AS DATE) as created_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', started_at) AS DATE) as start_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', completed_at) AS DATE) as completed_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', canceled_at) AS DATE) as canceled_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', triaged_at) AS DATE) as triaged_date
from
  ${source_catalog}.linear.issues
where
  archived_at is null
CREATE OR REPLACE MATERIALIZED VIEW projects AS
SELECT
  id,
  name,
  description,
  state,
  start_date,
  target_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', completed_at) AS DATE) as completed_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', canceled_at) AS DATE) as canceled_date,
  lead_id,
  lead_name
FROM
   ${source_catalog}.linear.projects
WHERE
  archived_at IS NULL;
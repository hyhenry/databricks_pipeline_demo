CREATE OR REPLACE MATERIALIZED VIEW initiatives AS
SELECT
  id,
  name,
  description,
  target_date,
  organization_id,
  organization_name,
  CAST(convert_timezone('UTC', 'America/Edmonton', completed_at) AS DATE) as completed_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', created_at) AS DATE) as created_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', updated_at) AS DATE) as updated_date
FROM
  ${source_catalog}.linear.initiatives
WHERE
  archived_at IS NULL;
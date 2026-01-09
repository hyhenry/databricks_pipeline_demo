CREATE OR REPLACE MATERIALIZED VIEW teams as
select
  id,
  name,
  key,
  description,
  organization_id,
  organization_name,
  CAST(convert_timezone('UTC', 'America/Edmonton', created_at) AS DATE) as created_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', updated_at) AS DATE) as updated_date
from
  ${source_catalog}.linear.teams
where
  archived_at is null
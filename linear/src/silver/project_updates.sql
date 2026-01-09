create or replace materialized view project_updates as
select
  pu.id,
  pu.body,
  pu.health,
  pu.project_id,
  pu.project_name,
  pu.user_id,
  pu.user_name,
  pu.user_email,
  CAST(convert_timezone('UTC', 'America/Edmonton', pu.created_at) AS DATE) as created_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', pu.updated_at) AS DATE) as updated_date
from
  ${source_catalog}.linear.project_updates pu
    inner join ${source_catalog}.linear.projects p
      on p.id = pu.project_id
      and p.archived_at is null
where
  pu.archived_at is null
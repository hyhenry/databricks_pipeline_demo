create or replace materialized view project_teams as
select
  pt.project_id,
  pt.team_id,
  pt.team_name,
  pt.team_key
from
  ${source_catalog}.linear.project_teams pt
    inner join ${source_catalog}.linear.projects p
      on p.id = pt.project_id
      and p.archived_at is null
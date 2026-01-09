create or replace materialized view project_labels as
select
  pl.project_id,
  pl.label_id,
  pl.label_name
from
  ${source_catalog}.linear.project_labels pl
    inner join ${source_catalog}.linear.projects p
      on p.id = pl.project_id
      and p.archived_at is null
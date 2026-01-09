CREATE OR REPLACE MATERIALIZED VIEW issue_labels AS
select
  issue_id,
  label_id,
  label_name
from
  ${source_catalog}.linear.issue_labels
    inner join ${source_catalog}.linear.issues i
      on i.id = issue_id
where
  i.archived_at is null
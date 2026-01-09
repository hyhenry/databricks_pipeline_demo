CREATE OR REPLACE MATERIALIZED VIEW initiative_projects AS
SELECT
  ip.initiative_id,
  ip.project_id
FROM
  ${source_catalog}.linear.initiative_projects ip
    INNER JOIN ${source_catalog}.linear.initiatives i
      ON ip.initiative_id = i.id
      AND i.archived_at IS NULL
    INNER JOIN ${source_catalog}.linear.projects p
      ON ip.project_id = p.id
      AND p.archived_at IS NULL
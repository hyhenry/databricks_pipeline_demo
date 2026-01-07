# petrinex data load example

This folder defines all source code for the petrinex pipeline:

- `resources/`: yml files used to configure jobs and pipelines.
- `src/`: elt notebooks and python/sql scripts used for the jobs/pipelines.

## configuration

- in `databricks.yml`, define your variables and your development and production targets.
- in `resources/*.job.yml`, make sure the job is configured correctly, and pass in any variables from `databricks.yml` as job parameters so they can be referenced in notebooks.
- in `resources/*.pipeline.yml`, make sure it is configured to point to the desired pipeline folder, and pass in the catalog and schema from `databricks.yml` to write to. For any additional custom parameters to use as reference in your pipeline scripts, pass them in as key-value pairs in the "configuration" section.

## how it works

For this particular example, this will create a job with two tasks:

1. The first task runs a notebook which will ingest raw data into the bronze layer (`bronze.petrinex`)
2. The second task runs a pipeline which will take the bronze data and merge into the clean layer (`silver.petrinex`)

When deploying in `development` mode:
  - the asset bundle will create a job and pipeline appended with '[dev username]'
  - it will also tag the jobs and pipelines with the username. This helps avoid conflict when multiple people are working on the same project in the workspace
  - the catalogs are switched to `bronze_dev` and `silver_dev`

When deploying in `production` mode:
  - the asset bundle will create a job and pipeline in the production workspace, as defined in the yml file
  - the assets will be copied to a Shared folder to ensure it's referencing a single copy only
  - the catalogs are switched to `bronze` and `silver`

# Databricks Pipeline Demo

A collection of end-to-end ELT pipeline examples built with [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html). Each pipeline follows a bronze → silver medallion architecture and is deployable to dev and production environments.

## Pipelines

| Pipeline | Source | Description |
|---|---|---|
| [linear/](linear/) | Linear (GraphQL API) | Ingests project management data (issues, teams, projects, users, etc.) from the Linear API |
| [petrinex/](petrinex/) | Petrinex (Public CSV) | Ingests Alberta and Saskatchewan oil & gas infrastructure data from the Petrinex public data portal |

## Architecture

Each pipeline follows the same two-stage pattern:

```
Source → Bronze (raw ingestion job) → Silver (DLT pipeline)
```

**Bronze layer** — A Databricks notebook job that pulls raw data from the source and writes it to Delta tables in the `bronze` catalog.

**Silver layer** — A Delta Live Tables (DLT) pipeline that reads from bronze and produces cleaned, filtered materialized views written to the `silver` catalog.

```
bronze.<schema>.<table>  →  silver.<schema>.<table>
```

In development mode, catalogs are prefixed with `_dev` (e.g., `bronze_dev`, `silver_dev`).

## Project Structure

```
databricks_pipeline_demo/
├── linear/
│   ├── databricks.yml               # Bundle config: variables, dev/prod targets
│   ├── resources/
│   │   ├── linear_ingestion.job.yml # Job: triggers bronze + silver tasks daily
│   │   └── linear_etl.pipeline.yml  # DLT pipeline: points to silver SQL scripts
│   └── src/
│       ├── bronze/
│       │   └── linear_raw_load.py   # Extracts data from Linear GraphQL API
│       └── silver/
│           └── *.sql                # Materialized views (issues, projects, teams, etc.)
├── petrinex/
│   ├── databricks.yml
│   ├── resources/
│   │   ├── petrinex_ingestion.job.yml
│   │   └── petrinex_etl.pipeline.yml
│   └── src/
│       ├── bronze/
│       │   └── petrinex_ingest_data.py  # Downloads and loads CSVs from Petrinex
│       └── silver/
│           └── *.sql                    # Materialized views (wells, facilities, etc.)
└── utilities/
    └── write_data.py                # Shared helper: Delta merge/overwrite utility
```

## Shared Utilities

[utilities/write_data.py](utilities/write_data.py) provides a `write_data()` function used by bronze notebooks to write DataFrames to Delta tables. It supports:

- **Overwrite** — replaces the table, with optional schema evolution
- **Merge** — upserts rows using provided merge keys
- **Liquid clustering** — optional clustering on up to 4 columns
- **Spark Connect compatibility** — SQL fallback for serverless environments

## Deployment

Each pipeline is deployed independently using the Databricks CLI.

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and authenticated
- Access to a Databricks workspace with Unity Catalog enabled

### Deploy a pipeline

```bash
# Navigate to the desired pipeline folder
cd linear   # or cd petrinex

# Deploy to dev (default)
databricks bundle deploy

# Deploy to production
databricks bundle deploy --target prod

# Run the job manually
databricks bundle run linear_ingestion
```

### Environment targets

Each `databricks.yml` defines two targets:

| Target | Mode | Catalogs | Notes |
|---|---|---|---|
| `dev` (default) | development | `bronze_dev`, `silver_dev` | Resources prefixed with `[dev <username>]`; schedules paused |
| `prod` | production | `bronze`, `silver` | Deployed to `/Workspace/Shared/data_engineering/` |

## Pipeline Details

### Linear

Extracts data from the [Linear GraphQL API](https://developers.linear.app/docs/graphql/working-with-the-graphql-api) and loads it into Unity Catalog.

**Requires:** A Databricks secret stored at scope `linear`, key `api_key`.

**Bronze tables loaded:**

`teams`, `users`, `issues`, `projects`, `project_updates`, `project_labels`, `project_teams`, `issue_labels`, `cycles`, `workflow_states`, `initiatives`, `initiative_projects`

**Silver materialized views:** filtered and cleaned versions of the above, with timestamps converted to `America/Edmonton` and archived records excluded.

The job supports both full and incremental load modes, controlled by the `load_type` widget (`full` or `incremental`).

### Petrinex

Downloads public infrastructure data from the [Petrinex public data portal](https://www.petrinex.gov.ab.ca) for Alberta (AB) and Saskatchewan (SK).

**Bronze tables loaded:**

`business_associate`, `well_infrastructure`, `well_licence`, `facility_infrastructure`, `facility_operator_history`, `well_to_facility_link`, `facility_licence` (for each of AB and SK)

**Silver materialized views:** cleaned views on top of the raw bronze tables.

Data is downloaded as nested zip files into a Unity Catalog Volume, extracted, and loaded as Delta tables.

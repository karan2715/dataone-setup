# DataOne Databricks Setup

Standalone Databricks source notebook that installs DataOne's shared data-quality and Genie infrastructure.

## What it creates

- Unity Catalog schema `workspace.dataone_dq`
- Managed volume `workspace.dataone_dq.uploads`
- An accessible serverless SQL warehouse, or `DataOne SQL Warehouse` when none exists
- Serverless Lakeflow pipeline `dataone-file-quality-pipeline`
- On-demand Lakeflow Job `dataone-file-quality-job`
- Materialized results table `workspace.dataone_dq.file_quality_results`
- Genie space `DataOne NL2SQL and Data Quality`

The notebook is idempotent: rerunning it updates resources with these names instead of intentionally creating duplicates.

## Run it

1. In Databricks, open **Workspace** and select **Create → Git folder**.
2. Clone `https://github.com/karan2715/dataone-setup.git`.
3. Open `marketplace/DataOne_Setup.py` from the cloned Git folder.
4. Attach **Serverless** notebook compute.
5. Select **Run all** and wait for the initial quality job to complete.
6. Save the JSON summary printed by the last cell.

The installing user needs permission to create Unity Catalog schemas and volumes and to manage SQL warehouses, Lakeflow pipelines/jobs, and Genie spaces.

## Attach the resources to DataOne

After creating the DataOne Databricks App, attach:

| Resource | Permission | App resource key |
|---|---|---|
| `DataOne NL2SQL and Data Quality` Genie space | Can run | `genie-space` |
| SQL warehouse printed by the notebook | Can use | `sql-warehouse` |
| `dataone-file-quality-job` | Can manage run | `job` |
| `workspace.dataone_dq.uploads` | Read and write | `volume` |
| `workspace.dataone_dq.file_quality_results` | Select | `table` |

The app service principal is created with the app, so these app-resource grants happen after app creation. Source catalogs and tables selected in DataOne also require `USE CATALOG`, `USE SCHEMA`, and `SELECT` for that service principal.

## Quality calculation

The Lakeflow pipeline calculates completeness, uniqueness, validity, freshness, and an overall score. A dimension is `NULL` when it cannot be measured. Genie can query and explain measured results; it is not the source of truth for the scores.

Supported landing-file formats: CSV, TSV, JSON, JSONL/NDJSON, Parquet, Avro, and ORC.

## Source repository

The setup repository is owned by the `karan2715` GitHub account and can be cloned directly into a Databricks Git folder.

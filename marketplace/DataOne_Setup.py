# Databricks notebook source
"""Install or update DataOne's shared data-quality and Genie resources.

Run this notebook once as a Databricks workspace administrator after cloning the
DataOne repository into a Databricks Git folder. It is idempotent: rerunning it
updates the named pipeline, job, and Genie space instead of creating duplicates.

Dataset selection is intentionally not an install-time parameter. DataOne users
select a catalog, schema, and table in the application; the app then creates or
updates a scoped Genie space for that selection.
"""

# COMMAND ----------

import datetime as dt
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, PipelineTask, QueueSettings, Task
from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

CATALOG = "workspace"
DQ_SCHEMA = "dataone_dq"
UPLOAD_VOLUME = "uploads"
FRESHNESS_SLA_HOURS = "24"
PIPELINE_NAME = "dataone-file-quality-pipeline"
JOB_NAME = "dataone-file-quality-job"
GENIE_TITLE = "DataOne NL2SQL and Data Quality"
WAREHOUSE_NAME = "DataOne SQL Warehouse"
RESULTS_TABLE = f"{CATALOG}.{DQ_SCHEMA}.file_quality_results"
LANDING_PATH = f"/Volumes/{CATALOG}/{DQ_SCHEMA}/{UPLOAD_VOLUME}"

w = WorkspaceClient()


def stage(message: str) -> None:
    print(f"[DataOne setup] {message}")


def notebook_path() -> str:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    path = context.notebookPath().get()
    if "/marketplace/" not in path:
        raise RuntimeError(
            "Run this notebook from the cloned DataOne repository's marketplace directory; "
            f"current path is {path!r}."
        )
    return path


def pipeline_source_path() -> str:
    root = notebook_path().split("/marketplace/", 1)[0]
    path = f"{root}/databricks/file_quality/src/file_quality_pipeline.py"
    try:
        w.workspace.get_status(path)
    except Exception as exc:
        raise RuntimeError(
            "The Lakeflow source file was not found. Clone the complete DataOne repository into "
            f"Databricks, then run this notebook in place. Expected: {path}"
        ) from exc
    return path


# COMMAND ----------

stage("creating Unity Catalog schema and managed upload volume")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{DQ_SCHEMA}` "  # noqa: F821
          "COMMENT 'DataOne data-quality results and upload landing area'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{DQ_SCHEMA}`.`{UPLOAD_VOLUME}` "  # noqa: F821
          "COMMENT 'DataOne landing zone for CSV, JSON, Parquet, and other supported files'")


def ensure_warehouse() -> str:
    warehouses = list(w.warehouses.list())
    exact = next((warehouse for warehouse in warehouses if warehouse.name == WAREHOUSE_NAME), None)
    if exact and exact.id:
        stage(f"reusing SQL warehouse {WAREHOUSE_NAME} ({exact.id})")
        return exact.id

    serverless = next(
        (warehouse for warehouse in warehouses if warehouse.id and warehouse.enable_serverless_compute),
        None,
    )
    if serverless:
        stage(f"reusing available serverless SQL warehouse {serverless.name} ({serverless.id})")
        return serverless.id

    stage(f"creating serverless SQL warehouse {WAREHOUSE_NAME}")
    created = w.warehouses.create(
        name=WAREHOUSE_NAME,
        cluster_size="2X-Small",
        min_num_clusters=1,
        max_num_clusters=1,
        auto_stop_mins=10,
        enable_photon=True,
        enable_serverless_compute=True,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
    ).result(timeout=dt.timedelta(minutes=20))
    if not created.id:
        raise RuntimeError("Databricks created the SQL warehouse without returning its id.")
    return created.id


warehouse_id = ensure_warehouse()

# COMMAND ----------

source_path = pipeline_source_path()
pipeline_settings = {
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "schema": DQ_SCHEMA,
    "serverless": True,
    "continuous": False,
    "development": False,
    "libraries": [PipelineLibrary(file=FileLibrary(path=source_path))],
    "configuration": {
        "dataone.fq.landing_path": LANDING_PATH,
        "dataone.fq.freshness_sla_hours": FRESHNESS_SLA_HOURS,
    },
}

matching_pipelines = [
    pipeline for pipeline in w.pipelines.list_pipelines()
    if pipeline.name == PIPELINE_NAME and pipeline.pipeline_id
]
if len(matching_pipelines) > 1:
    raise RuntimeError(f"More than one Lakeflow pipeline is named {PIPELINE_NAME!r}; remove the duplicate first.")
if matching_pipelines:
    pipeline_id = matching_pipelines[0].pipeline_id
    stage(f"updating Lakeflow pipeline {PIPELINE_NAME} ({pipeline_id})")
    w.pipelines.update(pipeline_id, **pipeline_settings)
else:
    stage(f"creating Lakeflow pipeline {PIPELINE_NAME}")
    pipeline_id = w.pipelines.create(**pipeline_settings).pipeline_id
if not pipeline_id:
    raise RuntimeError("Databricks did not return the Lakeflow pipeline id.")

# COMMAND ----------

job_settings = JobSettings(
    name=JOB_NAME,
    description="Runs the DataOne file data-quality Lakeflow pipeline on demand.",
    max_concurrent_runs=1,
    queue=QueueSettings(enabled=True),
    tasks=[Task(
        task_key="profile_files",
        pipeline_task=PipelineTask(pipeline_id=pipeline_id, full_refresh=False),
        max_retries=2,
        min_retry_interval_millis=60_000,
    )],
)
matching_jobs = [job for job in w.jobs.list(name=JOB_NAME) if job.settings and job.settings.name == JOB_NAME]
if len(matching_jobs) > 1:
    raise RuntimeError(f"More than one job is named {JOB_NAME!r}; remove the duplicate first.")
if matching_jobs:
    job_id = matching_jobs[0].job_id
    stage(f"updating job {JOB_NAME} ({job_id})")
    w.jobs.reset(job_id, job_settings)
else:
    stage(f"creating job {JOB_NAME}")
    job_id = w.jobs.create(
        name=job_settings.name,
        description=job_settings.description,
        max_concurrent_runs=job_settings.max_concurrent_runs,
        queue=job_settings.queue,
        tasks=job_settings.tasks,
    ).job_id
if not job_id:
    raise RuntimeError("Databricks did not return the data-quality job id.")

stage("running the job once to materialize the empty-or-current quality-results table")
initial_run = w.jobs.run_now(job_id).result(timeout=dt.timedelta(minutes=30))
run_state = initial_run.state.result_state.value if initial_run.state and initial_run.state.result_state else None
if run_state != "SUCCESS":
    raise RuntimeError(f"The initial DataOne quality job did not succeed; final state was {run_state!r}.")

# COMMAND ----------

stage("creating or updating the starter Genie space")
serialized_space = json.dumps({
    "version": 2,
    "data_sources": {"tables": [{"identifier": RESULTS_TABLE}]},
})
description = (
    "DataOne NL2SQL and data-quality agent. Quality scores must be read from measured result tables; "
    "never estimate a missing quality metric. Source tables are attached dynamically after a user selection."
)
spaces = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 100}).get("spaces") or []
matching_spaces = [space for space in spaces if space.get("title") == GENIE_TITLE]
if len(matching_spaces) > 1:
    raise RuntimeError(f"More than one Genie space is titled {GENIE_TITLE!r}; remove the duplicate first.")
genie_body = {
    "title": GENIE_TITLE,
    "description": description,
    "warehouse_id": warehouse_id,
    "serialized_space": serialized_space,
}
if matching_spaces:
    genie_space_id = matching_spaces[0].get("space_id") or matching_spaces[0].get("id")
    stage(f"updating Genie space {GENIE_TITLE} ({genie_space_id})")
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{genie_space_id}", body=genie_body)
else:
    created_space = w.api_client.do("POST", "/api/2.0/genie/spaces", body=genie_body)
    genie_space_id = created_space.get("space_id") or created_space.get("id")
if not genie_space_id:
    raise RuntimeError("Databricks did not return the Genie space id.")

# COMMAND ----------

summary = {
    "catalog": CATALOG,
    "dq_schema": DQ_SCHEMA,
    "upload_volume": f"{CATALOG}.{DQ_SCHEMA}.{UPLOAD_VOLUME}",
    "quality_results_table": RESULTS_TABLE,
    "warehouse_id": warehouse_id,
    "pipeline_id": pipeline_id,
    "job_id": str(job_id),
    "genie_space_id": genie_space_id,
    "schedule": "on_demand",
    "timezone": "UTC",
    "app_resource_keys": {
        "genie_space_id": "genie-space",
        "warehouse_id": "sql-warehouse",
        "job_id": "dq-job",
        "upload_volume": "dq-volume",
        "quality_results_table": "dq-results-table",
    },
    "next_step": (
        "Create the DataOne app, then attach this job (CAN_MANAGE_RUN), Genie space as resource "
        "'genie-space' (CAN_RUN), SQL warehouse as resource 'sql-warehouse' (CAN_USE), upload "
        "volume as 'dq-volume' (READ/WRITE), and quality-results table as 'dq-results-table' "
        "(SELECT). Attach a Lakebase Autoscaling database resource for persistent app state; "
        "no customer-created secret scope is required."
    ),
}
stage("complete")
print(json.dumps(summary, indent=2))

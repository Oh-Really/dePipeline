from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
)
from datetime import datetime
from pathlib import Path
import logging
import os

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = "/tmp"
BUCKET = os.environ.get('BUCKET')
PROJECT_ID = os.environ.get('PROJECT_ID')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET')
GCS_PREFIX = "ny_taxi"
GCP_CONN_ID = "google_cloud_default"
DOWNLOAD_DIR = "/tmp"

DATASETS = {
    "yellow": {
        "table": "yellow_taxi_trips",
        "filename_prefix": "yellow_tripdata",
        "bq_table": "yellow_external",
    },
    "green": {
        "table": "green_taxi_trips",
        "filename_prefix": "green_tripdata",
        "bq_table": "green_external",
    },
}


def build_copy_sql(table: str, cols: list[str]) -> str:
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    return f'COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT csv);'


@dag(
    dag_id="GCSUploadDag",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    schedule = "@monthly",
    catchup=True,
    max_active_runs=3,
    default_args={"retries": 3},
    tags=["nyc-tlc", "gcs", "bigquery"],
)
def download_parquet_and_upload_to_GCS():
    upload_tasks = []
    bq_table_tasks = []

    ensure_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_bq_dataset",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        if_exists='log',
        gcp_conn_id=GCP_CONN_ID,
    )

    for dataset, cfg in DATASETS.items():
        filename_prefix = cfg["filename_prefix"]
        bq_table = cfg["bq_table"]

        download = BashOperator(
            task_id=f"download_{dataset}",
            bash_command=f"""
            set -euo pipefail
            ym="{{{{ ds[:7] }}}}"
            fname="{filename_prefix}_${{ym}}.parquet"
            out="{DOWNLOAD_DIR}/${{fname}}"

            echo "Downloading ${{fname}} -> ${{out}}"
            wget -q -O "${{out}}" "{URL_PREFIX}/${{fname}}"
            """
        )

        upload = LocalFilesystemToGCSOperator(
        task_id=f"upload_{dataset}_to_gcs",
        src=f"{DOWNLOAD_DIR}/{filename_prefix}_{{{{ ds[:7] }}}}.parquet",
        dst=f"{GCS_PREFIX}/{dataset}/{filename_prefix}_{{{{ ds[:7] }}}}.parquet",
        bucket=BUCKET,
        gcp_conn_id=GCP_CONN_ID
        )

        create_external = BigQueryCreateTableOperator(
            task_id=f"ensure_bq_external_table_{dataset}",
            project_id=PROJECT_ID,
            dataset_id=BIGQUERY_DATASET,
            table_id=bq_table,
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": bq_table,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        f"gs://{BUCKET}/{GCS_PREFIX}/{dataset}/{filename_prefix}_*.parquet"
                    ],
                    "autodetect": True,
                },
            },
            gcp_conn_id=GCP_CONN_ID,
        )
       
        ensure_dataset >> download >> upload >> create_external

        upload_tasks.append(upload)
        bq_table_tasks.append(create_external)

    @task
    def cleanup_local_parquet(ds: str) -> None:
        ym = ds[:7]
        for _, cfg in DATASETS.items():
            p = Path(DOWNLOAD_DIR) / f"{cfg['filename_prefix']}_{ym}.parquet"
            p.unlink(missing_ok=True)

    cleanup = cleanup_local_parquet()
    for t in bq_table_tasks:
        t >> cleanup

download_parquet_and_upload_to_GCS()
    
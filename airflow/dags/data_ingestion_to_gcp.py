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

YELLOW_SCHEMA_FIELDS = [
    {"name" : "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name" : "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name" : "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name" : "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name" : "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name" : "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name" : "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name" : "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name" : "airport_fee", "type": "FLOAT", "mode": "NULLABLE"}
]

GREEN_SCHEMA_FIELDS = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
]


DATASETS = {
    "yellow": {
        "table": "yellow_taxi_trips",
        "filename_prefix": "yellow_tripdata",
        "bq_table": "yellow_external",
        "schema_fields": YELLOW_SCHEMA_FIELDS
    },
    "green": {
        "table": "green_taxi_trips",
        "filename_prefix": "green_tripdata",
        "bq_table": "green_external",
        "schema_fields": GREEN_SCHEMA_FIELDS
    },
}


@dag(
    dag_id="GCSUploadDag",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
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
                "schema": {
                    "fields" : cfg["schema_fields"]
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        f"gs://{BUCKET}/{GCS_PREFIX}/{dataset}/{filename_prefix}_*.parquet"
                    ],
                    "autodetect": False,
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
    
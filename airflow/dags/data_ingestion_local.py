from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import logging


from datetime import datetime
from pathlib import Path

PG_USER = "root2"
PG_PASSWORD = "root2"
PG_HOST = "pgdatabase"
PG_PORT = "5432"
PG_DB = "ny_taxi"

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = "/tmp"


DATASETS = {
    "yellow": {
        "table": "yellow_taxi_trips",
        "filename_prefix": "yellow_tripdata",
        "create_sql": """
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
          "VendorID" BIGINT,
          "tpep_pickup_datetime" TIMESTAMP,
          "tpep_dropoff_datetime" TIMESTAMP,
          "passenger_count" DOUBLE PRECISION,
          "trip_distance" DOUBLE PRECISION,
          "RatecodeID" DOUBLE PRECISION,
          "store_and_fwd_flag" TEXT,
          "PULocationID" BIGINT,
          "DOLocationID" BIGINT,
          "payment_type" BIGINT,
          "fare_amount" DOUBLE PRECISION,
          "extra" DOUBLE PRECISION,
          "mta_tax" DOUBLE PRECISION,
          "tip_amount" DOUBLE PRECISION,
          "tolls_amount" DOUBLE PRECISION,
          "improvement_surcharge" DOUBLE PRECISION,
          "total_amount" DOUBLE PRECISION,
          "congestion_surcharge" DOUBLE PRECISION,
          "airport_fee" DOUBLE PRECISION
        );
        """,
        "columns": [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ],
    },
    "green": {
        "table": "green_taxi_trips",
        "filename_prefix": "green_tripdata",
        "create_sql": """
        CREATE TABLE IF NOT EXISTS green_taxi_trips (
          "VendorID" BIGINT,
          "lpep_pickup_datetime" TIMESTAMP,
          "lpep_dropoff_datetime" TIMESTAMP,
          "store_and_fwd_flag" TEXT,
          "RatecodeID" DOUBLE PRECISION,
          "PULocationID" BIGINT,
          "DOLocationID" BIGINT,
          "passenger_count" DOUBLE PRECISION,
          "trip_distance" DOUBLE PRECISION,
          "fare_amount" DOUBLE PRECISION,
          "extra" DOUBLE PRECISION,
          "mta_tax" DOUBLE PRECISION,
          "tip_amount" DOUBLE PRECISION,
          "tolls_amount" DOUBLE PRECISION,
          "ehail_fee" DOUBLE PRECISION,
          "improvement_surcharge" DOUBLE PRECISION,
          "total_amount" DOUBLE PRECISION,
          "payment_type" DOUBLE PRECISION,
          "trip_type" DOUBLE PRECISION,
          "congestion_surcharge" DOUBLE PRECISION
        );
        """,
        "columns": [
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "store_and_fwd_flag",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "total_amount",
            "payment_type",
            "trip_type",
            "congestion_surcharge",
        ],
    },
}


def build_copy_sql(table: str, cols: list[str]) -> str:
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    return f'COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT csv);'


@dag(
        dag_id="LocalIngestionDag",
        start_date=datetime(2019, 1, 1),
        end_date=datetime(2020, 1, 1),
        schedule = "@monthly",
        catchup=True,
        max_active_runs=3,
        default_args={"retries": 3},
        tags=["nyc-tlc", "download", "postgres"],
)
def download_parquet_and_upload_to_postgres():

    load_paths = []
    for dataset, cfg in DATASETS.items():
        
        download = BashOperator(
            task_id=f"download_{dataset}",
            bash_command=f"""
            set -euo pipefail
            ym="{{{{ ds[:7] }}}}"
            fname="{cfg['filename_prefix']}_${{ym}}.parquet"
            out="/tmp/${{fname}}"

            echo "Downloading ${{fname}} -> ${{out}}"
            wget -q -O "${{out}}" "{{{{ params.url_prefix }}}}/${{fname}}"
            """,
            params={"url_prefix": URL_PREFIX},
        )

        @task(task_id=f"load_{dataset}")
        def load_dataset(ds: str, cfg=cfg):
            ym = ds[:7]
            parquet_path = f"/tmp/{cfg['filename_prefix']}_{ym}.parquet"

            import pyarrow.parquet as pq
            import io, csv, psycopg2, pandas as pd

            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )

            try:
                with conn:
                    with conn.cursor() as cur:
                        logging.info("Attempting to create SQL tables...")
                        cur.execute(cfg["create_sql"])
                        logging.info("SQL table created successfully.")
                        copy_sql = build_copy_sql(cfg["table"], cfg["columns"])

                        pf = pq.ParquetFile(parquet_path)
                        logging.info("Copying data into table..")
                        count=0
                        for batch in pf.iter_batches(batch_size=200_000):
                            df = batch.to_pandas()
                            df = df[cfg["columns"]]

                            buf = io.StringIO()
                            df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
                            buf.seek(0)
                            cur.copy_expert(copy_sql, buf)
                            logging.info(f"Batch {count} complete.")
                            count +=1
            finally:
                conn.close()

            return parquet_path

        load_dataset_task = load_dataset()
        download >> load_dataset_task
        load_paths.append(load_dataset_task)
   
    @task
    def cleanup(paths: list[str]):
        for p in paths:
            Path(p).unlink(missing_ok=True)


    #parquet_path = load_to_postgres()
    cleanup(load_paths)

    #download >> parquet_path

download_parquet_and_upload_to_postgres()

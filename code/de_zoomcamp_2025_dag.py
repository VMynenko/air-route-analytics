from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import kagglehub
import os
from google.cloud import bigquery

GCP_CONN_ID = "google_cloud_default"
DATASET_NAME = Variable.get("dataset_name")
BUCKET_NAME = Variable.get("bucket_name")
TABLE_ID = Variable.get("table_id")

def create_bucket(bucket_name, gcp_conn_id=GCP_CONN_ID):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.create_bucket(bucket_name=bucket_name)


def upload_file(bucket_name, source_file, gcp_conn_id=GCP_CONN_ID):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    destination_blob = os.path.basename(source_file)
    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob,
        filename=source_file
    )
    return f"gs://{bucket_name}/{destination_blob}"


def load_gcs_to_bq(gcs_uri, table_id, gcp_conn_id=GCP_CONN_ID):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    bigquery_client = hook.get_client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True
    )
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()


def pipeline():
    dataset_path = kagglehub.dataset_download(DATASET_NAME)
    source_file = os.path.join(dataset_path, os.listdir(dataset_path)[0])
    create_bucket(BUCKET_NAME)
    gcs_uri = upload_file(BUCKET_NAME, source_file)
    load_gcs_to_bq(gcs_uri, TABLE_ID)


def run_pipeline():
    pipeline()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 28),
    'retries': 1
}


dag = DAG(
    'kaggle_to_bigquery',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag
)
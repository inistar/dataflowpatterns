import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 2, 14),
    'email': ['ini.sathiamurthi@sprigml.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

schedule_interval = "00 10 * * *"

dag = DAG(
    'gcstobigquery_airflow',
    default_args=default_args,
    schedule_interval=schedule_interval
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "dataflowtesting-218212"
BQ_DATASET = "testing"

t1 = GoogleCloudStorageObjectSensor(
    task_id='gcs_sensor_trigger',
    bucket='dataflow-results-ini',
    object='gs://dataflow-results-ini/airflow-tesing/test.csv',
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

t2 = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='dataflow-results-ini',
    source_objects=['airflow-testing/test.csv'],
    schema_object='airflow-testing/test.json',
    destination_project_dataset_table="dataflowtesting-218212.testing.newone",
    write_disposition='WRITE_APPEND',
    autodetect=True,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

# t1 >> t2
# t1
t2
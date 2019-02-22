import json
import pendulum
from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 2, 16),
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
    # schedule_interval=schedule_interval
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "dataflowtesting-218212"
BQ_DATASET = "testing"

def new_ts_function(context):
    # print("initest : " + str(context.get("execution_date")))
    # print("initest : " + type(context.get("execution_date")))
    # print("initest : " + type(context.get("dag").schedule_interval))

    test = pendulum.datetime(2019, 2, 16, 10, 0, 0)
    print("initest Execution_date: " + str(context['execution_date']) + " Schedule_interval: " + str(context['dag'].schedule_interval))
    return context.get("execution_date") + test#context['dag'].schedule_interval

# def execute(self, context):
#     execution_date = context.get("execution_date")
t1 = GoogleCloudStorageObjectSensor(
    task_id='gcs_sensor_trigger',
    bucket='dataflow-results-ini',
    object='gs://dataflow-results-ini/airflow-tesing/test.csv',
    google_cloud_conn_id='gcs_conn',
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

t1 >> t2
# t1
# t2
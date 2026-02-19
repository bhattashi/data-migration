from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # Why this added?
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- CONFIGURATION ---
PROJECT_ID = 'project-6d37e6ba-d918-463b-93a'
REGION = 'us-central1'
BUCKET_NAME = 'data_mgrt1'
TEMPLATE_PATH = f'gs://{BUCKET_NAME}/daily_etl_template.json'
BQ_TABLE = f'{PROJECT_ID}:loans_migration.loans_tran'

# The file we are waiting for (using Airflow macro for daily date)
# Example: gs://bucket/landing/transactions_2023-10-27.csv
TARGET_OBJECT = 'input/sample_transactions.csv'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_etl_orch_with_sensor',
    default_args=default_args,
    description='Reactive Migration Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. THE SENSOR: Wait for the file to arrive in GCS
    wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_incoming_csv',
        bucket=BUCKET_NAME,
        object=TARGET_OBJECT,
        google_cloud_conn_id='google_cloud_default',
        timeout=60 * 60 * 12, # Timeout after 12 hours
        poke_interval=300,    # Check every 5 minutes (300 seconds)
        mode='reschedule'     # 'reschedule' releases the slot while sleeping (saves $!!)
    )

    # 2. THE DATAFLOW JOB (Only runs if Sensor succeeds)
    launch_template = DataflowTemplatedJobStartOperator(
        task_id='execute_etl_template',
        template=TEMPLATE_PATH,
        parameters={
            'input_file': f'gs://{BUCKET_NAME}/{TARGET_OBJECT}',
            'output_table': BQ_TABLE
        },
        location=REGION,
        wait_for_result=True
    )

    # Dependency Flow
    wait_for_file >> launch_template

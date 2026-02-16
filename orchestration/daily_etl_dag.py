# orchestration/dataflow_dag.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

with DAG(
    'daily_etl_orchestration',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    launch_template = DataflowTemplatedJobStartOperator(
        task_id='execute_etl_template',
        template='gs://data_mgrt1/daily_etl_template.json',
        location='us-central1',
        parameters={
            'input_file': 'gs://data_mgrt1/input/sample_transactions.csv',
            'output_table': 'project-6d37e6ba-d918-463b-93a:loans_migration.loans_tran'
        }
    )

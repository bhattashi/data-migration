# ------- Hive to BQ demo n reference data migration ------ final DAG ----- #

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
# from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
# from airflow.providers.http.sensors.http import HttpSensor
import requests
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

# --- CONFIGURATION ---
PROJECT_ID = "project-6d37e6ba-d918-463b-93a"
DATASET_RAW = "raw_zone"        # External Tables
DATASET_FINAL = "final_zone"    # Native Partitioned Tables
DATASET_AUDIT = "audit_zone"    # Validation History
TABLES = ["loans_ac"] ## TABLES = ["loans_ac", "custmers", "products", "inventory"] # Add all 25 here

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "hdfs_to_bq_production_pipeline",
    default_args=default_args,
    description="Fault-isolated Hive to BQ Migration with Auditing",
    schedule_interval="0 0 1 * *",  # Monthly Full Load
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["migration", "hive", "bq"],
) as dag:

    # Loop through each table to create a parallel "Lane"
    for table in TABLES:
        with TaskGroup(group_id=f"process_{table}") as table_group:
            
            # 1. We submit a tiny "ls" command to Dataproc
            check_file_job = DataprocSubmitJobOperator(
                task_id="check_hdfs_file_via_api",
                project_id="project-6d37e6ba-d918-463b-93a",
                region="us-central1",
                job={
                    "reference": {"project_id": "project-6d37e6ba-d918-463b-93a"},
                    "placement": {"cluster_name": "cluster-data-migration"},
                    "hadoop_job": {
                        "main_class": "org.apache.hadoop.fs.FsShell",
                        "args": ["-ls", "/user/data/demo_migration/loans_ac/_SUCCESS"],
                    },
                },
            )

            # 2. TRANSFER: HDFS to GCS (Individual Job for Fault Isolation)
            transfer_to_gcs = CloudDataTransferServiceRunJobOperator(
                task_id="transfer_to_gcs",
                job_name="transferJobs/14508589021579537930",
                project_id=PROJECT_ID
            )

            # 3. EXTERNAL TABLE: Create Metadata Link
            create_ext_table = BigQueryInsertJobOperator(
                task_id="create_external_table",
                configuration={
                    "query": {
                        "query": f"""
                            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_RAW}.{table}_ext`
                            OPTIONS (
                                format = 'PARQUET',
                                uris = ['gs://hive-bq-demo-data-migration/{table}/*.parquet']
                            );
                        """,
                        "useLegacySql": False,
                    }
                }
            )            

            # Task Dependencies within the Group
            check_file_job >> transfer_to_gcs >> create_ext_table

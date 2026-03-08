# ------- Hive to BQ demo n reference data migration ------ final DAG ----- #

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

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
            
            # 1. SENSOR: Wait for Dataproc HDFS flag
            wait_for_data = HdfsSensor(
                task_id="wait_for_hdfs_success",
                filepath=f"/user/data/demo_migration/{table}/_SUCCESS",
                hdfs_conn_id="dataproc_hdfs_default",
                ## hdfs_conn_id="on_prem_cdp_hdfs", # Points to the new WebHDFS connection # Airflow UI (Admin > Connections)
                poke_interval=60,
                mode="reschedule"
            )

            # 2. TRANSFER: HDFS to GCS (Individual Job for Fault Isolation)
            transfer_to_gcs = CloudDataTransferServiceRunJobOperator(
                task_id="transfer_to_gcs",
                job_name=f"projects/{PROJECT_ID}/transferJobs/15910871444372576019",
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
            wait_for_data >> transfer_to_gcs >> create_ext_table
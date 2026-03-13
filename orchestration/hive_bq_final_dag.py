# ------- Hive to BQ demo n reference data migration ------ final DAG ----- #

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import CloudDataTransferServiceJobStatusSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from datetime import datetime
# from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
import requests
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

# --- CONFIGURATION ---
PROJECT_ID = "project-6d37e6ba-d918-463b-93a"
DATASET_RAW = "raw"        # External Tables
DATASET_FINAL = "final"    # Native Partitioned Tables
DATASET_AUDIT = "audit"    # Validation History
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
			# ----- BQ Native table SQL ------
            native_load_sql = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_FINAL}.{table}`
PARTITION BY ingestion_date
AS
SELECT *, CURRENT_DATE() AS ingestion_date
FROM `{PROJECT_ID}.{DATASET_RAW}.{table}_ext`
"""
            
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

            # 3. This sensor polls the STS API to see if the job has finished successfully
            wait_for_transfer = CloudDataTransferServiceJobStatusSensor(
                task_id="wait_for_transfer",
                # Note: Use the same job_name format as the RunJobOperator
                job_name="transferJobs/14508589021579537930",
                project_id=PROJECT_ID,
                # 'SUCCEEDED' is the status for a successful STS operation
                expected_statuses={"SUCCEEDED","SUCCESS"},
                poke_interval=60,  # Check every 1 minute
                timeout=3600,      # Give up after 1 hour
                mode="reschedule"  # 'reschedule' releases worker slots while waiting
            )

            # 4. EXTERNAL TABLE: Create Metadata Link
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

			# # 5. Load data into Native BigQuery Table
   #          load_native_table = BigQueryInsertJobOperator(
   #              task_id="load_native_table",
   #              configuration={
   #                  "query": {
   #                      "query": native_load_sql,
   #                      "useLegacySql": False,
   #                  }
   #              }
   #          )

			# 5. Load data into Native Partitioned BigQuery Table
			# Get today's date in YYYYMMDD format for the partition decorator
            ds_nodash = datetime.now().strftime('%Y%m%d')
            load_native_table = BigQueryInsertJobOperator(
                task_id=f"load_native_table_{table}",
                configuration={
                    "query": {
                        "query": f"SELECT *, CURRENT_DATE() as ingestion_date FROM `{PROJECT_ID}.{DATASET_RAW}.{table}_ext` ",
						"destinationTable": {
							"projectId": PROJECT_ID,
							"datasetId": DATASET_FINAL,
							"tableId": f"{table}${ds_nodash}" # The '$' targets only today's partition
						},
						"writeDisposition": "WRITE_TRUNCATE", # Replaces ONLY today's partition
						"createDisposition": "CREATE_IF_NEEDED",
						"timePartitioning": {
							"type": "DAY",
							"field": "ingestion_date"
						},
                        "useLegacySql": False,
                    }
                }
            )

			# 6. VALIDATION: Ext vs Native table count check before raw files clean-up
            validate_migration = BigQueryCheckOperator(
                task_id=f"validate_migration_{table}",
                sql=f"""
				SELECT
				(SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_FINAL}.{table}`) =
				(SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_RAW}.{table}_ext`)
				""",
				use_legacy_sql=False,
            )

			# 7. CLEANUP: Delete GCS Parquet files after successful BQ load
            cleanup_gcs_raw_data = GCSDeleteObjectsOperator(
                task_id="cleanup_gcs_raw_data",
                bucket_name="hive-bq-demo-data-migration",
				# This deletes everything inside the folder for that specific table
				prefix=f"{table}/",
            )
			
            # Task Dependencies within the Group
            check_file_job >> transfer_to_gcs >> wait_for_transfer >> create_ext_table >> load_native_table >> validate_migration >> cleanup_gcs_raw_data

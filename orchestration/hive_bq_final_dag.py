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
from airflow.operators.email import EmailOperator

# --- CONFIGURATION ---
PROJECT_ID = "project-6d37e6ba-d918-463b-93a"
DATASET_RAW = "raw"        # External Tables
DATASET_FINAL = "final"    # Native Partitioned Tables
DATASET_AUDIT = "audit"    # Validation History
TABLES = ["loans_ac"] ## TABLES = ["loans_ac", "custmers", "products", "inventory"] # Add all 25 here

# This Jinja logic picks the manually triggered date OR the daily scheduled date (ds)
# The 'ds_nodash' version (YYYYMMDD) is required for the BQ $ decorator
target_date = "{{ dag_run.conf.get('manual_date', ds) }}"
partition_suffix = "{{ dag_run.conf.get('manual_date', ds) | replace('-', '') }}"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
	'email': ['shalaka.bhatt@gmail.com'],
    'email_on_failure': True,
	'email_on_retry': True,  # This alerts you on every retry attempt
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
                task_id=f"check_hdfs_file_via_api_{table}",
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
                task_id=f"transfer_to_gcs_{table}",
                job_name="transferJobs/14508589021579537930",
                project_id=PROJECT_ID
            )

            # 3. This sensor polls the STS API to see if the job has finished successfully
            wait_for_transfer = CloudDataTransferServiceJobStatusSensor(
                task_id=f"wait_for_transfer_{table}",
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
                task_id=f"create_external_table_{table}",
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
			
			# 5. Load data into Native Partitioned BigQuery Table
			# Get today's date in YYYYMMDD format for the partition decorator
            load_native_table = BigQueryInsertJobOperator(
                task_id=f"load_native_table_{table}",
                configuration={
                    "query": {
                        "query": f"SELECT *, DATE('{target_date}') as ingestion_date FROM `{PROJECT_ID}.{DATASET_RAW}.{table}_ext` ",
						"destinationTable": {
							"projectId": PROJECT_ID,
							"datasetId": DATASET_FINAL,
							"tableId": f"{table}${partition_suffix}" # The '$' targets only today's partition
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
				(SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_FINAL}.{table}` WHERE ingestion_date = DATE('{target_date}')) =
				(SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_RAW}.{table}_ext`)
				""",
				use_legacy_sql=False,
            )

			# 7. CLEANUP GCS: (Destructive - runs ONLY if validation passes)
            cleanup_gcs_raw_data = GCSDeleteObjectsOperator(
                task_id=f"cleanup_gcs_raw_data_{table}",
                bucket_name="hive-bq-demo-data-migration",
				# This deletes everything inside the folder for that specific table
				prefix=f"{table}/",
            )

			# 8. Success Notification Task
		    send_success_email = EmailOperator(
		        task_id=f"notify_success_{table}",
		        to='shalaka.bhatt@gmail.com',
		        subject=f"✅ Migration Successful: {table} ({target_date})",
		        html_content=f"""
		            <h3>Table Migration Complete</h3>
		            <p><b>Table:</b> {table}</p>
		            <p><b>Date:</b> {target_date}</p>
		            <p><b>Status:</b> All steps (Transfer, Load, Validation, Cleanup) passed successfully.</p>
		        """
		    )
			
            # Task Dependencies within the Group
            (
				check_file_job >> transfer_to_gcs >> wait_for_transfer >> create_ext_table >> load_native_table >> validate_migration >> 
				cleanup_gcs_raw_data >> send_success_email
			)

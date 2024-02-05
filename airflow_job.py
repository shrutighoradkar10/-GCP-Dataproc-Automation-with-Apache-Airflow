from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

from airflow.utils.dates import days_ago


PROJECT_ID = "peaceful-tome-408415"
BUCKET_1 =  "us-central1-airflow-be3c43e3-bucket"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'@daily',
}

dag = DAG('Gcp_Dataproc_Pyspark_dag',
          default_args=default_args,
          catchup=False,
          start_date=days_ago(1), 
          tags=['example'],
          description='A DAG to run Spark job on Dataproc',)

# Define cluster config
CLUSTER_NAME = 'computer1'
REGION = 'asia-south1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.0-debian10'  # Image version
    }
}


#GCSObjectExistenceSensor task

gcs_object_exists = GCSObjectsWithPrefixExistenceSensor(
           task_id="gcs_objects_exist_task",
            bucket=BUCKET_1,
            prefix="Airflow_Ass1/Daily_CSV/health_data_",  # Prefix for health_data_ files
            mode='poke',
            timeout=43200,
            poke_interval=300,
            dag=dag,
)


create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)



    
pyspark_job = {
    'main_python_file_uri': 'gs://us-central1-airflow-be3c43e3-bucket/Pyspark_Script.py'
}



submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)

gcs_object_exists >> create_cluster
create_cluster>>submit_pyspark_job 
submit_pyspark_job>> delete_cluster
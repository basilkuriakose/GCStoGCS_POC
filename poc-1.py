from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

default_args = {
    'depends_on_past': False   
}

CLUSTER_NAME = 'demo-airflow-cluster'
REGION='asia-northeast1'
PROJECT_ID='maximal-valve-330811'


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 256},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 256},
    }
}
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
    "jar_file_uris": ["gs://equifax-poc-3/poc-1_2.11-0.1.jar"],
    "main_class": "poc_1",
    },
}



with DAG(
    'dataproc-POC-1',
    default_args=default_args,
    description='create a Dataproc workflow',
    schedule_interval=None,
    start_date = days_ago(2)
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    create_cluster >>spark_task >>delete_cluster
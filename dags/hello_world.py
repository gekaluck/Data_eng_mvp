"""
Hello World DAG — M1 verification.

Two tasks that prove the local infrastructure works:
1. print_hello: confirms the scheduler can execute a task
2. test_minio_connection: confirms Airflow can reach MinIO and the three buckets exist
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello(**context):
    """Simple task to prove the scheduler is working."""
    logical_date = context["logical_date"]
    print(f"Hello from Airflow! logical_date = {logical_date}")


def test_minio_connection():
    """
    Connect to MinIO via boto3 and verify that the expected buckets exist.
    boto3 is imported inside the function body — this is an Airflow best practice
    so the import doesn't slow down DAG parsing for every scheduler heartbeat.
    """
    import boto3

    # Connect to MinIO using the same credentials as docker-compose
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print(f"Found buckets: {buckets}")

    expected = {"bronze", "silver", "gold"}
    missing = expected - set(buckets)
    if missing:
        raise ValueError(f"Missing buckets: {missing}")

    print("All expected buckets exist — MinIO connection verified!")


with DAG(
    dag_id="hello_world",
    description="M1 verification: proves Airflow scheduler and MinIO connection work",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # manual trigger only
    catchup=False,
    tags=["m1", "verification"],
) as dag:

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    t2 = PythonOperator(
        task_id="test_minio_connection",
        python_callable=test_minio_connection,
    )

    t1 >> t2  # run in sequence: print hello, then verify MinIO

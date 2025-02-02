from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random
import logging

# Function to simulate work with random sleep intervals
def task_function(task_id, **kwargs):
    sleep_time = random.randint(7, 13)  # Random sleep between 7 and 13 seconds
    logging.info(f"Starting task {task_id}. Sleeping for {sleep_time} seconds.")
    time.sleep(sleep_time)
    if task_id == "task_2":  # Simulate a failure in one task
        raise ValueError(f"Task {task_id} failed!")
    logging.info(f"Finished task {task_id}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Start date for backfilling
    'retries': 1,  # Retry failed tasks once
    'retry_delay': timedelta(minutes=1),  # Wait 1 minute before retrying
}

# Define the DAG with backfilling enabled
with DAG(
    dag_id='concurrent_tasks_with_backfill',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=True,  # Enable backfilling
    max_active_tasks=10,  # Allow up to 10 tasks to run concurrently
    concurrency=10,  # Allow up to 10 tasks per DAG run
) as dag:

    # Create multiple independent tasks
    for i in range(10):
        task = PythonOperator(
            task_id=f'task_{i}',
            python_callable=task_function,
            op_kwargs={'task_id': f'task_{i}'},
        )
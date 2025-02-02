from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def process_historical_data(execution_date, **kwargs):
    print(f"Processing historical data for {execution_date}")

# Default arguments for backfilling DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),  # Start from an earlier date for backfilling
    'retries': 1,
    'depends_on_past': False
}

# Define the DAG with catchup enabled for backfilling
with DAG(
    dag_id='backfill_historical_data_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Runs for each historical day
    catchup=True  # Allows execution for past dates
) as dag:
    
    task_1 = PythonOperator(
        task_id='process_historical_data',
        python_callable=process_historical_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    task_1

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.process_historical import process_historical_csv

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_historical_load():
    db_url = os.getenv("ANALYTICS_DB_URL", "postgresql://analytics_user:analytics_pass@postgres-analytics:5432/analytics_db")
    csv_file = "/opt/airflow/data/historical/online_retail.csv"
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Archivo histórico no encontrado en {csv_file}")
    process_historical_csv(csv_file, db_url)

with DAG(
    dag_id="dag_historical_kaggle_ingestion",
    default_args=default_args,
    schedule_interval=None,  # Solo ejecución manual / bajo demanda
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    load_kaggle_data = PythonOperator(
        task_id="process_kaggle_retail_dataset",
        python_callable=run_historical_load
    )
import json
import os
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.transformers import process_daily_events

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

EXCHANGE_RATES = {
    "USD": 1.0,
    "MXN": 0.055,
    "BRL": 0.18,
    "COP": 0.00024,
    "ARS": 0.0010,
    "CLP": 0.0011
}

def run_daily_pipeline(**context):
    batch_date = "2026-10-25"
    batch_id = f"BATCH_DAILY_{batch_date.replace('-', '')}"
    input_file = f"/opt/airflow/data/events_{batch_date.replace('-', '')}.jsonl"
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Archivo de eventos diarios no encontrado: {input_file}")

    with open(input_file, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    valid_events, quarantine, metrics = process_daily_events(
        event_rows=raw_lines,
        batch_id=batch_id,
        batch_date=batch_date,
        rates=EXCHANGE_RATES
    )

    db_url = os.getenv("ANALYTICS_DB_URL", "postgresql://analytics_user:analytics_pass@postgres-analytics:5432/analytics_db")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        # 1. Insertar en cuarentena
        if quarantine:
            insert_q_sql = """
                INSERT INTO analytical.quarantine_records 
                (source_name, record_identifier, rejection_reason, raw_payload, batch_id)
                VALUES %s
            """
            q_values = [
                (q["source_name"], q["record_identifier"], q["rejection_reason"], q["raw_payload"], q["batch_id"])
                for q in quarantine
            ]
            psycopg2.extras.execute_values(cur, insert_q_sql, q_values, page_size=5000)

        # 2. Insertar hechos con idempotencia (ON CONFLICT DO NOTHING)
        if valid_events:
            insert_fact_sql = """
                INSERT INTO analytical.fact_order_events
                (event_id, order_id, event_type, event_time, country, currency, 
                 amount, amount_usd, is_late, batch_id, record_hash)
                VALUES %s
                ON CONFLICT (record_hash) DO NOTHING
            """
            fact_values = [
                (
                    e["event_id"], e["order_id"], e["event_type"], e["event_time"],
                    e["country"], e["currency"], e["amount"], e["amount_usd"],
                    e["is_late"], e["batch_id"], e["record_hash"]
                )
                for e in valid_events
            ]
            psycopg2.extras.execute_values(cur, insert_fact_sql, fact_values, page_size=5000)

        # 3. Registrar auditoría del lote
        cur.execute("""
            INSERT INTO analytical.etl_batch_audit 
            (batch_id, batch_date, source_name, rows_read, rows_valid, rows_rejected, rows_duplicate, rows_late, rows_published, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET
                rows_read = EXCLUDED.rows_read,
                rows_valid = EXCLUDED.rows_valid,
                rows_rejected = EXCLUDED.rows_rejected,
                rows_duplicate = EXCLUDED.rows_duplicate,
                rows_late = EXCLUDED.rows_late,
                rows_published = EXCLUDED.rows_published,
                status = EXCLUDED.status;
        """, (
            batch_id, batch_date, 'daily_events', metrics["rows_read"], metrics["rows_valid"],
            metrics["rows_rejected"], metrics["rows_duplicate"], metrics["rows_late"],
            metrics["rows_valid"], 'SUCCESS'
        ))

        conn.commit()
        print(f"Lote {batch_id} procesado con éxito: {metrics}")
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id='dag_daily_orders_pipeline',
    default_args=default_args,
    description='Pipeline diario incremental de eventos e idempotencia',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    process_task = PythonOperator(
        task_id='process_daily_events_task',
        python_callable=run_daily_pipeline
    )
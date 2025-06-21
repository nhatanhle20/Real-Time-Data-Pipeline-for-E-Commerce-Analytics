from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def check_order_activity():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM transaction_data
        WHERE timestamp >= CURRENT_DATE;
    """)
    count = cur.fetchone()[0]
    if count == 0:
        raise Exception("No orders recorded today!")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    # 'email': ['your_email@gmail.com'],
    # 'email_on_failure': True,
}

dag = DAG('check_order_activity', default_args=default_args, schedule_interval='@daily', catchup=False)

task = PythonOperator(
    task_id='check_order_activity',
    python_callable=check_order_activity,
    dag=dag
)

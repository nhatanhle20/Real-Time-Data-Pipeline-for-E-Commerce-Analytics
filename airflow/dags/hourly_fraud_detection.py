from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import psycopg2

def check_large_orders(**context):
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()
    cur.execute("""
        SELECT id, user_id, quantity, timestamp FROM transaction_data
        WHERE quantity > 100 AND timestamp >= CURRENT_DATE;
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    if results:
        alert_body = "<br>".join([
            f"Order ID: {row[0]}, User ID: {row[1]}, Quantity: {row[2]}, Time: {row[3]}"
            for row in results
        ])
        context['ti'].xcom_push(key='fraud_alert_body', value=alert_body)
        return True
    else:
        context['ti'].xcom_push(key='fraud_alert_body', value="No suspicious orders detected today.")
        return False

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['lnafncy20@gmail.com'],
    'email_on_failure': True,
}

dag = DAG('fraud_detection_alert', default_args=default_args, schedule_interval='@hourly', catchup=False)

check_orders_task = PythonOperator(
    task_id='check_large_orders',
    python_callable=check_large_orders,
    provide_context=True,
    dag=dag
)

send_alert_task = EmailOperator(
    task_id='send_fraud_alert',
    to='lnafncy20@gmail.com',
    subject="Fraud Alert",
    html_content="{{ ti.xcom_pull(task_ids='check_large_orders', key='fraud_alert_body') }}",
    dag=dag,
    trigger_rule='all_success'
)

check_orders_task >> send_alert_task
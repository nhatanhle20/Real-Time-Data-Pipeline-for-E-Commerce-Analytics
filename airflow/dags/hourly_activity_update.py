from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import psycopg2

def get_order_count():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM transaction_data
        WHERE timestamp >= CURRENT_DATE;
    """)
    order = cur.fetchone()[0]
    cur.close()
    conn.close()
    return order

def get_user_count():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM users
        WHERE last_updated >= CURRENT_DATE;
    """)
    user = cur.fetchone()[0]
    cur.close()
    conn.close()
    return user

def prepare_email(**context):
    order = get_order_count()
    user = get_user_count()
    subject = "Hourly Activity Update"
    body = f"New total orders today: {order} <br> New user registrations today: {user}"
    context['ti'].xcom_push(key='email_subject', value=subject)
    context['ti'].xcom_push(key='email_body', value=body)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['lnafncy20@gmail.com'],
    'email_on_failure': True,
}

dag = DAG('hourly_order_update', default_args=default_args, schedule_interval='@hourly', catchup=False)

prepare_email_task = PythonOperator(
    task_id='prepare_email',
    python_callable=prepare_email,
    provide_context=True,
    dag=dag
)

send_email_task = EmailOperator(
    task_id='send_email',
    to='lnafncy20@gmail.com', 
    subject="{{ ti.xcom_pull(task_ids='prepare_email', key='email_subject') }}",
    html_content="{{ ti.xcom_pull(task_ids='prepare_email', key='email_body') }}",
    dag=dag
)

prepare_email_task >> send_email_task
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def export_to_sheets():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            CURRENT_DATE AS date,
            COUNT(DISTINCT order_id) AS orders,
            SUM(quantity) AS total_quantity,
            SUM(price * quantity) AS total_revenue,
            COUNT(DISTINCT user_id) AS users
        FROM transaction_data
        WHERE timestamp >= CURRENT_DATE;
    """)
    data = cur.fetchone()
    conn.close()

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/dags/credentials.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("Daily Ecommerce Summary").sheet1

    sheet.append_row([str(data[0]), data[1], data[2], float(data[3]), data[4]])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('daily_summary_export', default_args=default_args, schedule_interval='@daily', catchup=False)

task = PythonOperator(
    task_id='export_summary_to_gsheet',
    python_callable=export_to_sheets,
    dag=dag
)

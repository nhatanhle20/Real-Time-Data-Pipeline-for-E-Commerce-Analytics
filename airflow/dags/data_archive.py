from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def archive_old_data():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cur = conn.cursor()

    # Move to archive table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS archived_transaction_data (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            user_name VARCHAR(255) NOT NULL,
            user_email VARCHAR(255) NOT NULL,
            street VARCHAR(500) NOT NULL,
            city VARCHAR(255) NOT NULL,
            state VARCHAR(255) NOT NULL,
            postal_code VARCHAR(20) NOT NULL,
            country VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            category VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            shipping_method VARCHAR(100) NOT NULL,
            payment_method VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO archived_transaction_data
        SELECT * FROM transaction_data
        WHERE timestamp < NOW() - INTERVAL '30 days';
    """)

    cur.execute("""
        DELETE FROM transaction_data
        WHERE timestamp < NOW() - INTERVAL '30 days';
    """)

    conn.commit()
    cur.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('data_archive', default_args=default_args, schedule_interval='@daily', catchup=False)

task = PythonOperator(
    task_id='data_archive',
    python_callable=archive_old_data,
    dag=dag
)

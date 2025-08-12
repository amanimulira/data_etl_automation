from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('daily_etl', schedule_interval='0 6 * * *', start_date=...) as dag:
    ingest = PythonOperator(task_id='ingest', python_callable=...)
    transform = PythonOperator(task_id='transform', python_callable=...)
    load = PythonOperator(task_id='load', python_callable=...)

    ingest >> transform >> load
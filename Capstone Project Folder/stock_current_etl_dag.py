from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from helpers import current_stocks_etl, current_news_etl, data_quality_check 
import s3fs


default_args = {
    'owner': 'arnaud',
    'start_date': datetime(2020, 5, 27),
    'depends_on_past': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#set DAG run at 5:30 AM everyday
dag=DAG('stock_current_etl_dag', default_args=default_args, schedule_interval='@daily')

start_task = DummyOperator(task_id='dummy_start', dag=dag)

upload_current_news_to_S3_task = PythonOperator(
        task_id='upload_current_news_to_S3',
        python_callable=current_news_etl,
        dag=dag)

upload_current_stock_to_S3_task = PythonOperator(
        task_id='upload_current_stock_to_S3',
        python_callable=current_stocks_etl,
        op_kwargs={
                'list_of_stocks': ['AAPL', 'INTC', 'TSLA', 'GILD', 'BA', 'AMZN','CBB', 'DAL', 'MMM', 'MSFT'],
		'ndays': 30
            },
        dag=dag)

data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        op_kwargs={
                'folder': "current/2020.05.28/stocks",
		'n_files': 10
            },
        dag=dag)

end_task = DummyOperator(task_id='Stop_execution',  dag=dag)


# Use arrows to set dependencies between tasks
start_task >> [upload_current_news_to_S3_task, upload_current_stock_to_S3_task]
[upload_current_news_to_S3_task, upload_current_stock_to_S3_task] >> data_quality_check >> end_task 

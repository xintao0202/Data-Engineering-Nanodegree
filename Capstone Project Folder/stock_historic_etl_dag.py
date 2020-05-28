from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from helpers import upload_file_to_S3_with_hook, save_stock_s3, load_historic_process_save_s3
import s3fs


default_args = {
    'owner': 'arnaud',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

# Using the context manager alllows you not to duplicate the dag parameter in each operator
dag=DAG('stock_historic_etl_dag', default_args=default_args, schedule_interval='@once')

start_task = DummyOperator(task_id='dummy_start', dag=dag)

upload_historic_news_to_S3_task = PythonOperator(
        task_id='upload_historic_news_to_S3',
        python_callable=upload_file_to_S3_with_hook,
        op_kwargs={
               'filename': '/root/airflow/dags/download/stocknews.zip',
               'key': 'raw-historic-data/stocknews.zip',
               'bucket_name': 'stock.etl',
            },
        dag=dag)

upload_historic_pricing_to_S3_task = PythonOperator(
        task_id='upload_historic_pricing_to_S3',
        python_callable=upload_file_to_S3_with_hook,
        op_kwargs={
               'filename': '/root/airflow/dags/download/price-volume-data-for-all-us-stocks-etfs.zip',
               'key': 'raw-historic-data/stockpricing.zip',
               'bucket_name': 'stock.etl',
            },
        dag=dag)

upload_company_info_to_S3_task = PythonOperator(
        task_id='upload_company_info_to_S3',
        python_callable=upload_file_to_S3_with_hook,
        op_kwargs={
               'filename': '/root/airflow/dags/download/robinhood.zip',
               'key': 'raw-historic-data/companyinfo.zip',
               'bucket_name': 'stock.etl',
            },
        dag=dag)

load_historic_process_save_to_S3_task = PythonOperator(
        task_id='load_historic_process_save_to_S3',
        python_callable=load_historic_process_save_s3,
        op_kwargs={
               'key_company': "raw-historic-data/companyinfo.zip",
               'key_news': 'raw-historic-data/stocknews.zip',
               'key_stock': "raw-historic-data/stockpricing.zip",
            },
        dag=dag)

end_task = DummyOperator(task_id='Stop_execution',  dag=dag)


# Use arrows to set dependencies between tasks
start_task >> [upload_historic_news_to_S3_task, upload_historic_pricing_to_S3_task, upload_company_info_to_S3_task]
[upload_historic_news_to_S3_task, upload_historic_pricing_to_S3_task, upload_company_info_to_S3_task] >> load_historic_process_save_to_S3_task
load_historic_process_save_to_S3_task >> end_task 

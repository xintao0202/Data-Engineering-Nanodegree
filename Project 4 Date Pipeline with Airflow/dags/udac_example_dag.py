from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #Run once an hour at the beginning of the hour cron "0 * * * *"
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
task_id="create_tables",
dag=dag,
sql='create_tables.sql',
postgres_conn_id="redshift"
)

# The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.

# StageToRedshiftOperator is defined in /plugins/operators/stage_redshift.py
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_format = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query="songplay_table_insert",
    # Fact tables are usually so massive that they should only allow append type functionality
    append_data=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query="user_table_insert",
    # Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query="song_table_insert",
    # Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query="artist_table_insert",
    # Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query="time_table_insert",
    # Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql':'SELECT COUNT(*) FROM songplays;', 'expected_result': 0, 'method': 'greater_than'},
        {'check_sql':'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;', 'expected_result':0, 'method': 'equal'},
        {'check_sql':'SELECT COUNT(*) FROM artists;', 'expected_result': 0, 'method': 'greater_than'},
        {'check_sql':'SELECT COUNT(*) FROM songs WHERE songid IS NULL;', 'expected_result':0, 'method': 'equal'},
        {'check_sql':'SELECT COUNT(*) FROM users;', 'expected_result': 0, 'method': 'greater_than'},
        {'check_sql':'SELECT COUNT(*) FROM time WHERE start_time IS NULL;', 'expected_result':0, 'method': 'equal'}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# define dependencies
# load fact table
start_operator >> create_tables_task
create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
# load dimension table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
# run quality checks
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
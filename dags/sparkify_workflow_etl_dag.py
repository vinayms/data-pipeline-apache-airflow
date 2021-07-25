from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner':'udacity',
    'start_date':datetime(2020,7,1),
    'depends_on_past':False,
    'retries':3,
    'max_active_runs':1,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'catchup':False
}

dag = DAG('udacity_arflow_project_1',
          default_args=default_args,
          description='ETL Redshift with Airflow',
          schedule_interval='@hourly'
         )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    table = "staging_events",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    table = "staging_songs",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto"
)

load_songplays_fact_table= LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    conn_id="redshift",
    table="songplays",
    sql="songplay_table_insert",
    append_only=False
)

load_songs_dimension_table= LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    conn_id="redshift",
    table="songs",
    sql="song_table_insert",
    append_only=False
)

load_user_dimension_table= LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    conn_id="redshift",
    table="users",
    sql="user_table_insert",
    append_only=False
)

load_artist_dimension_table= LoadDimensionOperator(
    task_id="Load_artsit_dim_table",
    dag=dag,
    conn_id="redshift",
    redshift_conn_id="redshift",
    table="artists",
    sql="artist_table_insert",
    append_only=False
)

load_time_dimension_table= LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append_only=False
)

run_quality_checks=DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_check_sql=[
        {'test_sql':'select count(*) from public.staging_events WHERE userId is not null;','expected_result':0, 'comparison':'>'},
        {'test_sql':'select count(*) from public.users;','expected_result':0, 'comparison':'>'},
        {'test_sql':'select count(*) from public.users where first_name is null;','expected_result':0, 'comparison':'=='},
        {'test_sql':'select count(*) from public.songs;','expected_result':0, 'comparison':'>'},
        {'test_sql':'select count(*) from public.songs where title is null;','expected_result':0, 'comparison':'=='},
        {'test_sql':'select count(*) from public.time;','expected_result':0, 'comparison':'>'},
        {'test_sql':'select count(*) from public.artists;','expected_result':0, 'comparison':'>'}
    ]
)
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
#Task Dependecies -- 
start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift 

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_user_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_songs_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
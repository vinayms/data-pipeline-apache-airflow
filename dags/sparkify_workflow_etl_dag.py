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

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="JSON",
)

load_songplays_fact_table= LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    conn_id="redshift",
    table="songplay",
    sql="songplay_table_insert",
    append_only=False
)

load_songs_dimension_table= LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    conn_id="redshift",
    table="song",
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
    redshift_conn_id="redshift",
    table="artist",
    sql="artist_table_insert",
    append_only=False
)

load_time_dimension_table= LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append_only=False
)

run_quality_checks=DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["songplay","users", "song", "artist", "time"]
)
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
#Task Dependecies -- 
start_operator >> create_tables

create_tables >> stage_events_to_redshift >> load_songplays_fact_table
create_tables >> stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_user_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_songs_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
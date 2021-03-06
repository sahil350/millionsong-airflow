from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'retries': 3,
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    schema='public',
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='users',
    sql_stmt=SqlQueries.user_table_insert,
    truncate=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='songs',
    sql_stmt=SqlQueries.song_table_insert,
    truncate=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='artists',
    sql_stmt=SqlQueries.artist_table_insert,
    truncate=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='time',
    sql_stmt=SqlQueries.time_table_insert,
    truncate=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tests=[{'sql': SqlQueries.song_quality_check, 'expected_result': (1,)},
           {'sql': SqlQueries.artist_quality_check, 'expected_result': (1,)}]
    table='songs',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task execution order
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_song_dimension_table    >> run_quality_checks 
load_artist_dimension_table  >> run_quality_checks
load_time_dimension_table    >> run_quality_checks
load_user_dimension_table    >> run_quality_checks
run_quality_checks >> end_operator

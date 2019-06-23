from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from s3_redshift_subdag import get_s3_to_redshift_dag
from airflow.operators import (LoadFactOperator, PostgresOperator, 
                        LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries
from helpers import InitDbQueries
from airflow.models import Variable


dag_vars            = Variable.get("million_songs_s3_buckets", deserialize_json=True)
input_bucket        = dag_vars["bucket"]
events_data         = dag_vars["logs"]
songs_data          = dag_vars["songs"]

redshift_conn_id    = "redshift"
aws_conn_id         = "aws_credentials"

dag_id = "million_songs_dag_n"

songs_copy_task_id          = "stage_songs"
songs_staging_table         = "staging_songs"
songs_staging_copy_params   = [" json 'auto' "]

events_copy_task_id         = "stage_events"
events_staging_table        = "staging_events"
events_staging_json_paths   = dag_vars['logs_jsonpaths']
events_staging_copy_params  = [f" json '{events_staging_json_paths}'"]


start_date = datetime.now()-timedelta(hours = 4)
default_args = {
    'owner': 'Kofi Baafi',
    'start_date': start_date,
    'email': ['kbaafi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries':3,
    'retry_delay':timedelta(minutes = 5)
}

dag = DAG(dag_id,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          catchup=False,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

initialize_database = PostgresOperator(
    dag = dag, 
    task_id = 'Initialize_database',
    postgres_conn_id=redshift_conn_id,
    sql = InitDbQueries.init_db
)

stage_events_to_redshift = SubDagOperator(
    dag = dag,
    task_id = events_copy_task_id,
    subdag = get_s3_to_redshift_dag(
        dag_id,
        events_copy_task_id,
        redshift_conn_id,
        aws_conn_id,
        events_staging_table,
        "",
        input_bucket,
        events_data,
        False,
        events_staging_copy_params,
        start_date = start_date
    ))

stage_songs_to_redshift = SubDagOperator(
    dag = dag,
    task_id = songs_copy_task_id,
    subdag = get_s3_to_redshift_dag(
        dag_id,
        songs_copy_task_id,
        redshift_conn_id,
        aws_conn_id,
        songs_staging_table,
        "",
        input_bucket,
        songs_data,
        False,
        songs_staging_copy_params,
        start_date = start_date
    ))

load_songplays_table = LoadFactOperator(
    dag = dag,
    task_id='Load_songplays_fact_table',
    conn_id = redshift_conn_id,
    sql_stmt = SqlQueries.songplay_table_insert,
    target_table = "public.songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    dag = dag,
    task_id = 'Load_user_dim_table',
    conn_id = redshift_conn_id,
    sql_stmt = SqlQueries.user_table_insert,
    target_table = "public.users",
    truncate_before_insert = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = redshift_conn_id,
    sql_stmt = SqlQueries.song_table_insert,
    target_table = "public.songs",
    truncate_before_insert = False
)

load_artist_dimension_table = LoadDimensionOperator(
    dag = dag,
    task_id='Load_artist_dim_table',
    conn_id = redshift_conn_id,
    sql_stmt = SqlQueries.artist_table_insert,
    target_table = "public.artists",
    truncate_before_insert = False
)

load_time_dimension_table = LoadDimensionOperator(
    dag = dag,
    task_id='Load_time_dim_table',
    conn_id = redshift_conn_id,
    sql_stmt = SqlQueries.time_table_insert,
    target_table = "public.time",
    truncate_before_insert = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = redshift_conn_id,
    test_cases = SqlQueries.qa_test_cases)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>initialize_database
initialize_database >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table]>>run_quality_checks
load_songplays_table >> [load_user_dimension_table, load_time_dimension_table]>>run_quality_checks
run_quality_checks>>end_operator

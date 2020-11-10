from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, CheckNull

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Setup default_args object used in the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,    
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5), 
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
        'spartify_dag',
        default_args = default_args,
        description = 'Load and transform data in Redshift with Airflow',
        schedule_interval = '0 * * * *'
        )

start_operator = DummyOperator(task_id = 'Begin_execution', dag = dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data" ,
    create_sql = SqlQueries.staging_events_create,     
    data_format = 'json',
    json_path = 'log_json_path.json'    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    create_sql = SqlQueries.staging_songs_create,       
    data_format = 'json',
    json_path = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = "songplays",
    redshift_conn_id = "redshift",
    create_sql = SqlQueries.songplay_table_create,
    insert_sql = SqlQueries.songplay_table_insert   
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    table = "users",
    redshift_conn_id = "redshift",
    create_sql = SqlQueries.user_table_create,
    insert_sql = SqlQueries.user_table_insert,
    insert_type = "delete"     
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = "songs",
    redshift_conn_id = "redshift",
    create_sql = SqlQueries.song_table_create,
    insert_sql = SqlQueries.song_table_insert,
    insert_type = "delete" 
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = "artists",
    redshift_conn_id = "redshift",
    create_sql = SqlQueries.artist_table_create,
    insert_sql = SqlQueries.artist_table_insert,
    insert_type = "delete"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    table = "time",
    redshift_conn_id = "redshift",
    create_sql = SqlQueries.time_table_create,
    insert_sql = SqlQueries.time_table_insert,
    insert_type = "delete"
)

# Generate SQL based test cases and the expected results 
columns_dict = {
    "songplays": ["playid", "start_time"],
    "songs": ["songid"],
    "users": ["userid"],
    "artists": ["artistid"], 
    "time": ["start_time"]        
}
sql_statements, expected_values = CheckNull.get_check_null_test_cases(columns_dict)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    test_case_sqls = sql_statements,
    expected_results = expected_values
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

# Configure the task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

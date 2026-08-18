from airflow import DAG
from datawerehouse.dwh import core_table, staging_table
from dataquality.soda import yt_elt_data_quality
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

local_tz = pendulum.timezone("America/Chicago")

#args that can reused
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 8, 4, tzinfo=local_tz), # time at which airflow will begin running the dag, first run will bes scheduled at end of interraval, following next day
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# extract
with DAG(
    dag_id = "produce_json",
    default_args = default_args,
    description = "DAG to produce JSON file with raw data",
    schedule = "0 14 * * *", #cron tab guru
    catchup = False # not to catch up missed 
) as dag_produce:
    
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)
    
    #defining dependencies in dag
    
    playlist_id >> video_ids >> extract_data >> save_to_json_task
    
#load     
with DAG(
    dag_id = "update_db",
    default_args = default_args,
    description = "DAG to process JSON file and insert data into both staging and core schemas",
    schedule = "0 15 * * *", #cron tab guru
    catchup = False # not to catch up missed 
) as dag_produce:
    
    update_stagging_table = staging_table()
    update_core_table = core_table()
    #defining dependencies in dag
    
    update_stagging_table >> update_core_table


#data quality 
with DAG(
    dag_id = "data_quality",
    default_args = default_args,
    description = "DAG to check data quality on both schema",
    schedule = "0 16 * * *", #cron tab guru
    catchup = False # not to catch up missed 
) as dag_produce:
    
      # Define tasks
    soda_validate_staging = yt_elt_data_quality("staging")
    soda_validate_core = yt_elt_data_quality("core")

    # Define dependencies
    soda_validate_staging >> soda_validate_core
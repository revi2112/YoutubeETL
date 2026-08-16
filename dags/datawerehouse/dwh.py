from datawerehouse.data_loading import load_data
from datawerehouse.data_utils import get_conn_cursor, get_video_ids, close_conn_cursor,create_schema,create_table
from datawerehouse.data_modification import insert_rows, update_rows, delete_rows
from datawerehouse.data_transformation import parse_duration, transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    
    schema = "staging"
    
    conn, cur = None, None
    
    try:
        conn, cur = get_conn_cursor()
        # get data from json 
        # create schema create table 
        
        # get video ids  check if id from json not in video ids
        
        YT_data = load_data()
        
        create_schema(schema)
        create_table(schema)
        
        table_ids = get_video_ids(cur, schema)
        
        for row in YT_data:
            
            if len(table_ids) == 0 or row["video_id"] not in table_ids:
                insert_rows(cur, conn, schema, row)
            
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)
                    
        ids_in_json = {row["video_id"] for row in YT_data}
        
        ids_to_delete = set(table_ids) - ids_in_json
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            
        logger.info(f"{schema} table update completed ")
    
    except Exception as e:
        logger.error(f"An error occured during updating data of {schema} table, {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
            
            
@task
def core_table():
    
    schema = "core"
    
    conn, cur = None, None
    
    try:
        conn, cur = get_conn_cursor()
        # get data from json 
        # create schema create table 
        
        # get video ids  check if id from json not in video ids
        
        # stagging to core  so get data from staging
                
        create_schema(schema)
        create_table(schema)
        
        table_ids = get_video_ids(cur, schema)
        
        current_stagged_ids = set()
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()       
        
        for row in rows:
            current_stagged_ids.add(row["Video_ID"])
            
            # stagged needs to be transformed and instred 
            transformed_row = transform_data(row)
            if len(table_ids) == 0 or row["Video_ID"] not in table_ids:
                insert_rows(cur, conn, schema, transformed_row)
            
            else:
                if row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                    
        
        ids_to_delete = set(table_ids) - current_stagged_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            
        logger.info(f"{schema} table update completed ")
    
    except Exception as e:
        logger.error(f"An error occured during updating data of {schema} table, {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
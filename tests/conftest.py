import os 
import pytest
import psycopg2
from unittest import mock
from airflow.models import Varible, Connection, DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")
# library used to temporarily set, update, or clear values inside a dictionary or dictionary-like object (such as os.environ) during a test.
# It automatically restores the dictionary to its original state when the test block or function ends.
#AIRFLOW_VAR_API_KEY -> airflow var var name is api_key and Variable is used to fecth that

@pytest.fixture      
def channel_handle():
    with mock.patch.dict("os.envrion", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEESE"):
        yield Varible.get("CHANNEL_HANDLE")
        
@pytest.fixture
def mock_postgres_conn_var():
    conn = Connection(
        login = "mock_username", 
        password = "mock_password", 
        host = "mock_host", 
        port = 1234, 
        schema = "mock_db_name"
    )
    
    conn_uri = conn.get_uri()
    # we need to yierld the con details not just connection obj
    # PATCH IT AS AIRFLOW CON 
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_ELT = conn_uri):
        yield Connection.get_connection_from_secrets(conn_id = "POSTGRES_DB_ELT")
        
@pytest.fixture
def dagbag():
    yield DagBag()
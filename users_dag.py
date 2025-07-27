from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, Integer, Float, MetaData, Numeric, create_engine

# Endpoint name used to construct the staging table name and various task IDs
endpointName="users"

# --- Define a sample target table schema (students can adapt this) ---
# This section defines the structure of the table where data will be loaded.
# Students should define columns that match the data they expect from the API.
target_metadata = MetaData()
target_table = Table(
    endpointName, # Students should replace this with their desired table name
    target_metadata,
    Column("user_name", String(32), primary_key=True),
    Column("customer_zip_code", String(16)),    
	Column("customer_city", String(64)),
	Column("customer_state", String(64))
# Add more columns as needed based on the API response structure
)

# --- API Connection Details (students should fill these in) ---
API_BASE_URL = "http://34.16.77.121:1515" # e.g., "http://your.vm.external.ip:1515"
API_USERNAME = "admin" # e.g., "student1"
API_PASSWORD = "supersecret" # e.g., "pass123"

# --- MySQL Database Connection Details (students should fill these in) ---
MYSQL_HOST = "postgres" # e.g., "34.10.30.149"
MYSQL_PORT = 5432
MYSQL_DB_NAME = "airflow" # e.g., "STAGELOAD"
MYSQL_USERNAME = "airflow" # e.g., "rootroot"
MYSQL_PASSWORD = "airflow" # e.g., "root"

API_FETCH_TASK_ID=f"fetch_{endpointName}_from_api_task"
def fetch_data_from_api_callable():
    """
    Python callable to fetch data from the API.
    Students should implement the logic to make an authenticated HTTP GET request
    and return the raw JSON response text.
    """       
    endpoint = f"{API_BASE_URL}/{endpointName}/"
    print(f"Fetching data from: {endpoint}")
    
    try:
        response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
        response.raise_for_status()
        api_data_json = response.text
        print(f"Successfully fetched {len(api_data_json)} bytes from API.")
        return api_data_json
    except requests.exceptions.RequestException as e:
        print(f"Error fectching data from API: {e}")
        raise 

LOAD_TO_DB_TASK_ID=f"load_{endpointName}_to_db_task"
def load_data_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    Students should implement the logic to parse the JSON, connect to MySQL,
    and insert the data into their defined table.
    """
    api_data_json = ti.xcom_pull(task_ids=API_FETCH_TASK_ID) # Updated task_id
    
    if not api_data_json:
        print("No data fetched. Exiting load process.")
        return

    api_data = json.loads(api_data_json)

    if not api_data:
        print("API returned empty data. Nothing to load.")
        return

    # Example hint for database connection and table creation:
    db_url = (
        f"postgresql+psycopg2://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    )
    engine = create_engine(db_url)
    
    # Create the target table if it doesn't exist
    print(f"Attempting to create table '{target_table.name}' if it does not exist...")
    target_metadata.create_all(engine, tables=[target_table], checkfirst=True)
    print(f"Table '{target_table.name}' creation check complete.")
    
    # Get a direct database connection and cursor for data insertion
    # Using pymysql directly for insertion as MySqlHook is no longer used for connection details
    import psycopg2
    from psycopg2.extras import execute_values 
    conn = psycopg2.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB_NAME
    )
    cursor = conn.cursor()
    columns = [col.name for col in target_table.columns]
    insert_values = []
    for record in api_data:
        row_values = []
        for col_name in columns:
            row_values.append(record.get(col_name))
        insert_values.append(tuple(row_values))

    placeholders = ', '.join(['%s'] * len(columns))
    insert_stmt = f"INSERT INTO {target_table.name} ({', '.join(columns)}) VALUES %s"

    try:
        execute_values(cursor, insert_stmt, insert_values)
        conn.commit()
        print(f"Successfully loaded {len(api_data)} records into '{target_table.name}'.")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        engine.dispose()

# Define the Airflow DAG
with DAG(
    dag_id=f'{endpointName}_data_pipeline', # A more generic DAG ID
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'postgresql'],
    doc_md="""
    ### API to Database Data Pipeline Sample
    This DAG provides a skeletal structure for fetching data from an external API
    (which may require authentication) and loading it into a MySQL database.

    **Students:**
    1. **Fill in `API_BASE_URL`, `API_USERNAME`, `API_PASSWORD`** with your API
    details.
    2. **Fill in `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DB_NAME`,
    `MYSQL_USERNAME`, `MYSQL_PASSWORD`** with your MySQL database
    connection details.
    3. **Define `sample_target_table`** to match the schema of the data you expect to
    receive and the table you want to create/load into.
    4. **Implement the `fetch_data_from_api_callable` function** to make the actual API
    call.
    5. **Implement the `load_data_to_db` function** to process the fetched data and
    insert it into your MySQL table.
    """
) as dag:
    # Task to fetch data from the API
    fetch_from_api_task = PythonOperator(
        task_id=API_FETCH_TASK_ID,
        python_callable=fetch_data_from_api_callable,
        )

    # Task to load the fetched data into the database
    load_to_db_task = PythonOperator(
        task_id=LOAD_TO_DB_TASK_ID,
        python_callable=load_data_to_db,
        provide_context=True,
        )

    # Define the task dependencies
    fetch_from_api_task >> load_to_db_task

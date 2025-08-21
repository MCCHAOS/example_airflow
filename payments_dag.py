from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, Integer, Float, MetaData, Numeric, create_engine
import hashlib

def generate_md5_from_array(data_values):
    """Generate MD5 hash from array of values"""
    data_str = json.dumps(data_values, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

# Endpoint name used to construct the staging table name and various task IDs
endpointName="payments"
target_table_name="payment"

# --- Define a sample target table schema (students can adapt this) ---
# This section defines the structure of the table where data will be loaded.
# Students should define columns that match the data they expect from the API.
target_metadata = MetaData()
target_table = Table(
    target_table_name, # Students should replace this with their desired table name
    target_metadata,
    Column("order_id", String(32), primary_key=True),
    Column("payment_sequential", Integer),
    Column("payment_type", String(32)),
	Column("payment_installments", Integer),
    Column("payment_value", Integer),
    Column("md5_hash", String(32)),
    Column("dv_load_timestamp", String(32))
# Add more columns as needed based on the API response structure
)

# Create archive table by copying column definitions with modified primary keys
archive_columns = []
for col in target_table.columns:
    # For archive table, make dv_load_timestamp part of composite PK
    if col.name == 'dv_load_timestamp':
        archive_columns.append(Column(col.name, col.type, primary_key=True))
    else:
        # Keep original primary key status for other columns
        archive_columns.append(Column(col.name, col.type, primary_key=col.primary_key))

archive_table = Table(
    f"{target_table_name}_duplicate_archive",
    target_metadata,
    *archive_columns    
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
    and loads the data into a table with deduplication logic.
    """
    api_data_json = ti.xcom_pull(task_ids=API_FETCH_TASK_ID)
    
    if not api_data_json:
        print("No data fetched. Exiting load process.")
        return

    api_data = json.loads(api_data_json)

    if not api_data:
        print("API returned empty data. Nothing to load.")
        return

    # Database connection setup
    db_url = (
        f"postgresql+psycopg2://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    )
    engine = create_engine(db_url)
    
    # Create tables if they don't exist
    print(f"Creating tables if they don't exist...")
    target_metadata.create_all(engine, tables=[target_table, archive_table], checkfirst=True)
    print(f"Table creation check complete.")
    
    # Database connection for data operations
    import psycopg2
    from psycopg2.extras import execute_values
    from datetime import datetime
    
    conn = psycopg2.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB_NAME
    )
    cursor = conn.cursor()
    
    # Get column definitions
    data_columns = [col.name for col in target_table.columns 
                   if col.name not in ['md5_hash', 'dv_load_timestamp']]
    all_columns = [col.name for col in target_table.columns]
    current_timestamp = datetime.now().isoformat()
    
    # Calculate hashes for all records
    records_with_hash = []
    for record in api_data:
        data_values = [record.get(col) for col in data_columns]
        record_hash = generate_md5_from_array(data_values)
        full_record = [record.get(col) for col in data_columns] + [record_hash, current_timestamp]
        records_with_hash.append((record_hash, tuple(full_record)))
    
    # Bulk check for existing hashes
    all_hashes = [item[0] for item in records_with_hash]
    if all_hashes:
        hash_placeholders = ','.join(['%s'] * len(all_hashes))
        cursor.execute(
            f"SELECT md5_hash FROM {target_table.name} WHERE md5_hash IN ({hash_placeholders})",
            all_hashes
        )
        existing_hashes = {row[0] for row in cursor.fetchall()}
    else:
        existing_hashes = set()
    
    # Split records into new and duplicates
    new_records = []
    duplicate_records = []
    
    for record_hash, full_record in records_with_hash:
        if record_hash in existing_hashes:
            duplicate_records.append(full_record)
        else:
            new_records.append(full_record)
    
    try:
        # Insert new records into main table
        if new_records:
            insert_stmt = f"INSERT INTO {target_table.name} ({', '.join(all_columns)}) VALUES %s"
            execute_values(cursor, insert_stmt, new_records)
            print(f"Inserted {len(new_records)} new records into '{target_table.name}'.")
        
        # Insert duplicates into archive table
        if duplicate_records:
            archive_stmt = f"INSERT INTO {archive_table.name} ({', '.join(all_columns)}) VALUES %s"
            execute_values(cursor, archive_stmt, duplicate_records)
            print(f"Archived {len(duplicate_records)} duplicate records to '{archive_table.name}'.")
        
        if not new_records and not duplicate_records:
            print("No records to process.")
        
        conn.commit()
        print(f"Successfully processed {len(api_data)} total records.")
        
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

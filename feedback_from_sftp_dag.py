# CSV looks like this:
'''
feedback_id,order_id,feedback_score,feedback_form_sent_date,feedback_answer_date
7bc2406110b926393aa56f80a40eba40,73fc7af87114b39712e6da79b0a377eb,4,2018-01-18 00:00:00,2018-01-18 21:46:59
80e641a11e56f04c1ad469d5645fdfde,a548910a1c6147796b98fdf73dbeba33,5,2018-03-10 00:00:00,2018-03-11 03:05:13
228ce5500dc1d8e020d8d1322874b6f0,f9e4b658b201a9f2ecdecbb34bed034b,5,2018-02-17 00:00:00,2018-02-18 14:36:24
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import csv
import io
import paramiko
from sqlalchemy import Table, Column, String, Integer, MetaData, create_engine
import hashlib

def generate_md5_from_array(data_values):
    """Generate MD5 hash from array of values"""
    data_str = json.dumps(data_values, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

# Endpoint name used to construct the staging table name and various task IDs
endpointName="feedback"

# --- Define a sample target table schema (students can adapt this) ---
# This section defines the structure of the table where data will be loaded.
# Students should define columns that match the data they expect from the API.
target_metadata = MetaData()
target_table = Table(
    endpointName, # Students should replace this with their desired table name
    target_metadata,
    Column("feedback_id", String(32), primary_key=True),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", String(26)),
    Column("feedback_answer_date", String(26)), 
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
    f"{endpointName}_duplicate_archive",
    target_metadata,
    *archive_columns    
)

# --- SFTP Connection Details ---
SFTP_HOST = "34.16.77.121"
SFTP_PORT = 2222
SFTP_USERNAME = "BuildProject"
SFTP_PASSWORD = "student"
SFTP_FILE_PATH = "/upload/feedback_dataset.csv"

# --- MySQL Database Connection Details (students should fill these in) ---
MYSQL_HOST = "postgres" # e.g., "34.10.30.149"
MYSQL_PORT = 5432
MYSQL_DB_NAME = "airflow" # e.g., "STAGELOAD"
MYSQL_USERNAME = "airflow" # e.g., "rootroot"
MYSQL_PASSWORD = "airflow" # e.g., "root"

SFTP_FETCH_TASK_ID=f"fetch_{endpointName}_from_sftp_task"
def fetch_data_from_sftp_callable():
    """
    Python callable to fetch CSV data from the SFTP server.
    Returns the CSV data as a JSON string for compatibility with the rest of the pipeline.
    """
    print(f"Fetching data from SFTP: {SFTP_HOST}:{SFTP_PORT}{SFTP_FILE_PATH}")

    try:
        # Create SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to SFTP server
        ssh_client.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD
        )

        # Create SFTP client
        sftp_client = ssh_client.open_sftp()

        # Read the CSV file into memory
        with sftp_client.file(SFTP_FILE_PATH, 'r') as remote_file:
            csv_content = remote_file.read().decode('utf-8')

        # Parse CSV and convert to list of dictionaries (same format as API)
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        data_list = list(csv_reader)

        # Convert to JSON string for compatibility with existing pipeline
        json_data = json.dumps(data_list)

        print(f"Successfully fetched {len(csv_content)} bytes from SFTP server.")
        print(f"Parsed {len(data_list)} records from CSV.")

        return json_data

    except Exception as e:
        print(f"Error fetching data from SFTP: {e}")
        raise
    finally:
        try:
            sftp_client.close()
            ssh_client.close()
        except:
            pass

LOAD_TO_DB_TASK_ID=f"load_{endpointName}_to_db_task"
def load_data_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table with deduplication logic.
    """
    api_data_json = ti.xcom_pull(task_ids=SFTP_FETCH_TASK_ID)
    
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
    
    # Split records into new and duplicates
    new_records = []
    duplicate_records = []
    
    # Calculate hashes for all records and deduplicate by feedback_id within batch
    records_with_hash = []
    seen_feedback_ids = set()
    duplicate_within_batch = []
    
    for record in api_data:
        feedback_id = record.get('feedback_id')
        
        # Check for duplicates within the current batch
        if feedback_id in seen_feedback_ids:
            duplicate_within_batch.append(record)
            continue
        
        seen_feedback_ids.add(feedback_id)
        
        data_values = [record.get(col) for col in data_columns]
        
        # Convert feedback_score to integer to match API data type
        if 'feedback_score' in data_columns:
            score_idx = data_columns.index('feedback_score')
            if data_values[score_idx] is not None:
                data_values[score_idx] = int(data_values[score_idx])
        
        record_hash = generate_md5_from_array(data_values)
        full_record = [record.get(col) for col in data_columns] + [record_hash, current_timestamp]
        records_with_hash.append((record_hash, tuple(full_record)))
    
    if duplicate_within_batch:
        print(f"Found {len(duplicate_within_batch)} duplicate feedback_ids within batch - skipping these records")       
            
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
    dag_id=f'{endpointName}_data_from_sftp_pipeline', # A more generic DAG ID
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'sftp_integration', 'postgresql'],
    doc_md="""
    ### SFTP to Database Data Pipeline
    This DAG fetches feedback data from a CSV file on an SFTP server
    and loads it into a PostgreSQL database with deduplication logic.

    **Configuration:**
    - SFTP server: 34.16.77.121:2222
    - Username: BuildProject
    - Password: student
    - File path: /upload/feedback_dataset.csv

    The pipeline:
    1. Connects to the SFTP server using paramiko
    2. Downloads the CSV file into memory
    3. Parses the CSV data
    4. Calculates MD5 hashes for deduplication
    5. Loads new records into the main table
    6. Archives duplicate records in a separate table
    """
) as dag:
    # Task to fetch data from the SFTP server
    fetch_from_sftp_task = PythonOperator(
        task_id=SFTP_FETCH_TASK_ID,
        python_callable=fetch_data_from_sftp_callable,
        )

    # Task to load the fetched data into the database
    load_to_db_task = PythonOperator(
        task_id=LOAD_TO_DB_TASK_ID,
        python_callable=load_data_to_db,
        provide_context=True,
        )

    # Define the task dependencies
    fetch_from_sftp_task >> load_to_db_task

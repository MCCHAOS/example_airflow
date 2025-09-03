from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import io
import paramiko
import json
from sqlalchemy import Table, Column, String, Integer, Float, MetaData, Numeric, create_engine
import hashlib
import psycopg2
from psycopg2.extras import execute_values

def generate_md5_from_array(data_values):
    """Generate MD5 hash from array of values"""
    data_str = json.dumps(data_values, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

# Endpoint name used to construct the staging table name and various task IDs
endpointName="products"
target_table_name="product"
DB_SCHEMA = "stage"

# --- Define a sample target table schema (students can adapt this) ---
# This section defines the structure of the table where data will be loaded.
# Students should define columns that match the data they expect from the API.
target_metadata = MetaData()
target_table = Table(
    target_table_name, # Students should replace this with their desired table name
    target_metadata,
    Column("product_id", String(32), primary_key=True),
    Column("product_category", String(64)),    
	Column("product_name_length", Integer),
	Column("product_description_length", Integer),
	Column("product_photos_qty", Integer),
	Column("product_weight_g", Integer),
	Column("product_length_cm", Integer),
	Column("product_height_cm", Integer),
	Column("product_width_cm", Integer),
    Column("md5_hash", String(32)),
    Column("dv_load_timestamp", String(32)),
    schema=DB_SCHEMA
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
    *archive_columns,
    schema=DB_SCHEMA    
)

# --- SFTP Connection Details ---
SFTP_HOST = "34.16.77.121"
SFTP_PORT = 2222
SFTP_USERNAME = "BuildProject"
SFTP_PASSWORD = "student"
SFTP_FILE_PATH = "/upload/products_dataset.csv"

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
    
    # Calculate hashes for all records & Dedupe within batch
    records_to_process = []
    seen_ids = set()
    duplicate_within_batch = []
    primary_key_columns = sorted([col.name for col in target_table.primary_key.columns])

    # Get a list of column names that are of type Integer
    integer_columns = [col.name for col in target_table.columns if isinstance(col.type, Integer)]

    for record in api_data:
        id_values_tuple = tuple(record.get(col) for col in primary_key_columns)        
        if id_values_tuple in seen_ids:
            duplicate_within_batch.append(record)
            continue
        
        seen_ids.add(id_values_tuple)

        failed_conversions = []
        original_record = record.copy()
        for col_name in integer_columns:
            value = original_record.get(col_name)
            
            # Check for empty strings or 'NULL' values and convert them to None
            if value is None or (isinstance(value, str) and (value.strip() == '' or value.strip().upper() == 'NULL')):
                record[col_name] = None
                continue
                
            if value is not None:
                try:
                    # Attempt conversion, handling cases where values might be floats
                    record[col_name] = int(float(value))
                except (ValueError, TypeError):
                    failed_conversions.append(col_name)
        
        if failed_conversions:
            # Report all failed columns together in 1 row, but effectively drop the record
            # IMPORTANT - future enhancement would confirm if we want to drop the record, fail the DAG, or something else
            print(f"Conversion failure on {tuple(failed_conversions)} for '{json.dumps(original_record)}'")        
        else:
            data_values = [record.get(col) for col in data_columns]
            record_hash = generate_md5_from_array(data_values)
            full_record = [record.get(col) for col in data_columns] + [record_hash, current_timestamp]
            records_to_process.append((record_hash, tuple(full_record)))
    
    # Bulk check for existing primary keys and hashes in the database
    all_pks_from_batch = [tuple(record.get(col) for col in primary_key_columns) for record in api_data]
    
    existing_pks = set()
    existing_hashes = set()
    if all_pks_from_batch:
        # Dynamically build a generic WHERE clause for multi-column primary keys
        pk_conditions = []
        pk_params = []
        for pk_tuple in all_pks_from_batch:
            # Create a list of 'column = %s' conditions for each key in the tuple
            condition_parts = [f"{col} = %s" for col in primary_key_columns]
            # Join the conditions with 'AND' and wrap in parentheses
            pk_conditions.append(f"({' AND '.join(condition_parts)})")
            # Flatten the tuple of values into the parameter list
            pk_params.extend(pk_tuple)
        
        # Build the final query by joining the parenthetical conditions with 'OR'
        where_clause = ' OR '.join(pk_conditions)
        query = f"SELECT {', '.join(primary_key_columns)}, md5_hash FROM {target_table.schema}.{target_table.name} WHERE {where_clause}"
        
        cursor.execute(query, pk_params)

        for row in cursor.fetchall():
            pk_values = tuple(row[:-1])
            md5_hash = row[-1]
            existing_pks.add(pk_values)
            existing_hashes.add(md5_hash)

    # Split records into new and duplicates based on hash and existing PKs
    new_records = []
    colliding_records = []
    duplicate_records = []
    
    for record_hash, full_record in records_to_process:
        # Reconstruct the primary key tuple from the full_record
        pk_values = tuple(full_record[data_columns.index(pk)] for pk in primary_key_columns)

        if pk_values in existing_pks:
            # Check if this is a true collision (new hash) or a duplicate (same hash)
            if record_hash in existing_hashes:
                duplicate_records.append(full_record)
            else:
                colliding_records.append(full_record)
        else:
            new_records.append(full_record)
    
    try:
        # Insert new records into main table
        if new_records:
            insert_stmt = f"INSERT INTO {target_table.schema}.{target_table.name} ({', '.join(all_columns)}) VALUES %s"
            execute_values(cursor, insert_stmt, new_records)
            print(f"Inserted {len(new_records)} new records into '{target_table.schema}.{target_table.name}'.")
        
        # Insert duplicates (based on hash) into archive table
        if duplicate_records:
            archive_stmt = f"INSERT INTO {target_table.schema}.{archive_table.name} ({', '.join(all_columns)}) VALUES %s"
            execute_values(cursor, archive_stmt, duplicate_records)
            print(f"Archived {len(duplicate_records)} duplicate records to '{target_table.schema}.{archive_table.name}'.")

        # Insert colliding records (new hashes for existing PKs) into archive table
        if colliding_records:
            archive_stmt = f"INSERT INTO {target_table.schema}.{archive_table.name} ({', '.join(all_columns)}) VALUES %s"
            execute_values(cursor, archive_stmt, colliding_records)
            print(f"Archived {len(colliding_records)} colliding records (existing PKs with new data) to '{target_table.schema}.{archive_table.name}'.")
            
        if not new_records and not duplicate_records and not colliding_records:
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
    dag_id=f'{endpointName}_from_sftp_data_pipeline', # A more generic DAG ID
    start_date=datetime(2025, 1, 1),
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

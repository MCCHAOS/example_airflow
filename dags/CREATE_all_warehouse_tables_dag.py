from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import (
    Table,
    Column,
    String,
    Integer,
    Float,
    TIMESTAMP,
    Date,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    ForeignKeyConstraint,
    ForeignKey,
    create_engine,
    text
)
import hashlib
from sqlalchemy.dialects.postgresql import UUID

target_metadata = MetaData()

user_table = Table(
    "Dim_User",
    target_metadata,
    Column("UserID", String(32), primary_key=True),    
    Column("UserCity", String(64)),
    Column("UserState", String(64)),
    Column("UserZIPCode", String(16)),
    schema="dwh",
)

seller_table = Table(
    "Dim_Seller",
    target_metadata,
    Column("SellerID", String(32), primary_key=True),    
    Column("SellerCity", String(64)),
    Column("SellerState", String(64)),
    Column("SellerZIPCode", String(16)),
    schema="dwh",
)

product_table = Table(
    "Dim_Product",
    target_metadata,
    Column("ProductID", String(32), primary_key=True),        
    Column("ProductCategory", String(64)),
    Column("ProductNameLength", Integer),
    Column("ProductDescriptionLength", Integer),
    Column("ProductPhotosQty", Integer),
    Column("ProductWeight_g", Integer),
    Column("ProductLength_cm", Integer),
    Column("ProductHeight_cm", Integer),
    Column("ProductWidth_cm", Integer),
    schema="dwh",
)

order_table = Table(
    "Dim_Order",
    target_metadata,
    Column("OrderID", String(32), primary_key=True),
    Column("UserID", String(32)),
    Column("OrderStatus", String(11)),
    Column("OrderDate", TIMESTAMP),
    Column("OrderApprovedDate", TIMESTAMP),
    Column("PickupDate", TIMESTAMP),
    Column("DeliveredDate", TIMESTAMP),
    Column("EstimatedTimeDelivery", TIMESTAMP),
    schema="dwh",
)

feedback_table = Table(
    "Fact_Feedback",
    target_metadata,
    Column("FeedbackID", String(32), primary_key=True),
    Column("OrderID", String(32)),
    Column("FeedbackScore", Integer),
    Column("FeedbackFormSentDate", TIMESTAMP),
    Column("FeedbackAnswerDate", TIMESTAMP),
    schema="dwh",
)

payment_table = Table(
    "Fact_Payment",
    target_metadata,
    Column("PaymentID", Integer, primary_key=True),
    Column("OrderID", String(32)),
    Column("PaymentSequential", Integer),
    Column("PaymentType", String(32)),
    Column("PaymentInstallments", Integer),  
    Column("PaymentValue", Integer),  
    schema="dwh",
)
    
time_table = Table(
    "Dim_Time",
    target_metadata,
    Column("TimeKey", Integer, primary_key=True),
    Column("FullDate", Date),
    Column("Year", Integer),
    Column("Quarter", Integer),
    Column("Month", Integer),
    Column("Day", Integer),
    Column("DayOfWeek", Integer),
    schema="dwh"
)

fact_sales_table = Table(
    "Fact_SalesLineItem",
    target_metadata,
    Column("OrderID", String(32)),
    Column("ProductID", String(32)),
    Column("LineItemNumber", Integer),
    Column("UserID", String(32)),
    Column("SellerID", String(32)),    
    Column("TimeKey", Integer),
    Column("Price", Numeric(10, 2)),
    Column("ShippingCost", Numeric(10, 2)),

    # Define the Composite Primary Key
    PrimaryKeyConstraint(
        "OrderID", "LineItemNumber"
    ),

    # Define the Foreign Key Constraints
    ForeignKeyConstraint(["OrderID"], ["dwh.Dim_Order.OrderID"], name="fk_order"),
    ForeignKeyConstraint(["ProductID"], ["dwh.Dim_Product.ProductID"], name="fk_product"),
    ForeignKeyConstraint(["UserID"], ["dwh.Dim_User.UserID"], name="fk_user"),
    ForeignKeyConstraint(["SellerID"], ["dwh.Dim_Seller.SellerID"], name="fk_seller"),    
    ForeignKeyConstraint(["TimeKey"], ["dwh.Dim_Time.TimeKey"], name="fk_time"),
    schema="dwh"
)

intention_table = Table(
    "Dim_online_shoppers_intention",
    target_metadata,
    Column("event_id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),    
    Column("Administrative", Integer),
    Column("Administrative_Duration", Float),
    Column("Informational", Integer),
    Column("Informational_Duration",Float),
    Column("ProductRelated", Integer),
    Column("ProductRelated_Duration", Float),
    Column("BounceRates", Float),
    Column("ExitRates", Float),
    Column("PageValues", Float),
    Column("SpecialDay", Float),
    Column("Month", String(3)),
    Column("OperatingSystems", Integer),
    Column("Browser", Integer),
    Column("Region", Integer),
    Column("TrafficType", Integer),
    Column("VisitorType", String(32)), # Returning_Visitor / New_Visitor / Other
    Column("Weekend", String(5)), # TRUE / FALSE
    Column("Revenue", String(5)), # TRUE / FALSE
    Column("md5_hash", String(32)),
    Column("dv_load_timestamp", String(32)),
    schema="dwh"
)


tables = [
    user_table,
    seller_table,
    product_table,
    order_table,    
    feedback_table,
    payment_table,
    time_table,
    fact_sales_table,
    intention_table
]

# --- MySQL Database Connection Details (students should fill these in) ---
MYSQL_HOST = "postgres"  # e.g., "34.10.30.149"
MYSQL_PORT = 5432
MYSQL_DB_NAME = "airflow"  # e.g., "STAGELOAD"
MYSQL_USERNAME = "airflow"  # e.g., "rootroot"
MYSQL_PASSWORD = "airflow"  # e.g., "root"

create_tables_in_db_task_id = f"create_all_warehouse_tables_db_task"


def create_tables_in_db(ti):
    # Database connection setup
    db_url = (
        f"postgresql+psycopg2://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    )
    engine = create_engine(db_url)

    try:
        # Create tables if they don't exist
        print(f"Creating tables if they don't exist...")
        target_metadata.create_all(engine, tables=tables, checkfirst=True)
        print(f"Table creation check complete.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise
    finally:
        engine.dispose()


# Define the Airflow DAG
with DAG(
    dag_id="create_warehouse_tables_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data_pipeline", "api_integration", "postgresql"],
    doc_md="""
    ### 
    This DAG ensures all the warehouse tables are created in the database.
    """,
) as dag:
    create_tables_in_db_task = PythonOperator(
        task_id=create_tables_in_db_task_id,
        python_callable=create_tables_in_db,
        provide_context=True,
    )

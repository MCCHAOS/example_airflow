from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
import pendulum

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dwh_load_from_stage',
    default_args=default_args,
    description='Loads data from Staging to DWH Dimension and Fact tables',
    schedule_interval='@daily', # Or however often you want to run it
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dwh', 'transform'],
) as dag:

    # 1. Start Task
    start = EmptyOperator(
        task_id='start',
    )
    # Add this task to disable FK constraints
    disable_fact_fk = PostgresOperator(
        task_id='disable_fact_fk',
        postgres_conn_id='my_dwh_conn',
        sql="""
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_order;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_product;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_user;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_seller;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_payment;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_feedback;
        ALTER TABLE dwh."Fact_Sales" DROP CONSTRAINT IF EXISTS fk_time;
        """
        
    )
    # 2. Dimension Loading Tasks (these can run in parallel)
    load_dim_user = PostgresOperator(
        task_id='load_dim_users',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_seller.sql"    
    )
    load_dim_seller = PostgresOperator(
        task_id='load_dim_sellers',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_seller.sql"    
    )
    load_dim_product = PostgresOperator(
        task_id='load_dim_product',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_product.sql"
    )    
    load_dim_order = PostgresOperator(
        task_id='load_dim_order',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_order.sql"    
    )
    load_dim_feedback = PostgresOperator(
        task_id='load_dim_feedback',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_feedback.sql"    
    )    
    load_dim_time = PostgresOperator(
        task_id='load_dim_time',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_time.sql"
    )
    load_dim_payment = PostgresOperator(
        task_id='load_dim_payment',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_dim_payment.sql"
    )    
    # 3. Join Task - a dummy task to wait for all dimensions to finish
    dimensions_loaded = EmptyOperator(
        task_id='dimensions_loaded',
    )
    # 4. Fact Table Loading Task (runs after all dimensions are loaded)
    load_fact_sale = PostgresOperator(
        task_id='load_fact_sale',
        postgres_conn_id='my_dwh_conn',
        sql="sql/POPULATE_fact_sale.sql"
    )
    enable_fact_fk = PostgresOperator(
        task_id='enable_fact_fk',
        postgres_conn_id='my_dwh_conn',
        sql="""
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_order FOREIGN KEY ("OrderID") REFERENCES dwh."Dim_Order" ("OrderID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_product FOREIGN KEY ("ProductID") REFERENCES dwh."Dim_Product" ("ProductID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_user FOREIGN KEY ("UserID") REFERENCES dwh."Dim_User" ("UserID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_seller FOREIGN KEY ("SellerID") REFERENCES dwh."Dim_Seller" ("SellerID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentID") REFERENCES dwh."Dim_Payment" ("PaymentID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_feedback FOREIGN KEY ("FeedbackID") REFERENCES dwh."Dim_Feedback" ("FeedbackID");
            ALTER TABLE dwh."Fact_Sales" ADD CONSTRAINT fk_time FOREIGN KEY ("TimeKey") REFERENCES dwh."Dim_Time" ("TimeKey");
        """
    )
    # 5. End Task
    end = EmptyOperator(
        task_id='end',
    )

    # 6. Define Task Dependencies
    start >> disable_fact_fk >> [load_dim_user, load_dim_seller, load_dim_product, load_dim_order, load_dim_feedback, load_dim_time, load_dim_payment] >> dimensions_loaded
    dimensions_loaded >> load_fact_sale >> enable_fact_fk >> end
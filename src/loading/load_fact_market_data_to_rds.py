import boto3
import pandas as pd
import psycopg2
from psycopg2 import extras
import logging
import sys
import os
import traceback
from io import BytesIO

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION (Environment Variables)
# ==============================================================================
S3_BUCKET = os.getenv('S3_BUCKET', 'julian-crypto-s3-bucket')
S3_PREFIX = os.getenv('S3_PREFIX', 'processed/fact_market_data/')

DB_HOST = os.getenv('DB_HOST', 'crypto-etl-db.c87yis8u2wwc.us-east-1.rds.amazonaws.com')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD')

if not DB_PASSWORD:
    logger.error("DB_PASSWORD environment variable is required but not set")
    sys.exit(1)

TABLE_NAME = "fact_market_data"
BATCH_SIZE = 1000

# ==============================================================================
# CORE FUNCTIONS
# ==============================================================================

def create_s3_client():
    """Create and return an AWS S3 client."""
    try:
        s3_client = boto3.client('s3')
        logger.info("S3 client created successfully")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        raise

def create_db_connection():
    """Create and return a connection to the PostgreSQL RDS instance."""
    try:
        logger.info(f"Attempting to connect to database: {DB_NAME} at {DB_HOST}")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )
        logger.info(f"Successfully connected to database: {DB_NAME}")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def read_all_parquet_files(s3_client, bucket, prefix):
    """List and read all Parquet files from S3 into a single DataFrame."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            logger.warning(f"No files found in s3://{bucket}/{prefix}")
            return pd.DataFrame()
        
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        logger.info(f"Reading {len(files)} Parquet file(s)...")
        
        dataframes = []
        for file in files:
            resp = s3_client.get_object(Bucket=bucket, Key=file)
            df = pd.read_parquet(BytesIO(resp['Body'].read()))
            dataframes.append(df)
            
        return pd.concat(dataframes, ignore_index=True)
    except Exception as e:
        logger.error(f"Error reading Parquet files: {e}")
        raise

def prepare_data_for_insert(df):
    """Clean data and fix 'NaT' timestamp errors for PostgreSQL compatibility."""
    if df.empty:
        return df
    
    logger.info(f"SCHEMA CHECK - Columns found in S3: {df.columns.tolist()}")
    df_clean = df.copy()
    
    # 1. Convert Timestamps
    timestamp_cols = ['source_timestamp', 'load_date']
    for col in timestamp_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            if df_clean[col].dt.tz is None:
                df_clean[col] = df_clean[col].dt.tz_localize('UTC')

    # 2. VALIDATION: Ensure critical metrics aren't all NULL
    critical_cols = ['price_usd', 'market_cap_usd', 'volume_24h_usd']
    missing_data = df_clean[critical_cols].isnull().all()
    if missing_data.any():
        logger.error(f"DATA INTEGRITY ERROR: Columns {missing_data[missing_data].index.tolist()} are entirely NULL.")
        return pd.DataFrame()

    # 3. FIX: Convert NaT to None (Crucial for psycopg2 to insert NULL instead of invalid string "NaT")
    df_clean = df_clean.replace({pd.NaT: None})
    
    logger.info("Data preparation complete (NaT values converted to None)")
    return df_clean

def insert_data_in_batches(connection, df, table_name, batch_size):
    """Insert data into RDS using efficient batch execution."""
    if df.empty:
        logger.warning("No data to insert.")
        return 0
    
    cursor = connection.cursor()
    try:
        columns = list(df.columns)
        column_names = ','.join([f'"{col}"' for col in columns])
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
        
        rows_inserted = 0
        for batch_num in range(0, len(df), batch_size):
            batch = df.iloc[batch_num:batch_num + batch_size]
            batch_tuples = [tuple(row) for row in batch.values]
            
            extras.execute_batch(cursor, insert_query, batch_tuples, page_size=batch_size)
            connection.commit()
            rows_inserted += len(batch_tuples)
            logger.info(f"Inserted {rows_inserted}/{len(df)} rows...")
            
        return rows_inserted
    except Exception as e:
        connection.rollback()
        logger.error(f"Batch insertion failed: {e}")
        raise
    finally:
        cursor.close()

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================
def main():
    logger.info("=" * 60)
    logger.info("Starting ETL process: Load fact_market_data to RDS")
    logger.info("=" * 60)
    
    db_connection = None
    try:
        # STEP 1: Extract
        s3_client = create_s3_client()
        df = read_all_parquet_files(s3_client, S3_BUCKET, S3_PREFIX)
        
        if df.empty:
            return 0
        
        # STEP 2: Transform & Clean
        df = prepare_data_for_insert(df)
        if df.empty:
            logger.error("Stopping: Data failed integrity check.")
            return 1
        
        # STEP 3: Load
        db_connection = create_db_connection()
        rows_inserted = insert_data_in_batches(db_connection, df, TABLE_NAME, BATCH_SIZE)
        
        logger.info("=" * 60)
        logger.info(f"ETL SUCCESS: {rows_inserted} rows loaded into {TABLE_NAME}")
        logger.info("=" * 60)
        return 0
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"ETL FAILED: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return 1
    finally:
        if db_connection:
            db_connection.close()
            logger.info("Resources cleaned up.")

if __name__ == "__main__":
    sys.exit(main())

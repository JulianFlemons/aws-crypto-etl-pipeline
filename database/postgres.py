import psycopg2
import sys

# RDS connection details
host = 'crypto-etl-db.c87yis8u2wwc.us-east-1.rds.amazonaws.com'
port = 5432
database = 'postgres'
user = 'postgres'
password = 'Tyme4achg'

# Create a table schema
def create_table(conn, table_name="fact_market_data", schema_name="public"):
    """
    Create the fact_market_data table if it doesn't exist.
    
    Args:
        conn: psycopg2 connection object
        table_name: Name of the table to create (default: 'fact_market_data')
        schema_name: Schema name (default: 'public')
    
    Returns:
        bool: True if table was created successfully, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        # Create table with columns matching the ETL script output
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            coin_id VARCHAR(255),
            coin_symbol VARCHAR(50),
            price_usd DOUBLE PRECISION,
            market_cap_usd DOUBLE PRECISION,
            volume_24h_usd DOUBLE PRECISION,
            source_timestamp TIMESTAMP,
            load_date TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        print(f"Table '{schema_name}.{table_name}' created successfully (or already exists)!")
        return True
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        conn.rollback()
        return False


# Connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Connected to PostgreSQL successfully!")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

# Verify the connection
def verify_connection(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Connection verified! Query result: {result}")
        return result
    except psycopg2.Error as e:
        print(f"Connection verification failed: {e}")
        return None
    finally:
        cursor.close()


# Test the connection and create table
if __name__ == "__main__":
    print("Attempting to connect to PostgreSQL...")
    conn = connect_to_postgres()
    
    # Verify the connection works
    verify_connection(conn)
    
    # Create the table
    print("\nCreating table...")
    create_table(conn)
    
    # Close the connection
    conn.close()
    print("Connection closed.")
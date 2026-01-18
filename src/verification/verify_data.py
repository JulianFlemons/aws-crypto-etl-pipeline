import psycopg2
import os
from tabulate import tabulate # to display the data in a table format

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'crypto-etl-db.c87yis8u2wwc.us-east-1.rds.amazonaws.com')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def verify_rds_data():
    conn = None
    try:
        # Establish connection
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query the most recent 5 records
        query = "SELECT * FROM fact_market_data ORDER BY load_date DESC LIMIT 5;"
        cursor.execute(query)
        
        # Fetch results and column names
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

        # Display as a clean table
        if rows:
            print("\n--- Recent Records in fact_market_data ---")
            print(tabulate(rows, headers=colnames, tablefmt='grid'))
        else:
            print("The table is empty.")

    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    verify_rds_data()
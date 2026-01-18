import boto3

# Pseudo-code Walkthrough for creating an AWS Glue Data Catalog Database:
#
# 1. Initialize the Boto3 Glue client.
#This establishes the connection to AWS Glue.
glue_client = boto3.client('glue')

# 2. Define the database properties.
crypto_db = 'crypto_db'


# 3. Execute the creation command.   
response = glue_client.create_database(
    DatabaseInput={
        'Name': crypto_db,
        'Description': 'Database for crypto market data'
    } 
) 
# 4. Confirm creation.
print(f"Database '{crypto_db}' created successfully.")

import json
import boto3
import requests
from datetime import datetime

# Configuration
TARGET_S3_BUCKET = "julian-crypto-s3-bucket"
TARGET_COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCY = "usd"
API_ENDPOINT = "https://api.coingecko.com/api/v3/coins/markets"

# Initialize AWS clients
s3_client = boto3.client("s3")

# Fetch market data from CoinGecko API
def fetch_market_data():
    params = {
        "vs_currency": VS_CURRENCY,
        "ids": ",".join(TARGET_COINS)
    }
    response = requests.get(API_ENDPOINT, params=params, timeout=10)
    response.raise_for_status()
    return response.json()
def lambda_handler(event, context):
    try:
        data = fetch_market_data()

        current_time = datetime.now()
        timestamp = current_time.strftime("%Y-%m-%d-%H-%M-%S")

        filename = f"market_data_{timestamp}.json"

        s3_client.put_object(
            Bucket=TARGET_S3_BUCKET,
            Key=filename,
            Body=json.dumps(data),
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data uploaded successfully',
                'filename': filename,
                'bucket': TARGET_S3_BUCKET
            })
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
    
if __name__ == "__main__":
    # 1. Verify AWS Identity (helps debug account/permission issues)
    try:
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        print(f"Running as AWS Identity: {identity['Arn']}")
    except Exception as e:
        print(f"Warning: Could not verify AWS identity: {e}")

    # 2. Run the handler
    result = lambda_handler(None, None)
    print(f"Lambda Result: {result}")

    # 3. List bucket contents to verify upload
    if result['statusCode'] == 200:
        print(f"\nChecking bucket '{TARGET_S3_BUCKET}' contents:")
        try:
            response = s3_client.list_objects_v2(Bucket=TARGET_S3_BUCKET)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f" - {obj['Key']} ({obj['Size']} bytes)")
            else:
                print("Bucket is empty.")
        except Exception as e:
            print(f"Error listing bucket: {e}")
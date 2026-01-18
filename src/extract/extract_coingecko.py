import requests
import csv
from datetime import datetime, timezone
import os

COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCY = "usd"

def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": VS_CURRENCY,
        "ids": ",".join(COINS)
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    ingestion_time = datetime.now(timezone.utc).isoformat()

    records = []

    for coin in data:
        records.append({
            "coin_id": coin["id"],
            "symbol": coin["symbol"],
            "price_usd": coin["current_price"],
            "market_cap_usd": coin["market_cap"],
            "volume_24h_usd": coin["total_volume"],
            "snapshot_date": snapshot_date,
            "ingestion_timestamp": ingestion_time,
            "source": "coingecko"
        })

    return records


def write_to_csv(records):
    os.makedirs("data", exist_ok=True)

    filename = f"data/crypto_prices_{datetime.now(timezone.utc).date().isoformat()}.csv"

    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    print(f"CSV successfully written to {filename}")


if __name__ == "__main__":
    prices = fetch_crypto_prices()
    write_to_csv(prices)

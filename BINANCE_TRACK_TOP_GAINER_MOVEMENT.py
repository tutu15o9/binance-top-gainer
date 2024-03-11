import requests
import schedule
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Global variable to store data for each cryptocurrency
crypto_data = {}

def get_top_gainers(api_key, time=datetime.now()):
    base_url = "https://api.binance.com/api/v3"
    endpoint = "/ticker/24hr"
    url = base_url + endpoint

    headers = {
        "X-MBX-APIKEY": api_key,
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        data = sorted(data, key=lambda x: float(x['priceChangePercent']), reverse=True)
        top_gainers = []
        for i, ticker in enumerate(data, start=1):
            if float(ticker['priceChangePercent']) > 0 :
                ticker["position"] = i
                ticker["timestamp"] = time
                top_gainers.append(ticker)
        return top_gainers

    except requests.exceptions.RequestException as e:
        print("Error fetching data:", e)
        return None

def save_to_csv(top_gainers):

    # Open or create CSV file for writing
    file_path = 'crypto_data.csv'

    # Check if the CSV file exists
    if not os.path.isfile(file_path):
        # Create a new DataFrame if the file does not exist
        df = pd.DataFrame(columns=["symbol", "priceChange", "priceChangePercent", "weightedAvgPrice",
                                    "prevClosePrice", "lastPrice", "lastQty", "bidPrice", "bidQty",
                                    "askPrice", "askQty", "openPrice", "highPrice", "lowPrice",
                                    "volume", "quoteVolume", "openTime", "closeTime", "firstId",
                                    "lastId", "count", "position"])
    else:
        # Read the existing DataFrame from the CSV file
        df = pd.read_csv(file_path)

    # Create a DataFrame for the new data
    new_data = pd.DataFrame(top_gainers)

    # Append the new data to the existing DataFrame
    df = pd.concat([df, new_data], ignore_index=True)

    # Save the updated DataFrame to the CSV file
    df.to_csv(file_path, index=False)

def job():
    # Get API key and secret from environment variables
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    if api_key is None or api_secret is None:
        print("Binance API key or secret not found in environment variables.")
        return

    top_gainers = get_top_gainers(api_key)

    if top_gainers:
        print("Top Gainers:")
        print(len(top_gainers))

        # Save data to CSV after every API call
        save_to_csv(top_gainers)

    else:
        print("Failed to fetch top gainers.")

# Schedule the job to run every 1 minute
schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)


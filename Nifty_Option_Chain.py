import pandas as pd
import requests
from pandas import json_normalize
from datetime import datetime
import os
import schedule
import time

def fetch_option_chain_data(symbol, output_directory):
    try:
        # Create the URL for the API call
        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

        # Set the headers for the request
        headers = {
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.42"
        }

        # Send the request and retrieve the JSON response
        session = requests.Session()
        response = session.get(url, headers=headers)
        data = response.json()["records"]

        # Get the timestamp
        timestamp = data["timestamp"]
        print(timestamp) 

        # Convert the JSON data to a DataFrame
        data = pd.json_normalize(data["data"])
        data["timestamp"] = timestamp

        print(data)
        #print(data.columns)
        '''

        # Select the desired columns
        columns = [
            'strikePrice', 'expiryDate', 'CE.strikePrice', 'CE.expiryDate',
            'CE.underlying', 'CE.identifier', 'CE.openInterest',
            'CE.changeinOpenInterest', 'CE.pchangeinOpenInterest',
            'CE.totalTradedVolume', 'CE.impliedVolatility', 'CE.lastPrice',
            'CE.change', 'CE.pChange', 'CE.underlyingValue', 'PE.strikePrice',
            'PE.expiryDate', 'PE.underlying', 'PE.underlying', 'PE.identifier',
            'PE.openInterest', 'PE.changeinOpenInterest',
            'PE.pchangeinOpenInterest', 'PE.totalTradedVolume',
            'PE.impliedVolatility', 'PE.lastPrice', 'PE.change', 'PE.pChange',
            'PE.underlyingValue'
        ]

        '''

        # Generate the file name with the current date and time
        file_name = f"{symbol}_option_chain_{datetime.now().strftime('%Y%m%d')}.csv"
        file_path = os.path.join(output_directory, file_name)

        # Check if the file already exists
        if os.path.exists(file_path):
           # Read the existing CSV file with the appropriate encoding
           existing_data = pd.read_csv(file_path)

            # Append the new data to the existing data
           data = pd.concat([existing_data, data], ignore_index=True)


        # Save the data as a Parquet file
        data.to_csv(file_path, index=False)

        # Print the file path for confirmation
        print(f"Option chain data saved: {file_path}")
    except Exception as e:
        print("An error occurred:", e)

# Define the symbol and output directory
symbol = "NIFTY"
output_directory = "C:/Users/abhis/Documents/option chain"



# Run the scheduled jobs
while True:
    try:
        fetch_option_chain_data(symbol, output_directory)
        time.sleep(60)
    except Exception as e:
        print("An error occurred:", e)

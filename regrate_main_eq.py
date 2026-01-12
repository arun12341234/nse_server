import requests
import os
import csv
from datetime import date, datetime, timedelta
import pandas as pd

# Configuration
# Make sure your 'app.py' server is running on this port
BASE_URL = "http://127.0.0.1:5000/api/download"
YEAR_TO_FETCH = 2026

def fetch_dates(year):
    url = f"{BASE_URL}/{year}"
    print(f"Fetching dates from: {url}...")

    try:
        # Send GET request
        response = requests.get(url)

        # Check if the request was successful (Status Code 200)
        if response.status_code == 200:
            data = response.json()
            
            # Retrieve the list of dates from the JSON response
            date_list = data.get("dates", [])
            
            # print(f"\n--- Success! ---")
            # print(f"Total dates received: {len(date_list)}")
            # print(f"Start of range: {data.get('range_start')}")
            # print(f"End of range: {data.get('range_end')}")
            
            # # Print first 5 and last 5 dates as a preview
            # print("\nPreview of dates:")
            # print(date_list[:5], "...", date_list[-5:])
            
            return date_list
        else:
            print(f"Failed to retrieve data. Status Code: {response.status_code}")
            return None

    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server. Is 'app.py' running?")
        return None

from datetime import datetime, timedelta



if __name__ == "__main__":
    dates = fetch_dates(YEAR_TO_FETCH)
    # verfy take file and create
    # url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/track_date"
    # try:
    #     response = requests.get(url)
    #     if response.status_code == 200:
    #         data = response.json()
    #         print(data)
    #     else:
    #         print(f"Failed to retrieve data. Status Code: {response.status_code}")
    # except Exception as e:
    #     print(e)
    # Example: Loop through the dates if needed
    from step_1_equity_derivatives_list import start
    from step_2_create_fudata_sheet import FuDataSheetCreator
    from step_3_create_eqdata_sheet import EqDataSheetCreator
    start()
    url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking"
    try:
        payload = {
            "file_1": f"data_value_{date}"
        }
        # 4. Make the POST request
        # 'json=payload' automatically sets the Content-Type header to application/json
        # response = requests.post(url, json=payload)
        response = requests.get(url, json=payload)

        # 5. Check the response
        if response.status_code == 200:
            # print("Success:", response.json())
            data = response.json().get("data")
            file_1_list = [item["file_1"] for item in data if "file_1" in item]
            print(file_1_list)

            with open("symbols.csv") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    print(row['symbol'])
                    creator = EqDataSheetCreator(symbol=f"{row['symbol']}", series='EQ')
                    # # creator = FuDataSheetCreator(symbol=f"{row['symbol']}", lot_size=125)
                    creator.list_exp_start(file_1_list)
                    creator.create_eqdata_sheet('./Nse_files/'+str(YEAR_TO_FETCH)+'/'+row["symbol"]+'_FuData_Generated.xlsx')
                    # contracts_data = creator.create_fudata_sheet()
                    # creator.save_workbook('./Nse_files/'+str(YEAR_TO_FETCH)+'/'+row["symbol"]+'_FuData_Generated.xlsx')
                    break

        else:
            print(f"Error {response.status_code}:", response.text)

    except requests.exceptions.ConnectionError:
        print("Failed to connect. Is the Flask server (port 5000) running?")

    # if dates:
    #     for date in dates:
    #         print(date)
    #         url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking"
    #         try:
    #             payload = {
    #                 "file_1": f"data_value_{date}"
    #             }
    #             # 4. Make the POST request
    #             # 'json=payload' automatically sets the Content-Type header to application/json
    #             # response = requests.post(url, json=payload)
    #             response = requests.get(url, json=payload)

    #             # 5. Check the response
    #             if response.status_code == 200:
    #                 print("Success:", response.json())
    #                 # data = response.json()
    #                 # # print(data.get("data")["file_2"])
    #                 # if data.get("data")["file_1"] != 'None':
    #                 #     print(data.get("data")["file_1"],"pass")
    #                 #     df = pd.read_csv(data.get("data")["file_1"])
    #                 #     print(df)

    #             else:
    #                 print(f"Error {response.status_code}:", response.text)

    #         except requests.exceptions.ConnectionError:
    #             print("Failed to connect. Is the Flask server (port 5000) running?")
    #         # # read symebel list
    #         # with open("symbols.csv") as f:
    #         #     reader = csv.DictReader(f)
    #         #     for row in reader:
    #         #         print(row['symbol'], row['current_date'])


    #         #         break
    #         # break
            
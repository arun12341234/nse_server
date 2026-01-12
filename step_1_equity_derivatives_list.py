import requests
import pandas as pd
import time

BASE_URL = "https://www.nseindia.com"
API_URL = "https://www.nseindia.com/api/underlying-information"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/products-services/equity-derivatives-list-underlyings-information",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(headers)

# 1️⃣ Mandatory cookie warm-up
session.get(BASE_URL, timeout=10)
time.sleep(1)

# 2️⃣ Fetch underlying info
params = {
    "instrument": "equity_derivatives"
}

# check if done today don't repeate
import os
import csv
from datetime import date, datetime, timedelta

today = date.today()
# (date.today() - timedelta(days=1)).isoformat()
# 
FILENAME = "symbols.csv"

print(today)
def write_csv(response):
    # print(response)
    data = response.json()
    print(data)
    final_list = data["data"]["IndexList"]+data["data"]["UnderlyingList"]
    with open(FILENAME, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "current_date","underlying"])
        for symbol in final_list:
            writer.writerow([symbol["symbol"], today, symbol["underlying"]])
    print("symbols.csv created successfully")

def start():
    if not os.path.exists(FILENAME):
        # print("Not exist.")
        response = session.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        write_csv(response)
    else:
        with open("symbols.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_date = datetime.strptime(row["current_date"], "%Y-%m-%d").date()
                if row_date == today:
                    # print("SKIP")
                    continue  # 🚫 Skip API call
                else:
                    response = session.get(API_URL, params=params, timeout=10)
                    response.raise_for_status()
                    write_csv(response)
                # print("response")
                # break
start()
# from step_2_create_fudata_sheet import FuDataSheetCreator
# from step_3_create_eqdata_sheet import EqDataSheetCreator
# from step_4_create_oc_sheet import OptionChainSheetCreator
# with open("symbols.csv") as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         # print(row['symbol'], row['current_date'])
#         # creator = FuDataSheetCreator()
#         #     # symbol=f"{row['symbol']}")
#         # creator.list_exp_start()
#         # contracts_data = creator.create_fudata_sheet()
#         # output_file = creator.save_workbook(f'ABB_FuData_Generated.xlsx')

#         # creator1 = EqDataSheetCreator(symbol='ABB', series='EQ')
#         # creator1.list_exp_start()
#         # eq_data = creator1.create_eqdata_sheet("./future/ABB_FuData_Generated.xlsx")
#         # output_file = creator1.save_workbook("./future/ABB_FuData_Generated.xlsx")
#         creator2 = OptionChainSheetCreator(symbol='ABB', series='EQ')
#         creator2.list_exp_start()
#         # oc = creator2.create_oc_sheet("./future/ABB_FuData_Generated.xlsx")

#         break




















































# response = session.get(API_URL, params=params, timeout=10)
# response.raise_for_status()

# data = response.json()
# # print(data["data"]["IndexList"])
# # print(data["data"]["UnderlyingList"])

# # from reg_create_fudata_sheet import FuDataSheetCreator

# final_list = data["data"]["IndexList"]+data["data"]["UnderlyingList"]

# for symbol in final_list:
#     print(symbol["symbol"])










    # creator = FuDataSheetCreator(symbol=f'{symbol["symbol"]}', lot_size=125)
    # creator.list_exp_start()
    # contracts_data = creator.create_fudata_sheet()
    # output_file = creator.save_workbook(f'{symbol["symbol"]}_FuData_Generated.xlsx')






# df = pd.DataFrame(data["data"])

# # 3️⃣ Filter ONLY "Derivatives on Individual Securities"
# stocks_df = df[df["instrumentType"] == "Stock"]

# stocks_df = stocks_df[[
#     "underlying",
#     "symbol"
# ]].reset_index(drop=True)

# stocks_df.to_csv("nse_derivatives_individual_securities.csv", index=False)

# print("Fetched:", len(stocks_df), "individual securities")
# print(stocks_df.head())

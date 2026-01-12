import requests

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

def download_cmbc(date):
    from cm_bc import CMBhavcopyDownloader
    downloader = CMBhavcopyDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d")
    res = downloader.download_date(date)
    return res

def download_fobc(date):
    from bc import NSEBhavcopyDownloader
    downloader = NSEBhavcopyDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d")
    res = downloader.download_date(date)
    return res

def download_cmoi(date):
    from nse_combined_oi_downloader import NSECombinedOIDownloader
    downloader = NSECombinedOIDownloader()
    # Initialize session (required)
    if not downloader.init_session():
        print("Failed to initialize. Exiting.")
        exit(1)
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    res = downloader.download_combined_oi(date=date)
    return res
    

def download_fiis(date):
    from nse_fii_statistics_downloader import NSEAPIDownloader
    downloader = NSEAPIDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    res = downloader.download_fii_statistics(date=date)
    return res


def participant_oi(date):
    from nse_participant_oi_downloader import NSEAPIDownloader
    downloader = NSEAPIDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    res = downloader.download_participant_oi(date=date)
    return res


def participant_tv(date):
    from nse_participant_tv_downloader import NSEAPIDownloader
    downloader = NSEAPIDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    res = downloader.download_participant_tv(date=date)
    return res

def download_indices(date):
    from nse_complete_downloader import NSEDataDownloader
    downloader = NSEDataDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d")
    res = downloader.download_indices(date)
    return res

def download_equity_deliverable(date):
    from nse_complete_downloader import NSEDataDownloader
    downloader = NSEDataDownloader()
    date_str = (f"{date}").strip()
    date = datetime.strptime(date_str, "%Y-%m-%d")
    res = downloader.download_equity_deliverable(date)
    return res

if __name__ == "__main__":
    dates = fetch_dates(YEAR_TO_FETCH)
    # verfy take file and create
    url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/track_date"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Failed to retrieve data. Status Code: {response.status_code}")
    except Exception as e:
        print(e)
    # Example: Loop through the dates if needed
    if dates:
        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
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
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
                    if data.get("data")["file_1"]:
                        print(data.get("data")["file_1"],"pass")
                    else:
                        #download cmbc
                        file_data = download_cmbc(date)
                        payload = {
                            "file_1": f"{file_data}"
                        }
                        requests.post(url, json=payload)
                        print(data.get("data")["file_1"],"download")
                else:
                    #download cmbc
                    file_data = download_cmbc(date)
                    payload = {
                        "file_1": f"{file_data}"
                    }
                    requests.post(url, json=payload)
                    print("file_data", file_data)
                    # print(f"Error {response.status_code}:", response.text)

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")


        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_2": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
   
                    if data.get("data")["file_2"]:
                        print(data.get("data")["file_2"],"pass")
                    else:
                        #download fobc
                        file_data = download_fobc(date)
                        payload = {
                            "file_2": f"{file_data}"
                        }
                        requests.post(url, json=payload)
                        print(data.get("data")["file_2"],"download")
                else:
                    #download cmbc
                    file_data = download_fobc(date)
                    payload = {
                        "file_2": f"{file_data}"
                    }
                    requests.post(url, json=payload)
                    print(data.get("data")["file_2"],"download")

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")


        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_3": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    if data.get("data")["file_3"]:
                        print(data.get("data")["file_3"],"pass")
                    else:
                        #download cmoi
                        file_data = download_cmoi(date)
                        payload = {
                            "file_3": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_3"],"download")
                    
                else:
                    #download cmbc
                    file_data = download_cmoi(date)
                    payload = {
                        "file_3": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_3"],"download")

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")

        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_4": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
                    if data.get("data")["file_4"]:
                        print(data.get("data")["file_4"],"pass")
                    else:
                        #download fiistat
                        file_data = download_fiis(date)
                        payload = {
                            "file_4": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_4"],"download")
                        # break
                else:
                    #download fiistat
                    file_data = download_fiis(date)
                    payload = {
                        "file_4": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_4"],"download")

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")

        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_5": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    if data.get("data")["file_5"]:
                        print(data.get("data")["file_5"],"pass")
                    else:
                        #download participant_oi
                        file_data = participant_oi(date)
                        payload = {
                            "file_5": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_5"],"download")
                        # break
                else:
                    #download participant_oi
                    file_data = participant_oi(date)
                    payload = {
                        "file_5": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_5"],"download")

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")


        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_6": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
                    if data.get("data")["file_6"]:
                        print(data.get("data")["file_6"],"pass")
                    else:
                        #download participant_tv
                        file_data = participant_tv(date)
                        payload = {
                            "file_6": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_6"],"download")
                        # break
                else:
                    #download participant_tv
                    file_data = participant_tv(date)
                    payload = {
                        "file_6": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_6"],"download")
                    # break

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")


        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_7": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
                    if data.get("data")["file_7"]:
                        print(data.get("data")["file_7"],"pass")
                    else:
                        #download download_indices
                        file_data = download_indices(date)
                        payload = {
                            "file_7": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_7"],"download")
                        # break
                else:
                    #download download_indices
                    file_data = download_indices(date)
                    payload = {
                        "file_7": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_7"],"download")
                    # break

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")

        for date in dates:
            print(date)
            url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
            try:
                payload = {
                    "file_8": f"data_value_{date}"
                }
                # 4. Make the POST request
                # 'json=payload' automatically sets the Content-Type header to application/json
                # response = requests.post(url, json=payload)
                response = requests.get(url, json=payload)

                # 5. Check the response
                if response.status_code == 200:
                    print("Success:", response.json())
                    data = response.json()
                    # print(data.get("data")["file_2"])
                    if data.get("data")["file_8"]:
                        print(data.get("data")["file_8"],"pass")
                    else:
                        #download download_equity_deliverable
                        file_data = download_equity_deliverable(date)
                        payload = {
                            "file_8": f"{file_data}"
                        }
                        print("###########",file_data)
                        requests.post(url, json=payload)
                        print(data.get("data")["file_8"],"download")
                        # break
                else:
                    #download download_equity_deliverable
                    file_data = download_equity_deliverable(date)
                    payload = {
                        "file_8": f"{file_data}"
                    }
                    print("###########",file_data)
                    requests.post(url, json=payload)
                    print(data.get("data")["file_8"],"download")
                    # break

            except requests.exceptions.ConnectionError:
                print("Failed to connect. Is the Flask server (port 5000) running?")

        # for date in dates:
        #     print(date)
        #     url = f"http://127.0.0.1:5000/api/{YEAR_TO_FETCH}/tracking/{date}"
        #     try:
        #         payload = {
        #             "file_1": f"data_value_{date}"
        #         }
        #         # 4. Make the POST request
        #         # 'json=payload' automatically sets the Content-Type header to application/json
        #         # response = requests.post(url, json=payload)
        #         response = requests.get(url, json=payload)

        #         # 5. Check the response
        #         if response.status_code == 200:
        #             print("Success:", response.json())
        #             data = response.json()
        #             # print(data.get("data")["file_2"])
        #             if data.get("data")["file_1"]:
        #                 print(data.get("data")["file_1"],"pass")
        #             else:
        #                 #download cmbc
        #                 file_data = download_cmbc(date)
        #                 payload = {
        #                     "file_1": f"{file_data}"
        #                 }
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_1"],"download")
        #             if data.get("data")["file_2"]:
        #                 print(data.get("data")["file_2"],"pass")
        #             else:
        #                 #download fobc
        #                 file_data = download_fobc(date)
        #                 payload = {
        #                     "file_2": f"{file_data}"
        #                 }
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_2"],"download")
        #             if data.get("data")["file_3"]:
        #                 print(data.get("data")["file_3"],"pass")
        #             else:
        #                 #download cmoi
        #                 file_data = download_cmoi(date)
        #                 payload = {
        #                     "file_3": f"{file_data}"
        #                 }
        #                 print("###########",file_data)
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_3"],"download")
        #             if data.get("data")["file_4"]:
        #                 print(data.get("data")["file_3"],"pass")
        #             else:
        #                 #download fiistat
        #                 file_data = download_fiis(date)
        #                 payload = {
        #                     "file_4": f"{file_data}"
        #                 }
        #                 print("###########",file_data)
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_4"],"download")
        #                 # break
        #             if data.get("data")["file_5"]:
        #                 print(data.get("data")["file_5"],"pass")
        #             else:
        #                 #download participant_oi
        #                 file_data = participant_oi(date)
        #                 payload = {
        #                     "file_5": f"{file_data}"
        #                 }
        #                 print("###########",file_data)
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_5"],"download")
        #                 # break
        #             if data.get("data")["file_6"]:
        #                 print(data.get("data")["file_6"],"pass")
        #             else:
        #                 #download participant_tv
        #                 file_data = participant_tv(date)
        #                 payload = {
        #                     "file_6": f"{file_data}"
        #                 }
        #                 print("###########",file_data)
        #                 requests.post(url, json=payload)
        #                 print(data.get("data")["file_6"],"download")
        #                 # break
        #         else:
        #             #download cmbc
        #             file_data = download_cmbc(date)
        #             payload = {
        #                 "file_1": f"{file_data}"
        #             }
        #             requests.post(url, json=payload)
        #             print("file_data", file_data)
        #             # print(f"Error {response.status_code}:", response.text)

        #     except requests.exceptions.ConnectionError:
        #         print("Failed to connect. Is the Flask server (port 5000) running?")

















































































































            # try:
            #     response = requests.get(url)
            #     if response.status_code == 200:
            #         data = response.json()
            #         print(data)
            #     else:
            #         print(f"Failed to retrieve data. Status Code: {response.status_code}")
            # except Exception as e:
            #     print(e)
            # Example: Loop t

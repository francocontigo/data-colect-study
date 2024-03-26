# %%
import requests
import pandas as pd
import datetime
import json
import time


# %%
def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp


def save_data(data, option="json"):
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    if option == "json":
        filename = f"data/contents/json/{now}.json"
        try:
            with open(filename, "w") as open_file:
                json.dump(data, open_file, indent=4)
        except OSError as e:
            print(f"Error saving JSON file: {e}")
    elif option == "dataframe":
        df = pd.DataFrame(data)
        filename = f"data/contents/parquet/{now}"
        try:
            df.to_parquet(filename, index=False)
        except OSError as e:
            print(f"Error saving Parquet file: {e}")


# %%
page = 1
date_stop = pd.to_datetime("2024-03-01").date()
while True:
    print(page)
    resp = get_response(page=page, per_page=100, strategy="new")
    if resp.status_code == 200:
        data = resp.json()
        save_data(data)
        
        date = pd.to_datetime(data[-1]["update_at"])
        if len(data) < 100 or date < date_stop:
            break
        
        if len(data) < 100:
            break
        page += 1
        time.sleep(2)
    else:
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 15)

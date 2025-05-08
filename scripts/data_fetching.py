import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

API_KEY = os.getenv("EIA_API_KEY")




def fetch_eia_data(series_id, api_key, start_date, end_date):
    url = f"https://api.eia.gov/v2/seriesid/{series_id}"
    params = {
        "api_key": api_key,
        "start": start_date,
        "end": end_date
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"HTTP Error {response.status_code}: {response.text}")
        return pd.DataFrame()

    data = response.json()

    # Check for API errors
    if "error" in data:
        print(f"API Error: {data['error']}")
        return pd.DataFrame()

    if "response" in data and "data" in data["response"]:
        records = data["response"]["data"]
        if not records:
            print(f"No data found for series {series_id}")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df = df.rename(columns={"period": "date", "value": series_id})
        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", series_id]].sort_values("date")
        return df
    else:
        print(f"Unexpected API response: {data}")
        return pd.DataFrame()


if __name__ == "__main__":
   
    os.makedirs("data/raw/", exist_ok=True)

    BRENT_ID = "PET.RBRTE.D"
    WTI_ID = "PET.RWTC.D"
    START_DATE = "2015-01-01"
    END_DATE = "2024-12-31"

    # Fetch Brent Data
    print("Fetching Brent data...")
    brent_df = fetch_eia_data(BRENT_ID, API_KEY, START_DATE, END_DATE)
    if not brent_df.empty:
        brent_df.to_csv("data/raw/brent_prices.csv", index=False)
        print("Brent data saved!")
    else:
        print("Brent data fetch failed.")

    time.sleep(1)  

    # Fetch WTI Data
    print("Fetching WTI data...")
    wti_df = fetch_eia_data(WTI_ID, API_KEY, START_DATE, END_DATE)
    if not wti_df.empty:
        wti_df.to_csv("data/raw/wti_prices.csv", index=False)
        print("WTI data saved!")
    else:
        print("WTI data fetch failed.")


    

from fastapi import FastAPI
import requests
import datetime
from pandas_gbq import to_gbq
import pandas as pd
from google.oauth2 import service_account

app = FastAPI()

# Replace with your API Key and BigQuery target ID --> with enviroment variables
API_KEY = "YOUR_API_KEY"
BIGQUERY_TARGET_ID = "your-project-id.dataset.table"

# Replace with the path to your Google Cloud service account JSON key file
CREDENTIALS_PATH = "steadfast-canto-402015-279f991fefc3.json"

# Load Google Cloud credentials
credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
)

def fetch_weather_data(start_date, end_date, location, api_key):
    # Construct the URL with the provided parameters
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"

    params = {
        "unitGroup": "us",
        "key": api_key,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def process_weather_data(weather_data, location):
    total_temp = 0
    max_cloudcover = 0
    days_with_drizzle = 0
    days_with_fog = 0

    # Extracting relevant data from the response
    days_data = weather_data.get('days', [])

    total_days = len(days_data)

    start_date = days_data[0]['datetime'] if total_days > 0 else None
    end_date = days_data[-1]['datetime'] if total_days > 0 else None

    for day in days_data:
        total_temp += day['temp']
        if day['cloudcover'] is not None:
            max_cloudcover = max(max_cloudcover, day['cloudcover'])
        if "Drizzle" in day['conditions']:
            days_with_drizzle += 1
        if "Fog" in day['conditions']:
            days_with_fog += 1

    average_temp = total_temp / total_days if total_days > 0 else 0

    output_data = {
        "resolved_address": location,
        "start_date": start_date,
        "end_date": end_date,
        "query_cost": weather_data.get('queryCost', 0),
        "average_temp": average_temp,
        "max_cloudcover": max_cloudcover,
        "fog": days_with_fog > 0,
        "days_with_drizzle": days_with_drizzle,
        "runtime_timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    return output_data


def insert_into_bigquery(data, target_id, credentials):
    # Load credentials from a file
    pandas_df = pd.DataFrame([data])
    try:
        to_gbq(pandas_df, target_id, if_exists='append', credentials=credentials)
        print("Data inserted successfully")
    except Exception as e:
        print(f"An error occurred: {e}")


@app.get("/")
async def fetch_and_insert_weather_data(start_date: str, end_date: str, location: str):
    location = "Sterling%2C%20VA%2C%20US"

    # Fetch weather data
    weather_data = fetch_weather_data(start_date, end_date, location, API_KEY)

    # Process weather data
    processed_data = process_weather_data(weather_data, location)

    # Insert processed data into BigQuery
    insert_into_bigquery(processed_data, BIGQUERY_TARGET_ID, credentials)

    return {"message": "Weather data fetched, processed, and inserted into BigQuery."}

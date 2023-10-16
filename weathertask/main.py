import requests
import datetime
from pandas_gbq import to_gbq
import pandas as pd
from google.oauth2 import service_account


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


# Input-Parameter
start_date = "2022-01-01" # this should be env while deploying
end_date = "2022-01-31" # this should be env while deploying
location = "Berlin, Germany" # this should be env while deploying
api_key = "xxxxxxxxxxxxxxxxxxxxxxxx"
credentials = service_account.Credentials.from_service_account_file(
    'steadfast-canto-402015-279f991fefc3.json'
)
target_id = "steadfast-canto-402015.weather.weather_test"

if __name__ == '__main__':
    # call the functions
    weather_data = fetch_weather_data(start_date, end_date, location, api_key)
    processed_data = process_weather_data(weather_data, location)
    insert_into_bigquery(processed_data, target_id, credentials)

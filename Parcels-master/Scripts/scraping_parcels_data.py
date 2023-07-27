import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import requests
import time


def insert_log(cursor):
    """
    Inserts a log entry with the current timestamp into the 'logs' table.

    :param cursor: The SQLite cursor to execute the query.
    :return: None
    """
    utc_now = datetime.utcnow()
    local_timezone = timezone(timedelta(hours=4))
    local_time = utc_now.astimezone(local_timezone)
    timestamp_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO logs (log, timestamp) VALUES (?, ?)", ("", timestamp_str))
    cursor.connection.commit()


def scrape_data(lat, lng):
    """
    Scrape parcel data using the given latitude and longitude.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :return: JSON response containing scraped parcel data.
    """
    headers = {
        'X-Auth-Token': 'Yf-nJNNT33f8jiVZDKNS',
        'X-Auth-Email': 'adam.j@gmail.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.82',
    }
    url = f'https://green.parcels.id.land/parcels/parcels/by_location.json?lng={lng}&lat={lat}'
    response = requests.get(url, headers=headers)

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    return response_json


def modify_response_format(response_data):
    """
    Modifies the format of the JSON response.

    :param response_data: Original JSON response data.
    :return: Modified JSON response data.
    """
    return response_data.get("parcels", [])


def insert_data_into_db(response_str):
    """
    Inserts data into the SQLite database table 'results' and logs the insertion.

    :param response_str: The JSON response data in string format to be inserted.
    :return: None
    """
    conn = sqlite3.connect('../Parcels-master/parcel_data.db')
    cursor = conn.cursor()

    point_id = cursor.lastrowid
    cursor.execute("INSERT INTO results (origin_id, data) VALUES (?, ?)", (point_id, response_str))
    conn.commit()

    insert_log(cursor)
    conn.close()


def scrape_and_save_data(output_csv_path):
    """
    Scrapes data for latitude and longitude points from the 'input' table in the database,
    modifies the response format, and saves the scraped data to a new CSV file.

    :param output_csv_path: The file path to save the scraped data as a CSV file.
    :return: None
    """

    db_path = Path('../Parcels-master/parcel_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT lat, lng, status FROM input")
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=['lat', 'lng', 'status'])
    df['response'] = None

    for idx, row in df.iterrows():
        lat = row['lat']
        lng = row['lng']
        status = row['status']

        if status != 'TODO':

            continue

        df.at[idx, 'status'] = 'PROCESSING'
        conn.execute("UPDATE input SET status = 'PROCESSING' WHERE lat = ? AND lng = ?", (lat, lng))
        conn.commit()

        response_data = scrape_data(lat, lng)

        modified_response_data = modify_response_format(response_data)

        df.at[idx, 'response'] = modified_response_data

        df.at[idx, 'status'] = 'DONE'
        conn.execute("UPDATE input SET status = 'DONE' WHERE lat = ? AND lng = ?", (lat, lng))
        conn.commit()

        response_str = json.dumps(modified_response_data)  # Convert to JSON string for database insertion
        insert_data_into_db(response_str)
        conn.commit()

        time.sleep(0.5)

    df.to_csv(output_csv_path, index=False)

    conn.close()

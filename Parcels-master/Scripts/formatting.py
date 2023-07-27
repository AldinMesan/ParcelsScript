import json
import sqlite3
import pandas as pd


def parse_json_data(json_str):
    """
    Parses JSON data from a JSON-encoded string.

    :param json_str: JSON-encoded string to be parsed.
    :return: Parsed data as a Python list or dictionary.
    """
    try:
        if json_str.strip() == "":
            return None
        parsed_data = json.loads(json_str)
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def get_data_results(data):
    """
    Extracts relevant parcel data from the 'data' list and returns it as a dictionary.

    :param data: A list of dictionaries representing parcel data, where each dictionary contains a 'parcel_data' key.
    :return: A dictionary containing extracted parcel data.
    """
    if not data:
        # print("Parcel not found.")
        return {
            'parcel_id': None,
            'state': None,
            'apn': None,
            'acreage': None,
            'parcel_address': None,
            'parcel_owner': None,
            'POLYGON': None
        }
    else:
        # Assume the first entry in the data list is the relevant one
        parcel_data = data[0].get('parcel_data', {})
        return {
            'parcel_id': parcel_data.get('parcel_id', None),
            'state': parcel_data.get('state', None),
            'apn': parcel_data.get('apn', None),
            'acreage': parcel_data.get('acreage', None),
            'parcel_address': parcel_data.get('parcel_address', None),
            'parcel_owner': parcel_data.get('parcel_owner', None),
            'POLYGON': parcel_data.get('geom_as_wkt', None)
        }


def process_and_save_filtered_data(output_csv_filename):
    """
    Filters the data from the 'results' table, replaces empty lists with none, and saves it to a new CSV file.

    :param output_csv_filename: The name of the output CSV file to save the filtered data.
    :return: None
    """
    conn = sqlite3.connect('../Parcels-master/parcel_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT id, origin_id, data FROM results")
    data_rows = cursor.fetchall()

    processed_data = []
    for row in data_rows:
        row_id, origin_id, json_data = row

        data = parse_json_data(json_data)

        if data is None:
            data = []

        data_results = get_data_results(data)

        # Append the scraped data to processed_data list
        processed_data.append((
            row_id,
            origin_id,
            data_results['parcel_id'],
            data_results['state'],
            data_results['apn'],
            data_results['acreage'],
            data_results['parcel_address'],
            data_results['parcel_owner'],
            data_results['POLYGON']
        ))

    df = pd.DataFrame(processed_data,
                      columns=['id', 'origin_id', 'parcel_id', 'state', 'apn', 'acreage', 'parcel_address',
                               'parcel_owner', 'POLYGON'])

    df.to_csv(output_csv_filename, index=False)

    conn.close()

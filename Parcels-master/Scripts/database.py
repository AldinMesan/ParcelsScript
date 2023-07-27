import json
import sqlite3
import pandas as pd


def create_logs_and_results_tables():
    """
    Create the 'logs' and 'results' tables in the SQLite database if they don't exist.

    :return: None
    """
    conn = sqlite3.connect("parcel_data.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            log TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_id INTEGER,
            data TEXT,
            FOREIGN KEY (origin_id) REFERENCES input (id)
        )
    ''')

    conn.commit()
    conn.close()


def preprocess_json_string(json_str):
    """
    Replaces single quotes with double quotes in a JSON string.

    :param json_str: The JSON string to preprocess.
    :return: The preprocessed JSON string.
    """
    return json_str.replace("'", '"')


def parse_json_data(json_str):
    """
    Parses JSON data from a JSON-encoded string.

    :param json_str: JSON-encoded string to be parsed.
    :return: Parsed data as a Python list or dictionary.
    """
    try:
        parsed_data = json.loads(json_str)
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def parse_data_and_save_as_csv(output_csv_filename):
    """
    Fetches JSON data from the 'results' table, parses it, and saves it as a CSV file.

    :param output_csv_filename: The name of the output CSV file to save the parsed data.
    :return: None
    """
    conn = sqlite3.connect('../Parcels-master/parcel_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT id, origin_id, data FROM results")

    rows = cursor.fetchall()

    # Create a list to store parsed data
    parsed_data_list = []
    for row in rows:
        data_id, origin_id, json_data_str = row
        parsed_data = parse_json_data(json_data_str)
        if parsed_data is not None:
            parsed_data_list.append((data_id, origin_id, parsed_data))

    conn.close()

    # Convert the list of tuples into a DataFrame
    columns = ['id', 'origin_id', 'data']
    df = pd.DataFrame(parsed_data_list, columns=columns)

    df.to_csv(output_csv_filename, index=False)

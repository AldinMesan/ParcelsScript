from Scripts.database import create_logs_and_results_tables, parse_data_and_save_as_csv
from Scripts.formatting import process_and_save_filtered_data
from Scripts.scraping_parcels_data import scrape_and_save_data
from pathlib import Path
import pandas as pd
import sqlite3


def write_csv_to_table(csv_file_path, table_name):
    """
     Writes data from a CSV file to an SQLite table in the database.

    :param csv_file_path: The file path of the CSV file containing the data to be inserted.
    :param table_name: The name of the SQLite table where the data will be inserted.
    :return: None
    """
    conn = sqlite3.connect('../Parcels-master/parcel_data.db')
    cursor = conn.cursor()

    cursor.execute('''
            CREATE TABLE IF NOT EXISTS input (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT CHECK(status IN ('TODO', 'PROCESSING', 'DONE')),
                lat REAL,
                lng REAL
            )
        ''')

    df = pd.read_csv(csv_file_path)
    df['status'] = 'TODO'

    df.to_sql(table_name, conn, if_exists='append', index=False)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # input_csv_file1 = Path('./Input/parcel_lat_lng_data.csv')
    # write_csv_to_table(input_csv_file1, 'input')

    # Scrape parcel data and save it to the output CSV file
    # input_csv_file1 = Path('./Input/parcel_lat_lng_data.csv')
    create_logs_and_results_tables()
    output_csv_file1 = 'scraped_parcel_data.csv'
    # scrape_and_save_data(output_csv_file1)

    # Process the scraped data and save it to the database
    input_csv_file2 = Path('./scraped_parcel_data.csv')
    #process_and_save_data(input_csv_file2)

    # Formatting response from database and saving it to CSV file
    output_file = Path('./Output/formatted_parcel_data_json.csv')
    process_and_save_filtered_data(output_file)

    # output_csv_filename = 'parsed_data.csv'
    # parse_data_and_save_as_csv(output_csv_filename)

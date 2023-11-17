import pandas as pd
import numpy as np
import psycopg
import logging
import time
from tqdm import tqdm
from load_hss import write_invalid_rows_to_csv


def check_duplicate_id(curr, table_name, facility_id, date):
    """
    Check if there is a duplicate entry for the given facility ID and date in the specified table.

    Parameters:
    - curr: psycopg cursor
    - table_name: str, name of the table to check for duplicates
    - facility_id: str, facility ID to check for duplicates
    - date: date, date to check for duplicates

    Returns:
    - bool, True if a duplicate entry exists, False otherwise
    """
    query = f"SELECT COUNT(*) FROM {table_name} WHERE facility_id = %s AND data_date = %s"
    curr.execute(query, (facility_id, date))
    count = curr.fetchone()[0]
    return count > 0


def load_quality_data(csv_file, conn, date, invalid_file):
    """
    Load quality data from a CSV file into a PostgreSQL database.

    Parameters:
    - csv_file: str, path to the CSV file containing quality data
    - conn: psycopg connection, connection to the PostgreSQL database
    - date: str, date in 'YYYY-MM-DD' format

    Raises:
    - ValueError: If data loading fails
    """
    try:
        # Load CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        date = pd.to_datetime(date).date()

        # Do necessary processing on the DataFrame
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        df.replace({np.nan: None}, inplace=True)
        df.replace({'Not Available': 0}, inplace=True)

        df['hospital_overall_rating'] = df['hospital_overall_rating'].astype(float)
        df['emergency_services'] = df['emergency_services'].replace({'Yes': True, 'No': False})

        success_count = 0
        error_count = 0

        start_time = time.time()
        # Create a cursor and open a transaction
        with conn.cursor() as curr:
            # Insert data into the database
            insert_query = '''
                INSERT INTO HospitalQualityInformation (facility_id, hospital_overall_rating, emergency_services, hospital_type, hospital_ownership, data_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''

            quality_insert_df = []
            quality_invalid_ind = []

            for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing Rows"):
                # print(f"Processing row {index}")
                try:
                    # Skip if there's a duplicate for the given date and facility ID
                    if not check_duplicate_id(curr, 'HospitalQualityInformation', row['facility_id'], date):
                        quality_insert_df.append((row['facility_id'], row['hospital_overall_rating'], row['emergency_services'],
                            row['hospital_type'], row['hospital_ownership'], date))
                        success_count += 1
                    else:
                        logging.info(f"Skipping row {index} due to duplicate facility ID and date: {row['facility_id']}, {date}")
                        error_count += 1

                except Exception as e:
                    logging.error(f"Error inserting data for row {index}: {e}")
                    logging.error(f"Total rows processed: {len(df)}, Successful inserts: {success_count}, Errors: {error_count}")
                    quality_invalid_ind.append(index)

            # batch insert
            if quality_insert_df:
                curr.executemany(insert_query, quality_insert_df)

            end_time = time.time()
            logging.info("time: %s", end_time - start_time)

            # write to csv the invalid rows
            write_invalid_rows_to_csv(invalid_file, quality_invalid_ind, csv_file, df)

            # Commit the changes
            conn.commit()
            logging.info("Data loaded successfully.")

    except Exception as e:
        logging.error(f"Error: {e}")
        # Rollback the transaction in case of an error
        conn.rollback()
        raise ValueError("Data loading failed.") from e


if __name__ == "__main__":
    import sys

    if (len(sys.argv) != 3) and (len(sys.argv) != 4):
        logging.error("Usage: python load-quality.py <date> <csv_file>")
        sys.exit(1)

    date, csv_file = str(sys.argv[1]), str(sys.argv[2])
    if len(sys.argv) == 3:
        invalid_file = None
    else:
        invalid_file = str(sys.argv[3])

    try:
        with psycopg.connect(
                host="pinniped.postgres.database.azure.com",
                dbname="jihyoc",
                user="jihyoc",
                password="W0M4uzhKys"
        ) as conn:
            load_quality_data(csv_file, conn, date, invalid_file)
    except ValueError as ve:
        logging.error(ve)
    except psycopg.Error as e:
        logging.error(f"PostgreSQL error: {e}")

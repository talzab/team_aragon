import pandas as pd
import numpy as np
import psycopg
import time
import logging
from tqdm import tqdm


logging.basicConfig(level=logging.INFO)


def check_duplicate_id(curr, table_name, column_name, value, column_name2=None, value2=None):
    """
    Check if there is a duplicate entry in the specified table based on the provided column(s) and value(s).

    Parameters:
    - curr: psycopg cursor
    - table_name: str, name of the table to check for duplicates
    - column_name: str, name of the column to check for duplicates
    - value: value to check for in the specified column
    - column_name2: str (optional), name of the second column to check for duplicates (default is None)
    - value2: value (optional), value to check for in the second column (default is None)

    Returns:
    - bool, True if a duplicate entry exists, False otherwise
    """
    if column_name2 is None:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s"
        curr.execute(query, (value,))
    else:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s AND {column_name2} = %s"
        curr.execute(query, (value, value2))

    count = curr.fetchone()[0]
    return count > 0


def batch_insert_data(curr, table_name, column_names, data):
    """
    *Helper function for load_hhs_data() function
    Inserts batch of data to 'table_name' given the 'data'

    Parameters:
    - curr: psycopg connection, connection to the PostgresSQL database
    - table_name: str, table name
    - column_names: str list, list that contains column names of the table
    - data: str list list, list that contains a list of string values to insert to database
    """
    dummies = ', '.join(['%s'] * len(column_names))
    query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({dummies})"
    curr.executemany(query, data)


def load_hhs_data(csv_file, conn):
    """
    Load HHS (Health and Human Services) data from a CSV file into a PostgreSQL database.

    Parameters:
    - csv_file: str, path to the CSV file containing HHS data
    - conn: psycopg connection, connection to the PostgreSQL database

    Raises:
    - ValueError: If data loading fails
    """
    try:
        # Load CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return

    # Do necessary processing on the DataFrame (e.g., handle -999 values, parse dates)
    df.replace(-999999, None, inplace=True) 
    df.replace({np.nan: None}, inplace=True)

    df = df.astype(float, errors='ignore')
    df['collection_week'] = pd.to_datetime(df['collection_week'], format='%Y-%m-%d').dt.date

    hospitals_success_count = 0
    hospitals_error_count = 0

    hospitallocation_success_count = 0
    hospitallocation_error_count = 0

    hospitalbedinformation_success_count = 0
    hospitalbedinformation_error_count = 0

    start_time = time.time()
    try:
        # Create a cursor and open a transaction
        with conn.cursor() as curr:
            # Insert data into the database
            hospitals_data = []
            hospitallocations_data = []
            hospitalbedinformation_data = []

            for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing Rows"):
                # logging.info(f"Processing row {index}")

                try:
                    # Insert into Hospitals table
                    if not check_duplicate_id(curr, 'Hospitals', 'hospital_pk', row['hospital_pk']):
                        hospitals_data.append((row['hospital_pk'], row['hospital_name']))
                        hospitals_success_count += 1
                    else:
                        logging.info(f"Skipping row {index} due to duplicate ID in row: {row['hospital_pk']}")
                        hospitals_error_count += 1
                except Exception as e:
                    logging.error(f"Error inserting data into Hospitals for row {index}: {e}")

                try:
                    # Insert into HospitalLocations table
                    if not check_duplicate_id(curr, 'HospitalLocations', 'hospital_fk', row['hospital_pk']):
                        hospitallocations_data.append((row['hospital_pk'], row['state'], row['address'], row['city'], row['zip'], row['fips_code'], row['geocoded_hospital_address']))
                        hospitallocation_success_count += 1
                    else: 
                        logging.info(f"Skipping row {index} due to duplicate ID in row: {row['hospital_pk']}")
                        hospitallocation_error_count += 1
                except Exception as e:
                    logging.error(f"Error inserting data into HospitalLocations for row {index}: {e}")

                try:
                    # Insert into HospitalBedInformation table
                    if not check_duplicate_id(curr, 'HospitalBedInformation', 'hospital_fk', row['hospital_pk'], 'collection_week', row['collection_week']):
                        hospitalbedinformation_data.append((row['hospital_pk'], row['collection_week'], row['all_adult_hospital_beds_7_day_avg'], row['all_pediatric_inpatient_beds_7_day_avg'],
                                                            row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'], row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                                                            row['total_icu_beds_7_day_avg'], row['icu_beds_used_7_day_avg'], row['inpatient_beds_used_covid_7_day_avg'],
                                                            row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']))
                        hospitalbedinformation_success_count += 1
                    else: 
                        logging.info(f"Skipping row {index} due to duplicate ID and date in row: {row['hospital_pk']}, {row['collection_week']}")
                        hospitalbedinformation_error_count += 1
                except Exception as e:
                    logging.error(f"Error inserting data into HospitalBedInformation for row {index}: {e}")

            # Batch inserts
            batch_insert_data(curr, 'Hospitals', ['hospital_pk', 'hospital_name'], hospitals_data)
            batch_insert_data(curr, 'HospitalLocations', ['hospital_fk', 'state', 'address', 'city', 'zip', 'fips_code', 'geocoded_hospital_address'], hospitallocations_data)
            batch_insert_data(curr, 'HospitalBedInformation', ['hospital_fk', 'collection_week', 'all_adult_hospital_beds_7_day_avg', 'all_pediatric_inpatient_beds_7_day_avg',
                                                               'all_adult_hospital_inpatient_bed_occupied_7_day_coverage', 'all_pediatric_inpatient_bed_occupied_7_day_avg',
                                                               'total_icu_beds_7_day_avg', 'icu_beds_used_7_day_avg', 'inpatient_beds_used_covid_7_day_avg',
                                                               'staffed_icu_adult_patients_confirmed_covid_7_day_avg'], hospitalbedinformation_data)

            # Commit the changes
            conn.commit()
            logging.info("Data loaded successfully.")
            logging.info(f"Total rows processed: {len(df)}")
            logging.info(f"Successful Hospitals inserts: {hospitals_success_count}, Errors: {hospitals_error_count}")
            logging.info(f"Successful HospitalLocations inserts: {hospitallocation_success_count}, Errors: {hospitallocation_error_count}")
            logging.info(f"Successful HospitalBedInformation inserts: {hospitalbedinformation_success_count}, Errors: {hospitalbedinformation_error_count}")

    except Exception as e:
        logging.error(f"Error: {e}")
        # Rollback the transaction in case of an error
        conn.rollback()
        raise ValueError("Data loading failed.") from e

    end_time = time.time()
    logging.info("time: %s", end_time - start_time)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        logging.error("Usage: python load_hhs.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]

    conn = None
    try:
        conn = psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname="jihyoc",
            user="jihyoc",
            password="W0M4uzhKys"
        )
        load_hhs_data(csv_file, conn)
    except ValueError as ve:
        logging.error(ve)
    finally:
        # Close the database connection
        if conn:
            conn.close()

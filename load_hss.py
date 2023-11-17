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


def write_invalid_rows_to_csv(file_path, invalid_rows_ind, csv_file, data):
    """
    Write invalid rows to a CSV file.

    * Note that the CSV filepath is going to be 'invalid_data/given_csv_filename.csv' by default.
      However, if the user specified a filepath in the command line, then that filepath will be used.

    ex) Command line input: python ./loadd_hss.py ./hhs_data/2022-10-21-hhs-data.csv
        -> writes invalid csv file to: invalid_data/2022-10-21-hhs-data_invalid.csv

        Command line input: python ./load_hss.py ./hhs_data/2022-10-21-hhs-data-csv ./mydir/my_invalid_data_name.csv
        -> writes invalid csv file to: ./mydir/my_invalid_data_name.csv

    Parameters:
    - file_path: str, path to the CSV file
    - invalid_rows_ind: int list, list containing the indices of invalid rows in data
    - csv_file: str, path to the CSV file containing HHS data
    - data: pdDataFrame, data frame that is already preprocessed from reading csv_file
    """
    invalid_df = data.iloc[invalid_rows_ind]
    if file_path is None:
        new_file_path = "invalid_data/" + csv_file.split('/')[-1].split('.')[0] + "_invalid.csv"
        invalid_df.to_csv(new_file_path, index=False)
    else:
        invalid_df.to_csv(file_path, index=False)


def load_hhs_data(csv_file, conn, invalid_file):
    """
    Load HHS (Health and Human Services) data from a CSV file into a PostgreSQL database.

    Parameters:
    - csv_file: str, path to the CSV file containing HHS data
    - conn: psycopg connection, connection to the PostgreSQL database
    - invalid_file: str, path to the invalid rows CSV

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

    invalid_rows_index = []

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
                    invalid_rows_index.append(index)

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
                    invalid_rows_index.append(index)

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
                    invalid_rows_index.append(index)

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

    write_invalid_rows_to_csv(invalid_file, invalid_rows_index, csv_file, df)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        logging.error("Usage: python load_hhs.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    if len(sys.argv) == 2:
        invalid_file = None
    else:
        invalid_file = sys.argv[2]

    conn = None
    try:
        conn = psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname="jihyoc",
            user="jihyoc",
            password="W0M4uzhKys"
        )
        load_hhs_data(csv_file, conn, invalid_file)
    except ValueError as ve:
        logging.error(ve)
    finally:
        # Close the database connection
        if conn:
            conn.close()

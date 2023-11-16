import pandas as pd
import numpy as np
import psycopg


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


def load_quality_data(csv_file, conn, date):
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

        # Create a cursor and open a transaction
        with conn.cursor() as curr:
            # Insert data into the database
            for index, row in df.iterrows():
                print(f"Processing row {index}")
                try:
                    # Insert into HospitalQualityInformation table only if there's no duplicate for the given date and facility ID
                    if not check_duplicate_id(curr, 'HospitalQualityInformation', row['facility_id'], date):
                        curr.execute('''
                            INSERT INTO HospitalQualityInformation (facility_id, hospital_overall_rating, emergency_services, hospital_type, hospital_ownership, data_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        ''', (row['facility_id'], row['hospital_overall_rating'], row['emergency_services'],
                            row['hospital_type'], row['hospital_ownership'], date))
                        print(row['hospital_overall_rating'])
                        print(row['emergency_services'])
                        print(row['hospital_type'])
                        print(row['hospital_ownership'])
                        print(date)
                    else:
                        print(f"Skipping row {index} due to duplicate facility ID and date: {row['facility_id']}, {date}")

                except Exception as e:
                    print(f"Error inserting data for row {index}: {e}")

            # Commit the changes
            conn.commit()
            print("Data loaded successfully.")

    except Exception as e:
        print(f"Error: {e}")
        # Rollback the transaction in case of an error
        conn.rollback()
        raise ValueError("Data loading failed.") from e

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python load-quality.py <date> <csv_file>")
        sys.exit(1)

    date, csv_file = sys.argv[1], sys.argv[2]

    try:
        with psycopg.connect(
                host="pinniped.postgres.database.azure.com",
                dbname="talzaben",
                user="talzaben",
                password="klVgh!KCGA"
        ) as conn:
            load_quality_data(csv_file, conn, date)
    except ValueError as ve:
        print(ve)
    except psycopg.Error as e:
        print(f"PostgreSQL error: {e}")

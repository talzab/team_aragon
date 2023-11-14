import pandas as pd
import psycopg

def check_duplicate_id(cursor, table_name, id_column, value):
    # Check if the ID already exists in the specified table
    cursor.execute(f"SELECT 1 FROM {table_name} WHERE {id_column} = %s", (value,))
    return cursor.fetchone() is not None

def load_hhs_data(csv_file, connection):
    # Load CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    print("File read in...")

    # Do necessary processing on the DataFrame (e.g., handle -999 values, parse dates)
    df.replace(-999999, None, inplace=True) 
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    print("Preprocessing complete...")

    # Parse date columns if needed
    # df['collection_week'] = pd.to_datetime(df['collection_week'])

    # Create a cursor and open a transaction
    cursor = connection.cursor()

    cursor.execute('''
                   INSERT INTO Hospitals (hospital_pk, hospital_name, collection_week)
                    VALUES (%s, %s, %s)
                ''', ('029182', 'Hospital A', '2022-03-23'))
    print("inserted hospital")

    try:
        # Insert data into the database
        for index, row in df.iterrows():
            try:
                print("in here...")

                # Insert into Hospitals table
                cursor.execute('''
                    INSERT INTO Hospitals (hospital_pk, hospital_name, collection_week)
                    VALUES (%s, %s, %s)
                ''', (row['hospital_pk'], row['hospital_name'], row['collection_week']))

                # Insert into HospitalLocations table
                cursor.execute('''
                    INSERT INTO HospitalLocations (hospital_fk, state, address, city, zip, fips_code, geocoded_hospital_address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (row['hospital_pk'], row['state'], row['address'], row['city'], row['zip'], row['fips_code'], row['geocoded_hospital_address']))

                # Insert into HospitalBedInformation table
                cursor.execute('''
                    INSERT INTO HospitalBedInformation (hospital_fk, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, 
                        all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, 
                        total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, 
                        staffed_icu_adult_patients_confirmed_covid_7_day_avg)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (row['hospital_pk'], row['all_adult_hospital_beds_7_day_avg'], row['all_pediatric_inpatient_beds_7_day_avg'],
                    row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'], row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                    row['total_icu_beds_7_day_avg'], row['icu_beds_used_7_day_avg'], row['inpatient_beds_used_covid_7_day_avg'],
                    row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']))

            except Exception as e:
                print(f"Error inserting data for row {index}: {e}")
                print("Row causing the issue:", row)

        # Commit the changes
        connection.commit()

        cursor.execute("SELECT * FROM Hospitals limit 10")
        results = cursor.fetchall()
        for row in results:
            print(row)

        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error: {e}")
        # Rollback the transaction in case of an error
        connection.rollback()
        raise ValueError("Data loading failed.") from e
    finally:
        # Close the cursor
        cursor.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python load_hhs.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]

    try:
        # Connect to the PostgreSQL database
        connection = psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname="talzaben",
            user="talzaben",
            password="klVgh!KCGA"
        )

        load_hhs_data(csv_file, connection)
    except ValueError as ve:
        print(ve)
    finally:
        # Close the database connection
        if connection:
            connection.close()

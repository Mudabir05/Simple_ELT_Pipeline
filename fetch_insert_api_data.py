import requests
from datetime import datetime, timedelta
import json
import psycopg2
import pandas as pd
import sys

# Define the API version
api_version = "1.0"


with open('properties.json') as json_file:
    data = json.load(json_file)

# Select five properties
selected_properties = data[:5]  # Selects the first 5 properties for simplicity
# Extract the property codes
location_names = [prop['locationName'] for prop in selected_properties]
print("Selected Location Names:", location_names)

start_date = "2019-01-01"
end_date = "2019-01-14"
base_url = "https://helsinki-openapi.nuuka.cloud/api/v1.0/EnergyData/Daily/ListByProperty/"

energy_data = []

for name in location_names:
    params = {
        "Record": "locationName",
        'SearchString': name,
        'ReportingGroup': 'Electricity',
        "StartTime": start_date,
        "EndTime": end_date
    }

    # Make the GET request
    response = requests.get(base_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Return the JSON response
        energy_data.append(response.json())
    else:
        # Print an error message and return None
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        print(f"Error message: {response.text}")
    

print(energy_data)

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "adminpostgres"
DB_HOST = "localhost"
DB_PORT = "5432"

try : 
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="elt_pipeline_db",
        user="postgres",
        password="adminpostgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    print("Database connection established successfully.")

    # Check if the properties table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'energy_usage'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("The 'energy_usage' table exists.")
    else:
        print("The 'energy_usage' table does not exist. Exiting.")
        sys.exit(1)  # Exit the script if the table does not exist


    # Assuming `conn` is your database connection and `cursor` is your cursor
    for record in energy_data:
        for item in record:  
            cursor.execute("""
                INSERT INTO energy_usage (locationName, timestamp, energyUsage)
                VALUES (%s, %s, %s)""", 
                (
                item['locationName'],  # propertyCode from the API response
                item['timestamp'],  # Each day's timestamp
                item['value']  # The energy usage value for that day
            ))

    # Commit the transaction
    conn.commit()

except psycopg2.Error as e:
    print(f"An error occurred: {e}")
    sys.exit(1)  # Exit the script if there's a database error

finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")    




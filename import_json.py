import json
import psycopg2
import pandas as pd
import sys

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
            AND table_name = 'properties'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("The 'properties' table exists.")
    else:
        print("The 'properties' table does not exist. Exiting.")
        sys.exit(1)  # Exit the script if the table does not exist







    # Load JSON data
    with open('properties.json') as json_file:
        data = json.load(json_file)


    # # Insert data into the properties table
    for item in data:
        cursor.execute("""
            INSERT INTO properties ( locationName, propertyName, propertyCode)
            VALUES (%s, %s, %s) """, 
            (
            item.get('locationName'),
            item.get('propertyName'),
            item.get('propertyCode'), 
            ))

    # # Commit the transaction and close the connection
    conn.commit()
    print("Data inserted successfully into the 'properties' table.")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")
    sys.exit(1)  # Exit the script if there's a database error

finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")
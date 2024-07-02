# For this project, I assume to be a data engineer at a data
# consulty company, and I am assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas.
# Each highway is operated by a different toll operator with a different IT setup that uses different file formats. 
# My job is to collect data available in different formats and consolidate it into a single file.

# Import necessary libraries
import os
import sys, argparse, csv
import tarfile
import pandas as pd
import glob
import csv
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
from datetime import timedelta

# Define the paths to the csv, tsv, fixed width and consolidated data.
csv_data = '/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/vehicle-data.csv'
tsv_data = '/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/tollplaza-data.csv'
fixed_width_data = '/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/payment-data.csv'
final_csv_file = '/Users/jbshome/Desktop/python-etl-pipeline/files/final_csv/final_csv_file.csv'
cleaned_final_csv_file = '/Users/jbshome/Desktop/python-etl-pipeline/files/final_csv/cleaned_final_csv_file.csv'

# Function to unzip data
def unzip_data():
    # Define paths
    source_file = '/Users/jbshome/Desktop/python-etl-pipeline/staging/tolldata.tgz'
    destination_folder = '/Users/jbshome/Desktop/python-etl-pipeline/destination_folder'

    with tarfile.open(source_file, 'r:gz') as tar:
            tar.extractall(destination_folder)
            print(f"Extraction complete. Files extracted to {destination_folder}")

    return

# call the function
unzip_data()

# Function to extract data from the csv file
def extract_data_from_csv():
    csv_file_path = '/Users/jbshome/Desktop/python-etl-pipeline/files/destination_folder/vehicle-data.csv'

    # The data does not have column name, so I will specify my own column names.
    column_names = ['Rowid', 'Timestamp', 'Anonymized_vehicle_number', 'Vehicle_type', 'Number_of_axles', 'Vehicle_code']

    # Reading the csv file.
    data = pd.read_csv(csv_file_path, names=column_names)
    
    # For this data, we only need the first 4 columns. That is why i first named the columns to safely remove the columns I do not need.
    data = data.drop(['Number_of_axles', 'Vehicle_code'], axis=1)

    # Save data to a csv file
    data.to_csv(csv_data, index=False)
    
    return data

# call the function
extract_data_from_csv()

# Function to extract data from the tsv file
def extract_data_from_tsv():
    tsv_file_path = '/Users/jbshome/Desktop/python-etl-pipeline/files/destination_folder/tollplaza-data.tsv'

    # The data does not have column names, so I will specify my own column names to safely remove the data I do not need.
    column_names = ['Rowid', 'Timestamp', 'Anonymized_vehicle_number', 'Vehicle_type', 'Number_of_axles', 'Tollplaza_id', 'Tollplaza_code']

    # Reading the tsv file.
    data = pd.read_csv(tsv_file_path, delimiter='\t', names=column_names)
    
    # For this data, we only need the last three columns, so I am removing the first 4 columns because they already exist in the csv data.
    data = data.drop(['Rowid', 'Timestamp', 'Anonymized_vehicle_number', 'Vehicle_type'], axis=1)
    
    # Save data to a csv file
    data.to_csv(tsv_data, index=False)
    
    return data
# call the function
extract_data_from_tsv()

# Function to extract data from the txt file
def extract_data_from_txt():
    txt_file_path = '/Users/jbshome/Desktop/python-etl-pipeline/files/destination_folder/payment-data.txt'
    # Defining column widths.
    colspecs = [(57, 61), (62, -1)]
    # Defining the column names because they are not included in the file.
    column_names = ['Type_of_payment', 'Vehicle Code']
    # Read the fixed-width file
    data = pd.read_fwf(txt_file_path, index=False, colspecs=colspecs, names=column_names)
    
    # Save data to a csv file
    data.to_csv(fixed_width_data, index=False)

    return data

# call the function
extract_data_from_txt()

# Function to consolidate the newly created csv files into a single file
def consolidate_data():

    # Read the csv files
    csv_data = pd.read_csv('/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/vehicle-data.csv')
    tsv_data = pd.read_csv('/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/tollplaza-data.csv')
    fixed_width_data = pd.read_csv('/Users/jbshome/Desktop/python-etl-pipeline/files/files_to_csv/payment-data.csv')

    # Merge the data
    data = pd.concat([csv_data, tsv_data, fixed_width_data], axis=1)

    # Save the data to a csv file
    data.to_csv(final_csv_file, index=False)

    return data

# call the function
consolidate_data()

# Function to transform the data
# With this data, I only need to transform by making the column vehicle type to uppercase and add _ to the Rowid column and vehicle code.
def transform_data(file_path):
    df = pd.read_csv(file_path)
    df['Vehicle_type'] = df['Vehicle_type'].str.upper()
    df = df.rename(columns={'Rowid': 'Row_id', 'Vehicle Code': 'Vehicle_code'})
    df.to_csv(cleaned_final_csv_file, index=False)
    return df

# Call the function
print(transform_data(final_csv_file))

# Function to load data to a database
def load_data():
    db_params = {
        'host': 'localhost',
        'database': 'toll_data',
        'user': 'postgres',
        'password': 'D2racine4ac#',
        'port': '5432'
    }
    # Connect to the database 
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create the table
    cur.execute("""
        CREATE TABLE toll_data_01 (
                Row_id INT,
                Timestamp TIMESTAMP,
                Anonymized_vehicle_number INT,
                Vehicle_type VARCHAR(255),
                Number_of_axles INT,
                Tollplaza_id INT,
                Tollplaza_code VARCHAR(255),
                Type_of_payment VARCHAR(255),
                Vehicle_code VARCHAR(255) 
        );
    """)

    # Open the cleaned csv file
    with open(cleaned_final_csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row
        for row in reader:
            cur.execute(
                "INSERT INTO toll_data_01 (Row_id, Timestamp, Anonymized_vehicle_number, Vehicle_type, Number_of_axles, Tollplaza_id, Tollplaza_code, Type_of_payment, Vehicle_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                row
            )

    # Commit the transaction
    conn.commit()

    # Close the connection
    cur.close()
    conn.close()

    print('Data loaded to the database successfully')

    return

# Call the function
# load_data()
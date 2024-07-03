# Project Writeup

A semi-automated bash+python pipeline to extract, transform, and load data to a Postgres database a tgz file with multiple different files such as csv, tsv, and txt.

- unzips the tgz file to retrieve multiple files
- Extracts data from the files, csv, tsv, and txt
- concatenates them into one csv file
- transfoms the csv file 
- loads the transfomed data to Postgres

# Scenario

A compressed archive file that contains multiple files such as a csv file, tsv file, and a txt file. These files have fictional data. I assume that I am an etl developer at a company, and they want me to build an etl pipeline to make the data in the tgz file avaible for the analytic team in a postgres database. 

# The Process(Unzipping the file)

THE UNZIP FUNCTION

The function starts by specifying the source file path and the destination folder. It attempts to open and extract the contents of the tarball file to the specified destination folder, logging successful access to the source file and successful extraction to the destination folder. The function includes error handling to manage potential issues: it logs an error if the source file is not found (FileNotFoundError), if there is a problem reading the tar file (tarfile.ReadError), or if any other unexpected error occurs, logging the exception message. The function concludes by calling unzip_data to execute the defined extraction process.

THE EXTRACT CSV FILE FUNCTION

This code defines a function called extract_data_from_csv which is designed to read data from a specified CSV file, process it, and save the processed data to a new CSV file. The function begins by defining the path to the CSV file and specifying the column names. It reads the CSV file into a pandas DataFrame, logging a success message upon successful reading. The function then drops two unnecessary columns from the DataFrame and saves the remaining data to a new CSV file, logging another success message upon completion. Error handling is incorporated to manage potential issues such as the file not being found (FileNotFoundError), errors in parsing the CSV file (pd.errors.ParserError), and any other unexpected errors, logging the appropriate messages for each case. Finally, the function is called to execute the extraction process.

"""
THE EXTRACT TSV FILE AND THE EXTRACT TXT FILE FUNCTIONS COMPLETE SIMILAR JOBS AS THE EXTRACT CSV FILE FUNCTION!!!
"""

THE CONCATENATE FUNCTION
This code defines a function called consolidate_data that reads data from multiple CSV files, merges the data, and saves the consolidated data into a single CSV file. The function starts by reading three CSV files into pandas DataFrames, each representing different data sources (vehicle data, toll plaza data, and payment data). It then concatenates these DataFrames along the columns to combine them into a single DataFrame, logging a success message when the merge is successful. The consolidated DataFrame is then saved to a new CSV file, and another success message is logged. Error handling is implemented to manage cases where files are not found (FileNotFoundError), parsing errors occur (pd.errors.ParserError), and any other unexpected errors, logging appropriate messages for each scenario. Finally, the function is called to execute the data consolidation process.

THE TRANSFORMAION FUNCTION
The function reads data from a specified file path into a pandas DataFrame and logs a success message upon successful reading. It transforms the data by converting the values in the Vehicle_type column to uppercase and renaming the Rowid column to Row_id and Vehicle Code to Vehicle_code. The transformed DataFrame is then saved to a new CSV file, logging a message to indicate successful parsing and saving. The function also includes error handling to manage scenarios such as an empty CSV file (pd.errors.EmptyDataError), parsing errors (pd.errors.ParserError), and other unexpected errors, logging appropriate error messages for each case. Finally, the function is called to execute the transformation process, and the transformed DataFrame is printed.

THE LOAD FUNCTION
The function begins by setting up the database connection parameters, including host, database name, user, password, and port. It then attempts to establish a connection to the database and create a cursor object for executing SQL commands, logging a success message upon establishing the connection.

The function proceeds to create a table named toll_data_01 with specified columns if it doesn't already exist. It logs the success of the table creation or acknowledges if it already exists. Then, it opens the cleaned CSV file and reads its contents, skipping the header row. For each row in the CSV file, the function inserts the data into the toll_data_01 table and logs a success message for the data insertion.

After all data has been inserted, the function commits the transaction to the database and logs the transaction's success. It includes error handling to manage database-related errors (psycopg2.DatabaseError), cases where the CSV file is not found (FileNotFoundError), and any other unexpected errors, logging appropriate messages for each scenario. Finally, the function ensures that the database connection is closed, logging the closure, and returns from the function.

LASTLY
I created a bash script to make running the script easy.
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

FIRST FUNCTION

The function starts by specifying the source file path and the destination folder. It attempts to open and extract the contents of the tarball file to the specified destination folder, logging successful access to the source file and successful extraction to the destination folder. The function includes error handling to manage potential issues: it logs an error if the source file is not found (FileNotFoundError), if there is a problem reading the tar file (tarfile.ReadError), or if any other unexpected error occurs, logging the exception message. The function concludes by calling unzip_data to execute the defined extraction process.

SECOND FUNCTION

This code defines a function called extract_data_from_csv which is designed to read data from a specified CSV file, process it, and save the processed data to a new CSV file. The function begins by defining the path to the CSV file and specifying the column names. It reads the CSV file into a pandas DataFrame, logging a success message upon successful reading. The function then drops two unnecessary columns from the DataFrame and saves the remaining data to a new CSV file, logging another success message upon completion. Error handling is incorporated to manage potential issues such as the file not being found (FileNotFoundError), errors in parsing the CSV file (pd.errors.ParserError), and any other unexpected errors, logging the appropriate messages for each case. Finally, the function is called to execute the extraction process.

THE THIRD AND FOURTH FUNCTIONS COMPLETE A SIMILAR JOB AS THE SECOND FUNCTION

THE FIFTH FUNCTION

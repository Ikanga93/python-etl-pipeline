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

# First function
The function starts by specifying the source file path and the destination folder. It attempts to open and extract the contents of the tarball file to the specified destination folder, logging successful access to the source file and successful extraction to the destination folder. The function includes error handling to manage potential issues: it logs an error if the source file is not found (FileNotFoundError), if there is a problem reading the tar file (tarfile.ReadError), or if any other unexpected error occurs, logging the exception message. The function concludes by calling unzip_data to execute the defined extraction process.


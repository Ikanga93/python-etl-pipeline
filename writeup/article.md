# Project Writeup

A semi-automated bash+python pipeline to extract, transform, and load data to a Postgres database a tgz file with multiple different files such as csv, tsv, and txt.

- unzips the tgz file to retrieve multiple files
- Extracts data from the files, csv, tsv, and txt
- concatenates them into one csv file
- transfoms the csv file 
- loads the transfomed data to Postgres

# Scenario

A compressed archive file that contains multiple files such as a csv file, tsv file, and a txt file. These files have fictional data. I assume that I am an etl developer at a company, and they want me to build an etl pipeline to make the data in the tgz file avaible for the analytic team in a postgres database. 

# The Process


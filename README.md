# ETL Pipeline Portfolio

Welcome to my pipeline project!

## Project ([writeup](./writeup/article.md))

### python-etl-pipeline
A python ETL pipeline to extract, transform, and load data to a Postgres database a tgz file with multiple different files such as csv, tsv, and txt.

- unzips the tgz file to retrieve multiple files
- extracts data from csv, tsv, and txt
- concatenates them into one csv file
- transfoms the csv file 
- loads the transfomed data to Postgres
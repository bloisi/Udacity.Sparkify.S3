# Project Data Lake

This project reads data from JSON files to load into a parquet files. Source and target data are hosted on a AWS S3. 

# etl.py

This file has all functions to read JSON files and create parquet files on data lake. 

* process_song_data: reads data from song_data JSON files and creates songs and artists parquet files. 

* process_log_data: reads data from log_data JSON files and creates users, time and sonplays parquet files.

* main: main function executes all process and has S3 paths.

# Obs

The project can run in fast mode (some files) or complete mode (all files). You should select (comments)
the S3 path input files. 
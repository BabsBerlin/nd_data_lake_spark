## Spark Data Lake Project

### Introduction
For this project I build an **ETL pipeline for a Data Lake hosted on S3**. It was part of my *[Udacity Nanodegree in Data Engineering](https://www.udacity.com/course/data-engineer-nanodegree--nd027)*.

### The Task
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


### The Dataset
The database is based on two datasets. 
- **The song dataset** `(s3://udacity-dend/song_data)` is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
- **The logfile dataset** `(s3://udacity-dend/log_data)` consists of log files in JSON format generated by an [Event Simulator](https://github.com/Interana/eventsim) based on the songs in the song dataset. It simulates activity logs from a music streaming app based on specified configurations. The log files are partitioned by year and month.

### The Database Schema

The database (db) is modeled after the star schema. [The Star Schema](https://en.wikipedia.org/wiki/Star_schema) separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. 

For the Sparkify database we have the 'songplays' table as the fact table and the 'songs', 'artists', 'users', and 'time' tables as dimension tables.

### The Project Files

- `etl.py` contains code to extract the log files from S3, process them via Spark und load them back to S3 
- `dl.cfg` contains the parameters to connect to the AWS workspace. For security reasons this file does provide the structure, but no real values.

### Project Steps and how to run the project 

1. create an AWS ACCESS KEY and update AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in dl.cfg
2. create a output bucket in AWS S3
3. update the file path to the output bucket in etl.py line 126
4. run python etl.py in the terminal
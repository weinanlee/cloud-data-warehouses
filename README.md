# Project 3: Data Warehouse


## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Project

In this project, the goal is to apply Spark and data lakes to build an ETL pipeline for a database hosted on Redshift. To complete the project, the steps are as follows:

1. Load data from S3 to staging tables on Redshift.
2. Execute SQL statements that create the analytics tables from these staging tables.


## Files

- create_table.py:  create the fact and dimension tables for the star schema in Redshift.
- etl.py: load data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift.
- sql_queries.py: define the SQL statements, which will be imported into the two other files above.


## Datasets
 The two datasets reside in S3. Here are the S3 links for each:
 ```
 - Song data: s3://udacity-dend/song_data
 - Log data: s3://udacity-dend/log_data
 
 Log data json path: s3://udacity-dend/log_json_path.json
 ```


### Songs metadata

The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### User activity logs

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what a single activity log in 2018-11-13-events.json, looks like.

```
{"artist":null,"auth":"Logged In","firstName":"Kevin","gender":"M","itemInSession":0,"lastName":"Arellano","length":null,"level":"free","location":"Harrisburg-Carlisle, PA","method":"GET","page":"Home","registration":1540006905796.0,"sessionId":514,"song":null,"status":200,"ts":1542069417796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.125 Safari\/537.36\"","userId":"66"}
```

## ETL Processes

### Songs metadata
### Staging tables

#### 1. staging_events

| staging_events | | |
|---|---|---|
artist |       VARCHAR
auth  |       VARCHAR
firstName |    VARCHAR
gender |        VARCHAR
itemInSession | INTEGER
lastName |     VARCHAR
length |       NUMERIC
level |         VARCHAR
location |      VARCHAR
method |      VARCHAR
page |         VARCHAR
registration | FLOAT
sessionId |     INTEGER | SORTKEY DISTKEY
song |          VARCHAR
status |        INTEGER
ts |           BIGINT
userAgent |     VARCHAR
userId |        INTEGER

#### 2. staging_songs
|staging_songs| | |
|---|---|---|
num_songs |        INTEGER 
artist_id |        VARCHAR | SORTKEY DISTKEY
artist_latitude | VARCHAR
artist_longitude  |VARCHAR
artist_location |  VARCHAR
artist_name |     VARCHAR
song_id |          VARCHAR
title  |          VARCHAR
duration |        NUMERIC
year |            INTEGER

### Dimensional Tables

#### 1: songs table

- songs - songs in music database

| songs | | | |
|---|---|---|---|
song_id | VARCHAR | PRIMARY KEY | SORTKEY
title | VARCHAR 
artist_id | VARCHAR
year | INTEGER
duration | FLOAT


#### 2: artists table

- artists - artists in music database

| artists | | | |
|---|---|---|---|
artist_id | VARCHAR | PRIMARY KEY | SORTKEY
name | VARCHAR
location | VARCHAR
latitude | VARCHAR
longitude | VARCHAR

### User activity logs

#### 3: time table

-  time - timestamps of records in songplays broken down into specific units

| time | | | | |
|---|---|--|--|--|
start_time | TIMESTAMP | PRIMARY KEY | DISTKEY | SORTKEY|
hour | INTEGER
day | INTEGER
week | INTEGER
month | INTEGER
year | INTEGER
weekday | VARCHAR


#### 4: users table
- users - users in the app

| users | | |
|---|---|---|
user_id | INTEGER | PRIMARY KEY
first_name | VARCHAR
last_name | VARCHAR
gender | VARCHAR
level | VARCHAR

### Fact Table

#### 5: songsplays table
- songplays - records in log data associated with song plays i.e. records with page NextSong

| songplays | | |
|---|---|---|
songplay_id | INTEGER IDENTITY(0,1) | PRIMARY KEY
start_time | TIMESTAMP | FOREIGN KEY
user_id | VARCHAR | FOREIGN KEY
level | VARCHAR
song_id | VARCHAR | FOREIGN KEY
artist_id | VARCHAR | FOREIGN KEY
session_id | INTEGER
location | VARCHAR
user_agent | VARCHAR




## Usage

1. Use create_tables.py to create fact and dimension tables for the star schema in Redshift.
2. Use etl.py to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
```
$ python create_tables.py
$ python etl.py
```


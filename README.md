# Sparkify Data Lake

Welcome to our Sparkify Data Lake documentation!

## About this project

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

This project implements a scalable and cost effective solution to manage Sparkify's growing data requirements while
continuing to enable analysts to access crucial data for understanding Sparkify usage. The solution processes raw JSON
files stored in S3 and writes the cleaned and modeled data back to a different S3 bucket in the columnar Parquet format.
The cleaned S3 bucket brings together user activity data from the Sparkify app (log_data) with details about our music catalog (song_data). Both of these datasets are extracted from the raw data files, transformed to make the data as useful as possible for analysis, and loaded into fact and dimension tables to ensure efficient processing of analytic queries.

The Sparkify schema is designed to allow analysts to efficiently query the database while keeping the number of joins required to a minimum. Analysts can start their queries on the songplays table, joining the dimension tables as needed to get the information they need. Analysts can query the Parquet files directly using AWS Athena, or by using Pandas or Spark dataframes.

## Getting started with the project

Prerequisites: You must have access to a Spark cluster with the following requirements:

- Spark 2.4.0
- Hadoop 2.8.5
- YARN with Ganglia 3.7.2 and Zeppelin 0.8.0

1. Clone or fork the repository.
1. Add you AWS access key and secret access key to `dl.cfg`. The account must have permissions to read and write to S3.
1. At the command prompt transfer `etl.py` and `dl.cfg`.

```bash
{
  scp etl.py hadoop@ec2-52-38-49-185.us-west-2.compute.amazonaws.com:/home/hadoop &&
  scp dl.cfg hadoop@ec2-52-38-49-185.us-west-2.compute.amazonaws.com:/home/hadoop
}
```

1. Run `etl.py`.

```bash
  spark-submit etl.py
```

## Source data

The source data for this project is two collections of JSON files from the Sparkify music streaming app stored in AWS S3 buckets.

### song_data

A set of directories and JSON files with information about the songs available in the Sparkify app. This dataset is a subset of song data. Each file contains song and artist metadata.

Sample song record:

```
{
  "num_songs": 1,
  "artist_id": "ARULZCI1241B9C8611",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Luna Orbit Project",
  "song_id": "SOSWKAV12AB018FC91",
  "title": "Midnight Star",
  "duration": 335.51628,
  "year": 0
}
```

### log_data

A set of directories and JSON files that contain logs on user activity on the Sparkify app.

Sample log record:

```
{
    "artist":"Deas Vail",
    "auth":"Logged In",
    "firstName":"Elijah",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Davis",
    "length":237.68771,
    "level":"free",
    "location":"Detroit-Warren-Dearborn, MI",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540772343796,
    "sessionId":985,
    "song":"Anything You Say (Unreleased Version)",
    "status":200,
    "ts":1543607664796,
    "userAgent":"\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4\"",
    "userId":"5"
}
```

## Data Lake files

The following files implement the extract, transform, load (ETL) pipeline for the Sparkify data model:

- `etl.py` - Runs the ETL process to read data from the `udacity-dend` S3 bucket, applies the required transformations using Apache Spark, and writes the transformed data to the `udacity-sparkify-music-dl` S3 bucket. The finalized data model tables are stored in the Parquet columnar file format.
- `dl.config`- Contains configuration settings for connecting to AWS S3.

## Data Model

### Songplays

Fact table that contains a row for each song played.

| column      | type      |
| ----------- | --------- |
| songplay_id | INTEGER   |
| start_time  | TIMESTAMP |
| user_id     | INTEGER   |
| level       | STRING    |
| song_id     | STRING    |
| artist_id   | STRING    |
| session_id  | INTEGER   |
| location    | STRING    |
| user_agent  | STRING    |

### Users

Dimension table that contains a row for each Sparkify user.

| column     | type    |
| ---------- | ------- |
| user_id    | INTEGER |
| first_name | STRING  |
| last_name  | STRING  |
| gender     | STRING  |
| level      | STRING  |

### Songs

Dimension table that contains a row for each song in the Sparkify catalog.

| column    | type    |
| --------- | ------- |
| song_id   | STRING  |
| title     | STRING  |
| artist_id | STRING  |
| year      | INTEGER |
| duration  | FLOAT   |

### Artists

Dimension table that contains a row for each artist in the Sparkify catalog.

| column    | type   |
| --------- | ------ |
| artist_id | STRING |
| name      | STRING |
| location  | STRING |
| latitude  | DOUBLE |
| longitude | DOUBLE |

### Time

Dimension table that contains a row for each timestamp, with columns with preprocessed dimensions including day, month, and year to simplify and optimize date-based analytic queries.

| column     | type      |
| ---------- | --------- |
| start_time | TIMESTAMP |
| hour       | INTEGER   |
| day        | INTEGER   |
| week       | INTEGER   |
| month      | INTEGER   |
| year       | INTEGER   |
| weekday    | INTEGER   |

## Sample queries

1. Number of and percent of paid vs. free users by week

```
WITH free_vs_paid AS (
	SELECT
	  user_id,
	  start_time,
	  CASE WHEN "level" = 'paid' THEN 1 ELSE 0 END AS paid_user,
	  CASE WHEN "level" = 'free' THEN 1 ELSE 0 END AS free_user
	FROM songplays.parquet
)
SELECT
	week,
	SUM(paid_user) AS paid_users,
	SUM(free_user) AS free_users,
	ROUND(SUM(paid_user) * 100.0 / (SUM(free_user) + SUM(paid_user)), 1) as pct_paid
FROM free_vs_paid
INNER JOIN time ON free_vs_paid.start_time = time.parquet.start_time
GROUP BY week
ORDER BY week
```

2. Top 10 most active paid users by songs played

```
SELECT
first_name || ' ' || last_name AS username,
COUNT(songplay_id) AS songs_played
FROM songplays.parquet
INNER JOIN users.parquet ON songplays.parquet.user_id = users.user_id
WHERE users.level = 'paid'
GROUP BY first_name || ' ' || last_name
ORDER BY songs_played DESC
LIMIT 10
```

3. Top 5 artists by number of songs played

```
SELECT
	name AS artist,
	COUNT(songplay_id) AS songs_played
FROM songplays.parquet
INNER JOIN artists.parquet ON songplays.parquet.artist_id = artists.parquet.artist_id
GROUP BY name
ORDER BY songs_played DESC
LIMIT 5
```

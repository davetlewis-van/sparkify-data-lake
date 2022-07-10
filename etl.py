"""Sparkify Data Lake ETL processing script."""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (udf, row_number,
                                   year, month, dayofmonth, hour,
                                   weekofyear, date_format, dayofweek)
from pyspark.sql.types import (StructField, StructType, StringType,
                               DoubleType, FloatType, IntegerType,
                               TimestampType, DateType, LongType)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create and return a SparkSession object."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from the `song_data` S3 bucket.

    Write `songs` and `artists` data to parquet files in S3.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # define song_data schema with required data types
    song_data_schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
        ])

    # read song data file
    song_data_df = spark.read.json(song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = song_data_df.select(
                     song_data_df.song_id,
                     song_data_df.title,
                     song_data_df.artist_id,
                     song_data_df.duration,
                     song_data_df.year).dropDuplicates('song_id')

    # write songs table to parquet files partitioned by year and artist
    songs_table.repartition("year", "artist_id") \
        .write.partitionBy("year", "artist_id") \
        .parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = song_data_df \
        .select(song_data_df.artist_id,
                song_data_df.artist_name.alias("name"),
                song_data_df.artist_location.alias("location"),
                song_data_df.artist_latitude.alias("latitude"),
                song_data_df.artist_longitude.alias("longitude")) \
        .dropDuplicates('artist_id')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet",
                                mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Load data from the `log_data` S3 bucket.

    Write `songs` and `artists` data to parquet files in S3.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_data_df = spark.read.json(log_data)

    # # filter by actions for song plays
    log_data_df = log_data_df.filter(log_data_df.page == 'NextSong')

    # extract columns for users table
    users_table = log_data_df \
        .select(log_data_df.userId.alias("user_id"),
                log_data_df.firstName.alias("first_name"),
                log_data_df.lastName.alias("last_name"),
                log_data_df.gender,
                log_data_df.level) \
        .dropDuplicates('user_id')

    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),
                        TimestampType())
    log_data_df = log_data_df \
        .withColumn("timestamp", get_timestamp(log_data_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),
                       DateType())
    log_data_df = log_data_df \
        .withColumn("datetime", get_timestamp(log_data_df.ts))

    # extract columns to create time table
    time_table = log_data_df \
        .select('ts', 'datetime', 'timestamp') \
        .withColumn("hour", hour(log_data_df.timestamp)) \
        .withColumn("day", dayofmonth(log_data_df.timestamp)) \
        .withColumn("week", weekofyear(log_data_df.timestamp)) \
        .withColumn("month", month(log_data_df.timestamp)) \
        .withColumn("year", year(log_data_df.timestamp)) \
        .withColumn("weekday", dayofweek(log_data_df.timestamp)) \
        .withColumn("start_time", log_data_df.timestamp) \
        .dropDuplicates('start_time')

    time_table.repartition("year", "month") \
        .write.partitionBy("year", "month") \
        .parquet(output_data + "time.parquet", mode="overwrite")

    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    sp = log_data_df.join(song_df,
                          (log_data_df.song == song_df.title) &
                          (log_data_df.artist == song_df.artist_name),
                          "left")
    songplays_table = sp.select(sp.timestamp.alias("start_time"),
                                sp.userId.alias("user_id"),
                                sp.level,
                                sp.song_id,
                                sp.artist_id,
                                sp.sessionId.alias('session_id'),
                                sp.location,
                                sp.userAgent.alias('user_agent'))

    window = Window.orderBy(songplays_table.start_time)
    songplays_table = songplays_table \
        .withColumn('songplay_id', row_number().over(window)) \
        .withColumn('month', month(songplays_table.start_time)) \
        .withColumn('year', year(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
        .partitionBy("year", "month") \
        .parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    """
    Create a connection to the Spark instance and run the ETL process.

    Read song and event data from S3, processes the JSON files with 
    Spark, and then writes the data model tables back to a different
    S3 bucket in the Parquet file format.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity-sparkify-music-dl/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

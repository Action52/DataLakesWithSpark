# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, StringType, TimestampType, DateType, DatetimeConverter
from datetime import datetime
import os, configparser

@udf(TimestampType())
def to_timestamp(ts):
    """
    This udf function parses a raw timestamp into a readable datetime.
    :param ts: Raw unix timestamp
    """
    timestamp = datetime.fromtimestamp(ts // 1000)
    return timestamp


@udf(DateType())
def to_datetime(ts):
    """
    This udf function returns, instead of a TimestampType, a DateType.
    :param ts: Raw unix timestamp.
    :return:
    """
    dt = datetime.fromtimestamp(ts // 1000)
    return dt


def process_song_data(spark, s3_input_path, s3_output_path):
    """
        This function processes the song data to create the song and artists tables.
        :param spark: Instanciated SparkSession.
        :param s3_input_path: Input path to s3 song data files.
        :param s3_output_path: Output path to s3 song data files.
    """
    print("Reading jsons. Please stand by.")
    songdf = spark.read.json(s3_input_path)
    print("Done.")
    # Casting some of the fields to IntegerType
    songdf = songdf.withColumn("year", col("year").cast(IntegerType())) \
                    .withColumn("num_songs", col("num_songs").cast(IntegerType()))
    print(songdf.printSchema())
    # Create songs table
    print("Creating songs table...")
    songs_table = songdf.filter(col("song_id").isNotNull()) \
                    .select("song_id", "title", "artist_id", "year", "duration") \
                    .drop_duplicates(["song_id"])
    print(songs_table.limit(5).show())
    print("Done.")
    # Save into parquet
    print("Saving songs table into parquet...")
    songs_table.write.partitionBy("year", "artist_id").parquet(f"{s3_output_path}songs_table")
    print("Done.")
    # Create artists table
    print("Creating artists table...")
    artists_table = songdf.filter(col("artist_id").isNotNull()) \
                    .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
                    .drop_duplicates(["artist_id"])
    print(artists_table.limit(5).show())
    print("Done.")
    # Save into parquet
    print("Saving artists table into parquet...")
    artists_table.write.parquet(f"{s3_output_path}artists_table")
    print("Done.")
    return songs_table, artists_table


def process_log_data(spark, s3_input_path, s3_output_path, songs_table, artists_table):
    """
        This function processes the log data to create the user, time and songplays tables.
        :param spark: Instanciated SparkSession.
        :param s3_input_path: Input path to s3 song data files.
        :param s3_output_path: Output path to s3 song data files.
    """
    print("Reading jsons. Please stand by.")
    logdf = spark.read.json(s3_input_path)
    print("Done.")
    logdf = logdf.withColumn("itemInSession", col("itemInSession").cast(IntegerType())) \
                .withColumn("userId", col("userId").cast(IntegerType()))
    print(logdf.printSchema())
    # Create users table
    print("Creating users table...")
    users_table = logdf.filter(col("userId").isNotNull()) \
                    .select("userId", "firstName", "lastName", "gender", "level") \
                    .drop_duplicates(["userId"])
    print(users_table.limit(5).show())
    print("Done.")
    # Save into parquet
    print("Saving users table into parquet...")
    users_table.write.parquet(f"{s3_output_path}users_table")
    print("Done.")
    # Create time table
    print("Creating time table...")
    logdf = logdf.withColumn("timestamp", to_timestamp("ts"))
    year = udf(lambda x: x.year, IntegerType())
    month = udf(lambda x: x.month, IntegerType())
    day = udf(lambda x: x.day, IntegerType())
    week = udf(lambda x: x.isocalendar()[1], IntegerType())
    hour = udf(lambda x: x.hour, IntegerType())
    weekday = udf(lambda x: x.weekday(), IntegerType())
    time_table = logdf.withColumn("year", year("timestamp")) \
                  .withColumn("month", month("timestamp")) \
                  .withColumn("day", day("timestamp")) \
                  .withColumn("week", week("timestamp")) \
                  .withColumn("hour", hour("timestamp")) \
                  .withColumn("weekday", weekday("timestamp")) \
                  .selectExpr("ts AS start_time", "hour", "day", "week", "month", "year", "weekday") \
                  .drop_duplicates(["start_time"])
    print(time_table.limit(5).show())
    print("Done.")
    # Save into parquet
    print("Saving time table into parquet...")
    time_table.write.partitionBy("year", "month").parquet(f"{s3_output_path}time_table")
    print("Done.")
    # Create songplays table
    print("Creating songplays table...")
    cond1 = [logdf.song == songs_table.title]
    songslogs = logdf.join(songs_table, cond1) \
                  .withColumn("songplay_id", monotonically_increasing_id()) \
                  .selectExpr("ts AS start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent")
    songsplays = songslogs.withColumn("dt", to_datetime("start_time")) \
                  .withColumn("year", year("dt")) \
                  .withColumn("month", month("dt"))
    print(songsplays.limit(5).show())
    print("Done.")
    # Save into parquet
    print("Saving songplays table into parquet...")
    songsplays.write.partitionBy("year", "month").parquet("songplays_table")
    print("Done.")


def main():
    """
    Main function.
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", 'AWS_SECRET_ACCESS_KEY')

    s3_input_path_songs = config.get("AWS", "s3_input_path_songs")
    s3_input_path_logs = config.get("AWS", "s3_input_path_logs")
    s3_output_path = config.get("AWS", "s3_output_path")
    songs_table, artists_table = process_song_data(spark, s3_input_path_songs, s3_output_path)
    process_log_data(spark, s3_input_path_logs, s3_output_path, songs_table, artists_table)


if __name__ == "__main__":
    main()

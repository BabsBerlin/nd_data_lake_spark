import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''function to load raw song data from S3,
    extract song and artist data, and
    upload these new tables to S3 in parquet format'''

    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/C/*.json'

    # read song data file
    print('*** READING SONG META DATA ***')
    df = spark.read.json(song_data)

    # extract columns to create songs table
    print('*** PROCESSIONG SONG DATA ***')
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    print('*** WRITING SONG DATA ***')
    songs_table.write \
        .partitionBy('year', 'artist_id') \
        .mode('overwrite') \
        .parquet('{}songs/songs_table.parquet'.format(output_data))

    # extract columns to create artists table
    print('*** PROCESSIONG ARTIST DATA ***')
    artists_table = df.select(
        df.artist_id,
        df.artist_name.alias('name'),
        df.artist_location.alias('location'),
        df.artist_latitude.alias('latitude'),
        df.artist_longitude.alias('longitude'))

    # write artists table to parquet files
    print('*** WRITING ARTIST DATA ***')
    artists_table.write \
        .mode('overwrite') \
        .parquet('{}artists/artists_table.parquet'.format(output_data))


def process_log_data(spark, input_data, output_data):
    '''function to load raw log data from S3,
    extract users, time, and songplays data, and
    upload these new tables to S3 in parquet format'''

    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11'

    # read log data file
    print('*** READING LOG META DATA ***')
    df_log = spark.read.json(log_data)

    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table
    print('*** PROCESSING USERS DATA ***')
    users_table = df_log.select(
                            df_log.userId,
                            df_log.firstName.alias('first_name'),
                            df_log.lastName.alias('last_name'),
                            df_log.gender,
                            df_log.level)

    # write users table to parquet files
    print('*** WRITING USERS DATA ***')
    users_table.write\
        .mode('overwrite')\
        .parquet('{}users/users_table'.format(output_data))

    # create timestamp column from original timestamp column
    print('*** PROCESSING TIME DATA ***')
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))

    # extract columns to create time table
    time_table = df_log.select(
        col('timestamp').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        date_format('timestamp', 'E').alias('weekday')
        )

    # write time table to parquet files partitioned by year and month
    print('*** WRITING TIME DATA ***')
    time_table.write\
        .mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet('{}time/time_table'.format(output_data))

    # read in song data to use for songplays table
    print('*** PROCESSING SONGPLAYS DATA ***')
    song_data = input_data + "song_data/A/A/C/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df_log.join(song_df,
                                  (df_log.song == song_df.title) &
                                  (df_log.artist == song_df.artist_name) &
                                  (df_log.length == song_df.duration), 'left_outer') \
                            .select(
                                monotonically_increasing_id().alias('songplay_id'),
                                df_log.timestamp.alias('start_time'),
                                df_log.userId.alias('user_id'),
                                df_log.level,
                                song_df.song_id,
                                song_df.artist_id,
                                df_log.sessionId.alias("session_id"),
                                df_log.location,
                                df_log.userAgent.alias("user_agent"),
                                year('timestamp').alias('year'),
                                month('timestamp').alias('month')
                            )

    # write songplays table to parquet files partitioned by year and month
    print('*** WRITING SONGPLAYS DATA ***')
    songplays_table.write\
        .mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet('{}songplays/songplays_table'.format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://<ADD-YOUR-BUCKET-NAME-HERE>/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

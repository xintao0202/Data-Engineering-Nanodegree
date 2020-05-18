import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Create songs_table and artists_table from input_data, and write Dataframes to Hadoop parquet files. 
    """
    # get filepath to song data file
    # song data format example "song_data/A/B/C/TRABCEI128F424C983.json"
    song_data = os.path.join(input_data, 'song_data/A/B/C/*.json')
    
    # read song data file
    # read multiple json files into one dataFrame 
    # https://stackoverflow.com/questions/33710898/how-can-i-efficiently-read-multiple-json-files-into-a-dataframe-or-javardd
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols=('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table = df.select(*songs_cols)
    
    # write songs table to parquet files partitioned by year and artist
    # Spark read and write https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), mode='overwrite')

    # extract columns to create artists table
    
    artists_cols=('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table = df.select(*artists_cols)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =os.path.join(input_data, "log-data/2018/11/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_cols=('userId','firstName','lastName','gender','level')
    users_table = df.select(*users_cols)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet') , 'overwrite')

    # create timestamp column from original timestamp column
    # variable as function in spark
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    # pyspark create timestamp column
    df = df.withColumn('timestamp', get_timestamp('ts') )
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), )
    df = df.withColumn('start_time', get_timestamp('ts') )
    
    # extract columns to create time table
    
    df = df.withColumn('hour', F.hour('start_time'))\
            .withColumn('day', F.dayofmonth('start_time'))\
            .withColumn('week', F.weekofyear('start_time'))\
            .withColumn('month', F.month('start_time'))\
            .withColumn('year', F.year('start_time'))\
            .withColumn('weekday', F.dayofweek('start_time'))
    
    time_cols=('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_table=df.select(*time_cols)                
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,"songs/songs.parquet"))

    # extract columns from joined song and log datasets to create songplays table 
    
    # join df with song_df
    df_join = df.join(song_df, df.song == song_df.title, how='inner')
    songplays_cols=('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', df['year'], 'month')
    songplays_table=df_join.select(*songplays_cols)
    songplays_table=songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())
    
    #rename columns
    songplays_table = songplays_table.withColumnRenamed("userId", "user_id")\
                                     .withColumnRenamed("sessionId", "session_id")\
                                     .withColumnRenamed("userAgent", "user_agent")
    
    #reorder columns
    final_cols=('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'year', 'month')
    songplays_table=songplays_table.select(*final_cols)
    
    # write songplays table to parquet files partitioned by year and month
    # need to add 'year' and 'month' column for partition
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-engineer-exercise/data-lake-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

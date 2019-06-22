import configparser
from datetime import datetime
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
"""
Function to Create or return existing SparkSession
"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
"""
Function to extract data from songs data from input_data and creates the song and artist files in parquet format in the output_data folder.

Parameters: 
    spark: The Spark Session
    input_data: S3 Bucket path to the root folder which contains the song data in the JSON format
    output_data: S3 Bucket path to the folder in which the songs and artists table will be loaded.
    
    Returns: 
    None

"""
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet("s3a://datalake/songs/")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("s3a://datalake/artists/")

def process_log_data(spark, input_data, output_data):
"""
Function to read log_data from S3 and process it and also write back them in parquet format.

Parameters: 
    spark: The Spark Session
    input_data: S3 Bucket path to the root folder which contains the song data in the JSON format
    output_data: S3 Bucket path to the folder in which the songs and artists table will be loaded.
    
    Returns: 
    None
	
"""
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').filter(df.userId.isNotNull())

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("s3a://datalake/users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date_time',get_timestamp(df.ts))
    df.createOrReplaceTempView("stage_logs")
    
	# extract columns to create time table
    time_table = df.selectExpr("date_time as start_time",\
              "hour(date_time) as hour", \
              "dayofmonth(date_time) as day",\
              "weekofyear(date_time) as week",\
              "month(date_time) as month",\
              "year(date_time) as year",\
              "date_format(date_time,'u') as weekday").dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet("s3a://datalake/time/")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    df.createOrReplaceTempView("stage_songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''SELECT distinct l.ts as start_time,\
                                                   l.userId as user_id,\
                                                   l.level as level,\
                                                   s.song_id as song_id,\
                                                   s.artist_id as artist_id,\
                                                   l.sessionId as session_id,\
                                                   l.location as location,\
                                                   l.userAgent as user_agent,\
                                                   year(date_time) as year,\
                                                   month(date_time) as month\
                                                   FROM stage_logs l join stage_songs s\
                                                   ON (s.artist_name = l.artist and s.title=l.song and s.duration = l.length)''')
    
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet("s3a://datalake/songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

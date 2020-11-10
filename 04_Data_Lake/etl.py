import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
	'''
	Create a new or obtain an existing SparkSession
	'''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
		
    return spark


def process_song_data(spark, input_data, output_data):
    '''
	Reads song_data from S3, generate 'songs_table' and 'artists_table'
	and writes them back to S3
	
	Parameters: 
		spark: SparkSession
		input_data: S3 link of input data
		output_data: S3 link of where the tables will be saved
	'''

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
	songs_table \
		.write.mode("overwrite") \
		.partitionBy("year", "artist_id") \
		.parquet(output_data + "songs_table/")

    # extract columns to create artists table
    artists_table =  df.select(['artist_id', 'artist_name', 'artist_location', \
								'artist_latitude', 'artist_longitude']) \
						.withColumnRenamed('artist_name', 'name') \
						.withColumnRenamed('artist_location', 'location') \
						.withColumnRenamed('artist_latitude', 'latitude') \
						.withColumnRenamed('artist_longitude', 'longitude')
    
    # write artists table to parquet files
    artists_table \
		.write.mode("overwrite") \
		.parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    '''
	Reads log_data from S3, generate 'users_table', 'time_table' and 'songplays_table',
	and writes them back to S3
	
	Parameters: 
		spark: SparkSession
		input_data: S3 link of input data
		output_data: S3 link of where the tables will be saved
	'''

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userid', 'firstname', 'lastname', 'gender', 'level']) \
					.distinct() \
					.withColumnRenamed('userid', 'user_id') \
					.withColumnRenamed('firstname', 'first_name') \
					.withColumnRenamed('lastname', 'last_name') 
    
    # write users table to parquet files
    users_table \
		.write.mode("overwrite") \
		.parquet(output_data + "users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
	df = df.withColumn("start_time", get_timestamp(df.ts))
    
	# create udf for extracting 'weekday' column
	get_weekday = udf(lambda ts: datetime.date.fromtimestamp(ts / 1000.0).weekday())

	# Create timestamps of records columns
	df = df.withColumn("hour", hour(df.start_time)) \
			.withColumn("day", dayofmonth(df.start_time)) \
			.withColumn("week", weekofyear(df.start_time)) \
			.withColumn("month", month(df.start_time)) \
			.withColumn("year", year(df.start_time)) \
			.withColumn("weekday", get_weekday(df.ts)) 
    
    # extract columns to create time table
    time_table = df.select('start_time','hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table \
		.write.mode("overwrite") \
		.partitionBy("year", "month") \
		.parquet(output_data + "time_table/")	

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist) \
							& (song_df.duration == df.length), how = 'left') \
						.withColumn("songplay_id", monotonically_increasing_id()) \
						.select('songplay_id', df.start_time, df.userId, df.level, \
                            song_df.song_id, song_df.artist_id, df.sessionId, \
                            df.location, df.userAgent, df.year, df.month) \
						.withColumnRenamed('ts', 'start_time') \
						.withColumnRenamed('userId', 'user_id') \
						.withColumnRenamed('sessionId', 'session_id') \
						.withColumnRenamed('userAgent', 'user_agent') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
		.write.mode("overwrite") \
		.partitionBy("year", "month") \
		.parquet(output_data + "songplays_table/")	


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # Fill in the link of S3 bucket before executing this script
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

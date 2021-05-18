import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # Load song_data JSON files and saves songs and artists parquet files
    # Parameters 
    # spark: spark session
    # input_data: JSON files path
    # output_data: Parquet files path
    
    # Start load JSON
    start_json = datetime.now()
    print("Starting load song_data JSON files")
    print(start_json)   
    
    # get filepath to song data file - COMPLETE
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json") 
    
    # get filepath to song data file - TEST FAST RUN
    #song_data = os.path.join(input_data, "song_data/A/A/A/*.json") 
    
    # read song_data file 
    df = spark.read.json(song_data)
    
    # Print song_data schema and first 5 records 
    df.printSchema()
    df.show(5)

    # Start time song data
    start_song = datetime.now()
    print("Starting Song Data")
    print(start_song)   
    
    # song table
    df.createOrReplaceTempView("songs_table")
    
    # extract columns to create songs_table
    songs_table = spark.sql("""
        SELECT song_id     AS song_id, 
               title       AS name, 
               artist_id   AS artist_id, 
               year        AS year,
               duration    AS duration 
        FROM songs_table
    """)
         
    # Path to songs parquet files
    songs_path = output_data + "songs_table.parquet" + "_" \
                        + start_song.strftime('%Y-%m-%d-%H-%M-%S-%f')
    
    # write songs_table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_path)

    # Finish time do song data
    finish_song = datetime.now()
    print("Finishing Song Data")
    print(finish_song)      

    # Start time do artist data
    start_artist = datetime.now()
    print("Starting Artist Data")
    print(start_artist)   
    
    # Create artists table
    df.createOrReplaceTempView("artists_table")
    
    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS lattitude,
                artist_longitude AS longitude
        FROM artists_table
    """)
           
    # Path to artists parquet files
    artists_table_path = output_data  + "artists_table.parquet" + "_" \
                        + start_artist.strftime('%Y-%m-%d-%H-%M-%S-%f')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(artists_table_path)   

    # Finish time to artist data
    finish_artist = datetime.now()
    print("Finishing Artist Data")
    print(finish_artist)         

def process_log_data(spark, input_data, output_data):
    # Load log_data JSON files and saves Users, Time and Sonplays parquet files
    # Parameters
    # spark: spark session
    # input_data: JSON files path
    # output_data: Parquet files path
    
    # Start load Json
    start_json = datetime.now()
    print("Starting load log_data JSON files")
    print(start_json)   
    
    # Get filepath to log data file - COMPLETE
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    log_data = os.path.join(input_data, "s3a://udacity-lake/log_data/*/*/*.json")  
    
    # Get filepath to log data file - TEST FAST RUN
    # song_data = os.path.join(input_data, "song_data/A/A/A/*.json") 
    # log_data = os.path.join(input_data, "log_data/2018/11/2018-11-12-events.json")
        
    # Read log_data file
    df = spark.read.json(log_data)
    
    # Show log_data schema and 5 records
    df.printSchema()
    df.show(5)   
    
    # Filter by "NextSong" actions
    df = df.filter(df.page == 'NextSong')

    # Start load Users
    start_user = datetime.now()
    print("Starting load users files")
    print(start_user)   
      
    # Create users table
    df.createOrReplaceTempView("users_table")
    
    # Extract columns for users table
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender    AS gender,
                         level     AS level
        FROM users_table
    """)
    
    # Path to users parquet files
    users_table_path = output_data + "users_table.parquet" + "_" \
                        + start_user.strftime('%Y-%m-%d-%H-%M-%S-%f')
    
    # Write users table to parquet files
    users_table.write.mode("overwrite").parquet(users_table_path)
        
    # Finish time to Users data
    finish_users = datetime.now()
    print("Finishing Users Data")
    print(finish_users)         

    # Start load Time data
    start_time = datetime.now()
    print("Starting load time files")
    print(start_time)      
    
    # UDF Function get_timestamp: output data type Timestamp
    @udf(t.TimestampType())
    def convert_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    df = df.withColumn("timestamp", convert_timestamp("ts"))

    # UDF Function get_datetime: output data type String
    @udf(t.StringType())
    def convert_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')

    df = df.withColumn("datetime", convert_datetime("ts"))

    # Create time table
    df.createOrReplaceTempView("time_table")

    # Extract columns to create time table    
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table
    """)

    # Path to users parquet files
    time_table_path = output_data + "time_table.parquet" + "_" \
                    + start_time.strftime('%Y-%m-%d-%H-%M-%S-%f')

    # COMENT PARA ADIANTAR
    # Write time table to parquet files partitioned by year and month    
    time_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(time_table_path)
    # COMENT PARA ADIANTAR 
    
    # Finish time to time data
    finish_time = datetime.now()
    print("Finishing Time Data")
    print(finish_time)         

    # Start load songplays data
    start_time = datetime.now()
    print("Starting load songplays files")
    print(start_time)      
    
    # Load song_data JSON to retrieve artist_id and song_id
    song_df = spark.read.json(song_data) 
               
    # Create dataframe joining song and songplays dataframe      
    df_join = df.join(song_df, (df.artist == song_df.artist_name) & \
                     (df.song == song_df.title), "left")

    # Create songplay_id column binded monotonically_increasing_id function  
    df_join = df_join.withColumn("songplay_id", monotonically_increasing_id())

    # Create songplays table
    df_join.createOrReplaceTempView("songplays_table")
        
    # Extract columns from dataframe joined 
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent,
                year(timestamp) AS year,
                month(timestamp) AS month
        FROM songplays_table
    """)
 
    # Path to songplays parquet files
    songplays_table_path = output_data + "songplays_table.parquet" + "_" \
                            + start_time.strftime('%Y-%m-%d-%H-%M-%S-%f')
 
    # Write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(songplays_table_path)

    # Finish time to somgplays data
    finish_time = datetime.now()
    print("Finishing somgplays Data")
    print(finish_time)         
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-bloisi/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

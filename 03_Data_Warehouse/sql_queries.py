import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
	CREATE TABLE IF NOT EXISTS 
		staging_events(artist 			VARCHAR, 
						auth 			VARCHAR, 
						firstName 		VARCHAR, 
						gender 			CHAR(1), 
						itemInSession 	INT, 
						lastName 		VARCHAR, 
						length 			FLOAT, 
						level 			VARCHAR, 
						location 		VARCHAR, 
						method 			CHAR(3), 
						page 			VARCHAR, 
						registration 	FLOAT, 
						sessionId 		INT, 
						song 			VARCHAR, 
						status 			INT, 
						ts 				TIMESTAMP, 
						userAgent 		VARCHAR, 
						userId 			INT);""")

staging_songs_table_create =  ("""
	CREATE TABLE IF NOT EXISTS 
		staging_songs(num_songs 		INT			NOT NULL,
					artist_id			VARCHAR		NOT NULL,
					artist_latitude		FLOAT,	
					artist_longitude	FLOAT,
					artist_location		VARCHAR,		
					artist_name			VARCHAR		NOT NULL,	
					song_id				VARCHAR		NOT NULL,
					title				VARCHAR		NOT NULL,
					duration			FLOAT		NOT NULL,	
					year				INT			NOT NULL);""")
        
songplay_table_create = ("""
	CREATE TABLE IF NOT EXISTS 
		songplays (songplay_id 	INT             IDENTITY(0,1),
					start_time 	TIMESTAMP 		NOT NULL,
					user_id 	INT, 
					level 		VARCHAR 		NOT NULL, 
					song_id 	VARCHAR, 
					artist_id 	VARCHAR,
					session_id 	INT 			NOT NULL, 
					location 	VARCHAR 		NOT NULL, 
					user_agent 	VARCHAR 		NOT NULL, 
					PRIMARY KEY (songplay_id),
					FOREIGN KEY (start_time)	REFERENCES time(start_time),
					FOREIGN KEY (user_id)		REFERENCES users(user_id),
					FOREIGN KEY (song_id)		REFERENCES songs(song_id),
					FOREIGN KEY (artist_id)		REFERENCES artists(artist_id));""")
																																	
user_table_create = ("""
	CREATE TABLE IF NOT EXISTS 
			users (user_id 		INT			NOT	NULL, 
					first_name 	VARCHAR		NOT NULL, 
					last_name 	VARCHAR		NOT NULL, 
					gender 		CHAR(1)		NOT NULL,
					level 		VARCHAR 	NOT NULL, 
					PRIMARY KEY (user_id));""")

song_table_create = ("""
	CREATE TABLE IF NOT EXISTS 
			songs (song_id 		VARCHAR		NOT NULL,
					title 		VARCHAR 	NOT NULL, 
					artist_id 	VARCHAR 	NOT NULL,
					year 		INT 		NOT NULL, 
					duration 	FLOAT 		NOT NULL, 
					PRIMARY KEY (song_id));""")

artist_table_create = ("""
	CREATE TABLE IF NOT EXISTS 
			artists (artist_id		VARCHAR 	NOT NULL, 
						name 		VARCHAR		NOT NULL, 
						location 	VARCHAR,
						latitude 	FLOAT, 
						longitude 	FLOAT, 
						PRIMARY KEY (artist_id));""")

time_table_create = ("""
	CREATE TABLE IF NOT EXISTS
			time (start_time	TIMESTAMP   NOT NULL, 
					hour		VARCHAR 	NOT NULL, 
					day 		VARCHAR 	NOT NULL, 
					week 		VARCHAR 	NOT NULL, 
					month 		VARCHAR 	NOT NULL, 
					year 		VARCHAR 	NOT NULL, 
					weekday 	VARCHAR 	NOT NULL, 
					PRIMARY KEY (start_time));""")	
# STAGING TABLES

ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

staging_events_copy = ("""copy staging_events 
                            from {}
                            json {}
                            timeformat 'epochmillisecs' 
                            compupdate off 
                            region 'us-west-2'
                            emptyasnull
                            credentials 'aws_iam_role={}';""").format(LOG_DATA, LOG_JSONPATH, ARN)

staging_songs_copy = ("""copy staging_songs 
                            from {}
                            format as json 'auto'
                            compupdate off
                            region 'us-west-2'
                            credentials 'aws_iam_role={}';""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id,
                                                artist_id, session_id, location, user_agent)
                            SELECT se.ts, se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.userAgent
                            FROM staging_events se
                            LEFT JOIN staging_songs ss
                            ON (ss.title = se.song AND ss.artist_name = se.artist AND ss.duration = se.length)
                            WHERE se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
						SELECT DISTINCT userid, firstname, lastname, gender, level									
						FROM staging_events
						WHERE staging_events.page = 'NextSong';""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
						SELECT song_id, title, artist_id, year, duration
						FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                            SELECT ts as start_time,
                                TO_CHAR (ts, 'HH24') as hour,
                                TO_CHAR (ts, 'DD') as day,      
                                TO_CHAR (ts, 'WW') as week,                    
                                TO_CHAR (ts, 'MM') as month,
                                TO_CHAR (ts, 'YYYY') as year, 
                                TO_CHAR (ts, 'D') as weekday
                            FROM staging_events
                            WHERE staging_events.page = 'NextSong';""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

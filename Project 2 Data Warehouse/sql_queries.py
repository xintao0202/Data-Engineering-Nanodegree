import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

# Staging_events_table comes from log dataset without changing anything
# Not using NOT null, otherwise it shows "missing data for not-null field" error
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER);
""")

# Staging_songs_table comes from Song dataset without changing anything
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR,
    duration FLOAT,
    year INT);
""")

#songplay table is the fact table
songplay_table_create = ("""CREATE TABLE songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL sortkey,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE users (
        user_id VARCHAR NOT NULL PRIMARY KEY distkey,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR,
        level VARCHAR NOT NULL)
        DISTSTYLE KEY;
""")

song_table_create = ("""CREATE TABLE songs (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL distkey,
        year INT,
        duration FLOAT)
        DISTSTYLE KEY;
""")

artist_table_create = ("""CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY distkey,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT)
        DISTSTYLE KEY;
""")

time_table_create = ("""CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY sortkey distkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL)
        DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON  {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON  'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES
# Handles duplicate records by using NOT IN statements, if record already in the table do not update. Note ON CONFLICT not supported by current postgresql version on Redshift, which is PostgreSQL 8.0.2

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
        E.userId as user_id,
        E.level as level,
        S.song_id as song_id,
        S.artist_id as artist_id,
        E.sessionId as session_id,
        E.location as location,
        E.userAgent as user_agent
        
    FROM staging_events E
    INNER JOIN staging_songs as S on E.song=S.title AND E.artist=S.artist_name;                 
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
    
    FROM staging_events 
    WHERE userId IS NOT NULL
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL
    AND level IS NOT NULL
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
      SELECT DISTINCT
        song_id as song_id,
        title as title,
        artist_id as artist_id,
        year as year,
        duration as duration
        
     FROM staging_songs
     WHERE song_id IS NOT NULL AND title IS NOT NULL
     AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);

""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
      SELECT DISTINCT 
          artist_id as artist_id,
          artist_name as name,
          artist_location as location,
          artist_latitude as latitude,
          artist_longitude as longitude
     FROM staging_songs
     WHERE artist_id IS NOT NULL AND name IS NOT NULL
     AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
      SELECT DISTINCT
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
      FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events E     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

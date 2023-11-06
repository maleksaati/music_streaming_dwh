import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ROLE_ARN = config.get('IAM_ROLE', 'ARN') 
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id bigint NOT NULL,
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length varchar,
    level  varchar,
    location varchar,
    method varchar,
    page  varchar,
    registration varchar,
    sessionId int SORTKEY DISTKEY,
    song  varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId  int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id  varchar,
    artist_latitude  varchar,
    artist_longitude   varchar,
    artist_location   varchar,
    artist_name  varchar,
    song_id   varchar,
    title   varchar,
    duration  float,
    year int
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER PRIMARY KEY SORTKEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL DISTKEY, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY SORTKEY , 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY SORTKEY , 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration float) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY SORTKEY , 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY SORTKEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
     COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ROLE_ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
    SELECT DISTINCT
       TO_TIMESTAMP(ev.ts) as start_time,
                    ev.userId, ev.level, sng.song_id,  sng.artist_id,
                    ev.sessionId, ev.location, ev.userAgent
    FROM staging_songs sng
    INNER JOIN staging_events ev
    ON (sng.title = ev.song AND ev.artist = sng.artist_name)
    AND ev.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
        TO_TIMESTAMP(ts) as start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(weeks FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        to_char(start_time, 'Day') AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

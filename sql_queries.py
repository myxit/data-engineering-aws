import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS "
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR(64) NOT NULL,
        auth VARCHAR(64) NULL,
        first_name VARCHAR(32) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        item_in_session INTEGER NOT NULL,
        last_name VARCHAR(32) NOT NULL,
        length REAL NOT NULL,
        level VARCHAR(8) NOT NULL,
        location VARCHAR(64) NOT NULL,
        method VARCHAR(7) NOT NULL,
        page VARCHAR(32) NOT NULL,
        registration BIGINT NOT NULL,
        session_id INTEGER NOT NULL,
        song VARCHAR(64) NOT NULL,
        status INTEGER NOT NULL,
        ts BIGINT NOT NULL,
        user_agent VARCHAR(512),
        user_id INTEGER NOT NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR(18) NOT NULL,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(64) NULL,
        artist_name VARCHAR(64) NOT NULL,
        song_id VARCHAR(18) NOT NULL,
        title VARCHAR(64) NOT NULL,
        duration REAL NOT NULL,
        year INTEGER NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id IDENTITY(0,1),
        start_time, 
        user_id INTEGER NOT NULL, 
        level, 
        song_id INTEGER NOT NULL,
        artist_id INTEGER NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(64) NOT NULL,
        user_agent VARCHAR(512)
    ) DISTSTYLE key;
""")

user_table_create = ("""
    CREATE TABLE user (
        user_id INTEGER NOT NULL,
        first_name VARCHAR(32) NOT NULL,
        last_name VARCHAR(32) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(8) NOT NULL
    ) DISTSTYLE AUTO;
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id INTEGER NOT NULL,
        title, 
        artist_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        duration INTEGER NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id INTEGER NOT NULL,
        name, 
        location, 
        latitude, 
        longitude
    ) DISTSTYLE AUTO;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time INTEGER NOT NULL, #<< TODO: check
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
    FROM '{}' 
    CREDENTIALS 'aws_iam_role={}'
    JSON '{}'
    REGION 'us-west-2'
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
)

staging_songs_copy = ("""
COPY staging_songs 
    FROM '{}' 
    CREDENTIALS 'aws_iam_role={}'
    JSON auto
    REGION 'us-west-2'
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'),
)
# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

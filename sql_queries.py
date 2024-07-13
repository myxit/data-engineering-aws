import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist              TEXT NOT NULL,
        auth                TEXT NULL,
        first_name          TEXT NOT NULL,
        gender              VARCHAR(1) NOT NULL,
        item_in_session     INTEGER NOT NULL,
        last_name           TEXT NOT NULL,
        length              REAL NOT NULL,
        level               TEXT NOT NULL,
        location            TEXT NOT NULL,
        method              TEXT NOT NULL,
        page                TEXT NOT NULL,
        registration        BIGINT NOT NULL,
        session_id          BIGINT NOT NULL,
        song                TEXT NOT NULL,
        status              INTEGER NOT NULL,
        ts                  BIGINT NOT NULL,
        user_agent          TEXT,
        user_id             INTEGER NOT NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id           TEXT NOT NULL,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT NULL,
        artist_name         TEXT NOT NULL,
        song_id             TEXT NOT NULL,
        title               TEXT NOT NULL,
        duration            REAL NOT NULL,
        year                INTEGER NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id BIGINT IDENTITY(0,1),
        start_time  BIGINT NOT NULL, /* Timestamp */ 
        user_id     BIGINT NOT NULL, 
        level       TEXT, 
        song_id     BIGINT NOT NULL,
        artist_id   BIGINT NOT NULL,
        session_id  INTEGER NOT NULL,
        location    VARCHAR(64) NOT NULL,
        user_agent  VARCHAR(512),
        PRIMARY KEY(songplay_id)
    ) DISTSTYLE AUTO;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id     BIGINT IDENTITY(0,1),
        first_name  VARCHAR(32) NOT NULL,
        last_name   VARCHAR(32) NOT NULL,
        gender      VARCHAR(1) NOT NULL,
        level       VARCHAR(8) NOT NULL,
        PRIMARY KEY(user_id)
    ) DISTSTYLE AUTO;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id     BIGINT IDENTITY(0,1),
        title       TEXT, 
        artist_id   BIGINT NOT NULL,
        year        INTEGER NOT NULL,
        duration    REAL NOT NULL,
        PRIMARY KEY(song_id)
    ) DISTSTYLE AUTO;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id   BIGINT IDENTITY(0,1),
        name        TEXT NOT NULL, 
        location    TEXT NOT NULL, 
        latitude    FLOAT, 
        longitude   FLOAT,
        PRIMARY KEY(artist_id)
    ) DISTSTYLE AUTO;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time  BIGINT NOT NULL, /* Timestamp */ 
        hour        INTEGER NOT NULL,
        day         INTEGER NOT NULL,
        week        INTEGER NOT NULL,
        month       INTEGER NOT NULL,
        year        INTEGER NOT NULL, /* DISTKEY, if DISTSTYLE KEY */
        weekday     INTEGER NOT NULL,
        PRIMARY KEY(start_time)
    ) DISTSTYLE AUTO;
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

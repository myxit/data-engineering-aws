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
        artist              TEXT,
        auth                TEXT,
        first_name          TEXT,
        gender              VARCHAR(1),
        item_in_session     INTEGER,
        last_name           TEXT,
        length              REAL,
        level               TEXT,
        location            TEXT,
        method              TEXT,
        page                TEXT,
        registration        TIMESTAMP,
        session_id          BIGINT,
        song                TEXT,
        status              INTEGER,
        ts                  TIMESTAMP NOT NULL,
        user_agent          TEXT,
        user_id             INTEGER
    )
    SORTKEY (page)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id           TEXT,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            REAL,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id BIGINT IDENTITY(1,1),
        start_time  TIMESTAMP NOT NULL,
        user_id     INTEGER NOT NULL, 
        level       TEXT, 
        song_id     VARCHAR(18) NOT NULL,
        artist_id   VARCHAR(18) NOT NULL,
        session_id  INTEGER NOT NULL,
        location    TEXT,
        user_agent  TEXT,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY(user_id) references users(user_id),
        FOREIGN KEY(start_time) references time(start_time),
        FOREIGN KEY(song_id) references songs(song_id),
        FOREIGN KEY(artist_id) references artists(artist_id)
    ) 
    DISTKEY (songplay_id)
    SORTKEY (start_time)
    ;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id     INTEGER NOT NULL,
        first_name  TEXT,
        last_name   TEXT,
        gender      VARCHAR(1) NULL,
        level       TEXT,
        PRIMARY KEY(user_id)
    ) 
    DISTKEY (user_id)
    SORTKEY (user_id)
    ;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id     VARCHAR(18) NOT NULL,
        title       TEXT, 
        artist_id   VARCHAR(18) NOT NULL,
        year        INTEGER NOT NULL,
        duration    REAL NOT NULL,
        PRIMARY KEY(song_id)
    ) 
    DISTKEY (song_id)
    SORTKEY (song_id)
    ;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id   VARCHAR(18) NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT,
        PRIMARY KEY(artist_id)
    ) 
    DISTKEY (artist_id)
    SORTKEY (artist_id)
    ;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time  TIMESTAMP NOT NULL,
        hour        INTEGER NOT NULL,
        day         INTEGER NOT NULL,
        week        INTEGER NOT NULL,
        month       INTEGER NOT NULL,
        year        INTEGER NOT NULL,
        weekday     INTEGER NOT NULL,
        PRIMARY KEY(start_time)
    ) 
    DISTSTYLE AUTO
    SORTKEY (start_time)
    ;
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
    FROM {}
    IAM_ROLE {}
    JSON {}
    REGION 'us-west-2'
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
)

staging_songs_copy = ("""
COPY staging_songs 
    FROM {}
    IAM_ROLE {}
    JSON 'auto'
    REGION 'us-west-2'
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'),
)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT timestamp 'epoch' + ts * interval '0.001 seconds' as start_time, 
        user_id, 
        level, 
        songs.song_id as song_id, 
        artists.artist_id as artist_id,
        session_id as session_id, 
        staging_events.location as location, 
        user_agent as user_agent
    FROM staging_events 
    INNER JOIN artists on artists.name = staging_events.artist
    INNER JOIN songs on songs.title = staging_events.song 
    WHERE page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name, 
        gender,
        level
    ) 
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) 
    SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO "time" (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT DISTINCT start_time,
        date_part(hour, start_time) as hour,
        date_part(day, start_time) as day,
        date_part(week, start_time) as week,
        date_part(year, start_time) as year,
        date_part(weekday, start_time) as weekday            
    from songplay 
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create,]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, songplay_table_insert, time_table_insert]

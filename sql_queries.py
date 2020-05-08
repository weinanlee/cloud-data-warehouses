import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        VARCHAR,
    itemInSession INTEGER,
    lastName      VARCHAR,
    length        NUMERIC,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  FLOAT,
    sessionId     INTEGER SORTKEY DISTKEY,
    song          VARCHAR,
    status        INTEGER,
    ts            BIGINT,
    userAgent     VARCHAR,
    userId        INTEGER

)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
     num_songs        INTEGER,
     artist_id        VARCHAR SORTKEY DISTKEY, 
     artist_latitude  VARCHAR, 
     artist_longitude VARCHAR, 
     artist_location  VARCHAR, 
     artist_name      VARCHAR, 
     song_id          VARCHAR, 
     title            VARCHAR, 
     duration         NUMERIC,
     year             INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE SONGPLAYS 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     VARCHAR NOT NULL,
    level       VARCHAR NOT NULL,
    song_id     VARCHAR,
    artist_id   VARCHAR,
    session_id  INTEGER NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS 
(   user_id    INTEGER PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name  VARCHAR NOT NULL, 
    gender     VARCHAR, 
    level      VARCHAR NOT NULL
)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS 
(
    song_id   VARCHAR PRIMARY KEY, 
    title     VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year      INTEGER, 
    duration  FLOAT NOT NULL
)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS 
(
    artist_id VARCHAR PRIMARY KEY, 
    name      VARCHAR NOT NULL, 
    location  VARCHAR, 
    latitude  VARCHAR, 
    longitude VARCHAR
)
SORTKEY (artist_id);    
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TIME 
(
    start_time TIMESTAMP PRIMARY KEY, 
    hour       INTEGER NOT NULL, 
    day        INTEGER NOT NULL, 
    week       INTEGER NOT NULL, 
    month      INTEGER NOT NULL, 
    year       INTEGER ENCODE BYTEDICT ,
    weekday    VARCHAR ENCODE BYTEDICT
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'], 
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, 
                      user_id, 
                      level, 
                      song_id, 
                      artist_id, 
                      session_id, 
                      location, 
                      user_agent)
SELECT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time, 
       se.userid, 
       se.level, 
       ss.song_id, 
       ss.artist_id, 
       se.sessionid, 
       se.location, 
       se.useragent
       
FROM staging_songs ss 
JOIN staging_events se 
ON (ss.title = se.song AND 
    se.artist = ss.artist_name)
AND se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, 
                   first_name, 
                   last_name, 
                   gender, 
                   level)
SELECT DISTINCT userid, 
                firstname, 
                lastname, 
                gender, 
                level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, 
                   title, 
                   artist_id, 
                   year, 
                   duration)
SELECT DISTINCT song_id, 
                title, 
                artist_id, 
                year, 
                duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, 
                     name, 
                     location,
                     latitude, 
                     longitude)
SELECT DISTINCT artist_id, 
                artist_name, 
                artist_location,
                artist_latitude, 
                artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, 
                  hour, 
                  day, 
                  week, 
                  month, 
                  year, 
                  weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

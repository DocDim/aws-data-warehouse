import configparser


# Importation of configuration file "dwh.cfg"
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Tables drop queries string values
staging_events_query_drop = "DROP TABLE IF EXISTS stagingevents;"
staging_songs_query_drop = "DROP TABLE IF EXISTS stagingsongs;"
songplay_query_drop = "DROP TABLE IF EXISTS songplays"
user_query_drop = "DROP TABLE IF EXISTS users;"
song_query_drop = "DROP TABLE IF EXISTS songs;"
artist_query_drop = "DROP TABLE IF EXISTS artists;"
time_query_drop = "DROP TABLE IF EXISTS time;"

# initialize configuration string values
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# Tables creation queries string values
staging_events_query_create= ("""CREATE TABLE IF NOT EXISTS stagingevents (
                event_id    BIGINT  PRIMARY KEY     NOT NULL,
                artist      VARCHAR                 NULL,
                auth        VARCHAR                 NULL,
                firstName   VARCHAR                 NULL,
                gender      VARCHAR                 NULL,
                itemInSession VARCHAR               NULL,
                lastName    VARCHAR                 NULL,
                length      VARCHAR                 NULL,
                level       VARCHAR                 NULL,
                location    VARCHAR                 NULL,
                method      VARCHAR                 NULL,
                page        INTEGER                 NULL,
                registration VARCHAR                NULL,
                sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,
                song        VARCHAR                 NULL,
                status      INTEGER                 NULL,
                ts          BIGINT                  NOT NULL,
                userAgent   VARCHAR                 NULL,
                userId      INTEGER                 NULL);""")

staging_songs_query_create = ("""CREATE TABLE IF NOT EXISTS stagingsongs (
                num_songs           INTEGER PRIMARY KEY      NULL,
                artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
                artist_latitude     DOUBLE          NULL,
                artist_longitude    DOUBLE          NULL,
                artist_location     VARCHAR(500)    NULL,
                artist_name         VARCHAR(500)    NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR(500)    NULL,
                duration            DECIMAL(9)      NULL,
                year                INTEGER         NULL);""")

songplay_query_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1)  PRIMARY KEY  NOT NULL SORTKEY,
                start_time  TIMESTAMP               NOT NULL,
                user_id     VARCHAR(50)             NOT NULL DISTKEY,
                level       VARCHAR(10)             NOT NULL,
                song_id     VARCHAR(40)             NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                session_id  VARCHAR(50)             NOT NULL,
                location    VARCHAR(100)            NULL,
                user_agent  VARCHAR(255)            NULL
    );""")

user_query_create = (""" CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER  PRIMARY KEY    NOT NULL SORTKEY,
                first_name  VARCHAR(50)             NULL,
                last_name   VARCHAR(80)             NULL,
                gender      VARCHAR(10)             NULL,
                level       VARCHAR(10)             NULL
    ) diststyle all;""")

song_query_create = ("""CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(50) PRIMARY KEY NOT NULL SORTKEY,
                title       VARCHAR(500)            NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                year        INTEGER                 NOT NULL,
                duration    DECIMAL(9)              NOT NULL
    );""")

artist_query_create = ("""CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50) PRIMARY KEY NOT NULL SORTKEY,
                name        VARCHAR(500)            NULL,
                location    VARCHAR(500)            NULL,
                latitude    DOUBLE                  NULL,
                longitude   DOUBLE                  NULL
    ) diststyle all;""")

time_query_create = ("""CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP PRIMARY KEY   NOT NULL SORTKEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) diststyle all;""")

# Copy data from S3 to Staging Tables

staging_events_copy = ("""COPY stagingevents FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY stagingsongs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# Inser data into Final tables from Staging Tables

songplay_query_insert = ("""INSERT INTO songplays (start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent) SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM stagingevents AS se
    JOIN stagingsongs AS ss
        ON (se.artist = ss.artist_name and se.song=ss.title)
    WHERE se.page = 'NextSong';""")

user_query_insert = ("""INSERT INTO users (user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userId          AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM stagingevents AS se
    WHERE se.page = 'NextSong';""")

song_query_insert = ("""INSERT INTO songs(song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM stagingsongs AS ss;""")

artist_query_insert = ("""INSERT INTO artists (artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM stagingsongs AS ss;
""")

time_query_insert = ("""INSERT INTO time (start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    stagingevents                   AS se
    WHERE se.page = 'NextSong';""")

# Retrieve data  from database for analysis
total_number_user_query_count     = ("""SELECT COUNT(*) FROM users;""")
total_number_artist_query_count   = ("""SELECT COUNT(*) FROM artists;""")
total_number_song_query_count = ("""SELECT COUNT(*) FROM songs;""")
total_number_songplay_query_count = ("""SELECT COUNT(*) FROM songplays;""")

number_songplay_by_artist_query_count = ("""SELECT artist_id, COUNT(*)  FROM songplays GROUP BY artist_id ORDER BY COUNT(*) DESC limit 10;""")
number_songplay_by_user_query_count = ("""SELECT user_id, COUNT(*) FROM songplays GROUP BY user_id ORDER BY COUNT(*) DESC limit 10;""")
number_songplay_by_song_query_count = ("""SELECT song_id, COUNT(*) FROM songplays GROUP BY song_id ORDER BY COUNT(*) DESC limit 10;""")

# Create queries list
create_table_queries = [staging_events_query_create, staging_songs_query_create, songplay_query_create, user_query_create, song_query_create, artist_query_create, time_query_create]

drop_table_queries = [staging_events_query_drop, staging_songs_query_drop, songplay_query_drop, user_query_drop, song_query_drop, artist_query_drop, time_query_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_query_insert, user_query_insert, song_query_insert, artist_query_insert, time_query_insert]

count_user_query = [total_number_user_query_count]

count_artist_query = [total_number_artist_query_count]

count_song_query = [total_number_song_query_count]

number_songplay_artist_query = [number_songplay_by_artist_query_count]

number_songplay_user_query = [number_songplay_by_user_query_count]

number_songplay_sogn_query = [number_songplay_by_song_query_count]

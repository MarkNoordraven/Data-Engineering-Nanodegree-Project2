import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
CLUSTER_HOST = config.get("CLUSTER","HOST")
CLUSTER_DB_NAME = config.get("CLUSTER","DB_NAME")
CLUSTER_DB_USER = config.get("CLUSTER","DB_USER")
CLUSTER_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
CLUSTER_PORT = config.get("CLUSTER","DB_PORT")
CLUSTER_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar,
                                                                            auth varchar,
                                                                            firstName varchar,
                                                                            gender varchar(1),
                                                                            itemInSession int,
                                                                            lastName varchar,
                                                                            length float,
                                                                            level varchar(4),
                                                                            location varchar,
                                                                            method varchar(3),
                                                                            page varchar,
                                                                            registration float,
                                                                            sessionId int,
                                                                            song varchar,
                                                                            status int,
                                                                            ts bigint,
                                                                            userAgent varchar,
                                                                            userId int)
                              """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (song_id text primary key,
                                                                           title varchar(1024),
                                                                           duration float,
                                                                           year float,
                                                                           num_songs float,
                                                                           artist_id text,
                                                                           artist_name varchar(1024),
                                                                           artist_latitude float,
                                                                           artist_longitude float,
                                                                           artist_location varchar(1024))
                              """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1),
                                                                  start_time bigint,
                                                                  user_id int NOT NULL,
                                                                  level varchar,
                                                                  song_id varchar,
                                                                  artist_id varchar,
                                                                  session_id int,
                                                                  location varchar,
                                                                  user_agent varchar,
                                                                  primary key (start_time,user_id))
                         """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int,
                                                          first_name varchar,
                                                          last_name varchar,
                                                          gender varchar,
                                                          level varchar,
                                                          primary key (user_id))
                     """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar,
                                                         title varchar,
                                                         artist_id varchar,
                                                         year int,
                                                         duration numeric,
                                                         primary key (song_id))
                     """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar,
                                                              name varchar,
                                                              location varchar,
                                                              latitude numeric,
                                                              longitude numeric,
                                                              primary key (artist_id))
                       """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint,
                                                         hour int,
                                                         day int,
                                                         week int,
                                                         month int,
                                                         year int,
                                                         weekday int,
                                                         primary key (start_time))
                     """)

# LOAD AND INSERT TABLES
staging_events_copy = ("""COPY staging_events 
                          FROM {}
                          credentials 'aws_iam_role={}'   
                          JSON {}  
                          compupdate off
                          region 'us-west-2';
                        """).format(config.get("S3","LOG_DATA"), 
                                    config.get("IAM_ROLE", "ARN"), 
                                    config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         credentials 'aws_iam_role={}'
                         JSON 'auto'
                         truncatecolumns
                         compupdate off
                         region 'us-west-2';
                      """).format(config.get("S3","SONG_DATA"), 
                                  config.get("IAM_ROLE", "ARN"))

songplay_table_insert = ("""INSERT INTO songplays (start_time,
                                                   user_id,
                                                   level,
                                                   song_id,
                                                   artist_id,
                                                   session_id,
                                                   location,
                                                   user_agent)
                            SELECT DISTINCT staging_events.ts AS start_time,
                                            staging_events.userid::INTEGER AS user_id,
                                            staging_events.level,
                                            staging_songs.song_id,
                                            staging_songs.artist_id,
                                            staging_events.sessionid AS session_id,
                                            staging_events.location,
                                            staging_events.useragent AS user_agent
                            FROM staging_events
                            LEFT join staging_songs
                                 ON staging_events.song = staging_songs.title
                                 AND staging_events.artist = staging_songs.artist_name
                            LEFT OUTER join songplays
                                       ON staging_events.userid = songplays.user_id
                                       AND staging_events.ts = songplays.start_time
                            WHERE staging_events.page = 'NextSong'
                                  AND staging_events.userid IS NOT NULL
                                  AND staging_events.level IS NOT NULL
                                  AND staging_songs.song_id IS NOT NULL
                                  AND staging_songs.artist_id IS NOT NULL
                                  AND staging_events.sessionid IS NOT NULL
                                  AND staging_events.location IS NOT NULL
                                  AND staging_events.useragent IS NOT NULL
                                  AND songplays.songplay_id is NULL
                            ORDER BY start_time, user_id;
                         """)

user_table_insert = ("""INSERT INTO users
                        SELECT DISTINCT userid AS user_id,
                                        firstname AS first_name,
                                        lastname AS last_name,
                                        gender,
                                        level
                                  FROM staging_events
                                 WHERE user_id IS NOT NULL
                                 AND staging_events.page = 'NextSong'
                         GROUP BY user_id, first_name, last_name, gender, level
                         ORDER BY user_id;
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs
                          WHERE song_id IS NOT NULL
                       """)

time_table_insert = ("""INSERT INTO time
                        SELECT DISTINCT ts AS start_time,
                                        date_part(hour, '1970-01-01'::date + ts/1000 * interval '1 second') AS hour,
                                        date_part(day, '1970-01-01'::date + ts/1000 * interval '1 second') AS day,
                                        date_part(week, '1970-01-01'::date + ts/1000 * interval '1 second') AS week,
                                        date_part(month, '1970-01-01'::date + ts/1000 * interval '1 second') AS month,
                                        date_part(year, '1970-01-01'::date + ts/1000 * interval '1 second') AS year,
                                        date_part(weekday, '1970-01-01'::date + ts/1000 * interval '1 second') AS weekday
                        FROM songplays
                        GROUP BY ts
                        ORDER BY start_time
                     """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
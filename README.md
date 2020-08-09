## Project: Data Warehouse
Second project in the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)


![https://confirm.udacity.com/PLQJPKUN](https://github.com/MarkNoordraven/Data-Engineering-Nanodegree-CapstoneProject/blob/master/Data%20Engineering%20certificate.PNG)



### Introduction
Sparkify is startup operating a music streaming app.

The app collects user activity in JSON logs and stores JSON metadata on the songs. The data resides is Amazon S3, but the analytics team is unable to generate insight from the data in its current state.

Sparkify has tasked me as a data engineer to:
* build an ETL pipeline
  * extract data from Amazon S3
  * stage the data in Amazon Redshift
  * transform the data into a set of dimensional tables
 
### Goal 
Target for this task is to enable Sparkify's analytics team to finding insights in what songs their users are listening to.

### Database Schema

The database schema is optimized for the purposes of the analytics team. The team desires fast reads and short queries to speed up the analysis iteration process.

I chose to go for a star schema with the following fact table:

__songplays__  
`start_time` | `user_id ` | `level` | `song_id` | `artist_id` | `session_id` | `location` | `user_agent`  
_This table minimizes the number of columns in order to speed up table reads_

For the dimenions, the analytics team desires to slice and dice across the time axis, the user axis, songs axis and artist axis.

In order to facilitate this we have created the following dimensional tables:

__users__  
`user_id` | `first_name` | `last_name` | `gender` | `level`  
_This table enables us to find which surname is most common our users, or to check which songs are preferred by female listeners._

__songs__  
`song_id` | `title` | `artist_id` | `year` | `duration`  
_This allows us to perform analysis on how song length impacts the number of plays._

__artists__  
`artist_id` | `name` | `location` | `lattitude` | `longitude`   
_With the artists table we can report the countries which harbor the most popular artists._

__time__  
`start_time` | `hour` | `day` | `week` | `month` | `year` | `weekday`    
_The time table allows us to find peak traffic and allows us to plan our downtimes for maintenance more optimally._

This leads to the following diagram:
![alt text](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584109948/Song_ERD.png "Sparkify's schema")

### ETL Pipeline
First we create two staging tables as an intermediary step. We will read from these tables in order to create the final fact and dimensional tables.

A table `staging_events_copy` collects the JSON logs with the following SQL query:

```
staging_events_copy = ("""COPY staging_events 
                          FROM {}
                          credentials 'aws_iam_role={}'   
                          JSON {}  
                          compupdate off
                          region 'us-west-2';
                        """).format(config.get("S3","LOG_DATA"), 
                                    config.get("IAM_ROLE", "ARN"), 
                                    config.get("S3", "LOG_JSONPATH"))
```

A table `staging_songs_copy` collects the JSON song metadata with the following SQL query:

```
staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         credentials 'aws_iam_role={}'
                         JSON 'auto'
                         truncatecolumns
                         compupdate off
                         region 'us-west-2';
                      """).format(config.get("S3","SONG_DATA"), 
                                  config.get("IAM_ROLE", "ARN"))
```                               

After the staging tables have been created, we insert data into the fact table:

```
songplay_table_insert = 
("""INSERT INTO songplays (start_time,
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
 ```
 
We also insert data from the staging tables into the dimensional tables:
```
user_table_insert = 
("""INSERT INTO users
    SELECT DISTINCT userid AS user_id,
                    firstname AS first_name,
                    lastname AS last_name,
                    gender,
                    level
    FROM staging_events
    WHERE user_id IS NOT NULL
    GROUP BY user_id, first_name, last_name, gender, level
    ORDER BY user_id;
 """)
 ```

```
song_table_insert = 
("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
 """)
 ```

```
artist_table_insert = 
("""INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT DISTINCT (artist_id,
                     artist_name,
                     artist_location,
                     artist_latitude,
                     artist_longitude)
    FROM staging_songs
    WHERE song_id IS NOT NULL
 """)
 ```

```
time_table_insert = 
("""INSERT INTO time
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
 ```
 
### Files
* ___dwh.cfg__ contains cluster credentials, the AWS role and the S3 directory locations
* ___sql_queries.py___ contains the SQL queries to drop, create, copy and insert tables
* ___create_tables.py___ drops existing tables and creates fresh tables  
* ___etl.py__ creates staging tables and inserts data into fact and dimension tables

### Running the pipeline
1. Open terminal  
2. type "python create_tables.py" to create empty tables  
3. type "python etl.py" to copy data into staging tables, and insert the data into fact and dimension tables

This results in Redshift tables which can be queried by the Sparkify analytics team!

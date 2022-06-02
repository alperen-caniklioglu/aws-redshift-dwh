import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

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
CREATE TABLE IF NOT EXISTS staging_events
(
    se_artist VARCHAR,
    se_auth VARCHAR,
    se_firstname VARCHAR ,
    se_gender CHAR,
    se_iteminsession INTEGER,
    se_lastname VARCHAR ,
    se_length NUMERIC,
    se_level VARCHAR,
    se_location VARCHAR,
    se_method  VARCHAR,
    se_page  VARCHAR,
    se_registration BIGINT,
    se_sessionid INTEGER,
    se_song VARCHAR,
    se_status INTEGER,
    se_ts BIGINT sortkey,
    se_useragent VARCHAR,
    se_userid INTEGER 
) DISTSTYLE AUTO
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER sortkey,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
) DISTSTYLE EVEN
""")



songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    sp_songplayid INTEGER  IDENTITY(0,1) NOT NULL, 
    sp_starttime timestamp NOT NULL sortkey, 
    sp_userid integer NOT NULL ,
    sp_level VARCHAR,
    sp_songid VARCHAR,
    sp_artistid VARCHAR,
    sp_sessionid INTEGER,
    sp_location VARCHAR,
    sp_useragent VARCHAR,
    PRIMARY KEY (sp_songplayid)
) DISTSTYLE EVEN
""") 

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    u_userid INTEGER NOT NULL distkey sortkey, 
    u_firstname VARCHAR, 
    u_lastname varchar , 
    u_gender varchar , 
    u_level VARCHAR,
    PRIMARY KEY (u_userid))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    s_songid VARCHAR NOT NULL sortkey,
    s_title VARCHAR,
    s_artistid VARCHAR,
    s_year SMALLINT,
    s_duration NUMERIC,
    PRIMARY KEY (s_songid))
    DISTSTYLE ALL
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    a_artistid varchar NOT NULL sortkey,
    a_name VARCHAR,
    a_location VARCHAR,
    a_latitude FLOAT,
    a_longitude FLOAT,
    PRIMARY KEY (a_artistid))
    DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    t_start_time timestamp NOT NULL sortkey,
  	t_start_time_ms bigint not null,
    t_hour SMALLINT,
    t_day SMALLINT,
    t_week SMALLINT,
    t_month SMALLINT,
    t_year SMALLINT,
    t_weekday SMALLINT,
    PRIMARY KEY(t_start_time))
    DISTSTYLE ALL
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
iam_role {}
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
format as json {}
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs FROM {}
    iam_role {}
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
    (
        sp_starttime , sp_userid , sp_level , sp_songid , sp_artistid, sp_sessionid, sp_location, sp_useragent
    ) 
    
    select 
        timestamp 'epoch' + se_ts / 1000 * interval '1 second',
        se_userId,
        se_level, 
        ss.song_id ,
        ss.artist_id,
        se.se_sessionId,
        se.se_location,
        se.se_useragent
    from staging_events se
    left outer join staging_songs ss
    on se.se_artist = ss.artist_name  and se.se_song = ss.title
    where se.se_page = 'NextSong';
""")


users_table_delete = ("""
    delete from users
    using staging_events se
    where users.u_userid = se.se_userid
""")


user_table_insert = ("""
    INSERT INTO users 
    (
        u_userid , u_firstname , u_lastname , u_gender , u_level 
    ) 
    select distinct se.se_userid ,
        se.se_firstname ,
      	se.se_lastname ,
        se.se_gender,
        se.se_level from (
    select  
        distinct se1.se_userid ,
        se1.se_firstname ,
      	se1.se_lastname ,
        se1.se_gender,
        se1.se_level,
        row_number() over (partition by se1.se_userid order by se_ts desc) rn
    from staging_events se1
    where se1.se_userid is not null 
    and se1.se_page = 'NextSong') se where rn = 1;
""")



song_table_insert = ("""
    INSERT INTO songs 
    (
        s_songid , s_title , s_artistid , s_year , s_duration
    ) 
    select 
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where song_id not in (select s_songid from songs)
    and song_id is not null
""")


artist_table_insert = ("""
    INSERT INTO artists 
    (
        a_artistid , a_name , a_location, a_latitude, a_longitude
    ) 
    select distinct artist_id, 
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        from (
    select 
        artist_id, 
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude,
        row_number() over (partition by artist_id order by year desc) as rn
    from staging_songs s1
    where artist_id not in (select a_artistid from artists) and artist_id is not null)
    where rn=1
""")

time_table_insert = ("""
    INSERT INTO time 
    (
         t_start_time, t_start_time_ms, t_hour ,  t_day , t_week , t_month , t_year , t_weekday
    ) 
    select 
        distinct timestamp 'epoch' + se_ts /1000 * interval '1 second',
        se_ts,
        extract (hour from ( timestamp 'epoch' + se_ts / 1000 * interval '1 second')),
        extract (day from ( timestamp 'epoch' + se_ts / 1000 * interval '1 second')),
        extract (week from ( timestamp 'epoch' + se_ts / 1000 * interval '1 second')),
        extract (month from ( timestamp 'epoch' + se_ts / 1000 * interval '1 second')),
        extract (year from ( timestamp 'epoch' + se_ts / 1000  * interval '1 second')),
         extract (dow from ( timestamp 'epoch' + se_ts / 1000  * interval '1 second'))
    from staging_events se
    where se_ts not in (select t_start_time_ms from time)  and se_ts is not null      
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_delete, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

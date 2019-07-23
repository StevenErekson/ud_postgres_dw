# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS  songplays ( songplay_id int not null,
                                    start_time timestamp not null,
                                    user_id int not null ,
                                    level varchar not null,
                                    song_id int,
                                    artist_id int,
                                    session_id varchar not null,
                                    location varchar,
                                    user_agent varchar,
                                PRIMARY KEY (songplay_id),
                                constraint songplay_uq unique (session_id, start_time)    
                                    )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS  users (user_id int not null,
                                    first_name varchar not null,
                                    last_name varchar not null,
                                    gender varchar,
                                    level varchar,
                                PRIMARY KEY (user_id)
                                    )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS  songs (song_id int not null,
                                    title varchar not null ,
                                    artist_id int ,
                                    year varchar,
                                    duration float,
                                PRIMARY KEY (song_id), 
                                constraint title_uq unique (title)
                                    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS  artists (artist_id int not null,
                                    name varchar not null,
                                    location varchar,
                                    latitude varchar,
                                    longitude varchar,
                                PRIMARY KEY (artist_id),
                                constraint artname_uq unique (name)
                                    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS  time (start_time timestamp not null,
                                    hour int not null,
                                    day int not null,
                                    week int not null,
                                    month int not null,
                                    year int not null,
                                    weekday varchar not null,
                                PRIMARY KEY (start_time)
                                    )
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        VALUES(%s,%s, %s, %s, %s,%s, %s, %s, %s) ON CONFLICT (session_id, start_time) DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        VALUES(%s,%s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE
                                                                                SET first_name  = EXCLUDED.first_name,
                                                                                last_name = EXCLUDED.last_name,
                                                                                gender = EXCLUDED.gender,
                                                                                level = EXCLUDED.level""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        VALUES(%s,%s, %s, %s, %s) ON CONFLICT (title) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                        VALUES(%s,%s, %s, %s, %s)  ON CONFLICT (name) DO NOTHING
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        VALUES(%s,%s, %s, %s, %s, %s, %s)ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS

song_select = ("""Select s.song_id, a.artist_id FROM SONGS s JOIN artists a on s.artist_id = a.artist_id where s.title = %s and a.name = %s and s.duration = %s
""")

# artist_select = (f"""Select a.artist_id FROM artists a where a.name = {row.artist}
# """)


max_songplay_id = ("select max(songplay_id)  from songplays")

max_song_id = ("select max(song_id)  from songs")

max_artist_id = ("select max(artist_id)  from artists")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

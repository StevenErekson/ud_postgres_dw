Purpose: process song and log files to populate songplay dimensional model for analytics team to better understand the songs user are listening to.
    DROP/Create Tables(create_tables.py) : drops and rebuilds tables based on lists fed from sql_queries.py
    Inserts and other Selects (sql_queries.py) : script holds SQL needed in ETL
    ETL (etl.py): Script processes files and runs needed SQL with parameters.
    
    DB Schema:
        Songplays: fact table with all of the next song records from the file. Primary Key: SongPlay_Id: This id is int and is incremental value for each record in the log file. This is derived as there is not one unique id column in the data. Type 0 = no updates.
        Songs: dim table of all songs both from song and log files (made surrogate song_id as not all songs were found in song files). Primary Key: Song_id this is an int column that increments anytime a new song title is identified in either the song file or log file. I created this surrogate key because there was very few songs from the song file that appeared in the log file and the log file does not have the song_id. Type 0 = no updates.
        Artists: dim table of all artists both from song and log files (made surrogate artist_id as not all artists were found in song files).Primary Key: Artist_id this is an int column that increments anytime a new Artist name is identified in either the song file or log file. I created this surrogate key because there was very few artists from the song file that appeared in the log file and the log file does not have the artist_id. Type 0 = no updates.
        Users: dim table of all users log files. Primary Key: user_id from the log file this is used as it always appears in log file. Type 1 =  update non pk columns.
        Time: dim time for each unique start_time. Primary Key: Start_time as each record in the log file has a start_time ('ts') timestamp. Type 0 = no updates.
        
     
     sql queries called from sql_queries.py
     songplay_table_drop: Drops SongPlays table if it exists.
     user_table_drop: Drops Users table if it exists.
     song_table_drop: Drops Songs table if it exists.
     artist_table_drop: Drops Artists table if it exists.
     time_table_drop: Drops Time table if it exists.
     
     songplay_table_create: Creates SongPlays table if it does not exist.
     song_table_create: Creates Songs table if it does not exist.
     artist_table_create: Creates Artists table if it does not exist.
     user_table_create: Creates users table if it does not exist.
     time_table_create: Creates time table if it does not exist.
     
     songplay_table_insert: inserts record into SongPlays table if record already exists it does nothing.
     song_table_insert: inserts record into Songs table if record already exists it does nothing.
     artist_table_insert: inserts record into Artists table if record already exists it does nothing.
     user_table_insert: inserts record into Users table if record already exists it updates all columns but user_id.
     time_table_insert: inserts record into Time table if record already exists it does nothing.
     
     song_select: retrieves artist_id, song_id for SongPlays fact table as these ids are not in the log file.
     
     max_songplay_id: retrieves max SongPlay_id so ids can be incremented in loops.
     max_song_id: retrieves max Song_id so ids can be incremented in loops.
     max_artist_id: retrieves max artist_id so ids can be incremented in loops.
     
     
     
     Run Instrucstions: from terminal: "python create_tables.py" to drop, create db and tables
                                       "python etl.py" to process files and populate db and tables




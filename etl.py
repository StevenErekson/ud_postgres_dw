import os
import glob
import psycopg2
import pandas as pd
import sql_queries as q


def process_song_file(cur, filepath):
    """
    Processes song files.
    
    Parameters: 
    cur: postgres cursor
    filepath: directory of song files
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # grab max song surrogate key
    cur.execute(q.max_song_id)
    msong_id_lst = cur.fetchall()
    if msong_id_lst[0][0]:
        msong_id = int(msong_id_lst[0][0])
    else:
        msong_id = 0
    df['msong_id'] = msong_id + 1,
    df['artistkey'] = -1

    # insert song record
    song_data = list(df[['msong_id', 'title', 'artistkey', 'year', 'duration']].values[0])
    cur.execute(q.song_table_insert, song_data)

    # grab max artist surrogate key
    cur.execute(q.max_artist_id)
    martist_id_lst = cur.fetchall()
    if martist_id_lst[0][0]:
        martist_id = int(martist_id_lst[0][0])
    else:
        martist_id = 0
    df['martist_id'] = martist_id + 1

    # insert artist record
    artist_data = list(
        df[['martist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(q.artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes log files.
    
    Parameters: 
    cur: postgres cursor
    filepath: directory of log files
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    t = df['ts']
    h = df['ts'].dt.hour
    d = df['ts'].dt.day
    w = df['ts'].dt.week
    m = df['ts'].dt.month
    y = df['ts'].dt.year
    wd = df['ts'].dt.day_name()

    # prep time data for dict
    time_data = [t, h, d, w, m, y, wd]
    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))

    # make df
    time_df = pd.DataFrame(time_dict)

    # insert into dim
    for i, row in time_df.iterrows():
        cur.execute(q.time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(q.user_table_insert, row)

    # change NaNs to None/Nulls
    df = df.where((pd.notnull(df)), None)

    # load artist table
    artist_df = df[['artist']]

    # grab max surrogate key
    cur.execute(q.max_artist_id)
    martist_id_lst = cur.fetchall()
    if martist_id_lst[0][0]:
        martist_id = int(martist_id_lst[0][0])
    else:
        martist_id = 0

    # insert artist records
    for i, row in artist_df.iterrows():
        iartist_id = martist_id + (i + 1)
        artist_data = (iartist_id, row.artist, None, None, None)
        cur.execute(q.artist_table_insert, artist_data)

        # load song table
    song_df = df[['song', 'length', 'artist']]

    # grab max surrogate key
    cur.execute(q.max_song_id)
    msong_id_lst = cur.fetchall()
    if msong_id_lst[0][0]:
        msong_id = int(msong_id_lst[0][0])
    else:
        msong_id = 0

    # insert song records
    for i, row in song_df.iterrows():

        row.artist = row.artist.replace("'", "''")  # replace ' to '' for SQL formatting
        # get artistid from  artist tables
        cur.execute(
            f"""Select a.artist_id FROM artists a where a.name = '{row.artist}' """)  # f string worked better than %s
        results = cur.fetchone()

        if results:
            artistid = results
        else:
            artistid = None

        isong_id = msong_id + (i + 1)
        song_data = (isong_id, row.song, results, None, row.length)
        cur.execute(q.song_table_insert, song_data)

        # grab max surrogate key
    cur.execute(q.max_songplay_id)
    msongplay_id_lst = cur.fetchall()
    if msongplay_id_lst[0][0]:
        msongplay_id = int(msongplay_id_lst[0][0])
    else:
        msongplay_id = 0
    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(q.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        isongplay_id = msongplay_id + (index + 1)

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (isongplay_id, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(q.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Identifies all files to be processed.
    
    Parameters: 
    cur: postgres cursor
    conn: postgres db connection
    filepath: directories of all files to be processed
    func: above function to be called on file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main function where process files data is processed."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print('song file processing is complete')
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    print('log file processing is complete')
    conn.close()


if __name__ == "__main__":
    main()

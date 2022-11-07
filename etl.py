import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np



def process_song_file(cur, filepath):
    # open song file
    df_songs = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df_songs.values[0, 6], df_songs.values[0, 7],
                 df_songs.values[0, 1], df_songs.values[0, 9],
                 df_songs.values[0, 8]]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = [df_songs.values[0, 1], df_songs.values[0, 5],
               df_songs.values[0, 4], df_songs.values[0, 3],
               df_songs.values[0, 2]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t_data = (pd.to_datetime(df.ts, unit='ms')).dt

    # insert time data records
    time_data = [df.ts, t_data.hour, t_data.day,
                 t_data.isocalendar().week,
                 t_data.month, t_data.year,
                 t_data.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(np.array(time_data).T, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(np.array([df.userId, df.firstName,
                        df.lastName, df.gender, df.level]).T,
                        columns = ['user ID', 'first name',
                                   'last name', 'gender', 'level']
                      )

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId,
                         row.level, songid, artistid,
                         row.sessionId, row.location,
                         row.userAgent )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
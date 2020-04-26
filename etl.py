import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    song_data = (song_data[0], song_data[1], song_data[2], song_data[3], song_data[4])
    try:
        cur.execute(song_table_insert, song_data)
    except:
        pass
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    artist_data = (artist_data[0], artist_data[1], artist_data[2], artist_data[3], artist_data[4])
    try:
        cur.execute(artist_table_insert, artist_data)
    except:
        pass



def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']== 'NextSong']

    # convert timestamp column to datetime
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    df_t=df
    df_t['year']= df_t['datetime'].dt.year
    df_t['month']= df_t['datetime'].dt.month
    df_t['week']= df_t['datetime'].dt.week
    df_t['day']= df_t['datetime'].dt.day
    df_t['hour']= df_t['datetime'].dt.hour
    df_t['weekday']= df_t['datetime'].dt.weekday_name
    # insert time data records
    time_data = (df_t.datetime, df_t.hour, df_t.day, df_t.week, df_t.month, df_t.year, df_t.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        R=(str(row.song), str(row.artist), str(row.length))
    # get songid and artistid from song and artist tables
    

        results = cur.execute(song_select, R)
        songid, artistid = results if results else None, None

    # insert songplay record
        songplay_data = (str(row.ts), str(row.userId), str(row.level), str(songid), str(artistid), str(row.sessionId), str(row.location), str(row.userAgent))
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


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
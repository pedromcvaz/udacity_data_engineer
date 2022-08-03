import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Reads the log file containing the song and artist
    data andinserts it into the songs and artists tables
    respectively.

    Parameters
    ----------
    cur: psycopg2.cursor
        Cursor object to interact with the database
    filepath: str
        Path to the file being processed
    """
    # open song file
    df = pd.read_json(filepath, typ="series")

    # insert song record
    song_data = df[["song_id", "title", "artist_id",
                    "year", "duration"]].values.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id",
                      "artist_name",
                      "artist_location",
                      "artist_latitude",
                      "artist_longitude"]].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads the log file containing the metadata, filters for the relevant
    events, preprocesses the data, and inserts it into the time, user,
    and songplays tables.

    Parameters
    ----------
    cur: psycopg2.cursor
        Cursor object to interact with the database
    filepath: str
        Path to the file being processed
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit="ms").reset_index(drop=True)

    # insert time data records
    time_data = [[t[i], t[i].hour, t[i].day, t[i].week, t[i].month,
                  t[i].year, t[i].weekday()] for i in range(len(t))]
    column_labels = (
        "timestamp",
        "hour",
        "day",
        "week",
        "month",
        "year",
        "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Recursively reads all of the nested files under the
    path and processes them to the tables

    Parameters
    ----------
    cur: psycopg2.cursor
        Cursor object to interact with the database
    conn: psycopg2.connect
        Connection to the database
    filepath: str
        Path to the file being processed
    func: function
        Processing function to be called

    Returns
    -------
    all_files: list of str
        List containing the absolute path of all the
        files processed
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

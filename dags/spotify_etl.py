import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


def check_data(df: pd.DataFrame) -> bool:
    list_error = []
    #  is empty ?
    message_empty = "Nothing downloaded. Perhaps you listened no one song that day. Execution Finished"
    assert not df.empty, list_error.append(message_empty)

    # is duplicates ?
    message_keys = "Primary Key is violated, hence there are duplicates in data"
    assert pd.Series(df["played_at"]).is_unique, list_error.append(message_keys)

    # is null values ?
    message_null = "Null values found"
    assert not df.isnull().values.any(), list_error.append(message_null)

    # is it last 24 hours ?
    '''yesterday_ = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_ = yesterday_.replace(hour=0, minute=0, second=0, microsecond=0)
    today_ = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_list = [yesterday_, today_]
    timestamps = df["timestamp"].to_list()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") not in yesterday_list:
            print(yesterday_)
            list_error.append("Something wrong with timestamps")
            raise Exception("Something wrong with timestamps")'''


    if list_error:
        for error in list_error:
            print(error)
        return False
    return True
#if __name__ == '__main__':


def run_spotify_etl():
    db_location = 'sqlite:///my_tracks.sqlite'
    user_id = '05q92yrt0926gkmfrvves51wi'
    token = "BQCpSzKtFq7lLfz3Exos6GqBN1wz2aaD-rGBMaTITukD6B6Fg48PGOVFxlIjGogXcTMoAD4ty52VYeS_N5w1pAB-hF6WTFlv3CI4RDV56jmmi2tH_irmD3OxY1G1QvBwC0igVPKpjpGhaK8vNnLlQ_LoOi9Lx-VOsCy4xCcR"

    # Extract part
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
                         headers=headers)

    data = r.json()

    #print(data)

    songs = []
    artist = []
    played_at = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        songs.append(song["track"]["name"])
        artist.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song": songs,
        "artist": artist,
        "played_at": played_at,
        "timestamp": timestamps

    }
    song_df = pd.DataFrame(song_dict, columns=["song", "artist", "played_at", "timestamp"])

    # Validate
    if check_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(db_location)
    connect = sqlite3.connect("my_tracks.sqlite")
    cursor = connect.cursor()

    querry = """
    CREATE TABLE IF NOT EXISTS my_tracks(
        song VARCHAR(200),
        artist VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(querry)

    # song_df.to_sql("my_tracks", engine, index=False, if_exists='replace')
    try:
        song_df.to_sql("my_tracks", engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')

    connect.close()
    print("Close database successfully")

import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import _sqlite3




DATABASE_LOCATION = 'sqlite://Users/alcherniaev/PycharmProjects/ETL_spotify/my_tracks.sqlite'
USER_ID = '05q92yrt0926gkmfrvves51wi'
TOKEN = "BQAPZi8oW7WSdDJ0_jC6Qzf0Ih3OJIs4tcjJy3HUAUF_9e3eoX0GjcHMagi1nX5Bux4-cQkH9LwUGzqEkJ9VRzZKeC9EvjjIXmObYDy2dwOK1eCOJR4vu--BpH1voBJ-5zlwHyvypudR14KraHy4MHm3KGtHvGnH5tuMqBtT"

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

    '''    # is it last 24 hours ?
    yesterday_ = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_ = yesterday_.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].to_list()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday_:
            print(yesterday_)
            list_error.append("Something wrong with timestamps")
            raise Exception("Something wrong with timestamps")
    '''
    if not list_error:
        return True
    else:
        for error in list_error:
            print(error)
        return False


if __name__ == '__main__':

    # Extract part

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", headers=headers)

    data = r.json()

    print(data)

    songs = []
    artist = []
    played_at = []
    timestamps = []

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

    print(song_df)

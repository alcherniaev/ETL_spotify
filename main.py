import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import _sqlite3



# Extract part
DATABASE_LOCATION = 'sqlite://Users/alcherniaev/PycharmProjects/ETL_spotify/my_tracks.sqlite'
USER_ID = '05q92yrt0926gkmfrvves51wi'
TOKEN = 'BQAkW61Tv3QPVx6p-h7jqG6wPO11Py8NSOYZUCeIq7z62m7j85hVWD-IhEf4HFM7RwoWOiK2M9l97VbcIK3pl4qGIqRAmecHqsIFPv6oVAVT3kuVgjUGMEwLN5ayzHkosfANIOsz3LXCIdg156CKq0JA5UzXjS9jiZDTidj_'

if __name__ == '__main__':
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

    print(song_df)

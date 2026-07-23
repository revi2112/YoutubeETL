import json
import requests
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="./.env")
API_KEY =os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")

def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY} "

        response = requests.get(url)
        print(response)

        data = response.json()
        #py obj to json string
       # print(json.dumps(data, indent = 4))

        channel_items = data['items'][0]

        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        
        return channel_playlistId
        
    except requests.exceptions.RequestException as e:
        raise e
    
    
if __name__ == '__main__':
    get_playlist_id()

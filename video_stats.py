from datetime import date
import json
import requests
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="./.env")
API_KEY =os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
MAX_RESULTS = 50

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

def get_video_ids(playlistID):
    video_ids = []
    pageToken = None #AS WE LOOP THROUGH EACH BATCH OF VIDEO ( BATCH SIZE IS 50)
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlistID}&key={API_KEY}"

    try:
        while True:
            #loop through iteams in the data response.
            url = base_url 
            
            if pageToken:   
                url += f"&pageToken={pageToken}"
             
            response = requests.get(url)
            data = response.json()
            #data.get("items", []) get is needed instead of data['items'] since ref error could be possible
            for item in data.get("items", []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken') #need to use get not [] possiblility that key doesnt exit
            
            if not pageToken:
                break
    
    except requests.exceptions.RequestException as e:
        raise e    
    return video_ids


def extract_video_data(video_ids):
    # for each video form - title publishedAt duration viewCount LikeCount commentCOuy
    extracted_data = []
    
    try:
        # video gets retivved in batches
        #generator function -> produces a sequence of of res 1 50 100
        #yield pauses the excustion and return the res to the iterator
        def batch_video_list(video_id_lst, batch_size):
            for i in range(0, len(video_id_lst), batch_size):
                print(f"generator: about to yield slice starting at {i}")
                yield video_id_lst[i: i + batch_size] #[0:50] pause [50:100] pause ...
                print(f"generator: resumed after yield, i was {i}")
        
        #next(gen fun) in background
        for batch in batch_video_list(video_ids, MAX_RESULTS):
            print(f"  outer loop: got batch {batch}")
            video_id_str = ",".join(batch)
            
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=statistics&part=snippet&id={video_id_str}&key={API_KEY}"
            
            response = requests.get(url)
            data = response.json()
            
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item['snippet']
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                    }

                extracted_data.append(video_data)
            
    except requests.exceptions.RequestException as e:
        raise e  

def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}"
    
    with open(file_path, "w", encoding="utf-8") as json_output:
        json.dump(extracted_data, json_output, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    playlistID =  get_playlist_id()
    video_ids = get_video_ids(playlistID)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
from datetime import date
import requests, json
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

API_KEY = Variable.get("API_KEY")
MAX_RESULTS = 50
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
AWS_CONN_ID = "aws_default"

@task
def get_playlist_id(channel_handle):
    #every channel has a special auto-gen playlist called "uploads playlist" youtube maintains contains all public videos
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={API_KEY} "

        response = requests.get(url)
        data = response.json()
        #py obj to json string
       # print(json.dumps(data, indent = 4))

        channel_items = data['items'][0]
        
        channel_id = channel_items["id"]

        playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        
        return {
            "channel_handle": channel_handle,
            "channel_id": channel_id,
            "playlist_id": playlist_id,
        }        
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def get_video_ids(channel_info):
    playlist_id = channel_info["playlist_id"]
    video_ids = []
    pageToken = None #AS WE LOOP THROUGH EACH BATCH OF VIDEO ( BATCH SIZE IS 50)
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"

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
                video_ids.append(item["contentDetails"]["videoId"])

            pageToken = data.get('nextPageToken') #need to use get not [] possiblility that key doesnt exit
            if not pageToken:
                break
    
    except requests.exceptions.RequestException as e:
        raise e    
    return {"video_ids": video_ids, **channel_info}

@task
def extract_video_data(video_info):
    # for each video form - title publishedAt duration viewCount LikeCount commentCOuy
    channel_handle = video_info["channel_handle"]
    channel_id = video_info["channel_id"]
    video_ids = video_info["video_ids"]
    extracted_data = []
    
    try:
        # video gets retivved in batches
        #generator function -> produces a sequence of of res 1 50 100
        #yield pauses the excustion and return the res to the iterator
        def batch_video_list(video_id_lst, batch_size):
            for i in range(0, len(video_id_lst), batch_size):
                yield video_id_lst[i: i + batch_size] #[0:50] pause [50:100] pause ...
        
        #next(gen fun) in background
        for batch in batch_video_list(video_ids, MAX_RESULTS):
            video_id_str = ",".join(batch)
            
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=statistics&part=snippet&id={video_id_str}&key={API_KEY}"
            
            response = requests.get(url)
            data = response.json()
            
            for item in data.get("items", []  ):
                video_id = item["id"]
                snippet = item['snippet']
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                extracted_data.append({
                    "video_id": video_id,
                    "channel_id": channel_id,
                    "channel_handle": channel_handle,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                    })
                
        return {"channel_handle": channel_handle, "data": extracted_data}
            
    except requests.exceptions.RequestException as e:
        raise e  
    
@task
def save_to_s3(extracted_data, ds= None):
    '''
    Writes one immutable snapshot per channel per day to s3
    ds is Airflow's logical date (YYYY-MM-DD), injected automatically. (represents dag run date)
    replace=True only guards against re-running the SAME day's snapshot during
    testing/backfills - it never touches a previous day's file, since the date
    if try to upload key alredy exists false raise erroe instead of overwrite true overwrite
    is part of the key.
    '''
    channel_handle = extracted_data["channel_handle"]
    data = extracted_data["data"]
    
    s3_key = f"raw/youtube/channel={channel_handle}/dt={ds}/videos.json" #file path for obj inside bucket
    json_body = json.dumps(data,indent=4, ensure_ascii=False )
    
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    #where to put file , write once formot 
    
    hook.load_string(
        string_data=json_body,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
 
    return s3_key
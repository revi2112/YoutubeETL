## core layer table forms by applying stating transform
## duaration column is in iso 8601 format PT#h#M#S - > PT time period 
## 1 day long it is seperated P#DT#H#M#S
# # is a number 
#change to h min sec
#YT API doesnt give type of video long or short can be determined by duration < 1 min


from datetime import timedelta, datetime


def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")
    components = ["D", "H", "M", "S"]
    
    values = {"D": 0, "H": 0, "M": 0, "S": 0}
    num = ""
    for char in duration_str:
        if char.isdigit():
            num += char
        else:
            values[char] = int(num)
            num = ""
            
    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )       
    
    return total_duration     
        
        
def transform_data(row):
    
    duration_td = parse_duration(row["Duration"])
    
    row["Duration"] = (datetime.min + duration_td).time()
    
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Long Video"

    return row
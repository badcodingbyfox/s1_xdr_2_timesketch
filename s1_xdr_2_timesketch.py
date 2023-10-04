import pandas as pd
import time
import datetime
from timesketch_api_client import config
from timesketch_import_client import importer
import requests
import json

######################################
#    Global Request Variables    #
######################################

# Basic Settings
api_token = input("API Token: ")    #   S1 XDR API Token - Taken from the XDR page NOT the S1 console dashboard
query_string = input("Dataset Query String: ") # example: 'agent.uuid = "<Agent UUID>" && src.process.storyline.id="<StroyID>"' encapsulate in single quotes
ts_sketch_id = input("Sketch ID: ") #   Timesketch sketch ID number
dataset_uri = "https://xdr.us1.sentinelone.net/api/query"

#   XDR Time Range
#   date -d "July 5 2023" '+%s%3N'
#   best to use epoch with milliseconds 
start_time = input("Start Time: ")
end_time = input("End Time: ")

#   Used for Timeline Import naming
current_time = datetime.datetime.now()
import_timestamp = current_time.strftime("%Y%m%d%H%M%S")

###############################
#    XDR Dataset Variables    #
###############################

match_dict = []
matches_list = []
pagination_token = "Start"

###########################
#    XDR Data Fetching    #
###########################

def xdr_fetch(api_token, query_string, start_time, end_time, pagination_token):
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type":"application/json"
    }
    if pagination_token == "Start":
        payload = {
               "queryType": "log",
               "maxCount": 5000,
               "filter": str(query_string),
               "startTime": start_time,
               "endTime": end_time
        }
    else:
        payload = {
               "queryType": "log",
               "maxCount": 5000,
               "filter": query_string,
               "startTime": start_time,
               "endTime": end_time,
               "continuationToken": pagination_token
        }
    print(headers)
    print(payload)
    request = requests.post(dataset_uri , headers=headers, json=payload)
    print(request)
    results = json.loads(request.text)
    if "matches" not in results.keys():
        print(results)
    return results

while pagination_token == "Start" or "continuationToken" in results.keys() or results['matches']:
    results = xdr_fetch(api_token, query_string, start_time, end_time, pagination_token)
    for event in results['matches']:
        event['attributes']['raw_event'] = json.dumps(event)
        event['attributes']['message'] = f"{event['attributes']['event.type']} in Source Process StoryID: {event['attributes']['src.process.storyline.id']} - from Process: {event['attributes']['src.process.name']}"
        #event['attributes']['message'] = f"{event['attributes']['event.type']} in Source Process StoryID: {event['attributes']['src.process.storyline.id']}"
        match_dict.append(event['attributes'])
    if "continuationToken" in results.keys():
        pagination_token = results['continuationToken']


#################################
###    Form List of Results     #
#################################

        matches_dataframe = pd.DataFrame(match_dict)
        matches_dataframe = matches_dataframe.rename(columns={ 'event.time' : 'datetime'})
        matches_dataframe['datetime'] = matches_dataframe['datetime'].astype(int)
        matches_dataframe['datetime'] = pd.to_datetime(matches_dataframe['datetime'], origin='unix', unit='ms')

########################################
#    Import events into Timesketch     #
########################################

        ts = config.get_client()
        my_sketch = ts.get_sketch(int(ts_sketch_id))
        #timeline_name = matches_dataframe['endpoint.name']+" "+import_timestamp+" (XDR)"
        timeline_name = matches_dataframe['endpoint.name']+" (XDR)"

        with importer.ImportStreamer() as streamer:
            streamer.set_sketch(my_sketch)
            streamer.set_timestamp_description('SentinelOne Management Activity')
            streamer.set_timeline_name(timeline_name)
            streamer.set_message_format_string(matches_dataframe['message'])
            streamer.add_data_frame(matches_dataframe)
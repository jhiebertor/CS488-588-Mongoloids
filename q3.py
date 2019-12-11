import pymongo
import json
#Connect to mongo atlas database
client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster1-qspsa.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client["test"]
#Query Foster NB station length
cursor = db["freeway_stations"].find({"locationtext" : "Foster NB"},{"_id": 0, "length" : 1})
station_length = float(cursor[0]['length']) 
#Query loopdata for speeds on 9/22/2011 for Foster NB
cursor = db["freeway_loopdata"].find({"$and" : [
    {'starttime': {'$regex' :  "2011-09-22"}},
    {'$or' : [
        {'detectorid' : '1361'},
        {'detectorid' : '1362'},
        {'detectorid' : '1363'}]}
        ]},{'_id': 0, 'speed': 1})
speeds = []
#Append speeds to list and convert to ints
for x in cursor:
    if(x['speed'] == ""):
        speeds.append(0)
    else:
        speeds.append(int(x['speed']))
intervals = []
#Data is in 20 second intervals, 15 datapoints are aggregated into a 5 minute interval
for x in range(0, len(speeds), 15):
    intervals.append(speeds[x:x+15])
travel_times = []
#Filter out zero values and calculate average travel time
for interval in intervals:
    interval = filter(lambda x: x != 0, interval)
    if(len(interval) != 0):
        travel_times.append(3600*station_length/(sum(interval)/len(interval)))
print(travel_times)

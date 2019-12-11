/**************************************************************
Q1
***************************************************************/

// use the count function with a specified query as an argument
db.freeway_loopdata.count({speed: {$ne:""},$expr: {$gt: [{$toInt: "$speed"},100]}})

/**************************************************************
Q2
***************************************************************/

//Create an array to hold the "Foster NB" detectorids
var d_ids = [] 
//Get the values from the freeway_detector collection (only the detectorid values)
db.freeway_detectors.json.find({locationtext: "Foster NB"}, {"detectorid":1, "_id": 0}).forEach(function(u){d_ids.push(u.detectorid)})

// Use aggregate function to return the sum of volumes in documents that match the condition
db.freeway_loopdata.aggregate([
{$match:{detectorid:{$in: d_ids}, starttime: {$gte:"2011-09-21 00:00:00-0", $lt:"2011-09-22 00:00:00-0"}}},
{$group: {_id:null,'totalvolume':{$sum: {$toInt:"$volume"}}}} ])


/**************************************************************
Q4
***************************************************************/

// Retrieving Foster NB detector id
var d_ids = []
db.freeway_detectors.json.find({locationtext: "Foster NB"}, 
{"detectorid": 1, "_id": 0})
.forEach(function(u){d_ids.push(u.detectorid)})

// Retrieving Foster NB length
var s_len = []
db.freeway_stations.find({locationtext: "Foster NB"}, 
{"length": 1, "_id": 0})
.forEach(function(u){s_len.push(u.length)})

// Calculating the average travel time in hour
var a = 
db.freeway_loopdata.aggregate([
{$match: {detectorid:{$in:d_ids}, 
    speed: {$ne:""}, 
    starttime: {$gte: "2011-09-22 07:00:00-0", $lt: "2011-09-22 09:00:00-0"}, 
    starttime: {$gte: "2011-09-22 16:00:00-0", $lt: "2011-09-22 18:00:00-0"}}}, 
    {$group: {_id:null, 
    "avgTravel": {$avg: {$divide: [s_len[0], {$toInt: "$speed"}]}}}}])
    .toArray()

a[0].avgTravel * 3600


/**************************************************************
Q6
***************************************************************/

var stop = [];var names = []; var i; var rt = [];  

var finish = db.freeway_stations.aggregate(
[{$match:{locationtext:"Columbia to I-205 NB"}},
 {$project:{_id:0, stationid:1}}]);
 
finish.forEach(function(y) {stop.push(y.stationid)});

var cur = db.freeway_stations.find(
{"locationtext":"Johnson Cr NB"},
{_id:0, stationid:1, downstream:1, locationtext:1}); 

while(!(rt.includes(stop[0]))) {

cur.forEach(function(x) {
rt.push(x.stationid); 
names.push(x.locationtext);}); 

cur = db.freeway_stations.aggregate(
[{$match:{upstream:rt[rt.length-1]}},
 {$project:{_id:0, stationid:1, downstream:1, locationtext:1}}]);
}

print(names);

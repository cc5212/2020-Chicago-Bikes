-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,
hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,
latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);


from_total_traffic = GROUP raw BY from_station_id;
from_total_traffic_count = FOREACH from_total_traffic GENERATE COUNT(raw) AS count, group AS from_station_id, flatten($1.from_station_name);
from_total_traffic_count = DISTINCT from_total_traffic_count;
order_from_total_traffic_count = ORDER from_total_traffic_count BY count DESC;

/*
to_total_traffic = GROUP raw BY to_station_id;
to_total_traffic_count = FOREACH to_total_traffic GENERATE COUNT(raw) AS count, group AS to_station_id, flatten($1.to_station_name);
to_total_traffic_count = DISTINCT to_total_traffic_count;
to_total_traffic_count = FOREACH to_total_traffic_count GENERATE $0, $1, MIN($2);
order_to_total_traffic_count = ORDER to_total_traffic_count BY count DESC;

-- Output files
*/

STORE order_from_total_traffic_count INTO '/uhadoop2020/group12/bici/from_total_traffic';
-- STORE order_to_total_traffic_count INTO '/uhadoop2020/group12/bici/to_total_traffic';
-- STORE ordered_actress_count INTO '/uhadoop2020/group12/imdb-costars/female';



-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);

/* lugar donde se retiran mas bicicletas
from_total_traffic = GROUP raw BY from_station_id;
from_total_traffic_count = FOREACH from_total_traffic GENERATE COUNT(raw) AS count, group AS from_station_id, flatten($1.from_station_name);
from_total_traffic_count = DISTINCT from_total_traffic_count;
order_from_total_traffic_count = ORDER from_total_traffic_count BY count DESC;
*/


/* lugar donde se depositan mas bicicletas
to_total_traffic = GROUP raw BY to_station_id;
to_total_traffic_count = FOREACH to_total_traffic GENERATE COUNT(raw) AS count, group AS to_station_id, flatten($1.to_station_name);
to_total_traffic_count = DISTINCT to_total_traffic_count;
to_total_traffic_count = FOREACH to_total_traffic_count GENERATE $0, $1, MIN($2);
order_to_total_traffic_count = ORDER to_total_traffic_count BY count DESC;
*/


/* promedio por dia
raw_group_per_day = GROUP raw BY day; -- (day, {...})
raw_avg_per_day = FOREACH raw_group_per_day GENERATE $0, AVG(raw.tripduration);
*/

todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;



raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)qqmonth) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, AVG($1.duration);
raw_group_per_day_month = ORDER raw_group_per_day_month BY $1 DESC;

-- Output files
STORE raw_group_per_day_month INTO '/uhadoop2020/group12/bici/time_avg_dm2';

/*
-- STORE order_from_total_traffic_count INTO '/uhadoop2020/group12/bici/from_total_traffic';
-- STORE order_to_total_traffic_count INTO '/uhadoop2020/group12/bici/to_total_traffic';
-- STORE raw_avg_per_day INTO '/uhadoop2020/group12/bici/time_avg';
*/
-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);
--Filtro de los datos para hombres y mujeres
women = FILTER raw BY gender == 'Female';
men = FILTER raw BY gender == 'Male';


--Lugares donde se retiran mas bicicletas
from_total_traffic = GROUP raw BY from_station_id;
from_total_traffic_count = FOREACH from_total_traffic GENERATE COUNT(raw) AS count, group AS from_station_id, flatten($1.from_station_name);
from_total_traffic_count = DISTINCT from_total_traffic_count;
-- Cantidad de retiros, id_station, name_station 
order_from_total_traffic_count = ORDER from_total_traffic_count BY count DESC;



--Lugares donde se depositan mas bicicletas
to_total_traffic = GROUP raw BY to_station_id;
to_total_traffic_count = FOREACH to_total_traffic GENERATE COUNT(raw) AS count, group AS to_station_id, flatten($1.to_station_name);
to_total_traffic_count = DISTINCT to_total_traffic_count;
to_total_traffic_count = FOREACH to_total_traffic_count GENERATE $0, $1, $2;
-- Cantidad de depositos, id_station, name_station
order_to_total_traffic_count = ORDER to_total_traffic_count BY count DESC;



--Tiempo promedio por dia {lunes(0),...,domingo(6)}
raw_group_per_day = GROUP raw BY day; -- (day, {...})
-- dia {lunes(0),...,domingo(6)}, cantidad de viajes, duracion promedio de viajes, maximo tiempo de viaje, minimo tiempo de viaje
raw_avg_per_day = FOREACH raw_group_per_day GENERATE $0, COUNT(raw.tripduration),AVG(raw.tripduration), MAX(raw.tripduration), MIN(raw.tripduration);



--Tiempo promedio por dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, AVG($1.duration), MAX($1.duration), MIN($1.duration);
-- dia###mes tiempo de duracion del viaje, maximo tiempo de viaje, minimo tiempo de viaje
raw_group_per_day_month_avg = ORDER raw_group_per_day_month BY $1 DESC;





-- Output files (en orden)
--Lugares donde se retiran mas bicicletas
-- Cantidad de retiros, id_station, name_station
STORE order_from_total_traffic_count INTO '/uhadoop2020/group12/bici/from_total_traffic';

--Lugares donde se depositan mas bicicletas
-- Cantidad de depositos, id_station, name_station
STORE order_to_total_traffic_count INTO '/uhadoop2020/group12/bici/to_total_traffic';

--Tiempo promedio por dia {lunes(0),...,domingo(6)}
-- dia {lunes(0),...,domingo(6)}, cantidad de viajes, duracion promedio de viajes, maximo tiempo de viaje, minimo tiempo de viaje
STORE raw_avg_per_day INTO '/uhadoop2020/group12/bici/time_avg';

--Tiempo promedio por dia###mes
-- dia###mes tiempo de duracion del viaje, maximo tiempo de viaje, minimo tiempo de viaje
STORE raw_group_per_day_month_avg INTO '/uhadoop2020/group12/bici/time_avg_dm2';


-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);
/*
women = FILTER raw BY gender == 'Female';
men = FILTER raw BY gender == 'Male';
*/

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


/* promedio por dia {lunes(0),...,domingo(6)}
raw_group_per_day = GROUP raw BY day; -- (day, {...})
raw_avg_per_day = FOREACH raw_group_per_day GENERATE $0, AVG(raw.tripduration);
*/


/*
promedio por dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, AVG($1.duration);
raw_group_per_day_month = ORDER raw_group_per_day_month BY $1 DESC;
*/

/*
-- Cantidad de viajes que inician en el dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, COUNT($1.duration);
raw_group_per_day_month = ORDER raw_group_per_day_month BY $1 DESC;
*/

/*
-- por hora
raw_group_per_hour = GROUP raw BY hour;
raw_group_per_hour =  FOREACH raw_group_per_hour GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour = ORDER raw_group_per_hour BY $1 DESC;
*/

 /*
-- por mes
raw_group_per_month = GROUP raw BY month;
raw_group_per_month =  FOREACH raw_group_per_month GENERATE $0 , COUNT($1.trip_id);
raw_group_per_month = ORDER raw_group_per_month BY $0 DESC;

 */

 /*
--hombres vs mujeres en hora
raw_group_per_hour_men = GROUP men BY hour;
raw_group_per_hour_men =  FOREACH raw_group_per_hour_men GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_men = ORDER raw_group_per_hour_men BY $1 DESC;

raw_group_per_hour_women = GROUP women BY hour;
raw_group_per_hour_women =  FOREACH raw_group_per_hour_women GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_women = ORDER raw_group_per_hour_women BY $1 DESC;


raw_group_per_hour_men_vs_women = JOIN raw_group_per_hour_men BY $0 , raw_group_per_hour_women BY $0;
raw_group_per_hour_men_vs_women = FOREACH raw_group_per_hour_men_vs_women GENERATE $0, $1 ,$3;
  */


-- por hora dia habil
dia_habil = FILTER raw BY (int)day < 5;
raw_group_per_hour_dia_habil = GROUP dia_habil BY hour;
raw_group_per_hour_dia_habil =  FOREACH raw_group_per_hour_dia_habil GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_dia_habil = ORDER raw_group_per_hour_dia_habil BY $0 DESC;

-- fin de semana
fin_de_semana = FILTER raw BY (int)day >= 5;
raw_group_per_hour_fin_de_semana = GROUP fin_de_semana BY hour;
raw_group_per_hour_fin_de_semana =  FOREACH raw_group_per_hour_fin_de_semana GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_fin_de_semana = ORDER raw_group_per_hour_fin_de_semana BY $0 DESC;

-- join por hora dia habil vs fin de semana
hora_dia_habil_vs_fin_de_semana = JOIN  raw_group_per_hour_dia_habil BY $0 , raw_group_per_hour_fin_de_semana BY $0 ;
hora_dia_habil_vs_fin_de_semana = FOREACH hora_dia_habil_vs_fin_de_semana GENERATE (int)$0 , (int)$1/5 , (int)$3/2 ;
hora_dia_habil_vs_fin_de_semana = ORDER hora_dia_habil_vs_fin_de_semana BY $0 ;



-- todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration, temperature, events, gender;
-- Output files
STORE hora_dia_habil_vs_fin_de_semana INTO '/uhadoop2020/group12/bici/hora_dia_habil_vs_fin_de_semana';

/*
STORE hora_dia_habil_vs_fin_de_semana INTO '/uhadoop2020/group12/bici/hora_dia_habil_vs_fin_de_semana';
STORE raw_group_per_hour_dia_habil INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora_dia_habil';
STORE raw_group_per_hour_men_vs_women INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora_comparacion';
STORE raw_group_per_hour INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora';
STORE raw_group_per_day_month INTO '/uhadoop2020/group12/bici/cantidad_viajes_dia_mes';
STORE raw_group_per_day_month INTO '/uhadoop2020/group12/bici/time_avg_dm2';
STORE order_from_total_traffic_count INTO '/uhadoop2020/group12/bici/from_total_traffic';
STORE order_to_total_traffic_count INTO '/uhadoop2020/group12/bici/to_total_traffic';
STORE raw_avg_per_day INTO '/uhadoop2020/group12/bici/time_avg';
*/
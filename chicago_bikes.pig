-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);
/* --Filtro de los datos para hombres y mujeres
women = FILTER raw BY gender == 'Female';
men = FILTER raw BY gender == 'Male';
*/

/* --Lugares donde se retiran mas bicicletas
from_total_traffic = GROUP raw BY from_station_id;
from_total_traffic_count = FOREACH from_total_traffic GENERATE COUNT(raw) AS count, group AS from_station_id, flatten($1.from_station_name);
from_total_traffic_count = DISTINCT from_total_traffic_count;
order_from_total_traffic_count = ORDER from_total_traffic_count BY count DESC;
*/


/* --Lugares donde se depositan mas bicicletas
to_total_traffic = GROUP raw BY to_station_id;
to_total_traffic_count = FOREACH to_total_traffic GENERATE COUNT(raw) AS count, group AS to_station_id, flatten($1.to_station_name);
to_total_traffic_count = DISTINCT to_total_traffic_count;
to_total_traffic_count = FOREACH to_total_traffic_count GENERATE $0, $1, MIN($2);
order_to_total_traffic_count = ORDER to_total_traffic_count BY count DESC;
*/


/* --Tiempo promedio por dia {lunes(0),...,domingo(6)}
raw_group_per_day = GROUP raw BY day; -- (day, {...})
raw_avg_per_day = FOREACH raw_group_per_day GENERATE $0, AVG(raw.tripduration);
*/


/* --Tiempo promedio por dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, AVG($1.duration);
raw_group_per_day_month_avg = ORDER raw_group_per_day_month BY $1 DESC;
*/

/* --Cantidad de viajes que inician en el dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, COUNT($1.duration);
raw_group_per_day_month_cnt = ORDER raw_group_per_day_month BY $1 DESC;
*/

/* --Cantidad de viajes por hora
raw_group_per_hour = GROUP raw BY hour;
raw_group_per_hour =  FOREACH raw_group_per_hour GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour = ORDER raw_group_per_hour BY $1 DESC;
*/

/* --Cantidad de viajes por mes
raw_group_per_month = GROUP raw BY month;
raw_group_per_month =  FOREACH raw_group_per_month GENERATE $0 , COUNT($1.trip_id);
raw_group_per_month = ORDER raw_group_per_month BY $0 DESC;

*/

/* --Comparación entre hombres y mujeres para cantidad de viajes por hora
raw_group_per_hour_men = GROUP men BY hour;
raw_group_per_hour_men =  FOREACH raw_group_per_hour_men GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_men = ORDER raw_group_per_hour_men BY $1 DESC;

raw_group_per_hour_women = GROUP women BY hour;
raw_group_per_hour_women =  FOREACH raw_group_per_hour_women GENERATE $0 , COUNT($1.trip_id);
raw_group_per_hour_women = ORDER raw_group_per_hour_women BY $1 DESC;


raw_group_per_hour_men_vs_women = JOIN raw_group_per_hour_men BY $0 , raw_group_per_hour_women BY $0;
raw_group_per_hour_men_vs_women = FOREACH raw_group_per_hour_men_vs_women GENERATE $0, $1 ,$3;
*/

/* --Comparación entre día hábil (lunes-viernes) y fin de semana (sábado-domingo) para cantidad de viajes en promedio
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
*/


/* 
-- query x horario

*/

-- query x horario de trabajo duracion promedio viajes
-- SELECT AVG(tripduration) as ida from data WHERE (hour == '8' OR hour == '9') AND CAST(day as int)<5;
-- SELECT AVG(tripduration) as vuelta from data WHERE (hour == '17' OR hour == '18')AND CAST(day as int)<5;

dia_habil = FILTER raw BY (int)day < 5;

viaje_ida = FILTER dia_habil BY (int)hour >=8 AND (int)hour <= 9;
viaje_vuelta = FILTER dia_habil BY  (int)hour >=17 AND (int)hour <= 18;

promedio_viaje_ida = FOREACH viaje_ida GENERATE 'viaje ida' , tripduration as duration;
promedio_viaje_vuelta = FOREACH viaje_vuelta GENERATE 'viaje vuelta' ,tripduration as duration;

promedio_viaje_ida = GROUP promedio_viaje_ida BY $0;
promedio_viaje_vuelta = GROUP promedio_viaje_vuelta BY $0;

promedio_viaje_ida = FOREACH promedio_viaje_ida GENERATE $0, AVG($1.duration);
promedio_viaje_vuelta = FOREACH promedio_viaje_vuelta GENERATE $0, AVG($1.duration);

promedio_viajes = JOIN  promedio_viaje_ida BY $0 FULL OUTER, promedio_viaje_vuelta BY $0;

/*
-- se comparara solo dia de semana (no trabajo vs trabajo)
id_day_hour = FOREACH raw GENERATE tripduration, day, (
                CASE
                    WHEN (int)hour >= 0 AND (int)hour < 6  THEN 'Madrugada'
                    WHEN (int)hour >= 6 AND (int)hour < 12  THEN 'Manana'
                    WHEN (int)hour >= 12 AND (int)hour < 19  THEN 'Tarde'
                    WHEN (int)hour >= 19  THEN 'Noche'
                    END
                    ) as horario;

id_day_hour = FOREACH id_day_hour GENERATE CONCAT((chararray)day,'###',(chararray)horario) as datet, tripduration as duration;
id_day_hour = GROUP id_day_hour BY $0;
id_day_hour_count = FOREACH id_day_hour GENERATE $0, COUNT($1.duration);
id_day_hour_avg = FOREACH id_day_hour GENERATE $0, AVG($1.duration);
id_day_hour_count = ORDER id_day_hour_count BY $1;
id_day_hour_avg = ORDER id_day_hour_avg BY $1;

*/


/* Tiempo promedio del uso de las bicicletas

*/


/* Output files (en orden)
STORE id_day_hour_count INTO '/uhadoop2020/group12/bici/day_hour_count';
STORE id_day_hour_avg INTO '/uhadoop2020/group12/bici/day_hour_avg';
STORE order_from_total_traffic_count INTO '/uhadoop2020/group12/bici/from_total_traffic';
STORE order_to_total_traffic_count INTO '/uhadoop2020/group12/bici/to_total_traffic';
STORE raw_avg_per_day INTO '/uhadoop2020/group12/bici/time_avg';
STORE raw_group_per_day_month_avg INTO '/uhadoop2020/group12/bici/time_avg_dm2';
STORE raw_group_per_day_month_cnt INTO '/uhadoop2020/group12/bici/cantidad_viajes_dia_mes';
STORE raw_group_per_hour INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora';
STORE raw_group_per_month INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_mes';
STORE raw_group_per_hour_men_vs_women INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora_comparacion';
STORE hora_dia_habil_vs_fin_de_semana INTO '/uhadoop2020/group12/bici/hora_dia_habil_vs_fin_de_semana';
-- query por horario (?)
STORE promedio_viajes INTO '/uhadoop2020/group12/bici/duracion_viaje_trabajo_ida_vs_vuelta';

*/
-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);

--Filtro de los datos para hombres y mujeres
women = FILTER raw BY gender == 'Female';
men = FILTER raw BY gender == 'Male';

--Cantidad de viajes que inician en el dia###mes
todate_data = FOREACH raw GENERATE ToDate(starttime,'yyyy-MM-dd HH:mm:ss','America/Chicago') as (date_time:datetime), tripduration as duration ;
raw_group_per_day_month = FOREACH todate_data GENERATE GetDay(date_time) as day, GetMonth(date_time) as month, duration;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE CONCAT((chararray)day,'###',(chararray)month) as datet, duration;
raw_group_per_day_month = GROUP raw_group_per_day_month BY datet;
raw_group_per_day_month = FOREACH raw_group_per_day_month GENERATE $0, COUNT($1.duration);
-- dia###mes, cantidad
raw_group_per_day_month_cnt = ORDER raw_group_per_day_month BY $1 DESC;


--Cantidad de viajes por hora
raw_group_per_hour = GROUP raw BY hour;
raw_group_per_hour =  FOREACH raw_group_per_hour GENERATE $0 , COUNT($1.trip_id), AVG($1.tripduration), MAX($1.tripduration), MIN($1.tripduration);
-- hora, promedio de tiempo
raw_group_per_hour = ORDER raw_group_per_hour BY $1 DESC;


--Cantidad de viajes por mes
raw_group_per_month = GROUP raw BY month;
raw_group_per_month =  FOREACH raw_group_per_month GENERATE $0 , COUNT($1.trip_id) , AVG($1.tripduration), MAX($1.tripduration), MIN($1.tripduration);
-- mes, promedio de tiempo
raw_group_per_month = ORDER raw_group_per_month BY $0 DESC;



--Comparación entre hombres y mujeres para cantidad de viajes por hora
raw_group_per_hour_men = GROUP men BY hour;
raw_group_per_hour_men =  FOREACH raw_group_per_hour_men GENERATE $0 , COUNT($1.trip_id), AVG($1.tripduration);
raw_group_per_hour_men = ORDER raw_group_per_hour_men BY $1 DESC;

raw_group_per_hour_women = GROUP women BY hour;
raw_group_per_hour_women =  FOREACH raw_group_per_hour_women GENERATE $0 , COUNT($1.trip_id),AVG($1.tripduration);
raw_group_per_hour_women = ORDER raw_group_per_hour_women BY $1 DESC;


raw_group_per_hour_men_vs_women = JOIN raw_group_per_hour_men BY $0 , raw_group_per_hour_women BY $0;
-- hora, count_men, avg_men, count_women, avg_women
raw_group_per_hour_men_vs_women = FOREACH raw_group_per_hour_men_vs_women GENERATE $0, $1 ,$2, $4, $5;





-- Output files (en orden)
-- Cantidad de viajes que inician en el dia###mes
-- dia###mes, cantidad
STORE raw_group_per_day_month_cnt INTO '/uhadoop2020/group12/bici/cantidad_viajes_dia_mes';

-- Cantidad de viajes por hora
-- hora, promedio de tiempo
STORE raw_group_per_hour INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora';

-- Cantidad de viajes por mes
-- mes, promedio de tiempo
STORE raw_group_per_month INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_mes';

-- Comparación entre hombres y mujeres para cantidad de viajes por hora
-- hora, count_men, avg_men, count_women, avg_women
STORE raw_group_per_hour_men_vs_women INTO '/uhadoop2020/group12/bici/cantidad_viajes_por_hora_comparacion';

-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop2020/group12/data_without_header.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);


--Comparación entre día hábil (lunes-viernes) y fin de semana (sábado-domingo) para cantidad de viajes en promedio y promedio de tiempo de viaje

-- por hora dia habil
dia_habil = FILTER raw BY (int)day < 5;
raw_group_per_hour_dia_habil = GROUP dia_habil BY hour;
raw_group_per_hour_dia_habil =  FOREACH raw_group_per_hour_dia_habil GENERATE $0 , COUNT($1.trip_id), AVG($1.tripduration);
raw_group_per_hour_dia_habil = ORDER raw_group_per_hour_dia_habil BY $0 DESC;

-- fin de semana
fin_de_semana = FILTER raw BY (int)day >= 5;
raw_group_per_hour_fin_de_semana = GROUP fin_de_semana BY hour;
raw_group_per_hour_fin_de_semana =  FOREACH raw_group_per_hour_fin_de_semana GENERATE $0 , COUNT($1.trip_id),AVG($1.tripduration);
raw_group_per_hour_fin_de_semana = ORDER raw_group_per_hour_fin_de_semana BY $0 DESC;

-- join por hora dia habil vs fin de semana
hora_dia_habil_vs_fin_de_semana = JOIN  raw_group_per_hour_dia_habil BY $0 , raw_group_per_hour_fin_de_semana BY $0 ;
--hora, count_diahabil, avg_diahabil, count_finde, avg_finde
hora_dia_habil_vs_fin_de_semana = FOREACH hora_dia_habil_vs_fin_de_semana GENERATE (int)$0 , (int)$1/5 , $2 , (int) $4/2, $5;
--hora, count_diahabil, avg_diahabil, count_finde, avg_finde
hora_dia_habil_vs_fin_de_semana = ORDER hora_dia_habil_vs_fin_de_semana BY $0 ;


-- query x horario de trabajo duracion promedio viajes ida vs vuelta de trabajo

dia_habil = FILTER raw BY (int)day < 5;

viaje_ida = FILTER dia_habil BY (int)hour >=8 AND (int)hour <= 9;
viaje_vuelta = FILTER dia_habil BY  (int)hour >=17 AND (int)hour <= 18;

promedio_viaje_ida = FOREACH viaje_ida GENERATE 'viaje ida' , tripduration as duration;
promedio_viaje_vuelta = FOREACH viaje_vuelta GENERATE 'viaje vuelta' ,tripduration as duration;

promedio_viaje_ida = GROUP promedio_viaje_ida BY $0;
promedio_viaje_vuelta = GROUP promedio_viaje_vuelta BY $0;

promedio_viaje_ida = FOREACH promedio_viaje_ida GENERATE $0, AVG($1.duration);
promedio_viaje_vuelta = FOREACH promedio_viaje_vuelta GENERATE $0, AVG($1.duration);

-- tipo de viaje y promedio
promedio_viajes = JOIN  promedio_viaje_ida BY $0 FULL OUTER, promedio_viaje_vuelta BY $0;




-- Se compara por horario clasificado como {Madrugada,Manana,Tarde y Noche} y dia {Lunes (0), ... , Domingo(6)}
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
-- dia#hora , cantidad de viajes
id_day_hour_count = ORDER id_day_hour_count BY $1 DESC;
-- dia#hora , promedio de tiempo de duracion del viaje
id_day_hour_avg = ORDER id_day_hour_avg BY $1 DESC;




-- Por temperatura

temperature_file = FOREACH raw GENERATE (int)((double)temperature-32)*5/9 as temperature, tripduration, trip_id;
groupby_temperature = GROUP temperature_file BY temperature;
groupby_temperature_count_avg = FOREACH groupby_temperature GENERATE $0, COUNT($1.trip_id), AVG($1.tripduration), MAX($1.tripduration), MIN($1.tripduration);



temperature_stdev =  JOIN temperature_file BY $0, groupby_temperature_count_avg BY $0;

-- t= {temperature,tripduration,trip_id, temperature,count,avg,max,min} 
temperature_stdev = FOREACH temperature_stdev GENERATE $0, ((float)$1 - (float) $5)*((float)$1 - (float) $5) as dtime;
temperature_stdev = GROUP temperature_stdev BY $0;
temperature_stdev = FOREACH temperature_stdev GENERATE $0, SUM($1.dtime);
temperature_count_avg_stdev = JOIN temperature_stdev BY $0,  groupby_temperature_count_avg BY $0;
temperature_count_avg_stdev = FOREACH temperature_count_avg_stdev GENERATE (int)$0, $3, $4, $5, $6, SQRT((double) $1/(double)$3);
temperature_count_avg_stdev = ORDER temperature_count_avg_stdev BY $0 DESC;


-- Por clima

clima = GROUP raw BY events;
clima = FOREACH clima GENERATE $0, COUNT($1.trip_id), AVG($1.tripduration), MAX($1.tripduration),MIN($1.tripduration);
-- clima, cantidad, promedio de tiempo, maximo, minimo
clima = ORDER clima BY $1 DESC;





-- Output files (en orden)
--Comparación entre día hábil (lunes-viernes) y fin de semana (sábado-domingo) para cantidad de viajes en promedio y promedio de tiempo de viaje
--hora, count_diahabil, avg_diahabil, count_finde, avg_finde
STORE hora_dia_habil_vs_fin_de_semana INTO '/uhadoop2020/group12/bici/hora_dia_habil_vs_fin_de_semana';

-- query x horario de trabajo duracion promedio viajes ida vs vuelta de trabajo
-- tipo de viaje y promedio
STORE promedio_viajes INTO '/uhadoop2020/group12/bici/duracion_viaje_trabajo_ida_vs_vuelta';

-- Se compara por horario clasificado como {Madrugada,Manana,Tarde y Noche} y dia {Lunes (0), ... , Domingo(6)}
-- dia#hora , cantidad de viajes
STORE id_day_hour_count INTO '/uhadoop2020/group12/bici/day_hour_count';
-- dia#hora , promedio de tiempo de duracion del viaje
STORE id_day_hour_avg INTO '/uhadoop2020/group12/bici/day_hour_avg';

-- Agrupado por temperatura 
-- temperature,count,avg,max,min,stdev
STORE temperature_count_avg_stdev INTO '/uhadoop2020/group12/bici/temperature';
-- clima
-- clima, cantidad, promedio de tiempo, maximo, minimo
STORE clima INTO '/uhadoop2020/group12/bici/clima';



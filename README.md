# Patos-and-Chicago-Bikes-Repository

- data.csv

trip_id,year,month,week,day,hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end




-> enviar el archivo al servidor. (DONE)


-> hacer consultas (para las siguientes ideas):

Querys hechas:

- Mayor lugar de retiro de bicicletas  
- Mayor lugar de depósito de bicicletas 
El formato de entrega de ambos es "COUNT \t id_station \t name_station". Es importante notar que un id puede estar mas 
de una vez en la lista, esto se debe a que la estación puede tener más de un nombre (en caso de esto ser un problema, se
puede solucionar simplemente eligiendo el primer nombre arbitrariamente).


to do:
- Hacer query x mes.
- Hacer query x dia. -> puede ser [Lunes -> Domingo] o [1 -> 365 dias]
- Hacer query x hora
- Hacer query x horario 
- Hacer query x horario de trabajo -> asumir que se va a trabajar entre 8-9 y se regresa entre 5-6
- Tiempo promedio del uso de las bicicletas

IDEAS PARA MOSTRAR
- Horarios: las partes del día se dividen en Mañana: de 6 a 12, Tarde: de 12 a 19
 ,Noche: de 19 a 24 y Madrugada de 24 a 6. Podríamos ver qué género gana en cada horario.
- Notar si hay algún comportamiento con los meses del año y
 ver el pico del uso de las bicicletas.
- Tiempo promedio del uso de las bicicletas (?) 
según horario maybe, para distinguir cuándo van a trabajar o traslado y cuando lo hacen por hobbie.
- Tiempo promedio de uso vs temperatura o vs evento (cloudy, clear)
- Tiempo promedio de uso (separado por género) vs temperatura o vs evento (cloudy, clear)

 

-> Ver informe (se definió hacerlo en git)
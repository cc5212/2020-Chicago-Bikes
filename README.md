# 2020-Chicago-Bikes

Analisis de viajes realizados en bicicletas pertenecientes a la empresa Divvy (empresa de bike sharing) en la ciudad de Chicago (EEUU) entre los años 2013 y 2017, utilizando Pig Latin [Alfredo Castillo, Maximo Retamal, Lucas Torrealba. Grupo 3]

# Resumen
El propósito de este estudio es analizar los viajes realizados en Chicago a través de la compañía Divvy durante los años 2013-2017.

Se busca encontrar relaciones a los lugares donde parten y terminan los viajes, la cantidad y duración de viajes realizados con el momento en que se realiza, i.e. la hora del dia, el dia de la semana y la semana del año, con el propósito de analizar la diferencia entre los viajes realizados en horario de oficina con horarios no asociados al trabajar de las personas y cómo cambian los viajes dependiendo de la estación del año.

# Datos
El dataset con el cual se trabaja corresponde a viajes realizados en bicicletas de la compañía Divvy, estos datos son abiertos al público y fueron encontrados en Kaggle. Los datos de cada viaje son:

- Trip id (propio para cada viaje).
- Día del viaje (0 denota Lunes y 6 Domingo).
- Inicio del viaje.
- Fin del viaje.
- Nombre de estación inicio del viaje.
- ID Estación de inicio del viaje. 
- Latitud y Longitud de la estación de inicio del viaje.
- Capacidad de bicicletas a guardar en la estación del inicio del viaje.
- Nombre de estación fin del viaje.
- ID Estación de fin del viaje.
- Latitud y Longitud de la estación de fin del viaje.
- Capacidad de bicicletas a guardar en la estación del fin del viaje.
- Tipo de usuario (falta especificar).
- Género.
- Duración del viaje.
- Temperatura.
- Eventos (tormenta, despejado, etc).
- timestamp


Estos datos fueron recolectados durante aproximadamente 4 años, por lo que el grupo considera que son representativos para observar el uso de bicicletas pertenecientes a la compañía Divvy en la ciudad de Chicago, Estados Unidos.

La información se encuentra en un archivo .csv (valores separados por coma), cabe destacar que se trabaja con el archivo data.csv, que se diferencia del original ya que hubo una limpieza del dataset y se han mantenido los viajes dentro de a lo más 1 hora. El tamaño del archivo es aproximadamente 2 GB y contiene aproximadamente 9.5 millones de datos.

# Metodología

En el proyecto se utilizó la tecnología proveniente de la plataforma de manejo masivo de datos Apache Pig, creando programas MapReduce para correrlos en el servidor Hadoop del curso. Se decidió utilizar este lenguaje debido a la simplicidad del mismo y también por los tipos de queries que se tenían pensadas realizar, ya que los operadores utilizados en Pig Latin otorgaban justo lo que se necesitaba para nuestra dataset y a priori no era necesario realizar consultas muy complejas para obtener resultados interesantes.
De forma general, se tuvo solo una complicación a la hora de cargar los datos, puesto que se eligió un conjunto muy extenso, y que además se leían los headers de los datos, produciéndose errores a la hora de ejecutar las consultas. Otra complicación puntual fue que se presentaban horas geográficas que estaban en Chicago (ciudad donde se realizaron los registros) y no en Chile, por lo que se tuvo que especificar la zona horaria en la que estaban los datos para poder trabajar con esos campos. Además, hubo confusiones a la hora de trabajar con las consultas utilizando los índices de las columnas ($0, $1, etc), usando alguna columna que no se deseaba y recién notándose luego del error al ejecutar el .jar en Hadoop.

Respecto al código fuente, se trabajó principalmente con los operadores FILTER, GROUP, FOREACH GENERATE, ORDER, COUNT y JOIN.
En primer lugar, se filtraron y agruparon datos para obtener relaciones entre personas y factores como clima, horarios, ubicaciones, tiempos promedios de viaje, entre otros; o también para contar cuántas veces, por ejemplo, utilizaron bicicletas las personas por mes u hora.
En segundo lugar, se utilizó también el operador JOIN, para unir dos conjuntos de datos filtrados para realizar una comparación respecto a cantidad de usos, tiempo utilizado, entre otros factores. Por ejemplo, comparar la cantidad de viajes por hora entre hombres y mujeres, o también comparar la cantidad de viajes en promedio entre día hábil (lunes a viernes) y fin de semana (sábado y domingo).

# Resultados

Detail the results of the project. Different projects will have different types of results; e.g., run-times or result sizes, evaluation of the methods you're comparing, the interface of the system you've built, and/or some of the results of the data analysis you conducted.

# Conclusión
Entre los beneficios del lenguaje utilizado, corresponde a una sintaxis que facilitó la creación de consultas debido a sus operadores con nombres explícitos que justamente realizaban una función específica que se necesitaba, y no se tenía que recurrir a herramientas o funciones más complejas. 
Por otro lado, las dificultades se presentaban mayormente a la hora de tener errores, ya que si se corregía, por ejemplo, mediante prueba y error, se debía esperar a que el job finalizara en el cluster de uhadoop para notar si el error se había solucionado o persistía, tomando esto bastante tiempo en algunas ocasiones. Además, Apache Pig no posee por defecto operaciones estadísticas más avanzadas (desviación estándar, mediana, etc), por lo que no permitía mostrar tablas con análisis de datos dentro de los resultados y, por ende, se debió realizar esto luego de mostrar los resultados de las consultas con el desconocimiento de la representatividad de estos (salvo en los datos agrupados por temperatura).
Finalmente, se pudo haber realizado un mejor trabajo a la hora de utilizar nombres de variables, renombrando variables, columnas y conjuntos de datos con nombres más entendibles por cualquier usuario que quisiera leer el código fuente. Además, se requiere optimizar lo mejor posible las consultas debido al tiempo de espera del clúster.

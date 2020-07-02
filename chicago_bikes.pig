-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/data/2020/uhadoop/group12/data.csv' USING PigStorage(',') AS (trip_id,year,month,week,day,
hour,usertype,gender,starttime,stoptime,tripduration,temperature,events,from_station_id,from_station_name,
latitude_start,longitude_start,dpcapacity_start,to_station_id,to_station_name,latitude_end,longitude_end,dpcapacity_end);

-- Filter raw to separate between MALE and FEMALE genders
men = FILTER raw BY gender == 'Male';
women = FILTER raw BY gender == 'Female';

-- Generate new relation with full movie name
full_movies = FOREACH movies GENERATE CONCAT(movie,'##',year,'##',epnumber);

-- Generate new relation with full movie name and its actor
full_movies_actors = FOREACH actors GENERATE CONCAT(movie,'##',myear,'##',mnumber), name;
full_movies_actresses = FOREACH actresses GENERATE CONCAT(movie,'##',myear,'##',mnumber), name;

-- Left outer join not to ignore the null fields
movie_good_actors = JOIN full_movies_actors BY $0 LEFT OUTER, full_movies by $0; -- m#m#m act m#m#m
movie_good_actresses = JOIN full_movies_actresses BY $0 LEFT OUTER, full_movies by $0;

-- Generate an alias of the previous tables. If the row's third field is null, put 0. Otherwise, put 1.
movie_good_actors_alias = FOREACH movie_good_actors GENERATE $1, (($2 IS NULL) ?  0 : 1) as exist;
movie_good_actresses_alias = FOREACH movie_good_actresses GENERATE $1, (($2 IS NULL) ?  0 : 1) as exist;

-- Group previous tables by actor/actress name
actor_grouped = GROUP movie_good_actors_alias BY $0;
actress_grouped = GROUP movie_good_actresses_alias BY $0;

-- Sum the second columns if the row's actor/actress name is the same.
actor_sum = FOREACH actor_grouped GENERATE $0, SUM($1.exist) as count;
actresses_sum = FOREACH actress_grouped GENERATE $0, SUM($1.exist) as count;

-- The results are sorted descending by count
ordered_actor_count = ORDER actor_sum BY count DESC;
ordered_actress_count = ORDER actresses_sum BY count DESC;

-- Output files

STORE ordered_actor_count INTO '/uhadoop2020/group12/imdb-costars/male';
STORE ordered_actress_count INTO '/uhadoop2020/group12/imdb-costars/female';



------------------------------- Top 10 actors -------------------------------
-- Harris, Sam (II)        23
-- Miller, Harold (I)      18
-- Stevens, Bert (I)       18
-- Ratzenberger, John (I)  17
-- O'Brien, William H.     16
-- Tovey, Arthur   16
-- Baker, Frank (I)        16
-- Jackson, Samuel L.      15
-- De Niro, Robert 15
-- Sayre, Jeffrey  15
------------------------------- Top 10 actresses -------------------------------
-- Flowers, Bess   28
-- Lynn, Sherry (I)        15
-- McGowan, Mickie 12
-- Blanchett, Cate 10
-- Ridgeway, Suzanne       9
-- Derryberry, Debi        9
-- Marsh, Mae      8
-- Newman, Laraine 8
-- Astor, Gertrude 8
-- Doran, Ann      7
------------------------------- Random results we like -------------------------------
-- Pitt, Brad      12
-- Hanks, Tom      9
-- D'Imperio, Philippe  2
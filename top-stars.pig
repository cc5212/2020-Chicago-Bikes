-- This script finds the actors/actresses with the highest number of good movies

raw = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' USING PigStorage('\t') AS (dist, votes, score, movie, year, epnumber, type, epname);
raw_actors = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' USING PigStorage('\t') AS (name, movie, myear, mnumber, mtype, epname, starring, role, gender);

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
-- Now to implement the script

-- We want to compute the top actors / top actresses (separately).
-- Actors should be one output file, actresses in the other.
-- Gender is now given as 'MALE'/'FEMALE' in the gender column of raw_roles

-- To do so, we want to count how many good movies each starred in.
-- We count a movie as good if:
--   it has at least (>=) 10,001 votes (votes in raw_rating) 
--   it has a score >= 7.8 (score in raw_rating)


-- The best actors/actresses are those with the most good movies.

-- An actor/actress plays one role in each movie 
--   (more accurately, the roles are concatenated on one line like "role A/role B")

-- If an actor/actress does not star in a good movie
--  a count of zero should be returned (i.e., the actor/actress
--   should still appear in the output).


-- The results should be sorted descending by count.

-- We only want to count entries of type THEATRICAL_MOVIE (not tv series, etc.).
-- Again, note that only CONCAT(title,'##',year,'##',num) acts as a key for movies.

-- Test on smaller file first (as given above),
--  then test on larger file to get the results.

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------

-- Filter raw to make sure type equals 'THEATRICAL_MOVIE' and good movie
movies = FILTER raw BY type == 'THEATRICAL_MOVIE' AND votes > 10000 AND score >= 7.8;

-- Filter raw to separate between MALE and FEMALE genders
actors = FILTER raw_actors BY gender == 'MALE';
actresses = FILTER raw_actors BY gender == 'FEMALE';

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
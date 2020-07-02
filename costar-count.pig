-- This script finds the actors/actresses that have acted together the most times

-- Input: (actor, title, year, num, type, episode, billing, char)
raw = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test.tsv' USING PigStorage('\t') AS (actor, title, year, num, type, episode, billing, char);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output

-- Line 1: Filter raw to make sure type equals 'THEATRICAL_MOVIE' 
movies = FILTER raw BY type == 'THEATRICAL_MOVIE';

-- Line 2: Generate new relation with full movie name (concatenating title+##+year+##+num) and actor
full_movies = FOREACH movies GENERATE CONCAT(title,'##',year,'##',num), actor;

-- Line 3 + 4: Generate the co-star pairs 
full_movies_alias = FOREACH full_movies GENERATE $0, $1;
movie_actor_pairs = JOIN full_movies BY $0, full_movies_alias by $0;

-- Line 5: filter to ensure that the first co-star is lower alphabetically than the second
movie_actor_pairs_unique = FILTER movie_actor_pairs BY full_movies::actor < full_movies_alias::actor;

-- Line 6: concatenate the co-stars into one column 
simply_actor_pairs = FOREACH movie_actor_pairs_unique GENERATE CONCAT(full_movies::actor,'##',full_movies_alias::actor) AS actor_pair;

-- Line 7: group the relation by co-stars
actor_pairs_grouped = GROUP simply_actor_pairs BY actor_pair;

-- Line 8: count each group of co-stars
actor_pair_count = FOREACH actor_pairs_grouped GENERATE COUNT($1) AS count, group AS actor_pair; 

-- Line 9: order the count in descending order
ordered_actor_pair_count = ORDER actor_pair_count BY count DESC; 

-- output the final count
-- TODO: REPLACE ahogan WITH YOUR FOLDER
STORE ordered_actor_pair_count INTO '/uhadoop2020/ahogan/imdb-costars/';


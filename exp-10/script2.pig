movies = LOAD 'movies.csv' USING
   PigStorage(',') as (id:int,name:chararray,released_year:int);


movies_with_4 = FILTER movies by released_year >= 2018 AND released_year <= 2020;

movies_start_with_a = FILTER movies by name matches 'A.*';

group_by_years = GROUP movies BY released_year;

val = FOREACH group_by_years GENERATE group, COUNT(movies);

sorted_by_date = ORDER movies BY released_year DESC;

STORE movies_with_4 into 'movie_with_4.txt';
STORE movies_start_with_a into 'movies_start_with_a.txt';
STORE val into 'val.txt';
STORE sorted_by_date into 'sorted.txt';

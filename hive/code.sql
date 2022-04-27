create database maahi
;
use maahi;
CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
salary String, destination String)
COMMENT
ROW FORMAT DELIMITED
FIELDS TERMINATED BY
LINES TERMINATED BY
STORED AS TEXTFILE;
CREATE SCHEMA movie;
use movie
;
CREATE TABLE IF NOT EXISTS movie (id int, name String);
CREATE TABLE IF NOT EXISTS movie (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
CREATE TABLE  movie (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
CREATE TABLE  movieT (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
select * from movieT;
CREATE TABLE  ratings (id int, rating float, movieId int) COMMENT 'Movie Ratings' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
show tables;
alter table ratings add movieName String
;
alter table ratings add movieName (String)
;
alter table ratings add movieName;
alter table ratings add column ( movieName string);
alter table ratings add columns ( movieName string);
select * from ratings;
describe ratings;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
select * from ratings order by rating;
LOAD DATA LOCAL INPATH '/home/maahi/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings order by rating;
select * from ratings order by movieName;
alter table ratings add partition (rating=5) location /5/part5;
alter table ratings add partition (rating='5') location /5/part5;
alter table ratings add partition (rating='5') location '/5/part5';
alter table ratings add partition (rating=5) location '/5/part5';
select * from ratings cluster by rating;
select * from ratings sort by rating DESC;
select * from movieT;
select * from ratings;
select * from ratings group by rating;
select rating count(*) from ratings group by rating;
select rating, count(*) from ratings group by rating;


CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
salary String, destination String)
COMMENT
ROW FORMAT DELIMITED
FIELDS TERMINATED BY
LINES TERMINATED BY
STORED AS TEXTFILE;
CREATE SCHEMA movie;
use movie
;
CREATE TABLE IF NOT EXISTS movie (id int, name String);
CREATE TABLE IF NOT EXISTS movie (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
CREATE TABLE  movie (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
CREATE TABLE  movieT (id int, name String) COMMENT 'Movie Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
select * from movieT;
CREATE TABLE  ratings (id int, rating float, movieId int) COMMENT 'Movie Ratings' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
show tables;
alter table ratings add movieName String
;
alter table ratings add movieName (String)
;
alter table ratings add movieName;
alter table ratings add column ( movieName string);
alter table ratings add columns ( movieName string);
select * from ratings;
describe ratings;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/movie-data.txt' OVERWRITE INTO TABLE movieT;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings;
select * from ratings order by rating;
LOAD DATA LOCAL INPATH '/home/keshav/BDA_LAB/ratings-data.txt' OVERWRITE INTO TABLE ratings;
select * from ratings order by rating;
select * from ratings order by movieName;
alter table ratings add partition (rating=5) location /5/part5;
alter table ratings add partition (rating='5') location /5/part5;
alter table ratings add partition (rating='5') location '/5/part5';
alter table ratings add partition (rating=5) location '/5/part5';
select * from ratings cluster by rating;
select * from ratings sort by rating DESC;
select * from movieT;
select * from ratings;
select * from ratings group by rating;
select rating count(*) from ratings group by rating;
select rating, count(*) from ratings group by rating;

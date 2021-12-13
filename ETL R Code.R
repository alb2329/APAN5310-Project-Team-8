options(scipen = 100)

# Create Table Schema in pgAdmin

# Load Packages
library(odbc)
require('RPostgreSQL')

# Load the PostgreSQL driver:
drv <- dbDriver('PostgreSQL')

# Connect to the Database
con <- dbConnect(drv, dbname = 'gp',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

# Pass the SQL statements that creates tables
stmt = "
DROP TABLE IF EXISTS movie CASCADE;
create table movie
		(movie_id varchar(8),
		 type varchar(10),
		 title varchar(200) NOT NULL,
		 release_year SMALLINT,
		 rating varchar(10),
		 duration varchar(15),
		 budget BIGINT,
		 revenue BIGINT,
		 vote_average NUMERIC(3,1),
		 primary key (movie_id)
);
DROP TABLE IF EXISTS tv CASCADE;
create table tv
		(tv_id varchar(8),
		 type varchar(10),
		 title varchar(150)NOT NULL,
		 release_year SMALLINT,
		 rating varchar(10),
		 average_episode_duration INT,
		 episodes SMALLINT,
		 seasons SMALLINT,
		 vote_average NUMERIC(3,1),
		 primary key (tv_id)
		);
DROP TABLE IF EXISTS director CASCADE;
create table director
		(director_id varchar(8),
		 director varchar(200),
		 primary key (director_id)
		);
DROP TABLE IF EXISTS movie_director CASCADE;	
create table movie_director
		(movie_id varchar(8),
		 director_id varchar(8),
		 primary key (movie_id, director_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (director_id) references director (director_id) on delete cascade
		 );
DROP TABLE IF EXISTS tv_director CASCADE;
create table tv_director
		(tv_id varchar(8),
		 director_id varchar(8),
		 primary key (tv_id, director_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (director_id) references director (director_id) on delete cascade
		);
DROP TABLE IF EXISTS actor CASCADE;
create table actor
		(actor_id varchar(8),
		 actor varchar(250),
		 primary key (actor_id)
		);
DROP TABLE IF EXISTS movie_cast CASCADE;
create table movie_cast
		(movie_id varchar(8),
		 actor_id varchar(8),
		 primary key (movie_id, actor_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (actor_id) references actor (actor_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_cast CASCADE;
create table tv_cast
		(tv_id varchar(8),
		 actor_id varchar(8),
		 primary key (tv_id, actor_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (actor_id) references actor (actor_id) on delete cascade
		);
DROP TABLE IF EXISTS country CASCADE;
create table country
		(country_id varchar(8),
		 country varchar(200),
		 primary key (country_id)
		);
DROP TABLE IF EXISTS movie_country CASCADE;
create table movie_country
		(movie_id varchar(8),
		 country_id varchar(8),
		 primary key (movie_id,country_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (country_id) references country (country_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_country CASCADE;
 create table tv_country
		(tv_id varchar(8),
		 country_id varchar(8),
		 primary key (tv_id,country_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (country_id) references country (country_id) on delete cascade
		);
DROP TABLE IF EXISTS genre CASCADE;
create table genre
		(genre_id varchar(8),
		 genre varchar(200),
		 primary key (genre_id)
		);
DROP TABLE IF EXISTS movie_listed_in CASCADE;
create table movie_listed_in
		(movie_id varchar(8),
		 genre_id varchar(8),
		 primary key (movie_id,genre_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (genre_id) references genre (genre_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_listed_in CASCADE;		
create table tv_listed_in
		(tv_id varchar(8),
		 genre_id varchar(8),
		 primary key (tv_id,genre_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (genre_id) references genre (genre_id) on delete cascade
		);		
DROP TABLE IF EXISTS platform CASCADE;
create table platform
		(platform_id varchar(8),
		 platform varchar(15),
		 primary key (platform_id)
		);		
DROP TABLE IF EXISTS movie_platform CASCADE;
create table movie_platform
		(movie_id varchar(8) NOT NULL,
		 platform_id varchar(15) NOT NULL,
		 date_added date,
		 description varchar(300),
		 primary key (movie_id, platform_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (platform_id) references platform (platform_id) on delete cascade
		);		
DROP TABLE IF EXISTS tv_platform CASCADE;
create table tv_platform
		(tv_id varchar(8) NOT NULL,
		 platform_id varchar(15) NOT NULL,
		 date_added date,
		 description varchar(300),
		 primary key (tv_id, platform_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (platform_id) references platform (platform_id) on delete cascade
		);		
"

# Execute "Create table" statements on PostgreSQL
rs <- dbSendQuery(con, stmt)

# Close the Connection
dbDisconnect(con)
closeAllConnections()

# Load Cleaning Libraries
library(data.table)
library(lubridate)
library(tidyverse)

# Read in Data
dat <- read.csv("C:/Users/Owner/Downloads/output.csv")

# Drop ID Column
dat = dat[,-1]

# Replace Blanks with NA
dat$director[dat$director==""] <- NA
dat$cast[dat$cast==""] <- NA
dat$country[dat$country==""] <- NA
dat$rating[dat$rating==""] <- NA
dat$date_added[dat$date_added==""] <- NA
dat$listed_in[dat$listed_in==""] <- NA
dat$revenue[dat$revenue==""] <- NA
dat$budget[dat$budget==""] <- NA
dat$duration[dat$duration==""] <- NA
dat$vote_average[dat$vote_average==""] <- NA
dat$episode_run_time[dat$episode_run_time==""] <- NA
dat$number_of_episodes[dat$number_of_episodes==""] <- NA

# Create platform_id 
dat$platform_id[dat$platform=="Netflix"] <- 1
dat$platform_id[dat$platform=="Hulu"] <- 2
dat$platform_id[dat$platform == "Disney"] <- 3
dat$platform_id[dat$platform == "Amazon"] <- 4

# Clean Special Characters
dat$title <- iconv(dat$title,to="ASCII//TRANSLIT")
dat$title[dat$title == "A FRIENDSHIP"] <- "A Friendship"
dat$title[dat$title == "A?on Flux"] <- "Aeon Flux"

# Clean dates
mdy <- mdy(dat$date_added) 
dmy <- dmy(dat$date_added) 
mdy[is.na(mdy)] <- dmy[is.na(mdy)] # some dates are ambiguous, here we give 
dat$date_added <- mdy  
dat$date_added <- as.character(dat$date_added)
dat$release_year <- as.character(dat$release_year)

# Clean episode_run_time
dat$episode_run_time <- gsub("[[:punct:]]", "", dat$episode_run_time)
dat$episode_run_time <- as.numeric(dat$episode_run_time)
episodes <- dat %>% tidyr::separate(episode_run_time, sep=" ", into=c("ert1","ert2","ert3"), remove = TRUE)
episodes$ert1 <- as.numeric(episodes$ert1)
episodes$ert2 <- as.numeric(episodes$ert2)
episodes$ert3 <- as.numeric(episodes$ert3)
dat$episode_run_time <- rowMeans(episodes[,c("ert1","ert2","ert3")],na.rm=TRUE)
dat$episode_run_time[is.nan(dat$episode_run_time)]<-NA

# Remove Duplicate Rows
dat <- unique(dat)
duplicate_titles <- dat[duplicated(dat$title),]

# Remove 67 movies and shows with Duplicated Data
platform_repeated <- dat %>% select(title, type, platform_id)
dupes <- platform_repeated[duplicated(platform_repeated),]
issues <- unique(dupes$title)
dat <- subset(dat, !(title %in% issues))

# Clean type variable
dat$type[grepl("Season", dat$duration)] <- "TV Show"

# Movies and Shows with Same Title
tv_shows <- dat %>% filter(type== "TV Show")
tv_titles <- tv_shows$title
movies <- dat %>% filter(type== "Movie")
movie_titles <- movies$title
same_titles <- subset(movies, title %in% tv_titles)
same_titles <- same_titles$title
dat$title <- ifelse(dat$type == "Movie"&dat$title %in% same_titles, paste(dat$title, " - Movie"), paste(dat$title))

# Clean Ratings
dat$rating[dat$rating == "13+"] <- "PG-13"
dat$rating[dat$rating == "All"] <- "G"
dat$rating[dat$rating == "ALL"] <- "G"
dat$rating[dat$rating == "NOT RATED"] <- "NR"
dat$rating[dat$rating == "NOT_RATE"] <- "NR"
dat$rating[dat$rating == "ALL_AGES"] <- "G"
dat$rating[dat$rating == "AGES_16_"] <- "R"
dat$rating[dat$rating == "16+"] <- "TV-14"
dat$rating[dat$rating == "AGES_18_"] <- "R"
dat$rating[dat$rating == "18+"] <- "R"
dat$rating[dat$rating == "UNRATED"] <- "NR"
dat$rating[dat$rating == "UR"] <- "NR"
dat$rating[dat$rating == "TV-NR"] <- "NR"
dat$rating[dat$rating == "16"] <- "TV-14"
dat$rating[dat$rating == "TV-Y7-FV"] <- "TV-Y7"
dat$rating[dat$rating == "7+"]<- "TV-Y7"

# Clean Budget and Revenue
dat$budget[dat$budget == "0"]<- NA
dat$revenue[dat$revenue == "0"]<- NA
dat$budget[dat$budget < 10]<- NA
dat$revenue[dat$revenue < 10]<- NA
format(dat$budget, scientific = F)
format(dat$revenue, scientific = F)

## CREATE MOVIE TABLE
movie <- dat %>% filter(type == "Movie") %>%
  select(title, type, release_year, rating, duration, budget, revenue, vote_average) %>%
  unique()
movie <- movie[!duplicated(movie$title),]
movie$movie_id <- 1:nrow(movie)
movie$movie_id <- as.character(movie$movie_id)
movie$budget <- as.character(movie$budget)
movie$revenue <- as.character(movie$revenue)
movie$vote_average <- as.character(movie$vote_average)
col_names <- c("movie_id","type","title","release_year","rating","duration","budget", "revenue","vote_average")
movie <- movie[,col_names]

## CREATE TV TABLE
tv <- dat %>% filter(type == "TV Show") %>%
  select(type, title, release_year, rating, episode_run_time, number_of_episodes, duration, vote_average) %>%
  unique()
tv <- tv[!duplicated(tv$title),]
tv$tv_id <- 1:nrow(tv)
tv$tv_id <- as.character(tv$tv_id)
tv$episode_run_time <- as.character(tv$episode_run_time)
tv$number_of_episodes <- as.character(tv$number_of_episodes)
tv$vote_average <- as.character(tv$vote_average)
tv$duration <- gsub("Seasons", "", tv$duration)
tv$duration <- gsub("Season", "", tv$duration)
tv$duration <- str_trim(tv$duration, side = "both")
col_names <- c("tv_id","type","title","release_year","rating","episode_run_time",
               "number_of_episodes","duration","vote_average")
tv <- tv[,col_names]
names(tv)[names(tv)=='episode_run_time']<-'average_episode_duration'
names(tv)[names(tv)=='number_of_episodes']<-'episodes'
names(tv)[names(tv)=='duration']<-'seasons'

## CREATE PLATFORM TABLE
platform = c("Netflix", "Hulu", "Disney", "Amazon")
platform_id = c(1,2,3,4)
platform_id = as.character(platform_id)
platform <- data.frame(platform_id, platform)

## CREATE MOVIE PLATFORM TABLE
movie_platform <- merge(movie, dat, by = "title") %>% select(movie_id, platform_id, date_added, description)
movie_platform$platform_id <- as.character(movie_platform$platform_id)

## CREATE TV PLATFORM TABLE
tv_platform <- merge(tv, dat, by = "title") %>% select(tv_id, platform_id, date_added, description)
tv_platform$platform_id <- as.character(tv_platform$platform_id)

# Convert movie table to 1NF, separate combined column values into multiple rows
dat_movie <- merge(dat, movie, by="title", all = TRUE) %>% select(title, movie_id, platform_id, cast, director, country, listed_in)
dat2 <- merge(dat_movie, tv, by="title", all=TRUE) %>% select(title, tv_id, movie_id, platform_id, cast, director, country, listed_in)

dat2 <- dat2 %>%
  separate_rows(director, sep = ",") %>%
  separate_rows(cast, sep=",") %>%
  separate_rows(country, sep = ",") %>%
  separate_rows(listed_in, sep=",")

# Trim leading/trailing spaces
dat2$director <- str_trim(dat2$director, side = "both")
dat2$cast <- str_trim(dat2$cast, side = "both")
dat2$country <- str_trim(dat2$country, side = "both")
dat2$listed_in <- str_trim(dat2$listed_in, side = "both")

dat2$director[dat2$director==""] <- NA
dat2$cast[dat2$cast==""] <- NA
dat2$country[dat2$country==""] <- NA
dat2$listed_in[dat2$listed_in==""] <- NA

names(dat2)[names(dat2)=='cast']<-'actor'
names(dat2)[names(dat2)=='listed_in']<-'genre'

## CREATE DIRECTOR TABLE
director <- dat2 %>% select(director) %>% unique() %>% na.omit
director$director_id <- 1:nrow(director)
director$director_id <- as.character(director$director_id)

## CREATE MOVIE DIRECTOR TABLE
movie_director <- merge(dat2, director, by = "director") %>% select(movie_id, director_id) %>% unique() %>% na.omit

## CREATE TV DIRECTOR TABLE
tv_director <- merge(dat2, director, by = "director") %>% select(tv_id, director_id) %>% unique() %>% na.omit

## CREATE ACTOR TABLE
actor <- dat2 %>% select(actor) %>% unique() %>% na.omit
actor$actor_id <- 1:nrow(actor)
actor$actor_id <- as.character(actor$actor_id)

## CREATE MOVIE CAST TABLE
movie_cast <- merge(dat2, actor, by ="actor") %>% select(movie_id, actor_id) %>% unique()  %>% na.omit

## CREATE TV CAST TABLE
tv_cast <- merge(dat2, actor, by = "actor") %>% select(tv_id, actor_id) %>% unique() %>% na.omit

## CREATE COUNTRY TABLE
country <- dat2 %>% select(country) %>% unique() %>% na.omit
country$country_id <- 1:nrow(country)
country$country_id <- as.character(country$country_id)

## CREATE MOVIE COUNTRY TABLE
movie_country <- merge(dat2, country, by ="country") %>% select(movie_id, country_id) %>% unique()  %>% na.omit

## CREATE TV COUNTRY TABLE
tv_country <- merge(dat2, country, by ="country") %>% select(tv_id, country_id) %>% unique()  %>% na.omit

## CREATE GENRE TABLE
genre <- dat2 %>% select(genre) %>% unique() %>% na.omit
genre$genre_id <- 1:nrow(genre)
genre$genre_id <- as.character(genre$genre_id)

## CREATE MOVIE LISTED_IN TABLE
movie_listed_in <- merge(dat2, genre, by ="genre") %>% select(movie_id, genre_id) %>% unique()  %>% na.omit

## CREATE TV LISTED IN TABLE
tv_listed_in <- merge(dat2, genre, by ="genre") %>% select(tv_id, genre_id) %>% unique()  %>% na.omit

# Append Tables to pgAdmin 

# Load Packages
library(odbc)
require('RPostgreSQL')

# Load the PostgreSQL driver:
drv <- dbDriver('PostgreSQL')

# Connect to the Database
con <- dbConnect(drv, dbname = 'gp',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

movie = movie[]
movie_table <- sqlAppendTable(con, "movie", movie, row.names=FALSE)
dbExecute(con, movie_table)

tv = tv[]
tv_table <- sqlAppendTable(con, "tv", tv, row.names= FALSE)
dbExecute(con, tv_table)

director_table <- sqlAppendTable(con, "director", director, row.names= FALSE)
dbExecute(con, director_table)

movie_director_table <- sqlAppendTable(con, "movie_director", movie_director, row.names= FALSE)
dbExecute(con, movie_director_table)

tv_director_table <- sqlAppendTable(con, "tv_director", tv_director, row.names= FALSE)
dbExecute(con, tv_director_table)

actor_table <- sqlAppendTable(con, "actor", actor, row.names= FALSE)
dbExecute(con, actor_table)

movie_cast_table <- sqlAppendTable(con, "movie_cast", movie_cast, row.names= FALSE)
dbExecute(con, movie_cast_table)

tv_cast_table <- sqlAppendTable(con, "tv_cast", tv_cast, row.names= FALSE)
dbExecute(con, tv_cast_table)

country_table <- sqlAppendTable(con, "country", country, row.names= FALSE)
dbExecute(con, country_table)

movie_country_table <- sqlAppendTable(con, "movie_country", movie_country, row.names= FALSE)
dbExecute(con, movie_country_table)

tv_country_table <- sqlAppendTable(con, "tv_country", tv_country, row.names= FALSE)
dbExecute(con, tv_country_table)

genre_table <- sqlAppendTable(con, "genre", genre, row.names= FALSE)
dbExecute(con, genre_table)

movie_listed_in_table <- sqlAppendTable(con, "movie_listed_in", movie_listed_in, row.names= FALSE)
dbExecute(con, movie_listed_in_table)

tv_listed_in_table <- sqlAppendTable(con, "tv_listed_in", tv_listed_in, row.names= FALSE)
dbExecute(con, tv_listed_in_table)

platform_table <- sqlAppendTable(con, "platform", platform, row.names= FALSE)
dbExecute(con, platform_table)

movie_platform_table <- sqlAppendTable(con, "movie_platform", movie_platform, row.names= FALSE)
dbExecute(con, movie_platform_table)

tv_platform_table <- sqlAppendTable(con, "tv_platform", tv_platform, row.names= FALSE)
dbExecute(con, tv_platform_table)

## VIEWS FOR ANALYSTS

#Load the PostgreSQL driver:
drv <- dbDriver('PostgreSQL')

# Connect to the Database
con <- dbConnect(drv, dbname = 'gp',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

# Average Movie Budget, Revenue, Vote_Average by platform
stmt = "
	CREATE OR REPLACE VIEW movies_by_platform AS
	SELECT avg(m.budget) as avg_budget, 
	      avg(m.revenue) as avg_revenue,
	      avg(m.vote_average) as avg_vote,
	      p.platform as platform
	FROM movie as m
	JOIN movie_platform as mp ON m.movie_id = mp.movie_id
	JOIN platform as p ON mp.platform_id = p.platform_id
	GROUP BY p.platform;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM movies_by_platform;"

dbGetQuery(con, stmt)

# Average TV Episode Length, Number of Seasons, Vote_Average by platform
stmt = "
	CREATE OR REPLACE VIEW tv_by_platform AS
	SELECT avg(t.average_episode_duration) as avg_episode_length, 
	      avg(t.seasons) as avg_num_seasons,
	      avg(t.vote_average) as avg_vote,
	      p.platform as platform
	FROM tv as t
	JOIN tv_platform as tp ON t.tv_id = tp.tv_id
	JOIN platform as p ON tp.platform_id = p.platform_id
	GROUP BY p.platform;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM tv_by_platform;"

dbGetQuery(con, stmt)

# Group by Genre Movies
stmt = "
	CREATE OR REPLACE VIEW movies_by_genre AS
	SELECT avg(m.budget) as avg_budget, 
	      avg(m.revenue) as avg_revenue,
	      avg(m.vote_average) as avg_vote,
	      g.genre as genre
	FROM movie as m
	JOIN movie_listed_in as mg ON m.movie_id = mg.genre_id
	JOIN genre as g ON mg.genre_id = g.genre_id
	GROUP BY g.genre;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM movies_by_genre ORDER BY avg_vote;"

dbGetQuery(con, stmt)

# Group by Genre TV
stmt = "
	CREATE OR REPLACE VIEW tv_by_genre AS
	SELECT avg(t.average_episode_duration) as avg_episode_length, 
	      avg(t.seasons) as avg_num_seasons,
	      avg(t.vote_average) as avg_vote,
	      g.genre as genre
	FROM tv as t
	JOIN tv_listed_in as tg ON t.tv_id = tg.genre_id
	JOIN genre as g ON tg.genre_id = g.genre_id
	GROUP BY g.genre;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM tv_by_genre ORDER BY avg_vote;"

dbGetQuery(con, stmt)

# Group by Country Movie
stmt = "
	CREATE OR REPLACE VIEW movie_by_country AS
	SELECT avg(m.budget) as avg_budget, 
	      avg(m.revenue) as avg_revenue,
	      avg(m.vote_average) as avg_vote,
	      c.country as country
	FROM movie as m
	JOIN movie_country AS mc ON m.movie_id = mc.movie_id
	JOIN country as c ON mc.country_id = c.country_id
	GROUP BY c.country;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM movie_by_country ORDER BY avg_vote DESC;"

dbGetQuery(con, stmt)

# Group by Country TV
stmt = "
	CREATE OR REPLACE VIEW tv_by_country AS
	SELECT avg(t.average_episode_duration) as avg_episode_length, 
	      avg(t.seasons) as avg_num_seasons,
	      avg(t.vote_average) as avg_vote,
	      c.country as country
	FROM tv as t
	JOIN tv_country AS tc ON t.tv_id = tc.tv_id
	JOIN country as c ON tc.country_id = c.country_id
	GROUP BY c.country;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM tv_by_country ORDER BY avg_vote DESC;"

dbGetQuery(con, stmt)

# Most Popular Movie Actors Ranked
stmt = "
  CREATE OR REPLACE VIEW most_popular_actors_movie AS
  SELECT COUNT(ma.actor_id) AS freq,
         a.actor as actor_name
  FROM actor as a
  JOIN movie_cast as ma on a.actor_id = ma.actor_id
  GROUP BY a.actor
  ORDER BY COUNT(ma.actor_id) DESC;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM most_popular_actors_movie;"

dbGetQuery(con, stmt)

# Most Popular Movie Directors Ranked
stmt = "
  CREATE OR REPLACE VIEW most_popular_directors_movie AS
  SELECT COUNT(md.director_id) AS freq,
         d.director as director_name
  FROM director as d
  JOIN movie_director as md on d.director_id = md.director_id
  GROUP BY d.director
  ORDER BY COUNT(md.director_id) DESC;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM most_popular_directors_movie;"

dbGetQuery(con, stmt)

# Most Popular TV Actors Ranked
stmt = "
  CREATE OR REPLACE VIEW most_popular_actors_tv AS
  SELECT COUNT(ta.actor_id) AS freq,
         a.actor as actor_name
  FROM actor as a
  JOIN tv_cast as ta on a.actor_id = ta.actor_id
  GROUP BY a.actor
  ORDER BY COUNT(ta.actor_id) DESC;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM most_popular_actors_tv;"

dbGetQuery(con, stmt)

# Most Popular TV Directors Ranked
stmt = "
  CREATE OR REPLACE VIEW most_popular_directors_tv AS
  SELECT COUNT(td.director_id) AS freq,
         d.director as director_name
  FROM director as d
  JOIN tv_director as td on d.director_id = td.director_id
  GROUP BY d.director
  ORDER BY COUNT(td.director_id) DESC;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM most_popular_directors_tv;"

dbGetQuery(con, stmt)

# Data Table Movies
stmt = "
  CREATE OR REPLACE VIEW movies_filmed_in AS
  SELECT m.movie_id,
         m.title, 
         m.rating, 
         m.release_year, 
         m.duration, 
         m.budget, 
         m.revenue,
         m.vote_average,
         c.country
  FROM movie as m
  JOIN movie_country as mc on m.movie_id=mc.movie_id
  JOIN country as c on mc.country_id = c.country_id;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM movies_filmed_in;"

dbGetQuery(con, stmt)

# Data Table TV
stmt = "
  CREATE OR REPLACE VIEW tv_filmed_in AS
  SELECT t.tv_id,
         t.title, 
         t.rating, 
         t.release_year, 
         t.average_episode_duration, 
         t.episodes, 
         t.seasons,
         t.vote_average,
         c.country
  FROM tv as t
  JOIN tv_country as tc on t.tv_id=tc.tv_id
  JOIN country as c on tc.country_id = c.country_id;
"
dbGetQuery(con, stmt)

stmt = "SELECT * FROM tv_filmed_in;"

dbGetQuery(con, stmt)

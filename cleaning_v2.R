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
		 budget INT,
		 revenue INT,
		 vote_average NUMERIC(3,1),
		 primary key (movie_id)
);
DROP TABLE IF EXISTS tv CASCADE;
create table tv
		(tv_id varchar(8),
		 type varchar(10),
		 title varchar(100)NOT NULL,
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
		(director_id varchar(8) UNIQUE NOT NULL,
		 director varchar(80),
		 primary key (director_id)
		);
DROP TABLE IF EXISTS movie_director CASCADE;	
create table movie_director
		(movie_id varchar(8) UNIQUE NOT NULL,
		 director_id varchar(8) UNIQUE NOT NULL,
		 primary key (movie_id, director_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (director_id) references director (director_id) on delete cascade
		 );
DROP TABLE IF EXISTS tv_director CASCADE;
create table tv_director
		(tv_id varchar(8) UNIQUE NOT NULL,
		 director_id varchar(8) UNIQUE NOT NULL,
		 primary key (tv_id, director_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (director_id) references director (director_id) on delete cascade
		);
DROP TABLE IF EXISTS actor CASCADE;
create table actor
		(actor_id varchar(8) UNIQUE NOT NULL,
		 actor varchar(250),
		 primary key (actor)
		);
DROP TABLE IF EXISTS movie_cast CASCADE;
create table movie_cast
		(movie_id varchar(8) UNIQUE NOT NULL,
		 actor_id varchar(8) UNIQUE NOT NULL,
		 primary key (movie_id, actor_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (actor_id) references actor (actor_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_cast CASCADE;
create table tv_cast
		(tv_id varchar(8) UNIQUE NOT NULL,
		 actor_id varchar(8) UNIQUE NOT NULL,
		 primary key (tv_id, actor_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (actor_id) references actor (actor_id) on delete cascade
		);
DROP TABLE IF EXISTS country CASCADE;
create table country
		(country_id varchar(8) UNIQUE NOT NULL,
		 country varchar(80),
		 primary key (country_id)
		);
DROP TABLE IF EXISTS movie_country CASCADE;
create table movie_country
		(movie_id varchar(8) UNIQUE NOT NULL,
		 country_id varchar(8)UNIQUE NOT NULL,
		 primary key (movie_id,country_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (country_id) references country (country_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_country CASCADE;
 create table tv_country
		(tv_id varchar(8) UNIQUE NOT NULL,
		 country_id varchar(8)UNIQUE NOT NULL,
		 primary key (tv_id,country_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (country_id) references country (country_id) on delete cascade
		);
DROP TABLE IF EXISTS genre CASCADE;
create table genre
		(genre_id varchar(8) UNIQUE NOT NULL,
		 genre varchar(80),
		 primary key (genre_id)
		);
DROP TABLE IF EXISTS movie_listed_in CASCADE;
create table movie_listed_in
		(movie_id varchar(8) UNIQUE NOT NULL,
		 genre_id varchar(8) UNIQUE NOT NULL,
		 primary key (movie_id,genre_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (genre_id) references genre (genre_id) on delete cascade
		);
DROP TABLE IF EXISTS tv_listed_in CASCADE;		
create table tv_listed_in
		(tv_id varchar(8) UNIQUE NOT NULL,
		 genre_id varchar(8) UNIQUE NOT NULL,
		 primary key (tv_id,genre_id),
		 foreign key (tv_id) references tv (tv_id) on delete cascade,
		 foreign key (genre_id) references genre (genre_id) on delete cascade
		);		
DROP TABLE IF EXISTS platform CASCADE;
create table platform
		(platform_id varchar(8) UNIQUE NOT NULL,
		 platform varchar(15),
		 primary key (platform_id)
		);		
DROP TABLE IF EXISTS movie_platform CASCADE;
create table movie_platform
		(movie_id varchar(8) UNIQUE NOT NULL,
		 platform_id varchar(15) UNIQUE NOT NULL,
		 date_added date,
		 description varchar(300),
		 primary key (movie_id, platform_id),
		 foreign key (movie_id) references movie (movie_id) on delete cascade,
		 foreign key (platform_id) references platform (platform_id) on delete cascade
		);		
DROP TABLE IF EXISTS tv_platform CASCADE;
create table tv_platform
		(tv_id varchar(8) UNIQUE NOT NULL,
		 platform_id varchar(15) UNIQUE NOT NULL,
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

# Clean Ratings
unique(dat$rating)

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
dat$budget[dat$revenue == "0"]<- NA

# Clean country variable
countries <- separate(dat, country, sep = "," , into = c("country1","country2","country3"))
dat$country1 <- countries$country1
dat$country2 <- countries$country2
dat$country3 <- countries$country3

## CREATE MOVIE TABLE
movie <- dat %>% filter(type == "Movie") %>%
  select(title, type, release_year, rating, duration, budget, revenue, vote_average) %>%
  unique()
movie$movie_id <- 1:nrow(movie)
movie$movie_id <- as.character(movie$movie_id)
movie$budget <- as.character(movie$budget)
movie$revenue <- as.character(movie$revenue)
movie$vote_average <- as.character(movie$vote_average)
col_names <- c("movie_id","type","title","release_year","rating","duration","budget", "revenue","vote_average")
movie <- movie[,col_names]

# Append Movie Table to pgAdmin
library(odbc)
library(RPostgreSQL)
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname = 'gp',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')
movie_table <- sqlAppendTable(con, "movie", movie, row.names=FALSE)
#dbExecute(con, movie_table)

## CREATE MOVIE PLATFORM TABLE
movie_platform <- merge(movie, dat, by = "title") %>% select(movie_id, platform_id)
movie_platform$platform_id <- as.character(movie_platform$platform_id)
movie_platform_table <- sqlAppendTable(con, "movie_platform", movie_platform, row.names= FALSE)
#dbExecute(con, movie_platform_table)

## CREATE PLATFORM TABLE
platform = c("Netflix", "Hulu", "Disney", "Amazon")
platform_id = c(1,2,3,4)
platform_id = as.character(platform_id)
platform <- data.frame(platform_id, platform)

# Append Platform Table to pgAdmin 
platform_table <- sqlAppendTable(con, "platform", platform, row.names= FALSE)
dbExecute(con, platform_table)

# Split cast into actors
df_splitcast <- dat %>% tidyr::separate(
  cast, sep = ", ", 
  into = c("actor1", "actor2", "actor3", "actor4", "actor5", "actor6", "actor7", "actor8", 
           "actor9", "actor10", "actor11", "actor12", "actor13", "actor14", "actor15", "actor16",
           "actor17", "actor18", "actor19", "actor20", "actor21", "actor22", "actor23"), 
  remove = FALSE
)
head(df_splitcast)

# Remove columns after actor columns
df_splitcast <- df_splitcast[,1:28]
head(df_splitcast)

actor_temp <- melt(df_splitcast, 
                   id.vars = c("id", "type", "title", "director", "cast"),
                   measure.vars = c("actor1", "actor2", "actor3", "actor4", "actor5", "actor6", "actor7", "actor8", 
                                    "actor9", "actor10", "actor11", "actor12", "actor13", "actor14", "actor15", "actor16",
                                    "actor17", "actor18", "actor19", "actor20", "actor21", "actor22", "actor23"), 
                   na.rm = TRUE)

#order by id
actor_temp <- actor_temp[order(id),]
actor_temp
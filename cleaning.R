library(data.table)
library(lubridate)
library(tidyverse)



dat <- fread("output.csv")
head(dat)


# Replace Blank with NA

dat$director[dat$director==""] <- NA
dat$cast[dat$cast==""] <- NA
dat$country[dat$country==""] <- NA
dat$rating[dat$rating==""] <- NA
dat$date_added[dat$date_added==""] <- NA
dat$listed_in[dat$listed_in==""] <- NA

dat$revenue[dat$revenue==""] <- NA
dat$budget[dat$budget==""] <- NA
dat$vote_average[dat$vote_average==""] <- NA
dat$episode_run_time[dat$episode_run_time==""] <- NA
dat$number_of_episodes[dat$number_of_episodes==""] <- NA

#Create platform_id 

dat$platform_id[dat$platform=="Netflix"] <- 1
dat$platform_id[dat$platform=="Hulu"] <- 2
dat$platform_id[dat$platform == "Disney"] <- 3
dat$platform_id[dat$platform == "Amazon"] <- 4

# Clean dates
dat$date_added <- parse_date_time(netflix$date_added,'mdy')
dat$release_year <- parse_date_time(netflix$release_year,'y')



#Clean Ratings
unique(dat$rating)

dat$rating[dat$rating == "13+"] <- "PG-13"
dat$rating[dat$rating == "All"] <- "G"
dat$rating[dat$rating == "ALL"] <- "G"
dat$rating[dat$rating == "NOT RATED"] <- "NR"
dat$rating[dat$rating == "NOT_RATE"] <- "NR"
dat$rating[dat$rating == "ALL_AGES"] <- "G"
dat$rating[dat$rating == "AGES_16_"] <- "R"
dat$rating[dat$rating == "AGES_18_"] <- "R"
dat$rating[dat$rating == "UNRATED"] <- "UR"
dat$rating[dat$rating == "16"] <- "R"
dat$rating[dat$rating == "TV-Y7-FV"] <- "TV-Y7"

## CREATE MOVIE TABLE

movies <- dat %>% filter(type == "Movie") %>%
  select(type, title, date_added, release_year, rating, duration, budget, revenue, vote_average, description ) %>%
  unique()

movies <- transform(movies, movie_id = as.numeric(factor(title)))





#Movie Platform

movie_id <- movies %>% select(movie_id, title)

movie_platform_id <- dat %>% filter(type == "Movie") %>%
  select(platform_id, title)

movie_platform <- merge(movie_id, movie_platform_id, by = "title") %>% select(movie_id, platform_id)



#Platform

platform = c("Netflix", "Hulu", "Disney", "Amazon")

platform_id = c(1,2,3,4)

platform <- data.frame(platform_id, platform_name)


library(odbc)
library(RPostgreSQL)
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname = 'GP',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

#upload platform table 
dbWriteTable(con, c("public", "platform"), platform, row.names= FALSE,  append = T)




##splitting multiple values in a cell

#split cast into actors
df_splitcast <- dat %>% tidyr::separate(
  cast, sep = ", ", 
  into = c("actor1", "actor2", "actor3", "actor4", "actor5", "actor6", "actor7", "actor8", 
           "actor9", "actor10", "actor11", "actor12", "actor13", "actor14", "actor15", "actor16",
           "actor17", "actor18", "actor19", "actor20", "actor21", "actor22", "actor23"), 
  remove = FALSE
)
head(df_splitcast)

#remove columns after actor columns
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

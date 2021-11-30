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



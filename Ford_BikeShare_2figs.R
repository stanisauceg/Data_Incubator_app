# data from https://www.fordgobike.com/system-data

library(magrittr)
library(tidyverse)
library(lubridate)
library(sp)
library(elevatr)

# load 2017 data
data_2017 <- data.table::fread("2017-fordgobike-tripdata.csv")
data_2017 %<>% as.tbl(data_2017)

# load 2018 data
month_sheets <- paste("20180", 1:9, "-fordgobike-tripdata.csv", sep = "")
df.names <- paste("data_2018_0", 1:9, sep = "")

month.data <- vector(mode = "list", length = 9)

for(i in seq_along(df.names)) {
  month.data[[i]] <- assign(df.names[i], data.table::fread(month_sheets[i]))
} 

# Initial wrangling
data_2018a <- bind_rows(month.data[1:5])
data_2018b <- bind_rows(month.data[6:9])

# change station ID columns to char
data_2018a %<>%
  mutate(start_station_id = as.character(start_station_id),
         end_station_id = as.character(end_station_id))

data_2018 <- bind_rows(data_2018a, data_2018b)

# clean up
rm(list = ls(pattern = "2018_0"))
rm(month.data, data_2018a, data_2018b, df.names, i, month_sheets)

# change 2017 station ID columns to char
data_2017 %<>%
  mutate(start_station_id = as.character(start_station_id),
         end_station_id = as.character(end_station_id))

# combine years
data <- data_2017 %>% bind_rows(data_2018)

rm(data_2017, data_2018)

str(data)

# convert column types
data <- data %>%
  mutate(member_gender = as.factor(member_gender),
         user_type = as.factor(user_type),
         bike_share_for_all_trip = as.factor(bike_share_for_all_trip),
         start_time = ymd_hms(start_time),
         end_time = ymd_hms(end_time),
         bike_id = as.character(bike_id))

data$weekday <- factor(wday(data$start_time, label = TRUE), ordered = FALSE)

# turn station IDs back into numeric, turning "NULL" values into NAs
data %<>%
  mutate(start_station_id = as.numeric(start_station_id),
         end_station_id = as.numeric(end_station_id))

# make binary vbl is.weekend
data <- data %>%
  mutate(is.weekend = ifelse(weekday %in% c("Sat", "Sun"), TRUE, FALSE))

# convert "NULL" values into NAs
data <- data %>%
  mutate(start_station_name = replace(start_station_name, which(start_station_name == "NULL"), NA),
         end_station_name = replace(end_station_name, which(end_station_name == "NULL"), NA))


# for now: delete NA stations, since they are approx 0.5% of total
# and all stations starting at NA also end at NA
data.clean <- data %>%
  filter(!is.na(start_station_name))

# Compile station stats
departures <- data.clean %>%
  group_by(start_station_name) %>%
  summarise(departure.count = n()) %>%
  rename(station_name = start_station_name)

arrivals <- data.clean %>%
  group_by(end_station_name) %>%
  summarise(arrival.count = n()) %>%
  rename(station_name = end_station_name)

station.stats <- departures

# expand station.stats w/ arrivals, net change, proportional change
station.stats <- station.stats %>% 
  left_join(arrivals) %>%
  mutate(net.change = arrival.count - departure.count,
         prop.inflow = net.change/departure.count)

rm(arrivals, departures)

# make table of other station info (name, id, lat, long)
station.info <- data.clean %>%
  group_by(start_station_name, 
           start_station_id, 
           start_station_latitude, 
           start_station_longitude) %>%
  summarize(count = n()) %>%
  group_by(start_station_name) %>%
  rename(station_name = start_station_name,
         station_id = start_station_id,
         station_latitude = start_station_latitude,
         station_longitude = start_station_longitude) 

station.info

# find repeat stations
station.count <- station.info %>%
  summarize(station.count = n()) %>%
  arrange(desc(station.count))

id.count <- station.info %>%
  group_by(station_id) %>%
  summarise(n.per.id = n()) %>%
  arrange(desc(n.per.id))

# join station stats
station.stats <- station.stats %>%
  full_join(station.info) %>%
  full_join(id.count) %>%
  full_join(station.count) %>%
  select(station_id, n.per.id, 
         station_name, 
         station.count, 
         station_latitude,
         station_longitude, 
         departure.count:prop.inflow) %>%
  arrange(desc(n.per.id), station_id)

rm(id.count, station.info, station.count)

duplicates <- which(station.stats$station.count == 2)
station.stats[duplicates,] # yep, checks out

extras <- duplicates[c(2,4,6,8)]


# delete extras
station.stats <- station.stats[-extras,]
rm(duplicates, extras)

# station.count now moot, and n.per.id outdated
station.stats <- station.stats %>%
  select(-station.count, -n.per.id)

# recalc station ID counts
new.id.counts <- station.stats %>%
  group_by(station_id) %>%
  summarise(n.per.id = n()) %>%
  arrange(desc(n.per.id))

# add new station ID counts
station.stats <- station.stats %>%
  full_join(new.id.counts) %>%
  select(station_id, n.per.id, station_name:prop.inflow) %>%
  arrange(desc(n.per.id))
rm(new.id.counts)

# Label stations by city
# cluster stations by lat-long, for practice

station.locations <- station.stats %>% select(station_latitude, station_longitude)
dist_stations <- dist(station.locations, method = "euclidean")
hc_stations <- hclust(dist_stations, method = "complete")

# extract clusters
cluster_assigments <- cutree(hc_stations, k = 3)
stations_clustered <- mutate(station.locations, cluster = cluster_assigments)

stations_clustered <- stations_clustered %>%
  mutate(cluster = recode(cluster, '1' = "EastBay",
                          '2' = "SanJose",
                          '3' = "SanFrancisco")) %>%
  rename(city = cluster)

# should separate by long
stations_clustered %>%
  group_by(city) %>%
  summarise(min_long = min(station_longitude),
            max_long = max(station_longitude)) %>%
  as.data.frame() # show more decimal places quickly
# yes: divide along longitude: SJ > -122, -122 > EastBay > -122.3, SF < -122.3

city_by_long <- function(long) {
  if_else (long > -122, "SanJose",
           if_else (long < -122.3, "SanFrancisco",
                    "EastBay"))
}

# add city to station.stats
station.stats <- station.stats %>%
  mutate(city = city_by_long(station_longitude)) %>%
  select(station_id:station_longitude, city, departure.count:prop.inflow)

rm(station.locations, hc_stations, stations_clustered, cluster_assigments, dist_stations)

# Get station elevations

coord_df <- station.stats %>% select(station_longitude, station_latitude)
prj_dd <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

sp <- SpatialPoints(coord_df, proj4string = CRS(prj_dd))

spdf <- SpatialPointsDataFrame(sp, proj4string = CRS(prj_dd), data = station.stats)

# use USGS Elevation Point Query Service (slow, USA only)
spdf_elev_epqs <- get_elev_point(spdf, src = "epqs")

#export results for future use
readr::write_csv(as.data.frame(spdf_elev_epqs), "station_elevation_df.csv")

rm(coord_df, sp, spdf, sp_elev_epqs, prj_dd)

spdf_elev_epqs <- readr::read_csv("station_elevation_df.csv")

# align rows
spdf_elev_epqs %<>% arrange(station_id, station_name)
station.stats %<>% arrange(station_id, station_name)

# add elevation to station info
station.stats$elevation <- spdf_elev_epqs$elevation

# get transit stations
transit <- unique(c(grep("BART", station.stats$station_name, value = TRUE),
                    grep("train", station.stats$station_name, ignore.case = TRUE, value = TRUE),
                    grep("station", station.stats$station_name, ignore.case = TRUE, value = TRUE),
                    grep("ferry", station.stats$station_name, ignore.case = TRUE, value = TRUE)))[-22] # ignore Outside Lands temp station

station.stats <- station.stats %>%
  mutate(is_transit = station_name %in% transit)

# Fig 2 to share ####
ggplot(station.stats, aes(x = elevation, y = prop.inflow, color = city, weight = departure.count, size = departure.count)) + 
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", se = TRUE) +
  theme_bw() + facet_wrap(. ~ is_transit + city) +
  ggtitle("bike surplus proportion by elevation; transit stations in bottom panels")

# add elevation & transit to start & end stations
data.clean <- data.clean %>% 
  arrange(start_time) %>% 
  left_join(station.stats, by = c("start_station_name" = "station_name")) %>%
  mutate(start_station_city = city,
         start_station_is_transit = is_transit,
         start_station_elevation = elevation) %>%
  select(start_time:start_station_longitude, start_station_city:start_station_elevation,
         end_station_id:end_station_longitude, duration_sec, bike_id:is.weekend) %>%
  left_join(station.stats, by = c("end_station_name" = "station_name")) %>%
  mutate(end_station_city = city,
         end_station_is_transit = is_transit,
         end_station_elevation = elevation) %>%
  select(start_time, start_station_id:start_station_elevation,
         end_time, end_station_id:end_station_longitude, end_station_city:end_station_elevation,
         duration_sec:is.weekend) %>%
  mutate(elev_change = end_station_elevation - start_station_elevation)

# bin rides by endpoints
trip.counts <- data.clean %>%
  group_by(start_station_latitude, start_station_longitude, 
           end_station_latitude, end_station_longitude, hour(start_time)) %>%
  rename(start_hour = 'hour(start_time)') %>%
  summarise(start_count = n()) %>%
  as_data_frame()

# Fig 1 to share ####
ggplot(sample_frac(trip.counts, size = 0.1)) +
  geom_segment(aes(x = start_station_longitude, y = start_station_latitude,
                   xend = end_station_longitude, yend = end_station_latitude,
                   alpha = start_count/10), show.legend = FALSE) +
  geom_point(aes(x = start_station_longitude, y = start_station_latitude,
                 size = start_count/6), color = "blue", alpha = 0.2) +
  geom_point(aes(x = end_station_longitude, y = end_station_latitude,
                 size = start_count/6), color = "red", alpha = 0.05) +
  xlim(c(-122.48, -122.37)) + 
  ylim(c(37.745, 37.81)) +
  labs(title = "SF hourly bike traffic", x = "Longitude", y = "Latitude", size = "station traffic in\nriders per minute") +
  theme_bw() +
  facet_wrap(.~ start_hour)

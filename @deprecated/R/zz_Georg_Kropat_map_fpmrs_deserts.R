########################################################################
# This R script maps distances to FPMRS practices over the territory
# of the United States
#
# Author: Georg Kropat 
# Date: 26/02/2019
# It is recommended to use this script with the open-source software RStudio

###################################################################
# Load required packages ----
library(tidyverse) # data manipulation
library(sf) # GIS data processing
library(leaflet) # create interactive geographic maps
library(raster) # GIS raster data processing
library(fasterize) # GIS raster data processing
library(USAboundaries) # load geographic boundaries of the US
library(USAboundariesData) # load geographic boundaries of the US
library(mapview) # load geographic boundaries of the US
library(tidycensus) # download data of US census
library(geofacet) # plot population pyramids
library(extrafont) # additional font types
library(wesanderson) # specific color palettes
library(shiny) # create interactive Shiny applications
library(htmlwidgets)

############################################################
# Set parameters ----
# Avoid scientific notation
options(scipen=999)

#setwd("~/Dropbox/fpmrs_deserts/fprms_deserts")
getwd()
###################################################################
# Define data sources ----
fpmrs_data_file <- "Data/FPMRS_data/leaflet_FPMRS_1269_doctors_revised_new.csv"
duration_raster_file <- "us_raster_duration_2019_02_27_21_41_59.sav"
distance_raster_file <- "us_raster_distance_2019_02_27_21_41_59.sav"
# duration_raster_file <- "us_raster_duration_2019_02_26_17_18_00.sav"
# distance_raster_file <- "us_raster_distance_2019_02_26_17_18_20.sav"


################################################################
# Load data ----

# Load FPMRS data
fpmrs_data <- read_csv(fpmrs_data_file) %>%
  as_tibble()

# Load US county boundaries
us_counties_sf <- us_counties() %>%
  filter(!state_name %in% c("Alaska", "Puerto Rico", "Hawaii"))

# Load national/continental boundary for the US
us_boundaries_sf <- us_boundaries() %>%
  filter(!(state_name %in% c("Alaska", "Puerto Rico", "Hawaii"))) %>%
  st_union()

# Load pixel map of distances to next FPRMS (us_raster_final)
load(file=distance_raster_file)
load(file=duration_raster_file)

# Load demographic data from US decennial census in 2010
age <- get_decennial(geography = "county", table = "P012", summary_var = "P001001")

################################################################
# Preprocess data ----

# Create geo-referenced FPMRS data
fpmrs_sf <- fpmrs_data %>%
  filter(!duplicated(data.frame(lon, lat))) %>%
  filter(!(is.na(lon) | is.na(lat))) %>%
  st_as_sf(coords = c("lon", "lat"), crs=4326)

# Create labels for FPMRS locations
label_function <- function(physician_name, street_address, city, state){
  if(length(physician_name)==1)
  {
    physician_name <- ifelse(is.na(physician_name), "Name not given", physician_name)
    sprintf("<strong>%s</strong>
            <br/>%s
            <br/>%s
            <br/>%s",
            physician_name,
            street_address,
            city,
            state) %>% lapply(htmltools::HTML)
  } else {
    apply(data.frame(physician_name, street_address, city, state), 1,
          function(x){
            x[1] <- ifelse(is.na(x[1]), "Name not given", x[1])
            sprintf("<strong>%s</strong>
                    <br/>%s
                    <br/>%s
                    <br/>%s",
                    x[1],
                    x[2],
                    x[3],
                    x[4]) %>% lapply(htmltools::HTML)
          }) %>%
      unlist() %>%
      paste(collapse="<br/>") %>% lapply(htmltools::HTML)
  }
}

fpmrs_sf <- fpmrs_data %>%
  filter(!duplicated(data.frame(lon, lat))) %>%
  filter(!is.na(complete_address)) %>%
  filter(!(is.na(lon) | is.na(lat))) %>%
  st_as_sf(coords = c("lon", "lat"), crs=4326)

fpmrs_labels <- fpmrs_data %>%
  filter(!(is.na(lon) | is.na(lat))) %>%
  group_by(lon, lat) %>%
  summarize(label= label_function(fullname1, `Line 1 Street Address`, City.y, State.y) %>%
              lapply(htmltools::HTML))%>%
  st_as_sf(coords = c("lon", "lat"), crs=4326)

fpmrs_sf <- fpmrs_sf %>%
  st_join(fpmrs_labels)


# Compute average duration to next FPMRS per county
county_stats_duration <- us_raster_duration_final %>%
  rasterToPoints(spatial = TRUE) %>%
  st_as_sf() %>%
  st_join(us_counties_sf) %>%
  group_by(geoid) %>%
  summarize(mean_duration = mean(layer)) %>%
  ungroup() %>%
  as_tibble() %>%
  dplyr::select(-geometry)

# Compute average distance to next FPMRS per county
county_stats_distance <- us_raster_distance_final %>%
  rasterToPoints(spatial = TRUE) %>%
  st_as_sf() %>%
  st_join(us_counties_sf) %>%
  group_by(geoid) %>%
  summarize(mean_dist = mean(layer)) %>%
  ungroup() %>%
  as_tibble() %>%
  mutate(mean_dist_cat = base::cut(mean_dist, c(0, 10, 50, 100, 500, max(mean_dist) + 1))) %>%
  dplyr::select(-geometry)
levels(county_stats_distance$mean_dist_cat) <- c("< 10", "10-50", "50-100", "100-500", ">500")

county_stats <- county_stats_duration %>%
  left_join(county_stats_distance, by = "geoid")


# Replace geoid of Shannon County with geoid of Oglala Lakota County
age$GEOID[age$GEOID == 46113]  <- 46102

# Replace geoid of Wade Hampton Census Area with geoid of Kusilvak Census Area
age$GEOID[age$GEOID == 02270]  <- 02158

# Count women over 45 in each county
age_45_by_county <- age %>% 
  filter(variable %in% paste0("P0120", c(39:49))) %>%
  group_by(GEOID) %>%
  summarize(n_over_45 = sum(value)) %>%
  ungroup()

# Join county polygons with US census data and average distances to next FPMRS
us_counties_age <- us_counties_sf %>%
  left_join(age_45_by_county, by=c("geoid"="GEOID")) %>%
  left_join(county_stats, by = "geoid")

################################################################
# Plot number of woman over 45 per distance to next FPMRS ----
us_counties_age  %>%
  filter(!is.na(mean_dist_cat) & !is.na(n_over_45)) %>%
  group_by(mean_dist_cat) %>%
  summarize(n_over_45_sum = sum(n_over_45)/1000) %>%
  ungroup() %>%
  ggplot(aes(y = n_over_45_sum, x= mean_dist_cat)) +
  geom_col() +
  labs(x="Mean driving distance to next FPMRS (mi)",
       y = "Number of women over \n45 years (in thousands)") +
  geom_text(aes(label = format(round(n_over_45_sum), big.mark = ",", scientific = FALSE)), vjust = -0.5) +
  scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE), limits=c(0,45000))
ggsave(paste0("n_woman_per_distance", format(Sys.time(),'_%Y%m%d_%H%M%S'), ".png"), plot = last_plot(), device = "png", scale = 1, width = 12, height = 10, units = c("cm"), dpi = 500,  bg = "transparent")

################################################################
# Create county map ----

# Create color palette for county mapping
county_pal <- colorFactor(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                          us_counties_age$mean_dist_cat,
                          na.color = "transparent")

# Create labels with county name, number of women
# over 45 and average distance to next FPMRS
county_labels <- sprintf(
  "<strong>%s County, %s</strong>
  <br/>%s women older than age 45
  <br/>Mean driving distance to closest FPMRS: %s mi
    <br/>Mean driving time to closest FPMRS: %s min",
  us_counties_age$name,
  us_counties_age$state_name,
  format(us_counties_age$n_over_45, nsmall=0, big.mark=","),
  round(us_counties_age$mean_dist, digits = 0),
  round(us_counties_age$mean_duration, digits = 0)
) %>% lapply(htmltools::HTML)

# Create county average map
map <- us_counties_age %>%
  leaflet() %>% 
  addProviderTiles(providers$Esri.WorldStreetMap) %>%
  addPolygons(color = "#444444", weight = 1, smoothFactor = 0.5,
              fillColor = ~ county_pal(mean_dist_cat),
              opacity = 1.0, fillOpacity = 0.5,
              label = county_labels,
              highlightOptions = highlightOptions(color = "white", weight = 2,
                                                  bringToFront = TRUE),
              labelOptions = labelOptions(
                style = list("font-weight" = "normal", padding = "3px 8px"),
                textsize = "12px",
                direction = "auto")) %>%
  addLegend(pal = county_pal, values = us_counties_age$mean_dist_cat,
            title = "Driving distance to\nnext FPMRS (mi)") %>%
  # addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations") %>%
  addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations",
             icon = icons(
               iconUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-icon.png",
               iconWidth = 12.5,
               iconHeight = 20.5,
               iconAnchorX = 6.25, 
               iconAnchorY = 20.5,
               shadowUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-shadow.png",
               shadowWidth = 20.5,
               shadowHeight = 20.5,
               shadowAnchorX = 0,
               shadowAnchorY = 20.5),
             options = markerOptions(opacity = 0.5)
  ) %>%
  addLayersControl(overlayGroups = "FPMRS locations",
                   options = layersControlOptions(collapsed = FALSE)) %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4)
map
saveWidget(map, file="county_map.html")

################################################################
# Create driving duration pixel map ----

# # Round distance calculation for display on map
# us_raster_distance_final_rounded <- us_raster_distance_final
# us_raster_distance_final_rounded@data@values <- round(us_raster_distance_final_rounded@data@values)

# Categorize driving durations
us_raster_duration_cat <- us_raster_duration_final
us_raster_duration_values <- base::cut(us_raster_duration_cat@data@values,
                                       c(0,20,60,120,12*60, 24*60, 
                                         max(us_raster_duration_final@data@values, na.rm = T)))
levels(us_raster_duration_values) <- c("<20 minutes", "20-60 minutes", "1-2 hours", "2-12 hours", "12-24 hours", ">24hours")

us_raster_duration_cat[] <- us_raster_duration_values
us_raster_duration_cat <- as.factor(us_raster_duration_cat)

# Create color palette for driving duration categories
duration_pal_cat <- colorFactor(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                                us_raster_duration_cat[],
                                na.color = "transparent")


# Create pixel map with driving duration categories
duration_pixel_map_cat <- leaflet() %>%
  addProviderTiles(providers$Esri.WorldStreetMap) %>%
  addRasterImage(x = us_raster_duration_cat, opacity = 0.5,
                 colors = duration_pal_cat,
                 layerId = "Driving duration (minutes)", group = "driving_duration") %>%
  # addImageQuery(x = us_raster_duration_cat, project = TRUE,
  #               layerId = "Driving duration (minutes)", group = "driving_duration") %>%
  # addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations") %>%
  addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations",
             icon = icons(
               iconUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-icon.png",
               iconWidth = 12.5,
               iconHeight = 20.5,
               iconAnchorX = 6.25, 
               iconAnchorY = 20.5,
               shadowUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-shadow.png",
               shadowWidth = 20.5,
               shadowHeight = 20.5,
               shadowAnchorX = 0,
               shadowAnchorY = 20.5),
             options = markerOptions(opacity = 0.5)
  ) %>%
  addLayersControl(overlayGroups = "FPMRS locations",
                   options = layersControlOptions(collapsed = FALSE)) %>%
  addLegend(colors = duration_pal_cat(seq_along(levels(us_raster_duration_values))),
            values = us_raster_duration_cat[],
            labels = levels(us_raster_duration_values),
            title = "Driving duration to\nnext FPMRS") %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4)
duration_pixel_map_cat
saveWidget(duration_pixel_map_cat, file="duration_pixel_map.html")

################################################################
# Create driving distance pixel map ----

# Categorize driving distances
us_raster_distance_cat <- us_raster_distance_final
us_raster_distance_values <- base::cut(us_raster_distance_cat@data@values,
                                       c(0, 10, 50, 100, 500, max(us_raster_distance_final@data@values, na.rm = T)))
levels(us_raster_distance_values) <- c("< 10", "10-50", "50-100", "100-500", ">500")

us_raster_distance_cat[] <- us_raster_distance_values
us_raster_distance_cat <- as.factor(us_raster_distance_cat)

distance_pal_cat <- colorFactor(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                                us_raster_distance_cat[],
                                na.color = "transparent")


# Create pixel map with driving distance categories
distance_pixel_map <- leaflet() %>%
  addProviderTiles(providers$Esri.WorldStreetMap) %>%
  addRasterImage(x = us_raster_distance_cat, opacity = 0.5, colors = distance_pal_cat,
                 layerId = "Driving distance (mi)", group = "driving_distance") %>%
  # addImageQuery(x = us_raster_distance_final_rounded, project = TRUE,
  #               layerId = "Driving distance (mi)", group = "driving_distance") %>%
  # addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations") %>%
  addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations",
             icon = icons(
               iconUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-icon.png",
               iconWidth = 12.5,
               iconHeight = 20.5,
               iconAnchorX = 6.25, 
               iconAnchorY = 20.5,
               shadowUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-shadow.png",
               shadowWidth = 20.5,
               shadowHeight = 20.5,
               shadowAnchorX = 0,
               shadowAnchorY = 20.5),
             options = markerOptions(opacity = 0.5)
  ) %>%
  addLayersControl(overlayGroups = "FPMRS locations",
                   options = layersControlOptions(collapsed = FALSE)) %>%
  addLegend(colors = duration_pal_cat(seq_along(levels(us_raster_distance_values))),
            values = us_raster_distance_cat[],
            labels = levels(us_raster_distance_values),
            title = "Driving distance to\nnext FPMRS (mi)") %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4)
distance_pixel_map
saveWidget(distance_pixel_map, file="distance_pixel_map.html")

###################################################################
# Create map with buffer around FPRMS locations ----

# Create color palette for buffer map
buffer_pal <- wes_palette("Darjeeling1")[c(2,5,3,4,1)]

# Compute buffer polygons for different geodesic distances
fpmrs_buffer_sf <- lapply(c(10, 50, 100, 500), function(distance, fpmrs_sf, us_boundaries_sf){
  fpmrs_sf %>%
    st_transform(crs=3857) %>%
    st_buffer(dist = distance * 1000 * 1.60934) %>%
    st_transform(crs=4326) %>%
    st_union() %>%
    st_intersection(us_boundaries_sf)
}, fpmrs_sf = fpmrs_sf, us_boundaries_sf = us_boundaries_sf)
fpmrs_buffer_sf[[length(fpmrs_buffer_sf) + 1]] <- 
  st_difference(us_boundaries_sf,fpmrs_buffer_sf[[length(fpmrs_buffer_sf)]])
names(fpmrs_buffer_sf) <- str_c(c(10, 50, 100, 500, ">500"))

# Create buffer map
buffer_map <- leaflet() %>%
  addProviderTiles(providers$Esri.WorldStreetMap) %>%
  addPolygons(data = fpmrs_buffer_sf[[">500"]],
              color = buffer_pal[5],
              fillColor = buffer_pal[5],
              weight = 1) %>%
  addPolygons(data = fpmrs_buffer_sf[["500"]],
              color = buffer_pal[4],
              fillColor = buffer_pal[4],
              weight = 1) %>%
  addPolygons(data = fpmrs_buffer_sf[["100"]],
              color = buffer_pal[3],
              fillColor = buffer_pal[3],
              weight = 1) %>%
  addPolygons(data = fpmrs_buffer_sf[["50"]],
              color = buffer_pal[2],
              fillColor = buffer_pal[2],
              weight = 1) %>%
  addPolygons(data = fpmrs_buffer_sf[["10"]],
              color = buffer_pal[1],
              fillColor = buffer_pal[1],
              weight = 1) %>%
  # addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations") %>%
  addMarkers(data=fpmrs_sf, label = fpmrs_sf$label, group = "FPMRS locations",
             icon = icons(
               iconUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-icon.png",
               iconWidth = 12.5,
               iconHeight = 20.5,
               iconAnchorX = 6.25, 
               iconAnchorY = 20.5,
               shadowUrl = "https://unpkg.com/leaflet@1.3.1/dist/images/marker-shadow.png",
               shadowWidth = 20.5,
               shadowHeight = 20.5,
               shadowAnchorX = 0,
               shadowAnchorY = 20.5),
             options = markerOptions(opacity = 0.5)
  ) %>%
  addLayersControl(overlayGroups = "FPMRS locations",
                   options = layersControlOptions(collapsed = FALSE)) %>%
  addLegend(title = "Geodesic distance to next FPMRS within",
            colors = buffer_pal,
            labels=c("10 miles", "50 miles", "100 miles", "500 miles", ">500 miles")) %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4)
buffer_map

saveWidget(buffer_map, file="buffer_map.html")



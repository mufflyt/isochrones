########################################################################
# This R script computes the driving times and distances between each pixel
# of a regular grid all over the US andFPMRS practices
# The driving times and distances are computed using a local OSRM server
# which has to be installed prior to the use of this script.
# The installation of this server is outlined on this website
# https://github.com/Project-OSRM/osrm-backend/wiki/Building-OSRM
#
# Author: Georg Kropat 
# Date: 26/02/2019
# It is recommended to use this script with the open-source software RStudio

#Git commit
#setwd("~/Dropbox/fpmrs_deserts/fprms_deserts")

###################################################################
# Load required packages ----
library(tidyverse) # data manipulation
library(sf) # GIS data processing
library(leaflet) # create interactive geographic maps
library(raster) # GIS raster data processing
library(fasterize) # GIS raster data processing
library(osrm) # interface to local OSRM API
library(USAboundaries) # load geographic boundaries of the US
library(nngeo) # nearest neighbours search based on geodesic distances
# library(doParallel)
library(mapview) # query images on leaflet maps
# library(tidycensus) # download data of US census
# library(geofacet) # plot population pyramids
# library(extrafont) # additional font types
# library(USAboundariesData)
# library(future.apply)

############################################################
# Set parameters ----
distance_measure <- c("distance", "time")[2]

# Enable parallel computations (not working yet)
parallel <- FALSE
if(parallel)
{
  # Initiate cluster
  plan(multiprocess, workers = 2) ## Parallelize using four cores
  # cl <- makeCluster(3)
  # # Register cluster
  # registerDoParallel(cl)
  # clusterCall(cl, function() {
  #   .libPaths("/Library/Frameworks/R.framework/Versions/3.5/Resources/library")
  #   library(sf)
  #   library(tidyverse)
  #   library(osrm)
  # })
}

############################################################
# Specify file names ----

fpmrs_data_file <- "~/Dropbox/workforce/Data/FPMRS_data/leaflet_FPMRS_1269_doctors_revised_new.csv"

# Download data ----

# Load geographic boundaries of the US
us_boundaries_poly <- us_boundaries()

# Load FPMRS data
fpmrs_data <- read_csv(fpmrs_data_file) %>%
  as_tibble()

############################################################
# Preprocess data ----
# Remove duplicated addresses
fpmrs_locations <- fpmrs_data %>%
  filter(!duplicated(data.frame(lon, lat)))%>%
  filter(complete.cases(lon) & complete.cases(lat))

us_boundaries_poly_m_units <- us_boundaries_poly %>%
  st_transform(crs=3857)

us_bbox <- st_bbox(us_boundaries_poly_m_units)

us_extent <- extent(us_bbox["xmin"],
                    us_bbox["xmax"],
                    us_bbox["ymin"],
                    us_bbox["ymax"])
resolution <- 4000 *1.60934

empty_raster <- raster(ext = us_extent, resolution = resolution,
                       crs = st_crs(us_boundaries_poly_m_units)$proj4string)
empty_raster <- projectRaster(from=empty_raster, crs=st_crs(us_boundaries_poly)$proj4string)

us_raster_template <- rasterize(us_boundaries_poly, empty_raster)
values(us_raster_template) <- 1

# load(file="us_raster_2019_02_22_17_13_08.sav")
us_raster_distance <- us_raster_template
values(us_raster_distance) <- NA
us_raster_duration <- us_raster_template
values(us_raster_duration) <- NA

############################################################
# Compute driving durations and distances for different
# geographic regions ----

region_list <- c("northeast", "midwest", "south", "west")
# region <- region_list[1]
for(region in region_list)
{
  print(paste("Processing:", region))
  print(paste("Time:", Sys.time()))
  region_folder <- paste0("Data/osrm_data/us_", region, "/")
  
  # Read coordinates of region boundaries
  us_region_poly_raw <- read_delim(file = paste0(region_folder, "us-", region, ".poly"),
                                   skip = 2, col_names = FALSE,
                                   delim= " ") %>%
    head(-2) %>%
    apply(1, function(x) as.numeric(x)) %>%
    t() %>%
    as.matrix()
  
  us_region_polygon <- st_polygon(list(us_region_poly_raw)) %>%
    st_sfc(crs = 4326) %>%
    st_sf()
  
  us_region_polygon$region_id <- 1
  
  # Restrict raster values to region polygon
  us_region_raster <- mask(us_raster_template, us_region_polygon)
  
  us_region_sf <- coordinates(us_region_raster) %>%
    as_tibble() %>%
    rename(lon = x, lat = y) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    filter(!is.na(us_region_raster$layer@data@values)) # filter points that do not lie in the OSRM polygon
  
  us_region_coords <- us_region_sf %>%
    st_coordinates() %>%
    as_tibble() %>%
    mutate(id = row_number()) %>%
    rename(lon = X, lat = Y) %>%
    dplyr::select(id, lon, lat)
  
  
  
  
  
  # create georeferenced FPMRS data
  fpmrs_locations_sf <- fpmrs_locations %>%
    st_as_sf(coords = c("lon", "lat"), crs=4326) %>%
    st_join(us_region_polygon) %>%
    filter(!is.na(region_id))
  
  fpmrs_coords <- fpmrs_locations_sf %>%
    as_tibble() %>%
    mutate(lon = map_dbl(geometry, ~ st_coordinates(.)[1])) %>%
    mutate(lat = map_dbl(geometry, ~ st_coordinates(.)[2])) %>%
    mutate(id = NPI) %>%
    dplyr::select(id, lon, lat)
  
  # compute geodesic distancess between raster points and FPMRS to reduce
  # search space for the driving distance computation
  print("Computing geodesic distances")
  geo_distances <- st_distance(us_region_sf, fpmrs_locations_sf)
  # for each raster point get 5 nearest FPMRS by geodesic distance
  if(parallel){
    system.time(nearest_npis <- future_apply(geo_distances, 1, 
                                             function(x, fpmrs_locations_sf)
                                               fpmrs_locations_sf[order(x)[1:5], "NPI"],
                                             fpmrs_locations_sf = fpmrs_locations_sf))
  } else {
    nearest_npis <- apply(geo_distances, 1, function(x, fpmrs_locations_sf) fpmrs_locations_sf[order(x)[1:5], "NPI"],
                          fpmrs_locations_sf = fpmrs_locations_sf)
  }
  
  
  
  
  
  # for each FPMRS get raster points that include this FPMRS in their nearest 5 FPMRS
  if(parallel)
  {
    raster_points_at_npi <- list()
    foreach(npi=fpmrs_coords$id) %dopar% 
      {
        raster_points_at_npi[id] <- which(
          sapply(nearest_npis, 
                 function(nearest_npi_list, npi) npi %in% nearest_npi_list$NPI,
                 npi = npi))
      }
  } else{
    raster_points_at_npi <- sapply(fpmrs_coords$id, function(npi, nearest_npis)
      which(sapply(nearest_npis, function(nearest_npi_list, npi) npi %in% nearest_npi_list$NPI,
                   npi = npi)),
      nearest_npis = nearest_npis)
  }
  
  
  
  print("Computing driving times & distances")
  # launch OSRM server
  system(paste0("nohup osrm-routed --algorithm=MLD ", region_folder, "us-", region, "-latest.osrm > ", region_folder, "us_", region, ".out &"))
  Sys.sleep(20)
  # get process ID of OSRM server
  process_id <- system("ps -ef", intern = TRUE) %>%
    str_subset("osrm-routed") %>%
    str_split(" ") %>%
    .[[1]] %>%
    str_subset("[^()]") %>%
    .[2] %>%
    as.numeric()
  
  Sys.sleep(20)
  # get address of OSRM server
  server_address <- read_file(paste0(region_folder, "us_", region, ".out")) %>%
    str_split("\n") %>%
    .[[1]] %>%
    str_subset("Listening") %>%
    str_replace("^.*on: ", "")
  
  # connect to OSRM server
  options(osrm.server = paste0("http://", server_address,"/"))
  Sys.sleep(5)   
  system.time({
    distance_to_5_nearest_fpmrs <- tibble(raster_id = numeric(),
                                          duration = numeric(),
                                          distance = numeric(),
                                          npi = numeric())
    
    for(i in 1:nrow(fpmrs_coords))
    {
      current_npi <- fpmrs_coords %>%
        slice(i) %>%
        pull(id)
      if(length(raster_points_at_npi[[i]])>0)
      {
        us_region_coords_batch <- us_region_coords[raster_points_at_npi[[i]], ]
        batch_size <- nrow(us_region_coords_batch)
        if(batch_size > 40)
        {
          n_splits <- ceiling(batch_size/40)
          split_vector <- sort(rep(1:n_splits, 40))[1:batch_size]
          splitted_batch <- split.data.frame(us_region_coords_batch, f = split_vector)
          # distances_temp <- sapply(splitted_batch, function(us_region_coords_batch, fpmrs_coord){
          #   osrmTable(src = us_region_coords_batch, dst = fpmrs_coord, measure = "distance")$distances
          # }, fpmrs_coord = fpmrs_coords[i,])
          durations_distances <- lapply(splitted_batch, function(us_region_coords_batch, fpmrs_coord){
            osrm_request <- osrmTable(src = us_region_coords_batch, dst = fpmrs_coord, measure = c('duration', 'distance'))
            data.frame(duration = as.numeric(osrm_request$durations), distance = as.numeric(osrm_request$distances))
          }, fpmrs_coord = fpmrs_coords[i,])
          durations_distances <- do.call("rbind", durations_distances)
        } else {
          osrm_request <- osrmTable(src = us_region_coords_batch, dst = fpmrs_coords[i,], measure = c('duration', 'distance'))
          
          durations_distances <- data.frame(duration = as.numeric(osrm_request$durations), distance = as.numeric(osrm_request$distances))
          # distances <- osrmTable(src = us_region_coords_batch, dst = fpmrs_coords[i,], measure = c('duration', 'distance'))$distances
        }
        
        
        
        distance_to_5_nearest_fpmrs <- bind_rows(distance_to_5_nearest_fpmrs,
                                                 tibble(raster_id = raster_points_at_npi[[i]],
                                                        duration = durations_distances$duration,
                                                        distance = durations_distances$distance,
                                                        npi = current_npi))
        print(paste0("Number: ", i, " of ", nrow(fpmrs_coords)))
      }
    }
    
  })
  # kill OSRM server
  system(str_c("kill ", process_id))
  if(parallel)
    stopCluster(cl)
  
  distance_to_nearest_fpmrs <- distance_to_5_nearest_fpmrs %>%
    group_by(raster_id) %>%
    summarise(duration = first(duration, order_by=distance),
              distance = first(distance, order_by=distance)/1000) %>%
    ungroup()
  
  us_region_duration <- rep(NA, nrow(us_region_sf))
  us_region_duration[distance_to_nearest_fpmrs$raster_id] <- distance_to_nearest_fpmrs$duration
  us_raster_duration[!is.na(us_region_raster)] <- us_region_duration
  
  us_region_distance <- rep(NA, nrow(us_region_sf))
  us_region_distance[distance_to_nearest_fpmrs$raster_id] <- distance_to_nearest_fpmrs$distance
  us_raster_distance[!is.na(us_region_raster)] <- us_region_distance
  
  current_time <- Sys.time()
  save(us_raster_duration, file=paste0("us_raster_duration_", format(current_time, "%Y_%m_%d_%H_%M_%S"), ".sav"))
  save(us_raster_distance, file=paste0("us_raster_distance_", format(current_time, "%Y_%m_%d_%H_%M_%S"), ".sav"))
}



# crop raster to US boundaries
us_raster_duration_final <- us_boundaries_poly %>%
  filter(!state_name %in% c("Alaska", "Puerto Rico", "Hawaii")) %>%
  mask(us_raster_duration, .)
save(us_raster_duration_final, file=paste0("us_raster_duration_", format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), ".sav"))

us_raster_distance@data@values <- us_raster_distance@data@values/1.60934
us_raster_distance_final <- us_boundaries_poly %>%
  filter(!state_name %in% c("Alaska", "Puerto Rico", "Hawaii")) %>%
  mask(us_raster_distance, .)
save(us_raster_distance_final, file=paste0("us_raster_distance_", format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), ".sav"))





# create buffer to avoid non-covered US territory
us_boundaries_buff <- us_boundaries_poly %>%
  st_transform(crs=3857) %>%
  st_buffer(dist= 10000) %>%
  st_transform(crs=4326)

us_templ_buff<- mask(us_raster_template, us_boundaries_buff)

us_templ_buff[!is.na(us_raster_final)] <- NA

# get buffer coordinates
us_buffer_sf <- us_templ_buff %>%
  rasterToPoints(spatial = TRUE) %>%
  st_as_sf()


# get coordinates of non-missing values of final raster
us_raster_final_sf <- us_raster_final %>%
  rasterToPoints(spatial = TRUE) %>%
  st_as_sf()

nearest_neighbours <- st_nn(us_buffer_sf, us_raster_final_sf, k= 1)

# compute next nearest neighbour for each buffer pixel
nn2()

# st_crs(us_boundaries_transf)$units

pal_distance <- colorNumeric(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                             na.omit(us_raster_distance_final@data@values),
                             na.color = "transparent")


labels <- paste(fpmrs_locations_sf$`Provider First Name`,
                fpmrs_locations_sf$`Provider Last Name (Legal Name)`,
                ", ",
                fpmrs_locations_sf$`Provider Credential Text`)

map <- leaflet() %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addMarkers(data = fpmrs_locations_sf, label = labels) %>%
  addRasterImage(x = us_raster_distance_final, opacity = 0.5, colors = pal_distance,
                 layerId = "Driving distance (km)", group = "driving_distance") %>%
  # addRasterImage(x = us_templ_buff, opacity = 0.5, colors = "green") %>%
  # addImageQuery(x = us_raster_final, project = TRUE,
  #               layerId = "Driving distance (km)", type = "click", group = "driving_distance") %>%
  addLayersControl(overlayGroups = "driving_distance") %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4) %>%
  addLegend(pal = pal_distance, values = us_raster_distance_final@data@values,
            title = "Driving distance to\nnext FPMRS (km)")
map

pal_duration <- colorNumeric(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                             na.omit(us_raster_duration_final@data@values),
                             na.color = "transparent")

map <- leaflet() %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  # addMarkers(data = fpmrs_locations_sf, label = labels) %>%
  addRasterImage(x = us_raster_duration_final, opacity = 0.5, colors = pal_duration,
                 layerId = "Driving duration (h)", group = "driving_duration") %>%
  # addRasterImage(x = us_templ_buff, opacity = 0.5, colors = "green") %>%
  # addImageQuery(x = us_raster_final, project = TRUE,
  #               layerId = "Driving duration (h)", type = "click", group = "driving_duration") %>%
  addLayersControl(overlayGroups = "driving_duration") %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4) %>%
  addLegend(pal = pal_duration, values = us_raster_duration_final@data@values,
            title = "Driving duration to\nnext FPMRS (h)")
map

map <- us_boundaries_poly %>%
  leaflet() %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons() %>%
  # addMarkers(lng=fpmrs_data$lon, lat=fpmrs_data$lat, label=fpmrs_data$fullname1) %>%
  # addMarkers(lng=-86.93941, 45.41622, label="pixel") %>%
  # addMarkers(lng=us_region_coords$lon[temp$raster_id], us_region_coords$lat[temp$raster_id], label="pixel") %>%
  addRasterImage(x = us_raster_distance_final, opacity = 0.5, colors = "green",
                 layerId = "Driving distance (km)", group = "driving_distance") %>%
  # addImageQuery(x = us_raster_final, project = TRUE,
  #               layerId = "Driving distance (km)", type = "click", group = "driving_distance") %>%
  # addLayersControl(overlayGroups = "driving_distance") %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4)
# addLegend(pal = pal, values = us_templ_buff$layer@data@values,
#           title = "Driving distance to\nnext FPMRS (km)")
map

pal_test <- colorNumeric(c("#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"),
                         na.omit(us_raster_distance@data@values),
                         na.color = "transparent")
map <- leaflet() %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  # addMarkers(lng=fpmrs_coords$lon, lat=fpmrs_coords$lat, label=fpmrs_data$fullname1) %>%
  # addMarkers(lng=-86.93941, 45.41622, label="pixel") %>%
  # addMarkers(lng=us_region_coords$lon, us_region_coords$lat, label="pixel") %>%
  addRasterImage(x = us_raster_distance, opacity = 0.5, colors = pal_test,
                 layerId = "Driving distance (km)", group = "driving_distance") %>%
  addImageQuery(x = us_raster_distance, project = TRUE,
                layerId = "Driving distance (km)", group = "driving_distance") %>%
  # addLayersControl(overlayGroups = "driving_distance") %>%
  addMarkers(data=fpmrs_locations_sf, label=fpmrs_locations_sf$fullname1) %>%
  addScaleBar(position = "bottomleft", options = scaleBarOptions(metric = FALSE, maxWidth = 200)) %>%
  setView(lat = 39.8282, lng = -98.5795, zoom = 4) %>%
  addLegend(pal = pal_test, values = us_raster_distance$layer@data@values,
            title = "Driving distance to\nnext FPMRS (km)")
map
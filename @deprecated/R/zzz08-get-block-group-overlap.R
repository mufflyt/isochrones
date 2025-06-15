#######################
source("R/01-setup.R")
#######################

## 3.  Calculate how much each Census block group in our area of interest overlaps with the isochrones

# To calculate how many people live within and outside of the drive time isochrones, we'll need to identify the percent of each Census block group that lies within the isochrones.
#
# Note: you might see some simplified analyses that look at block group centroids instead of calculating the overlap. If you're working with a massive dataset that type of approach can make sense. But there's no need to simplify so much here.
#
# The block group shapefile is from the 2021 ACS via [NHGIS](https://www.nhgis.org/). This file is clipped to shorelines but not interior bodies of water like lakes. It was clipped to water using the `tigris::erase_water()`.
#
# Block group boundaries do not follow lake or other inland water boundaries. So if a lake takes up 75% of the block group, that makes a big difference for drive time calculations. Exclude water, since people don't live in those lakes and ponds --- unless you're in an area with a lot of people in houseboats.
#
# First, make our block groups and isochrones planar and then valid for computation (fixing any minor shape issues) using `st_make_valid()`.
#
# ```{r prep_shp, include=TRUE}
# Define file paths
block_groups_file <- "data/shp/block-groups/" #Several of the blocks are outside of the state of colorado so I am using the entire USA even though this is a small scale experiment.

# Read, transform, and process block groups shapefile in one chain
block_groups <- sf::st_read(block_groups_file) %>%
  filter(STATEFP == "08") %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 10000)

#**************************
# SANITY CHECK ON PLOTTING BLOCK GROUPS
#**************************
block_groups_map <- sf::st_read(block_groups_file) %>%
  filter(STATEFP == "08") %>%
  sf::st_transform(4326) %>%
  sf::st_make_valid()

# Create a basic Leaflet map
map <- leaflet() %>%
  #addTiles() # Add the default map tiles
  addProviderTiles(providers$Stadia.StamenTonerLines,
                   options = providerTileOptions(opacity = 0.35))

# Turn off scientific notation
options(scipen = 999)

# Add the isochrones as polygons to the map
invisible(gc())
map <- map %>%
  setView(lng = -104.8319,
          lat = 39.7294,
          zoom = 10) %>%
  addProviderTiles("CartoDB.Voyager") %>%
  addPolygons(
    data = block_groups_map,
    fillColor = "lightblue",
    fillOpacity = 0.5,
    weight = 1,
    color = "black",
    popup = ~paste("Block Group ID: ", block_groups_map$GISJOIN, "<br>Land Area: ", format(block_groups_map$ALAND/2589988L, big.mark = ","), " square miles")
  ); map


#TODO:  Turn this code writing the shapefile back on later.
# Write processed block groups shapefile
# sf::st_write(block_groups,
#              dsn = "data/08-get-block-group-overlap/",
#              layer = "block_groups",
#              driver = "ESRI Shapefile",
#              quiet = FALSE, append = FALSE)
# ```
#
# ```{r water, include=FALSE}
# TYLER WE WILL NEED TO CHECK THIS OUT!

# block_groups <- block_groups  %>%
#   tigris::erase_water(area_threshold = 0.75)

# Write processed block groups shapefile
# sf::st_write(block_groups, "data/shp/no_water_simplified_us_lck_grp_2021.shp")
# ```

isochrones <- sf::st_read(dsn = "data/07-isochrone-mapping")

# Assuming 'isochrones' is already loaded and contains spatial data
# Process 'isochrones' shapefile
isochrones <- isochrones %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000) %>%
  dplyr::rename("drive_time" = "range")

#Isochrone union needs to be filtered for each rank

# Write processed isochrones
#sf::st_write(isochrones, "data/shp/simplified_isochrones.shp")

# For computation, we don't care about individual isochrones, many of which overlap. Instead we just want to know how a block group fits in with any of the isochrones period, not each individual one. So combine them into one single feature with `st_union()` and then make into a nice sf object with `st_sf()`.
# ```{r join isochrones, include=TRUE}

unique(isochrones$drive_time)

isochrones_joined_30_minutes <- create_union_isochrones(isochrones, minutes = 30)
isochrones_joined_60_minutes <- create_union_isochrones(isochrones, minutes = 60)
isochrones_joined_120_minutes <- create_union_isochrones(isochrones, minutes = 120)
isochrones_joined_180_minutes <- create_union_isochrones(isochrones, minutes = 180)


#**************************
# QUICK MAP FOR REFERENCE AND TO MAKE SURE ISOCHRONE JOIN IS WORKING CORRECTLY
# NEED TO PROJECT BACK TO LAT/LNG FOR MAPPING
#**************************
isochrone_map_30 <- create_isochrone_st_union_map(isochrones_joined_30_minutes, drive_time = 30,palette = "viridis")
isochrone_map_60 <- create_isochrone_st_union_map(isochrones_joined_60_minutes, drive_time = 60,palette = "viridis")
isochrone_map_120 <- create_isochrone_st_union_map(isochrones_joined_120_minutes, drive_time = 120, palette = "viridis")
isochrone_map_180 <- create_isochrone_st_union_map(isochrones_joined_180_minutes, drive_time = 180, palette = "viridis")

# View the maps
isochrone_map_30
isochrone_map_60
isochrone_map_120
isochrone_map_180


#**************************
# CALCULATE OVERLAP BETWEEN BLOCK GROUPS AND ISOCHRONES
#**************************
drive_times <- unique(isochrones$drive_time)


### Filter GYN Oncologists with `st_intersects`
#Important note: for computations in sf we should use a planar projection, [not a lat/long projection](https://r-spatial.github.io/sf/articles/sf6.html#although-coordinates-are-longitudelatitude-xxx-assumes-that-they-are-planar) like we'd use for making Leaflet maps. We'll use projection ESPG [2163](https://epsg.io/2163).Now calculate the percent overlap between each block group and the isochrones.

## Overall, this code is used to calculate and summarize the overlap percentages between block groups and isochrones, which can be useful for spatial analysis and understanding the degree of overlap between these geographic features.

###### EVAL ===== FALSE
#```{r calculate_overlap, include=FALSE}
# Calculate area in all block groups.  It calculates the area of each block group in the block_groups dataset using the sf::st_area function and adds this information as a new column called bg_area in the block_groups data frame.

#Adding to function
#block_groups <- dplyr::mutate(block_groups, bg_area = sf::st_area(block_groups))

# Calculate intersection - will take some minutes.  It calculates the intersection between the block_groups and isochrones_joined datasets using sf::st_intersection. This operation identifies the areas where the block groups and isochrones overlap spatially.
# It also calculates the area of the intersection and stores it in a new column called intersect_area in the resulting data frame.
# It selects only the GEOID and intersect_area columns and drops the geometry information from the data frame using sf::st_drop_geometry.

#Adding to function
# intersect_30 <- sf::st_intersection(block_groups, isochrones_joined_30_minutes) %>%
# mutate(intersect_area = st_area(.)) %>%
# select(GEOID, intersect_area) %>%
# sf::st_drop_geometry()

#sf::st_write(intersect, "data/shp/intersect.shp")

# Merge intersection area by geoid.  It performs a left join between the original block_groups data frame and the intersect data frame based on the common column "GEOID." This step adds the intersect_area information to the block_groups data frame.

#Adding to function
#block_groups <- left_join(block_groups, intersect, by = "GEOID")

# Calculate overlap percent between block groups and isochrones.  It calculates the overlap percentage between each block group and the isochrones by dividing the intersect_area by the bg_area. If there is no intersection (represented by NA in intersect_area), it sets the overlap percentage to 0.

##Adding to function
# block_groups <- block_groups %>%
# 	# If missing it's because it didn't overlap, so 0
# 	mutate(intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
# 		overlap = as.numeric(intersect_area/bg_area))
#
# write_rds(block_groups, "data/08-get-block-group-overlap/block_groups.rds")


#####
# calculate_overlap_percentages <- function(isochrones, block_groups, drive_times) {
#   # Create an empty list to store overlap percentages for each drive time
#   overlap_percentages_list <- list()
#
#   #drive_time <- 30L
#   #drive_times <- 30L #unique(isochrones$drive_time)
#
#   for (drive_time in drive_times) {
#     # Filter isochrones for the specified drive time
#     isochrones_filtered <- isochrones %>%
#       dplyr::rename("isochrone_time" = "drive_time") %>%
#       dplyr::filter(isochrone_time == drive_time) %>%
#       dplyr::rename("drive_time" = "isochrone_time")
#
#     # Calculate intersection
#     intersect <- sf::st_intersection(block_groups, isochrones_filtered) %>%
#       dplyr::mutate(intersect_area = st_area(.)) %>%
#       dplyr::select(GEOID, intersect_area, id, rank) %>%
#       sf::st_drop_geometry()
#
#     write_csv(block_groups, "block_groups.csv")
#     write_csv(intersect, "intersect.csv")
#
#     # Merge intersection area by GEOID
#     #block_groups <- dplyr::left_join(block_groups, intersect, by = c("GEOID"))
#     block_groups <- block_groups %>%
#       exploratory::left_join(`intersect`, by = join_by(`GEOID` == `GEOID`))
#
#     # Calculate area in all block groups
#     block_groups <- block_groups %>%
#       dplyr::mutate(bg_area = st_area(block_groups))
#
#     # Calculate overlap percent between block groups and isochrones
#     block_groups1 <- block_groups %>%
#       dplyr::mutate(
#         intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
#         overlap = as.numeric(intersect_area / bg_area)
#       )
#
#     # Summary of the overlap percentages
#     summary_bg <- summary(block_groups1$overlap)
#
#     # Store the summary in the list for this drive time
#     overlap_percentages_list[[as.character(drive_time)]] <- summary_bg[[4]] * 100
#   }
#
#   return(overlap_percentages_list)
# }
#
#
# # List of unique drive times (drive_time values)
# drive_times <- unique(isochrones$drive_time)
# class(drive_times)
#
# # Calculate overlap percentages for each drive time
# overlap_percentages <- calculate_overlap_percentages(isochrones, block_groups, drive_times)
#
# # Print the results
# for (drive_time in drive_times) {
#   cat("Drive Time:", drive_time, "minutes\n")
#   cat("Overlap Percentage:", overlap_percentages[[as.character(drive_time)]], "%\n\n")
# }

#**************************
# CALCULATE OVERLAP BETWEEN BLOCK GROUPS AND ISOCHRONES
#**************************
drive_times <- unique(isochrones$drive_time)

#TODO: Not going over every drive time.
calculate_overlap_percentages <- function(isochrones, block_groups_file, drive_times = unique(isochrones$drive_time)) {
  # Create an empty list to store overlap percentages for each drive time
  invisible(gc())
  overlap_percentages_list <- list()

  # drive_times <- 30L # for testing
  # drive_time <- 30L # for testing

  #### ISOCHRONES
  for (drive_time in drive_times) {
  cat("Calculating overlap for Drive Time:", drive_time, "minutes\n")

  # Filter isochrones for the specified drive time
  isochrones_filtered <- isochrones %>%
    dplyr::rename("isochrone_time" = "drive_time") %>%
    dplyr::filter(isochrone_time == drive_time) %>%
    dplyr::rename("drive_time" = "isochrone_time")

  cat("Filter isochrones for Drive Time:", drive_time, "minutes\n")
  isochrones_variable_for_a_specific_time <- create_union_isochrones(isochrones, minutes = drive_time)

  #### BLOCK GROUPS - Several of the blocks are outside of the state of colorado so I am using the entire USA even though this is a small scale experiment.

  # Read, transform, and process block groups shapefile in one chain
  invisible(gc())
  block_groups <- sf::st_read(block_groups_file) %>%
    sf::st_transform(2163) %>%
    sf::st_make_valid() #%>%
    #sf::st_simplify(preserveTopology = FALSE, dTolerance = 10000)

  st_crs(block_groups) == st_crs(isochrones_variable_for_a_specific_time)

  #### OVERLAP OF ISOCHRONES AND BLOCK GROUPS
  intersect_sf <- sf::st_intersection(block_groups, isochrones_variable_for_a_specific_time) %>%
    mutate(intersect_area = st_area(.)) %>%
    select(GEOID, intersect_area)

  # Remove geometry column to create a data frame
  intersect_df <- sf::st_drop_geometry(intersect_sf)

  # Merge intersection area by GEOID
  block_groups <- dplyr::left_join(block_groups, intersect_df, by = "GEOID")

  cat("Merged intersection area for Drive Time:", drive_time, "minutes\n")
  # Calculate area in all block groups
  block_groups <- block_groups %>%
    dplyr::mutate(bg_area = st_area(block_groups))

  cat("Calculated area in all block groups for Drive Time:", drive_time, "minutes\n")

  # Calculate overlap percent between block groups and isochrones
  block_groups_shp <- block_groups %>%
    dplyr::mutate(
      intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
      overlap = as.numeric(intersect_area / bg_area)
    )

  cat("Calculated overlap percentages for Drive Time:", drive_time, "minutes\n")

  cat("SAVE SF OUT AS RDS", drive_time, "minutes\n")
  # Save out files - big! Don't save in git
  readr::write_rds(block_groups_shp, "data/08-get-block-group-overlap/block-group-isochrone-overlap.rds")

  cat("SAVE SF OUT AS DATAFRAME", drive_time, "minutes\n")
  bg_overlap <- as.data.frame(block_groups_shp)
  write.csv(bg_overlap, "data/08-get-block-group-overlap/block-group-isochrone-overlap.csv", na = "", row.names = F)

  # Summary of the overlap percentages
  summary_bg <- summary(bg_overlap$overlap)

  # Store the summary in the list for this drive time
  #overlap_percentages_list[[as.character(drive_time)]] <- summary_bg[[4]] * 100
  # Store the summary in the list for this drive time
  overlap_percentages_list[[as.character(drive_time)]] <- as.numeric(summary_bg[1]) * 100

}
  return(overlap_percentages_list)
}

# Calculate overlap percentages for each drive time
overlap_percentages <- calculate_overlap_percentages(isochrones,
                                                     block_groups_file = "data/shp/block-groups",
                                                     drive_times); overlap_percentages

# Print the results
for (drive_time in drive_times) {
  cat("Drive Time:", drive_time, "minutes\n")
  cat("Overlap Percentage:", overlap_percentages[[as.character(drive_time)]], "%\n\n")
}

#####

# Summary of the overlap percents.  It calculates a summary of the overlap values in the block_groups data frame using the summary function.
summary(block_groups$overlap)

summary_bg <- summary(block_groups$overlap)

round(summary_bg[[4]], 4) *100

paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) *100,"% of the block groups in the block groups provided by area. " )


#######
# List of unique drive times (range values)
range_values <- unique(isochrones$range)

# Calculate overlap percentages for each range
overlap_percentages <- calculate_overlap_percentages(isochrones, block_group_file = "data/shp/block-groups/", drive_times)

# Print the results
for (range in range_values) {
  cat("Range:", range, "minutes\n")
  cat("Overlap Percentage:", overlap_percentages[[as.character(range)]], "%\n\n")
}


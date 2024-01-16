#######################
source("R/01-setup.R")
#######################
#This code is primarily focused on processing and analyzing the spatial overlap between block groups and isochrones. It starts by reading a shapefile of block groups for Colorado, transforming it to a suitable projection, simplifying geometries, and writing the processed shapefile to a new location. It then reads isochrones data, applies similar transformations, combines them into a single feature, and creates a reference map. Next, it calculates the percentage overlap between each block group and the isochrones, summarizing the results to provide information about the extent of overlap. The final output includes a summary message indicating the percentage of block groups that overlap with the isochrones.

# Define file paths
# The shp directory has general use files.  
block_groups_file <- "data/shp/block-groups/colorado/" # Colorado is a smaller state for the toy example.  
#block_groups_file <- "data/07.5-prep-get-block-group=overlap" #For full project 

# Read, transform, and process block groups shapefile in one chain
block_groups <- sf::st_read(block_groups_file) %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

# Write processed block groups shapefile
sf::st_write(block_groups,
             dsn = "data/08-get-block-group-overlap/simplified_colorado", #ONLY doing COLORADO and not the USA
             layer = "block_groups",
             driver = "ESRI Shapefile",
             quiet = FALSE, append = FALSE)

# TODO: How to clip water out of the isochrones?  Rstudio crashes with a national file.  Maybe tigris::erase_water in 07.5?

isochrones <- sf::st_read(dsn = "data/07-isochrone-mapping")

isochrones <- isochrones %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

# For computation, we don't care about individual isochrones, many of which overlap. Instead we just want to know how a block group fits in with any of the isochrones period, not each individual one. So combine them into one single feature with `st_union()` and then make into a nice sf object with `st_sf()`.
isochrones_joined <- sf::st_union(isochrones)
isochrones_joined <- sf::st_sf(iso_id = 1, geometry = isochrones_joined) %>%
	sf::st_transform(2163)

### Single Isochrone Coverage Map
#Make another quick reference map to make sure you have just one single isochrones feature. Remember before you could see all the overlapping shapes.

isochrones_joined_map <- isochrones_joined %>%
  sf::st_transform(4326)

# List of unique drive times for which you want to create plots and shapefiles
drive_times <- unique(isochrones$range)

isochrones <- isochrones %>%
  rename("drive_time" ="range")
tyler::create_individual_isochrone_plots(isochrones, drive_times) # TODO: only gives one map, needs to return multiple 

### Filter GYN Oncologists with `st_intersects`
#Important note: for computations in sf we should use a planar projection, [not a lat/long projection](https://r-spatial.github.io/sf/articles/sf6.html#although-coordinates-are-longitudelatitude-xxx-assumes-that-they-are-planar) like we'd use for making Leaflet maps. We'll use projection ESPG [2163](https://epsg.io/2163).Now calculate the percent overlap between each block group and the isochrones.

## Overall, this code is used to calculate and summarize the overlap percentages between block groups and isochrones, which can be useful for spatial analysis and understanding the degree of overlap between these geographic features.

block_groups <- dplyr::mutate(block_groups, bg_area = sf::st_area(block_groups))

# Calculate intersection - will take some minutes.  It calculates the intersection between the block_groups and isochrones_joined datasets using sf::st_intersection. This operation identifies the areas where the block groups and isochrones overlap spatially.
intersect <- sf::st_intersection(block_groups, isochrones_joined) %>%
		mutate(intersect_area = st_area(.)) %>%
		select(GEOID, intersect_area) %>%
		sf::st_drop_geometry()

block_groups <- left_join(block_groups, intersect, by = "GEOID")

block_groups <- block_groups %>%
	# If missing it's because it didn't overlap, so 0
	mutate(intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
		overlap = as.numeric(intersect_area/bg_area))

# Summary of the overlap percents.  It calculates a summary of the overlap values in the block_groups data frame using the summary function.
summary(block_groups$overlap)

summary_bg <- summary(block_groups$overlap)

round(summary_bg[[4]], 4) *100

paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) *100,"% of the block groups in the block groups provided by area. " )

# TRASH
#######
#This function takes the block_groups and isochrones datasets as input, along with the breaks argument for specifying drive times. It calculates overlap percentages for each drive time, saves individual shapefiles, and returns a list of summaries for each drive time.

### TRASH
#```{r, eval=FALSE}
# Function to calculate intersection between block groups and isochrones,
# calculate overlap percentages, and save to a shapefile
# calculate_intersection_overlap_and_save <- function(block_groups, isochrones, drive_time, output_dir) {
#   drive_time <- 60L
#   output_dir <- "data/"
# 
#   # Filter isochrones for the specified drive time
#   isochrones_filtered <- isochrones %>%
#     filter(drive_time == drive_time)
# 
#   # Calculate intersection
#   intersect <- st_intersection(block_groups, isochrones_filtered) %>%
#     #mutate(intersect_area = st_area(.)) %>%
#     select(GEOID, intersect_area) %>%
#     st_drop_geometry()
# 
#   # Log the progress
#   message(paste("Calculating intersection for", drive_time, "minutes..."))
# 
#   tryCatch(
#     {
#       # Write the intersection shapefile
#       output_shapefile <- file.path(output_dir, paste0("intersect_", drive_time, "_minutes.shp"))
#       st_write(intersect, output_shapefile, append = FALSE)
#       message("Intersection calculated and saved successfully.")
# 
#       # Merge intersection area by GEOID
#       block_groups <- left_join(block_groups, intersect, by = "GEOID")
# 
#       # Calculate area in all block groups
#       block_groups <- block_groups %>%
#         mutate(bg_area = st_area(block_groups))
# 
#       # Calculate overlap percent between block groups and isochrones
#       block_groups <- block_groups %>%
#         mutate(
#           intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
#           overlap = as.numeric(intersect_area / bg_area)
#         )
# 
#       # Filter out missing overlap values for quantile calculation
#       non_missing_overlap <- block_groups$overlap
# 
#       # Summary of the overlap percentiles
#       summary_bg <- summary(non_missing_overlap)
# 
#       # Print the summary
#       message("Summary of Overlap Percentages for", drive_time, "minutes:")
#       cat(summary_bg)
# 
#       # Calculate and print the 50th percentile of overlap percentages
#       #median <- round(quantile(non_missing_overlap, probs = 0.5), 4) * 100
#       #message("50th Percentile of Overlap Percentages:", median, "%")
# 
#       # Calculate and print the 75th percentile of overlap percentages
#       # mean <- round(mean(non_missing_overlap), 4) * 100
#       # message("75th Percentile of Overlap Percentages:", mean, "%")
# 
#     },
#     error = function(e) {
#       message("Error: ", e)
#     }
#   )
# }
# 
# # List of unique drive times for which you want to calculate intersection
# unique_drive_times <- unique(isochrones$drive_time)
# 
# # Specify the output directory
# output_dir <- "data/shp/"
# 
# # Loop through unique drive times and calculate intersection for each
# for (drive_time in unique_drive_times) {
#   calculate_intersection_overlap_and_save(block_groups, isochrones, drive_time, output_dir)
# }
# 

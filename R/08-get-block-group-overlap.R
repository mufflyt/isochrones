#######################
source("R/01-setup.R")
#######################
# #The script you've shared orchestrates a complex process involving spatial data analysis, specifically focused on determining the coverage of isochrones over block groups in Colorado. This is particularly useful in urban planning or geography to assess accessibility or coverage areas.
# 
# ### Initial Setup and Data Preparation
# The code initiates by setting up the environment and loading necessary R scripts and packages. It reads shapefiles for block groups (specifically for Colorado) and isochrones from predefined paths. These shapefiles contain geographic data that can represent areas within a city or county that are accessible within a given drive time from a certain point.
# 
# ### Data Transformation and Simplification
# Upon loading the data, transformations are applied to ensure the data is in the correct Coordinate Reference System (CRS) for accurate spatial analysis. This typically involves converting geographic coordinates into a projection that is more suitable for area calculations and spatial joins, which are less distorted than latitude and longitude when it comes to measuring real-world distances.
# 
# ### Spatial Analysis and Overlap Calculation
# The core of this script involves calculating the overlap of isochrones with block groups using spatial join techniques. This is done by intersecting isochrones with block groups and computing the area of overlap. The overlap percentage is calculated to provide insights into how much of each block group's area falls within the accessible regions defined by isochrones. This analysis is repeated for different drive times (e.g., 30, 60, 120, 180 minutes), allowing for a multi-tiered analysis of accessibility.
# 
# ### Data Augmentation and Output
# Further, the script augments the block group data with additional columns indicating the drive time associated with each isochrone overlap calculation. This augmented data is then saved into CSV files for each drive time category, facilitating easy access and further analysis. These files are prepared for further processing or reporting purposes, potentially supporting decisions on urban development, emergency service coverage, or transportation planning.
# 
# ### Visualization and Verification
# Finally, the script uses the `leaflet` package to visualize the isochrones for each drive time on interactive maps. This visualization step is crucial for verifying the correctness of the spatial calculations and provides a graphical representation of data which is more intuitive and informative. It helps in understanding the geographical spread of accessibility across different time thresholds.
# 
# Overall, this script demonstrates a thorough approach to handling and analyzing spatial data with the goal of assessing coverage by isochrones across block groups. This type of analysis is integral in fields such as urban planning, environmental studies, and logistics.

block_groups_file <- "data/07.5-prep-get-block-group-overlap/block_groups_tigris_2022.shp" #For full project 
isochrones_file <- "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp"
drive_time_variable <- "60"
output_dir <- "data/08-get-block-group-overlap"

calculate_intersection_overlap_and_save(block_groups_file, isochrones_file, drive_time_variable, output_dir)

## List of unique drive times for which you want to calculate intersection
unique_drive_times <- c(30, 60, 120, 180)
## Loop through unique drive times and calculate intersection for each
for (drive_time in unique_drive_times) {
  calculate_intersection_overlap_and_save(block_groups_file, isochrones_file, drive_time, output_dir)
}

# Usage
add_drive_time_column("data/08-get-block-group-overlap/intersect_block_group_cleaned_30minutes.csv", "30")
glimpse(read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_30minutes.csv"))

add_drive_time_column("data/08-get-block-group-overlap/intersect_block_group_cleaned_60minutes.csv", "60")
glimpse(read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_60minutes.csv"))

add_drive_time_column("data/08-get-block-group-overlap/intersect_block_group_cleaned_120minutes.csv", "120")
glimpse(read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_120minutes.csv"))

add_drive_time_column("data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv", "180")
glimpse(read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv"))
# 
# glimpse(readr::read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_30minutes.csv"))
# glimpse(readr::read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_60minutes.csv"))
# glimpse(readr::read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_120minutes.csv"))
# glimpse(readr::read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv"))

# Read the CSV files
df_30 <- read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_30minutes.csv")
df_60 <- read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_60minutes.csv")
df_120 <- read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_120minutes.csv")
df_180 <- read_csv("data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv")

# Bind rows
combined_df <- bind_rows(df_30, df_60, df_120, df_180) %>% 
  select(-LSAD) %>%
  reorder_cols(overlap)

#AFFGEOID = American FactFinder summary level code + geovariant code + "00US" + GEOID
glimpse(combined_df)
write_csv(combined_df, "data/08-get-block-group-overlap/combined_df.csv")

#Look at the maps
shapefile30 <- st_read("data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_30_minutes.shp") %>%
  st_transform(crs = 4326) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(); shapefile30

shapefile60 <- st_read("data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_60_minutes.shp") %>%
  st_transform(crs = 4326) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(); shapefile60

shapefile120 <- st_read("data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_120_minutes.shp") %>%
  st_transform(crs = 4326) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(); shapefile120

shapefile180 <- st_read("data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_180_minutes.shp") %>%
  st_transform(crs = 4326) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(); shapefile180
############ All the stuff below here was incorporated into the calculate_intersection_overlap_and_save function 
# Define file paths
# The shp directory has general use files.  
# block_groups_file <- "data/shp/block-groups/colorado/" # Colorado is a smaller state for the toy example.  
# TODO:  we will need to figure out how to get the correct yearly block group here.  
# block_groups_file <- "data/07.5-prep-get-block-group-overlap/block_groups_tigris_2022.shp" #For full project 
# 
# # Read, transform, and process block groups shapefile in one chain
# block_groups <- sf::st_read(block_groups_file) %>%
#   sf::st_transform(2163) %>%
#   sf::st_make_valid() %>%
#   sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
# 
# # Write processed block groups shapefile
# sf::st_write(block_groups,
#              dsn = "data/08-get-block-group-overlap/simplified", 
#              #ONLY doing COLORADO and not the USA
#              layer = "block_groups",
#              driver = "ESRI Shapefile",
#              quiet = FALSE, append = FALSE)
# 
# # Dataframe of 242,297 block groups
# block_groups_df <- st_read("data/08-get-block-group-overlap/simplified") %>%
#   st_drop_geometry() %>%
#   as_tibble() %>%
#   arrange(STATEFP); block_groups_df
# 
# # A tibble: 242,297 × 11
# # STATEFP COUNTYFP TRACTCE BLKGRPCE AFFGEOID              GEOID        NAME  NAMELSAD      LSAD     ALAND  AWATER
# # <chr>   <chr>    <chr>   <chr>    <chr>                 <chr>        <chr> <chr>         <chr>    <dbl>   <dbl>
# #   1 01      073      010500  1        1500000US010730105001 010730105001 1     Block Group 1 BG      800598       0
# # 2 01      073      010703  2        1500000US010730107032 010730107032 2     Block Group 2 BG      558182       0
# # 3 01      097      005100  2        1500000US010970051002 010970051002 2     Block Group 2 BG      493801       0
# 
# # Pull in the isochrones shapefile
# # TODO: How to clip water out of the isochrones?  Rstudio crashes with a national file.  Maybe tigris::erase_water in 07.5?
# # isochrones <- sf::st_read(dsn = "data/07-isochrone-mapping")
# isochrones <- sf::st_read(
#   dsn = "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp") %>%
#   dplyr::arrange(desc(rank)) %>% #This is IMPORTANT for the layering. 
#   rename(drive_time = range)
# 
# isochrones <- isochrones %>%
#   sf::st_transform(2163) %>%
#   sf::st_make_valid() %>%
#   sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
# 
# # For computation, we don't care about individual isochrones, many of which overlap. Instead we just want to know how a block group fits in with any of the isochrones period, not each individual one. So combine them into one single feature with `st_union()` and then make into a nice sf object with `st_sf()`.
# 
# # Filter isochrones for drive_time == 30 before performing st_union
# isochrones_joined_30 <- isochrones %>%
#   filter(drive_time == 30) %>%
#   sf::st_union()
# 
# class(isochrones_joined_30)
# 
# isochrones_joined_30 <- sf::st_sf(iso_id = 1, geometry = isochrones_joined_30) %>%
# 	sf::st_transform(2163)
# 
# plot(isochrones_joined_30)
# ### Single Isochrone Coverage Map
# #Make another quick reference map to make sure you have just one single isochrones feature. Remember before you could see all the overlapping shapes.
# 
# isochrones_joined_map_30 <- isochrones_joined_30 %>%
#   sf::st_transform(4326)
# 
# # # List of unique drive times for which you want to create plots and shapefiles
# # drive_times <- unique(isochrones$drive_time)
# # 
# # create_individual_isochrone_plots <- function(isochrones, drive_times) {
# #   cat("\033[34mInstructions:\033[0m\n")
# #   cat("\033[34mTo use this function, follow the example code below:\033[0m\n")
# #   cat("\n")
# #   cat("\033[34m# Load isochrone data:\033[0m\n")
# #   cat("\033[34misochrones <- readRDS(\"path_to_isochrones.rds\")\n")
# #   cat("\n")
# #   cat("\033[34m# List of unique drive times for which you want to create plots and shapefiles:\033[0m\n")
# #   cat("\033[34mdrive_times <- unique(isochrones$drive_time)\n")
# #   cat("\n")
# #   cat("\033[34m# Create individual isochrone maps and shapefiles:\033[0m\n")
# #   cat("\033[34mcreate_individual_isochrone_plots(isochrones, drive_times)\n")
# #   
# #   message("Creating individual isochrone plots and shapefiles...")
# #   
# #   html_files <- list()
# #   shapefiles <- list()
# #   
# #   for (time in drive_times) {
# #     message(paste("Processing isochrones for", time, "minutes..."))
# #     
# #     isochrones_filtered <- dplyr::filter(isochrones, drive_time == time)
# #     isochrones_combined <- sf::st_union(isochrones_filtered)
# #     isochrones_sf <- dplyr::tibble(iso_id = 1, geometry = isochrones_combined) %>% 
# #       sf::st_sf()
# #     isochrones_sf <- sf::st_transform(isochrones_sf, crs = 4326)
# #     
# #     colors <- grDevices::rainbow(length(drive_times))
# #     index <- which(drive_times == time)
# #     
# #     my_map <- tyler::create_base_map("")
# #     
# #     message(paste("Creating a Leaflet map of isochrones for", time, "minutes..."))
# #     isochrone_map <- my_map %>% 
# #       leaflet::addProviderTiles("CartoDB.Voyager") %>% 
# #       leaflet::addPolygons(data = isochrones_sf, 
# #                            fillColor = colors[index], 
# #                            fillOpacity = 1, 
# #                            weight = 0.5, 
# #                            smoothFactor = 0.2, 
# #                            stroke = TRUE, 
# #                            color = "black")
# #     output_file <- paste0("figures/isochrone_maps/isochrone_map_", time, "_minutes.html")
# #     htmlwidgets::saveWidget(isochrone_map, file = output_file)
# #     message(paste("Saved isochrone map for", time, "minutes as:", output_file))
# #     html_files[[time]] <- output_file
# #     
# #     output_shapefile <- paste0("data/08-get-block-group-overlap/isochrone_files/isochrones_", time, "_minutes.shp")
# #     sf::st_write(isochrones_sf, output_shapefile, append = FALSE)
# #     message(paste("Saved shapefile for", time, "minutes as:", output_shapefile))
# #     shapefiles[[time]] <- output_shapefile
# #     
# #     message(paste("Processed isochrones for", time, "minutes."))
# #   }
# #   
# #   message("Individual isochrone plots and shapefiles creation completed.")
# #   
# #   return(list(html_files = html_files, shapefiles = shapefiles))
# # }
# 
# 
# # isochrones <- isochrones %>%
# #   rename("drive_time" ="range")
# # create_individual_isochrone_plots(isochrones, drive_times) # TODO: only gives one map, needs to return multiple 
# # 
# 
# 
# ### Filter GYN Oncologists with `st_intersects`
# #Important note: for computations in sf we should use a planar projection, [not a lat/long projection](https://r-spatial.github.io/sf/articles/sf6.html#although-coordinates-are-longitudelatitude-xxx-assumes-that-they-are-planar) like we'd use for making Leaflet maps. We'll use projection ESPG [2163](https://epsg.io/2163).Now calculate the percent overlap between each block group and the isochrones.
# 
# ## Overall, this code is used to calculate and summarize the overlap percentages between block groups and isochrones, which can be useful for spatial analysis and understanding the degree of overlap between these geographic features.
# 
# block_groups <- dplyr::mutate(block_groups, bg_area = sf::st_area(block_groups)) %>%
#   mutate(block_groups_drive_time = 30) 
# 
# # Calculate intersection - will take some minutes.  It calculates the intersection between the block_groups and isochrones_joined datasets using sf::st_intersection. This operation identifies the areas where the block groups and isochrones overlap spatially.
# intersect <- sf::st_intersection(block_groups, isochrones_joined_30) %>%
# 		mutate(intersect_area = st_area(.)) %>%
# 		select(GEOID, intersect_area) %>%
#     mutate(intersect_drive_time = 30) %>%
# 		sf::st_drop_geometry()
# 
# intersect_block_group <- left_join(block_groups, intersect, by = "GEOID")
# 
# intersect_block_group_cleaned <- intersect_block_group %>% 
#   mutate(
#     intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),  # Assign 0 to NA intersect_area values
#     overlap = as.numeric(intersect_area / bg_area)  # Calculate overlap as a proportion
#   )
# write_csv(intersect_block_group_cleaned, "data/08-get-block-group-overlap/intersect_block_group_cleaned.csv")
# 
# # Summary of the overlap percents.  It calculates a summary of the overlap values in the block_groups data frame using the summary function.
# summary(intersect_block_group_cleaned$overlap)
# 
# summary_bg <- summary(intersect_block_group_cleaned$overlap)
# 
# round(summary_bg[[4]], 4) *100
# 
# paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) *100,"% of the block groups in the block groups provided by area. " )
# 
# 
# 

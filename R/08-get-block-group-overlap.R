#######################
source("R/01-setup.R")
#######################
#This code is primarily focused on processing and analyzing the spatial overlap between block groups and isochrones. It starts by reading a shapefile of block groups for Colorado, transforming it to a suitable projection, simplifying geometries, and writing the processed shapefile to a new location. It then reads isochrones data, applies similar transformations, combines them into a single feature, and creates a reference map. Next, it calculates the percentage overlap between each block group and the isochrones, summarizing the results to provide information about the extent of overlap. The final output includes a summary message indicating the percentage of block groups that overlap with the isochrones.

#This function takes the block_groups and isochrones datasets as input, along with the breaks argument for specifying drive times. It calculates overlap percentages for each drive time, saves individual shapefiles, and returns a list of summaries for each drive time.

#Function to calculate intersection between block groups and isochrones, calculate overlap percentages, and save to a shapefile

calculate_intersection_overlap_and_save <- function(block_groups_file, isochrones_file, drive_time_variable, output_dir) {
    
    # Read block groups shapefile and process it
    block_groups <- sf::st_read(block_groups_file) %>%
      sf::st_transform(2163) %>%
      sf::st_make_valid() %>%
      sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
    
    # Get CRS of block_groups
    block_groups_crs <- st_crs(block_groups)
    
    # Read isochrones shapefile and process it
    isochrones <- sf::st_read(isochrones_file) %>%
      dplyr::arrange(desc(rank)) %>% # This is IMPORTANT for the layering. 
      rename(drive_time = range) %>%
      sf::st_transform(2163) %>%
      sf::st_make_valid() %>%
      sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000) %>%
      sf::st_set_crs(block_groups_crs)  # Set CRS to match block_groups
    
    # Filter isochrones for the specified drive time
    isochrones_filtered <- isochrones %>%
      filter(drive_time == drive_time_variable) %>%
      mutate(isochrones_drive_time = drive_time_variable) 
    
    # Log the progress
    message(paste("Plot of Isochrones for", drive_time_variable, "minutes..."))
    
    # Function to plot isochrones with USA and state borders, roads, and save as an image file
    plot_isochrones_and_save <- function(isochrones, drive_time_variable, output_dir) {
      # Load USA and state borders data
      usa_border <- ne_countries(scale = "small", country = "United States of America", returnclass = "sf")
      state_border <- ne_states(country = "United States of America", returnclass = "sf")
      
      # Load roads data
      #roads <- ne_download(scale = 110, type = "roads", category = "physical", returnclass = "sf")
      
      # Define limits for the x and y axes to focus on the contiguous USA
      xlim <- c(-125, -65)  # Adjust as needed
      ylim <- c(25, 50)     # Adjust as needed
      
      # Create the ggplot
      p <- ggplot() +
        geom_sf(data = usa_border, color = "black", fill = NA) +  # USA borders
        geom_sf(data = state_border, color = "darkgray", fill = NA) +  # State borders
        #geom_sf(data = block_groups, color = "lightgray") +  
        geom_sf(data = isochrones[1], fill = alpha("blue", 0.5)) +  # Isochrones with fill alpha
        labs(title = paste("Isochrones for", drive_time_variable, "Minutes")) +
        coord_sf(xlim = xlim, ylim = ylim)  # Use coord_sf for spatial data and set limits
      
      # Define the output filename
      output_filename <- paste0("plot_", drive_time_variable, "_minutes.png")
      
      # Save the ggplot as an image file
      ggsave(file.path(output_dir, "isochrone_files", output_filename), plot = p, width = 7, height = 7)
    }
    
    # Call the function to plot and save the isochrones
    plot_isochrones_and_save(isochrones_filtered, drive_time_variable, output_dir)
    
    
    # Call the function to plot and save the isochrones
    plot_isochrones_and_save(isochrones_filtered, drive_time_variable, output_dir)
    
    # Write the filtered isochrones as a shapefile with the drive time variable in the filename
    output_filename <- paste0("filtered_isochrones_", drive_time_variable, "_minutes.shp")
    st_write(
      isochrones_filtered, append = FALSE,
      file.path(
        "data/08-get-block-group-overlap/isochrone_files",
        output_filename
      )
    )
    
    # Calculate intersection between block groups and isochrones
    intersect <- st_intersection(block_groups, isochrones_filtered) %>%
      mutate(intersect_area = st_area(.)) %>%
      select(GEOID, intersect_area) %>%
      mutate(intersect_drive_time = drive_time_variable) %>%
      st_drop_geometry()
    
    # Log the progress
    message(paste("Calculating intersection for", drive_time_variable, "minutes..."))
    
    tryCatch(
      {
        # Merge intersection area by GEOID
        block_groups_intersected <- left_join(block_groups, intersect, by = "GEOID")
        
        # Calculate area in all block groups
        block_groups_intersected <- block_groups_intersected %>%
          mutate(bg_area = st_area(block_groups_intersected))
        
        # Calculate overlap percent between block groups and isochrones
        block_groups_intersected_cleaning <- block_groups_intersected %>%
          mutate(
            intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),
            overlap = as.numeric(intersect_area / bg_area)
          )
        write_csv(block_groups_intersected_cleaning, "data/08-get-block-group-overlap/block_groups_intersected_cleaning.csv")
        # Filter out missing overlap values for quantile calculation
        non_missing_overlap <- block_groups_intersected_cleaning$overlap
        
        # Summary of the overlap percentiles
        summary_bg <- summary(non_missing_overlap)
        
        # Print the summary
        message("Summary of Overlap Percentages for", drive_time_variable, "minutes:")
        message(paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) *100,"% of the block groups in the block groups provided by area. " ))
      },
      error = function(e) {
        message("Error: ", e)
      }
    )
  }

block_groups_file <- "data/07.5-prep-get-block-group-overlap/block_groups_tigris_2022.shp" #For full project 
isochrones_file <- "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp"
drive_time_variable <- 60
output_dir <- "data/08-get-block-group-overlap"

calculate_intersection_overlap_and_save(block_groups_file, isochrones_file, drive_time_variable, output_dir)


## List of unique drive times for which you want to calculate intersection
unique_drive_times <- c(30, 60, 120, 180)
## Loop through unique drive times and calculate intersection for each
for (drive_time in unique_drive_times) {
  calculate_intersection_overlap_and_save(block_groups_file, isochrones_file, drive_time, output_dir)
}



############ All the stuff below here was incorporated into the calculate_intersection_overlap_and_save function 
# Define file paths
# The shp directory has general use files.  
# block_groups_file <- "data/shp/block-groups/colorado/" # Colorado is a smaller state for the toy example.  
# TODO:  we will need to figure out how to get the correct yearly block group here.  
block_groups_file <- "data/07.5-prep-get-block-group-overlap/block_groups_tigris_2022.shp" #For full project 

# Read, transform, and process block groups shapefile in one chain
block_groups <- sf::st_read(block_groups_file) %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

# Write processed block groups shapefile
sf::st_write(block_groups,
             dsn = "data/08-get-block-group-overlap/simplified", 
             #ONLY doing COLORADO and not the USA
             layer = "block_groups",
             driver = "ESRI Shapefile",
             quiet = FALSE, append = FALSE)

# Dataframe of 242,297 block groups
block_groups_df <- st_read("data/08-get-block-group-overlap/simplified") %>%
  st_drop_geometry() %>%
  as_tibble() %>%
  arrange(STATEFP); block_groups_df

# A tibble: 242,297 × 11
# STATEFP COUNTYFP TRACTCE BLKGRPCE AFFGEOID              GEOID        NAME  NAMELSAD      LSAD     ALAND  AWATER
# <chr>   <chr>    <chr>   <chr>    <chr>                 <chr>        <chr> <chr>         <chr>    <dbl>   <dbl>
#   1 01      073      010500  1        1500000US010730105001 010730105001 1     Block Group 1 BG      800598       0
# 2 01      073      010703  2        1500000US010730107032 010730107032 2     Block Group 2 BG      558182       0
# 3 01      097      005100  2        1500000US010970051002 010970051002 2     Block Group 2 BG      493801       0

# Pull in the isochrones shapefile
# TODO: How to clip water out of the isochrones?  Rstudio crashes with a national file.  Maybe tigris::erase_water in 07.5?
# isochrones <- sf::st_read(dsn = "data/07-isochrone-mapping")
isochrones <- sf::st_read(
  dsn = "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp") %>%
  dplyr::arrange(desc(rank)) %>% #This is IMPORTANT for the layering. 
  rename(drive_time = range)

isochrones <- isochrones %>%
  sf::st_transform(2163) %>%
  sf::st_make_valid() %>%
  sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

# For computation, we don't care about individual isochrones, many of which overlap. Instead we just want to know how a block group fits in with any of the isochrones period, not each individual one. So combine them into one single feature with `st_union()` and then make into a nice sf object with `st_sf()`.

# Filter isochrones for drive_time == 30 before performing st_union
isochrones_joined_30 <- isochrones %>%
  filter(drive_time == 30) %>%
  sf::st_union()

class(isochrones_joined_30)

isochrones_joined_30 <- sf::st_sf(iso_id = 1, geometry = isochrones_joined_30) %>%
	sf::st_transform(2163)

plot(isochrones_joined_30)
### Single Isochrone Coverage Map
#Make another quick reference map to make sure you have just one single isochrones feature. Remember before you could see all the overlapping shapes.

isochrones_joined_map_30 <- isochrones_joined_30 %>%
  sf::st_transform(4326)

# # List of unique drive times for which you want to create plots and shapefiles
# drive_times <- unique(isochrones$drive_time)
# 
# create_individual_isochrone_plots <- function(isochrones, drive_times) {
#   cat("\033[34mInstructions:\033[0m\n")
#   cat("\033[34mTo use this function, follow the example code below:\033[0m\n")
#   cat("\n")
#   cat("\033[34m# Load isochrone data:\033[0m\n")
#   cat("\033[34misochrones <- readRDS(\"path_to_isochrones.rds\")\n")
#   cat("\n")
#   cat("\033[34m# List of unique drive times for which you want to create plots and shapefiles:\033[0m\n")
#   cat("\033[34mdrive_times <- unique(isochrones$drive_time)\n")
#   cat("\n")
#   cat("\033[34m# Create individual isochrone maps and shapefiles:\033[0m\n")
#   cat("\033[34mcreate_individual_isochrone_plots(isochrones, drive_times)\n")
#   
#   message("Creating individual isochrone plots and shapefiles...")
#   
#   html_files <- list()
#   shapefiles <- list()
#   
#   for (time in drive_times) {
#     message(paste("Processing isochrones for", time, "minutes..."))
#     
#     isochrones_filtered <- dplyr::filter(isochrones, drive_time == time)
#     isochrones_combined <- sf::st_union(isochrones_filtered)
#     isochrones_sf <- dplyr::tibble(iso_id = 1, geometry = isochrones_combined) %>% 
#       sf::st_sf()
#     isochrones_sf <- sf::st_transform(isochrones_sf, crs = 4326)
#     
#     colors <- grDevices::rainbow(length(drive_times))
#     index <- which(drive_times == time)
#     
#     my_map <- tyler::create_base_map("")
#     
#     message(paste("Creating a Leaflet map of isochrones for", time, "minutes..."))
#     isochrone_map <- my_map %>% 
#       leaflet::addProviderTiles("CartoDB.Voyager") %>% 
#       leaflet::addPolygons(data = isochrones_sf, 
#                            fillColor = colors[index], 
#                            fillOpacity = 1, 
#                            weight = 0.5, 
#                            smoothFactor = 0.2, 
#                            stroke = TRUE, 
#                            color = "black")
#     output_file <- paste0("figures/isochrone_maps/isochrone_map_", time, "_minutes.html")
#     htmlwidgets::saveWidget(isochrone_map, file = output_file)
#     message(paste("Saved isochrone map for", time, "minutes as:", output_file))
#     html_files[[time]] <- output_file
#     
#     output_shapefile <- paste0("data/08-get-block-group-overlap/isochrone_files/isochrones_", time, "_minutes.shp")
#     sf::st_write(isochrones_sf, output_shapefile, append = FALSE)
#     message(paste("Saved shapefile for", time, "minutes as:", output_shapefile))
#     shapefiles[[time]] <- output_shapefile
#     
#     message(paste("Processed isochrones for", time, "minutes."))
#   }
#   
#   message("Individual isochrone plots and shapefiles creation completed.")
#   
#   return(list(html_files = html_files, shapefiles = shapefiles))
# }


# isochrones <- isochrones %>%
#   rename("drive_time" ="range")
# create_individual_isochrone_plots(isochrones, drive_times) # TODO: only gives one map, needs to return multiple 
# 


### Filter GYN Oncologists with `st_intersects`
#Important note: for computations in sf we should use a planar projection, [not a lat/long projection](https://r-spatial.github.io/sf/articles/sf6.html#although-coordinates-are-longitudelatitude-xxx-assumes-that-they-are-planar) like we'd use for making Leaflet maps. We'll use projection ESPG [2163](https://epsg.io/2163).Now calculate the percent overlap between each block group and the isochrones.

## Overall, this code is used to calculate and summarize the overlap percentages between block groups and isochrones, which can be useful for spatial analysis and understanding the degree of overlap between these geographic features.

block_groups <- dplyr::mutate(block_groups, bg_area = sf::st_area(block_groups)) %>%
  mutate(block_groups_drive_time = 30) 

# Calculate intersection - will take some minutes.  It calculates the intersection between the block_groups and isochrones_joined datasets using sf::st_intersection. This operation identifies the areas where the block groups and isochrones overlap spatially.
intersect <- sf::st_intersection(block_groups, isochrones_joined_30) %>%
		mutate(intersect_area = st_area(.)) %>%
		select(GEOID, intersect_area) %>%
    mutate(intersect_drive_time = 30) %>%
		sf::st_drop_geometry()

intersect_block_group <- left_join(block_groups, intersect, by = "GEOID")

intersect_block_group_cleaned <- intersect_block_group %>% 
  mutate(
    intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),  # Assign 0 to NA intersect_area values
    overlap = as.numeric(intersect_area / bg_area)  # Calculate overlap as a proportion
  )
write_csv(intersect_block_group_cleaned, "data/08-get-block-group-overlap/intersect_block_group_cleaned.csv")

# Summary of the overlap percents.  It calculates a summary of the overlap values in the block_groups data frame using the summary function.
summary(intersect_block_group_cleaned$overlap)

summary_bg <- summary(intersect_block_group_cleaned$overlap)

round(summary_bg[[4]], 4) *100

paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) *100,"% of the block groups in the block groups provided by area. " )




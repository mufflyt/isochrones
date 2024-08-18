#######################
source("R/01-setup.R")
#######################
#####Please check data/07.5-prep-get-block-group-overlap to see if these have all been created already.  

#This code downloads, transforms, and writes shapefiles for block groups in the USA for multiple years, including 2022, 2021, 2020, and 2019. It then proceeds to download block group data for earlier years (from 2018 to 2008) for the entire USA by merging state-level data, ensuring a comprehensive dataset for block groups spanning these years.

# Required Input Files for Script Execution

# 1. Block Group Data:
#    - Directory: "/Volumes/Video Projects Muffly 1/block_group_data"
#    - Example files:
#      - "block_groups_2020.shp"
#      - "block_groups_2021.shp"

# 2. Isochrone Data:
#    - Directory: "/Volumes/Video Projects Muffly 1/isochrones_data"
#    - Example files:
#      - "isochrones_2020.rds"
#      - "isochrones_2021.rds"


# TODO: Figure out if we can run tidycensus::erase_water on each state 
# TODO: Rebuild both these functions.  Ugh.  

#block groups are fully formed for the nation from the US Census Bureau via tigris package
download_transform_write_shapefiles(2022)
download_transform_write_shapefiles(2021)
download_transform_write_shapefiles(2020)
download_transform_write_shapefiles(2019)

#No national blockgroups after 2019.  So we download each state block group and combined into one map 
download_each_state_and_merge_to_nation_block_groups(2018)
download_each_state_and_merge_to_nation_block_groups(2017)
download_each_state_and_merge_to_nation_block_groups(2016)
download_each_state_and_merge_to_nation_block_groups(2015)
download_each_state_and_merge_to_nation_block_groups(2014)
download_each_state_and_merge_to_nation_block_groups(2013)
download_each_state_and_merge_to_nation_block_groups(2012)
download_each_state_and_merge_to_nation_block_groups(2011)
download_each_state_and_merge_to_nation_block_groups(2010)
download_each_state_and_merge_to_nation_block_groups(2009)
download_each_state_and_merge_to_nation_block_groups(2008)



#######
download_and_merge_block_groups <- function(vintage, dest_dir, fips_codes = "all", ggplot = FALSE, leaflet = FALSE) {
  # Specific list of state FIPS codes and names
  us_fips_names <- tigris::fips_codes %>%
    dplyr::distinct(state_code, state_name) %>%
    dplyr::filter(as.integer(state_code) < 56) %>%
    dplyr::select(state_code, state_name)
  
  # Create a named vector for easy lookup of state names by FIPS code
  us_fips_list <- setNames(us_fips_names$state_code, us_fips_names$state_name)
  
  # Determine the FIPS codes to use
  if (length(fips_codes) == 1 && fips_codes == "all") {
    fips_list <- us_fips_list
    message("Using all FIPS codes: ", paste(names(fips_list), collapse = ", "))
  } else {
    fips_list <- us_fips_names %>% 
      dplyr::filter(state_code %in% fips_codes) %>% 
      dplyr::pull(state_code) %>%
      setNames(us_fips_names$state_name[us_fips_names$state_code %in% fips_codes])
    message("Using specified FIPS codes: ", paste(names(fips_list), collapse = ", "))
  }
  
  # Create destination directory if it doesn't exist
  if (!fs::dir_exists(dest_dir)) {
    fs::dir_create(dest_dir)
    message("Created directory: ", dest_dir)
  } else {
    message("Directory already exists: ", dest_dir)
  }
  
  # Loop through each year in the vintage argument
  for (year in vintage) {
    message("Processing year: ", year)
    
    # Initialize an empty list to store sf objects for each year
    sf_list <- list()
    
    # Loop through each state and download the block group data
    for (state_code in fips_list) {
      state_name <- names(fips_list)[fips_list == state_code]
      message("Processing state: ", state_name, " (FIPS: ", state_code, ") for year: ", year)
      
      # Attempt to download block group data for the state
      tryCatch({
        message("Downloading block group data for state: ", state_name, " for the year: ", year)
        state_block_groups <- tigris::block_groups(state = state_code, cb = TRUE, year = year)
        message("Downloaded block group data for state: ", state_name)
        
        # Convert to sf object
        message("Converting to sf object for state: ", state_name)
        state_sf <- sf::st_as_sf(state_block_groups)
        message("Converted to sf object for state: ", state_name)
        
        # Make the geometry valid
        message("Making geometry valid for state: ", state_name)
        state_sf <- sf::st_make_valid(state_sf)
        message("Geometry is valid for state: ", state_name)
        
        # Transform to a common CRS for analysis (EPSG:2163)
        message("Transforming CRS for state: ", state_name)
        state_sf <- sf::st_transform(state_sf, crs = 2163)
        message("Transformed CRS for state: ", state_name)
        
        # Simplify the geometry
        message("Simplifying geometry for state: ", state_name)
        state_sf <- sf::st_simplify(state_sf, preserveTopology = FALSE, dTolerance = 1000)
        message("Simplified geometry for state: ", state_name)
        
        # Add a 'year' column
        message("Adding 'year' column for state: ", state_name)
        state_sf$year <- year
        message("'Year' column added for state: ", state_name)
        
        # Add to the list
        message("Adding sf object to list for state: ", state_name)
        sf_list[[state_code]] <- state_sf
      }, error = function(e) {
        message("Failed to download block group data for state: ", state_name, " for the year: ", year)
      })
    }
    
    # Combine all sf objects in the list into one sf object for the year, if any
    if (length(sf_list) > 0) {
      message("Combining sf objects into one sf object for year: ", year)
      usa_block_groups <- do.call(rbind, sf_list)
      message("Combined sf objects for year: ", year)
      
      # Write to a shapefile in the destination directory
      shapefile_name <- file.path(dest_dir, paste0("block_groups_tigris_", year, ".shp"))
      message("Writing combined sf object to shapefile: ", shapefile_name)
      sf::st_write(usa_block_groups, shapefile_name, append = FALSE)
      message("Shapefile written: ", shapefile_name)
      
      # Plot the combined sf object using ggplot2 if requested
      if (ggplot) {
        message("Plotting the combined sf object using ggplot2")
        plot <- ggplot2::ggplot(data = usa_block_groups) +
          ggplot2::geom_sf() +
          ggplot2::ggtitle(paste("USA Block Groups - ", year)) +
          ggplot2::theme_minimal() +
          ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5))
        print(plot)
      }
      
      # Plot the combined sf object using leaflet if requested
      if (leaflet) {
        message("Transforming CRS to WGS84 for leaflet")
        usa_block_groups_leaflet <- sf::st_transform(usa_block_groups, crs = 4326)
        message("Plotting the combined sf object using leaflet")
        leaflet_map <- leaflet::leaflet(data = usa_block_groups_leaflet) %>%
          leaflet::addTiles() %>%
          leaflet::addPolygons() %>%
          leaflet::addControl(paste("USA Block Groups - ", year), position = "topright")
        print(leaflet_map)
      }
    } else {
      message("No data available for the year: ", year)
    }
  }
}

download_and_merge_block_groups(
  vintage = 2013:2023, 
  dest_dir = "/Volumes/Video Projects Muffly 1/isochrones_data", 
  fips_codes = "all",
  ggplot = TRUE,
  leaflet = TRUE
)



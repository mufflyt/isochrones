# The script in question represents a sophisticated geospatial data handling process, utilizing R's `tidyverse` and `sf` (simple features) packages to merge and analyze spatial data. It's a part of a larger workflow designed to integrate and visualize data related to medical subspecialists and geographical accessibility areas defined by isochrones. The script highlights the capabilities of R in processing complex geographical data for practical insights.
# 
# ### Data Preparation and Sanity Checks
# Initially, the script loads a CSV file containing geocoded data of subspecialists. This data is merged with ACOG district information to enrich the dataset with additional geographical context. Substantial effort is devoted to data integrity and visualization: the script includes several sanity checks to ensure the data's validity, such as plotting points on a state and national map to visually inspect their correctness and distribution.
# 
# ### Geographical Operations and Spatial Joins
# The script sets up for a spatial join—a critical step where it combines subspecialist data with isochrone data (geographical shapes representing reachable areas within certain timeframes from a given point). Before this, it ensures that both datasets share the same Coordinate Reference System (CRS) and that the geometries are valid, avoiding common pitfalls in spatial data handling like invalid shapes or mismatched projections.
# 
# ### Visualization and Output
# Further along, the script prepares for visualization by arranging data and applying a color palette for clear representation. The merging of subspecialist data with isochrones allows for richer spatial analysis, such as identifying which medical specialists are accessible within specific time bounds from various locations. The script makes use of `leaflet`, a powerful tool for interactive maps, to create detailed visualizations that include interactive elements like popups.
# 
# ### File Management and Data Export
# Lastly, the script is geared towards robust output management by writing the processed data back into shapefiles—a popular format in geographic information systems (GIS) that supports geometric location and attribute information. This step ensures that the spatial data, now enriched and validated, is stored in a standardized format ready for further analysis or sharing.

# Required Input Files for Script Execution

# 1. Setup Script:
#    - R Script: "R/01-setup.R"

# 2. Input and Output Directories:
#    - Output Directory: "data/06-isochrones/"
#    - File Path: "data/04-geocode/end_geocoded_data_nominatim.csv"

# 3. GOBA Unrestricted Data:
#    - RDS File: "data/06-isochrones/goba/goba_unrestricted.rds"

# 4. Geocoded Data:
#    - CSV File: "data/04-geocode/end_geocoded_data_nominatim.csv"

# 5. ACOG Districts Data:
#    - Data Source: `tyler::ACOG_Districts`

# 6. Shapefiles:
#    - Shapefile: "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp"

# 7. Leaflet Map Output:
#    - HTML File: "figures/isochrone_map_<timestamp>.html"
#    - PNG File: "figures/isochrone_map_<timestamp>.png"


#######################
source("R/01-setup.R")
#######################

ACOG_Districts <- tyler::ACOG_Districts

# Assuming subspecialists_lat_long and isochrones are both sf dataframes
# First, create a copy of subspecialists_lat_long with the point geometries
subspecialists_lat_long <- read_csv("data/04-geocode/end_geocoded_data_nominatim.csv") %>%
  exploratory::left_join(`ACOG_Districts`, by = join_by(`State` == `State`)) 

#**********************************************
# SANITY CHECK ON THE POINTS
#**********************************************
#*. Not perfect because the borders for Hawaii and Alaska are missing.  Jesus.
#*
# Create a ggplot object and plot the data with national borders
states <- ggplot2::map_data("state")

# Convert ACOG_District to a factor
subspecialists_lat_long$ACOG_District <- as.factor(subspecialists_lat_long$ACOG_District)

# Create a ggplot object and plot the data with state and national borders
ggplot(data = subspecialists_lat_long) +
  geom_polygon(
    data = map_data("usa"),
    aes(x = long, y = lat, group = group),
    color = "black",
    fill = NA
  ) +  # Add national borders
  geom_polygon(
    data = states,
    aes(x = long, y = lat, group = group),
    color = "black",
    fill = NA
  ) +  # Add state borders
  geom_point(
    aes(x = long, y = lat, color = ACOG_District), # Color by ACOG_District
    size = 2,
    alpha = 0.2
  ) +
  labs(title = "Subspecialists Lat-Long Plot", x = "Longitude", y = "Latitude") +
  theme(
    plot.title = element_text(hjust = 0.5, size = 16, face = "bold"),
    axis.title = element_text(size = 14),
    axis.text = element_text(size = 12),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  coord_fixed(ratio = 1) +
  scale_color_viridis(discrete = TRUE, option = "magma")

subspecialists_lat_long_copy <- subspecialists_lat_long %>%
  sf::st_as_sf(coords = c("long", "lat")) %>%
  sf::st_set_crs(4326) %>%
  distinct(address, .keep_all = TRUE)

# Plot the isochrones
# Read in shapefile of isochrones
end_isochrones_sf_clipped <- sf::st_read(
  dsn = "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp") %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering. 
#View(end_isochrones_sf_clipped)

# Filter out rows with empty geometries
end_isochrones_sf_clipped <- end_isochrones_sf_clipped[!sf::st_is_empty(end_isochrones_sf_clipped), ]
end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)

#**********************************************
# SANITY CHECK of the ISOCHRONES
#**********************************************
# Create a ggplot object to plot the isochrones data
ggplot() +
  geom_sf(data = end_isochrones_sf_clipped, aes(fill = range)) +
  scale_fill_viridis_d(alpha = 0.2) + # Apply colors
  labs(title = "Isochrones SF Clipped TO USA BORDERS")

##########################################################################
# Perform a spatial join using st_join
# Make sure both data frames are in the same CRS
# sf::st_crs(subspecialists_lat_long_copy) <- sf::st_crs(end_isochrones_sf_clipped)
# 
# # Make the geometry valid
# subspecialists_lat_long_copy <- sf::st_make_valid(subspecialists_lat_long_copy)
# end_isochrones_sf_clipped <- sf::st_make_valid(end_isochrones_sf_clipped)
# #subspecialists_lat_long_copy <- sf::st_simplify(subspecialists_lat_long_copy, preserveTopology = TRUE, dTolerance =1000) #dTolerance is in meters
# #end_isochrones_sf_clipped <- sf::st_simplify(end_isochrones_sf_clipped, preserveTopology = TRUE, dTolerance = 1000) #dTolerance is in meters
# 
# 
# # Set the correct CRS for your data
# subspecialists_lat_long_copy <- sf::st_set_crs(subspecialists_lat_long_copy, 4326)  # Assuming EPSG 4326 (WGS 84) for lat/lon

# result <- sf::st_join(end_isochrones_sf_clipped, subspecialists_lat_long_copy, left = FALSE, suffix = c("_subspecialists_lat_long", "_isochrones")) # 'left' argument set to FALSE means you are retaining the isochrone polygons that intersect with the points in subspecialists_lat_long
# 
# result <- result %>%
#   # arrange(`id`, `rank`, `range`) %>%
#   # group_by(address) %>%
#   # distinct(range, .keep_all = TRUE) %>%
#   # arrange(rank)
#   dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.
# 
# write_csv(result, "data/result.csv")
# 
# # Ensure unique field names
# names(result)[names(result) %in% duplicate_names] <- paste0(duplicate_names, "_2")
# result <- clean_names(result)
# 
# # Write the shapefile
# sf::st_write(result,
#              dsn = "data/07-isochrone-mapping",
#              layer = "results_simplified_validated_isochrones",
#              driver = "ESRI Shapefile",
#              abbreviate = FALSE,
#              quiet = FALSE, 
#              append = FALSE)
# 
# 
# plot(result[1])
# rm(filtered_result)
# invisible(gc())

#*******************************
# SANITY CHECK
#******************************* 
end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)
color_palette <- viridis::magma(length(unique(end_isochrones_sf_clipped$range)))

isochrone_map <- leaflet() %>%
  addProviderTiles("CartoDB.Positron", group = "Greyscale") %>%
  addProviderTiles("CartoDB.Positron", group = "Thunderforest") %>%
  addProviderTiles("Esri.WorldStreetMap", group = "ESRI") %>%

  addPolygons(
    data = end_isochrones_sf_clipped,
    fillColor = ~color_palette[match(range, unique(end_isochrones_sf_clipped$range))],
    # Use of color palette
    fillOpacity = 0.4,
    weight = 0.5,
    smoothFactor = 0.5,
    stroke = TRUE,
    color = "black",
    opacity = 0.2,
    popup = ~paste0("<strong>Isochrone:</strong> ", range, " <br />"),
    group = "Isochrones"
  ) %>%
  
  leaflet::addCircleMarkers(
    data = subspecialists_lat_long_copy,
    radius = 2,
    fill = T,
    fillOpacity = 0.4,
    color = "#1f77b4",
    popup = ~paste0("<strong>Address:</strong> ", address, "<br />",
                    "<strong>Location:</strong> ", city, ", ", state)#,
    # group = "Obstetrician/Gynecologist Subspecialist"
  ) %>%
  
  addLegend(
    data = end_isochrones_sf_clipped,
    position = "bottomright",
    colors = color_palette,
    labels = unique(end_isochrones_sf_clipped$range),  # Corrected to use drive_time
    title = "Drive Time (minutes)",
    opacity = 0.6
  ) %>%
  setView(lng = -105, lat = 39, zoom = 4); isochrone_map

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

html_file <- paste0("figures/isochrone_map_", timestamp, ".html")
png_file <- paste0("figures/isochrone_map_", timestamp, ".png")
htmlwidgets::saveWidget(widget = isochrone_map, file = html_file,
                        selfcontained = TRUE)
cat("Leaflet map saved as HTML:", html_file, "\n")
webshot(html_file, file = png_file)
cat("Screenshot saved as PNG:", png_file, "\n")


#TRASH
#************************************
# SANITY CHECK
#************************************
#Now join information from the gyn_onc dataframe to the isochrones for later use.  After joining, it calculates the drive_time in minutes and reorders the columns. Finally, it saves the joined and modified isochrones data as an RDS file with a timestamp in the filename.
#```{r isochrones_save, eval = TRUE, include=TRUE}
# Add characteristics to isochrones file, save out
# Want to join a plain data frame to the isochrones, not an sf object
# gyn_onc <- subspecialists_lat_long_copy
# gyn_onc_physicians_df <- as.data.frame(gyn_onc) %>%
#   dplyr::select(-geometry)
#
# isochrones <- result
#
# isochrones_data <- dplyr::left_join(isochrones, gyn_onc_physicians_df, by = c("address" = "address"))
#
# isochrones_data <- isochrones_data %>%
#   dplyr::select(range, everything()) %>%
#   dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.
#
# readr::write_rds(isochrones_data, paste("data/isochrones_gyn_onc_joined_", format(Sys.time(), format = "%Y-%m-%d_%H-%M-%S"), ".rds", sep = ""))
# #```
#
# #Map the isochrones to make sure they make sense. We should see one isochrone per point.
# #```{r read_isochrones, echo = FALSE, message = FALSE, warning = FALSE}
# #isochrones <- isochrones_data %>% filter(NPI_goba.x==1437595840) #for testing
# end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)
# #```
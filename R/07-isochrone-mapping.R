#######################
source("R/01-setup.R")
#######################

# This code performs a spatial join between two sf (Simple Features) data frames, subspecialists_lat_long and isochrones. It starts by creating a copy of subspecialists_lat_long with point geometries and ensures both data frames have the same Coordinate Reference System (CRS). Then, it makes the geometries valid (fixes any invalid geometries) in both data frames. Finally, it uses sf::st_join to join the two data frames based on their spatial relationship, and the left = FALSE argument ensures that it retains the isochrone polygons that intersect with the points in subspecialists_lat_long. The code checks if the number of rows in the result is equal to the number of rows in isochrones to verify if no data was lost during the spatial join.

ACOG_Districts <- tyler::ACOG_Districts

# Assuming subspecialists_lat_long and isochrones are both sf dataframes
# First, create a copy of subspecialists_lat_long with the point geometries
subspecialists_lat_long <- read_csv("data/04-geocode/end_completed_clinician_data_geocoded_addresses_12_8_2023.csv") %>%
  mutate(id = 1:n()) %>%
  mutate(postal_code = stringr::str_sub(postal_code, 1, 5)) %>%
  mutate(access = exploratory::str_remove(access, regex("^POINT \\(", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  mutate(access = exploratory::str_remove(access, regex("\\)$", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  separate(access, into = c("long", "lat"), sep = "\\s+", convert = TRUE) %>%
  left_join(`ACOG_Districts`, by = join_by(`state_code` == `State_Abbreviations`)) %>%
  select(-id, -rank, -type, -district, -state) %>%
  filter(country %in% c("United States", "Puerto Rico")) %>%
  mutate(postal_code = str_sub(postal_code, 1, 5)) %>%
  rename(zip = postal_code) %>%
  mutate(across(c(lat, long), parse_number)) %>%
  filter(!is.na(lat)) %>%
  mutate(ACOG_District = as.factor(ACOG_District)) %>%
  distinct(address, .keep_all = TRUE)

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
sf::st_crs(subspecialists_lat_long_copy) <- sf::st_crs(end_isochrones_sf_clipped)

# Make the geometry valid
subspecialists_lat_long_copy <- sf::st_make_valid(subspecialists_lat_long_copy)
end_isochrones_sf_clipped <- sf::st_make_valid(end_isochrones_sf_clipped)
subspecialists_lat_long_copy <- sf::st_simplify(subspecialists_lat_long_copy, preserveTopology = TRUE, dTolerance =1000) #dTolerance is in meters
end_isochrones_sf_clipped <- sf::st_simplify(end_isochrones_sf_clipped, preserveTopology = TRUE, dTolerance = 1000) #dTolerance is in meters


# Set the correct CRS for your data
subspecialists_lat_long_copy <- sf::st_set_crs(subspecialists_lat_long_copy, 4326)  # Assuming EPSG 4326 (WGS 84) for lat/lon

result <- sf::st_join(end_isochrones_sf_clipped, subspecialists_lat_long_copy, left = FALSE, suffix = c("_subspecialists_lat_long", "_isochrones")) # 'left' argument set to FALSE means you are retaining the isochrone polygons that intersect with the points in subspecialists_lat_long

result <- result %>%
  # arrange(`id`, `rank`, `range`) %>%
  # group_by(address) %>%
  # distinct(range, .keep_all = TRUE) %>%
  # arrange(rank)
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.

readr::write_csv(result, "data/result.csv")

# Filter out feature 188562 from the result
filtered_result <- result %>%
filter(row_number() != 188562)

sf::st_write(result,
             dsn = "data/07-isochrone-mapping",
             layer = "results_simplified_validated_isochrones",
             driver = "ESRI Shapefile",
             quiet = FALSE, 
             append = FALSE)

plot(result[1])
rm(filtered_result)
invisible(gc())

#*******************************
# SANITY CHECK
#******************************* 
end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)
color_palette <- viridis::magma(length(unique(end_isochrones_sf_clipped$range)))
north_arrow <- "<svg width='30' height='30' viewBox='0 0 30 30'><polygon points='15,2 18,12 15,10 12,12' fill='black'/><text x='15' y='25' text-anchor='middle' font-size='10'>N</text></svg>"
isochrone_map <- leaflet() %>%
  addProviderTiles("CartoDB.Positron", group = "Greyscale") %>%
  addProviderTiles(providers$CartoDB.Positron, group = "Thunderforest") %>%
  addProviderTiles("Esri.WorldStreetMap", group = "ESRI") %>%
  addScaleBar(position = "bottomleft") %>%
  addControl(html = north_arrow, position = "topright") %>%

  addPolygons(
    data = end_isochrones_sf_clipped,
    fillColor = ~color_palette[match(range, unique(end_isochrones_sf_clipped$range))],
    # Use of color palette
    fillOpacity = 0.1,
    weight = 0.5,
    smoothFactor = 0.5,
    stroke = TRUE,
    color = "black",
    opacity = 0.2,
    popup = ~paste0("<strong>Isochrone:</strong> ", range, " minutes <br />"),
    group = "Isochrones"
  ) %>%
  
  leaflet::addCircleMarkers(
    data = subspecialists_lat_long_copy,
    radius = 2,
    fill = TRUE,
    fillOpacity = 0.1,
    color = "#1f77b4",
    popup = ~paste0("<strong>Address:</strong> ", address, "<br />",
                    "<strong>Location:</strong> ", city, ", ", state_code)
    # group = "Obstetrician/Gynecologist Subspecialist"
  ) %>%
  
  addLegend(
    data = end_isochrones_sf_clipped,
    position = "bottomright",
    colors = color_palette,
    labels = unique(end_isochrones_sf_clipped$range),  # Corrected to use drive_time
    title = "Drive Time (minutes)",
    opacity = 0.3
  ) %>%
  setView(lng = -105, lat = 39, zoom = 6); isochrone_map

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
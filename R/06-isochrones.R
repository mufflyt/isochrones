#######################
source("R/01-setup.R")
#######################
# This code performs a series of data processing and spatial analysis tasks using R and the tidyverse packages. It starts by reading a CSV file named "end_inner_join_postmastr_clinician_data.csv" into the `input_file` data frame and adds a unique identifier column "id" to it. Rows with a specific condition in the "postmastr.name.x" column are filtered out. Then, it keeps only distinct rows based on the "here.address" column and additional filtering criteria involving "postmastr.pm.state" and "postmastr.pm.city."  After preprocessing, the code performs isochrone testing using the "test_and_process_isochrones" function and stores potential errors in the "errors" object. It then filters out rows that are expected to produce errors, creating "input_file_no_error_rows." Next, the code generates isochrones using an external API, and it needs to save them. However, there seems to be a TODO comment indicating that the "process_and_save_isochrones" function needs modification for specifying the save path. The dimensions and class of the resulting isochrones data are checked, and there's a comment that mentions this step takes approximately 15 minutes.Following this, the code reads the isochrones data from a file, arranges it, and clips it to the borders of the USA. The clipped isochrones are validated, and any invalid geometries are corrected. Finally, the clipped isochrones are written to an ESRI Shapefile format in the specified directory. This code is part of a larger data processing and spatial analysis workflow and is designed to handle geographical data, isochrone calculations, and data validation.


# Validate the file of geocoded data.

##### To do the actual gathering of the isochrones: `process_and_save_isochrones`.  We do this in chunks of 25 because we were losing the entire searched isochrones when one error hit.  There is a 1:1 relationship between isochrones and rows in the `input_file` so to match exactly on row we need no errors.  Lastly, we save as a shapefile so that we can keep the MULTIPOLYGON geometry setting for the sf object making it easier to work with the spatial data in the future for plotting, etc.  I struggled because outputing the data as a dataframe was not easy to write it back to a MULTIPOLYGON.

input_file <- readr::read_csv("data/05-geocode-cleaning/end_inner_join_postmastr_clinician_data.csv") %>%
  dplyr::mutate(id = row_number()) %>%
  dplyr::filter(postmastr.name.x != "Hye In Park, MD") %>%
  dplyr::distinct(here.address, .keep_all = TRUE) %>%
  dplyr::filter(postmastr.pm.state == "CO" & postmastr.pm.city == "AURORA")

#**********************************************
# TESTING ISOCHRONES WITH A ONE SECOND ISOCHRONE
#**********************************************
errors <- test_and_process_isochrones(input_file)
errors

#Output from the `test_and_process_isochrones` function
# Filter out the rows that are going to error out after using the test_and_process_isochrones function.
error_rows <- c()

# filter out rows with errors.
input_file_no_error_rows <- input_file %>%
  dplyr::filter(!id %in% error_rows)

# Number of rows of unique physician points * 4 (number of isochrones)
nrow(input_file_no_error_rows) * 4 # 8,544

#**************************
#* HERE API CREATES ISOCHRONES
#**************************

# TODO I need to fix the process_and_save_isochrones function to give it a path to save.
# Call the `process_and_save_isochrones` function with your input_file
isochrones_sf <- process_and_save_isochrones(input_file_no_error_rows)

# Check the dimensions of the final isochrones_data
dim(isochrones_sf)
class(isochrones_sf)

#This takes 15 minutes for some reasons
isochrones_df <- sf::st_read("data/isochrones/isochrones_ 20231223111020 _chunk_ 1 _to_ 4") %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.
  #dplyr::distinct(id, rank, .keep_all = TRUE)
  #dplyr::select(woe_id, woe_name, latitude, longitude, fips, postal, iso_a2, id, rank, departure, arrival, range) %>% filter(woe_name == "Colorado")

# Clip the isochrones to the USA border.
usa_borders <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") %>%
  sf::st_set_crs(4326) %>%
  select(name, iso_a2, woe_id, woe_label, woe_name, latitude, longitude, postal)

isochrones_df <- isochrones_df %>%
  sf::st_set_crs(4326)

invisible(gc())
isochrones_sf_clipped <- sf::st_intersection(isochrones_df, usa_borders) %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.
rm(isochrones_df)

isochrones_sf_clipped <- sf::st_make_valid(isochrones_sf_clipped)
invalid <- st_is_valid(isochrones_sf_clipped, reasons = TRUE)

invisible(gc())
sf::st_write(
  isochrones_sf_clipped,
  dsn = "data/06-isochrones/end_isochrones_sf_clipped",
  layer = "isochrones",
  driver = "ESRI Shapefile",
  quiet = FALSE, append = FALSE)

######################################
# SANITY CHECK
######################################

end_isochrones_sf_clipped <- sf::st_read("data/06-isochrones/end_isochrones_sf_clipped") %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.

# Create a basic Leaflet map
map <- leaflet() %>%
  addTiles() # Add the default map tiles

# Define a cooler color palette (e.g., "viridis")
cool_palette <- viridis::magma(length(end_isochrones_sf_clipped$range))

# Add the isochrones as polygons to the map
map <- map %>%
  addPolygons(
    data = end_isochrones_sf_clipped,
    fillColor = ~cool_palette,
    fillOpacity = 0.5,
    weight = 1,
    color = "black",
    popup = ~paste("ID: ", id, "<br>Range: ", range, " seconds")
  ); map

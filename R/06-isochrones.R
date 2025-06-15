#######################
source("R/01-setup.R")
#######################
# This code performs a series of data processing and spatial analysis tasks using R and the tidyverse packages. It starts by reading a CSV file named "end_inner_join_postmastr_clinician_data.csv" into the `input_file` data frame and adds a unique identifier column "id" to it. Rows with a specific condition in the "postmastr.name.x" column are filtered out. Then, it keeps only distinct rows based on the "here.address" column and additional filtering criteria involving "postmastr.pm.state" and "postmastr.pm.city."  After preprocessing, the code performs isochrone testing using the "test_and_process_isochrones" function and stores potential errors in the "errors" object. It then filters out rows that are expected to produce errors, creating "input_file_no_error_rows." Next, the code generates isochrones using an external API, and it needs to save them. However, there seems to be a TODO comment indicating that the "process_and_save_isochrones" function needs modification for specifying the save path. The dimensions and class of the resulting isochrones data are checked, and there's a comment that mentions this step takes approximately 15 minutes.Following this, the code reads the isochrones data from a file, arranges it, and clips it to the borders of the USA. The clipped isochrones are validated, and any invalid geometries are corrected. Finally, the clipped isochrones are written to an ESRI Shapefile format in the specified directory. This code is part of a larger data processing and spatial analysis workflow and is designed to handle geographical data, isochrone calculations, and data validation.


# Validate the file of geocoded data.

##### To do the actual gathering of the isochrones: `process_and_save_isochrones`.  We do this in chunks of 25 because we were losing the entire searched isochrones when one error hit.  There is a 1:1 relationship between isochrones and rows in the `input_file` so to match exactly on row we need no errors.  Lastly, we save as a shapefile so that we can keep the MULTIPOLYGON geometry setting for the sf object making it easier to work with the spatial data in the future for plotting, etc.  I struggled because outputing the data as a dataframe was not easy to write it back to a MULTIPOLYGON.

input_file <- readr::read_csv("data/05-geocode-cleaning/end_inner_join_postmastr_clinician_data.csv") %>%
  dplyr::mutate(id = row_number()) %>% # creates a unique identifier number
  dplyr::filter(postmastr.name.x != "Hye In Park, MD") %>% # for testing
  dplyr::distinct(here.address, .keep_all = TRUE) %>%
  # dplyr::mutate (id = seq_len(nrow(.))) %>% # creates a unique identifier number
  dplyr::filter(postmastr.pm.state == "CO" & postmastr.pm.city == "AURORA") # For testing with a small sample

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
nrow(input_file_no_error_rows) * 4 

#**************************
#* HERE API CREATES ISOCHRONES
#**************************

# Many thanks to Dr. Unterfinger who maintains the hereR package for emailing me regarding:  https://github.com/munterfi/hereR/issues/153.  "One potential solution I had considered is the option to specify an ID column in the request to hereR. This would tell the package to use this ID column from the input data.frame and append these IDs to the response. However, there are a few issues with this approach. Firstly, it results in the duplication of information. Secondly, the package would need to check the suitability of the column for use as an ID (e.g. duplicate IDs), which I think should not be the responsibility of the package.

# The hereR package sends the requests asynchronously to the Here API and considers the rate limits in the case of a freemium plan. Although the responses from the API will not be received in the same order, the package guarantees that the responses are returned in the same order as the the rows in the input sf data.frame (or sfc column). Therefore, joining the output with the input based on the length should be straightforward and error-free, as long as the input data isn't altered between the request and the response from the package:"
## Add IDs (or use row.names if they are the default sequence)
#poi$id <- seq_len(nrow(poi))

# Request isolines, without aggregating
#iso = isoline(poi, aggregate = FALSE)
#> Sending 8 request(s) with 1 RPS to: 'https://isoline.router.hereapi.com/v8/isolines?...'
#> Received 8 response(s) with total size: 82.9 Kb

# non-spatial join
#(iso_attr <- st_sf(merge(as.data.frame(poi), iso,  by = "id", all = TRUE)))

# TODO I need to fix the process_and_save_isochrones function to give it a path to save.
# Call the `process_and_save_isochrones` function with your input_file
iso_datetime_yearly <- c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
  "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
  "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
  "2022-10-21 09:00:00")


isochrones_sf <- process_and_save_isochrones(input_file_no_error_rows, 
                                             chunk_size = 25, 
                                             iso_datetime = as.POSIXct("2023-10-20 09:00:00"),
                                             iso_ranges = c(30*60, 60*60, 120*60, 180*60),
                                             crs = 4326, 
                                             transport_mode = "car",
                                             file_path_prefix = "data/06-isochrones/isochrones_")

# Check the dimensions of the final isochrones_data
dim(isochrones_sf)
class(isochrones_sf)

isochrones_df <- sf::st_read(
  "data/06-isochrones/isochrones_20231223111020_chunk_1_to_4/isochrones.shp"
) %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering in the leaflet map later on.

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

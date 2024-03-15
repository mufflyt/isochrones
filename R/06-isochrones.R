#######################
source("R/01-setup.R")
#######################
# This code performs a series of data processing and spatial analysis tasks using R and the tidyverse packages. It starts by reading a CSV file named "end_inner_join_postmastr_clinician_data.csv" into the `input_file` data frame and adds a unique identifier column "id" to it. Rows with a specific condition in the "postmastr.name.x" column are filtered out. Then, it keeps only distinct rows based on the "here.address" column and additional filtering criteria involving "postmastr.pm.state" and "postmastr.pm.city."  After preprocessing, the code performs isochrone testing using the "test_and_process_isochrones" function and stores potential errors in the "errors" object. It then filters out rows that are expected to produce errors, creating "input_file_no_error_rows." Next, the code generates isochrones using an external API, and it needs to save them. However, there seems to be a TODO comment indicating that the "process_and_save_isochrones" function needs modification for specifying the save path. The dimensions and class of the resulting isochrones data are checked, and there's a comment that mentions this step takes approximately 15 minutes.Following this, the code reads the isochrones data from a file, arranges it, and clips it to the borders of the USA. The clipped isochrones are validated, and any invalid geometries are corrected. Finally, the clipped isochrones are written to an ESRI Shapefile format in the specified directory. This code is part of a larger data processing and spatial analysis workflow and is designed to handle geographical data, isochrone calculations, and data validation.


# Validate the file of geocoded data.

##### To do the actual gathering of the isochrones: `process_and_save_isochrones`.  We do this in chunks of 25 because we were losing the entire searched isochrones when one error hit.  There is a 1:1 relationship between isochrones and rows in the `input_file` so to match exactly on row we need no errors.  Lastly, we save as a shapefile so that we can keep the MULTIPOLYGON geometry setting for the sf object making it easier to work with the spatial data in the future for plotting, etc.  I struggled because outputing the data as a dataframe was not easy to write it back to a MULTIPOLYGON.

input_file <- #readr::read_csv("data/05-geocode-cleaning/end_inner_join_postmastr_clinician_data.csv") %>%
  readr::read_csv("data/04-geocode/end_geocoded_data_nominatim.csv") %>%
  dplyr::mutate(id = row_number()) %>% # creates a unique identifier number
  dplyr::distinct(address, .keep_all = TRUE) %>%
  dplyr::filter(!is.na(lat) & !is.na(long)) %>% # Exclude rows where lat or long is NA
  dplyr::filter(sub1 == "ONC") %>%
  # dplyr::mutate (id = seq_len(nrow(.))) %>% # creates a unique identifier number
  dplyr::filter(State == "Colorado")# & city_goba == "Aurora") # For testing with a small sample

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
iso_datetime_yearly <-
  c(
    "2013-10-18 09:00:00",
    "2014-10-17 09:00:00",
    "2015-10-16 09:00:00",
    "2016-10-21 09:00:00",
    "2017-10-20 09:00:00",
    "2018-10-19 09:00:00",
    "2019-10-18 09:00:00",
    "2020-10-16 09:00:00",
    "2021-10-15 09:00:00",
    "2022-10-21 09:00:00",
    "2023-10-20 09:00:00"
  )

isochrones_sf <- process_and_save_isochrones(
  input_file = input_file_no_error_rows,
  chunk_size = 25,
  iso_datetime = "2023-10-20 09:00:00",
  iso_ranges = c(30 * 60, 
                 60 * 60, 
                 120 * 60, 
                 180 * 60),
  crs = 4326,
  transport_mode = "car",
  file_path_prefix = "data/06-isochrones/isochrones_"
)

# Check the dimensions of the final isochrones_data
dim(isochrones_sf)
class(isochrones_sf)
glimpse(isochrones_sf)
class(isochrones_sf$id)
dim(input_file_no_error_rows)
class(input_file_no_error_rows)
glimpse(input_file_no_error_rows)
input_file_no_error_rows$id <- as.numeric(input_file_no_error_rows$id)
class(input_file_no_error_rows$id)

# Create the master key data frame
master_key <- data.frame(input_file_no_error_rows_id = unique(input_file_no_error_rows$id),
                         isochrones_sf_id = seq_len(nrow(isochrones_sf))) %>%
  distinct(input_file_no_error_rows_id, .keep_all = TRUE) %>%
  rename("id" = "input_file_no_error_rows_id"); master_key

# write_csv(input_file_no_error_rows, "data/06-isochrones/input_file_no_error_rows.csv")
# write_csv(isochrones_sf, "data/06-isochrones/isochrones_sf.csv")
# write_csv(master_key, "data/06-isochrones/master_key.csv")

# Merge based on the corresponding IDs in the master key
merged_data <- input_file_no_error_rows %>%
  left_join(master_key, by = c("id" = "id")) %>%
  left_join(isochrones_sf, by = c("isochrones_sf_id" = "id"))

# Clean up the merged data (optional)
merged_data <- merged_data[, !(names(merged_data) %in% c("id.x", "id.y"))] # Remove duplicate ID columns

merged_data <- merged_data %>%
  mutate(range = range/60L)
#View(merged_data)

# Check the merged data
glimpse(merged_data)
class(merged_data)
write_csv(merged_data, "data/06-isochrones/end_merged_data_isochrones_input_file.csv")

# Write the resulting sf object to a shapefile
#st_write(merged_sf, "data/06-isochrones/merged_sf.shp")

# Define a function to generate range descriptions
generate_range_description <- function(range) {
  if (range >= 0 && range <= 30) {
    return("0-29 minutes")
  } else if (range > 30 && range <= 60) {
    return("30-59 minutes")
  } else if (range > 60 && range <= 120) {
    return("60-119 minutes")
  } else if (range > 120 && range <= 180) {
    return("120-179 minutes")
  } else {
    return("180+ minutes")
  }
}

# Apply the function to create the range_description column
merged_data$range_description <- sapply(merged_data$range, generate_range_description); merged_data$range_description 

# If you need to save the spatial data with geometry, consider using st_write to save as a shapefile or GeoJSON
# Convert the geometries to POLYGON
st_write(merged_data, "data/06-isochrones/merged_data_sf.shp", append = FALSE)

# Clip the isochrones to the USA border.
usa_borders <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") %>%
  sf::st_set_crs(4326) %>%
  select(name, iso_a2, woe_id, woe_label, woe_name, latitude, longitude, postal)

# Convert geometry column to WKT format
merged_data$wkt_geometry <- sf::st_as_text(merged_data$geometry)

# Remove the original geometry column
merged_data <- dplyr::select(merged_data, -geometry)

# Convert your merged_data to sf object
isochrones_df <- st_as_sf(merged_data, 
                           wkt = "wkt_geometry",  # Specify the column containing WKT (well-known text ) geometry
                           crs = 4326)           # Specify the coordinate reference system (CRS), here using EPSG:4326

invisible(gc())
isochrones_sf_clipped <- sf::st_intersection(isochrones_df, usa_borders) %>%
  dplyr::arrange(desc(rank)) #This is IMPORTANT for the layering.
rm(isochrones_df)


# Truncate column names to 10 characters
names(isochrones_sf_clipped) <- substr(names(isochrones_sf_clipped), 1, 10)
anyDuplicated(names(isochrones_sf_clipped))

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
map <- leaflet::leaflet() %>%
  leaflet::addTiles() # Add the default map tiles

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
    popup = ~paste("NPI: ", NPI, "<br>Range: ", range, "<br>Date: ", departr)
  ); map

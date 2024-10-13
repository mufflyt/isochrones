# This R script meticulously orchestrates a sequence of operations focused on geospatial analysis, specifically the creation and handling of isochrones. These isochrones represent areas accessible within certain time limits from specific points, offering valuable insights into geographical accessibility. The script utilizes R's `tidyverse` packages and spatial data handling capabilities provided by the `sf` (simple features) package to manage and process geospatial data efficiently.
# 
# ### Initialization and Data Preprocessing
# The script initializes by setting a coordinate reference system (CRS) relevant for geographic computations and specifies paths for input and output data directories. It loads a pre-processed dataset (`goba_unrestricted.rds`) that contains extensive geographic information. The primary geocoded data from the batch process is loaded from a CSV file, which is then pruned to ensure that only entries with valid geographical coordinates are considered. This dataset is enriched by joining additional demographic and professional data related to medical practitioners, focusing on specific subspecialties.
# 
# ### Isochrone Creation and Error Handling
# A crucial part of the workflow involves generating isochrones using the HERE API, facilitated by the `hereR` package. The script prepares the data by assigning unique identifiers to each entry to maintain order and correspondence between requests and responses. The process is done in chunks to manage API limitations and to handle potential errors effectively. This methodical chunking helps in isolating and addressing errors without affecting the entirety of the dataset.
# 
# ### Spatial Data Management and Output
# After generating the isochrones, the script performs several spatial data manipulations—such as arranging the data by specified criteria, clipping the isochrones to the U.S. borders, and validating the geometries to correct any inconsistencies. These steps ensure that the spatial data is accurate and ready for further analysis or visualization. The script then saves the processed data in the ESRI Shapefile format, preserving the complex geometrical data structure required for detailed spatial analysis.
# 
# ### Visualization and Quality Assurance
# Towards the end, the script creates visual representations of the data using Leaflet maps, which allow for interactive exploration of the isochrones. These visualizations are augmented with data-driven pop-ups that provide additional information on demand. A final sanity check involves reading the clipped isochrones data, ensuring that the processed data aligns with expected median values, signifying the reliability of the isochrones in representing true geographic accessibility.
# 
# ### Concluding Steps
# The script concludes by saving the Leaflet map as an HTML file, facilitating easy sharing and presentation of the results. This comprehensive script not only demonstrates advanced capabilities in handling and analyzing geospatial data but also ensures that the results are accessible and informative, suitable for stakeholders needing detailed geographic insights.

# Required Input Files for Script Execution ----

# 1. Setup Script:
#    - R Script: "R/01-setup.R"

# 2. Input and Output Directories:
#    - Output Directory: "data/06-isochrones/"
#    - File Path: "data/04-geocode/geocoded_batch/end_rejoined_geocoded_data_nominatim.csv"

# 3. GOBA Unrestricted Data:
#    - RDS File: "data/06-isochrones/goba/goba_unrestricted.rds"

# 4. Geocoded Data:
#    - CSV File: "data/04-geocode/geocoded_batch/end_rejoined_geocoded_data_nominatim.csv"

# 5. Merged Data Output:
#    - CSV File: "data/06-isochrones/end_merged_data_isochrones_input_file.csv"

# 6. Shapefiles:
#    - Shapefile: "data/06-isochrones/merged_sf.shp"
#    - Shapefile: "data/06-isochrones/merged_data_sf.shp"
#    - Shapefile: "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp"

# 7. US States Data:
#    - GeoJSON Data: "state_data.csv"

# 8. Isochrones Clipped Output:
#    - Shapefile: "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp"

# 9. Leaflet Map Output:
#    - HTML File: "data/06-isochrones/map.html"


# Load setup script
#######################
source("R/01-setup.R")

# Define CRS and file paths
#######################
crs <- 4326
output_iso_path <- "data/06-isochrones/"
file_path_unique_address <- "end_rejoined_geocoded_data_nominatim_go_unique_address.csv"

# Define Isochrone ranges (in seconds)
#######################
iso_ranges <- c(30 * 60, 60 * 60, 120 * 60, 180 * 60) # 30 min, 60 min, 120 min, 180 min

# Load required datasets
#######################
goba_unrestricted <- readr::read_rds("data/06-isochrones/goba/goba_unrestricted.rds")

# Prepare unique address dataset and save
end_rejoined_geocoded_data_nominatim_go_unique_address <- readr::read_csv("data/04-geocode/end_rejoined_geocoded_data_nominatim_go.csv") %>%
  distinct(address, .keep_all = TRUE) %>%
  write_csv("end_rejoined_geocoded_data_nominatim_go_unique_address.csv")

end_rejoined_geocoded_data_nominatim_go_unique_address <- read_csv("end_rejoined_geocoded_data_nominatim_go_unique_address.csv")
file_path <- "end_rejoined_geocoded_data_nominatim_go_unique_address.csv"

# Validate the file of geocoded data.

##### To do the actual gathering of the isochrones: `process_and_save_isochrones`.  We do this in chunks of 25 because we were losing the entire searched isochrones when one error hit.  There is a 1:1 relationship between isochrones and rows in the `input_file` so to match exactly on row we need no errors.  Lastly, we save as a shapefile so that we can keep the MULTIPOLYGON geometry setting for the sf object making it easier to work with the spatial data in the future for plotting, etc.  I struggled because outputing the data as a dataframe was not easy to write it back to a MULTIPOLYGON.

# https://platform.here.com/
#######################
# Track API Calls & Cost Estimate
#######################
track_api_calls_and_cost(iso_ranges = iso_ranges, num_rows = nrow(read_csv(file_path_unique_address)))


# Generate a random seed to sample the input file randomly
#######################
random_seed <- sample.int(10000, 1)
set.seed(random_seed)


input_file <- 
  readr::read_csv(file_path) %>%
  dplyr::mutate(id = row_number()) %>% # creates a unique identifier number
  dplyr::distinct(address, .keep_all = TRUE) %>%
  dplyr::filter(!is.na(lat) & !is.na(long)) %>% # Exclude rows where lat or long is NA
  #dplyr::filter(sub1 == "ONC") %>%
  dplyr::mutate (id = seq_len(nrow(.))) %>% # creates a unique identifier number
  #dplyr::filter(State %in% c("CO")) %>% # & ploccityname == "AURORA") # For testing with a small sample.  ALL CAPS.  
  #dplyr::sample_n(size = 100) %>%
  exploratory::left_join(`goba_unrestricted`, by = join_by(`npi` == `NPI_goba`), target_columns = c("NPI_goba", "sub1", "sub1startDate", "Medical school namePhysicianCompare", "Graduation yearPhysicianCompare", "Number of Group Practice membersPhysicianCompare", "Professional accepts Medicare AssignmentPhysicianCompare", "num_org_mem", "honorrific_end")) %>%
  #dplyr::filter(sub1 %in% c("FPM", "MFM", "ONC", "REI")) %>%
  dplyr::distinct(address, .keep_all = TRUE) #%>%
  #dplyr::filter(sub1 == "ONC")
  # Unique addresses here.  To send to the HERE API.  
  #select(address, sub1, id, lat, long, pnmdtst) %>%
  #dplyr::sample_n(size = 100) 

#**********************************************
# TESTING ISOCHRONES WITH A ONE SECOND ISOCHRONE
# #**********************************************
# errors <- test_and_process_isochrones(input_file)
# errors
# 
# #Output from the `test_and_process_isochrones` function
# # Filter out the rows that are going to error out after using the test_and_process_isochrones function.
# error_rows <- c()
# 
# # filter out rows with errors.
# input_file_no_error_rows <- input_file %>%
#   dplyr::filter(!id %in% error_rows)

input_file_no_error_rows <- input_file 
dim(input_file)

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

# Create the master key data frame
master_key <- data.frame(
  input_file_no_error_rows_id = input_file_no_error_rows$id,
  isochrones_sf_id = seq_len(nrow(input_file_no_error_rows))
) %>%
  distinct(input_file_no_error_rows_id, .keep_all = TRUE) %>%
  rename("id" = "input_file_no_error_rows_id")

conflicted::conflicts_prefer(base::ceiling)

# Define parameters
iso_file_path_prefix <- "data/06-isochrones/isochrones_"
chunk_size <- 25
iso_ranges <- c(30 * 60, 60 * 60, 120 * 60, 180 * 60)
transport_mode <- "car"

# Define cache filesystem
fc <- cache_filesystem(file.path(".cache"))

# Process and Save Isochrones Function
conflicted::conflicts_prefer(base::setdiff)
isochrones_sf <- process_and_save_isochrones(
  input_file = input_file_no_error_rows,
  chunk_size = chunk_size,
  iso_ranges = iso_ranges,
  crs = crs,
  iso_datetime_yearly = iso_datetime_yearly,
  transport_mode = transport_mode,
  file_path_prefix = iso_file_path_prefix
)

#Arranging the data by the rank column ensures that the layers are plotted in the correct order, with higher-ranked layers plotted on top of lower-ranked ones. This is indeed important for proper layering in the plot. 
isochrones_sf <- isochrones_sf %>%
  dplyr::arrange(desc(rank))
plot(isochrones_sf["range"])

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

# write_csv(input_file_no_error_rows, "data/06-isochrones/input_file_no_error_rows.csv")
# write_csv(isochrones_sf, "data/06-isochrones/isochrones_sf.csv")
# write_csv(master_key, "data/06-isochrones/master_key.csv")

# Merge based on the corresponding IDs in the master key
merged_data <- input_file_no_error_rows %>%
  dplyr::left_join(master_key, by = c("id" = "id")) %>%
  dplyr::left_join(isochrones_sf, by = c("isochrones_sf_id" = "id")); merged_data

# Clean up the merged data (optional)
merged_data <- merged_data[, !(names(merged_data) %in% c("id.x", "id.y"))] # Remove duplicate ID columns

merged_data <- merged_data %>%
  dplyr::mutate(range = range/60L)
#View(merged_data)

# Check the merged data
glimpse(merged_data)
class(merged_data)
write_csv(merged_data, "data/06-isochrones/end_merged_data_isochrones_input_file.csv")

# Apply the function to create the range_description column
merged_data$range_description <- sapply(merged_data$range, generate_range_description); merged_data$range_description 

# Convert dataframe to sf object
merged_sf <- st_as_sf(merged_data, 
                      coords = c("long", "lat"),
                      crs = crs)

# Write sf object to shapefile
st_write(merged_sf, "data/06-isochrones/merged_sf.shp", 
         append = FALSE)

# Plot merged data
us_states <- ne_states(country = "united states of america", returnclass = "sf") %>%
  dplyr::filter(!name %in% c("Alaska", "Hawaii"))

ggplot() +
  geom_sf(data = merged_sf, color = "red", alpha = 0.5) +
  geom_sf(data = us_states, fill = NA, color = "black") + # Overlay US shapefile
  theme_minimal()

st_write(merged_data, "data/06-isochrones/merged_data_sf.shp", append = FALSE)

# Clip the isochrones to the USA border.
usa_borders <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") %>%
  sf::st_set_crs(crs) %>%
  select(name, iso_a2, woe_id, woe_label, woe_name, latitude, longitude, postal)

merged_data$wkt_geometry <- sf::st_as_text(merged_data$geometry)
merged_data <- dplyr::select(merged_data, -geometry)


isochrones_df <- st_as_sf(merged_data, wkt = "wkt_geometry", crs = crs)
isochrones_sf_clipped <- st_intersection(isochrones_df, usa_borders) %>%
  arrange(desc(rank)) %>%
  st_make_valid()

plot(isochrones_sf_clipped["sub1"])
plot(isochrones_sf_clipped["range"])

# Write clipped isochrones to shapefile
st_write(isochrones_sf_clipped, dsn = file.path(output_iso_path, "end_isochrones_sf_clipped"), 
         layer = "isochrones", driver = "ESRI Shapefile", quiet = FALSE, append = FALSE)


invisible(gc())
#rm(isochrones_df)


######################################
# SANITY CHECK
######################################

# Define file path
iso_file_path <- "data/06-isochrones/end_isochrones_sf_clipped"

# Read clipped isochrones data and filter by median pnmdtst
end_isochrones_sf_clipped <- st_read(iso_file_path) %>%
  arrange(desc(rank)) %>%
  dplyr::filter(pnmdtst == median(pnmdtst))

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
    popup = ~paste("NPI: ", npi, "<br>Range: ", range, "<br>Date of Data: ", departr)
  ); map

# Save the map as an HTML file
htmlwidgets::saveWidget(map, paste0(output_iso_path, "map.html"))


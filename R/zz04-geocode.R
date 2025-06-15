
#######################
# This script loads provider data, geocodes addresses, and summarizes counts by state.
source("R/01-setup.R")
#######################

#The purpose of this code is to geocode the addresses of clinician data using the HERE geocoding service. It starts by reading a CSV file containing clinician data, combines address components into a single address field, and then writes this data to a new CSV file for geocoding. After geocoding, the resulting geocoded data is saved as a separate CSV file, providing geographic coordinates for each clinician's address.

#Provenance: GOBA file.  
readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>%
  tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  #head(10) %>% #for testing.
  readr::write_csv(., "data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") -> a

a$address

#**************************
#* GEOCODE THE DATA USING HERE API.  The key is hard coded into the function.  
#**************************
csv_file <- "data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv"

geocoded_data <- create_geocode(csv_file)
write_csv(geocoded_data, "data/04-geocode/end_completed_clinician_data_geocoded_addresses_12_8_2023.csv")
# geocoded_data <- readr::read_csv("data/04-geocode/end_completed_clinician_data_geocoded_addresses_12_8_2023.csv") #for testing


#**********************************************
# SANITY CHECK
#**********************************************
mean(geocoded_data$score) #accuracy of the geocode

# Found in the isochrones/ path.  
state_data <- read_csv("results/state_data.csv")

# Step 1: Aggregate your data by state_code and subspecialist count
state_data <- geocoded_data %>%
  group_by(state_code) %>%
  summarize(count = n())

# Step 2: Get the GeoJSON data for U.S. states from 'rnaturalearth'
us_states <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")

#TODO: take a look at this.  
merged_data <- state_data %>%
  exploratory::left_join(`us_states`, by = join_by(`state_code` == `postal`), target_columns = c("postal", "geometry"))

class(us_states$postal)
class(state_data$state_code)

# TODO:  This throws an error:  Error in as(merged_data, "Spatial") : 
#no method or default for coercing “tbl_df” to “Spatial”

# Convert merged_data to SpatialPolygonsDataFrame
merged_data_sp <- as(merged_data, "Spatial") 

# TODO: Does not work with sf
# Replace 'geometry' with your actual geometry column name
# Specify the correct geometry type and CRS
# merged_data_sf <- st_as_sf(merged_data, wkt = "geometry", crs = "+proj=longlat +datum=WGS84 +no_defs", agr = "constant")

# Create the chloropleth map
map <- leaflet(data = merged_data_sp) %>%
  addTiles() %>%
  addPolygons(
    data = merged_data_sp,
    fillColor = ~colorQuantile("YlOrRd", count)(count),
    fillOpacity = 0.7,
    color = "white",
    weight = 1,
    highlight = highlightOptions(
      weight = 2,
      color = "black",
      bringToFront = TRUE
    ),
    label = ~paste(name, "OBGYN Subspecialist Count: ", count)
  ) %>%
  addLegend(
    "bottomright",
    pal = colorQuantile("YlOrRd", domain = merged_data_sp$count),
    values = ~count,
    title = "Subspecialist Count"
  )

# Display the map
map


# The following function uses the Nominatim API for geocoding and returns a list object with 2 sublists:
# 
# - wo_geocode: will be return a message or will include the rows where the 'lat', 'long' did not have values (NA's)
# - geocode: will be return a message or will include an 'sf' object with the additional columns 'lat', 'long'
#
# If the 'geocode' sublist is not empty then a .csv file will be saved based on the data.frame (not the 'sf' object)

#######################
source("R/01-setup.R")
#######################

# Huge thanks to Lampros for this work.  

#Provenance: GOBA file.  
readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>%
  tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  #head(10) %>% #for testing.
  readr::write_csv(., "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") -> a

a$address

# For testing
#read_csv("data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") %>% head(10) %>% write_csv("data/04-geocode/SHORT_for_street_matching_with_HERE_results_clinician_data.csv")

# Example usage:
csv_file <- "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv"
output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"
ACOG_Districts <- tyler::ACOG_Districts

#**********************************************
# GEOCODING FUNCTION
#**********************************************
create_geocode_nominatim(csv_file, output_file)
# str(geocoded_data)
geocoded_data <- readr::read_csv(output_file) %>%
  rename ("State" = "state") %>%
  left_join(ACOG_Districts, by = "State") %>%
  write_csv(output_file)

#**********************************************
# SANITY CHECK
#**********************************************
# Convert the list to a data frame
# Extract the 'geocode' component from the list
geocoded_data_geocode <- geocoded_data

# Check if it's not "There are no missing values"
if (!is.character(geocoded_data_geocode)) {
  # Convert the 'geocode' component to a data frame
  geocoded_data_df <- as.data.frame(geocoded_data_geocode)
  
  # Check the class of the resulting data frame
  class(geocoded_data_df)
} else {
  message("The 'geocode' component contains character values: There are no missing values")
}

class(geocoded_data_geocode)

# Found in the isochrones/ path.  
state_data <- read_csv("state_data.csv")

# Step 1: Aggregate your data by state_code and subspecialist count
state_data <- geocoded_data_geocode %>%
  group_by(State) %>%
  summarize(count = n()) %>%
  arrange(desc(count)); state_data

# Step 2: Get the GeoJSON data for U.S. states from 'rnaturalearth'
us_states <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") 

merged_data <- state_data %>%
  exploratory::left_join(`us_states`, by = join_by(`State` == `postal`), target_columns = c("postal", "geometry"))


#**********************************************
# SANITY CHECK
#**********************************************
# Define the number of ACOG districts
num_acog_districts <- 11

# Create a custom color palette using viridis.  I like using the viridis palette because it is ok for color blind folks.  
district_colors <- viridis::viridis(num_acog_districts, option = "viridis")

# Generate ACOG districts with geometry borders in sf using tyler::generate_acog_districts_sf()
acog_districts_sf <- tyler::generate_acog_districts_sf()

leaflet::leaflet(data = geocoded_data) %>%
  leaflet::addPolygons(
    data = acog_districts_sf,
    color = district_colors[as.numeric(geocoded_data$ACOG_District)],      # Boundary color
    weight = 2,         # Boundary weight
    fill = TRUE,       # No fill
    opacity = 0.1,      # Boundary opacity
    popup = ~acog_districts_sf$ACOG_District) %>%
  leaflet::addCircleMarkers(
    data = geocoded_data,
    lng = ~long,
    lat = ~lat,
    radius = 3,         # Adjust the radius as needed
    stroke = TRUE,      # Add a stroke (outline)
    weight = 1,         # Adjust the outline weight as needed
    color = district_colors[as.numeric(geocoded_data$ACOG_District)],   # Set the outline color to black
    fillOpacity = 0.8,  # Fill opacity
    popup = ~address)  # Popup text based on popup_var argument
   
  # Add ACOG district boundaries
   # Popup text # %>%
  #Add a legend
  # leaflet::addLegend(
  #   position = "bottomright",   # Position of the legend on the map
  #   colors = district_colors,   # Colors for the legend
  #   labels = levels(geocoded_data$ACOG_District),   # Labels for legend items
  #   title = "ACOG Districts"   # Title for the legend
  # )

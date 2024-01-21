
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
  readr::write_csv(., "data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") -> a

a$address

# For testing
#read_csv("data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") %>% head(10) %>% write_csv("data/04-geocode/SHORT_for_street_matching_with_HERE_results_clinician_data.csv")

# Example usage:
csv_file <- "data/04-geocode/SHORT_for_street_matching_with_HERE_results_clinician_data.csv"
output_file <- "data/SHORT_04-geocode/geocoded_addresses.csv"
geocoded_data <- create_geocode_nominatim(csv_file, output_file)
# str(geocoded_data)

#**********************************************
# SANITY CHECK
#**********************************************
# Convert the list to a data frame
# Extract the 'geocode' component from the list
geocoded_data_geocode <- geocoded_data$geocode

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
  group_by(state) %>%
  summarize(count = n())

# Step 2: Get the GeoJSON data for U.S. states from 'rnaturalearth'
us_states <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") %>%
  as.data.frame()

merged_data <- state_data %>%
  exploratory::left_join(`us_states`, by = join_by(`state` == `postal`), target_columns = c("postal", "geometry"))

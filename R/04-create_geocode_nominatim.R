
# The following function uses the Nominatim API for geocoding and returns a list object with 2 sublists:
# 
# - wo_geocode: will be return a message or will include the rows where the 'lat', 'long' did not have values (NA's)
# - geocode: will be return a message or will include an 'sf' object with the additional columns 'lat', 'long'
#
# If the 'geocode' sublist is not empty then a .csv file will be saved based on the data.frame (not the 'sf' object)

#######################
source("R/01-setup.R")
#######################

#Provenance: GOBA file.  
readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>%
  tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  #head(10) %>% #for testing.
  readr::write_csv(., "data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") -> a

a$address


# geocoding based on Nominatim
create_geocode_nominatim <- function(csv_file, output_file) {

  # Check if the CSV file exists
  if (!file.exists(csv_file)) {
    stop("CSV file not found.")
  }
  
  # Read the CSV file into a data frame
  data <- read.csv(csv_file)
  
  # Check if the data frame contains a column named "address"
  if (!"address" %in% colnames(data)) {
    stop("The CSV file must have a column named 'address' for geocoding.")
  }
  
  # We have to rename the 'address' column to 'addr' because for some reason we receive the error:
  # "Error: Do not use other address component parameters with the single line 'address' parameter"
  idx_addr = which(colnames(data) == 'address')
  colnames(data)[idx_addr] = 'addr'
  
  res_geoc = tidygeocoder::geocode(
    .tbl = data, 
    method = 'osm',                       # uses: Nominatim, url:  https://nominatim.org  
    address = addr,
    lat = "lat",                          # the name of the latitude column
    long = "long",                        # the name of the longitude column
    limit = 1,                            # maximum number of results to return per input address
    return_input = TRUE,
    progress_bar = TRUE,
    quiet = FALSE
  )
  
  # use the same column name as before
  idx_addr = which(colnames(res_geoc) == 'addr')
  colnames(res_geoc)[idx_addr] = 'address'

  # create a list to save 2 sublists:
  # - wo_geocode: will be either NULL or will include the rows where the 'lat', 'long' did not have values (NA's)
  # - geocode: will be either NULL or will include an 'sf' object with the additional columns 'lat', 'long'
  lst_out = list()
  
  idx_nan = which(is.na(res_geoc$lat) | is.na(res_geoc$long))
  if (length(idx_nan) > 0) {
    
    # appned the without geocode observations to the list
    lst_out[['wo_geocode']] = res_geoc[idx_nan, , drop = F]
    
    # overwrite the previous tibble object
    res_geoc = res_geoc[-idx_nan, , drop = F]
  } else {
    lst_out[['wo_geocode']] = 'There are no missing values'
  }
  
  if (nrow(res_geoc) > 0) {
    
    # Save the geocoded data to an output file
    write.csv(res_geoc, file = output_file, row.names = FALSE)
    
    # convert to an 'sf' object
    res_geoc_sf = sf::st_as_sf(res_geoc, coords = c('long', 'lat'), crs = 4326)
    lst_out[['geocode']] = res_geoc_sf
  }  else {
    lst_out[['geocode']] = 'The tidygeocoder::geocode() function returned only NA values'
  }

  return(lst_out)
}

# For testing
#read_csv("data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv") %>% head(10) %>%
 # write_csv("data/04-geocode/SHORT_for_street_matching_with_HERE_results_clinician_data.csv")

# Example usage:
csv_file <- "data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv"
output_file <- "data/04-geocode/geocoded_addresses.csv"
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

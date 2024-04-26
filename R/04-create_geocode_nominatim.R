
# The following function uses the Nominatim API for geocoding and returns a list object with 2 sublists:
# 
# - wo_geocode: will be return a message or will include the rows where the 'lat', 'long' did not have values (NA's)
# - geocode: will be return a message or will include an 'sf' object with the additional columns 'lat', 'long'
#
# If the 'geocode' sublist is not empty then a .csv file will be saved based on the data.frame (not the 'sf' object)

#######################
source("R/01-setup.R")
#######################

states <- c("Alaska", "Alabama", "American Samoa", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
            "Delaware", "District of Columbia", "Florida", "Georgia", "Guam", "Hawaii", "Idaho", "Illinois", 
            "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", 
            "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
            "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Northern Mariana Islands", 
            "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", 
            "South Dakota", "Tennessee", "Texas", "U.S. Virgin Islands", "Utah", "Vermont", "Virginia", "Washington", 
            "West Virginia", "Wisconsin", "Wyoming")
  
# Huge thanks to Lampros for this work.  

#Provenance: GOBA file.  
#readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>% #This is nice because it is merged with the NPPES file so there is more address data there.  

#TODO make specific to the year of the data.  
a <- readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") %>%
  tidyr::unite(address, `Provider First Line Business Practice Location Address.y`, city_goba, state_goba, `Provider Business Practice Location Address Postal Code.y`, sep = ", ", remove = FALSE, na.rm = TRUE) %>%
  select(-lat, -long) %>%
  distinct(address, .keep_all = TRUE) %>%
  readr::write_csv(., "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv")


readr::read_csv("data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") %>% head(5) %>%
  write_csv("data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv")

ACOG_Districts <- tyler::ACOG_Districts

#**********************************************
# PREGEOCODING DATA QUALITY CHECK
#**********************************************
csv_file <- "data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv"

# Read the original CSV data
original_data <- read_csv(csv_file)

# Filter rows based on the State column
unmatched <- read_csv(csv_file) %>%
  rename(State = state_goba) %>%
  filter(State %nin% states)

input_lat_long <- read_csv(csv_file) %>%
  distinct(npi, .keep_all = TRUE) 

# Display the count of unmatched rows
paste0("There were ", nrow(input_lat_long), " distinct NPI numbers represented in the data BEFORE it was geocoded. There are ", nrow(unmatched), " rows that will not able to be geocoded. Mainly because they have NA for the state and are out of the country.")

#**********************************************
# GEOCODING FUNCTION
#**********************************************
simple_create_geocode_nominatim <- function(csv_file, output_file) {
  # Check if the CSV file exists
  if (!file.exists(csv_file)) {
    stop("Error: CSV file not found.")
  }
  
  cat(sprintf("Reading CSV file...\n"))
  
  # Read the CSV file into a data frame
  data <- read.csv(csv_file)

  # Check if the data frame contains a column named "address"
  if (!"address" %in% colnames(data)) {
    stop("Error: The CSV file must have a column named 'address' for geocoding.")
  }
  
  cat(sprintf("Geocoding %d addresses using Nominatim...\n", nrow(data)))
  
  data$address
  
  # Geocode the addresses using Nominatim
  res_geoc <- tidygeocoder::geocode(
    .tbl = data, 
    method = 'cascade', #'osm'                       
    address = 'address',                       
    lat = lat,                          
    long = long,                        
    limit = 1,                            
    return_input = TRUE,                  
    progress_bar = TRUE,                 
    quiet = FALSE                         
  )
  
  cat(sprintf("Geocoded %d addresses.\n", nrow(res_geoc)))
  
  # Save the geocoded data to an output CSV file
  write_csv(res_geoc, file = output_file)
  
  cat(sprintf("Saved the geocoded data to %s.\n", output_file))
  
  return(invisible(res_geoc))
}

csv_file <- "data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv"
output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"
geocoded_results_df <- simple_create_geocode_nominatim(csv_file, output_file)

# Left join the original CSV data with the geocoded data based on the "address" column
joined_data <- original_data %>%
  dplyr::left_join(dplyr::select(geocoded_results_df, address, lat, long), by = "address") %>%
  dplyr::rename ("State" = "state_goba") %>%
  dplyr::left_join(ACOG_Districts, by = "State") %>%
  filter(State %in% states) %>%
  readr::write_csv(output_file)

paste0("There were ", nrow(joined_data), " rows where the state column MATCHED CORRECTLY to a US state name.")

#**********************************************
# QUALITY CHECK
#**********************************************
input_addresses <- input_lat_long; nrow(input_addresses) 

output_lat_long <- read_csv(output_file) %>%
  distinct(npi, .keep_all = TRUE)

#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Missing lat and longitude 
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
output_rows_with_missing_coords <- output_lat_long %>%
  filter(is.na(lat) | is.na(long)); nrow(output_rows_with_missing_coords) 

paste0("There were ", nrow(output_lat_long), " distinct NPI numbers represented in the data AFTER it was geocoded.  There were ", nrow(output_rows_with_missing_coords), " observations with NA in the latitude of longitude.") 

glimpse(output_rows_with_missing_coords)

#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Try different methods to geocode the data.  
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####

# Combine city_goba and State into a single column
output_rows_with_missing_coords <- output_rows_with_missing_coords %>%
  as.data.frame() %>%
  mutate(location = paste(city_goba, State, sep = ", "))

# Check if output_rows_with_missing_coords is empty
if (nrow(output_rows_with_missing_coords) == 0) {
  # Handle case where output_rows_with_missing_coords is empty
  print("No data found in output_rows_with_missing_coords.")
} else {
  # Mutate the data if output_rows_with_missing_coords is not empty
  geocoded_data <- output_rows_with_missing_coords %>%
    mutate(location = paste(city_goba, State, sep = ", ")) %>%
    mutate(geocoded = tidygeocoder::geocode(location, method = "osm")) %>%
    unnest(cols = c(geocoded))
}

# Check the resulting dataframe
glimpse(geocoded_data)

# Compare the number of rows in the input and output datasets
nrow(input_addresses)
nrow(output_lat_long)

missing_addresses <- dplyr::anti_join(input_addresses, output_lat_long, by = c("npi"="npi"))
glimpse(missing_addresses)

#**********************************************
# SANITY CHECK
#**********************************************
# Convert the list to a data frame
# Extract the 'geocode' component from the list
geocoded_data_geocode <- joined_data
geocoded_data <- joined_data

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
  exploratory::left_join(`us_states`, by = join_by(`State` == `name`), target_columns = c("name", "geometry"))


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


csv_file <- "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv"
output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"

# Calculate the required values
input_addresses <- read_csv(csv_file) %>% 
  filter(sub1 == "ONC") %>%
  nrow(.); input_addresses 

unique_addresses <- readr::read_csv("data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") %>%
  filter(sub1 == "ONC") %>%
  distinct(address) %>%
  nrow(.); unique_addresses 

output_lat_long <- read_csv(output_file) %>%                    
  filter(!is.na(lat) | !is.na(long)) %>%
  filter(sub1 == "ONC") %>%
  distinct(address) %>%
  nrow(.); output_lat_long

unique_address_excluded <- unique_addresses - output_lat_long

# Flow chart of the geocoding process
# Set the values for each label
a1 <- paste0('Number of\n Gynecologic Oncologists\n to be Geocoded,\n n = ', format(input_addresses, big.mark = ","))
b1 <- ''
c1 <- ''
d1 <- paste0('Number of Unique Gynecologic\nOncologist Addresses,\n n = ', format(unique_addresses, big.mark = ","))
e1 <- paste0('Unique Addresses Geocoded,\nn = ', format(output_lat_long, big.mark = ","))
a2 <- ''
b2 <- paste0('Using address, city, state\n with tidygeocoder')
c2 <- paste0('Excluded because\nUnable to Geocode,\nn = ', format(unique_address_excluded, big.mark = ","))
d2 <- ''
e2 <- ''

# Create a node dataframe
ndf <- create_node_df(
  n = 10,
  label = c(a1, b1, c1, d1, e1, a2, b2, c2, d2, e2),
  style = c('solid', 'invis', 'invis', 'solid', 'solid', 'invis', 'solid', 'solid', 'invis', 'invis'),
  shape = c('box', 'point', 'point', 'box', 'box', 'plaintext', 'box', 'box', 'point', 'point'),
  width = c(3, 0.001, 0.001, 3, 3, 2, 2.5, 2.5, 0.001, 0.001),
  height = c(1, 0.001, 0.001, 1, 1, 1, 1, 1, 0.001, 0.001),
  fontsize = rep(14, 10),
  fontname = rep('Helvetica', 10),
  penwidth = 1.5,
  fixedsize = 'true'
)

# Create an edge dataframe
edf <- create_edge_df(
  from = c(1, 2, 3, 4, 6, 7, 8, 9, 2),  # Include c2 in the 'from' values
  to = c(2, 3, 4, 5, 7, 8, 9, 10, 8),     # Include c2 in the 'to' values
  arrowhead = c('none', 'none', 'normal', 'normal', 'none', 'none', 'none', 'none', 'normal'),  # Adjusted the arrowhead values
  color = rep(c('black', '#00000000'), each = 4),
  constraint = c(rep('true', 8), rep('false', 1))  # Adjusted the constraint length
)

# Create the graph
g <- DiagrammeR::create_graph(ndf, edf, attr_theme = NULL)

# Render and export the graph
DiagrammeR::render_graph(g)
export_graph(g, file_name = "data/04-geocode/geocode_flowchart.png")


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
#readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>% #This is nice because it is merged with the NPPES file so there is more address data there.  

asdf <- readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv")
names(asdf)


readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") %>%
  tidyr::unite(address, `Organization legal namePhysicianCompare`, `Provider First Line Business Practice Location Address.y`, city_goba, state_goba, `Provider Business Practice Location Address Postal Code.y`, sep = ", ", remove = FALSE, na.rm = TRUE) %>%
  head(50) %>% #for testing.
  readr::write_csv(., "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") -> a

a$address

# Example usage:
csv_file <- "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv"
output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"
ACOG_Districts <- tyler::ACOG_Districts

#**********************************************
# GEOCODING FUNCTION
#**********************************************
simple_create_geocode_nominatim <- function(csv_file, output_file) {
  csv_file <- "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv"
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
    method = 'osm',                       
    address = 'address',                       
    lat = "lat",                          
    long = "long",                        
    limit = 1,                            
    return_input = TRUE,                  
    progress_bar = TRUE,                 
    quiet = FALSE                         
  )
  
  cat(sprintf("Geocoded %d addresses.\n", nrow(res_geoc)))
  
  # Save the geocoded data to an output CSV file
  write.csv(res_geoc, file = output_file, row.names = FALSE)
  
  cat(sprintf("Saved the geocoded data to %s.\n", output_file))
  
  return(invisible(res_geoc))
}


df <- simple_create_geocode_nominatim(csv_file, output_file)


str(geocoded_data)
geocoded_data <- readr::read_csv(output_file) %>%
  rename ("State" = "state") %>%
  left_join(ACOG_Districts, by = "State") %>%
  write_csv(output_file)

#**********************************************
# QUALITY CHECK
#**********************************************
# Filter the rows where either lat or long is missing
# Read the output file
input_lat_long <- read_csv(csv_file) %>%
  distinct(npi, .keep_all = TRUE) 

input_addresses <- input_lat_long; nrow(input_addresses) 
#6,655

output_lat_long <- read_csv(output_file) %>%
  distinct(NPI, .keep_all = TRUE)
#3811

output_rows_with_missing_coords <- output_lat_long %>%
  filter(is.na(lat) | is.na(long)); nrow(output_rows_with_missing_coords) 

# Compare the number of rows in the input and output datasets
nrow(input_addresses)
nrow(output_lat_long)

missing_addresses <- anti_join(input_addresses, output_lat_long, by = c("npi"="NPI"))
glimpse(missing_addresses)

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


csv_file <- "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv"
output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"

# Calculate the required values
input_addresses <- read_csv(csv_file) %>% nrow(.); input_addresses. #6,655
output_lat_long <- read_csv(output_file) %>%                        #3,811
  filter(!is.na(lat) | !is.na(long)) %>%
  nrow(.)

# Filter the rows where either lat or long is missing
rows_with_missing_coords <- output_lat_long %>%
  filter(is.na(lat) | is.na(long))

View(read_csv(output_file))

No_match <- read_csv(output_file) %>%                        #3,811
  filter(is.na(lat) | is.na(long))

# Set the values for each label
a1 <- paste0('Number of all OBGYNs\nfor all years, n = ', format(all_docs, big.mark = ","))
b1 <- ''
c1 <- ''
d1 <- paste0('Number of Gynecologic\nOncologists included\nfor analysis, n = ', format(subspecialists, big.mark = ","))
e1 <- paste0('Number of Gynecologic\nOncologists with\nNPPES Demographics,\nn = ', format(nrow(onc_with_demographics), big.mark = ","))
a2 <- ''
b2 <- paste0('Excluded because\nGeneral OBGYN,\nn = ', format(excluded_bc_generalists, big.mark = ","))
c2 <- paste0('Non-Gynecologic\nOncology Subspecialist\nn = ', format(non_onc_subspecialists, big.mark = ","))
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
  from = c(1, 2, 3, 4, 6, 7, 8, 9, 2, 3),
  to = c(2, 3, 4, 5, 7, 8, 9, 10, 7, 8),
  arrowhead = c('none', 'none', 'normal', 'normal', 'none', 'none', 'none', 'none', 'normal', 'normal'),
  color = rep(c('black', '#00000000'), each = 4),
  constraint = c(rep('true', 8), rep('false', 2))
)

# Create the graph
g <- DiagrammeR::create_graph(ndf, edf, attr_theme = NULL)

# Render and export the graph
DiagrammeR::render_graph(g)
export_graph(g, file_name = "data/02.5-subspecialists_over_time/flowchart.png")

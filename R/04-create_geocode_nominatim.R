# The provided R script focuses on data preparation, geocoding, and quality control for a dataset concerning subspecialists, potentially from a medical or similar professional database. The script includes several steps that are crucial for ensuring the data is usable for further analysis, particularly in geographic and location-based studies.
# 
# **Data Preparation and Setup:**
#   The script starts by loading necessary libraries and the initial setup file which configures the environment. It defines a vector of states, which includes territories of the United States as well. This array is later used to filter and validate data. The script reads a CSV file containing detailed addresses of medical subspecialists, unites several address-related columns into a single column to facilitate geocoding, and eliminates unnecessary columns like latitude and longitude coordinates, which might be re-derived from geocoding.
# 
# **Geocoding Preparation:**
#   Before proceeding with geocoding, the script performs a data quality check by ensuring the addresses are unique and saving this preprocessed data for geocoding. This preparation is crucial to reduce redundancy in the geocoding process, which can be time-consuming and resource-intensive.
# 
# **Geocoding Process:**
#   The script uses a memoised function to perform geocoding, which means results of the function calls are cached. This is particularly useful when dealing with large datasets or costly operations like geocoding. The script aims to read the cleaned data, checks for the existence of an 'address' column necessary for geocoding, and processes the data in batches using different geocoding services like Nominatim and ArcGIS. It is set up to handle errors and report on the progress of geocoding, including saving interim results periodically, which is essential for managing large datasets.
# 
# **Post-Geocoding Quality Control:**
#   After geocoding, the script performs several quality checks. It reads the geocoded data to check how many entries were successfully geocoded and identifies any entries with missing coordinates. The goal is to ensure high data quality and to prepare the dataset for further analytical tasks. The script also integrates this geocoded data with other relevant data, potentially enhancing the dataset with additional geographic or demographic information.
# 
# **Visualization and Further Analysis:**
#   The script is structured to possibly support further steps like data visualization or deeper analysis, evidenced by placeholders and comments that suggest integrating additional datasets or performing group-based summaries.

# Required Input Files for Script Execution ----

# 1. Subspecialists Data:
#    - CSV File: "data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds"

# 2. GOBA Unrestricted Cleaned Data:
#    - CSV File: "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv"

# 3. Geocoding Data for Nominatim Results:
#    - CSV File: "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv"
#    - CSV File: "data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv"

# 4. NBER Data:
#    - CSV File: "data/02.33-nber_nppes_data/end_sp_duckdb_npi_all.csv"

# 5. State Data for Sanity Check:
#    - CSV File: "state_data.csv"

# 6. Final Geocoded Data:
#    - CSV File: "data/04-geocode/end_sp_duckdb_npi_all_geocoded_data_nominatim.csv"
#    - CSV File: "data/04-geocode/end_rejoined_geocoded_data_nominatim.csv"
#    - CSV File: "data/04-geocode/end_geocoded_data_nominatim.csv"

# 7. Final Geocoded Data for Output:
#    - CSV File: "data/04-geocode/end_rejoined_geocoded_data_nominatim.csv"

# 8. Final Output:
#    - PNG File: "data/04-geocode/geocode_flowchart.png"


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

ACOG_Districts <- tyler::ACOG_Districts

#**********************************************
# PREGEOCODING DATA QUALITY CHECK
#**********************************************
csv_file <- "data/02.5-subspecialists_over_time/nber_all_collected_go_pre_geocode.csv"

# Read the original CSV data
original_data <- read_csv(csv_file)

conflicted::conflicts_prefer(dplyr::filter)

input_lat_long <- read_csv(csv_file) %>%
  distinct(npi, lastupdatestr, address, .keep_all = TRUE) 

#################
#GEOCODING
#################

conflicted::conflicts_prefer(base::setdiff)
conflicted::conflicts_prefer(base::ceiling)
conflicted::conflicts_prefer(dplyr::bind_rows)
# Memoize the function
simple_create_geocode_nominatim <- memoise(function(csv_file, output_file) {
  if (!file.exists(csv_file)) {
    stop("Error: CSV file not found.")
  }
  
  cat(sprintf("Reading CSV file...\n"))
  conflicted::conflicts_prefer(base::setdiff)
  conflicted::conflicts_prefer(base::ceiling)
  conflicted::conflicts_prefer(dplyr::bind_rows)
  
  data <- read.csv(csv_file) %>%
    distinct(address, .keep_all = TRUE) #%>%
  #head(20)
  
  if (!"address" %in% colnames(data)) {
    stop("Error: The CSV file must have a column named 'address' for geocoding.")
  }
  
  cat(sprintf("Geocoding %d addresses using Nominatim...\n", nrow(data)))
  
  # Define multiple queries for geocoding
  queries <- list(
    list(method = 'osm'),
    list(method = 'arcgis')
    # Add more queries as needed
  )
  
  # Split data into batches of size 10
  batches <- split(data, ceiling(seq_along(data$address) / 10))
  
  # Initialize empty list to store geocoded results
  geocoded_results <- list()
  
  # Geocode each batch and save results
  for (i in seq_along(batches)) {
    batch <- batches[[i]]
    res_geoc <- tidygeocoder::geocode_combine(
      .tbl = batch, 
      queries = queries,
      global_params = list(address = 'address'),
      return_list = FALSE,
      cascade = TRUE
    )
    geocoded_results[[i]] <- res_geoc
    
    # Save results every 10 addresses
    if (i %% 10 == 0) {
      batch_output_file <- sprintf("%s_batch_%d.csv", output_file, i)
      write_csv(res_geoc, file = batch_output_file)
      cat(sprintf("Saved batch %d to %s.\n", i, batch_output_file))
      Sys.sleep(1)
    }
  }
  
  # Combine results from all batches
  all_geocoded_results <- bind_rows(geocoded_results)
  
  # Save combined results
  write_csv(all_geocoded_results, file = output_file)
  
  cat(sprintf("Saved the geocoded data to %s.\n", output_file))
  
  return(invisible(all_geocoded_results))
})

# Example usage
csv_file <- "data/02.5-subspecialists_over_time/nber_all_collected_go_pre_geocode.csv"
output_file <- "data/04-geocode/end_sp_duckdb_nber_all_collected_go_pre_geocode_nominatim.csv"
geocoded_results_df <- simple_create_geocode_nominatim(csv_file, output_file)

# Read the output data
temp <- read_csv(output_file) %>%
  select(npi, pfname, plname, lat, long, query)
dim(temp)

data <- read_csv(csv_file)
paste0("The input file contained ", nrow(read_csv(csv_file)), "rows of unique addresses.")

# Plot the output data
ggplot(temp, aes(x = long, y = lat, color = query)) +
  geom_point() +
  labs(x = "Longitude", y = "Latitude", title = "Geocoded Locations") +
  theme_minimal()

# Left join the original CSV data with the geocoded data based on the "address" column
joined_data <- read_csv(csv_file) %>%
  dplyr::left_join(dplyr::select(geocoded_results_df, address, lat, long, query), by = "address") %>%
  dplyr::rename ("State" = "plocstatename") %>%
  #dplyr::left_join(ACOG_Districts, by = "State") %>%
  #filter(State %in% states) %>%
  readr::write_csv("data/04-geocode/end_rejoined_geocoded_data_nominatim_go.csv")

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

paste0("There were ", nrow(output_lat_long), " distinct NPI numbers represented in the data AFTER it was geocoded.  There were ", nrow(output_rows_with_missing_coords), " observations with NA in the latitude or longitude.") 

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


# 
# # The following function uses the Nominatim API for geocoding and returns a list object with 2 sublists:
# # 
# # - wo_geocode: will be return a message or will include the rows where the 'lat', 'long' did not have values (NA's)
# # - geocode: will be return a message or will include an 'sf' object with the additional columns 'lat', 'long'
# #
# # If the 'geocode' sublist is not empty then a .csv file will be saved based on the data.frame (not the 'sf' object)
# 
# #######################
# source("R/01-setup.R")
# #######################
# 
# states <- c("Alaska", "Alabama", "American Samoa", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
#             "Delaware", "District of Columbia", "Florida", "Georgia", "Guam", "Hawaii", "Idaho", "Illinois", 
#             "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", 
#             "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
#             "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Northern Mariana Islands", 
#             "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", 
#             "South Dakota", "Tennessee", "Texas", "U.S. Virgin Islands", "Utah", "Vermont", "Virginia", "Washington", 
#             "West Virginia", "Wisconsin", "Wyoming")
#   
# # Huge thanks to Lampros for this work.  
# 
# #Provenance: GOBA file.  
# #readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") %>% #This is nice because it is merged with the NPPES file so there is more address data there.  
# 
# #TODO make specific to the year of the data.  
# a <- readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") %>%
#   tidyr::unite(address, `Provider First Line Business Practice Location Address.y`, city_goba, state_goba, `Provider Business Practice Location Address Postal Code.y`, sep = ", ", remove = FALSE, na.rm = TRUE) %>%
#   select(-lat, -long) %>%
#   distinct(address, .keep_all = TRUE) %>%
#   readr::write_csv(., "data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv")
# 
# 
# readr::read_csv("data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") %>% head(5) %>%
#   write_csv("data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv")
# 
# ACOG_Districts <- tyler::ACOG_Districts
# 
# #**********************************************
# # PREGEOCODING DATA QUALITY CHECK
# #**********************************************
# csv_file <- "data/04-geocode/short_for_testing_for_geocoding_with_nominatim__results_clinician_data.csv"
# 
# # Read the original CSV data
# original_data <- read_csv(csv_file)
# 
# # Filter rows based on the State column
# unmatched <- read_csv(csv_file) %>%
#   rename(State = state_goba) %>%
#   filter(State %nin% states)
# 
# input_lat_long <- read_csv(csv_file) %>%
#   distinct(npi, .keep_all = TRUE) 
# 
# # Display the count of unmatched rows
# paste0("There were ", nrow(input_lat_long), " distinct NPI numbers represented in the data BEFORE it was geocoded. There are ", nrow(unmatched), " rows that will not able to be geocoded. Mainly because they have NA for the state and are out of the country.")
# 
# 
# 
# conflicted::conflicts_prefer(base::setdiff)
# conflicted::conflicts_prefer(base::ceiling)
# conflicted::conflicts_prefer(dplyr::bind_rows)
# # Memoize the function
# simple_create_geocode_nominatim <- memoise(function(csv_file, output_file) {
#   if (!file.exists(csv_file)) {
#     stop("Error: CSV file not found.")
#   }
#   
#   cat(sprintf("Reading CSV file...\n"))
#   conflicted::conflicts_prefer(base::setdiff)
#   conflicted::conflicts_prefer(base::ceiling)
#   conflicted::conflicts_prefer(dplyr::bind_rows)
#   
#   data <- read.csv(csv_file) %>%
#     distinct(address, .keep_all = TRUE) #%>%
#     #head(20)
#   
#   if (!"address" %in% colnames(data)) {
#     stop("Error: The CSV file must have a column named 'address' for geocoding.")
#   }
#   
#   cat(sprintf("Geocoding %d addresses using Nominatim...\n", nrow(data)))
#   
#   # Define multiple queries for geocoding
#   queries <- list(
#     list(method = 'osm'),
#     list(method = 'arcgis')
#     # Add more queries as needed
#   )
#   
#   # Split data into batches of size 10
#   batches <- split(data, ceiling(seq_along(data$address) / 10))
#   
#   # Initialize empty list to store geocoded results
#   geocoded_results <- list()
#   
#   # Geocode each batch and save results
#   for (i in seq_along(batches)) {
#     batch <- batches[[i]]
#     res_geoc <- tidygeocoder::geocode_combine(
#       .tbl = batch, 
#       queries = queries,
#       global_params = list(address = 'address'),
#       return_list = FALSE,
#       cascade = TRUE
#     )
#     geocoded_results[[i]] <- res_geoc
#     
#     # Save results every 10 addresses
#     if (i %% 10 == 0) {
#       batch_output_file <- sprintf("%s_batch_%d.csv", output_file, i)
#       write_csv(res_geoc, file = batch_output_file)
#       cat(sprintf("Saved batch %d to %s.\n", i, batch_output_file))
#     }
#   }
#   
#   # Combine results from all batches
#   all_geocoded_results <- bind_rows(geocoded_results)
#   
#   # Save combined results
#   write_csv(all_geocoded_results, file = output_file)
#   
#   cat(sprintf("Saved the geocoded data to %s.\n", output_file))
#   
#   return(invisible(all_geocoded_results))
# })
# 
# # Example usage
# csv_file <- "data/02.33-nber_nppes_data/end_sp_duckdb_npi_all.csv"
# output_file <- "data/04-geocode/end_sp_duckdb_npi_all_geocoded_data_nominatim.csv"
# geocoded_results_df <- simple_create_geocode_nominatim(csv_file, output_file)
# 
# # Read the output data
# temp <- read_csv(output_file) %>%
#   select(npi, pfname, plname, lat, long, query); print(temp, n=10000)
# 
# # Plot the output data
# ggplot(temp, aes(x = long, y = lat, color = query)) +
#   geom_point() +
#   labs(x = "Longitude", y = "Latitude", title = "Geocoded Locations") +
#   theme_minimal()
# 
# # Left join the original CSV data with the geocoded data based on the "address" column
# joined_data <- read_csv(csv_file) %>%
#   dplyr::left_join(dplyr::select(geocoded_results_df, address, lat, long, query), by = "address") %>%
#   dplyr::rename ("State" = "plocstatename") %>%
#   #dplyr::left_join(ACOG_Districts, by = "State") %>%
#   #filter(State %in% states) %>%
#   readr::write_csv("data/04-geocode/end_rejoined_geocoded_data_nominatim.csv")
# 
# paste0("There were ", nrow(joined_data), " rows where the state column MATCHED CORRECTLY to a US state name.")
# 
# #**********************************************
# # QUALITY CHECK
# #**********************************************
# input_addresses <- input_lat_long; nrow(input_addresses) 
# 
# output_lat_long <- read_csv(output_file) %>%
#   distinct(npi, .keep_all = TRUE)
# 
# #### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
# #####  Missing lat and longitude 
# #### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
# output_rows_with_missing_coords <- output_lat_long %>%
#   filter(is.na(lat) | is.na(long)); nrow(output_rows_with_missing_coords) 
# 
# paste0("There were ", nrow(output_lat_long), " distinct NPI numbers represented in the data AFTER it was geocoded.  There were ", nrow(output_rows_with_missing_coords), " observations with NA in the latitude of longitude.") 
# 
# glimpse(output_rows_with_missing_coords)
# 
# #### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
# #####  Try different methods to geocode the data.  
# #### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
# 
# # Combine city_goba and State into a single column
# output_rows_with_missing_coords <- output_rows_with_missing_coords %>%
#   as.data.frame() %>%
#   mutate(location = paste(city_goba, State, sep = ", "))
# 
# # Check if output_rows_with_missing_coords is empty
# if (nrow(output_rows_with_missing_coords) == 0) {
#   # Handle case where output_rows_with_missing_coords is empty
#   print("No data found in output_rows_with_missing_coords.")
# } else {
#   # Mutate the data if output_rows_with_missing_coords is not empty
#   geocoded_data <- output_rows_with_missing_coords %>%
#     mutate(location = paste(city_goba, State, sep = ", ")) %>%
#     mutate(geocoded = tidygeocoder::geocode(location, method = "osm")) %>%
#     unnest(cols = c(geocoded))
# }
# 
# # Check the resulting dataframe
# glimpse(geocoded_data)
# 
# # Compare the number of rows in the input and output datasets
# nrow(input_addresses)
# nrow(output_lat_long)
# 
# missing_addresses <- dplyr::anti_join(input_addresses, output_lat_long, by = c("npi"="npi"))
# glimpse(missing_addresses)
# 
# #**********************************************
# # SANITY CHECK
# #**********************************************
# # Convert the list to a data frame
# # Extract the 'geocode' component from the list
# geocoded_data_geocode <- joined_data
# geocoded_data <- joined_data
# 
# # Check if it's not "There are no missing values"
# if (!is.character(geocoded_data_geocode)) {
#   # Convert the 'geocode' component to a data frame
#   geocoded_data_df <- as.data.frame(geocoded_data_geocode)
#   
#   # Check the class of the resulting data frame
#   class(geocoded_data_df)
# } else {
#   message("The 'geocode' component contains character values: There are no missing values")
# }
# 
# class(geocoded_data_geocode)
# 
# # Found in the isochrones/ path.  
# state_data <- read_csv("state_data.csv")
# 
# # Step 1: Aggregate your data by state_code and subspecialist count
# state_data <- geocoded_data_geocode %>%
#   group_by(State) %>%
#   summarize(count = n()) %>%
#   arrange(desc(count)); state_data
# 
# # Step 2: Get the GeoJSON data for U.S. states from 'rnaturalearth'
# us_states <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf") 
# 
# merged_data <- state_data %>%
#   exploratory::left_join(`us_states`, by = join_by(`State` == `name`), target_columns = c("name", "geometry"))
# 
# 
# #**********************************************
# # SANITY CHECK
# #**********************************************
# # Define the number of ACOG districts
# num_acog_districts <- 11
# 
# # Create a custom color palette using viridis.  I like using the viridis palette because it is ok for color blind folks.  
# district_colors <- viridis::viridis(num_acog_districts, option = "viridis")
# 
# # Generate ACOG districts with geometry borders in sf using tyler::generate_acog_districts_sf()
# acog_districts_sf <- tyler::generate_acog_districts_sf()
# 
# leaflet::leaflet(data = geocoded_data) %>%
#   leaflet::addPolygons(
#     data = acog_districts_sf,
#     color = district_colors[as.numeric(geocoded_data$ACOG_District)],      # Boundary color
#     weight = 2,         # Boundary weight
#     fill = TRUE,       # No fill
#     opacity = 0.1,      # Boundary opacity
#     popup = ~acog_districts_sf$ACOG_District) %>%
#   leaflet::addCircleMarkers(
#     data = geocoded_data,
#     lng = ~long,
#     lat = ~lat,
#     radius = 3,         # Adjust the radius as needed
#     stroke = TRUE,      # Add a stroke (outline)
#     weight = 1,         # Adjust the outline weight as needed
#     color = district_colors[as.numeric(geocoded_data$ACOG_District)],   # Set the outline color to black
#     fillOpacity = 0.8,  # Fill opacity
#     popup = ~address)  # Popup text based on popup_var argument
#    
#   # Add ACOG district boundaries
#    # Popup text # %>%
#   #Add a legend
#   # leaflet::addLegend(
#   #   position = "bottomright",   # Position of the legend on the map
#   #   colors = district_colors,   # Colors for the legend
#   #   labels = levels(geocoded_data$ACOG_District),   # Labels for legend items
#   #   title = "ACOG Districts"   # Title for the legend
#   # )
# 
# 
# csv_file <- "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv"
# output_file <- "data/04-geocode/end_geocoded_data_nominatim.csv"
# 
# # Calculate the required values
# input_addresses <- read_csv(csv_file) %>% 
#   filter(sub1 == "ONC") %>%
#   nrow(.); input_addresses 
# 
# unique_addresses <- readr::read_csv("data/04-geocode/for_geocoding_with_nominatim__results_clinician_data.csv") %>%
#   filter(sub1 == "ONC") %>%
#   distinct(address) %>%
#   nrow(.); unique_addresses 
# 
# output_lat_long <- read_csv(output_file) %>%                    
#   filter(!is.na(lat) | !is.na(long)) %>%
#   filter(sub1 == "ONC") %>%
#   distinct(address) %>%
#   nrow(.); output_lat_long
# 
# unique_address_excluded <- unique_addresses - output_lat_long
# 
# # Flow chart of the geocoding process
# # Set the values for each label
# a1 <- paste0('Number of\n Gynecologic Oncologists\n to be Geocoded,\n n = ', format(input_addresses, big.mark = ","))
# b1 <- ''
# c1 <- ''
# d1 <- paste0('Number of Unique Gynecologic\nOncologist Addresses,\n n = ', format(unique_addresses, big.mark = ","))
# e1 <- paste0('Unique Addresses Geocoded,\nn = ', format(output_lat_long, big.mark = ","))
# a2 <- ''
# b2 <- paste0('Using address, city, state\n with tidygeocoder')
# c2 <- paste0('Excluded because\nUnable to Geocode,\nn = ', format(unique_address_excluded, big.mark = ","))
# d2 <- ''
# e2 <- ''
# 
# # Create a node dataframe
# ndf <- create_node_df(
#   n = 10,
#   label = c(a1, b1, c1, d1, e1, a2, b2, c2, d2, e2),
#   style = c('solid', 'invis', 'invis', 'solid', 'solid', 'invis', 'solid', 'solid', 'invis', 'invis'),
#   shape = c('box', 'point', 'point', 'box', 'box', 'plaintext', 'box', 'box', 'point', 'point'),
#   width = c(3, 0.001, 0.001, 3, 3, 2, 2.5, 2.5, 0.001, 0.001),
#   height = c(1, 0.001, 0.001, 1, 1, 1, 1, 1, 0.001, 0.001),
#   fontsize = rep(14, 10),
#   fontname = rep('Helvetica', 10),
#   penwidth = 1.5,
#   fixedsize = 'true'
# )
# 
# # Create an edge dataframe
# edf <- create_edge_df(
#   from = c(1, 2, 3, 4, 6, 7, 8, 9, 2),  # Include c2 in the 'from' values
#   to = c(2, 3, 4, 5, 7, 8, 9, 10, 8),     # Include c2 in the 'to' values
#   arrowhead = c('none', 'none', 'normal', 'normal', 'none', 'none', 'none', 'none', 'normal'),  # Adjusted the arrowhead values
#   color = rep(c('black', '#00000000'), each = 4),
#   constraint = c(rep('true', 8), rep('false', 1))  # Adjusted the constraint length
# )
# 
# # Create the graph
# g <- DiagrammeR::create_graph(ndf, edf, attr_theme = NULL)
# 
# # Render and export the graph
# DiagrammeR::render_graph(g)
# export_graph(g, file_name = "data/04-geocode/geocode_flowchart.png")

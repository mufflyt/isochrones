# Required Input Files for Script Execution

# 1. NBER Historical Downloads:
#    - Database File: "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"

# 2. NPPES Data Dissemination Files:
#    - CSV Files from "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/"
#      Examples include:
#      - "NPPES_Data_Disseminat_April_2010_npidata_20050523-20100208.csv"
#      - "NPPES_Data_Disseminat_April_2021_npidata_pfile_20050523-20210411.csv"
#      - Additional files for years up to 2024

# 3. Facility Affiliation Files:
#    - Directory: "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files"
#    - Note: Specific file names are used in the script but not detailed here.

# 4. Medicare Part D Prescribers Files:
#    - Directory: "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
#    - Files starting with "MUP_DPR_RY21_P04_V10_DY" for various years.

# 5. Open Payments Data:
#    - Directory: "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files"
#    - Example file pattern: "OP_DTL_GNRL_PGYR2020_P01182024" and similar for other years.


source("R/01-setup.R")
conflicted::conflicts_prefer(dplyr::left_join)
conflicted::conflicts_prefer(dplyr::filter)

# Define the original data
toy_df <- tribble(
  ~npi, ~lastupdatestr, ~penumdatestr, ~pfname, ~plname, ~address, ~plocline1, ~ploccityname, ~plocstatename, ~ploczip, ~ploctel, ~pmailline1, ~pmailcityname, ~pmailstatename, ~pmailzip, ~pgender, ~pcredential, ~ptaxcode1, ~ptaxcode2, ~soleprop,
  1528060639, 2005, 2005, "TY", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", NA, NA,
  1528060639, 2007, 2005, "MATT", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", "X", "X",
  1528060639, 2015, 2005, "STACY", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", NA, "N"
)


# Load the CSV file
dataframe_i_have <- toy_df

# Create a data frame with all years from 2005 to 2020
years <- tibble(year = 2005:2020)

# Expand data for all npi and all years, while bringing all columns along
dataframe_expanded <- dataframe_i_have %>%
  distinct(npi, .keep_all = TRUE) %>%
  expand(npi, nesting(pfname, plname, address, plocline1, ploccityname, plocstatename, ploczip, ploctel, pmailline1, pmailcityname, pmailstatename, pmailzip, pgender, pcredential, ptaxcode1, ptaxcode2, soleprop), year = 2005:2020)
View(dataframe_expanded)

# Define a function to assign lastupdatestr based on the year
assign_lastupdate <- function(npi, year, updates) {
  # Find the specific updates list for the given npi
  update_values <- updates %>% 
    filter(npi == !!npi) %>%
    pull(lastupdatestr) %>%
    unlist() %>%
    sort(decreasing = FALSE)
  
  # Determine the correct lastupdatestr for each year
  last_value <- NA_integer_
  for (update in update_values) {
    if (year >= update) {
      last_value <- update
    } else {
      break
    }
  }
  return(last_value)
}

# Apply the function to each row
dataframe_final <- dataframe_expanded %>%
  rowwise() %>%
  mutate(lastupdatestr = assign_lastupdate(npi, year, dataframe_i_have)) %>%
  ungroup() %>%
  arrange(npi, year)

print(dataframe_final, n=100)


























#####################################################################################
# Install and load the geosphere package if not already installed
if (!requireNamespace("geosphere", quietly = TRUE)) {
  install.packages("geosphere")
}
library(geosphere)

# Define the latitude and longitude coordinates for row 1 and row 3
row1_coords <- c(-95.458038987339, 29.039289000928)  # Latitude, Longitude for row 1
row3_coords <- c(-95.456838, 29.04013185)            # Latitude, Longitude for row 3

# Calculate the distance between the coordinates using the haversine formula
distance <- distHaversine(row1_coords, row3_coords)

# Print the distance
print(distance)



# Example data
data <- data.frame(
  npi = c(1003002627, 1003002627),
  ploccityname = c("LAKE JACKSON", "LAKE JACKSON"),
  State = c("TX", "TX"),
  lat = c(29.039289000928, 29.04013185),
  long = c(-95.458038987339, -95.456838)
)

# Install and load the geosphere package if not already installed
if (!requireNamespace("geosphere", quietly = TRUE)) {
  install.packages("geosphere")
}
library(geosphere)

# Define a function to calculate distances within each group
calculate_distances_within_group <- function(group_data) {
  # If there's only one row in the group, return NA
  if (nrow(group_data) == 1) {
    return(NA)
  }
  
  # Calculate the distance between the first and second rows in the group
  distance <- distHaversine(
    c(group_data$long[1], group_data$lat[1]),
    c(group_data$long[2], group_data$lat[2])
  )
  
  return(distance)
}

# Group data by npi, ploccityname, and State, then calculate distances within each group
distances <- data %>%
  group_by(npi, ploccityname, State) %>%
  summarize(distance = calculate_distances_within_group(.))

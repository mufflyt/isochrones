# The R script begins by loading necessary libraries and setting up paths and API keys sourced from "R/01-setup.R". It defines functions for retrieving and filtering FIPS codes ("get_fips_codes"), fetching ACS data for specified FIPS codes and a vintage year using Census API ("get_acs_data"), and cleaning and transforming ACS data into detailed demographic statistics ("clean_and_transform_acs_data"). In the main execution block, ACS data for Alabama (FIPS code "01") from the year 2021 is retrieved and processed using these functions. The cleaned and transformed demographic data is then analyzed in conjunction with block group overlap data from "data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv". This data is sorted, relevant columns are selected, and the resulting block group overlap data is saved as "data/09-get-census-population/block-group-isochrone-overlap.csv" for further analysis or visualization.


# Load necessary libraries
library(tidyverse)
library(tigris)
library(sf)
library(lubridate)

# Setup: Define Paths and API Keys
source("R/01-setup.R")
cat(str_c(Sys.time(), " - Loaded setup and libraries.\n"))

# Function to retrieve and filter FIPS codes
get_fips_codes <- function() {
  cat(str_c(Sys.time(), " - Retrieving FIPS codes.\n"))
  tigris::fips_codes() %>%
    #dplyr::filter(state_code < 56) %>%
    dplyr::pull(state_code)
}

# Function to retrieve ACS data
get_acs_data <- function(us_fips_list, vintage, acs_variables) {
  cat(str_c(Sys.time(), " - Retrieving ACS data for the specified FIPS codes and vintage year.\n"))
  state_data <- lapply(us_fips_list, function(fips_code) {
    cat(str_c(Sys.time(), " - Processing FIPS code: ", fips_code, "\n"))
    getCensus(name = "acs/acs5", vintage = vintage,
              vars = c("NAME", acs_variables),
              region = "block group:*",
              regionin = paste("state:", fips_code, "&in=county:*&in=tract:*", sep = ""),
              key = Sys.getenv("CENSUS_API_KEY"))
  })
  result <- dplyr::bind_rows(state_data) %>% dplyr::mutate(year = vintage)
  cat(str_c(Sys.time(), " - Finished retrieving ACS data.\n"))
  return(result)
}

# Function to clean and prepare ACS data with detailed demographic transformations
clean_and_transform_acs_data <- function(acs_data) {
  cat(str_c(Sys.time(), " - Cleaning and transforming ACS data.\n"))
  acs_data %>%
    dplyr::rename(population = B01001_001E,
           race_universe_number = B02001_001E,
           race_white_number = B02001_002E,
           race_black_number = B02001_003E,
           race_aian_number = B02001_004E,
           race_asian_number = B02001_005E,
           race_nhpi_number = B02001_006E) %>%
    dplyr::mutate(race_other_number = race_universe_number - race_white_number - race_black_number - race_aian_number - race_asian_number - race_nhpi_number,
           race_white_pct = race_white_number / race_universe_number,
           race_black_pct = race_black_number / race_universe_number,
           race_aian_pct = race_aian_number / race_universe_number,
           race_asian_pct = race_asian_number / race_universe_number,
           race_nhpi_pct = race_nhpi_number / race_universe_number,
           race_other_pct = race_other_number / race_universe_number) %>%
    dplyr::mutate(across(c(race_white_pct, race_black_pct, race_aian_pct, race_asian_pct, race_nhpi_pct, race_other_pct), round, digits = 3)) %>%
    dplyr::rename(fips_state = state) %>%
    dplyr::arrange(fips_state) %>%
    dplyr::mutate(fips_county = paste0(fips_state, county),
           fips_tract = paste0(fips_state, county, tract),
           fips_block_group = paste0(fips_state, county, tract, block_group)) %>%
    dplyr::rename(name = NAME) %>%
    dplyr::select(fips_block_group, fips_state, fips_county, fips_tract, name, everything()) %>%
    dplyr::select(-starts_with("B"), -contains("universe"), -county, -tract, -block_group)
}

# Main execution block
us_fips_list <- "01"
vintage_year <- 2021
acs_variables <- c("B01001_001E", "B02001_001E", "B02001_002E", "B02001_003E", "B02001_004E", "B02001_005E", "B02001_006E")

# Retrieve ACS data
acs_data <- get_acs_data(us_fips_list, vintage_year, acs_variables)

# Clean, transform, and prepare ACS data
demographics_bg <- clean_and_transform_acs_data(acs_data)

# Display the first few rows of the transformed data
head(demographics_bg)
cat(str_c(Sys.time(), " - Displayed the first few rows of the transformed data.\n"))

# Analyzing the intersection with isochrones
intersect_block_group_cleaned_file <- "data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv"

cat(str_c(Sys.time(), " - Reading intersection data from: ", intersect_block_group_cleaned_file, "\n"))
intersect_block_group_cleaned <- read_csv(intersect_block_group_cleaned_file) %>%
  dplyr::arrange(GEOID) %>%
  dplyr::select(-LSAD, -AWATER)
cat(str_c(Sys.time(), " - Prepared block group overlap data.\n"))

bg_overlap <- intersect_block_group_cleaned %>%
  dplyr::select(GEOID, overlap, intersect_drive_time) %>%
  sf::st_drop_geometry()
write_csv(bg_overlap, "data/09-get-census-population/block-group-isochrone-overlap.csv")
cat(str_c(Sys.time(), " - Block group isochrone overlap data saved.\n"))

#View(bg_overlap)
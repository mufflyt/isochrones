# The script `08.75-ACS.R` is structured to retrieve and process demographic data from the American Community Survey (ACS) via the U.S. Census API. Here's a streamlined overview:
#
# 1. **Setup and Preparation**: It begins by sourcing an initial setup script (`01-setup.R`). The script then uses the `tigris` package to obtain FIPS codes for U.S. states, applying filters to exclude codes above 55, thus focusing on state codes.
#
# 2. **ACS Data Retrieval Function**: Defines `get_acs_data`, a function designed to pull ACS data for specified FIPS codes, a given vintage year, and selected variables. This process iterates through each FIPS code, querying the Census API for block group data within the states of interest. The gathered data is combined, marked with the vintage year, saved as a CSV file, and returned.
#
# 3. **Example Usage and Initial Data Processing**: The script demonstrates using `get_acs_data` with Colorado (FIPS code 08), fetching 2021 vintage data for predetermined demographic variables. After fetching, the data is saved to a CSV file. Subsequently, the dataset undergoes processing to rename variables, compute additional demographics (like racial group percentages), and adjust FIPS codes for precise geographic identification.
#
# 4. **Data Cleaning and Summarization**: It concludes with organizing the `demographics_bg` dataframe by block group FIPS codes and saving this cleaned and structured demographic data into a CSV file.
#
# This script essentially prepares demographic data from the ACS for analysis, showcasing a tidyverse-based methodology for data handling and storage.

#######################
source("R/01-setup.R")
#######################

us_fips_list <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  dplyr::filter(state_code < 56) %>%
  dplyr::select(state_code) %>%
  dplyr::pull()
# us_fips_list <- c("08")  # Example FIPS codes for COLORADO for TESTING

#https://api.census.gov/data/2022/acs/acs1/variables.html
#************************************
# GET THE ACS CENSUS VARIABLES
#************************************
# Total female populations
acs_variables <- c(
  "B01001_001E",   # Total female population
  "B02001_001E",   # Total female population by race
  "B02001_002E",   # Female Population of one race: White alone
  "B02001_003E",   # Female Population of one race: Black or African American alone
  "B02001_004E",   # Female Population of one race: American Indian and Alaska Native alone
  "B02001_005E",   # Female Population of one race: Asian alone
  "B02001_006E"    # Female Population of one race: Native Hawaiian and Other Pacific Islander alone
)# Example ACS variables for TESTING

acs_data <- get_acs_data(us_fips_list, 
                         vintage = 2021, 
                         acs_variables); acs_data
readr::write_csv(acs_data, "data/08.75-acs/acs-data.csv")
# acs_data <- readr::read_csv("data/08.75-acs/acs-data.csv") #for testing

demographics_bg <- acs_data %>%
rename(population = B01001_001E, race_universe_number = B02001_001E, race_white_number = B02001_002E, race_black_number = B02001_003E, race_aian_number = B02001_004E, race_asian_number = B02001_005E, race_nhpi_number = B02001_006E) %>%
  mutate(race_other_number = race_universe_number - race_white_number - race_black_number - race_aian_number - race_asian_number - race_nhpi_number) %>%
  mutate(race_white_pct = race_white_number/race_universe_number) %>%
  mutate(race_black_pct = race_black_number/race_universe_number) %>%
  mutate(race_aian_pct = race_aian_number/race_universe_number) %>%
  mutate(race_asian_pct = race_asian_number/race_universe_number) %>%
  mutate(race_nhpi_pct = race_nhpi_number/race_universe_number) %>%
  mutate(race_other_pct = race_other_number/race_universe_number) %>%
  mutate(across(c(race_white_pct, race_black_pct, race_aian_pct, race_asian_pct, race_nhpi_pct, race_other_pct), ~ (round(., digits = 3)))) %>%
  rename(fips_state = state) %>%
  arrange(fips_state) %>%
  mutate(fips_county = paste0(fips_state, county)) %>%
  mutate(fips_tract = paste0(fips_state, county, tract)) %>%
  mutate(fips_block_group = paste0(fips_state, county, tract, block_group)) %>%
  rename(name = NAME) %>%
  select(fips_block_group, fips_state, fips_county, fips_tract, name, everything()) %>%
  select(-starts_with("B"), -contains("universe"), -county, -tract, -block_group); head(demographics_bg)

summary(demographics_bg$population)
demographics_bg <- demographics_bg %>% arrange(fips_block_group)
write.csv(demographics_bg, "data/08.75-acs/acs-block-group-demographics.csv", na = "", row.names = F)

#************************************
# CLEANS THE ACS CENSUS VARIABLES
#************************************
# demographics_bg <- all_census_data %>%
#   dplyr::rename(name = NAME,
#                 population = B01001_026E, #total female population
#                 fips_state = state) %>%
#   dplyr::mutate(fips_block_group = paste0(
#     fips_state,
#     county,
#     str_pad(tract, width = 6, pad = "0"),
#     block_group
#   ),) %>%
#   dplyr::arrange(fips_state) %>%
#   dplyr::select(fips_block_group, name, population); demographics_bg

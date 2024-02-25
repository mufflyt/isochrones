#######################
source("R/01-setup.R")
#######################

us_fips_list <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  dplyr::filter(state_code < 56) %>%
  dplyr::select(state_code) %>%
  dplyr::pull()

#https://api.census.gov/data/2022/acs/acs1/variables.html
#************************************
# GET THE ACS CENSUS VARIABLES
#************************************
get_acs_data <- function(us_fips_list, vintage = 2019, acs_variables) {
state_data <- list()
for (fips_code in us_fips_list) {
  stateget <- paste("state:", fips_code, "&in=county:*&in=tract:*", sep = "")
  state_data[[fips_code]] <- getCensus(name = "acs/acs5", vintage = vintage,
                                       vars = c("NAME", acs_variables),
                                       region = "block group:*", regionin = stateget,
                                       key = "485c6da8987af0b9829c25f899f2393b4bb1a4fb")
}
acs_raw <- dplyr::bind_rows(state_data)
acs_raw <- acs_raw %>%
  mutate(year = vintage)
Sys.sleep(1)

readr::write_csv(acs_raw, paste0("data/08.75-acs/08.75-acs_", vintage, "vintage.csv"))
return(acs_raw)
}

# Example usage
us_fips_list <- c("08")  # Example FIPS codes
acs_variables <- c("B01001_001E", "B02001_001E", "B02001_002E", "B02001_003E", "B02001_004E", "B02001_005E", "B02001_006E")  # Example ACS variables
acs_data <- get_acs_data(us_fips_list, vintage = 2021, acs_variables); acs_data
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

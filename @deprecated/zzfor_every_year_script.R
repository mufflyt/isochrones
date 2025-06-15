############################
# ISOCHRONES
############################

# Setting things up here - I'm grabbing the requested dates / times you gave me.
library(tidyverse)
library(sf)
library(mapview)
library(hereR)
library(tidycensus)

# For the real paper
iso_datetime_yearly <- tibble(
  date = c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
           "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
           "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
           "2022-10-21 09:00:00", "2023-10-20 09:00:00"),
  year = c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023")
)

### Generate isochrones for each of the two dates
# We need the following intervals:
# 30, 60, 120, and 180-minute drive time intervals

# We can do this in R with the hereR package.  I grabbed my own key for this.
key <- "VnDX-Rafqchcmb4LUDgEpYlvk8S1-LCYkkrtb1ujOrM"
set_key(key)

# Let's build the physician location table; presumably you have a separate table you can plug in
# for this step.
# Your physician locations data
physician_locations <- data.frame(
  name = c("Alpha, MD", "Beta, MD", "Delta, MD", "Gamma, MD", "Zulu, MD", "Xray, MD", "Yankee, MD", "Alpha, MD"),
  Latitude = c(39.7392, 41.8781, 40.7128, 39.7294, 42.3314, 48.7519, 18.4655, 39.7392),
  Longitude = c(-104.9903, -87.6298, -74.0060, -104.8319, -83.0458, -122.4787, -66.1057, -104.9903),
  year = c(2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023)
)

# We'll want to convert our table to an sf object to calculate the isochrones.
physician_sf <- physician_locations %>%
  st_as_sf(coords = c("Longitude", "Latitude"),
           crs = 4326)

# Function to calculate isochrones for a specific year
calculate_isochrones <- function(date, year, physician_sf) {
  log_message <- paste("Calculating isochrones for year:", year)
  print(log_message)
  
  result <- physician_sf %>%
    dplyr::filter(year == year) %>%
    isoline(
      datetime = as.POSIXct(date, tz = "UTC"),
      range = c(30, 60, 120, 180) * 60
    ) %>%
    dplyr::mutate(year = year)
  
  return(result)
}

# Iterate over each year and calculate isochrones
physician_isos_all_years <- iso_datetime_yearly %>%
  purrr::pmap_dfr(~calculate_isochrones(..1, ..2, physician_sf))

# Check the resulting combined sf object
print(physician_isos_all_years)

mapview(physician_isos_all_years)

# physician_isos_all_years <- read_rds("data/physician_isos_all_years.rds")
write_rds(physician_isos_all_years, "data/physician_isos_all_years.rds")
physician_isos_all_years <- read_rds("data/physician_isos_all_years.rds")

physician_isos_unique <- physician_isos_all_years %>%
  group_by(year, range) %>%
  summarise()

### Static Facet map
library(ggplot2)
library(dplyr)
library(sf)
library(maps)

# Assuming physician_isos_all_years has already been calculated as per the previous steps
# Arrange the data for plotting
physician_isos_unique <- physician_isos_all_years %>%
  arrange(desc(range))

# Load the US states boundaries
us_states <- st_as_sf(maps::map("state", plot = FALSE, fill = TRUE)) %>%
  st_transform(crs = st_crs(physician_isos_unique))

# Create the static map with facets by year and add US state boundaries
static_facet_map <- ggplot(data = physician_isos_unique) +
  geom_sf(data = us_states, fill = NA, color = "black", size = 0.5) +  # Adding state boundaries
  geom_sf(aes(fill = factor(range)), color = NA) +
  scale_fill_viridis_d(name = "Travel Time (minutes)", labels = c("30", "60", "120", "180")) +
  facet_wrap(~ year, ncol = 3) +
  theme_minimal() +
  labs(
    title = "Isochrones Facet Map by Year",
    subtitle = "Grouped by Year and Travel Time Ranges",
    x = "Longitude",
    y = "Latitude"
  ) +
  theme(
    strip.background = element_rect(fill = "lightgrey"),
    strip.text = element_text(size = 10),
    legend.position = "bottom"
  )

# Display the map
print(static_facet_map)

# Save the static facet map to a file
ggsave(filename = "figures/static_facet_map.png", plot = static_facet_map, width = 12, height = 8, dpi = 300)

###### Pull the ACS Variables
library(dplyr)
library(tidycensus)
library(sf)


# # Calculate the isochrones around the physicians.  We
# # need to supply the range in seconds.
# physician_isos_2023 <- physician_sf %>%
#   filter(year == 2023) %>%
#   isoline(
#     datetime = iso_datetime_yearly, #as.POSIXct("2023-10-20 09:00:00"),
#     range = c(30, 60, 120, 180) * 60
#   ) 

# physician_isos_2023_unique <- physician_isos_2023 %>%
#   group_by(range) %>%
#   summarise()

# # Unique is all isochrones put together.  
# mapview(arrange(physician_isos_2023_unique, desc(range)),
#         zcol = "range", layer.name = "Isochrones seconds") +
#   mapview(filter(physician_sf, year == 2023), legend = FALSE)

# Let's add the physician name to each isochrone.
# physician_isos_2023 <- physician_isos_2023 %>%
#   mutate(physician = rep(physician_sf$name[1:4], each = 4))
# 
# # I'm a fan of the mapview package for quick visual inspection of results:
# mapview(arrange(physician_isos_2023, desc(range)),
#         zcol = "range", layer.name = "Isochrones") +
#   mapview(filter(physician_sf, year == 2023), legend = FALSE)
# 
# 
# # Now, we can carry out the same process for 2010.
# physician_isos_2010 <- physician_sf %>%
#   filter(year == 2010) %>%
#   isoline(
#     datetime = as.POSIXct("2010-10-15 09:00:00"),
#     range = c(30, 60, 120, 180) * 60
#   )
# 
# physician_isos_2010 <- physician_isos_2010 %>%
#   mutate(physician = rep(physician_sf$name[5:8], each = 4))
# 
# # I'm a fan of the mapview package for quick visual inspection of results:
# mapview(arrange(physician_isos_2010, desc(range)),
#         zcol = "range", layer.name = "Isochrones") +
#   mapview(filter(physician_sf, year == 2010), legend = FALSE)
# 
# physician_isos_2010_unique <- physician_isos_2010 %>%
#   group_by(range) %>%
#   summarise()
# 
# # Unique is all isochrones put together.  
# mapview(arrange(physician_isos_2010_unique, desc(range)),
#         zcol = "range", layer.name = "Isochrones seconds") +
#   mapview(filter(physician_sf, year == 2010), legend = FALSE)
# 
# ## PR land and isochrones are clipped
# puerto_rico <- tigris::states(cb = TRUE) %>%
#   filter(NAME=="Puerto Rico") %>%
#   st_transform(4326)
# 
# puerto_rico_isos <- st_intersection(physician_isos_2010, puerto_rico)
# 
# mapview(arrange(puerto_rico_isos, desc(range)),
#         zcol = "range", layer.name = "Isochrones") +
#   mapview(filter(physician_sf, year == 2010), legend = FALSE)
# 
# ## US land and isochrones are now clipped
# us <- tigris::nation() %>%
#   st_transform(4326)
# 
# mapview(us)

############################
# CENSUS DATA
############################

# Below, I have the lists of variables you've compiled that you'd like to pull from the ACS
# at the block group level.

# These are the ages of interest from the ACS:
female_vars <- c(
  total_female_026 = "B01001_026",
  female_less_than_5 = "B01001_027",
  female_5_to_9 = "B01001_028",
  female_10_to_14 = "B01001_029",
  female_15_to_17 = "B01001_030",
  female_18_to_19 = "B01001_031",
  female_20years = "B01001_032",
  female_21years = "B01001_033",
  female_22_to_24 = "B01001_034",
  female_25_to_29 = "B01001_035",
  female_30_to_34 = "B01001_036",
  female_35_to_39 = "B01001_037",
  female_40_to_44 = "B01001_038",
  female_45_to_49 = "B01001_039",
  female_50_to_54 = "B01001_040",
  female_55_to_59 = "B01001_041",
  female_60_to_61 = "B01001_042",
  female_62_to_64 = "B01001_043",
  female_65_to_66 = "B01001_044",
  female_67_to_69 = "B01001_045",
  female_70_to_74 = "B01001_046",
  female_75_to_79 = "B01001_047",
  female_80_to_84 = "B01001_048",
  female_85_over = "B01001_049"
)

# These are the block group race/ethnicities of interest from the ACS:
# https://www.socialexplorer.com/data/ACS2010/metadata/?ds=ACS10&table=B01001B
acs_variables <- c(
  #total_female_and_male = "B01001_001E",   #  B01001_001 covers the total population (both male and female)
  total_female_race = "B01001_026",   # Total female population by race, This variable represents the total number of females across all age groups and all racial categories in the United States
  total_female_white = "B01001A_026",   # Female Population of one race: White alone
  total_female_black = "B01001B_026",   # Female Population of one race: Black or African American alone
  total_female_aian = "B01001C_026",   # Female Population of one race: American Indian and Alaska Native alone
  total_female_asian = "B01001D_026",   # Female Population of one race: Asian alone
  total_female_hipi = "B01001E_026"    # Female Population of one race: Native Hawaiian and Other Pacific Islander alone
)

# I'll combine these into a single vector so we can make one pull.
all_vars = c(female_vars, acs_variables)

# Let's grab these at the block group level in wide format
# for the 2009-13 ACS (for 2010) and from the most recent ACS
# A note: block groups aren't on the API for the 2008-2012 ACS and earlier
# We could use the 2010 Census for this or you'd need to look at other sources.
options(tigris_use_cache = TRUE)

# Function to pull ACS data for each year listed in iso_datetime_yearly and process it
library(dplyr)
library(tidycensus)
library(sf)
library(purrr)
library(lubridate)
library(tidycensus)
library(dplyr)
library(lme4)

# Function to pull ACS data for each year listed in iso_datetime_yearly and process it
pull_acs_data_by_year <- function(iso_datetime_yearly, all_vars, destination_dir, geography = "county") {
  
  # Set tigris to use cache
  options(tigris_use_cache = TRUE)
  
  # Ensure the destination directory exists
  if (!dir.exists(destination_dir)) {
    dir.create(destination_dir, recursive = TRUE)
    message(paste("Created directory:", destination_dir))
  }
  
  # Create an empty list to store the results
  results_list <- list()
  
  # Iterate over each row in iso_datetime_yearly to pull ACS data for each year
  for (i in seq_along(iso_datetime_yearly$year)) {
    current_year <- as.integer(iso_datetime_yearly$year[i])
    
    log_message <- paste("Starting to pull ACS data for year:", current_year, "and geography:", geography)
    print(log_message)
    
    # Try to pull ACS data for the current year
    acs_data <- tryCatch({
      get_acs(
        geography = geography,
        variables = all_vars,
        state = c(state.abb, "DC", "PR"), # Grab for the US
        year = current_year,
        geometry = TRUE,
        output = "wide"
      )
    }, error = function(e) {
      error_message <- paste("Error retrieving data for year:", current_year, "-", e$message)
      message(error_message)
      return(NULL)
    })
    
    # Skip if the data retrieval failed
    if (is.null(acs_data)) {
      message(paste("Skipping year", current_year, "due to errors"))
      next
    }
    
    # Add the year column to the data
    acs_data <- acs_data %>%
      mutate(year = current_year)
    
    log_message <- paste("Successfully pulled ACS data for year:", current_year, "and geography:", geography)
    print(log_message)
    
    # Append the data to the results list
    results_list[[i]] <- acs_data
  }
  
  # Combine all the results into a single data frame
  combined_data <- bind_rows(results_list)
  
  # Define the output file path
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  output_file <- file.path(destination_dir, paste0("combined_acs_data_", geography, "_", timestamp, ".rds"))
  
  # Save the combined data to a file
  tryCatch({
    saveRDS(combined_data, output_file)
    message(paste("Data successfully saved to:", output_file))
  }, error = function(e) {
    error_message <- paste("Failed to save data to file:", output_file, "-", e$message)
    message(error_message)
  })
  
  return(combined_data)
}

destination_dir <- "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script"
iso_datetime_yearly <- tibble(
  date = c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
           "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
           "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
           "2022-10-21 09:00:00", "2023-10-20 09:00:00"),
  year = c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023")
)
combined_acs_data <- pull_acs_data_by_year(iso_datetime_yearly, all_vars, destination_dir, geography = "county")

# combined_acs_data_entire_usa <- pull_acs_data_by_year(iso_datetime_yearly, all_vars, destination_dir, geography = "us")

year <- c(2013:2022)

# # Pull ACS data for the entire US
# acs_data_us <- get_acs(
#   geography = "us",
#   variables = all_vars, # Replace with your actual variables
#   year = year,
#   survey = "acs5"
# ) %>%
#   mutate(year = year)

library(tidycensus)
library(dplyr)

# Define the variables and years you want to pull data for
years <- 2013:2022 # Replace with the years you're interested in

# Function to pull ACS data for a given year and geography
pull_acs_data_for_year <- function(year, variables, geography) {
  get_acs(
    geography = geography,
    variables = variables,
    year = year,
    survey = "acs5"
  ) %>%
    mutate(year = year, geography = geography)
}

# Initialize an empty dataframe
acs_data_us_all_years <- tibble()

# Loop through each year and bind the results
for (year in years) {
  acs_data_year <- pull_acs_data_for_year(year, all_vars, geography = "us")
  acs_data_us_all_years <- bind_rows(acs_data_us_all_years, acs_data_year)
}; acs_data_us_all_years


combined_acs_data1 <- combined_acs_data %>%
  select(!ends_with("M")) %>%
  rename_with(.fn = ~stringr::str_remove(.x, "E$")) %>%
  st_transform("ESRI:102003"); combined_acs_data1

write_rds(combined_acs_data1, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/acs_data_us_all_years.rds")

# combined_acs_data1 <- readr::read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/combined_acs_data_us_20240830_184028.rds")


# 
# pull_2010 <- get_acs(
#   geography = "county",
#   variables = all_vars,
#   state = c(state.abb, "DC", "PR"), # Grab for the US
#   year = 2013,
#   geometry = TRUE,
#   output = "wide"
# )

# Write this to a file to avoid re-downloading
# write_rds(pull_2010, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2010.rds")
# pull_2010 <- read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2010.rds")


# pull_2020 <- get_acs(
#   geography = "county",
#   variables = all_vars,
#   state = c(state.abb, "DC", "PR"), # Grab for the US
#   year = 2022,
#   geometry = TRUE,
#   output = "wide"
# )
# 
# write_rds(pull_2020, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2020.rds")
# pull_2020 <- read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2020.rds")

# There are a few different ways to tabulate this data by isochrone.  Let's go from the simplest to the most complicated.
# The simplest method is to do block group centroid allocation.  This involves a _spatial join_.
# Let's first remove the margin of error columns and the estimate suffix from our data.
# Also note that I'm "projecting" the data to an equal-area coordinate reference system;
# this tries to keep the area of shapes consistent around the US, which is important for
# some of the methods we are using.
# data_2010 <- pull_2010 %>%
#   select(!ends_with("M")) %>%
#   rename_with(.fn = ~str_remove(.x, "E$")) %>%
#   st_transform("ESRI:102003")

#centroids <- st_centroid(combined_acs_data1)

# We can convert block groups to "centroids" representing the average X and Y values of the coordinates in each
# block group polygon.
#centroids_2010 <- st_centroid(data_2010)

# From here, we can perform point-in-polygon overlay and sum up the values in each column for each isochrone.
# tabulated_2010 <- physician_isos_2010 %>%
#   st_transform("ESRI:102003") %>%
#   select(physician, range) %>%
#   st_join(centroids, left = FALSE) %>%
#   st_drop_geometry() %>%
#   group_by(physician, range) %>%
#   summarize(across(total_female:total_female_hipi,
#                    .fns = ~sum(.x, na.rm = TRUE))) %>%
#   ungroup()

pull <- readr::read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/acs_data_us_all_years.rds")

# Let's do this again for 2020 (2023):
data <- pull %>%
  dplyr::select(!dplyr::ends_with("M")) %>%
  rename_with(.fn = ~str_remove(.x, "E$")) %>%
  st_transform("ESRI:102003")

centroids <- st_centroid(data)

tabulated_2020 <- physician_isos_2023 %>%
  st_transform("ESRI:102003") %>%
  select(physician, range) %>%
  st_join(centroids_2020, left = FALSE) %>%
  st_drop_geometry() %>%
  group_by(physician, range) %>%
  summarize(across(total_female:total_female_hipi,
                   .fns = ~sum(.x, na.rm = TRUE))) %>%
  ungroup()


######
centroids_2020 <- st_centroid(combined_acs_data1)

## For table, total number of women within each isochrones
#total_female_white is the numerator
# Center of the mid-year of the sample.  2020 and later use 18-22 ACS.  
# tabulated_2020 <- #physician_isos_2023_unique %>% 
#   physician_isos_unique %>% 
#   st_transform("ESRI:102003") %>%
#   select(range) %>%
#   st_join(centroids_2020, left = FALSE) %>%
#   st_drop_geometry() %>%
#   group_by(range) %>%
#   summarize(across(total_female:total_female_hipi,
#                    .fns = ~sum(.x, na.rm = TRUE))) %>%
#   ungroup() %>%
#   mutate(range = range/60L) %>%
#   select(-total_female_race); tabulated_2020

library(dplyr)
library(scales)

# List to store tabulated results for all years
tabulated_all_years_list <- list()

# Loop through each year present in physician_isos_unique
for (current_year in unique(physician_isos_unique$year)) {
  
  # Filter data for the current year
  centroids_year <- combined_acs_data1 %>%
    filter(year == current_year)
  
  physician_isos_year <- physician_isos_unique %>%
    filter(year == current_year)
  
  # Transform, join, and keep the year column
  tabulated_year <- physician_isos_year %>%
    st_transform("ESRI:102003") %>%
    st_join(centroids_year, left = FALSE) %>%
    st_drop_geometry() %>%
    mutate(year = current_year) %>%  # Ensure the year column is present
    group_by(year, range) %>%
    summarize(across(total_female_026:total_female_hipi, .fns = ~sum(.x, na.rm = TRUE))) %>%
    ungroup()
  
  # Add the result to the list
  tabulated_all_years_list[[current_year]] <- tabulated_year
}

# Combine the results into a single dataframe
tabulated_all_years <- bind_rows(tabulated_all_years_list)

readr::write_rds(tabulated_all_years, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_centroid_tabulated_2020.rds")

#tabulated_all_years <- readr::read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_centroid_tabulated_2020.rds")

# Apply the improvements to your table
tabulated_all_years_clean <- tabulated_all_years %>%
  mutate(across(total_female_026:total_female_hipi, as.numeric)) %>%
  mutate(
    range = range / 60L,
    across(total_female_026:total_female_hipi, ~comma(.))  # Format large numbers with commas
  ) %>%
  #dplyr::select(-total_female, everything(), total_female) %>%
  #dplyr::select(-total_female) %>%
  rename(
    "Year" = year,
    "Driving Time (minutes)" = range,
    #"Total Female" = total_female_race,
    "White" = total_female_white,
    "Black" = total_female_black,
    "American Indian/Alaska Native" = total_female_aian,
    "Asian" = total_female_asian,
    "Native Hawaiian or Pacific Islander" = total_female_hipi
  ) %>%
  arrange(Year, `Driving Time (minutes)`)  # Ensure rows are sorted by Year and Driving Time

# View the cleaned table
print(tabulated_all_years_clean)

library(writexl)

# Define the file name with a timestamp
file_name <- paste0("data/tabulated_all_years_clean_", Sys.Date(), ".xlsx")

# Save the dataframe to an Excel file
write_xlsx(tabulated_all_years_clean, path = file_name)

# Print the file path
print(paste("Data saved to:", file_name))


# No difference in the number of women over the years studied
tabulated_all_years_clean <- tabulated_all_years_clean %>%
  dplyr::mutate(total_female_026 = as.numeric(gsub(",", "", total_female_026)))

kruskal_test_all_women <- kruskal.test(total_female_026 ~ Year, data = tabulated_all_years_clean); kruskal_test_all_women

# Difference in number of women served by isochrone
kruskal_test <- kruskal.test(total_female_026 ~ tabulated_all_years_clean$`Driving Time (minutes)`, data = tabulated_all_years_clean)
print(kruskal_test)

#To test whether there is a statistically significant change in the number of women served in each of the Drive Time isochrones over the years

# To obtain a single p-value that tests whether the fixed effects for Year are jointly significant, you can use a likelihood ratio test (LRT) comparing the full model (with Year as a predictor) to a reduced model (without Year).
# Full model with Year as a predictor
full_model <- lmer(as.numeric(total_female_026) ~ Year + (1 | `Driving Time (minutes)`), 
                   data = tabulated_all_years_clean)

# Reduced model without Year
reduced_model <- lmer(as.numeric(total_female_026) ~ 1 + (1 | `Driving Time (minutes)`), 
                      data = tabulated_all_years_clean)

# Likelihood ratio test
anova(reduced_model, full_model)

# Convert columns to the appropriate types
tabulated_all_years_clean <- tabulated_all_years_clean %>%
  dplyr::mutate(
    total_female_026 = as.numeric(gsub(",", "", total_female_026)),
    Year = as.factor(Year),
    `Driving Time (minutes)` = as.factor(`Driving Time (minutes)`)
  ) %>%
  dplyr::filter(!is.na(total_female_026))

# Fit the mixed-effects model
model <- lmer(total_female_026 ~ Year + (1 | `Driving Time (minutes)`), data = tabulated_all_years_clean)

# Summarize the model
summary(model)


######
# analyzing racial disparities in access to gynecologic oncologists based on the drive times and racial breakdowns provided in your data.
tabulated_all_years_numeric <- tabulated_all_years_clean %>%
  mutate(
    White = gsub(",", "", White),
    Black = gsub(",", "", Black),
    `American Indian/Alaska Native` = gsub(",", "", `American Indian/Alaska Native`),
    Asian = gsub(",", "", Asian),
    `Native Hawaiian or Pacific Islander` = gsub(",", "", `Native Hawaiian or Pacific Islander`),
    total_female_026 = gsub(",", "", total_female_026)
  )

tabulated_all_years_numeric <- tabulated_all_years_numeric %>%
  mutate(
    White = as.numeric(White),
    Black = as.numeric(Black),
    AIAN = as.numeric(`American Indian/Alaska Native`),
    Asian = as.numeric(Asian),
    NHPI = as.numeric(`Native Hawaiian or Pacific Islander`),
    total_female_026 = as.numeric(total_female_026)
  ) %>%
  mutate(
    White_prop = White / total_female_026,
    Black_prop = Black / total_female_026,
    AIAN_prop = AIAN / total_female_026,
    Asian_prop = Asian / total_female_026,
    NHPI_prop = NHPI / total_female_026
  )

#Analyze the lowest level of access for racial minorities across all periods and all isochrones:
summary_by_year <- tabulated_all_years_numeric %>%
  group_by(Year) %>%
  summarize(
    White_mean = mean(White_prop, na.rm = TRUE),
    Black_mean = mean(Black_prop, na.rm = TRUE),
    AIAN_mean = mean(AIAN_prop, na.rm = TRUE),
    Asian_mean = mean(Asian_prop, na.rm = TRUE),
    NHPI_mean = mean(NHPI_prop, na.rm = TRUE)
  ); summary_by_year

#Analyze the lowest level of access for racial minorities across all periods and all isochrones:
summary_by_year_by_isochrone <- tabulated_all_years_numeric %>%
  group_by(Year, `Driving Time (minutes)`) %>%
  summarize(
    White_mean = mean(White_prop, na.rm = TRUE),
    Black_mean = mean(Black_prop, na.rm = TRUE),
    AIAN_mean = mean(AIAN_prop, na.rm = TRUE),
    Asian_mean = mean(Asian_prop, na.rm = TRUE),
    NHPI_mean = mean(NHPI_prop, na.rm = TRUE)
  ); summary_by_year_by_isochrone


#Question: "Women in racial minority populations exhibited the lowest level of access to gynecologic oncologists across all periods. For example, xx% of American Indian or Alaska Native women did not have access to a physician within a 3-hour drive, which improved over time."

# Example usage:
summary_sentence <- race_drive_time_generate_summary_sentence(tabulated_all_years_numeric, 180, race = "American Indian/Alaska Native")
print(summary_sentence)

#Question: "Women in racial minority populations exhibited the lowest level of access to gynecologic oncologists across all periods. For example, xx% of American Indian or Alaska Native women did not have access to a physician within a 3-hour drive, which improved over time."

#####
linear_regression_summary_sentence <- linear_regression_race_drive_time_generate_summary_sentence(
  tabulated_data = tabulated_all_years_numeric,
  driving_time_minutes = 60,
  race = "all"
)
print(linear_regression_summary_sentence)













# Load necessary libraries
library(dplyr)
library(ggplot2)

# Check if the data for both groups (AIAN_prop and Black_prop) are distinct
distinct_check <- tabulated_all_years_numeric %>%
  select(Year, `Driving Time (minutes)`, AIAN_prop, Black_prop) %>%
  distinct()

# Print the number of unique rows for both AIAN_prop and Black_prop
distinct_count <- distinct_check %>%
  summarise(
    distinct_AIAN = n_distinct(AIAN_prop),
    distinct_Black = n_distinct(Black_prop)
  )
print(distinct_count)

# Calculate and print summary statistics (mean, median, variance) for AIAN_prop and Black_prop across years
summary_stats <- tabulated_all_years_numeric %>%
  group_by(Year) %>%
  summarise(
    mean_AIAN = mean(AIAN_prop, na.rm = TRUE),
    median_AIAN = median(AIAN_prop, na.rm = TRUE),
    variance_AIAN = var(AIAN_prop, na.rm = TRUE),
    mean_Black = mean(Black_prop, na.rm = TRUE),
    median_Black = median(Black_prop, na.rm = TRUE),
    variance_Black = var(Black_prop, na.rm = TRUE)
  )

# Print the summary statistics
print(summary_stats)

ggplot(data = tabulated_all_years_numeric, aes(x = Year)) +
  geom_line(aes(y = AIAN_prop, color = "AIAN_prop"), linewidth = 1) +  # Updated from `size` to `linewidth`
  geom_line(aes(y = Black_prop, color = "Black_prop"), linewidth = 1) +  # Updated from `size` to `linewidth`
  labs(title = "Comparison of AIAN_prop and Black_prop Over Years",
       y = "Proportion",
       color = "Group") +
  theme_minimal()


# Ensure that the Kruskal-Wallis test was correctly applied
# Apply Kruskal-Wallis test separately for both groups
kruskal_test_AIAN <- kruskal.test(AIAN_prop ~ Year, data = tabulated_all_years_numeric)
kruskal_test_Black <- kruskal.test(Black_prop ~ Year, data = tabulated_all_years_numeric)

# Print the Kruskal-Wallis test results
print(kruskal_test_AIAN)
print(kruskal_test_Black)






####
# 1 year ACS back to 2005

# Separate pull with iterators
# Total population by end year by 5 year ACS
us_female_by_year <- purrr::map_dfr(.x = 2009:2022, .f = function(year) {
  get_acs(geography = "us", variables = "B01001_026", survey = "acs5", year = year) %>%
    mutate(year = year)
}
)




acs1_variables <- tidycensus::load_variables(2022, "acs1")
View(acs1_variables)
#B01001_026

# I'll compute the following two methods only with the 2020 / 2023 data;
# we can extend if you'd like.
#
# The "medium" method of complication is area-weighted areal interpolation.
# One issue with the spatial join approach that I outlined above is that the irregular
# shape of isochrones can partially cover block groups on the edges.  So some block groups
# will be excluded, and others fully included, that are partially covered.
#
# We typically use areal interpolation to solve this problem.  Areal interpolation involves the
# transfer / aggregation of attributes from one irregular spatial dataset to another irregular
# spatial dataset.
#
# The simplest version of this method used area weights. You can think of it this way:
# the overlap between isochrone and block group is calculated, and a percentage corresponding
# to the area of overlap is transferred to the isochrone.  So if a block group is 60% covered
# by the isochrone, the isochrone gets 60% of its population.
physician_iso_projected <- st_transform(physician_isos_2023, "ESRI:102003")

aw_2020 <- data_2020 %>%
  st_filter(physician_iso_projected) %>%
  select(female_80_to_84:total_female_hipi) %>%
  st_interpolate_aw(to = physician_iso_projected, extensive = TRUE)

mapview(aw_2020)

write_rds(aw_2020, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_area_weighted_2020.rds")


# We notice that the result is similar to, but not identical to, the block group centroid approach.
#
# The most complex version of this is the population-weighted areal interpolation approach.  Instead of using area
# which is by definition baked in to each polygon, we need an ancillary dataset to use as weights that more
# accurately represents the locations where people live within each polygon.  This is particularly useful when
# analyzing low-density or rural areas as large areas of overlap may entirely miss where people actually live
# within those block groups.
#
# I've implemented this method in tidycensus with the `interpolate_pw()` function.  Typically we use Census blocks as
# the interpolation weights, which are available in the tigris R package.  One caveat is that they can take a while
# to download!  So, we'll only want to download blocks for locations that overlap our isochrones.
library(tigris)

overlapping_states <- states(cb = TRUE) %>%
  st_transform(st_crs(physician_iso_projected)) %>%
  st_filter(physician_iso_projected) %>%
  pull(STUSPS)

req_blocks <- map_dfr(overlapping_states, function(x) {
  blocks(state = x, year = 2020)
})

pw_2020 <- data_2020 %>%
  st_filter(physician_iso_projected) %>%
  interpolate_pw(
    to = physician_iso_projected,
    extensive = TRUE,
    weights = req_blocks,
    weight_column = "POP20",
    crs = "ESRI:102003"
  )

write_rds(pw_2020, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/population_weighted_2020.rds")

# This is much slower to compute! It may also need optimization on a smaller-memory laptop.
# However, it may be slightly more accurate.
# As an aside as well: to get the block population data for 2010, you'd need this code instead:
req_blocks_2010 <- map_dfr(overlapping_states, function(x) {
  get_decennial(
    geography = "block",
    variables = "P001001",
    year = 2010,
    state = x,
    sumfile = "sf1",
    geometry = TRUE
  )
})
# Your weight column would then be named "value".
#
# My take-away: for the purposes of your project, the block group centroid approach
# may be all you need.  All the results are fairly similar given the large
# size of your isochrones - and given the large population sizes, the small
# discrepancies may not matter.  However: if you want the most "sophisticated" method
# for your research project, the population-weighted method is the best one to use.

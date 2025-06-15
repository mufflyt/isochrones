############################
# ISOCHRONES
############################

# Setting things up here - I'm grabbing the requested dates / times you gave me.
library(tidyverse)
library(sf)
library(mapview)
library(hereR)
library(tidycensus)

iso_datetime_yearly <- tibble(
  date = c(
    "2010-10-15 09:00:00",
    "2023-10-20 09:00:00"),
  year = c("2010", "2022")
)

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
key <- Sys.getenv("HERE_API_KEY")
if (identical(key, "")) {
  stop("HERE_API_KEY environment variable is not set.")
}
set_key(key)

# Let's build the physician location table; presumably you have a separate table you can plug in
# for this step.
physician_locations <- data.frame(
  name = c("Alpha, MD", "Beta, MD", "Delta, MD", "Gamma, MD", "Zulu, MD", "Xray, MD", "Yankee, MD", "Alpha, MD"),
  Latitude = c(39.7392, 41.8781, 40.7128, 39.7294, 42.3314, 48.7519, 18.4655, 39.7392),
  Longitude = c(-104.9903, -87.6298, -74.0060, -104.8319, -83.0458, -122.4787, -66.1057, -104.9903),
  year = c(2023, 2023, 2023, 2023, 2010, 2010, 2010, 2010)
)

# We'll want to convert our table to an sf object to calculate the isochrones.
physician_sf <- physician_locations %>%
  st_as_sf(coords = c("Longitude", "Latitude"),
           crs = 4326)

# Calculate the isochrones around the physicians.  We
# need to supply the range in seconds.
physician_isos_2023 <- physician_sf %>%
  filter(year == 2023) %>%
  isoline(
    datetime = as.POSIXct("2023-10-20 09:00:00"),
    range = c(30, 60, 120, 180) * 60
  ) 

# Unique is all isochrones put together. 
physician_isos_2023_unique <- physician_isos_2023 %>%
  group_by(range) %>%
  summarise()

mapview(arrange(physician_isos_2023_unique, desc(range)),
        zcol = "range", layer.name = "Isochrones seconds") +
  mapview(filter(physician_sf, year == 2023), legend = FALSE)

# Let's add the physician name to each isochrone.
physician_isos_2023 <- physician_isos_2023 %>%
  mutate(physician = rep(physician_sf$name[1:4], each = 4))

# I'm a fan of the mapview package for quick visual inspection of results:
mapview(arrange(physician_isos_2023, desc(range)),
        zcol = "range", layer.name = "Isochrones") +
  mapview(filter(physician_sf, year == 2023), legend = FALSE)


# Now, we can carry out the same process for 2010.
physician_isos_2010 <- physician_sf %>%
  filter(year == 2010) %>%
  isoline(
    datetime = as.POSIXct("2010-10-15 09:00:00"),
    range = c(30, 60, 120, 180) * 60
  )

physician_isos_2010 <- physician_isos_2010 %>%
  mutate(physician = rep(physician_sf$name[5:8], each = 4))

# I'm a fan of the mapview package for quick visual inspection of results:
mapview(arrange(physician_isos_2010, desc(range)),
        zcol = "range", layer.name = "Isochrones") +
  mapview(filter(physician_sf, year == 2010), legend = FALSE)

physician_isos_2010_unique <- physician_isos_2010 %>%
  group_by(range) %>%
  summarise()

# Unique is all isochrones put together.  
mapview(arrange(physician_isos_2010_unique, desc(range)),
        zcol = "range", layer.name = "Isochrones seconds") +
  mapview(filter(physician_sf, year == 2010), legend = FALSE)

## PR land and isochrones are clipped
puerto_rico <- tigris::states(cb = TRUE) %>%
  filter(NAME=="Puerto Rico") %>%
  st_transform(4326)

puerto_rico_isos <- st_intersection(physician_isos_2010, puerto_rico)

mapview(arrange(puerto_rico_isos, desc(range)),
        zcol = "range", layer.name = "Isochrones") +
  mapview(filter(physician_sf, year == 2010), legend = FALSE)

## US land and isochrones are now clipped
us <- tigris::nation() %>%
  st_transform(4326)

mapview(us)

############################
# CENSUS DATA
############################

# Below, I have the lists of variables you've compiled that you'd like to pull from the ACS
# at the block group level.

# These are the ages of interest from the ACS:
female_vars <- c(
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
acs_variables <- c(
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

pull_2010 <- get_acs(
  geography = "county",
  variables = all_vars,
  state = c(state.abb, "DC", "PR"), # Grab for the US
  year = 2013,
  geometry = TRUE,
  output = "wide"
)

# Write this to a file to avoid re-downloading
write_rds(pull_2010, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2010.rds")
# pull_2010 <- read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2010.rds")


pull_2020 <- get_acs(
  geography = "county",
  variables = all_vars,
  state = c(state.abb, "DC", "PR"), # Grab for the US
  year = 2022,
  geometry = TRUE,
  output = "wide"
)

write_rds(pull_2020, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2020.rds")
# pull_2020 <- read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_data_2020.rds")

# There are a few different ways to tabulate this data by isochrone.  Let's go from the simplest to the most complicated.
# The simplest method is to do block group centroid allocation.  This involves a _spatial join_.
# Let's first remove the margin of error columns and the estimate suffix from our data.
# Also note that I'm "projecting" the data to an equal-area coordinate reference system;
# this tries to keep the area of shapes consistent around the US, which is important for
# some of the methods we are using.
data_2010 <- pull_2010 %>%
  select(!ends_with("M")) %>%
  rename_with(.fn = ~str_remove(.x, "E$")) %>%
  st_transform("ESRI:102003")

# We can convert block groups to "centroids" representing the average X and Y values of the coordinates in each
# block group polygon.
centroids_2010 <- st_centroid(data_2010)

# From here, we can perform point-in-polygon overlay and sum up the values in each column for each isochrone.
tabulated_2010 <- physician_isos_2010 %>%
  st_transform("ESRI:102003") %>%
  select(physician, range) %>%
  st_join(centroids_2010, left = FALSE) %>%
  st_drop_geometry() %>%
  group_by(physician, range) %>%
  summarize(across(total_female_race:total_female_hipi,
                   .fns = ~sum(.x, na.rm = TRUE))) %>%
  ungroup()

# Let's do this again for 2020 (2023):
data_2020 <- pull_2020 %>%
  select(!ends_with("M")) %>%
  rename_with(.fn = ~str_remove(.x, "E$")) %>%
  st_transform("ESRI:102003")

centroids_2020 <- st_centroid(data_2020)

tabulated_2020 <- physician_isos_2023 %>%
  st_transform("ESRI:102003") %>%
  select(physician, range) %>%
  st_join(centroids_2020, left = FALSE) %>%
  st_drop_geometry() %>%
  group_by(physician, range) %>%
  summarize(across(total_female_race:total_female_hipi,
                   .fns = ~sum(.x, na.rm = TRUE))) %>%
  ungroup()

## For table, total number of women within each isochrones
#total_female_white is the numerator
# Center of the mid-year of the sample.  2020 and later use 18-22 ACS.  


# ????????????????????????????
# tabulated_2020 <- physician_isos_2023_unique %>%
#   st_transform("ESRI:102003") %>%
#   select(range) %>%
#   st_join(centroids_2020, left = FALSE) %>%
#   st_drop_geometry() %>%
#   group_by(range) %>%
#   summarize(across(total_female_race:total_female_hipi,
#                    .fns = ~sum(.x, na.rm = TRUE))) %>%
#   ungroup() %>%
#   mutate(range = range/60L); tabulated_2020



write_rds(tabulated_2020, "/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_centroid_tabulated_2020.rds")
# tabulated_2020 <- read_rds("/Volumes/Video Projects Muffly 1/isochrones_data/walker_script/county_centroid_tabulated_2020.rds")


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

#==============================================================================
# Gynecologic Oncology Accessibility Project
# Analysis of Access Disparities (2013-2022)
#==============================================================================
# 
# PURPOSE:
# This script analyzes nationwide accessibility to gynecologic oncologists across
# demographic groups and geographic areas in the United States from 2013 to 2022.
# The analysis focuses on identifying disparities in access based on:
#   - Race/ethnicity
#   - Urban vs. rural location
#   - Changes in accessibility over time
# 
# METHODS:
# - Uses drive time isochrones (30, 60, 180, 240 minutes) generated from 
#   gynecologic oncologists' practice locations
# - Analyzes census tract centroids that fall within these isochrones
# - Calculates the percentage of female population with access by demographic group
# - Examines disparities between urban and rural areas
# - Tracks changes in accessibility from 2013 to 2022
#
# DATA SOURCES:
# - American Community Survey (ACS) for demographic data
# - National Provider Identifier (NPI) data for physician locations
# - Generated isochrones from the HERE Maps API
# - Census Bureau urban area definitions
#
# PREREQUISITES:
# Required packages:
#   - sf: For spatial data handling
#   - tidycensus: For accessing Census data
#   - tidyverse: For data manipulation
#   - tigris: For Census boundary files
#
# Required files:
#   - "/Users/tylermuffly/Dropbox (Personal)/walker_maps/data/acs_pulls.rds"
#   - "/Users/tylermuffly/Dropbox (Personal)/walker_maps/data/20241013161700.shp"
#
# API keys:
#   - Census API key (for tidycensus)
#
# OUTPUTS:
# - Tabulated access percentages by demographic group
# - Rural vs. urban access disparities
# - Temporal trends in accessibility (2013-2022)
#==============================================================================


library(sf)
#remotes::install_github("walkerke/tidycensus")
library(tidycensus)
library(tidyverse)
options(tigris_use_cache = TRUE)
sf_use_s2(FALSE)

# ABSTRACT
# Geographic Disparities in Potential Accessibility to Gynecologic Oncologists in the United States from 2013 to 2022.
#
# Objective: To use a spatial modeling approach to capture potential disparities of gynecologic oncologist accessibility in the United States at the block group level between 2013 and 2022.
#
# Methods: Physician registries identified the 2013 to 2022 gynecologic oncology workforce and were aggregated to each county. The at-risk cohort (women aged 18 years or older) was stratified by race and rurality demographics. We computed the distance from at-risk women to physicians. We set drive time to 30, 60, 180, and 240 minutes.
#
# Results: Between 2013 and 2022, the gynecologic oncology workforce increased. By 2022, there were x active physicians, and x% practiced in urban block groups. Geographic disparities were identified, with x physicians per 100,000 women in urban areas compared with 0.1 physicians per 100,000 women in rural areas. In total, x block groups (x million at-risk women) lacked a gynecologic oncologist. Additionally, there was no increase in rural physicians, with only x% practicing in rural areas in 2013-2015 relative to ??% in 2016-2022 (p=?). Women in racial minority populations exhibited the lowest level of access to physicians across all periods. For example, xx% of American Indian or Alaska Native women did not have access to a physician with a 3-hour drive from 2013-2015, which did not improve over time. Black women experience an increase in relative accessibility, with a ??% increase by 2016-2022. However, Asian or Pacific Islander women exhibited significantly better access than ??? women across all periods.
#
# Conclusion: Although the US gynecologic oncologist workforce has increased steadily over 20 years, this has not translated into evidence of improved access for many women from rural and underrepresented areas. However, distance and availability may not influence healthcare utilization and cancer outcomes only.

# Define and get the ACS variables
# We need to take care to get data from the appropriate tables
acs_variables <- c(
  total_female = "B01001_026",   # Total female population
  total_female_race = "B01001_026",   # Total female population by race
  total_female_white = "B01001A_017",   # Female Population of one race: White alone
  total_female_black = "B01001B_017",   # Female Population of one race: Black or African American alone
  total_female_aian = "B01001C_017",   # Female Population of one race: American Indian and Alaska Native alone
  total_female_asian = "B01001D_017",   # Female Population of one race: Asian alone
  total_female_hipi = "B01001E_017"    # Female Population of one race: Native Hawaiian and Other Pacific Islander alone
)

# We can get "yearly" ACS data from 2011-2015 through 2019-2023. We'll have to
# approximate for 2022; Tyler OK'd using the 2019-23 ACS for '22.

all_states <- c(state.abb, "DC", "PR")

# Let's define the ACS by the "mid-year"
mid_years <- 2013:2021
names(mid_years) <- paste0("midyear_", mid_years)

# This will take some time if tracts are not previously cached.
# I've included the full ACS file in the data folder.
#
# Commenting out to avoid re-running
# acs_pulls <- map(mid_years, ~{
#   get_acs(
#     geography = "tract",
#     variables = acs_variables,
#     state = all_states,
#     year = .x + 2,
#     geometry = TRUE,
#     output = "wide"
#   )
# })
#
# write_rds(acs_pulls, "data/acs_pulls.rds")

# Read back in the ACS pulls
acs_pulls <- read_rds("/Users/tylermuffly/Dropbox (Personal)/walker_maps/data/acs_pulls.rds")

# Let's bring in the isochrones
isos <- sf::st_read("/Users/tylermuffly/Dropbox (Personal)/zzTo_Walker_for_consult/data/20241013161700.shp")

# Then generate a unique ID column so we can track results
isos <- isos %>%
  mutate(unique_id = 1:nrow(.))


# As discussed with Tyler: we'll use a "tract centroid within isochrone" approach.
# Given that we are interested in _overall_ accessibility, we'll need to union the isochrones
# by year and range to avoid double-counting tracts within range bands.
isos_union <- isos %>%
  mutate(year = str_sub(departure, 1, 4)) %>%
  group_by(year, range) %>%
  summarize()
st_write(isos_union, "data/isos_union.shp")

# First, we need to get yearly totals for each demographic subgroup.
# This will serve as the accessibility denominator.
years <- 2013:2022
names(years) <- years

yearly_tables <- purrr::map(years, function(x) {
  ix <- paste0("midyear_", x)
  # Overwrite for '22 (just pay attention to this)
  if (x == 2022) {
    ix <- "midyear_2021"
  }
  acs_table <- acs_pulls[[ix]]

  iso_year <- dplyr::filter(isos_union, year == x)

  totals_by_year <- acs_table %>%
    dplyr::select(-dplyr::ends_with("M")) %>%
    dplyr::rename_with(.fn = ~stringr::str_remove(.x, "E$")) %>%
    sf::st_drop_geometry() %>%
    dplyr::summarize(dplyr::across(total_female:total_female_hipi,
                                   .fns = ~sum(.x, na.rm = TRUE)))
  

})
write_rds(yearly_tables, "data/yearly_tables.rds")


# We'll now convert our ACS data to centroids prior to the isochrone spatial join.
acs_centroids <- map(acs_pulls, function(x) {
  x %>%
    st_centroid()
})
write_rds(acs_centroids, "data/acs_centroids.rds")

# We'll now need to tabulate for each isochrone, for each year.
isos_tabulated <- map_dfr(years, function(x) {
  # In the isos table, we first extract the year
  iso_year <- dplyr::filter(isos, str_sub(departure, 1, 4) == x)

  # We can "roll up" tract centroids to each isochrone
  ix <- paste0("midyear_", x)

  # Using the 2019-2023 ACS for 2022 data per Tyler
  # We are dropping 2023 isos pending additional conversation
  if (x == 2022) {
    ix <- "midyear_2021"
  }

  acs_year <- acs_centroids[[ix]] %>%
    dplyr::select(-dplyr::ends_with("M")) %>%
    dplyr::rename_with(.fn = ~stringr::str_remove(.x, "E$"))

  # Run the spatial join
  isos_joined <- iso_year %>%
    st_join(st_transform(acs_year, 4326)) %>%
    st_drop_geometry() %>%
    group_by(id, rank, departure, arrival, range, unique_id) %>%
    summarize(across(total_female:total_female_hipi,
                     .fns = ~sum(.x, na.rm = TRUE))) %>%
    ungroup()

  isos_joined

})
write_rds(isos_tabulated, "data/isos_tabulated.rds")

# After that, we will need to tabulate by range.
# We need to use the unioned isochrones to do this to
# avoid any double-counting.
access_by_year <- map_dfr(years, function(x) {
  # In the isos table, we first extract the year
  iso_year <- dplyr::filter(isos_union, year == x)

  # We can "roll up" tract centroids to each isochrone
  ix <- paste0("midyear_", x)

  # Using the 2019-2023 ACS for 2022 data per Tyler
  # We are dropping 2023 isos pending additional conversation
  if (x == 2022) {
    ix <- "midyear_2021"
  }

  acs_year <- acs_centroids[[ix]] %>%
    dplyr::select(-dplyr::ends_with("M")) %>%
    dplyr::rename_with(.fn = ~stringr::str_remove(.x, "E$"))

  # Run the spatial join
  isos_joined <- iso_year %>%
    st_join(st_transform(acs_year, 4326)) %>%
    st_drop_geometry() %>%
    group_by(year, range) %>%
    summarize(across(total_female:total_female_hipi,
                     .fns = ~sum(.x, na.rm = TRUE))) %>%
    ungroup()

  isos_joined

})
write_rds(access_by_year, "data/access_by_year.rds")

# From here, we can align with the yearly totals to determine percentages
# Let's convert to long form, join on the year,
# then get the percentages
access_by_year_long <- access_by_year %>%
  pivot_longer(
    total_female:total_female_hipi,
    names_to = "category",
    values_to = "count"
  )

# Collapse the yearly totals into a similar structure
yearly_long <- yearly_tables %>%
  bind_rows(.id = "year") %>%
  pivot_longer(
    -year,
    names_to = "category",
    values_to = "total"
  )

access_merged <- left_join(
  access_by_year_long,
  yearly_long,
  by = c("year", "category")
) %>%
  mutate(percent = 100 * (count / total))
write_rds(access_merged, "data/access_merged.rds")

# The `access_merged` table will get you demographic groups and percentages
# that we want

# Rural vs. urban accessibility by demographic group
# We'll use Census Bureau-defined urban areas to accomplish this
urban <- tigris::urban_areas(year = 2022)

# For Census tracts, we can create an urban vs. rural flag,
# taking care to get both 2010 and 2020 tracts
tracts2010 <- tigris::tracts(year = 2019, cb = TRUE)

urban_tracts <- tracts2010 %>%
  st_centroid() %>%
  st_filter(urban) %>%
  select(GEOID)

urban_tract_dict <- tracts2010 %>%
  select(GEOID) %>%
  mutate(urban = ifelse(GEOID %in% urban_tracts$GEOID, "urban", "rural")) %>%
  st_drop_geometry()

tracts2020 <- tigris::tracts(year = 2023, cb = TRUE)

urban_tracts20 <- tracts2020 %>%
  st_centroid() %>%
  st_filter(urban) %>%
  select(GEOID)
st_write(urban_tract_dict20, "data/urban_tracts20.shp")

urban_tract_dict20 <- tracts2020 %>%
  select(GEOID) %>%
  mutate(urban = ifelse(GEOID %in% urban_tracts20$GEOID, "urban", "rural")) %>%
  st_drop_geometry()

st_write(urban_tract_dict20, "data/urban_tract_dict20.shp")

# As Tyler mentioned, all practitioners will be in urban areas, so we are most interested in tabulating disparities among the demand population
# We'll need to break out race by urban vs. rural, which can be done by tabulating over urban and rural tracts

# This will require a slight modification of the above code to include the urban flag
yearly_tables_urban <- map(years, function(x) {
  ix <- paste0("midyear_", x)
  # Overwrite for '22 (just pay attention to this)
  if (x == 2022) {
    ix <- "midyear_2021"
  }
  acs_table <- acs_pulls[[ix]]

  if (x > 2017) {
    acs_table <- acs_table %>%
      left_join(urban_tract_dict20, by = "GEOID")
  } else {
    acs_table <- acs_table %>%
      left_join(urban_tract_dict, by = "GEOID")
  }

  iso_year <- dplyr::filter(isos_union, year == x)

  totals_by_year <- acs_table %>%
    select(!ends_with("M")) %>%
    rename_with(.fn = ~str_remove(.x, "E$")) %>%
    st_drop_geometry() %>%
    summarize(across(total_female:total_female_hipi,
                     .fns = ~sum(.x, na.rm = TRUE)),
              .by = "urban") %>%
    filter(!is.na(urban))

})

# Now we re-run the unioned isochrones with the urban flag
urban_access_by_year <- map_dfr(years, function(x) {
  # In the isos table, we first extract the year
  iso_year <- dplyr::filter(isos_union, year == x)

  # We can "roll up" tract centroids to each isochrone
  ix <- paste0("midyear_", x)

  # Using the 2019-2023 ACS for 2022 data per Tyler
  # We are dropping 2023 isos pending additional conversation
  if (x == 2022) {
    ix <- "midyear_2021"
  }

  acs_year <- acs_centroids[[ix]] %>%
    select(!ends_with("M")) %>%
    rename_with(.fn = ~str_remove(.x, "E$"))

  # Add the urban flag
  if (x > 2017) {
    acs_year <- acs_year %>%
      left_join(urban_tract_dict20, by = "GEOID")
  } else {
    acs_year <- acs_year %>%
      left_join(urban_tract_dict, by = "GEOID")
  }

  # Run the spatial join
  isos_joined <- iso_year %>%
    st_join(st_transform(acs_year, 4326)) %>%
    st_drop_geometry() %>%
    group_by(year, range, urban) %>%
    summarize(across(total_female:total_female_hipi,
                     .fns = ~sum(.x, na.rm = TRUE))) %>%
    ungroup() %>%
    filter(!is.na(urban))

  isos_joined

})

# From here, we can align with the yearly totals to determine percentages
# I'd probably like to convert to long form, join on the year,
# then get the percentages
urban_access_by_year_long <- urban_access_by_year %>%
  pivot_longer(
    total_female:total_female_hipi,
    names_to = "category",
    values_to = "count"
  )

# Collapse the yearly totals into a similar structure
urban_yearly_long <- yearly_tables_urban %>%
  bind_rows(.id = "year") %>%
  pivot_longer(
    -c(year, urban),
    names_to = "category",
    values_to = "total"
  )
write_rds(urban_yearly_long, "data/urban_yearly_long.rds")

urban_access_merged <- left_join(
  urban_access_by_year_long,
  urban_yearly_long,
  by = c("year", "category", "urban")
) %>%
  mutate(percent = 100 * (count / total))

write_rds(urban_access_merged, "data/urban_access_merged.rds")

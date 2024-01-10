#######################
source("R/01-setup.R")
#######################

#This code is designed to evaluate the accessibility of obgyn specialists for the female population across the United States. It begins by gathering essential census data, including demographic information for block groups, and creating a unique identifier for each block group. The code then calculates the degree of overlap between these block groups and predefined isochrones, which represent areas reachable within a specified time frame. The resulting overlap data is saved for future reference. Next, the code combines this overlap information with the demographic data, facilitating a comprehensive analysis. It calculates summary statistics, particularly the percentage of the female population residing within a certain distance of a gynecologic oncologist, providing valuable insights into healthcare resource distribution. The code concludes by generating a message that communicates these findings, highlighting the proportion of U.S. female residents with proximity to gynecologic oncologists and those residing further away. Overall, this code assists in understanding healthcare accessibility disparities and informs healthcare planning efforts.

#The GEOID for block groups in the United States can be constructed using the following format: STATECOUNTYTRACTBLOCK_GROUP. Specifically:
# STATE is a 2-digit code for the state.
# COUNTY is a 3-digit code for the county.
# TRACT is a 6-digit code for the census tract.
# BLOCK_GROUP is a 1-digit code for the block group within the tract.

us_fips_list <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  dplyr::filter(state_code < 56) %>%
  dplyr::select(state_code) %>%
  dplyr::pull()

#************************************
# GET THE ACS CENSUS VARIABLES
#************************************
#* ???????????????????????????????????????????????????????????????????
all_census_data <- tyler::get_census_data(us_fips_list = us_fips_list)

demographics_bg <- all_census_data %>%
  dplyr::rename(name = NAME,
         population = B01001_026E,
         #total female population
         fips_state = state) %>%
  dplyr::mutate(fips_block_group = paste0(
    fips_state,
    county,
    str_pad(tract, width = 6, pad = "0"),
    block_group
  ),) %>%
  dplyr::arrange(fips_state) %>%
  dplyr::select(fips_block_group, name, population)

head(demographics_bg)
 
# And finally! Multiply the population of each block group by its overlap percent to calculate population within and not within ???45???-minutes of a gynecologic oncologist.
#
# First, make a flat non-sf dataframe with the overlap information and join to population. Then multiply and summarize. This is the easiest part of the project.

bg_overlap <- block_groups %>% dplyr::select(geoid = GEOID, overlap) %>%
	sf::st_drop_geometry()
bg_overlap <- as.data.frame(bg_overlap)
write.csv(bg_overlap, "data/09-get-census-population/block-group-isochrone-overlap.csv", na = "", row.names = F)
write_rds(bg_overlap, "data/09-get-census-population/bg_overlap.rds")

write_rds(demographics_bg, "data/09-get-census-population/demographics_bg.rds") #Census Block Group Code

class(bg_overlap$geoid) == class(demographics_bg$fips_block_group)

# Join data
bg <- left_join(bg_overlap, demographics_bg, by = c("geoid" = "fips_block_group"))
bg

# Calculate!
state_sums <- bg %>% select(geoid, overlap, population) %>%
	summarize(within_total = sum(population * overlap, na.rm = TRUE),
						population_total = sum(population, na.rm = TRUE)) %>%
	mutate(within_total_pct = within_total/population_total) %>%
	ungroup()

head(state_sums)
within_total_pct <- round(state_sums[[3]]*100, 2)
n_population <- format(round(state_sums[[1]], 0), big.mark = ",")

paste0(within_total_pct, "% (N = ", n_population, ") of US female residents live within ??45?? minutes of a gynecologic oncologist, while the remaining", 100L - within_total_pct, "% live further away.")

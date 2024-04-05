# The non-commented code focuses on analyzing the accessibility of obstetrics and gynecology (OBGYN) specialists for the female population in the United States, leveraging demographic data from the American Community Survey (ACS). Here’s a condensed summary of its operations:
#
# 2. **Retrieval of Census Data**: The script uses a custom function to fetch census data for a list of state FIPS codes. This step gathers detailed demographic information across various geographic levels, such as states, counties, tracts, and block groups.
#
# 3. **Data Cleaning and Transformation**: Following data retrieval, it undergoes processing to rename columns, compute the total female population, and generate a unique identifier for each block group (GEOID) by concatenating state, county, and tract codes.
#
# 4. **Overlap Calculation with Isochrones**: It calculates the overlap between census block groups and predefined isochrones to assess the accessibility of OBGYN specialists. This involves reading a CSV file with overlap data, selecting pertinent columns, and preparing this data for analysis.
#
# 5. **Data Joining and Analysis**: The script merges demographic data with overlap information, facilitating an assessment of healthcare accessibility. This combined data allows for the computation of both the absolute and relative female population within specified distances from a gynecologic oncologist.
#
# 6. **Summary and Output**: The concluding steps summarize the dataset to emphasize the proportion of the U.S. female population within reach of gynecologic oncologists. It generates a textual summary of these findings, highlighting healthcare accessibility disparities and supporting planning efforts.
#
# This script presents a detailed method for assessing healthcare accessibility using geographic and demographic data, focusing on data manipulation, geographical analysis, and summarization to yield insightful conclusions.

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
all_census_data <- tyler::get_census_data(us_fips_list = us_fips_list)
#write_csv(all_census_data, "data/09-get-census-population/all_census_data.csv")
all_census_data <- read_csv("data/09-get-census-population/all_census_data.csv")

# > all_census_data
# A tibble: 217,329 × 24
 #   state county tract block_group NAME  B01001_001E B01001_026E B01001_033E B01001_034E B01001_035E B01001_036E
 #   <chr> <chr>  <chr>       <dbl> <chr>       <dbl>       <dbl>       <dbl>       <dbl>       <dbl>       <dbl> 
 # 1 01   039    9620…           2 Bloc…         884         543           0          31           9          10        
 # 2 01  039    9618…           2 Bloc…        1395         777           0          27          23          44

#************************************
# CLEANS THE ACS CENSUS VARIABLES
#************************************
#*
#* #total female population
demographics_bg <- all_census_data %>%
  dplyr::rename(name = NAME,
         population = B01001_026E, #total female population
         fips_state = state) %>%
  dplyr::mutate(fips_block_group = paste0(
    fips_state,
    county,
    str_pad(tract, width = 6, pad = "0"),
    block_group
  ),) %>%
  dplyr::arrange(fips_state) %>%
  dplyr::select(fips_block_group, name, population); demographics_bg

write_csv(demographics_bg, "data/09-get-census-population/demographics_bg.csv") #Census Block Group Code

# This is what we used in Exploratory.io for demographics_bg_raw.  
head(demographics_bg)
 
# And finally! Multiply the population of each block group by its overlap percent to calculate population within and not within ???45???-minutes of a gynecologic oncologist.
#
# First, make a flat non-sf dataframe with the overlap information and join to population. Then multiply and summarize. This is the easiest part of the project.
intersect_block_group_cleaned_file <- "data/08-get-block-group-overlap/intersect_block_group_cleaned_180minutes.csv"
intersect_block_group_cleaned <- read_csv(intersect_block_group_cleaned_file) %>%
  arrange(GEOID) %>%
  select(-LSAD, -AWATER)

bg_overlap <- intersect_block_group_cleaned %>% dplyr::select(GEOID, overlap, intersect_drive_time) %>%
	sf::st_drop_geometry()

write_csv(bg_overlap, "data/09-get-census-population/block-group-isochrone-overlap.csv")

class(bg_overlap$GEOID) == class(demographics_bg$fips_block_group)

# Join data of block group overlap and Census bureau demographics
bg <- left_join(bg_overlap, demographics_bg, by = c("GEOID" = "fips_block_group")); bg
write_csv(bg, "data/09-get-census-population/bg.csv")

# Calculate!
state_sums <- bg %>% select(GEOID, overlap, population, intersect_drive_time) %>%
	summarize(population_total = sum(population, na.rm = TRUE),
	          within_total = sum(population * overlap, na.rm = TRUE),
	          intersect_drive_time = intersect_drive_time) %>%
	mutate(within_total_pct = within_total/population_total) %>%
	ungroup() %>%
  distinct(population_total, .keep_all = TRUE); state_sums

within_total_pct <- round(state_sums[[4]]*100, 2); within_total_pct
n_population <- format(round(state_sums[[2]], 0), big.mark = ","); n_population
intersect_drive_time <- state_sums$intersect_drive_time; intersect_drive_time

paste0(within_total_pct, "% (N = ", n_population, ") of US total female residents live within ", intersect_drive_time," minutes of a gynecologic oncologist, while the remaining ", 100L - within_total_pct, "% live further away.")


# Create the Graph called "Many Arkansas, Mississippi Residents Live Far From Stroke Care"
# https://kffhealthnews.org/news/article/appalachia-mississippi-delta-stroke-treatment-advanced-care-rural-access/
# Recreate with GYN Oncologists within a 30-minute drive, a 60-minute drive, a 120-minute drive, a 180-minute drive.  Horizontal bar chart.  


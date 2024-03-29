# Reading and Preparing Data

# 1. Census Data Loading: The script begins by loading demographic data for census block groups from a CSV file. 
# This dataset (census_data_by_bg) likely contains detailed demographic information by block group, 
# including population counts and potentially other socioeconomic indicators.

# 2. Overlap Data Loading and NA Imputation: Next, it loads data describing the overlap between block groups 
# and drive-time isochrones to healthcare services. The impute_na function call (though not directly executable 
# in the given form and should be replaced with if_else(is.na(overlap), 0, overlap)) is intended to replace NA 
# values in the overlap column with 0, indicating no overlap for those entries.

# Calculating Overlaps

# 3. Merging Data: The script then merges the census data with the overlap data on the common identifier 
# (fips_block_group = GEOID). This step aligns each block group’s demographic data with its accessibility 
# to healthcare services measured by the overlap with isochrones.

# 4. Adjusting Demographics by Overlap: It proceeds to adjust demographic numbers and percentages within each 
# block group based on the overlap proportion. For columns ending with _number, it multiplies the demographic 
# counts by the overlap to estimate the portion of each demographic category within the accessible area. 
# Similarly, for columns ending with _pct, it adjusts the percentage figures based on the overlap, though this 
# operation might not be meaningful as the percentages are recalculated from the adjusted counts rather than 
# directly modified by overlap.

# Summarizing Data

# 5. Aggregating Adjusted Data: Finally, the script attempts to aggregate the adjusted data to calculate total 
# and within-accessible-area figures for the entire dataset. This includes total populations, as well as specific 
# demographic totals (e.g., for black, white, NHPI populations) and their respective counts within the isochrones’ 
# overlap. This step is crucial for understanding the broader accessibility patterns across demographic groups.


#######################
source("R/01-setup.R")
#######################

census_data_by_bg <- read_csv("data/08.75-acs/acs-block-group-demographics.csv")
bg_overlap <-       read_csv("data/09-get-census-population/block-group-isochrone-overlap.csv") %>%
  mutate(overlap = impute_na(overlap, type = "value", val = 0))


######
# Granular overlap by each block group in the entire dataset (May be Colorado or USA)
census_data_by_bg_overlap <- left_join(census_data_by_bg, bg_overlap, by = c("fips_block_group" = "GEOID")) %>%
  mutate(across(ends_with("_number"), ~ . * overlap, .names = "overlap_{.col}")) %>%
  # Multiply variables ending with "_pct" by the overlap column
  # and create new columns with prefix "overlap_" plus the old column title ending with "_pct"
  mutate(across(ends_with("_pct"), ~ . * overlap, .names = "overlap_{.col}")); census_data_by_bg_overlap
# View(census_data_by_bg_overlap) #for testing
# write_csv(census_data_by_bg_overlap, "data/09.5-jammin/census_data_by_bg_overlap.csv")


######
# Total overlap across the entire dataset (May be Colorado or USA)
# Based on last step in the tutorial: https://www.hrecht.com/r-drive-time-analysis-tutorial/tutorial.html#Calculate_populations_within_and_further_than_a_45-minute_drive
census_data_by_bg_overlap <- census_data_by_bg_overlap %>%
  summarise(
    within_total = sum(population * overlap),
    population_total = sum(population),
    within_black = sum(race_black_number * overlap),
    population_black = sum(race_black_number),
    within_white = sum(race_white_number * overlap),
    population_white = sum(race_white_number),
    population_nhpi = sum(race_nhpi_number),
    within_nhpi = sum(race_nhpi_number * overlap)) 
View(census_data_by_bg_overlap)

##
# Rows: 117682 Columns: 12
# ── Column specification ─────────────────────────────────────────────────────────────────────────────────────────────────────────
# Delimiter: ","
# chr (10): geoid, name, block_group, tract, county, fips_state, variable, fips_block_group, abbreviated, complete_description
# dbl  (2): overlap, population
# 
# ℹ Use `spec()` to retrieve the full column specification for this data.
# ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
# # A tibble: 117,682 × 12
# geoid  overlap name  block_group tract county fips_state variable population fips_block_group abbreviated complete_description
# <chr>    <dbl> <chr> <chr>       <chr> <chr>  <chr>      <chr>         <dbl> <chr>            <chr>       <chr>               
#   1 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12AE_0…          7 Colorado Jeffer… AI/AN       Female:, AI/AN ALON…
# 2 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12Y_02…         14 Colorado Jeffer… AI/AN       Female:, AI/AN ALON…
# 3 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12C_02…          1 Colorado Jeffer… AI/AN       Female:, AI/AN ALON…
# 4 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12R_02…          0 Colorado Jeffer… AI/AN       Female:, AI/AN ALON…
# 5 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12K_02…          1 Colorado Jeffer… AI/AN       Female:, AI/AN ALON…
# 6 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12AF_0…          1 Colorado Jeffer… ASIAN       Female:, ASIAN ALON…
# 7 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12Z_02…         14 Colorado Jeffer… ASIAN       Female:, ASIAN ALON…
# 8 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12D_02…          8 Colorado Jeffer… ASIAN       Female:, ASIAN ALON…
# 9 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12S_02…          0 Colorado Jeffer… ASIAN       Female:, ASIAN ALON…
# 10 08059…   0.441 Bloc… Block Grou… Cens… Jeffe… Colorado   P12L_02…          8 Colorado Jeffer… ASIAN       Female:, ASIAN ALON…



#************************************
# CHANGED TO A WIDE FORMAT CALCULATE PERCENTAGES
#************************************
# Steps to produce branching_point_1
# `branching_point_1`<- exploratory::read_delim_file("data/08.5-prep-the-census-variables/temp_bg.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) #%>%
#   readr::type_convert() %>%
#   exploratory::clean_data_frame() %>%  # On the Census Demographic Profile they do a "Total races tallied" using "White alone or in combination with one or more races" so I will try that here.
# filter(str_ends(complete_description, fixed(", NOT HISPANIC OR LATINO)")) & str_detect(complete_description, fixed("OR IN COMBINATION WITH ONE OR MORE OTHER RACES"))) %>%
#   reorder_cols(variable, abbreviated)
# 
# # Steps to produce temp_bg_1
# `temp_bg_1` <- `branching_point_1` %>%
#   group_by(abbreviated, geoid) %>%
# 
#   # Bring all of one race together (White, White non-Hispanic, White Hispanic, White Other)
#   summarize_group(group_cols = c(`abbreviated` = "abbreviated",  `geoid` = "geoid"),group_funs = c("none",  "none"),population = sum(population, na.rm = TRUE)) %>%
#   arrange(geoid) %>%
#   rename(all_types_of_race_together_population = population)
# 
# # Steps to produce the output
# `branching_point_1` %>%
#   left_join(`temp_bg_1`, by = join_by(`abbreviated` == `abbreviated`, `geoid` == `geoid`)) %>%
#   distinct(abbreviated, geoid, .keep_all = TRUE) %>%
#   select(-complete_description) %>%
#   mutate(abbreviated = recode(abbreviated, "BLACK" = "race_black_number", "AI/AN" = "race_aian_number", "ASIAN" = "race_asian_number", "NHPI" = "race_nhpi_number", "WHITE" = "race_white_number", type_convert = TRUE)) %>% 
#   #"HISPANIC" = "race_hispanic_number", "TWO" = "race_two_number",
#   select(-variable, -population) %>%
#   pivot_longer(cols = c(`abbreviated`), values_to = 'value', names_to = c("key"), values_drop_na = TRUE, names_repair = 'unique') %>%
#   pivot_wider(names_from = c(value), values_from = c(all_types_of_race_together_population)) %>%
#   select(-key) %>%
#   reorder_cols(race_aian_number, race_asian_number, race_black_number, #race_hispanic_number, race_two_number,
#                race_nhpi_number, race_white_number, overlap, geoid, variable, name, block_group, tract, county, fips_state, fips_block_group, complete_description) %>%
#   group_by(geoid) %>%
#   mutate(race_universal_number = race_aian_number+race_asian_number+race_black_number+race_nhpi_number+race_white_number) %>% #+race_hispanic_number+race_two_number
#   ungroup() %>%
#   reorder_cols(race_universal_number, race_aian_number, race_asian_number, race_black_number, #race_hispanic_number,  race_two_number,
#                race_nhpi_number, race_white_number, geoid, overlap, name, block_group, tract, county,
#                fips_state, fips_block_group) %>%
#   rename(population = race_universal_number) %>%
#   summarise(
#     within_total = sum(population * overlap),
#     population_total = sum(population),
#     within_black = sum(race_black_number * overlap),
#     population_black = sum(race_black_number),
#     within_white = sum(race_white_number * overlap),
#     population_white = sum(race_white_number),
#     population_nhpi = sum(race_nhpi_number),
#     within_nhpi = sum(race_nhpi_number * overlap)#,
#     #population_two_number = sum(race_two_number),
#     #population_hispanic = sum(race_hispanic_number),
#     #within_two_number = sum(race_two_number * overlap),
#     #within_hispanic = sum(race_hispanic_number * overlap)
#   ) %>%
#   reorder_cols(population, race_aian_number, race_asian_number, race_black_number, race_nhpi_number,  race_white_number, geoid, overlap, name, block_group, tract, county, fips_state, fips_block_group, within_total, population_total, within_black, population_black, within_white, population_white,  within_nhpi, population_nhpi) %>%#, race_hispanic_number, race_two_number, within_two_number, population_two_number, within_hispanic, population_hispanic) %>%
# 
#   # In this code:
#   #
#   # across(starts_with("within_")) selects all columns that start with "within_".
#   # list(pct = ~ . / get(paste0("population_", sub("within_", "", cur_column())))) calculates the percentage for each selected column by dividing it by the corresponding population column.
#   # .names = "{.col}_pct" specifies the naming pattern for the new percentage columns.
#   mutate(across(starts_with("within_"),
#                 list(pct = ~ . / get(paste0("population_", sub("within_", "", cur_column())))),
#                 .names = "{.col}_pct")) %>%
#   # select(ends_with("_pct"), everything()) %>%
#   write_csv(., "data/08.5-prep-the-census-variables/demographics_bg.csv") -> abc
# 
# read_csv("data/08.5-prep-the-census-variables/demographics_bg.csv")
# abc
#TODO:  This is broken:  within_total

# Generate descriptions for each column in abc with thousandths commas
# descriptions <- c(
#   paste0("within_total: ", format(abc[[1]], big.mark = ",", scientific = FALSE), ". This column represents the total population count within the specified areas of interest, considering all races and ethnicities."),
#   paste0("population_total: ", format(abc[[2]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the entire dataset, irrespective of the areas of interest or race/ethnicity."),
#   paste0("within_black: ", format(abc[[3]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the Black or African American racial group."),
#   paste0("population_black: ", format(abc[[4]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the Black or African American racial group in the entire dataset."),
#   paste0("within_white: ", format(abc[[5]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the White racial group."),
#   paste0("population_white: ", format(abc[[6]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the White racial group in the entire dataset."),
#   paste0("within_nhpi: ", format(abc[[7]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the Native Hawaiian and Other Pacific Islander (NHPI) racial group."),
#   paste0("population_nhpi: ", format(abc[[8]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the NHPI racial group in the entire dataset."),
#   paste0("within_total_pct: ", format(abc[[9]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the total population within the specified areas relative to the total population in the dataset."),
#   paste0("within_black_pct: ", format(abc[[10]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the Black or African American population within the specified areas relative to the total Black or African American population in the dataset."),
#   paste0("within_white_pct: ", format(abc[[11]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the White population within the specified areas relative to the total White population in the dataset."),
#   paste0("within_nhpi_pct: ", format(abc[[12]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the NHPI population within the specified areas relative to the total NHPI population in the dataset.")
# )
# 
# # Print the descriptions
# for (desc in descriptions) {
#   cat(desc, "\n\n")
# }

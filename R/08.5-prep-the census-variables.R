#######################
source("R/01-setup.R")
#######################
# TODO: get block variables of total female population per block group for non-decennial census years.

# This code is focused on retrieving and preparing census variables for analysis. It starts by loading demographic and housing characteristic variables for the 2020 decennial census. The code filters and cleans these variables to include only the data related to females, excluding males and irrelevant annotations, and then writes the cleaned data to a CSV file. It also extracts ACS (American Community Survey) variables for 2020, specifically targeting variables related to population demographics. The code selects and stores these variables in a data frame and saves them as a CSV file. Overall, the code prepares demographic data for further analysis, focusing on gender and race-related variables.

#************************************
# GET THE DECENNIAL CENSUS VARIABLES
#************************************
#*
# Crosswalk of variables to descriptions downloaded from the ACS API web site.
#https://api.census.gov/data/2020/dec/dhc/variables.html
#write_csv(vars, "data/09-get-census-population/census_DHC.csv") #sorted in Workforce Exploratory.io , named CENSUS_DHC
vars <- load_variables(year = 2020, "dhc") #Demographic and Housing Characteristics summary files


#***********************************************************************
# CLEAN THE DECENNIAL CENSUS VARIABLES INCLUDING BOTH AGE AND RACE
#***********************************************************************
#*
census_variables_prepped <- vars %>%
  # variables with only the prefix of P12_ are SEX BY AGE FOR SELECTED AGE CATEGORIES.  They are summary variables.
  filter(!str_detect(name, fixed("P12_"))) %>%
  # We only want "SEX BY AGE" vars
  filter(str_detect(concept, fixed("SEX BY AGE"))) %>%
  # No males.
  filter(!str_detect(label, fixed("!!Male:!!", ignore_case=TRUE))) %>%
  # No annotations.
  filter(!str_detect(label, fixed("Annotation of Margin of Error", ignore_case=TRUE))) %>%
  mutate(label = str_remove(label, regex("^Annotation of Estimate!!Total:!!Female:!!", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  filter(!str_detect(label, fixed("Margin of Error!!", ignore_case=TRUE))) %>%
  # Removing the fucking !! that are all over this document.  Jesus.
  mutate(label = str_remove(label, regex("^Annotation of Estimate!!Total:!!Female:!!", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  mutate(label = str_remove(label, regex("^Estimate!!Total:!!Female:!!", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  filter(!str_detect(name, fixed("EA")) & !str_detect(label, fixed("!!Male:"))) %>%
  mutate(label = str_remove(label, regex("!!", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  mutate(label = str_remove(label, regex("!!", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  mutate(label = str_remove(label, regex("(!!)", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  # Keep only females.
  filter(str_detect(label, fixed("female", ignore_case=TRUE))) %>%
  mutate(label = str_remove(label, regex("^Total:", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  arrange(concept) %>%
  # Remove QUARTERS.
  filter(!str_detect(concept, fixed("QUARTERS"))) %>%
  mutate(concept = str_replace_all(concept, regex("AMERICAN INDIAN AND ALASKA NATIVE", ignore_case = TRUE), "AI/AN")) %>%
  mutate(concept = str_replace(concept, regex("NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER", ignore_case = TRUE), "NHPI")) %>%
  filter(!str_detect(concept, fixed("HOUSEHOLDS")) & !str_detect(concept, fixed("SOME OTHER RACE")) & !str_detect(concept, fixed("FOR THE POPULATION UNDER 20 YEARS"))) %>%
  mutate(concept = recode(concept, "SEX BY AGE FOR SELECTED AGE CATEGORIES" = "total female population", type_convert = TRUE)) %>%
  # variables with only the prefix of P12_ are SEX BY AGE FOR SELECTED AGE CATEGORIES.  "P12_" are summary variables.
  filter(!str_detect(name, fixed("P12_"))) %>%
  # All ages included, all races included.
  mutate(concept = str_remove_all(concept, "^SEX BY AGE FOR SELECTED AGE CATEGORIES \\(", remove_extra_space = TRUE)) %>%
  rename(description = concept) %>%
  unite(complete_description, label, description, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  mutate(abbreviated = word(description, 1, sep = "\\s+"), .after = ifelse("description" %in% names(.), "description", last_col())) %>%
  select(-label, -description) %>%
  reorder_cols(abbreviated); census_variables_prepped
  # Age and Race included, 696 rows included
##
# > census_variables_prepped
# A tibble: 696 × 3
# abbreviated name       complete_description                                                                             
# <chr>       <chr>      <chr>                                                                                            
#   1 AI/AN       P12AE_026N Female:, AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, HISPANIC OR LATINO)   
# 2 AI/AN       P12AE_027N Female:Under 5 years, AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, HISPANIC OR 
# 3 AI/AN       P12AE_028N Female:5 to 9 years, AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, HISPANIC OR 

write_csv(census_variables_prepped, "data/08.5-prep-the-census-variables/end_census_variables_prepped.csv"); head(census_variables_prepped)

#***********************************************************************
# CLEAN THE DECENNIAL CENSUS VARIABLES FOR TOTALS ONLY
#***********************************************************************
# Totals only
totals_census_variables_prepped <- census_variables_prepped %>%
  filter(str_detect(name, fixed("_026N"))) %>%
  # Only totals by Race.  29 rows
  write_csv(., "data/08.5-prep-the-census-variables/end_totals_census_variables_prepped.csv"); head(totals_census_variables_prepped)

# # A tibble: 6 × 3                                                                                                        
# abbreviated name       complete_description                                                                        
# <chr>       <chr>      <chr>                                                                                       
#   1 AI/AN       P12AE_026N Female:, AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, HISPANIC OR LATINO)    
# 2 AI/AN       P12Y_026N  Female:, AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
# 3 AI/AN       P12C_026N  Female:, AI/AN ALONE)                                                                       
# 4 AI/AN       P12R_026N  Female:, AI/AN ALONE, HISPANIC OR LATINO)                                                   
# 5 AI/AN       P12K_026N  Female:, AI/AN ALONE, NOT HISPANIC OR LATINO)                                               
# 6 ASIAN       P12AF_026N Female:, ASIAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, HISPANIC OR LATINO)


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
descriptions <- c(
  paste0("within_total: ", format(abc[[1]], big.mark = ",", scientific = FALSE), ". This column represents the total population count within the specified areas of interest, considering all races and ethnicities."),
  paste0("population_total: ", format(abc[[2]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the entire dataset, irrespective of the areas of interest or race/ethnicity."),
  paste0("within_black: ", format(abc[[3]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the Black or African American racial group."),
  paste0("population_black: ", format(abc[[4]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the Black or African American racial group in the entire dataset."),
  paste0("within_white: ", format(abc[[5]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the White racial group."),
  paste0("population_white: ", format(abc[[6]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the White racial group in the entire dataset."),
  paste0("within_nhpi: ", format(abc[[7]], big.mark = ",", scientific = FALSE), ". This column represents the population count within the specified areas belonging to the Native Hawaiian and Other Pacific Islander (NHPI) racial group."),
  paste0("population_nhpi: ", format(abc[[8]], big.mark = ",", scientific = FALSE), ". This column represents the total population count for the NHPI racial group in the entire dataset."),
  paste0("within_total_pct: ", format(abc[[9]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the total population within the specified areas relative to the total population in the dataset."),
  paste0("within_black_pct: ", format(abc[[10]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the Black or African American population within the specified areas relative to the total Black or African American population in the dataset."),
  paste0("within_white_pct: ", format(abc[[11]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the White population within the specified areas relative to the total White population in the dataset."),
  paste0("within_nhpi_pct: ", format(abc[[12]], big.mark = ",", scientific = FALSE), ". This column represents the percentage of the NHPI population within the specified areas relative to the total NHPI population in the dataset.")
)

# Print the descriptions
for (desc in descriptions) {
  cat(desc, "\n\n")
}

#************************************
# GET THE ACS CENSUS VARIABLES
#************************************
vars_acs <- tidycensus::load_variables(year = 2020, "acs5")

#https://api.census.gov/data/2022/acs/acs1/variables.html
 acs_variables_to_search <- c(
   "NAME",
   paste0("B01001_0", c("17", "E")),
   paste0("B01001A_017E"),
   paste0("B01001B_017E"),
   paste0("B01001C_017E"),
   paste0("B01001D_017E"),
   paste0("B01001E_017E")) %>% as.data.frame()

 # Totals only
 acs_totals_variables_prepped <- acs_variables_to_search [-1, ] %>%
   as.data.frame(); acs_totals_variables_prepped

   write_csv(acs_totals_variables_prepped, "data/08.5-prep-the-census-variables/end_acs_totals_census_variables_prepped.csv"); head(acs_totals_variables_prepped)
   
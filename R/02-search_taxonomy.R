#######################
source("R/01-setup.R")

# ---------------------------------------------------------------------------
# Constants for subspecialty searches and taxonomy descriptions
# ---------------------------------------------------------------------------
SUBSPECIALTIES <- c(
  "Gynecologic Oncology",
  "Urogynecology and Reconstructive Pelvic Surgery",
  "Reproductive Endocrinology",
  "Maternal & Fetal Medicine"
)

TAXONOMY_DESCRIPTIONS <- c(
  "Obstetrics & Gynecology, Female Pelvic Medicine and Reconstructive Surgery",
  "Obstetrics & Gynecology, Gynecologic Oncology",
  "Obstetrics & Gynecology, Maternal & Fetal Medicine",
  "Obstetrics & Gynecology, Reproductive Endocrinology",
  "Obstetrics & Gynecology, Urogynecology and Reconstructive Pelvic Surgery"
)

TAXONOMY_RECODE_MAP <- c(
  "Obstetrics & Gynecology, Female Pelvic Medicine and Reconstructive Surgery" = "FPM",
  "Obstetrics & Gynecology, Gynecologic Oncology" = "ONC",
  "Obstetrics & Gynecology, Maternal & Fetal Medicine" = "MFM",
  "Obstetrics & Gynecology, Reproductive Endocrinology" = "REI"
)
#######################

# The code you provided appears to be R code that performs the following tasks:
# 1. It sources an R script called "01-setup.R," which likely contains some setup or configuration code.
# 2. It defines a function called "search_by_taxonomy" to retrieve data related to different subspecialties in the field of Obstetrics & Gynecology (OBGYN).
# 3. It calls the "search_by_taxonomy" function four times with different subspecialties: "Gynecologic Oncology," "Female Pelvic Medicine and Reconstructive Surgery," "Reproductive Endocrinology," and "Maternal & Fetal Medicine."
# 4. It then merges the data obtained from the four calls into a single data frame called "all_taxonomy_search_data" using the "bind_rows" function from the dplyr package.
# 5. The code cleans and processes the "all_taxonomy_search_data" data frame. It filters the data to keep only rows with specific taxonomy descriptions related to OBGYN subspecialties, extracts the first five characters of the postal code, converts the "basic_enumeration_date" to a date object and extracts the year, shortens the "basic_middle_name" to a single character, and removes punctuation from names.
# 6. It renames and recodes some columns for clarity and consistency.
# 7. Finally, it writes the cleaned data frame to an RDS file named "end_cleaned_all_taxonomy_search_data.rds" in the "data/02-search_taxonomy" directory.

##########################################################################
# search_by_taxonomy
# This will allow us to get subspecialty names and NPI numbers as a starting point.  FYI the limit is 1,200 physicians.  
# Search the NPI Registry database using npi_search.  Taxonomy description from the NUCC: https://taxonomy.nucc.org/ 

search_results_list <- lapply(SUBSPECIALTIES, search_by_taxonomy)

# Merge all data frames of each of the subspecialties into one
all_taxonomy_search_data <- dplyr::bind_rows(search_results_list) %>%
  dplyr::distinct(npi, .keep_all = TRUE)


cleaned_all_taxonomy_search_data <-
  all_taxonomy_search_data %>%
  distinct(npi, .keep_all = TRUE) %>%
  # Keep only the OBGYN subspecialist taxonomy descriptions.
  filter(
    taxonomies_desc %in% TAXONOMY_DESCRIPTIONS
  ) %>%
  mutate(addresses_postal_code = str_sub(addresses_postal_code,1 ,5)) %>% # Extract the first five of the zip code
  mutate(basic_enumeration_date = ymd(basic_enumeration_date)) %>%
  mutate(basic_enumeration_date_year = year(basic_enumeration_date), .after = ifelse("basic_enumeration_date" %in% names(.), "basic_enumeration_date", last_col())) %>% # Pull the year out of the enumeration full data.
  mutate(basic_middle_name = str_sub(basic_middle_name,1 ,1)) %>%
  mutate(across(c(basic_first_name, basic_last_name, basic_middle_name), .fns = ~str_remove_all(., "[[\\p{P}][\\p{S}]]"))) %>%
  # Get data ready to add these taxonomy rows to the `search_npi`/GOBA data set.

  rename(NPI = npi, first_name = basic_first_name, last_name = basic_last_name, middle_name = basic_middle_name, GenderPhysicianCompare = basic_gender, sub1 = taxonomies_desc, city = addresses_city, state = addresses_state, name.x = full_name, `Zip CodePhysicianCompare` = addresses_postal_code) %>%
  mutate(GenderPhysicianCompare = recode(GenderPhysicianCompare, "F" = "Female", "M" = "Male")) %>%

  # Show the subspecialty from goba.
  mutate(sub1 = recode(sub1, !!!TAXONOMY_RECODE_MAP))

 
write_rds(cleaned_all_taxonomy_search_data, "data/02-search_taxonomy/end_cleaned_all_taxonomy_search_data.rds")

#**********************************************
# SANITY CHECK
#**********************************************
#* cleaned_all_taxonomy_search_data <- read_rds("data/02-search_taxonomy/end_cleaned_all_taxonomy_search_data.rds")
dim(cleaned_all_taxonomy_search_data)
glimpse(cleaned_all_taxonomy_search_data)
janitor::tabyl(cleaned_all_taxonomy_search_data$sub1)
janitor::tabyl(cleaned_all_taxonomy_search_data$GenderPhysicianCompare)
janitor::tabyl(cleaned_all_taxonomy_search_data$state)


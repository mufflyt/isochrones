##########################################################################
# tyler::search_and_process_npi
# Filter the subspecialists_only.csv file/goba file to those without NPI numbers.  Then send those names into the search_and_process_npi

#######################
source("R/01-setup.R")
#######################

# Read and Rename Columns: It begins by reading a CSV file that contains information about subspecialists. The code renames specific columns in this dataset to more convenient names.
# 
# Filter Rows: Rows in the dataset are filtered to select only those where the National Provider Identifier (NPI) number is missing (NA), indicating subspecialists without NPI numbers. The filtered data is then saved as a new CSV file.
# 
# Retrieve NPI Numbers: A custom function (search_and_process_npi) is used to retrieve NPI numbers for the subspecialists who lack them. The retrieved NPI numbers are processed, including removing punctuation, converting credential information to uppercase, and filtering based on specific criteria. The processed NPI data is saved as another CSV file.
# 
# Read Original Subspecialty Data: The original dataset containing subspecialist information is read again, and the NPI column is converted to a numeric format.
# 
# Coalesce NPI Numbers: The missing NPI numbers in the original subspecialist dataset are filled in (coalesced) with the NPI numbers from the previous step. Unnecessary columns from the NPI dataset are removed.
# 
# Combine DataFrames: The code combines the datasets related to NPI numbers and taxonomy information into a single dataset. This merged dataset likely contains comprehensive information about subspecialists, including their NPI numbers and taxonomy data.
# 
# Merge Rows: Rows are merged from the taxonomy dataset and the NPI dataset to include younger subspecialists who have a taxonomy code but not board certification yet. Additional data transformations, filtering, and column manipulation are performed during this step.
# 
# Save Results: The final merged dataset is saved in RDS (R Data Store) format for further analysis or use in other R scripts.

### Read in file and clean it up
# File Provenance: "/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/goba/subspecialists_only.csv"
filtered_subspecialists <- readr::read_csv("data/03-search_and_process_npi/subspecialists_only.csv") %>%
  dplyr::rename(first = first_name) %>%
  dplyr::rename(last = last_name) %>%
  dplyr::filter(is.na(NPI)) %>%
  readr::write_csv("data/03-search_and_process_npi/filtered_subspecialists_only.csv")

#**************************
#* GET NPI NUMBERS for those that do not have any in subspecialists.csv, using search_and_process_npi
input_file <- "data/03-search_and_process_npi/filtered_subspecialists_only.csv"
output_result <- search_and_process_npi(input_file) #Runs the function to get data from the NPPES website

searched_npi_numbers <- output_result %>%
  dplyr::distinct(npi, .keep_all = TRUE) %>%
  dplyr::mutate(across(c(basic_first_name, basic_last_name, basic_credential),
                .fns = ~sstringr::tr_remove_all(., "[[\\p{P}][\\p{S}]]"))) %>%
  dplyr::mutate(basic_credential = stringr::str_to_upper(basic_credential)) %>%
  dplyr::filter(stringr::str_detect(basic_credential, "MD|DO")) %>%
  dplyr::mutate(basic_credential = stringr::str_sub(basic_credential,1 ,2)) %>%
  dplyr::filter(basic_credential %in% c("DO", "MD")) %>%
  dplyr::filter(stringr::str_detect(taxonomies_desc, fixed("Gyn", ignore_case=TRUE))) %>%
  dplyr::distinct(npi, .keep_all = TRUE) %>%
  dplyr::mutate(npi = as.numeric(npi)) %>%
  readr::write_csv("data/03-search_and_process_npi/searched_npi_numbers.csv") ### File with NPI numbers to complete subspecialty.csv file.    We need to merge the results of new NPI numbers in `searched_npi_numbers` with subspecialty.csv

### Read in the original subspecialty.csv file.  This file is given and is not calculated earlier in the workflow.  
# File Provenance: "/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/goba/subspecialists_only.csv"
subspecialists_only <- read_csv("data/03-search_and_process_npi/subspecialists_only.csv") %>%
  mutate(NPI = as.numeric(NPI))

class(subspecialists_only$NPI) == class(searched_npi_numbers$npi)

# Coalesce NPI numbers to fill NA gaps in `subspecialists_only`
all_NPI_numbers_we_will_ever_find <-
  subspecialists_only %>%
  exploratory::left_join(searched_npi_numbers, by =
                           c("first" = "basic_first_name",
                             "last" = "basic_last_name"), ignorecase=TRUE) %>%
  dplyr::mutate(NPI = dplyr::coalesce(NPI, npi)) %>% #Fills in the NPI numbers with NA
  dplyr::select(-npi, -basic_credential, -basic_sole_proprietor, -basic_gender, - basic_enumeration_date, -basic_last_updated, -basic_status, -basic_name_prefix, -taxonomies_code,  -taxonomies_taxonomy_group, -taxonomies_desc, -taxonomies_state, -taxonomies_license, -taxonomies_primary, -basic_middle_name, -basic_name_suffix, -basic_certification_date) # removes the unneeded rows from `searched_npi_numbers`

### Bring together the rows of the taxonomy data and the rows of the goba NPI numbers
taxonomy_plus_NPI <- all_NPI_numbers_we_will_ever_find %>%
  exploratory::bind_rows(all_taxonomy_search_data, id_column_name = "ID", current_df_name = "subspecialists_only",         force_data_type = TRUE) %>%
  dplyr::distinct(NPI, .keep_all = TRUE)

### Merge rows of `all_taxonomy_search_data` from 02-search_taxonomy and `searched_npi_numbers` from 03-search_and_process_npi 
# Brings in the younger subspecialists who have a taxonomy code but not board-certification yet.  Board certification for OBGYN usually takes 2 years after graduating from fellowship.  
complete_npi_for_subspecialists <- all_NPI_numbers_we_will_ever_find %>%
  exploratory::bind_rows(all_taxonomy_search_data, id_column_name = "ID", current_df_name = "subspecialists_only", force_data_type = TRUE) %>%
  dplyr::distinct(NPI, .keep_all = TRUE) %>%
  dplyr::select(-ID, -userid, -startDate, -certStatus, -sub1startDate, -sub1certStatus, -honorrific_end, -`Medical school namePhysicianCompare`, -`Graduation yearPhysicianCompare`, -`Organization legal namePhysicianCompare`, -`Number of Group Practice membersPhysicianCompare`, -`Professional accepts Medicare AssignmentPhysicianCompare`, -search_term, -basic_sole_proprietor, -basic_enumeration_date, -taxonomies_primary, -addresses_address_1, -addresses_telephone_number, -npi, -name.x, -basic_first_name, -basic_last_name, -basic_middle_name, -basic_gender, -taxonomies_desc, -addresses_city, -addresses_state, -addresses_postal_code, -full_name) %>%
  dplyr::mutate(row_number = dplyr::row_number()) %>%
  dplyr::mutate(zip = stringr::str_sub(`Zip CodePhysicianCompare`,1 ,5), .after = ifelse("Zip CodePhysicianCompare" %in% names(.), "Zip CodePhysicianCompare", last_col())) %>%
  dplyr::filter(!is.na(state) & !is.na(city)) %>%

  # TODO: i need to change this back eventually.
  dplyr::mutate(zip = exploratory::impute_na(zip, type = "value", val = "")) %>%
  tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  dplyr::distinct(address, .keep_all = TRUE) %>%
  readr::write_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds")

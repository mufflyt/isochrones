# Workflow for Geographic Disparities in Potential Accessibility to Obstetrics and Gynecology Subspecialists in the United States from 2017 to 2023

source("R/01-setup.R")
source("R/02-search_taxonomy.R")
source("R/03-search_and_process_npi.R")
source("R/03-search_and_process_npi_extra.R")
source("R/04-geocode.R")
source("R/05-geocode-cleaning.R")
source("R/06-isochrones.R")
source("R/07-isochrone-mapping.R")
source("R/07.5-prep-get-block-group-overlap.R")
source("08-get-block-group-overlap.R")
source("08.5-prep-the census-variables.R")
source("09-get-census-population.R")

##########################################################################

      ########################################################
      # Sanity check for dot map
      #######################################################

      # Change the sf object into lat and long columns
geocoded_data <- readr::read_csv("/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/NPPES/NPPES_November_filtered_data_for_geocoding_geocoded_addresses.csv") %>%
        mutate(id = 1:n()) %>%
        mutate(postal_code = stringr::str_sub(postal_code,1 ,5)) %>%
        mutate(access = exploratory::str_remove(access, regex("^POINT \\(", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
        mutate(access = exploratory::str_remove(access, regex("\\)$", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
        separate(access, into = c("lat", "long"), sep = "\\s+", convert = TRUE) %>%
        select(-id, -rank, -type, -district, -state) %>%
        dplyr::filter(country %in% c("United States", "Puerto Rico")) %>%
        mutate(postal_code = str_sub(postal_code,1 ,5)) %>%
        rename(zip = postal_code) %>%
        mutate(across(c(lat, long), parse_number)) %>%
        dplyr::filter(!is.na(lat))

      create_and_save_physician_dot_map(physician_data = geocoded_data, color_palette = "magma", popup_var = "name")


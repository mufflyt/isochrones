##########################################################################
### RUN on search_and_process_npi second machine overnight ### OR ### Run against the database ####
### Retrieve clinician data using "tyler::retrieve_clinician_data"
#readr::read_csv("data/complete_npi_for_subspecialists.csv")
source("R/01-setup.R")

input_data <- ("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds")
clinician_data <- tyler::search_and_process_npi(input_data)
readr::write_csv(clinician_data, "data/clinician_data.csv")

#readr::read_csv("data/clinician_data.csv")

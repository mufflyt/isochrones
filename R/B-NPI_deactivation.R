

library(tidyverse)
library(readxl)

# Read the deactivated NPI file, skipping the first row and assigning column names
deactivated_npi <- readxl::read_xlsx(
  path = "data/nppes_deactivated_downloads/NPPES Deactivated NPI Report 20250414.xlsx",
  skip = 2,
  col_names = c("NPI", "nppes_deactivation_date")
) %>%
  dplyr::mutate(
    nppes_deactivation_date = lubridate::mdy(nppes_deactivation_date),
    nppes_deactivation_date = lubridate::year(nppes_deactivation_date)
  ); deactivated_npi

readr::write_csv(deactivated_npi, "data/nppes_deactivated_downloads/deactivated_npi.csv")

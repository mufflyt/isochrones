#######################
source("R/01-setup.R")
#######################

# File Path Constants ----
NPPES_DEACTIVATED_DIR <- "data/nppes_deactivated_downloads"
NPPES_DEACTIVATED_XLSX <- file.path(NPPES_DEACTIVATED_DIR, "NPPES Deactivated NPI Report 20250414.xlsx")
NPPES_DEACTIVATED_CSV <- file.path(NPPES_DEACTIVATED_DIR, "deactivated_npi.csv")



library(tidyverse)
source("R/01-setup.R")
library(readxl)

# Read the deactivated NPI file, skipping the first row and assigning column names
deactivated_npi <- readxl::read_xlsx(
  path = NPPES_DEACTIVATED_XLSX,
  skip = 2,
  col_names = c("NPI", "nppes_deactivation_date")
) %>%
  dplyr::mutate(
    nppes_deactivation_date = lubridate::mdy(nppes_deactivation_date),
    nppes_deactivation_date = lubridate::year(nppes_deactivation_date)
  ); deactivated_npi

readr::write_csv(deactivated_npi, NPPES_DEACTIVATED_CSV)

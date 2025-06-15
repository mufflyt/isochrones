# Must download by hand from https://data.cms.gov/provider-data/dataset/27ea-46a8
# Facility Affiliation CMS Data was first published by the Centers for Medicare & Medicaid Services (CMS) in August 2023. The most recent dataset was released to the public on April 17, 2025.
source("R/01-setup.R")

library(tidyverse)

facility_affiliation <- read_csv("/Users/tylermuffly/Dropbox (Personal)/isochrones/data/facility_affiliation/Facility_Affiliation.csv"); facility_affiliation

facility_affiliation_output <- facility_affiliation %>%
  dplyr::distinct(NPI, Ind_PAC_ID, .keep_all = TRUE) %>%
  dplyr::select(-suff) %>%
  filter(facility_type == "Hospital") %>%
  dplyr::mutate(year = 2025)

names(facility_affiliation_output)
facility_affiliation_output

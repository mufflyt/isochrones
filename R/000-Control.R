# Workflow for Geographic Disparities in Potential Accessibility to Obstetrics and Gynecology Subspecialists in the United States from 2017 to 2023

source("R/01-setup.R")
source("R/02-search_taxonomy.R")
source("R/03-search_and_process_npi.R")
source("R/03a-search_and_process_extra.R")
source("R/04-geocode.R")
source("R/06-isochrones.R")
source("R/07-isochrone-mapping.R")
source("R/07.5-prep-get-block-group-overlap.R")
source("R/08-get-block-group-overlap.R")
source("R/08.5-prep-the census-variables.R")
source("R/09-get-census-population.R")
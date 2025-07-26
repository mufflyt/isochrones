# Workflow for Geographic Disparities in Potential Accessibility to Obstetrics and Gynecology Subspecialists in the United States from 2017 to 2023

source("R/01-setup.R")
source("R/02-nber_nppes.R")
source("R/03-search_and_process_npi.R")
source("R/03a-search_and_process_extra.R")
source("R/04-geocode.R")
source("R/06-isochrones.R")
source("R/07-isochrone-mapping.R")
source("R/07.5-prep-get-block-group-overlap.R")
source("R/08-get-block-group-overlap.R")
source("R/08.5-prep-the census-variables.R")
source("R/09-get-census-population.R")

#===============================================================================
# RECOMMENDED NBER/NPPES PROCESSING PIPELINE
#===============================================================================

# PHASE 1: Data Preparation (File B)
source("R/B-nber_nppes_combine_columns.R")
# → Produces: clean, standardized combined_obgyn_providers.csv

# PHASE 2: Load cleaned data into DuckDB for analysis  
# → Import File B output into DuckDB database
# → Add reference tables (NUCC, Physician Compare)

# PHASE 3: Advanced Analysis (File C functions)
source("R/C-Extracting_and_Processing_NPPES_Provider_Data.R")
# → Use File C's analytical functions on cleaned File B data
# → Specialty change analysis, provider tracking, etc.
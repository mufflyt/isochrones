#==============================================================================
# Extracting and Processing NPPES Provider Data
#
# Purpose: This script connects to a DuckDB database of National Provider 
# Identifier (NPI) data, analyzes the tables, and extracts information about
# OBGYN practitioners across multiple years.
#==============================================================================

#------------------------------------------------------------------------------
# 1. Load Required Packages
#------------------------------------------------------------------------------
# Core database packages
library(DBI)
library(duckdb)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(tyler)      # For state abbreviation conversion
library(logger)     # For logging operations
library(assertthat) # For input validation
library(tibble)     # For enhanced data frames

# Load custom functions for NPPES data processing
invisible(gc()); invisible(gc())
source("R/bespoke_functions.R")

#------------------------------------------------------------------------------
# 2. Database Connection and Exploration
#------------------------------------------------------------------------------
# Define database path
db_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_path)

# List all tables in the database
tables <- dbListTables(con)
print("Tables in the database:")
print(tables)

#------------------------------------------------------------------------------
# 3. Year-to-Table Mapping
#------------------------------------------------------------------------------
# Create a mapping between years and table names for historical NPPES data
# Generate the table mapping automatically
table_year_mapping <- create_nppes_table_mapping(con)

# Manually add the 2020 table to the mapping
table_year_mapping <- table_year_mapping %>%
  rbind(tibble::tibble(
    table_name = "npidata_pfile_20050523_20201011",
    year = 2020
  )) %>%
  arrange(year) %>%
  # Remove the incorrect 2010 mapping (this is actually a 2020 table)
  dplyr::filter(!(year == 2010 & table_name == "npidata_pfile_20050523_20201011")) %>%
  # Handle the 2022 duplicate - keep only one of them
  dplyr::group_by(year) %>%
  dplyr::slice(1) %>%
  dplyr::ungroup() %>%
  # Ensure the mapping is sorted by year
  dplyr::arrange(year)

# Print the mapping to verify
print(table_year_mapping)

#------------------------------------------------------------------------------
# 4. Define OBGYN Taxonomy Codes
#------------------------------------------------------------------------------
# These codes identify different specialties within obstetrics and gynecology
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology
  "207VX0201X",    # Gynecologic Oncology
  "207VE0102X",    # Reproductive Endocrinology
  "207VG0400X",    # Gynecology
  "207VM0101X",    # Maternal & Fetal Medicine
  "207VF0040X",    # Female Pelvic Medicine
  "207VB0002X",    # Bariatric Medicine
  "207VC0200X",    # Critical care medicine
  "207VC0040X", 
  "207VC0300X",    # Complex family planning
  "207VH0002X",    # Palliative care
  "207VX0000X"     # Obstetrics only
)

#------------------------------------------------------------------------------
# 5. Find and Process OBGYN Practitioners
#------------------------------------------------------------------------------
# Retrieve all physicians with these taxonomy codes
obgyn_physicians_all_years <- find_providers_across_years(
  connection = con,
  table_year_mapping = table_year_mapping,
  taxonomy_codes = obgyn_taxonomy_codes,
  verbose = TRUE
) 

# Clean and process the data
obgyn_physicians_all_years <- obgyn_physicians_all_years %>%
  dplyr::mutate(NPI = as.character(NPI)) %>%
  # Remove duplicate entries for the same NPI in the same year
  dplyr::distinct(NPI, Year, .keep_all = TRUE) %>%
  # Filter for US-based providers only
  dplyr::filter(
    `Provider Business Mailing Address Country Code (If outside U.S.)` %in% c("US") &
      `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US"
  ) %>%
  # Remove unnecessary country code columns
  dplyr::select(
    -`Provider Business Mailing Address Country Code (If outside U.S.)`,
    -`Provider Business Practice Location Address Country Code (If outside U.S.)`
  ) 

# Check dimensions of the resulting dataset
dim(obgyn_physicians_all_years)
#View(obgyn_physicians_all_years)

# Display a sample of the data
print(head(obgyn_physicians_all_years, 10))
glimpse(obgyn_physicians_all_years)
hist(as.numeric(obgyn_physicians_all_years$Year))

obgyn_physicians_all_years <- obgyn_physicians_all_years %>%
  # Group by NPI and Year
  dplyr::group_by(NPI, Year) %>%
  # Take the first row from each group (eliminates duplicates)
  dplyr::slice(1) %>%
  # Ungroup to avoid affecting future operations
  dplyr::ungroup()

#------------------------------------------------------------------------------
# 6. Final Data Processing
#------------------------------------------------------------------------------
# Convert state abbreviations to full state names
obgyn_physicians_all_years$`Provider Business Practice Location Address State Name` <- 
  tyler::phase0_convert_state_abbreviations(
    obgyn_physicians_all_years$`Provider Business Practice Location Address State Name`
  )

obgyn_physicians_all_years$`Provider Business Mailing Address State Name` <- 
  tyler::phase0_convert_state_abbreviations(
    obgyn_physicians_all_years$`Provider Business Mailing Address State Name`
  )

# Ensure Year is stored as character for consistency
obgyn_physicians_all_years$Year <- as.character(obgyn_physicians_all_years$Year)

#------------------------------------------------------------------------------
# 7. Save Results and Cleanup
#------------------------------------------------------------------------------
# Save the processed data to an RDS file
saveRDS(obgyn_physicians_all_years, "data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.rds")
invisible(gc())

#------------------------------------------------------------------------------
# Example: Load NUCC Taxonomy Codes into Database (For Reference)
#------------------------------------------------------------------------------
# First check if DuckDB can see the file
file_exists <- file.exists("/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/nucc/nucc_taxonomy_201.csv")
print(paste("File exists:", file_exists))

# Import the CSV file into DuckDB if it exists
if(file_exists) {
  # Read the CSV into R first
  nucc_data <- read.csv("/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/nucc/nucc_taxonomy_201.csv", 
                        stringsAsFactors = FALSE, 
                        na.strings = c("", "NA", "<NA>"))
  
  # Write to DuckDB
  dbWriteTable(con, "nucc_taxonomy", nucc_data, overwrite = TRUE)
}

# Close the database connection
dbDisconnect(con)
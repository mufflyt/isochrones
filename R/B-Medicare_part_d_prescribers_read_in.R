#######################
source("R/01-setup.R")
#######################

# ------------------------------------------------------------------------------
# Medicare Part D Prescribers Data Processing Script
# ------------------------------------------------------------------------------
source("R/01-setup.R")
# Source: https://data.cms.gov/resources/medicare-part-d-prescribers-methodology

# ------------------------------------------------------------------------------
# Executive Summary:
# This script loads Medicare Part D prescriber data from 2013–2023 into DuckDB,
# filters for gynecologic oncologists and OB/GYNs, identifies each provider's
# last year of continuous prescribing, and exports the results for analysis.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Table of Contents:
# 1. Setup and Load Libraries
# 2. Download Medicare Part D Files
# 3. Connect to DuckDB and Create Tables
# 4. Filter and Clean Data
# 5. Merge and Save Intermediate Dataset
# 6. Sanity Checks on Key NPIs
# 7. Calculate Last Consecutive Year of Prescribing
# 8. Save Final Outputs and Disconnect
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Key Variables:
# - download_dir: where downloaded files are saved
# - file_urls: list of Medicare Part D files to download
# - duckdb_path: path to the DuckDB database
# - output_csv_path: path to save merged intermediate dataset
# - final_output_path: path to save final cleaned dataset
# - npi_named_list: list of NPIs and corresponding physician names for checks
# ------------------------------------------------------------------------------


#
# This script processes data from the Medicare Part D Prescribers – by Provider 
# and Drug dataset, published annually by the Centers for Medicare & Medicaid 
# Services (CMS). The data contains information on:
#
#   - Prescribing providers (identified by NPI)
#   - Drugs prescribed (brand and generic)
#   - Total number of prescription fills
#   - Total drug cost
#
# Purpose of this script:
#   1. Load raw CSV files into DuckDB for fast processing
#   2. Filter to prescribers in Gynecologic Oncology and Obstetrics & Gynecology
#      within the United States
#   3. Exclude records with extremely high claim counts (>50,000)
#   4. Annotate each record with the corresponding year
#   5. Combine all years into a single dataset
#   6. Write merged datasets to CSV
#   7. Identify the last consecutive year of Medicare Part D prescribing for
#      each provider, useful for tracking sustained prescribing activity
#
# Output files:
#   - Medicare_part_D_prescribers_merged_data.csv:
#       Intermediate dataset after filtering and merging
#
#   - end_Medicare_part_D_prescribers_combined_df.csv:
#       Final cleaned dataset with standardized year values
#
#   - end_Medicare_part_D_prescribers_last_consecutive_year.csv:
#       Summary of each NPI’s last consecutive year of prescribing
#
# Note:
#   - Data spans reporting years 2021 through 2023, using file patterns like:
#       MUP_DPR_RY21_P04_V10_DY13_NPI_csv_1
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Medicare Part D Prescribers Data Processing Script
# ------------------------------------------------------------------------------
# This script downloads, processes, and analyzes Medicare Part D Prescribers data
# specifically for Gynecologic Oncology and Obstetrics & Gynecology specialties.
# It uses DuckDB for efficient data handling and performs multiple transformations
# to prepare data for longitudinal analysis of prescribing patterns.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# 3. DuckDB Database Connection and Table Creation
# ------------------------------------------------------------------------------
# Establishes a connection to a DuckDB database file, then loads each downloaded
# CSV file into a separate DuckDB table for efficient querying.

# ------------------------------------------------------------------------------
# 4. Data Filtering and Transformation
# ------------------------------------------------------------------------------
# Filters data specifically to providers within "Gynecological Oncology" and
# "Obstetrics & Gynecology" in the United States. Removes records with very high
# prescription claim counts (over 50,000) to exclude anomalies. Annotates each
# record with the source file year for longitudinal analysis.

# ------------------------------------------------------------------------------
# 5. Data Merging and Output
# ------------------------------------------------------------------------------
# Merges data from all processed tables into a single dataset, standardizes the
# year format, and outputs intermediate and final cleaned datasets as CSV files.
# Performs sanity checks on key providers to ensure data integrity.

# ------------------------------------------------------------------------------
# 6. Consecutive Year Calculation
# ------------------------------------------------------------------------------
# Determines the last consecutive year each provider actively prescribed drugs
# under Medicare Part D. Results are outputted into a CSV for further analysis.

# ------------------------------------------------------------------------------
# 7. Final Checks and Database Disconnection
# ------------------------------------------------------------------------------
# Conducts detailed checks on specific provider NPIs to confirm the results.
# Safely disconnects from the DuckDB database upon completion of all operations.
# ------------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------

# Load libraries
library(logger)
library(duckdb)
library(dplyr)
library(readr)
library(stringr)
library(assertthat)

# List of NPIs to check
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  "1437904760",   # Lizeth Acosta, MD
  "1568738854",   # Aaron Lazorwitz, MD
  "1194571661",   # Ana Gomez, MD
  "1699237040",   # Erin W. Franks, MD
  "1003311044",   # CATHERINE Callinan, MD
  "1609009943",   # Kristin Powell, MD
  "1114125051",   # Nathan Kow, MD
  "1043432123",   # Elena Tunitsky, MD
  "1215490446",   # PK
  "1487879987"    # Peter Jeppson
)

# Logger configuration
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Conflict preferences
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

# Establish logger settings
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Directory and file paths
directory_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
duckdb_path <- duckdb_file_path
output_csv_path <- "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv"

# Establish conflict preferences
conflicted::conflicts_prefer(stringr::str_remove_all)
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

# --------------------------------------------------------------------------
# Database connection
# --------------------------------------------------------------------------
logger::log_info("Establishing DuckDB connection")
con <- dbConnect(duckdb(), duckdb_path)
assertthat::assert_that(!is.null(con), msg = "Failed to connect to DuckDB")

# Create tables from files in directory
logger::log_info("Creating tables from files in directory: {directory_path}")
file_names <- list.files(directory_path)
created_tables <- character(0)

# --------------------------------------------------------------------------
# Create tables from Medicare Part D Prescribers File from the downloaded CSV files
# --------------------------------------------------------------------------
logger::log_info("Loading CSV files into DuckDB")

for (i in seq_along(file_names)) {
  file_name <- file_names[i]
  full_path <- file.path(directory_path, file_name)
  table_name <- tools::file_path_sans_ext(gsub("[^A-Za-z0-9]", "_", file_name))
  table_name <- paste0(table_name, "_", i)
  
  logger::log_info("Creating table '{table_name}' from '{file_name}'")
  
  # Construct SQL command to create a table
  sql_command <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
    table_name, full_path
  )
  
  # Execute the SQL command
  tryCatch({
    dbExecute(con, sql_command)
    logger::log_info("Table '{table_name}' created successfully")
    created_tables <- c(created_tables, table_name)
  }, error = function(e) {
    logger::log_error("Failed to create table '{table_name}': {e$message}")
  })
}

# List all tables created in DuckDB
logger::log_info("Created tables: {paste(created_tables, collapse=', ')}")

# Specify the table names to process
table_names <- c(
  "MUP_DPR_RY21_P04_V10_DY13_NPI_csv_1",
  "MUP_DPR_RY21_P04_V10_DY14_NPI_csv_2",
  "MUP_DPR_RY21_P04_V10_DY15_NPI_csv_3",
  "MUP_DPR_RY21_P04_V10_DY16_NPI_csv_4",
  "MUP_DPR_RY21_P04_V10_DY17_NPI_csv_5",
  "MUP_DPR_RY21_P04_V10_DY18_NPI_csv_6",
  "MUP_DPR_RY21_P04_V10_DY19_NPI_csv_7",
  "MUP_DPR_RY22_P04_V10_DY20_NPI_csv_8",
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9", 
  "MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10" 
)

# --------------------------------------------------------------------------
# ✅ Sanity Check: Confirm all required tables were created
# --------------------------------------------------------------------------
logger::log_info("Performing sanity check: verifying all required tables are created")

# Get list of all tables currently in DuckDB
existing_tables <- DBI::dbListTables(con)

# Identify any missing tables
missing_tables <- setdiff(table_names, existing_tables)

# Report results
if (length(missing_tables) == 0) {
  logger::log_info("✅ All {length(table_names)} tables were successfully created and are available.")
} else {
  logger::log_error("❌ Missing tables detected: {paste(missing_tables, collapse = ', ')}")
  stop("Some tables were not created successfully. Review the errors above.")
}

# Log the selected tables
logger::log_info("Selected tables for processing: {paste(table_names, collapse=', ')}")


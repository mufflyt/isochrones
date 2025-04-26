# ------------------------------------------------------------------------------
# Script: download_latest_nppes_deactivation_wget.R
#
# Title: Download Most Recent NPPES Deactivated NPI Report (Version 1)
#
# Description:
# This script automates the retrieval of the most recent Monthly NPI Deactivation
# Report (Version 1) from the CMS National Plan and Provider Enumeration System (NPPES).
#
# About the Data:
# The NPPES Deactivated NPI Report is a public dataset maintained by the Centers
# for Medicare & Medicaid Services (CMS). It contains information about National
# Provider Identifiers (NPIs) that have been deactivated, which often reflects
# provider retirement, practice closure, or death.
#
# These reports are updated monthly and are made available at:
# → https://download.cms.gov/nppes/NPI_Files.html
#
# File Format:
# Each report is a ZIP archive containing a single CSV file. The filename follows
# this pattern:
#     NPPES_Deactivated_NPI_Report_MMDDYY.zip
#
# The CSV includes:
#   • NPI (National Provider Identifier)
#   • Deactivation date
#   • Reason for deactivation (optional/variable over time)
#
# Use Case:
# This script is intended to support research into physician retirement patterns
# by programmatically retrieving the freshest NPPES deactivation data without
# manual downloads.
#
# Features:
#   • Web scraping via `rvest` and `httr`
#   • Logs progress and issues with the `logger` package
#   • Uses `wget` for robust downloading with resume and retry capabilities
#   • Filters only Version 1 (original length fields) of the deactivation reports
#
# Output:
#   The most recent deactivation report ZIP file is saved to:
#   → /Volumes/Video Projects Muffly 1/nppes_deactivation_reports
#
# Author: Tyler Muffly, MD
# Last Updated: April 14, 2025
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Script: download_nppes_deactivation_duckdb_script.R
#
# Purpose: Download the latest NPPES Deactivated NPI Report, unzip it,
#          and load the data into a DuckDB database.
#
# Author: Original by Tyler Muffly, MD; Modified as requested
# Last Updated: April 17, 2025
# ------------------------------------------------------------------------------

# Load required packages
# Note: The required packages would normally be imported in the roxygen comments
# of the function, but are included here as explicit library calls as requested
library(rvest)
library(httr)
library(stringr)
library(logger)
library(assertthat)
library(duckdb)
library(DBI)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Set parameters (these would normally be function arguments)
download_directory <- "/Volumes/Video Projects Muffly 1/nppes_deactivation_reports"
database_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"
verify_wget <- TRUE
verbose <- TRUE

# Input validation for parameters we already have
assertthat::assert_that(is.character(download_directory))
assertthat::assert_that(is.character(database_path))
assertthat::assert_that(is.logical(verify_wget))
assertthat::assert_that(is.logical(verbose))

# Configure logging based on verbosity
if (verbose) {
  logger::log_threshold(logger::INFO)
} else {
  logger::log_threshold(logger::WARN)
}

logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

logger::log_info("Starting NPPES deactivation data processing")
logger::log_info("Download directory: {download_directory}")
logger::log_info("Database path: {database_path}")

# ------------------------------------------------------------------------------
# Step 1: Create directory structure and check prerequisites
# ------------------------------------------------------------------------------

# Create directory if it doesn't exist
if (!dir.exists(download_directory)) {
  dir.create(download_directory, recursive = TRUE)
  logger::log_info("Created directory: {download_directory}")
} else {
  logger::log_debug("Directory already exists: {download_directory}")
}

# Check for wget if required
if (verify_wget) {
  logger::log_debug("Checking wget availability")
  
  if (Sys.which("wget") == "") {
    error_message <- paste(
      "❌ 'wget' is not installed or not found in your system PATH.",
      "Please install wget or set verify_wget=FALSE if you're using",
      "an alternative download method."
    )
    logger::log_error(error_message)
    stop(error_message)
  }
  
  logger::log_debug("wget is available")
}

# ------------------------------------------------------------------------------
# Step 2: Access NPPES website and find the latest deactivation file
# ------------------------------------------------------------------------------

npi_page_url <- "https://download.cms.gov/nppes/NPI_Files.html"

logger::log_info("Accessing NPPES download page: {npi_page_url}")
response <- httr::GET(npi_page_url, httr::user_agent("Mozilla/5.0"))

if (httr::status_code(response) != 200) {
  error_message <- paste0(
    "❌ Failed to access CMS download page. Status code: ",
    httr::status_code(response)
  )
  logger::log_error(error_message)
  stop(error_message)
}

html_page <- rvest::read_html(response)
logger::log_info("✅ Successfully read the NPPES HTML content")

# Find all links on the page
all_links <- html_page %>%
  rvest::html_nodes("a") %>%
  rvest::html_attr("href")

# Keep only Version 1 deactivation ZIPs
v1_links <- all_links[stringr::str_detect(
  all_links, 
  "NPPES_Deactivated_NPI_Report_\\d{6}\\.zip$"
)]

if (length(v1_links) == 0) {
  error_message <- "❌ No matching NPPES deactivation files found"
  logger::log_error(error_message)
  stop(error_message)
}

# Build full URLs
v1_full_links <- stringr::str_c("https://download.cms.gov/nppes/", v1_links)

# Extract date codes and find the most recent file
dates <- stringr::str_extract(v1_full_links, "\\d{6}")
parsed_dates <- as.Date(dates, format = "%m%d%y")
latest_index <- which.max(parsed_dates)
latest_url <- v1_full_links[latest_index]

# Clean up "./" if present
latest_url <- stringr::str_replace(latest_url, "\\./", "")
latest_file <- basename(latest_url)

logger::log_info("Found latest NPPES file: {latest_file}")
logger::log_debug("Download URL: {latest_url}")

# ------------------------------------------------------------------------------
# Step 3: Download the latest file using wget
# ------------------------------------------------------------------------------

dest_file <- file.path(download_directory, latest_file)

# Check if file already exists
if (file.exists(dest_file)) {
  logger::log_info("File already exists: {dest_file}")
  logger::log_info("Skipping download")
} else {
  # Quote the paths to safely handle spaces
  quoted_dest_file <- shQuote(dest_file)
  quoted_latest_url <- shQuote(latest_url)
  
  logger::log_info("Downloading file: {latest_file}")
  logger::log_info("Saving to: {dest_file}")
  
  # Use wget to download the file
  wget_args <- c(
    "--continue",                    # resume support
    "--tries=3",                     # retry up to 3 times
    "--timeout=600",                 # 10-minute timeout
    "--output-document", quoted_dest_file,
    quoted_latest_url
  )
  
  download_output <- system2(
    "wget", 
    args = wget_args, 
    stdout = TRUE, 
    stderr = TRUE
  )
  
  # Confirm download success
  if (file.exists(dest_file)) {
    logger::log_info("✅ Successfully downloaded: {latest_file}")
  } else {
    error_message <- paste0(
      "❌ Download failed. wget output:\n",
      paste(download_output, collapse = "\n")
    )
    logger::log_error(error_message)
    stop(error_message)
  }
}

# ------------------------------------------------------------------------------
# Step 4: Extract the ZIP file contents
# ------------------------------------------------------------------------------

logger::log_info("Extracting: {dest_file}")

# First, let's examine the ZIP file contents
file_list <- utils::unzip(
  dest_file,
  exdir = download_directory,
  list = TRUE
)

logger::log_info("ZIP file contains {length(file_list$Name)} files")
logger::log_debug("File list: {paste(file_list$Name, collapse=', ')}")

# Find CSV files in the archive - check for both .csv and .CSV extensions
csv_files <- file_list$Name[stringr::str_detect(file_list$Name, "\\.(csv|CSV)$")]

# If no CSV files found directly, extract all files and look for text files
if (length(csv_files) == 0) {
  logger::log_warn("No CSV files found directly in ZIP. Extracting all files...")
  
  # Extract all files
  utils::unzip(
    dest_file,
    exdir = download_directory,
    overwrite = TRUE
  )
  
  # Look for any extracted text files that might be CSV format
  extracted_files <- list.files(download_directory, full.names = TRUE)
  potential_data_files <- extracted_files[stringr::str_detect(
    extracted_files, 
    "\\.(txt|TXT|dat|DAT|csv|CSV)$"
  )]
  
  if (length(potential_data_files) > 0) {
    logger::log_info("Found {length(potential_data_files)} potential data files after extraction")
    
    # Use the first potential data file
    csv_file_path <- potential_data_files[1]
    logger::log_info("Using file for import: {csv_file_path}")
  } else {
    # If still no suitable files, try using the first file of any type
    all_extracted <- list.files(download_directory, full.names = TRUE)
    newest_files <- all_extracted[order(file.mtime(all_extracted), decreasing = TRUE)]
    
    if (length(newest_files) > 0) {
      csv_file_path <- newest_files[1]
      logger::log_warn("No standard data files found. Attempting to use: {csv_file_path}")
    } else {
      error_message <- paste0(
        "❌ No suitable files found in ZIP archive or after extraction: ", 
        dest_file
      )
      logger::log_error(error_message)
      stop(error_message)
    }
  }
} else {
  # Use the first CSV file found directly
  logger::log_info("Found {length(csv_files)} CSV files in the archive")
  
  # Extract the files
  utils::unzip(
    dest_file,
    exdir = download_directory,
    overwrite = TRUE
  )
  
  csv_file_path <- file.path(download_directory, csv_files[1])
  logger::log_info("Using file for import: {csv_file_path}")
}

# Final check to see if the chosen file exists
if (!file.exists(csv_file_path)) {
  error_message <- paste0("❌ Failed to extract data file: ", csv_file_path)
  logger::log_error(error_message)
  stop(error_message)
}

logger::log_info("✅ Successfully extracted: {csv_file_path}")

# ------------------------------------------------------------------------------
# Step 5: Load data into DuckDB
# ------------------------------------------------------------------------------

# Extract date from filename and format it for table name
file_date_code <- stringr::str_extract(latest_file, "\\d{6}")
# Convert to Date object - assuming format is MMDDYY
date_parsed <- as.Date(file_date_code, format = "%m%d%y")
# Format with full month name, day, and 4-digit year
formatted_date <- format(date_parsed, "%B_%d_%Y")
# Create table name with formatted date
table_name <- paste0("deactivated_npis_", formatted_date)

logger::log_info("Loading data to DuckDB: {database_path}")
logger::log_info("Target table: {table_name}")

# Connect to DuckDB
db_conn <- duckdb::dbConnect(duckdb::duckdb(), database_path)
logger::log_info("✅ Connected to DuckDB database")

# Check if table exists and drop if it does
tables_query <- "SELECT name FROM sqlite_master WHERE type='table'"
existing_tables <- DBI::dbGetQuery(db_conn, tables_query)

if (table_name %in% existing_tables$name) {
  logger::log_info("Table {table_name} already exists, dropping it")
  drop_query <- paste0("DROP TABLE IF EXISTS ", table_name)
  DBI::dbExecute(db_conn, drop_query)
}

# Import CSV to DuckDB
logger::log_info("Reading data from CSV: {csv_file_path}")

# First, check the file content to determine the format
file_sample <- readLines(csv_file_path, n = 5)
logger::log_info("Sample of file content: {paste(substr(file_sample[1], 1, 50), '...', sep = '')}")

# Attempt to import using DuckDB's auto detection
import_query <- sprintf(
  "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true, ignore_errors=true);",
  table_name,
  csv_file_path
)

tryCatch({
  DBI::dbExecute(db_conn, import_query)
  
  # Get row count
  count_query <- paste0("SELECT COUNT(*) AS count FROM ", table_name)
  row_count_result <- DBI::dbGetQuery(db_conn, count_query)
  row_count <- row_count_result$count[1]
  
  logger::log_info("✅ Successfully loaded {row_count} rows into table {table_name}")
  
}, error = function(e) {
  error_message <- paste0(
    "❌ Failed to load data into DuckDB: ", 
    conditionMessage(e)
  )
  logger::log_error(error_message)
  DBI::dbDisconnect(db_conn)
  stop(error_message)
})

# ------------------------------------------------------------------------------
# Step 6: Final success message and return connection
# ------------------------------------------------------------------------------

logger::log_info("NPPES deactivation processing completed successfully")
logger::log_info("Database connection remains open for further operations")
logger::log_info("Remember to disconnect with DBI::dbDisconnect(db_conn) when finished")

# Note: In a non-function script, we keep the connection open for further use
# but remind the user to close it when done


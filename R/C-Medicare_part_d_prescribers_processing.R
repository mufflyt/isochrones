#######################
source("R/01-setup.R")
source("R/constants.R")
#######################

# ------------------------------------------------------------------------------
# Medicare Part D Prescribers Data Processing Script
# ------------------------------------------------------------------------------
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
DIRECTORY_PATH <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
DUCKDB_FILE_PATH <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
DUCKDB_PATH <- DUCKDB_FILE_PATH
OUTPUT_CSV_PATH <- "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv"

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
con <- dbConnect(duckdb(), DUCKDB_PATH)
assertthat::assert_that(!is.null(con), msg = "Failed to connect to DuckDB")

# Create tables from files in directory
logger::log_info("Creating tables from files in directory: {DIRECTORY_PATH}")
file_names <- list.files(DIRECTORY_PATH)
created_tables <- character(0)

# --------------------------------------------------------------------------
# Create tables from Medicare Part D Prescribers File from the downloaded CSV files
# --------------------------------------------------------------------------
logger::log_info("Loading CSV files into DuckDB")

for (i in seq_along(file_names)) {
  file_name <- file_names[i]
  full_path <- file.path(DIRECTORY_PATH, file_name)
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

#####
# More QI checks
#####

# Load required packages
library(logger)
library(dplyr)
library(DBI)
library(assertthat)

# Configure logger
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

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
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9"
)

# Get database connection
logger::log_info("Establishing DuckDB connection")
con <- dbConnect(duckdb::duckdb(), "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb")
assertthat::assert_that(!is.null(con), msg = "Failed to connect to DuckDB")

# Verify which tables exist
logger::log_info("Checking which tables exist in the database")
existing_tables <- DBI::dbListTables(con)
available_tables <- intersect(table_names, existing_tables)
missing_tables <- setdiff(table_names, existing_tables)

# Report on available and missing tables
logger::log_info("Found {length(available_tables)} of {length(table_names)} requested tables")
if (length(missing_tables) > 0) {
  logger::log_warn("Missing tables: {paste(missing_tables, collapse = ', ')}")
}

# Check column consistency across tables
logger::log_info("Checking column consistency across available tables")
all_column_names <- list()
column_counts <- integer(0)
column_types_by_table <- list()

# First, collect all column names and types from available tables
for (table_name in available_tables) {
  logger::log_info("Reading column information from table: {table_name}")
  
  # Safely attempt to get column information
  tryCatch({
    # Get column info directly from database schema
    column_info <- DBI::dbColumnInfo(DBI::dbSendQuery(con, paste("SELECT * FROM", table_name, "LIMIT 0")))
    
    # Store column names
    column_names <- column_info$name
    all_column_names[[table_name]] <- column_names
    column_counts[table_name] <- length(column_names)
    
    # Store column types
    column_types <- column_info$type
    column_types_by_table[[table_name]] <- setNames(column_types, column_names)
    
    logger::log_info("Table {table_name} has {length(column_names)} columns")
  }, error = function(e) {
    logger::log_error("Error reading column info from table {table_name}: {e$message}")
  })
}

# Check for column name consistency
if (length(all_column_names) > 1) {
  # Get a unique set of all column names across all tables
  all_unique_columns <- unique(unlist(all_column_names))
  logger::log_info("Found {length(all_unique_columns)} unique column names across all tables")
  
  # Create a presence/absence matrix for columns
  column_presence <- matrix(
    FALSE, 
    nrow = length(all_unique_columns), 
    ncol = length(all_column_names),
    dimnames = list(all_unique_columns, names(all_column_names))
  )
  
  # Fill in the presence/absence matrix
  for (table_name in names(all_column_names)) {
    column_presence[all_column_names[[table_name]], table_name] <- TRUE
  }
  
  # Identify columns that are in every table
  common_columns <- all_unique_columns[rowSums(column_presence) == ncol(column_presence)]
  logger::log_info("Found {length(common_columns)} columns common to all tables")
  
  # Identify columns that are in some but not all tables
  partial_columns <- all_unique_columns[
    rowSums(column_presence) > 0 & 
      rowSums(column_presence) < ncol(column_presence)
  ]
  
  if (length(partial_columns) > 0) {
    logger::log_warn("Found {length(partial_columns)} columns in some but not all tables")
    
    # Show which tables are missing each partial column
    for (col in partial_columns) {
      missing_in <- names(all_column_names)[!column_presence[col, ]]
      logger::log_warn("Column '{col}' missing in: {paste(missing_in, collapse=', ')}")
    }
  }
  
  # Identify columns with inconsistent data types
  logger::log_info("Checking column data types across tables")
  
  # Create a mapping of column data types across tables
  column_types_matrix <- data.frame(
    column_name = all_unique_columns,
    stringsAsFactors = FALSE
  )
  
  for (table_name in names(column_types_by_table)) {
    column_types_matrix[[table_name]] <- NA_character_
    present_columns <- names(column_types_by_table[[table_name]])
    column_types_matrix[column_types_matrix$column_name %in% present_columns, table_name] <- 
      column_types_by_table[[table_name]][column_types_matrix$column_name[column_types_matrix$column_name %in% present_columns]]
  }
  
  # Check for inconsistent types
  inconsistent_types <- character(0)
  for (i in 1:nrow(column_types_matrix)) {
    col_name <- column_types_matrix$column_name[i]
    type_values <- as.character(column_types_matrix[i, -1])
    unique_types <- unique(type_values[!is.na(type_values)])
    
    if (length(unique_types) > 1) {
      inconsistent_types <- c(inconsistent_types, col_name)
      
      # Create type description
      type_info <- paste(sapply(names(column_types_by_table), function(tbl) {
        if (col_name %in% names(column_types_by_table[[tbl]])) {
          paste0(tbl, ": ", column_types_by_table[[tbl]][[col_name]])
        } else {
          paste0(tbl, ": missing")
        }
      }), collapse = ", ")
      
      logger::log_warn("Column '{col_name}' has inconsistent types: {type_info}")
    }
  }
  
  if (length(inconsistent_types) > 0) {
    logger::log_warn("Found {length(inconsistent_types)} columns with inconsistent data types")
  } else {
    logger::log_info("All common columns have consistent data types across tables")
  }
  
  # Check sample data for key columns
  key_columns <- c("PRSCRBR_NPI", "Tot_Clms", "PRSCRBR_TYPE")
  key_columns <- intersect(key_columns, common_columns)
  
  if (length(key_columns) > 0) {
    logger::log_info("Checking data patterns in {length(key_columns)} key columns")
    
    for (col in key_columns) {
      logger::log_info("Sampling data for column '{col}'")
      
      # Determine column type to handle NULL/empty checks correctly
      col_type <- column_types_by_table[[names(column_types_by_table)[1]]][[col]]
      
      for (table_name in names(all_column_names)) {
        # Check if column exists in this table
        if (col %in% all_column_names[[table_name]]) {
          # Get sample data
          sample_query <- paste0("SELECT ", col, " FROM ", table_name, " LIMIT 5")
          sample_data <- DBI::dbGetQuery(con, sample_query)
          
          # Log sample values
          sample_values <- paste(head(sample_data[[1]], 5), collapse = ", ")
          logger::log_info("Table {table_name}, column '{col}' sample: {sample_values}")
          
          # Check for NULL values (handle differently based on type)
          if (grepl("VARCHAR|STRING", col_type, ignore.case = TRUE)) {
            # String type column
            null_query <- paste0("SELECT COUNT(*) FROM ", table_name, 
                                 " WHERE ", col, " IS NULL OR ", col, " = ''")
          } else {
            # Numeric or other type column
            null_query <- paste0("SELECT COUNT(*) FROM ", table_name, 
                                 " WHERE ", col, " IS NULL")
          }
          
          null_count <- tryCatch({
            DBI::dbGetQuery(con, null_query)[[1]]
          }, error = function(e) {
            logger::log_warn("Error checking NULL values in {table_name}.{col}: {e$message}")
            return(NA)
          })
          
          if (!is.na(null_count)) {
            total_query <- paste0("SELECT COUNT(*) FROM ", table_name)
            total_count <- DBI::dbGetQuery(con, total_query)[[1]]
            
            null_percent <- (null_count / total_count) * 100
            logger::log_info("Table {table_name}, column '{col}' NULL/empty values: {null_count} ({round(null_percent, 2)}%)")
          }
        }
      }
    }
  }
  
  # Check for other data quality issues
  logger::log_info("Checking for potential data quality issues")
  
  # Check for very small or very large tables
  row_counts <- sapply(available_tables, function(t) {
    count_query <- paste0("SELECT COUNT(*) FROM ", t)
    DBI::dbGetQuery(con, count_query)[[1]]
  })
  
  min_rows <- min(row_counts)
  max_rows <- max(row_counts)
  mean_rows <- mean(row_counts)
  
  if (max_rows / min_rows > 10) {
    logger::log_warn("Large variation in table sizes detected: min={min_rows}, max={max_rows}, mean={round(mean_rows)}")
    
    # Identify outlier tables
    small_tables <- names(row_counts)[row_counts < mean_rows / 2]
    large_tables <- names(row_counts)[row_counts > mean_rows * 2]
    
    if (length(small_tables) > 0) {
      logger::log_warn("Unusually small tables: {paste(small_tables, collapse=', ')}")
    }
    
    if (length(large_tables) > 0) {
      logger::log_warn("Unusually large tables: {paste(large_tables, collapse=', ')}")
    }
  }
  
  # Generate overall quality summary
  logger::log_info("Generating table quality summary")
  quality_summary <- data.frame(
    table_name = names(all_column_names),
    column_count = sapply(names(all_column_names), function(t) length(all_column_names[[t]])),
    missing_common_columns = sapply(names(all_column_names), function(t) {
      sum(!common_columns %in% all_column_names[[t]])
    }),
    row_count = row_counts[names(all_column_names)]
  )
  
  # Sort by table name (which should follow year order)
  quality_summary <- quality_summary[order(quality_summary$table_name), ]
  
  print(quality_summary)
  
  # Write summary to CSV
  output_dir <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/analysis"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  write.csv(quality_summary, file.path(output_dir, "medicare_part_d_table_quality_summary.csv"), row.names = FALSE)
  
  # Create summary of column presence (TRUE/FALSE matrix)
  column_presence_df <- as.data.frame(column_presence)
  column_presence_df$column_name <- rownames(column_presence)
  column_presence_df <- column_presence_df[, c("column_name", names(all_column_names))]
  write.csv(column_presence_df, file.path(output_dir, "medicare_part_d_column_presence.csv"), row.names = FALSE)
  
  # Create summary of column types
  write.csv(column_types_matrix, file.path(output_dir, "medicare_part_d_column_types.csv"), row.names = FALSE)
  
  # Generate a comprehensive report of findings
  logger::log_info("Writing comprehensive quality report")
  sink(file.path(output_dir, "medicare_part_d_quality_report.txt"))
  
  cat("Medicare Part D Data Quality Report\n")
  cat("==================================\n\n")
  cat("Date: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
  
  cat("Tables Analyzed: ", length(available_tables), "\n")
  cat("Total Columns Found: ", length(all_unique_columns), "\n")
  cat("Common Columns Across All Tables: ", length(common_columns), "\n\n")
  
  cat("Table Summary\n")
  cat("=============\n\n")
  print(quality_summary)
  cat("\n\n")
  
  cat("Data Type Consistency\n")
  cat("====================\n\n")
  if (length(inconsistent_types) > 0) {
    cat("Columns with inconsistent data types: ", length(inconsistent_types), "\n")
    for (col in inconsistent_types) {
      cat("- ", col, "\n")
    }
  } else {
    cat("All columns have consistent data types across tables.\n")
  }
  cat("\n\n")
  
  cat("Key Column Analysis\n")
  cat("==================\n\n")
  for (col in key_columns) {
    cat("Column: ", col, "\n")
    cat("--------", rep("-", nchar(col)), "\n", sep="")
    
    # Collect NULL/empty percentages
    null_percentages <- numeric()
    for (table_name in names(all_column_names)) {
      if (col %in% all_column_names[[table_name]]) {
        if (grepl("VARCHAR|STRING", column_types_by_table[[table_name]][[col]], ignore.case = TRUE)) {
          null_query <- paste0("SELECT COUNT(*) FROM ", table_name, 
                               " WHERE ", col, " IS NULL OR ", col, " = ''")
        } else {
          null_query <- paste0("SELECT COUNT(*) FROM ", table_name, 
                               " WHERE ", col, " IS NULL")
        }
        
        null_count <- tryCatch({
          DBI::dbGetQuery(con, null_query)[[1]]
        }, error = function(e) {
          return(NA)
        })
        
        if (!is.na(null_count)) {
          total_query <- paste0("SELECT COUNT(*) FROM ", table_name)
          total_count <- DBI::dbGetQuery(con, total_query)[[1]]
          
          null_percent <- (null_count / total_count) * 100
          null_percentages[table_name] <- null_percent
          
          cat(table_name, ": ", null_count, " NULL/empty values (", 
              round(null_percent, 2), "%)\n", sep="")
        }
      }
    }
    
    # Check for trends in NULL percentages
    if (length(null_percentages) > 1) {
      null_range <- range(null_percentages, na.rm = TRUE)
      if (null_range[2] - null_range[1] > 5) {
        cat("\nNOTE: Significant variation in NULL/empty percentages detected.\n")
      }
    }
    
    cat("\n")
  }
  
  sink()
}

# Perform table-level data quality checks
logger::log_info("Performing table-level data quality checks")

# Check specific to Medicare Part D data
for (table_name in available_tables) {
  logger::log_info("Running quality checks on table: {table_name}")
  
  # Create a function to safely run queries and log results
  safe_query <- function(query, success_msg, error_msg) {
    tryCatch({
      result <- DBI::dbGetQuery(con, query)
      logger::log_info(success_msg)
      return(result)
    }, error = function(e) {
      logger::log_error(error_msg)
      logger::log_error("Error: {e$message}")
      return(NULL)
    })
  }
  
  # Check 1: Count records with missing NPI values
  if ("PRSCRBR_NPI" %in% all_column_names[[table_name]]) {
    query <- paste0("SELECT COUNT(*) FROM ", table_name, " WHERE PRSCRBR_NPI IS NULL")
    result <- safe_query(
      query, 
      "Check for missing NPI values completed", 
      "Error checking for missing NPI values"
    )
    
    if (!is.null(result)) {
      missing_npi_count <- result[[1]]
      if (missing_npi_count > 0) {
        logger::log_warn("Table {table_name} has {missing_npi_count} records with missing NPI values")
      } else {
        logger::log_info("Table {table_name} has no missing NPI values")
      }
    }
  }
  
  # Check 2: Count records with unusually high claim counts (potential outliers)
  if ("Tot_Clms" %in% all_column_names[[table_name]]) {
    query <- paste0("SELECT COUNT(*) FROM ", table_name, " WHERE Tot_Clms > 10000")
    result <- safe_query(
      query,
      "Check for high claim counts completed",
      "Error checking for high claim counts"
    )
    
    if (!is.null(result)) {
      high_claims_count <- result[[1]]
      if (high_claims_count > 0) {
        logger::log_warn("Table {table_name} has {high_claims_count} records with unusually high claim counts (>10,000)")
        
        # Get examples of high claim records
        detail_query <- paste0(
          "SELECT PRSCRBR_NPI, Tot_Clms FROM ", table_name, 
          " WHERE Tot_Clms > 10000 ORDER BY Tot_Clms DESC LIMIT 5"
        )
        details <- safe_query(
          detail_query,
          "Successfully retrieved high claim examples",
          "Error retrieving high claim examples"
        )
        
        if (!is.null(details) && nrow(details) > 0) {
          for (i in 1:nrow(details)) {
            logger::log_warn("High claims example: NPI {details$PRSCRBR_NPI[i]} with {details$Tot_Clms[i]} claims")
          }
        }
      } else {
        logger::log_info("Table {table_name} has no records with unusually high claim counts")
      }
    }
  }
}

# Clean up and close connection
logger::log_info("Data quality check completed")
DBI::dbDisconnect(con)
logger::log_info("Database connection closed")


###
# Visualizations of the outliers
###

# Load required packages
library(ggplot2)
library(dplyr)
library(tidyr)
library(DBI)
library(duckdb)
library(patchwork)
library(scales)
library(logger)

# Configure logger
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Set up constants
HIGH_CLAIMS_THRESHOLD <- 10000
TOP_OUTLIERS_TO_TRACK <- 5

# Establish database connection
logger::log_info("Establishing DuckDB connection")
con <- dbConnect(duckdb::duckdb(), "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb")

# Define the table names to process
table_names <- c(
  "MUP_DPR_RY21_P04_V10_DY13_NPI_csv_1",
  "MUP_DPR_RY21_P04_V10_DY14_NPI_csv_2",
  "MUP_DPR_RY21_P04_V10_DY15_NPI_csv_3",
  "MUP_DPR_RY21_P04_V10_DY16_NPI_csv_4",
  "MUP_DPR_RY21_P04_V10_DY17_NPI_csv_5",
  "MUP_DPR_RY21_P04_V10_DY18_NPI_csv_6",
  "MUP_DPR_RY21_P04_V10_DY19_NPI_csv_7",
  "MUP_DPR_RY22_P04_V10_DY20_NPI_csv_8",
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9"
)

# Map table names to years for better readability
year_mapping <- c(
  "MUP_DPR_RY21_P04_V10_DY13_NPI_csv_1" = 2013,
  "MUP_DPR_RY21_P04_V10_DY14_NPI_csv_2" = 2014,
  "MUP_DPR_RY21_P04_V10_DY15_NPI_csv_3" = 2015,
  "MUP_DPR_RY21_P04_V10_DY16_NPI_csv_4" = 2016,
  "MUP_DPR_RY21_P04_V10_DY17_NPI_csv_5" = 2017,
  "MUP_DPR_RY21_P04_V10_DY18_NPI_csv_6" = 2018,
  "MUP_DPR_RY21_P04_V10_DY19_NPI_csv_7" = 2019,
  "MUP_DPR_RY22_P04_V10_DY20_NPI_csv_8" = 2020,
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9" = 2021
)

# 1. Collect table statistics
logger::log_info("Collecting table statistics")
table_stats <- data.frame(
  table_name = character(0),
  year = integer(0),
  total_records = integer(0),
  outlier_records = integer(0),
  outlier_percentage = numeric(0),
  min_claims = integer(0),
  median_claims = numeric(0),
  mean_claims = numeric(0),
  max_claims = integer(0)
)

for (table_name in table_names) {
  logger::log_info("Processing statistics for table: {table_name}")
  year <- year_mapping[table_name]
  
  # Get total record count
  total_query <- paste0("SELECT COUNT(*) FROM ", table_name)
  total_count <- DBI::dbGetQuery(con, total_query)[[1]]
  
  # Get outlier record count
  outlier_query <- paste0("SELECT COUNT(*) FROM ", table_name, " WHERE Tot_Clms > ", HIGH_CLAIMS_THRESHOLD)
  outlier_count <- DBI::dbGetQuery(con, outlier_query)[[1]]
  
  # Calculate outlier percentage
  outlier_percentage <- (outlier_count / total_count) * 100
  
  # Get claims statistics
  stats_query <- paste0("SELECT 
                          MIN(Tot_Clms) as min_claims,
                          MEDIAN(Tot_Clms) as median_claims,
                          AVG(Tot_Clms) as mean_claims,
                          MAX(Tot_Clms) as max_claims
                        FROM ", table_name)
  
  claims_stats <- DBI::dbGetQuery(con, stats_query)
  
  # Combine into table_stats
  table_stats <- rbind(table_stats, data.frame(
    table_name = table_name,
    year = year,
    total_records = total_count,
    outlier_records = outlier_count,
    outlier_percentage = outlier_percentage,
    min_claims = claims_stats$min_claims,
    median_claims = claims_stats$median_claims,
    mean_claims = claims_stats$mean_claims,
    max_claims = claims_stats$max_claims
  ))
}

# 2. Track top outlier NPIs across years
logger::log_info("Tracking top outlier NPIs across years")

# Identify top outliers across all years
top_outliers_query <- paste0(
  "WITH combined AS (
    ", paste(sapply(table_names, function(tbl) {
      paste0("SELECT '", tbl, "' as table_name, ", year_mapping[tbl], " as year, PRSCRBR_NPI, Tot_Clms 
              FROM ", tbl, " 
              WHERE Tot_Clms > ", HIGH_CLAIMS_THRESHOLD)
    }), collapse = " UNION ALL "),
  "
  )
  SELECT PRSCRBR_NPI, SUM(Tot_Clms) as total_claims
  FROM combined
  GROUP BY PRSCRBR_NPI
  ORDER BY total_claims DESC
  LIMIT ", TOP_OUTLIERS_TO_TRACK
)

top_outliers <- DBI::dbGetQuery(con, top_outliers_query); top_outliers
logger::log_info("Identified {nrow(top_outliers)} top outlier NPIs")

# Create a data frame to track these NPIs across years
top_npi_claims <- data.frame()

for (i in 1:nrow(top_outliers)) {
  npi <- top_outliers$PRSCRBR_NPI[i]
  logger::log_info("Tracking NPI {npi} across years")
  
  for (table_name in table_names) {
    year <- year_mapping[table_name]
    
    query <- paste0("SELECT Tot_Clms FROM ", table_name, " WHERE PRSCRBR_NPI = '", npi, "'")
    result <- DBI::dbGetQuery(con, query)
    
    claims <- if (nrow(result) > 0) result$Tot_Clms[1] else 0
    
    top_npi_claims <- rbind(top_npi_claims, data.frame(
      npi = npi,
      year = year,
      claims = claims
    ))
  }
}

# 3. Create visualizations
logger::log_info("Creating visualizations")
output_dir <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/analysis/plots"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Plot 1: Outlier percentage by year
plot1 <- ggplot(table_stats, aes(x = year, y = outlier_percentage)) +
  geom_line(linewidth = 1.2, color = "darkblue") +
  geom_point(size = 3, color = "darkblue") +
  labs(
    title = "Percentage of Records with High Claim Counts (>10,000)",
    subtitle = "Trend across Medicare Part D data years",
    x = "Year",
    y = "Percentage of Records"
  ) +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))

# Plot 2: Max claim count by year
plot2 <- ggplot(table_stats, aes(x = year, y = max_claims)) +
  geom_line(linewidth = 1.2, color = "darkred") +
  geom_point(size = 3, color = "darkred") +
  labs(
    title = "Maximum Claim Count by Year",
    subtitle = "Highest number of claims per provider",
    x = "Year",
    y = "Maximum Claims"
  ) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))

# Plot 3: Distribution of claim counts (boxplot)
# First, collect sample data for boxplot
logger::log_info("Collecting sample data for distribution boxplot")
sample_data <- data.frame()

for (table_name in table_names) {
  year <- year_mapping[table_name]
  
  # Sample 1000 records from each year for the boxplot
  sample_query <- paste0("SELECT Tot_Clms FROM ", table_name, " ORDER BY RANDOM() LIMIT 1000")
  year_sample <- DBI::dbGetQuery(con, sample_query)
  
  year_sample$year <- year
  sample_data <- rbind(sample_data, year_sample)
}

plot3 <- ggplot(sample_data, aes(x = factor(year), y = Tot_Clms)) +
  geom_boxplot(fill = "steelblue", alpha = 0.7) +
  coord_cartesian(ylim = c(0, 5000)) +  # Limit y-axis to see the distribution better
  labs(
    title = "Distribution of Claim Counts by Year",
    subtitle = "Boxplot excludes extreme outliers for better visualization",
    x = "Year",
    y = "Total Claims"
  ) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))

# Plot 4: Top outlier NPIs over time
plot4 <- ggplot(top_npi_claims, aes(x = year, y = claims, color = factor(npi), group = npi)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(
    title = "Top 5 Providers with Highest Claims Over Time",
    subtitle = "Tracking individual NPIs with extremely high claim counts",
    x = "Year",
    y = "Total Claims",
    color = "Provider NPI"
  ) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "bottom"
  )

# Plot 5: Table row counts by year
plot5 <- ggplot(table_stats, aes(x = year, y = total_records)) +
  geom_col(fill = "darkgreen", alpha = 0.7) +
  geom_text(aes(label = scales::comma(total_records)), vjust = -0.5, size = 3) +
  labs(
    title = "Medicare Part D Record Counts by Year",
    subtitle = "Total number of records in each year's data",
    x = "Year",
    y = "Number of Records"
  ) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))

# Save individual plots
ggsave(file.path(output_dir, "outlier_percentage_by_year.png"), plot1, width = 10, height = 6)
ggsave(file.path(output_dir, "max_claims_by_year.png"), plot2, width = 10, height = 6)
ggsave(file.path(output_dir, "claims_distribution_by_year.png"), plot3, width = 10, height = 6)
ggsave(file.path(output_dir, "top_outlier_npis_over_time.png"), plot4, width = 10, height = 6)
ggsave(file.path(output_dir, "record_counts_by_year.png"), plot5, width = 10, height = 6)

# Create a combined dashboard
if (requireNamespace("patchwork", quietly = TRUE)) {
  logger::log_info("Creating combined dashboard")
  
  # Arrange plots in a dashboard layout
  combined_plot <- (plot1 | plot2) / (plot3 | plot5) / plot4 +
    plot_layout(heights = c(1, 1, 1.2))
  
  # Save combined dashboard
  ggsave(
    file.path(output_dir, "medicare_part_d_quality_dashboard.png"),
    combined_plot,
    width = 16,
    height = 14
  )
}

# Generate summary table for the report
summary_table <- table_stats %>%
  select(year, total_records, outlier_records, outlier_percentage, 
         min_claims, median_claims, mean_claims, max_claims) %>%
  arrange(year)

# Save summary table
write.csv(summary_table, file.path(output_dir, "medicare_part_d_quality_summary.csv"), row.names = FALSE)


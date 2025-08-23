#######################
source("R/01-setup.R")
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
# Script: download_medicare_part_d_files.R
# Purpose: Download Medicare Part D CSV files from data.cms.gov using wget
# --------------------------------------------------------------------------

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

# Prescriber types included in analysis
PRESCRIBER_TYPES <- c("Gynecological Oncology", "Obstetrics & Gynecology")

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
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9"#,
  #"MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10"
)

# Threshold for excluding extremely high claim counts
MAX_CLAIMS_THRESHOLD <- 50000

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

# Synchronizing column names for quality check
logger::log_info("Checking column consistency across tables")
all_column_names <- list()

for (table_name in table_names) {
  table_data <- dbReadTable(con, table_name)
  column_names <- names(table_data)
  all_column_names[[table_name]] <- column_names
  logger::log_debug("Table {table_name} has {length(column_names)} columns")
}

# Process and filter tables
logger::log_info("Starting to process tables with filtering criteria")
processed_tables <- list()
all_data <- data.frame()

# Create output directory if needed
if (!dir.exists(dirname(output_csv_path))) {
  logger::log_info("Creating output directory: {dirname(output_csv_path)}")
  dir.create(dirname(output_csv_path), recursive = TRUE)
}



# --------------------------------------------------------------------------
# Process each table filtering "Prscrbr_Type" only OBGYN and "Gynecological Oncology"
# --------------------------------------------------------------------------

# Process each table
for (i in 1:length(table_names)) {
  table_name <- table_names[i]
  logger::log_info("Processing table: {table_name}")
  
  tryCatch({
    # Create reference to the table
    table_ref <- tbl(con, table_name)
    
    # Apply filters and transformations
    processed_data <- table_ref %>%
      dplyr::filter(Prscrbr_Type %in% PRESCRIBER_TYPES &
                      Prscrbr_Cntry == "US") %>%
      dplyr::select(PRSCRBR_NPI, Tot_Clms) %>%
      dplyr::filter(Tot_Clms < MAX_CLAIMS_THRESHOLD) %>%
      dplyr::mutate(Prescribed = "Prescription written") %>%
      dplyr::distinct(PRSCRBR_NPI, .keep_all = TRUE) %>%
      dplyr::mutate(year = table_name)
    
    logger::log_info("Collecting processed data from table: {table_name}")
    processed_data_df <- processed_data %>%
      dplyr::compute() %>%
      collect()
    logger::log_info("Collected {nrow(processed_data_df)} rows from {table_name}")
    
    # Store results
    all_data <- dplyr::bind_rows(all_data, processed_data_df)
    processed_tables[[table_name]] <- processed_data
    
  }, error = function(e) {
    logger::log_error("Error processing table {table_name}: {e$message}")
  })
}

logger::log_info("Total rows in combined dataset: {nrow(all_data)}")

# Write the merged data to CSV
logger::log_info("Writing processed data to: {output_csv_path}")
tryCatch({
  readr::write_csv(all_data, output_csv_path)
  logger::log_info("Data successfully written to: {output_csv_path}")
}, error = function(e) {
  logger::log_error("Failed to write CSV: {e$message}")
})

# --------------------------------------------------------------------------
# Sanity check - check specific NPIs
# --------------------------------------------------------------------------
logger::log_info("Performing sanity checks on specified NPIs")

first_processed_table <- processed_tables[[1]]

# Loop through each NPI and log findings
for (npi in npi_list) {
  npi_numeric <- as.numeric(npi)
  
  npi_check <- first_processed_table %>%
    dplyr::filter(PRSCRBR_NPI == npi_numeric) %>%
    dplyr::compute() %>%
    collect() %>%
    as.data.frame()
  
  if (nrow(npi_check) > 0) {
    logger::log_info("✅ Sanity check for NPI {npi}: Found {nrow(npi_check)} rows")
  } else {
    logger::log_warn("⚠️ Sanity check for NPI {npi}: No rows found")
  }
}

# --------------------------------------------------------------------------
# Clean up the year
# --------------------------------------------------------------------------
logger::log_info("Creating combined dataframe with clean year values")
medicare_part_d_prescribers_combined <- lapply(processed_tables, function(tbl) {
  tbl %>%
    dplyr::compute() %>%
    collect() %>%
    as.data.frame()
}) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(year = stringr::str_extract(year, "DY\\d+")) %>%
  dplyr::mutate(year = stringr::str_remove(year, "DY")) %>%
  dplyr::mutate(year = factor(year)) %>%
  dplyr::distinct(PRSCRBR_NPI, year, .keep_all = TRUE) %>%
  dplyr::ungroup()

logger::log_info("Combined dataframe created with {nrow(medicare_part_d_prescribers_combined)} rows")

# --------------------------------------------------------------------------
# Sanity check for specific NPIs on the medicare_part_d_prescribers_combined file
# --------------------------------------------------------------------------
# Create a named vector of NPIs and Physician Names
npi_named_list <- c(
  "1689603763" = "Tyler Muffly, MD",
  "1528060639" = "John Curtin, MD",
  "1346355807" = "Pedro Miranda, MD",
  "1437904760" = "Lizeth Acosta, MD",
  "1568738854" = "Aaron Lazorwitz, MD",
  "1194571661" = "Ana Gomez, MD",
  "1699237040" = "Erin W. Franks, MD",
  "1003311044" = "Catherine Callinan, MD",
  "1609009943" = "Kristin Powell, MD",
  "1114125051" = "Nathan Kow, MD",
  "1043432123" = "Elena Tunitsky, MD",
  "1215490446" = "PK",
  "1487879987" = "Peter Jeppson, MD"
)

# Improved NPI check with names
logger::log_info("Performing final dataset checks on specified NPIs with physician names")

for (npi in names(npi_named_list)) {
  physician_name <- npi_named_list[[npi]]
  npi_numeric <- as.numeric(npi)
  
  npi_check_final <- medicare_part_d_prescribers_combined %>%
    dplyr::filter(PRSCRBR_NPI == npi_numeric)
  
  if (nrow(npi_check_final) > 0) {
    logger::log_info(
      "✅ Final check for {physician_name} (NPI: {npi}): Found {nrow(npi_check_final)} rows"
    )
  } else {
    logger::log_warn(
      "⚠️ Final check for {physician_name} (NPI: {npi}): No rows found"
    )
  }
}

  # --------------------------------------------------------------------------
  # Write final combined dataset to CSV
  # --------------------------------------------------------------------------
  final_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"

  logger::log_info("Writing final combined dataset to: {final_output_path}")
  readr::write_csv(medicare_part_d_prescribers_combined, final_output_path)
  logger::log_info("Final dataset successfully written")
  
  # # Process year data for consecutive year calculation
  # logger::log_info("Processing data for consecutive year calculation")
  # consecutive_year_data <- readr::read_csv(final_output_path)
  # logger::log_info("Loaded {nrow(consecutive_year_data)} rows for consecutive year calculation")
  # 
  # # Extract and clean year values
  # consecutive_year_data$year <- sub(".*RY(\\d+).*", "\\1", consecutive_year_data$year)
  # consecutive_year_data$year <- paste0("20", consecutive_year_data$year)
  # consecutive_year_data$year <- sub("_.*", "", consecutive_year_data$year)
  # consecutive_year_data$year <- as.numeric(consecutive_year_data$year)
  # 
  # logger::log_info("Year values cleaned and converted to numeric")
  # 
  # # Calculate last consecutive year for each NPI
  # logger::log_info("Calculating last consecutive year for each provider")
  # gap = 1L # Year gap between last prescribing.  Years 2016 → 2017 → 2018 are consecutive ✅.  Gap between 2018 → 2020 ❌
  # # Allowable gap between consecutive prescribing years
  # 
  # last_consecutive_year <- consecutive_year_data %>%
  #   dplyr::arrange(PRSCRBR_NPI, year) %>%
  #   dplyr::group_by(PRSCRBR_NPI) %>%
  #   dplyr::summarise(
  #     last_consecutive_year_Medicare_part_D_prescribers = 
  #       max(base::cumsum(c(0, diff(year) != gap)) + year),
  #     gap_years = gap  # Add a constant column explaining the gap value used
  #   ) %>%
  #   dplyr::ungroup()
  # 
  # 
  # logger::log_info("Calculated last consecutive year for {nrow(last_consecutive_year)} providers")
  # 
  # # Check specific providers
  # last_consecutive_year %>%
  #   dplyr::filter(PRSCRBR_NPI == 1689603763L) %>%
  #   {logger::log_info("Last consecutive year for NPI 1689603763: {.$last_consecutive_year_Medicare_part_D_prescribers}")}
  # 
  # last_consecutive_year %>%
  #   dplyr::filter(PRSCRBR_NPI == 1508953654) %>%
  #   {logger::log_info("Last consecutive year for NPI 1508953654 (Sue Davidson): {.$last_consecutive_year_Medicare_part_D_prescribers}")}
  # 
  # last_consecutive_year %>%
  #   dplyr::filter(PRSCRBR_NPI == 1972523165) %>%
  #   {logger::log_info("Last consecutive year for NPI 1972523165 (Chris Carey): {.$last_consecutive_year_Medicare_part_D_prescribers}")}
  # 
  # last_consecutive_year %>%
  #   dplyr::filter(PRSCRBR_NPI == 1548363484) %>%
  #   {logger::log_info("Last consecutive year for NPI 1548363484 (Karlotta Davis): {.$last_consecutive_year_Medicare_part_D_prescribers}")}
  # 
  # # Write final consecutive year data to CSV
  # consecutive_year_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_last_consecutive_year.csv"
  # logger::log_info("Writing last consecutive year data to: {consecutive_year_output_path}")
  # readr::write_csv(last_consecutive_year, consecutive_year_output_path)
  # logger::log_info("Last consecutive year data successfully written")
  # 
  # 
  # # Calculate gaps for each physician
  # logger::log_info("Calculating gaps between prescribing years for each provider")
## Execute ----
  # Path to Medicare Part D data
  # medicare_file <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
  final_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
  data <- read_csv(final_output_path)
  
  prescriber_data <- 
    analyze_physician_retirement(data, 
                                 npi_column = "PRSCRBR_NPI", 
                                 year_column = "year",
                                 gap_threshold = 2,
                                 completeness_threshold = 0.8,
                                 verbose = TRUE)  
  
  prescriber_data$physician_summary
  prescriber_data$complete_data
  
    
  # gap_analysis <- consecutive_year_data %>%
  #   dplyr::arrange(PRSCRBR_NPI, year) %>%
  #   dplyr::group_by(PRSCRBR_NPI) %>%
  #   dplyr::summarise(
  #     first_year = min(year),
  #     last_year = max(year),
  #     max_gap_detected = max(diff(year), na.rm = TRUE),
  #     n_years_recorded = dplyr::n(),
  #     .groups = "drop"
  #   ) %>%
  #   dplyr::mutate(
  #     gap_descriptive = dplyr::case_when(
  #       max_gap_detected == 0 ~ "Gap of 0 years",
  #       max_gap_detected == 1 ~ "Gap of 1 year",
  #       max_gap_detected == 2 ~ "Gap of 2 years",
  #       max_gap_detected == 3 ~ "Gap of 3 years",
  #       max_gap_detected >= 4 & max_gap_detected <= 5 ~ "Gap of 4-5 years",
  #       max_gap_detected > 5 ~ "Gap of greater than 5 years",
  #       TRUE ~ "Unknown"
  #     )
  #   )
  # 
  # logger::log_info("Gap analysis completed: {nrow(gap_analysis)} physicians evaluated")
  # 
  # # View a few example rows
  # print(head(gap_analysis))
  # 
  # # Save output
  # gap_analysis_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_gap_analysis.csv"
  # 
  # logger::log_info("Saving gap analysis to: {gap_analysis_output_path}")
  # readr::write_csv(gap_analysis, gap_analysis_output_path)
  # logger::log_info("Gap analysis successfully written to {gap_analysis_output_path}")
  # 
  # # Create a plot of gap_descriptive
  # plot_gap_descriptive <- ggplot2::ggplot(
  #   gap_analysis,
  #   ggplot2::aes(x = gap_descriptive)
  # ) +
  #   ggplot2::geom_bar() +
  #   ggplot2::labs(
  #     title = "Distribution of Gap Lengths in Prescribing",
  #     x = "Gap Description",
  #     y = "Number of Physicians"
  #   ) +
  #   ggplot2::theme_minimal() +
  #   ggplot2::theme(
  #     axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
  #     plot.title = ggplot2::element_text(hjust = 0.5)
  #   )
  # 
  # # Print the plot
  # print(plot_gap_descriptive)
  # 
  # # Optionally, save the plot
  # timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  # ggplot2::ggsave(
  #   filename = paste0("gap_descriptive_plot_", timestamp, ".png"),
  #   plot = plot_gap_descriptive,
  #   width = 8,
  #   height = 6
  # )
  
  
  # Disconnect from database
  logger::log_info("Disconnecting from database")
  dbDisconnect(con)
  logger::log_info("Processing complete")


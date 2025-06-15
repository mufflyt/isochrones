# ------------------------------------------------------------------------------
# Medicare Part D Prescribers Data Processing Script
# ------------------------------------------------------------------------------
# Source: https://data.cms.gov/resources/medicare-part-d-prescribers-methodology
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


# --------------------------------------------------------------------------
# Script: download_medicare_part_d_files.R
# Purpose: Download Medicare Part D CSV files from data.cms.gov using wget
# --------------------------------------------------------------------------

# Load logger
library(logger)

# Set up logger
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Directory to save the files
download_dir <- "/Volumes/Video Projects Muffly 1/medicare_part_d_ry24_files"
if (!dir.exists(download_dir)) {
  dir.create(download_dir, recursive = TRUE)
  logger::log_info("Created download directory: {download_dir}")
}

# Named vector of filenames and URLs
file_urls <- c(
  "MUP_DPR_RY24_P04_V10_DY21_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/43359391-e7fa-40b9-9bd4-5dc295e18712/MUP_DPR_RY24_P04_V10_DY21_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY20_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/75fecc51-c9e8-4904-b570-9da9dc101721/MUP_DPR_RY24_P04_V10_DY20_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY19_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/129e7c21-d492-425b-be03-d2e59d933ab6/MUP_DPR_RY24_P04_V10_DY19_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY18_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/be3019b4-1164-4b40-af5d-cab40847d222/MUP_DPR_RY24_P04_V10_DY18_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY17_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/d9d685a0-dc49-4da5-9416-4cf3e6349296/MUP_DPR_RY24_P04_V10_DY17_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY16_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/67c457c6-b62d-424f-ad85-2bb8117c928d/MUP_DPR_RY24_P04_V10_DY16_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY15_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/bc1caf7f-dcbb-4258-b243-ee3666b6a20b/MUP_DPR_RY24_P04_V10_DY15_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY14_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/06e87540-68a5-4a10-bf9c-a0521dc4ebed/MUP_DPR_RY24_P04_V10_DY14_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY13_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/5fb694b1-2ec5-4e00-8efe-14161bdbdbea/MUP_DPR_RY24_P04_V10_DY13_NPIBN.csv"
)

# Check if wget is available
if (Sys.which("wget") == "") {
  stop("wget is not available on your system. Please install wget to proceed.")
}

# Download all files using wget
for (file_name in names(file_urls)) {
  url <- file_urls[[file_name]]
  dest_file <- file.path(download_dir, file_name)
  
  logger::log_info("Downloading {file_name} ...")
  
  # Build wget command with resume and retry
  cmd <- "wget"
  args <- c(
    "--tries=3",              # Retry up to 3 times
    "--continue",             # Resume download if partially completed
    "--timeout=600",          # Timeout per attempt: 600 seconds
    "--directory-prefix", download_dir,
    "--output-document", dest_file,
    url
  )
  
  # Run wget command
  result <- system2(cmd, args, stdout = TRUE, stderr = TRUE)
  
  if (file.exists(dest_file)) {
    logger::log_info("✅ Successfully downloaded: {file_name}")
  } else {
    logger::log_error("❌ Failed to download: {file_name}. Output:\n{paste(result, collapse = '\n')}")
  }
}

logger::log_info("🚚 All download attempts complete. Files saved in: {download_dir}")


# Establish logger settings
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Directory and file paths
directory_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
output_csv_path <- "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv"

# Establish conflict preferences
conflicted::conflicts_prefer(stringr::str_remove_all)
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

# Connect to DuckDB
logger::log_info("Connecting to DuckDB at: {duckdb_file_path}")
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
assertthat::assert_that(!is.null(con), msg = "Database connection failed")

# Create tables from files in directory
logger::log_info("Creating tables from files in directory: {directory_path}")
file_names <- list.files(directory_path)
created_tables <- character(0)

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
  "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9"
)

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

# Process each table
for (i in 1:length(table_names)) {
  table_name <- table_names[i]
  logger::log_info("Processing table: {table_name}")
  
  tryCatch({
    # Create reference to the table
    table_ref <- tbl(con, table_name)
    
    # Apply filters and transformations
    processed_data <- table_ref %>%
      dplyr::filter(Prscrbr_Type %in% c("Gynecological Oncology", "Obstetrics & Gynecology") & 
                      Prscrbr_Cntry == "US") %>%
      dplyr::select(PRSCRBR_NPI, Tot_Clms) %>%
      dplyr::filter(Tot_Clms < 50000) %>%
      dplyr::mutate(Prescribed = "Prescription written") %>%
      dplyr::distinct(PRSCRBR_NPI, .keep_all = TRUE) %>%
      dplyr::mutate(year = table_name)
    
    logger::log_info("Collecting processed data from table: {table_name}")
    processed_data_df <- processed_data %>% collect()
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

# Sanity check - check specific NPIs
logger::log_info("Performing sanity checks")
first_processed_table <- processed_tables[[1]]
first_processed_table %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L) %>%
  collect() %>%
  as.data.frame() %>%
  {logger::log_info("Sanity check for NPI 1689603763: Found {nrow(.)} rows")}

# Convert all tables to a single dataframe with cleaner year values
logger::log_info("Creating combined dataframe with clean year values")
medicare_part_d_prescribers_combined <- lapply(processed_tables, function(tbl) {
  tbl %>% collect() %>% as.data.frame()
}) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(year = stringr::str_extract(year, "DY\\d+")) %>%
  dplyr::mutate(year = stringr::str_remove(year, "DY")) %>%
  dplyr::mutate(year = factor(year)) %>%
  dplyr::distinct(PRSCRBR_NPI, year, .keep_all = TRUE) %>%
  dplyr::ungroup()

logger::log_info("Combined dataframe created with {nrow(medicare_part_d_prescribers_combined)} rows")

# Check specific NPIs in the final dataset
medicare_part_d_prescribers_combined %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L) %>%
  {logger::log_info("Final dataset check for NPI 1689603763: Found {nrow(.)} rows")}

medicare_part_d_prescribers_combined %>%
  dplyr::filter(PRSCRBR_NPI == 1508953654) %>%
  {logger::log_info("Final dataset check for NPI 1508953654: Found {nrow(.)} rows")}

# Write final combined dataset to CSV
final_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
logger::log_info("Writing final combined dataset to: {final_output_path}")
readr::write_csv(medicare_part_d_prescribers_combined, final_output_path)
logger::log_info("Final dataset successfully written")

# Process year data for consecutive year calculation
logger::log_info("Processing data for consecutive year calculation")
consecutive_year_data <- readr::read_csv(final_output_path)
logger::log_info("Loaded {nrow(consecutive_year_data)} rows for consecutive year calculation")

# Extract and clean year values
consecutive_year_data$year <- sub(".*RY(\\d+).*", "\\1", consecutive_year_data$year)
consecutive_year_data$year <- paste0("20", consecutive_year_data$year)
consecutive_year_data$year <- sub("_.*", "", consecutive_year_data$year)
consecutive_year_data$year <- as.numeric(consecutive_year_data$year)

logger::log_info("Year values cleaned and converted to numeric")

# Calculate last consecutive year for each NPI
logger::log_info("Calculating last consecutive year for each provider")
last_consecutive_year <- consecutive_year_data %>%
  dplyr::arrange(PRSCRBR_NPI, year) %>%
  dplyr::group_by(PRSCRBR_NPI) %>%
  dplyr::summarise(
    last_consecutive_year_Medicare_part_D_prescribers = 
      max(base::cumsum(c(0, diff(year) != 1)) + year)
  )

logger::log_info("Calculated last consecutive year for {nrow(last_consecutive_year)} providers")

# Check specific providers
last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L) %>%
  {logger::log_info("Last consecutive year for NPI 1689603763: {.$last_consecutive_year_Medicare_part_D_prescribers}")}

last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1508953654) %>%
  {logger::log_info("Last consecutive year for NPI 1508953654 (Sue Davidson): {.$last_consecutive_year_Medicare_part_D_prescribers}")}

last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1972523165) %>%
  {logger::log_info("Last consecutive year for NPI 1972523165 (Chris Carey): {.$last_consecutive_year_Medicare_part_D_prescribers}")}

last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1548363484) %>%
  {logger::log_info("Last consecutive year for NPI 1548363484 (Karlotta): {.$last_consecutive_year_Medicare_part_D_prescribers}")}

# Write final consecutive year data to CSV
consecutive_year_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_last_consecutive_year.csv"
logger::log_info("Writing last consecutive year data to: {consecutive_year_output_path}")
readr::write_csv(last_consecutive_year, consecutive_year_output_path)
logger::log_info("Last consecutive year data successfully written")

# Disconnect from database
logger::log_info("Disconnecting from database")
dbDisconnect(con)
logger::log_info("Processing complete")




##############
# Medicare Part D Prescribers Data Processing Script
# Purpose: Process and analyze Medicare Part D prescriber data
# ---------------------------------------------------------------

# Setup logging for better tracking
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Define all paths in one centralized location
directory_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
output_csv_path <- "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv"
final_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
consecutive_year_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_last_consecutive_year.csv"

# Set conflict preferences for function namespace resolution
logger::log_info("Setting up function conflict preferences")
conflicted::conflicts_prefer(stringr::str_remove_all)
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

# Database connection
logger::log_info("Connecting to DuckDB at: {duckdb_file_path}")
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
assertthat::assert_that(!is.null(con), msg = "Database connection failed")

# Check if tables already exist and skip creation if they do
logger::log_info("Checking existing tables in database")
existing_tables <- dbListTables(con)
logger::log_info("Found {length(existing_tables)} existing tables")

# Get file list
file_names <- list.files(directory_path)
logger::log_info("Found {length(file_names)} files in directory")

# Create tables only if they don't exist
created_tables <- character(0)
for (i in seq_along(file_names)) {
  file_name <- file_names[i]
  full_path <- file.path(directory_path, file_name)
  table_name <- tools::file_path_sans_ext(gsub("[^A-Za-z0-9]", "_", file_name))
  table_name <- paste0(table_name, "_", i)
  
  if (table_name %in% existing_tables) {
    logger::log_info("Table '{table_name}' already exists, skipping creation")
    created_tables <- c(created_tables, table_name)
    next
  }
  
  logger::log_info("Creating table '{table_name}' from '{file_name}'")
  
  # SQL command with better error handling
  sql_command <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
    table_name, full_path
  )
  
  tryCatch({
    dbExecute(con, sql_command)
    logger::log_info("Table '{table_name}' created successfully")
    created_tables <- c(created_tables, table_name)
  }, error = function(e) {
    logger::log_error("Failed to create table '{table_name}': {e$message}")
  })
}

# Define tables to process - directly use the known table names
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

# Validate tables exist
missing_tables <- setdiff(table_names, existing_tables)
if (length(missing_tables) > 0) {
  logger::log_error("The following tables are missing: {paste(missing_tables, collapse=', ')}")
  stop("Missing required tables")
}

logger::log_info("All required tables are available for processing")

# Optional: Check column consistency (quick schema validation)
logger::log_info("Performing schema validation across tables")
schema_check <- list()
column_counts <- integer(length(table_names))

for (i in seq_along(table_names)) {
  table_name <- table_names[i]
  table_cols <- names(dbReadTable(con, table_name, n = 1))
  schema_check[[table_name]] <- table_cols
  column_counts[i] <- length(table_cols)
  logger::log_info("Table {table_name} has {length(table_cols)} columns")
}

# Check if all tables have same number of columns
if (length(unique(column_counts)) > 1) {
  logger::log_warn("Tables have different column counts. This may cause issues.")
}

# Process each table with filtering criteria
logger::log_info("Processing tables for gynecological specialties in the US")
processed_records <- list()
combined_data <- data.frame()

# Set filtering criteria
specialty_types <- c("Gynecological Oncology", "Obstetrics & Gynecology")
country_code <- "US"
max_claim_threshold <- 50000

# Ensure output directory exists
output_dir <- dirname(output_csv_path)
if (!dir.exists(output_dir)) {
  logger::log_info("Creating output directory: {output_dir}")
  dir.create(output_dir, recursive = TRUE)
}

# Process each table with improved error handling
for (i in seq_along(table_names)) {
  table_name <- table_names[i]
  logger::log_info("Processing table {i}/{length(table_names)}: {table_name}")
  
  tryCatch({
    # Create reference and apply filters
    table_ref <- tbl(con, table_name)
    
    # Document each step for clarity
    logger::log_info("Applying specialty and country filters")
    filtered_by_specialty <- table_ref %>%
      dplyr::filter(Prscrbr_Type %in% specialty_types & Prscrbr_Cntry == country_code)
    
    logger::log_info("Selecting key columns and applying claim threshold")
    filtered_by_claims <- filtered_by_specialty %>%
      dplyr::select(PRSCRBR_NPI, Tot_Clms) %>%
      dplyr::filter(Tot_Clms < max_claim_threshold)
    
    logger::log_info("Adding prescription indicator and year")
    processed_data <- filtered_by_claims %>%
      dplyr::mutate(Prescribed = "Prescription written") %>%
      dplyr::distinct(PRSCRBR_NPI, .keep_all = TRUE) %>%
      dplyr::mutate(year = table_name)
    
    # Collect the processed data
    logger::log_info("Collecting processed data from table")
    processed_data_df <- processed_data %>% collect()
    record_count <- nrow(processed_data_df)
    logger::log_info("Collected {record_count} records from {table_name}")
    
    # Store results
    combined_data <- dplyr::bind_rows(combined_data, processed_data_df)
    processed_records[[table_name]] <- processed_data
    
  }, error = function(e) {
    logger::log_error("Failed to process table {table_name}: {e$message}")
  })
}

total_records <- nrow(combined_data)
logger::log_info("Total records in combined dataset: {total_records}")

# Write intermediate result to CSV
logger::log_info("Writing processed data to: {output_csv_path}")
tryCatch({
  readr::write_csv(combined_data, output_csv_path)
  logger::log_info("Successfully wrote {total_records} records to {output_csv_path}")
}, error = function(e) {
  logger::log_error("Failed to write CSV: {e$message}")
})

# Perform sanity checks on specific NPIs
logger::log_info("Performing sanity checks on specific NPIs")
test_npi <- 1689603763L

test_result <- processed_records[[1]] %>%
  dplyr::filter(PRSCRBR_NPI == test_npi) %>%
  collect()

if (nrow(test_result) > 0) {
  logger::log_info("Sanity check passed: Found records for NPI {test_npi}")
} else {
  logger::log_warn("Sanity check: No records found for NPI {test_npi} in first table")
}

# Create finalized dataset with clean year values
logger::log_info("Creating combined dataset with standardized year format")
medicare_prescribers_final <- lapply(processed_records, function(table_ref) {
  table_ref %>% collect() %>% as.data.frame()
}) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(year = stringr::str_extract(year, "DY\\d+")) %>%
  dplyr::mutate(year = stringr::str_remove(year, "DY")) %>%
  dplyr::mutate(year = factor(year)) %>%
  dplyr::distinct(PRSCRBR_NPI, year, .keep_all = TRUE) %>%
  dplyr::ungroup()

logger::log_info("Combined final dataset contains {nrow(medicare_prescribers_final)} records")

# Verify data for specific providers
key_npis <- c(
  1689603763L, # Self
  1508953654,  # Sue Davidson
  1972523165,  # Chris Carey
  1548363484   # Karlotta
)

for (npi in key_npis) {
  npi_count <- medicare_prescribers_final %>%
    dplyr::filter(PRSCRBR_NPI == npi) %>%
    nrow()
  
  logger::log_info("Found {npi_count} records for NPI {npi}")
}

# Write final combined dataset
logger::log_info("Writing final dataset to: {final_output_path}")
readr::write_csv(medicare_prescribers_final, final_output_path)
logger::log_info("Final dataset successfully written")

# Calculate consecutive years analysis
logger::log_info("Analyzing consecutive years of prescribing")
consecutive_year_data <- readr::read_csv(final_output_path)
logger::log_info("Loaded {nrow(consecutive_year_data)} records for consecutive year analysis")

# Extract and standardize year format
logger::log_info("Standardizing year format")
consecutive_year_data <- consecutive_year_data %>%
  dplyr::mutate(
    year = sub(".*RY(\\d+).*", "\\1", year),
    year = paste0("20", year),
    year = sub("_.*", "", year),
    year = as.numeric(year)
  )

logger::log_info("Computing last consecutive year for each provider")
last_consecutive_year <- consecutive_year_data %>%
  dplyr::arrange(PRSCRBR_NPI, year) %>%
  dplyr::group_by(PRSCRBR_NPI) %>%
  dplyr::summarise(
    last_consecutive_year_Medicare_part_D_prescribers = 
      max(base::cumsum(c(0, diff(year) != 1)) + year)
  )

logger::log_info("Identified last consecutive year for {nrow(last_consecutive_year)} providers")

# Check specific providers of interest
for (npi in key_npis) {
  provider_data <- last_consecutive_year %>%
    dplyr::filter(PRSCRBR_NPI == npi)
  
  if (nrow(provider_data) > 0) {
    logger::log_info("Provider {npi} last consecutive year: {provider_data$last_consecutive_year_Medicare_part_D_prescribers}")
  } else {
    logger::log_warn("No consecutive year data for provider {npi}")
  }
}

# Write consecutive year analysis to CSV
logger::log_info("Writing consecutive year analysis to: {consecutive_year_output_path}")
readr::write_csv(last_consecutive_year, consecutive_year_output_path)
logger::log_info("Consecutive year analysis successfully written")

# Clean up
logger::log_info("Disconnecting from database")
dbDisconnect(con)
logger::log_info("Processing complete")


#######
# NPI Deactivated
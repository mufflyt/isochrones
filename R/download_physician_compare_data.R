# Required packages
library(dplyr)
library(stringr)
library(DBI)
library(duckdb)
library(logger)
library(assertthat)
library(fs)

# Set up logging
logger::log_threshold(logger::INFO)
logger::log_formatter(logger::formatter_glue)
logger::log_info("Starting streamlined data processing pipeline")

# Define paths
zip_files_dir <- "/Volumes/Video Projects Muffly 1/physician_compare"
unzip_dir <- file.path(zip_files_dir, "unzipped_files")
database_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"

# Find the largest CSV file in each year directory
logger::log_info("Finding largest CSV file in each year directory")
year_dirs <- fs::dir_ls(unzip_dir, type = "directory")

largest_files <- purrr::map_chr(year_dirs, function(year_dir) {
  # Get all CSV files in this year directory
  year_csvs <- fs::dir_ls(year_dir, glob = "*.csv", type = "file")
  
  if (length(year_csvs) > 0) {
    # Get file sizes
    year_csv_sizes <- file.size(year_csvs)
    # Get the largest file
    largest_file <- year_csvs[which.max(year_csv_sizes)]
    year_str <- basename(year_dir)
    largest_size_mb <- round(file.size(largest_file) / (1024 * 1024), 1)
    logger::log_info("Selected {basename(largest_file)} ({largest_size_mb} MB) from {year_str}")
    return(largest_file)
  } else {
    logger::log_warn("No CSV files found in {basename(year_dir)}")
    return(NA_character_)
  }
})

# Remove any NA values
main_data_files <- largest_files[!is.na(largest_files)]
logger::log_info("Selected {length(main_data_files)} files for processing (one per year)")

# Connect to DuckDB
logger::log_info("Connecting to DuckDB at {database_path}")
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = database_path)

# Create target table with simplified structure
table_name <- "physician_compare_data"
logger::log_info("Creating target table with simplified structure")

# Create target table with key columns (common across most years)
create_table_sql <- "
CREATE TABLE IF NOT EXISTS physician_compare_data (
  NPI VARCHAR,
  PAC_ID VARCHAR, 
  Provider_Last_Name VARCHAR,
  Provider_First_Name VARCHAR,
  Provider_MI VARCHAR,
  Provider_Credentials VARCHAR,
  Gender VARCHAR,
  Medical_school VARCHAR,
  Graduation_year VARCHAR,
  Primary_specialty VARCHAR,
  Secondary_specialty1 VARCHAR,
  Secondary_specialty2 VARCHAR,
  Secondary_specialty3 VARCHAR,
  Secondary_specialty4 VARCHAR,
  All_secondary_specialties VARCHAR,
  Organization_legal_name VARCHAR,
  Group_PAC_ID VARCHAR,
  Address_Line_1 VARCHAR,
  Address_Line_2 VARCHAR,
  City VARCHAR,
  State VARCHAR,
  Zip VARCHAR,
  Phone_Number VARCHAR,
  source_file VARCHAR,
  data_year INTEGER
)"

# Execute create table
DBI::dbExecute(con, create_table_sql)
logger::log_info("Created table structure")

# Process each file one at a time
for (file_path in main_data_files) {
  file_name <- basename(file_path)
  year_str <- stringr::str_extract(dirname(file_path), "\\d{4}$")
  
  logger::log_info("Processing file: {file_name} from {year_str}")
  
  # Create a temporary table name for this file
  temp_table <- paste0("temp_", gsub("[^a-zA-Z0-9]", "_", file_name))
  
  # Import CSV directly into DuckDB with error handling enabled
  import_sql <- paste0(
    "CREATE TABLE ", temp_table, " AS SELECT * FROM read_csv_auto('", 
    file_path, "', header=true, sample_size=1000, all_varchar=true, ignore_errors=true, strict_mode=false, null_padding=true);"
  )
  
  tryCatch({
    # Import the file
    DBI::dbExecute(con, import_sql)
    
    # Get columns in this file
    temp_cols <- DBI::dbListFields(con, temp_table)
    logger::log_info("File has {length(temp_cols)} columns")
    
    # Map the standard column names to this file's columns
    # This handles the different column naming conventions across years
    col_mappings <- list(
      NPI = c("NPI", "National_Provider_Identifier", "National Provider Identifier", "NPI Number"),
      PAC_ID = c("PAC_ID", "Professional_Enrollment_ID", "Professional Enrollment ID", "Professional_Enrollment__ID", "Enrollment_ID"),
      Provider_Last_Name = c("Last_Name", "Last Name", "Provider_Last_Name", "Provider Last Name"),
      Provider_First_Name = c("First_Name", "First Name", "Provider_First_Name", "Provider First Name"),
      Provider_MI = c("Middle_Name", "Middle Name", "Provider_Middle_Name", "Provider Middle Name", "MI"),
      Provider_Credentials = c("Credential", "Credentials", "Provider_Credential", "Provider Credential"),
      Gender = c("Gender", "Provider_Gender", "Provider Gender", "Sex"),
      Medical_school = c("Medical_school", "Medical School", "Medical school", "School"),
      Graduation_year = c("Graduation_year", "Graduation Year", "Year_of_Graduation"),
      Primary_specialty = c("Primary_specialty", "Primary Specialty", "Provider_Primary_Specialty", "Provider Primary Specialty"),
      Secondary_specialty1 = c("Secondary_specialty", "Secondary Specialty", "Provider_Secondary_Specialty_1"),
      Organization_legal_name = c("Organization_legal_name", "Group_Practice_Name", "Group Practice Name", "Organization_Legal_Name"),
      Group_PAC_ID = c("Group_PAC_ID", "Organization_PAC_ID", "Group_PAC_ID", "Group PAC ID"),
      Address_Line_1 = c("Address_Line_1", "Address Line 1", "Line_1_Street_Address", "Line 1 Street Address"),
      Address_Line_2 = c("Address_Line_2", "Address Line 2", "Line_2_Street_Address", "Line 2 Street Address"),
      City = c("City", "City_Name", "City Name"),
      State = c("State", "State_Name", "State Name"),
      Zip = c("Zip", "Zip_Code", "Zip Code", "ZIP", "ZIP_Code", "Postal_Code"),
      Phone_Number = c("Phone_Number", "Phone Number", "Telephone_Number")
    )
    
    # Build column mapping SQL
    select_columns <- sapply(names(col_mappings), function(std_col) {
      # Try to find a matching column in the temp table
      col_matches <- intersect(unlist(col_mappings[std_col]), temp_cols)
      if (length(col_matches) > 0) {
        # If found, use the first matching column
        paste0('"', col_matches[1], '" AS "', std_col, '"')
      } else {
        # If not found, use NULL
        paste0('NULL AS "', std_col, '"')
      }
    })
    
    # Get row count
    row_count <- DBI::dbGetQuery(con, paste0("SELECT COUNT(*) FROM ", temp_table))[[1]]
    logger::log_info("Importing {row_count} rows from {file_name}")
    
    # Insert data in chunks to avoid memory issues
    chunk_size <- 100000
    num_chunks <- ceiling(row_count / chunk_size)
    
    for (chunk in 1:num_chunks) {
      offset <- (chunk - 1) * chunk_size
      
      insert_sql <- paste0(
        "INSERT INTO ", table_name, " \n",
        "SELECT ", paste(select_columns, collapse = ", "), ", \n",
        "'", file_name, "' AS source_file, \n", 
        year_str, " AS data_year \n",
        "FROM ", temp_table, " \n",
        "LIMIT ", chunk_size, " OFFSET ", offset
      )
      
      DBI::dbExecute(con, insert_sql)
      logger::log_info("Imported chunk {chunk}/{num_chunks} from {file_name}")
    }
    
    # Drop temporary table to free memory
    DBI::dbExecute(con, paste0("DROP TABLE ", temp_table))
    logger::log_info("Completed import of {file_name}")
    
  }, error = function(e) {
    logger::log_error("Failed to process {file_name}: {e$message}")
    # Try to clean up if temp table was created
    tryCatch({
      DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", temp_table))
    }, error = function(e2) {
      # Ignore cleanup errors
    })
  })
  
  # Force garbage collection after each file
  gc()
}

# Count rows to verify
row_count <- DBI::dbGetQuery(con, paste0("SELECT COUNT(*) FROM ", table_name))[[1]]
logger::log_info("Table {table_name} created with {row_count} rows")

# Create indexes for faster querying
logger::log_info("Creating indexes on common query columns")
DBI::dbExecute(con, paste0("CREATE INDEX IF NOT EXISTS idx_year ON ", table_name, "(data_year)"))
DBI::dbExecute(con, paste0("CREATE INDEX IF NOT EXISTS idx_npi ON ", table_name, "(NPI)"))
DBI::dbExecute(con, paste0("CREATE INDEX IF NOT EXISTS idx_last_name ON ", table_name, "(Provider_Last_Name)"))
DBI::dbExecute(con, paste0("CREATE INDEX IF NOT EXISTS idx_state ON ", table_name, "(State)"))

# Create a sample query to check results
sample_query <- "
WITH provider_counts AS (
  SELECT 
    data_year, 
    COUNT(*) as record_count, 
    COUNT(DISTINCT NPI) as unique_npi_count,
    COUNT(DISTINCT State) as state_count
  FROM physician_compare_data
  GROUP BY data_year
)
SELECT * FROM provider_counts
ORDER BY data_year
"

year_summary <- DBI::dbGetQuery(con, sample_query)
logger::log_info("Summary by year: {paste(capture.output(print(year_summary)), collapse='\\n')}")

# Query for the total number of unique providers
unique_providers_query <- "
SELECT COUNT(DISTINCT NPI) as total_unique_providers
FROM physician_compare_data
"
unique_providers <- DBI::dbGetQuery(con, unique_providers_query)[[1]]
logger::log_info("Total unique providers across all years: {unique_providers}")

# Close connection
DBI::dbDisconnect(con)
logger::log_info("Disconnected from DuckDB. Process complete!")


#####


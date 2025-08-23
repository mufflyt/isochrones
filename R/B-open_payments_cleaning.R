#######################
source("R/01-setup.R")
#######################

#' Process Open Payments Data Using DuckDB
# Load global setup
source("R/01-setup.R")

# Valid payment type categories
PAYMENT_TYPES <- c("general", "research", "ownership")
#'
#' This function reads Open Payments CSV files (General, Research, or Ownership payments)
#' into a DuckDB database, normalizes the schema across years, and merges them into a
#' unified table for analysis. It supports verbose logging, selective column inclusion,
#' and limiting the number of rows per file for testing.
#'
#' @param base_dir Character. Path to the root directory containing Open Payments CSV files.
#'                 This directory may include subdirectories by year.
#' @param db_path Character. Full file path to the DuckDB database (.duckdb file).
#' @param years Numeric vector or NULL. Optional filter for years to process (e.g., c(2019, 2020)).
#'              If NULL, all detected years will be processed.
#' @param payment_type Character. One of "general", "research", or "ownership". Determines which
#'                     file type to import. Must match naming in Open Payments data files.
#' @param output_table_name Character. Name of the merged table to create in the DuckDB database.
#' @param verbose Logical. If TRUE, logs detailed progress using the `logger` package.
#'                If FALSE, only success messages will be printed.
#' @param force_reimport Logical. If TRUE, imports all matching files even if previously processed.
#'                       If FALSE, skips already-imported files based on metadata.
#' @param preserve_all_columns Logical. If TRUE, retains all columns found across years.
#'                             If FALSE, keeps only a consistent subset of key columns.
#' @param max_rows Numeric or NULL. Maximum number of rows to import from each file (useful for testing).
#'                 If NULL, all rows will be imported.
#'
#' @return A named list with the following elements:
#'   \item{connection}{DuckDB connection object}
#'   \item{output_table}{Name of the merged table}
#'   \item{years_processed}{Vector of processed years}
#'   \item{files_processed}{Count of files processed}
#'   \item{rows_imported}{Total number of rows merged into the final table}
#'   \item{columns_mapped}{Count of standardized columns in the final schema}
#'
#' @export
process_open_payments_data <- function(base_dir,
                                       db_path,
                                       years = NULL,
                                       payment_type = "general",
                                       output_table_name = "open_payments_merged",
                                       verbose = TRUE,
                                       force_reimport = FALSE,
                                       preserve_all_columns = TRUE,
                                       max_rows = 20) {
  
  # -------------------------------------------------------------------
  # Set up the logger output (INFO level if verbose, SUCCESS otherwise)
  # -------------------------------------------------------------------
  setup_open_payments_logger(verbose)
  
  # ---------------------------------------------------------------
  # Ensure all arguments are valid and correctly formatted
  # ---------------------------------------------------------------
  logger::log_debug("Validating input parameters")
  validate_open_payments_inputs(
    base_dir,
    db_path,
    years,
    payment_type,
    output_table_name,
    verbose,
    force_reimport,
    preserve_all_columns,
    max_rows
  )
  logger::log_debug("Input validation completed")
  
  # ---------------------------------------------------------------
  # Define regex patterns for the different types of payment files
  # ---------------------------------------------------------------
  file_patterns <- list(
    general = "OP_DTL_GNRL_PGYR\\d{4}.*\\.csv$",
    research = "OP_DTL_RSRCH_PGYR\\d{4}.*\\.csv$",
    ownership = "OP_DTL_OWNRSHP_PGYR\\d{4}.*\\.csv$"
  )
  
  # ---------------------------------------------------------------
  # Ensure selected payment type is valid and extract its pattern
  # ---------------------------------------------------------------
  assertthat::assert_that(
    payment_type %in% PAYMENT_TYPES,
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  file_pattern <- file_patterns[[payment_type]]
  
  # Start processing with info message
  logger::log_info("Starting Open Payments data processing")
  
  # Log max rows limit if set
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Limiting import to %d rows per file", max_rows))
  }
  
  # Log column preservation setting
  if (preserve_all_columns) {
    logger::log_info("Preserving all original columns from source files")
  }
  
  # ---------------------------------------------------------------
  # Connect to or create DuckDB database at provided path
  # ---------------------------------------------------------------
  payments_conn <- connect_to_duckdb(db_path)
  
  # ----------------------------------------------------------------
  # Create metadata tracking tables if they don't exist or are wrong
  # ----------------------------------------------------------------
  initialize_metadata_tables(payments_conn)
  
  # -------------------------------------------------------------------
  # Search for matching Open Payments CSV files recursively
  # -------------------------------------------------------------------
  logger::log_info(sprintf("Searching for files matching: %s", file_pattern))
  csv_files <- find_open_payments_files(base_dir, file_pattern)
  
  
  # -------------------------------------------------------------------
  # Import and track each file, skipping ones already loaded unless forced
  # -------------------------------------------------------------------
  processed_files <- import_open_payments_files(
    csv_files = csv_files,
    conn = payments_conn,
    years = years,
    force_reimport = force_reimport,
    max_rows = max_rows
  )
  
  # -------------------------------------------------------------------
  # Create standardized column mapping for all imported years
  # -------------------------------------------------------------------
  column_mapping <- create_column_mapping(
    conn = payments_conn,
    years = processed_files$years,
    preserve_all_columns = preserve_all_columns,
    payment_type = payment_type
  )
  
  # -------------------------------------------------------------------
  # Merge all yearly tables into one long-format final table
  # -------------------------------------------------------------------
  merged_data <- merge_open_payments_data(
    conn = payments_conn,
    years = processed_files$years,
    output_table_name = output_table_name,
    column_mapping = column_mapping
  )
  
  # -------------------------------------------------------------------
  # Validate presence and content of NPI-related columns in final table
  # -------------------------------------------------------------------
  check_npi_data(payments_conn, output_table_name)
  
  # -------------------------------------------------------------------
  # Signal completion
  # -------------------------------------------------------------------
  logger::log_success("Open Payments data processing completed successfully")
  
  # -------------------------------------------------------------------
  # Return list summarizing the entire import and merge process
  # -------------------------------------------------------------------
  return(list(
    connection = payments_conn,
    output_table = output_table_name,
    years_processed = processed_files$years,
    files_processed = processed_files$files,
    rows_imported = merged_data$total_rows,
    columns_mapped = length(column_mapping$final_columns)
  ))
}

#' @noRd
find_open_payments_files <- function(base_dir, file_pattern) {
  # Get all CSV files in the directory
  all_files <- list.files(
    path = base_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )
  
  # Filter for files matching the pattern
  matching_files <- all_files[grep(file_pattern, basename(all_files))]
  
  # Ensure we found at least one file
  assertthat::assert_that(
    length(matching_files) > 0,
    msg = sprintf("No files matching pattern '%s' found in %s", file_pattern, base_dir)
  )
  
  # Get file sizes for logging
  file_sizes <- file.size(matching_files) / (1024 * 1024)  # Convert to MB
  total_size <- sum(file_sizes)
  
  # Log file information
  logger::log_info(sprintf("Found %d files totaling %.2f MB", 
                           length(matching_files), total_size))
  
  # Return data frame with file information
  file_data <- data.frame(
    file_path = matching_files,
    file_size = file_sizes,
    file_name = basename(matching_files),
    stringsAsFactors = FALSE
  )
  
  # Extract year from filename
  file_data$year <- as.integer(stringr::str_extract(
    file_data$file_name, 
    "PGYR(\\d{4})", 
    group = 1
  ))
  
  return(file_data)
}


#' @noRd
setup_open_payments_logger <- function(verbose) {
  # Configure logger based on verbose setting
  if (verbose) {
    logger::log_threshold(logger::INFO)
  } else {
    logger::log_threshold(logger::SUCCESS)
  }
  
  # Set log layout with timestamp
  logger::log_layout(logger::layout_glue_generator(
    format = "{level} [{time}] {msg}"
  ))
}

#' @noRd
validate_open_payments_inputs <- function(base_dir, 
                                          db_path, 
                                          years, 
                                          payment_type, 
                                          output_table_name, 
                                          verbose,
                                          force_reimport, 
                                          preserve_all_columns, 
                                          max_rows) {
  # Validate base_dir
  assertthat::assert_that(
    is.character(base_dir),
    dir.exists(base_dir),
    msg = "base_dir must be a valid directory path"
  )
  
  # Validate db_path
  assertthat::assert_that(
    is.character(db_path),
    msg = "db_path must be a character string"
  )
  
  # Validate years if provided
  if (!is.null(years)) {
    assertthat::assert_that(
      is.numeric(years),
      all(years >= 2013),
      all(years <= as.numeric(format(Sys.Date(), "%Y"))),
      msg = "years must be a numeric vector with valid years (2013 or later)"
    )
  }
  
  # Validate payment_type
  assertthat::assert_that(
    is.character(payment_type),
    payment_type %in% PAYMENT_TYPES,
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  
  # Validate output_table_name
  assertthat::assert_that(
    is.character(output_table_name),
    nchar(output_table_name) > 0,
    msg = "output_table_name must be a non-empty character string"
  )
  
  # Validate verbose
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be a logical value (TRUE or FALSE)"
  )
  
  # Validate force_reimport
  assertthat::assert_that(
    is.logical(force_reimport),
    msg = "force_reimport must be a logical value (TRUE or FALSE)"
  )
  
  # Validate preserve_all_columns
  assertthat::assert_that(
    is.logical(preserve_all_columns),
    msg = "preserve_all_columns must be a logical value (TRUE or FALSE)"
  )
  
  # Validate max_rows if provided
  if (!is.null(max_rows)) {
    assertthat::assert_that(
      is.numeric(max_rows),
      max_rows > 0,
      msg = "max_rows must be a positive numeric value or NULL"
    )
  }
}

#' @noRd
connect_to_duckdb <- function(db_path) {
  # Try to connect with existing db
  conn <- tryCatch({
    DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  }, error = function(e) {
    # If connection fails, try with a new database file
    new_path <- paste0(tools::file_path_sans_ext(db_path), "_new.duckdb")
    logger::log_warn(paste0("Failed to connect to existing database. Creating new one at: ", new_path))
    DBI::dbConnect(duckdb::duckdb(), dbdir = new_path, read_only = FALSE)
  })
  
  logger::log_success("Successfully connected to DuckDB")
  return(conn)
}

#' @noRd
initialize_metadata_tables <- function(conn) {
  logger::log_info("Initializing database tables")
  
  # Check structure of op_file_metadata
  if (DBI::dbExistsTable(conn, "op_file_metadata")) {
    existing_columns <- DBI::dbListFields(conn, "op_file_metadata")
    required_columns <- c("file_path", "file_size", "year",
                          "import_timestamp", "row_count", "status")
    
    if (!all(required_columns %in% existing_columns)) {
      logger::log_warn("Existing op_file_metadata table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_file_metadata")
    }
  }
  
  # Create metadata table for tracking imported files if it doesn't exist
  if (!DBI::dbExistsTable(conn, "op_file_metadata")) {
    DBI::dbExecute(conn, "
      CREATE TABLE op_file_metadata (
        file_path VARCHAR,
        file_size DOUBLE,
        year INTEGER,
        import_timestamp TIMESTAMP,
        row_count INTEGER,
        status VARCHAR,
        PRIMARY KEY (file_path)
      )
    ")
    logger::log_success("Created op_file_metadata table")
  }
  
  # Check and create op_column_mappings with correct schema
  if (DBI::dbExistsTable(conn, "op_column_mappings")) {
    existing_columns <- DBI::dbListFields(conn, "op_column_mappings")
    required_columns <- c("year", "original_column", "standardized_column", "data_type")
    
    if (!all(required_columns %in% existing_columns)) {
      logger::log_warn("Existing op_column_mappings table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_column_mappings")
    }
  }
  
  if (!DBI::dbExistsTable(conn, "op_column_mappings")) {
    DBI::dbExecute(conn, "
    CREATE TABLE op_column_mappings (
      year INTEGER,
      original_column VARCHAR,
      standardized_column VARCHAR,
      data_type VARCHAR,
      PRIMARY KEY (year, original_column)
    )
  ")
    logger::log_success("Created op_column_mappings table")
  }
}

#' @noRd
import_open_payments_files <- function(csv_files, conn, years, force_reimport, max_rows) {
  # If specific years requested, filter files
  if (!is.null(years)) {
    csv_files <- csv_files[csv_files$year %in% years, ]
    logger::log_info(sprintf("Filtered to %d files for requested years", nrow(csv_files)))
  }
  
  # Sort files by year
  csv_files <- csv_files[order(csv_files$year), ]
  
  # Check if force_reimport is enabled
  if (force_reimport) {
    logger::log_info("Force reimport is enabled. All files will be reimported.")
  } else {
    # Check which files have already been imported
    imported_files <- DBI::dbGetQuery(
      conn,
      "SELECT file_path, year, row_count FROM op_file_metadata WHERE status = 'completed'"
    )
    
    if (nrow(imported_files) > 0) {
      # Filter out already imported files
      csv_files <- csv_files[!csv_files$file_path %in% imported_files$file_path, ]
      logger::log_info(sprintf("%d files already imported, skipping them", 
                               nrow(imported_files)))
    }
  }
  
  # If no files to import, return early
  if (nrow(csv_files) == 0) {
    logger::log_info("No new files to import")
    
    # Get list of imported years
    imported_years <- DBI::dbGetQuery(
      conn,
      "SELECT DISTINCT year FROM op_file_metadata ORDER BY year"
    )$year
    
    return(list(
      years = imported_years,
      files = 0
    ))
  }
  
  # Log import start
  logger::log_info("Importing files to DuckDB with batched processing")
  
  # Process each file
  imported_years <- c()
  for (i in 1:nrow(csv_files)) {
    file_info <- csv_files[i, ]
    logger::log_info(sprintf("Importing file %d/%d: %s (%.2f MB)", 
                             i, nrow(csv_files), 
                             basename(file_info$file_path), 
                             file_info$file_size))
    
    # Import the file
    import_status <- import_single_file(
      conn = conn,
      file_path = file_info$file_path,
      year = file_info$year,
      max_rows = max_rows
    )
    
    if (import_status) {
      # Track imported years
      imported_years <- unique(c(imported_years, file_info$year))
    }
  }
  
  # Return years and count of files imported
  return(list(
    years = sort(imported_years),
    files = nrow(csv_files)
  ))
}

#' @noRd
import_single_file <- function(conn, file_path, year, max_rows) {
  # Generate a table name for this file
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  table_name <- sprintf("op_data_%d_%s", year, timestamp)
  
  # Create a temporary table name
  temp_table_name <- sprintf("op_temp_%d", year)
  
  # Try to create a schema with relaxed CSV parsing options
  sample_cmd <- sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=1000, AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false)",
    temp_table_name,
    file_path
  )
  
  # Create schema with relaxed parsing
  schema_success <- tryCatch({
    DBI::dbExecute(conn, sample_cmd)
    logger::log_success("Created table schema with relaxed parsing")
    TRUE
  }, error = function(e) {
    logger::log_error(sprintf("Failed to create schema: %s", e$message))
    # Try even more relaxed options
    alt_cmd <- sprintf(
      "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=1000, ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
      temp_table_name,
      file_path
    )
    tryCatch({
      DBI::dbExecute(conn, alt_cmd)
      logger::log_success("Created table schema with all columns as VARCHAR")
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed even with relaxed parsing: %s", e$message))
      FALSE
    })
  })
  
  if (!schema_success) {
    return(FALSE)
  }
  
  # Get column information
  columns <- DBI::dbListFields(conn, temp_table_name)
  logger::log_debug(sprintf("Detected %d columns in the file", length(columns)))
  
  # Check for NPI column variations
  npi_columns <- columns[grep("NPI", columns, ignore.case = TRUE)]
  if (length(npi_columns) > 0) {
    logger::log_debug(sprintf("Found NPI-related columns: %s", 
                              paste(npi_columns, collapse = ", ")))
  } else {
    logger::log_debug("No NPI-related columns found in this file")
  }
  
  # Create the new table with the same schema
  create_table_cmd <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0",
    table_name,
    temp_table_name
  )
  DBI::dbExecute(conn, create_table_cmd)
  
  # Import data with row limit if specified and relaxed CSV parsing
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Importing first %d rows only", max_rows))
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=false, AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false) LIMIT %d",
      table_name,
      file_path,
      max_rows
    )
  } else {
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=false, AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false)",
      table_name,
      file_path
    )
  }
  
  # Execute the import command
  import_success <- tryCatch({
    DBI::dbExecute(conn, import_cmd)
    TRUE
  }, error = function(e) {
    logger::log_error(sprintf("Failed with AUTO_DETECT, trying with ALL_VARCHAR: %s", e$message))
    
    # Try with all VARCHAR as a fallback
    if (!is.null(max_rows)) {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, ignore_errors=true, strict_mode=false) LIMIT %d",
        table_name,
        file_path,
        max_rows
      )
    } else {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
        table_name,
        file_path
      )
    }
    
    tryCatch({
      DBI::dbExecute(conn, fallback_cmd)
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed to import file: %s", e$message))
      FALSE
    })
  })
  
  if (!import_success) {
    # Record failure in metadata
    metadata_cmd <- sprintf(
      "INSERT OR REPLACE INTO op_file_metadata (file_path, file_size, year, import_timestamp, row_count, status)
       VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, 0, 'failed')",
      file_path,
      file.size(file_path) / (1024 * 1024),
      year
    )
    DBI::dbExecute(conn, metadata_cmd)
    return(FALSE)
  }
  
  # Check how many rows were imported
  row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", table_name)
  row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
  
  # Verify NPI data if present
  npi_columns <- columns[grep("NPI", columns, ignore.case = TRUE)]
  if (length(npi_columns) > 0) {
    for (npi_col in npi_columns) {
      npi_check_query <- sprintf(
        "SELECT COUNT(\"%s\") AS npi_count FROM %s WHERE \"%s\" IS NOT NULL", 
        npi_col, table_name, npi_col
      )
      npi_count <- DBI::dbGetQuery(conn, npi_check_query)$npi_count
      logger::log_debug(sprintf("Found %d non-NULL values in column %s", npi_count, npi_col))
    }
  }
  
  # Log success
  if (!is.null(max_rows)) {
    logger::log_success(sprintf("Successfully imported %s rows (limited to %s)",
                                format_with_commas(row_count),
                                format_with_commas(max_rows)))
  } else {
    logger::log_success(sprintf("Successfully imported %s rows",
                                format_with_commas(row_count)))
  }
  
  # Record in metadata table
  metadata_cmd <- sprintf(
    "INSERT OR REPLACE INTO op_file_metadata (file_path, file_size, year, import_timestamp, row_count, status)
     VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, %d, 'completed')",
    file_path,
    file.size(file_path) / (1024 * 1024),
    year,
    row_count
  )
  DBI::dbExecute(conn, metadata_cmd)
  
  # Record column mappings
  for (col in columns) {
    # Create standardized column name (lowercase, spaces to underscores)
    std_col <- stringr::str_replace_all(col, " ", "_")
    std_col <- stringr::str_to_lower(std_col)
    
    # Skip if mapping already exists
    mapping_exists <- DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT COUNT(*) AS cnt FROM op_column_mappings 
         WHERE year = %d AND original_column = '%s'",
        year, col
      )
    )$cnt > 0
    
    if (!mapping_exists) {
      # Get the column type
      col_type_query <- sprintf(
        "SELECT data_type FROM information_schema.columns 
         WHERE table_name = '%s' AND column_name = '%s'",
        temp_table_name, col
      )
      col_type <- tryCatch({
        DBI::dbGetQuery(conn, col_type_query)$data_type[1]
      }, error = function(e) {
        "VARCHAR"
      })
      
      if (is.null(col_type) || length(col_type) == 0) {
        col_type <- "VARCHAR"
      }
      
      # Insert mapping
      mapping_cmd <- sprintf(
        "INSERT INTO op_column_mappings (year, original_column, standardized_column, data_type)
         VALUES (%d, '%s', '%s', '%s')",
        year, col, std_col, col_type
      )
      DBI::dbExecute(conn, mapping_cmd)
    }
  }
  
  return(TRUE)
}

#' @noRd
create_column_mapping <- function(conn, years, preserve_all_columns, payment_type) {
  logger::log_info("Creating comprehensive column mapping to preserve all columns")
  
  # Initialize list to store column information for each year
  year_columns <- list()
  column_types <- list()
  
  # Define high-priority columns
  high_priority_columns <- c(
    "Physician_Profile_ID", "Teaching_Hospital_ID", "Record_ID",
    "Covered_Recipient_NPI", "Physician_NPI", "Recipient_NPI", "NPI",
    "Program_Year", "Total_Amount_of_Payment_USDollars", "Date_of_Payment",
    "Nature_of_Payment_or_Transfer_of_Value", "Form_of_Payment_or_Transfer_of_Value",
    "Physician_First_Name", "Physician_Middle_Name", "Physician_Last_Name",
    "Recipient_State", "Recipient_Country", "Covered_Recipient_Type",
    "Covered_Recipient_Primary_Type_1", "Covered_Recipient_Specialty_1",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Product_Indicator", "Name_of_Associated_Covered_Drug_or_Biological1"
  )
  
  # Get columns for each year
  for (year in years) {
    # Get the table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    
    # Get columns for this year
    columns <- DBI::dbListFields(conn, year_table)
    
    # Get column types
    column_type_query <- sprintf(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'",
      year_table
    )
    column_type_info <- tryCatch({
      DBI::dbGetQuery(conn, column_type_query)
    }, error = function(e) {
      data.frame(column_name = columns, data_type = rep("VARCHAR", length(columns)))
    })
    
    # Store column types
    column_types[[as.character(year)]] <- setNames(
      column_type_info$data_type, 
      column_type_info$column_name
    )
    
    # Store column information
    year_columns[[as.character(year)]] <- columns
    
    # Find and log NPI columns
    npi_cols <- columns[grep("NPI", columns, ignore.case = TRUE)]
    if (length(npi_cols) > 0) {
      logger::log_debug(sprintf("Year %d: Found NPI columns: %s", 
                                year, paste(npi_cols, collapse = ", ")))
    }
    
    # Log column count
    logger::log_debug(sprintf("Year %d: Mapped %d columns", year, length(columns)))
  }
  
  # Create comprehensive list of all unique columns across years
  all_columns <- unique(unlist(year_columns))
  
  # Find all NPI-related columns
  all_npi_columns <- all_columns[grep("NPI", all_columns, ignore.case = TRUE)]
  if (length(all_npi_columns) > 0) {
    logger::log_debug(sprintf("All NPI-related columns across years: %s", 
                              paste(all_npi_columns, collapse = ", ")))
  }
  
  # If not preserving all columns, use a subset of key columns
  if (!preserve_all_columns) {
    # Ensure all NPI fields are included
    core_columns <- unique(c(high_priority_columns, all_npi_columns))
    
    # Filter to only include core columns
    all_columns <- all_columns[all_columns %in% core_columns | 
                                 grepl("NPI", all_columns, ignore.case = TRUE)]
  }
  
  # Return the mapping information
  return(list(
    year_columns = year_columns,
    column_types = column_types,
    final_columns = all_columns,
    npi_columns = all_npi_columns
  ))
}

#' @noRd
merge_open_payments_data <- function(conn, years, output_table_name, column_mapping) {
  logger::log_info(sprintf("Merging data into table: %s", output_table_name))
  
  # Create final table with all columns if it doesn't exist
  if (!DBI::dbExistsTable(conn, output_table_name)) {
    # Generate column definitions
    column_defs <- character(length(column_mapping$final_columns))
    
    for (i in seq_along(column_mapping$final_columns)) {
      col <- column_mapping$final_columns[i]
      col_type <- "VARCHAR"  # Default to VARCHAR
      
      # Try to find a more specific type from any year
      for (year in names(column_mapping$column_types)) {
        if (!is.null(column_mapping$column_types[[year]][[col]])) {
          col_type <- column_mapping$column_types[[year]][[col]]
          break
        }
      }
      
      # For NPI columns, ensure they're VARCHAR to preserve leading zeros
      if (grepl("NPI", col, ignore.case = TRUE)) {
        col_type <- "VARCHAR"
        logger::log_debug(sprintf("Setting column %s as VARCHAR to preserve NPI format", col))
      }
      
      column_defs[i] <- sprintf('"%s" %s', col, col_type)
    }
    
    # Add source table column
    column_defs <- c(column_defs, "Source_Table VARCHAR")
    
    # Create the table
    create_table_cmd <- sprintf(
      "CREATE TABLE %s (%s)",
      output_table_name,
      paste(column_defs, collapse = ", ")
    )
    DBI::dbExecute(conn, create_table_cmd)
  } else {
    # Clear existing data if table exists
    DBI::dbExecute(conn, sprintf("DELETE FROM %s", output_table_name))
  }
  
  # Set batch size for processing
  batch_size <- 100000
  
  # Track total rows merged
  total_rows_merged <- 0
  
  # Process each year
  for (year in years) {
    # Get the most recent table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    
    # Get row count for this year
    row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", year_table)
    year_row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
    
    # Calculate number of batches
    num_batches <- ceiling(year_row_count / batch_size)
    
    logger::log_info(sprintf("Merging %d rows from year %d in %d chunks", 
                             year_row_count, year, num_batches))
    
    # Process each batch
    for (batch in 1:num_batches) {
      # Calculate offsets
      offset <- (batch - 1) * batch_size
      limit <- batch_size
      
      # Generate column list for this year's table
      year_columns <- column_mapping$year_columns[[as.character(year)]]
      
      # Generate SQL SELECT expressions, handling missing columns
      select_expressions <- character(length(column_mapping$final_columns))
      for (i in seq_along(column_mapping$final_columns)) {
        col <- column_mapping$final_columns[i]
        
        if (col %in% year_columns) {
          # Special handling for NPI columns to ensure they're preserved correctly
          if (grepl("NPI", col, ignore.case = TRUE)) {
            select_expressions[i] <- sprintf('CAST("%s" AS VARCHAR) AS "%s"', col, col)
          } else {
            select_expressions[i] <- sprintf('"%s"', col)
          }
        } else {
          select_expressions[i] <- sprintf('NULL AS "%s"', col)
        }
      }
      
      # Create the full SELECT statement
      select_clause <- paste(select_expressions, collapse = ", ")
      
      # Add source table information
      select_clause <- paste(select_clause, sprintf(", '%s' AS Source_Table", year_table), sep = "")
      
      # Create the INSERT statement
      insert_cmd <- sprintf(
        "INSERT INTO %s SELECT %s FROM %s OFFSET %d LIMIT %d",
        output_table_name,
        select_clause,
        year_table,
        offset,
        limit
      )
      
      # Execute the insert
      tryCatch({
        DBI::dbExecute(conn, insert_cmd)
        logger::log_debug(sprintf("Merged chunk %d/%d for year %d", 
                                  batch, num_batches, year))
      }, error = function(e) {
        logger::log_error(sprintf("Failed to merge chunk %d for year %d: %s", 
                                  batch, year, e$message))
      })
    }
    
    # Update total rows
    total_rows_merged <- total_rows_merged + year_row_count
  }
  
  # Log completion
  logger::log_success(sprintf("Successfully merged %s rows into %s",
                              format_with_commas(total_rows_merged),
                              output_table_name))
  
  # Return summary
  return(list(
    table_name = output_table_name,
    total_rows = total_rows_merged,
    years = years
  ))
}

#' @noRd
check_npi_data <- function(conn, table_name) {
  # Get column information
  column_info <- tryCatch({
    DBI::dbGetQuery(conn, sprintf("PRAGMA table_info('%s')", table_name))
  }, error = function(e) {
    return(NULL)
  })
  
  if (is.null(column_info)) {
    logger::log_error("Could not retrieve column information for final table")
    return(FALSE)
  }
  
  # Find NPI columns
  npi_columns <- column_info$name[grep("NPI", column_info$name, ignore.case = TRUE)]
  
  if (length(npi_columns) == 0) {
    logger::log_error("No NPI columns found in the final merged table")
    return(FALSE)
  }
  
  logger::log_info(sprintf("Found %d NPI-related columns in final table: %s", 
                           length(npi_columns), paste(npi_columns, collapse = ", ")))
  
  # Check for non-NULL NPI values
  for (npi_col in npi_columns) {
    query <- sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL", 
                     table_name, npi_col)
    result <- DBI::dbGetQuery(conn, query)
    
    if (result$cnt > 0) {
      logger::log_success(sprintf("Column %s contains %d non-NULL values", 
                                  npi_col, result$cnt))
    } else {
      logger::log_debug(sprintf("Column %s contains only NULL values", npi_col))
      
      # Try to fix NPI data if it's all NULL by copying from temporary tables
      fix_npi_data_from_temp_tables(conn, table_name, npi_col)
    }
  }
  
  return(TRUE)
}

#' @noRd
fix_npi_data_from_temp_tables <- function(conn, output_table_name, npi_column) {
  logger::log_info(sprintf("Attempting to fix missing NPI data for column %s", npi_column))
  
  # Find all temporary tables for each year
  temp_tables <- DBI::dbGetQuery(conn, "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_temp_%'")$table_name
  
  if (length(temp_tables) == 0) {
    logger::log_debug("No temporary tables found for NPI data recovery")
    return(FALSE)
  }
  
  updated_rows <- 0
  
  # Try each temp table
  for (temp_table in temp_tables) {
    # Check if temp table has the NPI column
    temp_columns <- DBI::dbListFields(conn, temp_table)
    
    if (npi_column %in% temp_columns) {
      # Check if the temp table has non-NULL NPI values
      check_query <- sprintf(
        "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL",
        temp_table, npi_column
      )
      has_npi_data <- DBI::dbGetQuery(conn, check_query)$cnt > 0
      
      if (has_npi_data) {
        logger::log_debug(sprintf("Found NPI data in %s, attempting to copy to main table", temp_table))
        
        # Extract year from temp table name
        year <- as.integer(stringr::str_extract(temp_table, "\\d{4}"))
        
        # Create update query based on matching keys
        update_query <- sprintf(
          "UPDATE %s SET \"%s\" = t.\"%s\" 
           FROM %s t 
           WHERE %s.\"Program_Year\" = %d 
           AND %s.\"Physician_Profile_ID\" = t.\"Physician_Profile_ID\"
           AND %s.\"%s\" IS NULL",
          output_table_name, npi_column, npi_column,
          temp_table,
          output_table_name, year,
          output_table_name,
          output_table_name, npi_column
        )
        
        # Execute the update
        tryCatch({
          DBI::dbExecute(conn, update_query)
          
          # Check how many rows were updated
          updated_check_query <- sprintf(
            "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL AND \"Program_Year\" = %d",
            output_table_name, npi_column, year
          )
          updated_count <- DBI::dbGetQuery(conn, updated_check_query)$cnt
          
          logger::log_success(sprintf("Updated %d rows with NPI data from year %d", 
                                      updated_count, year))
          
          updated_rows <- updated_rows + updated_count
        }, error = function(e) {
          logger::log_error(sprintf("Failed to update NPI data from %s: %s", 
                                    temp_table, e$message))
        })
      }
    }
  }
  
  if (updated_rows > 0) {
    logger::log_success(sprintf("Successfully recovered %d NPI values in total", updated_rows))
    return(TRUE)
  } else {
    logger::log_debug("Could not recover any NPI values from temporary tables")
    return(FALSE)
  }
}
###
# Eexcute
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"

####################
# Process the data with error-tolerant CSV parsing
####################
result <- process_open_payments_data(
  base_dir = base_dir,            
  db_path = db_path,              
  payment_type = "general",       
  output_table_name = "open_payments_merged",
  verbose = TRUE,
  force_reimport = TRUE,
  preserve_all_columns = TRUE,
  max_rows = 20                    
)

# Verify the NPI data in the merged table
con <- result$connection
npi_verification <- DBI::dbGetQuery(
  con,
  "SELECT 
     COUNT(*) as total_rows,
     COUNT(\"Covered_Recipient_NPI\") as npi_count,
     (COUNT(\"Covered_Recipient_NPI\") * 100.0 / NULLIF(COUNT(*), 0)) as npi_percentage
   FROM open_payments_merged"
)
print(npi_verification)

# Check the structure of the table to confirm all expected columns exist
table_structure <- DBI::dbGetQuery(
  con,
  "SELECT column_name FROM information_schema.columns WHERE table_name = 'open_payments_merged' 
   AND (column_name LIKE '%NPI%' OR column_name IN ('Physician_Profile_ID', 'Program_Year'))"
)
print(table_structure)


#' ## zz old
#' ####
#' # Second function??
#' ####
#' 
#' # Load required packages
#' library(DBI)
#' library(duckdb)
#' library(dplyr)
#' library(logger)
#' library(assertthat)
#' library(purrr)
#' library(fs)
#' 
#' #####
#' # Improving R function robustness and modularity
#' #####
#' 
#' #' Process and Merge Open Payments Data Using DuckDB with Memory Optimization
#' #'
#' #' @param base_dir Character string. Path to base directory with Open Payments data.
#' #' @param db_path Character string. Path for DuckDB database file.
#' #' @param years Character vector. Years to process. If NULL, all years.
#' #' @param payment_type Character string. "general", "research", or "ownership". Default "general".
#' #' @param file_pattern Character string. Pattern to identify payment files. Default is auto-determined.
#' #' @param selected_columns Character vector. Columns to include. If NULL, uses default key columns.
#' #' @param output_table_name Character string. Name for merged table. Default "open_payments_merged".
#' #' @param save_csv Logical. Whether to save CSV. Default FALSE.
#' #' @param output_csv_path Character string. Path for CSV. Only if save_csv=TRUE.
#' #' @param verbose Logical. Whether to print detailed info. Default TRUE.
#' #' @param force_reimport Logical. Whether to force reimport. Default FALSE.
#' #'
#' #' @return List with DuckDB connection and table name.
#' #'
#' #' @importFrom DBI dbConnect dbExecute dbDisconnect dbListTables dbGetQuery
#' #' @importFrom duckdb duckdb
#' #' @importFrom dplyr tbl filter mutate pull select collect arrange
#' #' @importFrom logger log_info log_success log_error log_debug log_warn
#' #' @importFrom assertthat assert_that
#' #' @importFrom purrr map_dfr map
#' #' @importFrom fs file_size dir_ls
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom tools md5sum
#' #'
#' #' @export
#' process_open_payments_data_optimized <- function(base_dir = "/Volumes/Video Projects Muffly 1/open_payments_data",
#'                                                  db_path = "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb",
#'                                                  years = NULL,
#'                                                  payment_type = "general",
#'                                                  file_pattern = NULL,
#'                                                  selected_columns = NULL,
#'                                                  output_table_name = "open_payments_merged",
#'                                                  save_csv = FALSE,
#'                                                  output_csv_path = NULL,
#'                                                  verbose = TRUE,
#'                                                  force_reimport = FALSE) {
#'   # Set up logging
#'   logger::log_layout(logger::layout_simple)
#'   log_level <- if (verbose) logger::DEBUG else logger::INFO
#'   logger::log_threshold(log_level)
#'   
#'   # Validate inputs - NOTE: Fixed by including force_reimport here
#'   validate_inputs(base_dir, db_path, years, payment_type, file_pattern,
#'                   output_table_name, save_csv, output_csv_path, verbose, force_reimport)
#'   
#'   # Determine file pattern if not provided
#'   if (is.null(file_pattern)) {
#'     type_pattern <- switch(payment_type,
#'                            "general" = "GNRL",
#'                            "research" = "RSRCH",
#'                            "ownership" = "OWNRSHP",
#'                            "GNRL")
#'     file_pattern <- paste0("OP_DTL_", type_pattern, "_PGYR\\d{4}.*\\.csv$")
#'   }
#'   
#'   # Log parameters
#'   logger::log_info("Starting Open Payments data processing")
#'   
#'   # Check database file permissions
#'   check_db_permissions(db_path)
#'   
#'   # Set up database with read_only=FALSE
#'   db_conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
#'   logger::log_success("Successfully connected to DuckDB")
#'   
#'   # Configure for memory efficiency
#'   DBI::dbExecute(db_conn, "PRAGMA memory_limit='4GB'")
#'   DBI::dbExecute(db_conn, "PRAGMA threads=2")
#'   DBI::dbExecute(db_conn, "PRAGMA force_compression='ZSTD'")
#'   
#'   # Initialize tables
#'   initialize_database_tables(db_conn, verbose)
#'   
#'   # Find payment files
#'   payment_files <- find_payment_files(base_dir, file_pattern, years, verbose)
#'   if (nrow(payment_files) == 0) {
#'     logger::log_error("No payment files found. Process aborted.")
#'     DBI::dbDisconnect(db_conn)
#'     return(NULL)
#'   }
#'   
#'   # Check existing imports
#'   if (!force_reimport) {
#'     payment_files <- check_imported_files(db_conn, payment_files, verbose)
#'   } else {
#'     logger::log_info("Force reimport is enabled. All files will be reimported.")
#'     payment_files$already_imported <- FALSE
#'     payment_files$reimport <- TRUE
#'     payment_files$existing_table <- NA_character_
#'   }
#'   
#'   # Import files in batches to reduce memory usage
#'   imported_tables <- import_files_to_duckdb_batched(
#'     db_conn, payment_files, verbose, force_reimport, file_pattern
#'   )
#'   
#'   # Explicitly run garbage collection to free memory after imports
#'   gc()
#'   
#'   # Get columns and mappings
#'   if (is.null(selected_columns)) {
#'     selected_columns <- get_key_columns(payment_type)
#'   }
#'   
#'   all_years <- unique(c(names(imported_tables$new_tables), names(imported_tables$existing_tables)))
#'   column_mapping <- create_column_mapping(
#'     db_conn, imported_tables$new_tables, imported_tables$existing_tables,
#'     selected_columns, verbose
#'   )
#'   
#'   # Merge data with chunking for memory efficiency
#'   merged_table <- merge_payment_data_chunked(
#'     db_conn, imported_tables$new_tables, imported_tables$existing_tables,
#'     column_mapping, output_table_name, verbose
#'   )
#'   
#'   # Free memory
#'   gc()
#'   
#'   # Export if requested
#'   if (save_csv && !is.null(output_csv_path)) {
#'     export_to_csv_chunked(db_conn, output_table_name, output_csv_path, verbose)
#'   }
#'   
#'   # Return result
#'   result <- list(connection = db_conn, table_name = output_table_name)
#'   logger::log_success("Open Payments data processing completed successfully")
#'   
#'   return(result)
#' }
#' 
#' #' Check database permissions
#' #' @param db_path Path to the database
#' #' @noRd
#' check_db_permissions <- function(db_path) {
#'   db_dir <- dirname(db_path)
#'   if (!dir.exists(db_dir)) {
#'     dir.create(db_dir, recursive = TRUE)
#'   }
#'   
#'   if (file.exists(db_path) && file.access(db_path, 2) != 0) {
#'     stop("Database file exists but is not writable: ", db_path)
#'   }
#' }
#' 
#' #' Validate input parameters - Fixed version with force_reimport
#' #' @noRd
#' validate_inputs <- function(base_dir, db_path, years, payment_type, file_pattern,
#'                             output_table_name, save_csv, output_csv_path, verbose,
#'                             force_reimport) {
#'   logger::log_debug("Validating input parameters")
#'   
#'   # Base validations
#'   assertthat::assert_that(is.character(base_dir),
#'                           msg = "base_dir must be a character string")
#'   assertthat::assert_that(dir.exists(base_dir),
#'                           msg = paste0("Directory not found: ", base_dir))
#'   assertthat::assert_that(is.character(db_path),
#'                           msg = "db_path must be a character string")
#'   
#'   # Payment type validation
#'   assertthat::assert_that(is.character(payment_type),
#'                           msg = "payment_type must be a character string")
#'   assertthat::assert_that(payment_type %in% PAYMENT_TYPES,
#'                           msg = "payment_type must be one of: general, research, ownership")
#'   
#'   # File pattern validation
#'   if (!is.null(file_pattern)) {
#'     assertthat::assert_that(is.character(file_pattern),
#'                             msg = "file_pattern must be a character string")
#'   }
#'   
#'   # Output table validation
#'   assertthat::assert_that(is.character(output_table_name),
#'                           msg = "output_table_name must be a character string")
#'   
#'   # Years validation
#'   if (!is.null(years)) {
#'     assertthat::assert_that(is.character(years),
#'                             msg = "years must be a character vector")
#'   }
#'   
#'   # CSV validation
#'   assertthat::assert_that(is.logical(save_csv),
#'                           msg = "save_csv must be a logical value")
#'   if (save_csv) {
#'     assertthat::assert_that(!is.null(output_csv_path),
#'                             msg = "output_csv_path must be provided when save_csv is TRUE")
#'     assertthat::assert_that(is.character(output_csv_path),
#'                             msg = "output_csv_path must be a character string")
#'   }
#'   
#'   # Verbose and force_reimport validations
#'   assertthat::assert_that(is.logical(verbose),
#'                           msg = "verbose must be a logical value")
#'   assertthat::assert_that(is.logical(force_reimport),
#'                           msg = "force_reimport must be a logical value")
#'   
#'   logger::log_debug("Input validation completed")
#' }
#' 
#' #' Initialize database tables with optimized schema
#' #' @noRd
#' initialize_database_tables <- function(connection, verbose) {
#'   logger::log_info("Initializing database tables")
#'   
#'   tables <- DBI::dbListTables(connection)
#'   
#'   # Create metadata table if needed
#'   if (!"op_file_metadata" %in% tables) {
#'     metadata_query <- paste0(
#'       "CREATE TABLE op_file_metadata (",
#'       "filepath VARCHAR PRIMARY KEY, ",
#'       "year VARCHAR, ",
#'       "table_name VARCHAR, ",
#'       "file_size_bytes BIGINT, ",
#'       "file_hash VARCHAR, ",
#'       "import_date TIMESTAMP, ",
#'       "file_pattern VARCHAR",
#'       ")"
#'     )
#'     DBI::dbExecute(connection, metadata_query)
#'     logger::log_success("Created metadata table")
#'   } else {
#'     # Check for BIGINT column type
#'     schema_query <- "PRAGMA table_info('op_file_metadata')"
#'     schema_info <- DBI::dbGetQuery(connection, schema_query)
#'     size_col_info <- schema_info[schema_info$name == "file_size_bytes", ]
#'     
#'     if (nrow(size_col_info) > 0 && size_col_info$type == "INTEGER") {
#'       # Update schema to BIGINT
#'       update_schema_query <- paste0(
#'         "CREATE TABLE op_file_metadata_new (",
#'         "filepath VARCHAR PRIMARY KEY, ",
#'         "year VARCHAR, ",
#'         "table_name VARCHAR, ",
#'         "file_size_bytes BIGINT, ",
#'         "file_hash VARCHAR, ",
#'         "import_date TIMESTAMP, ",
#'         "file_pattern VARCHAR",
#'         ")"
#'       )
#'       DBI::dbExecute(connection, update_schema_query)
#'       DBI::dbExecute(connection, "INSERT INTO op_file_metadata_new SELECT filepath, year, table_name, CAST(file_size_bytes AS BIGINT), file_hash, import_date, file_pattern FROM op_file_metadata")
#'       DBI::dbExecute(connection, "DROP TABLE op_file_metadata")
#'       DBI::dbExecute(connection, "ALTER TABLE op_file_metadata_new RENAME TO op_file_metadata")
#'     }
#'   }
#'   
#'   # Create column mappings table if needed
#'   if (!"op_column_mappings" %in% tables) {
#'     mapping_query <- paste0(
#'       "CREATE TABLE op_column_mappings (",
#'       "year VARCHAR, ",
#'       "standard_column VARCHAR, ",
#'       "actual_column VARCHAR, ",
#'       "PRIMARY KEY (year, standard_column)",
#'       ")"
#'     )
#'     DBI::dbExecute(connection, mapping_query)
#'     logger::log_success("Created column mappings table")
#'   }
#' }
#' 
#' #' Find payment files with optimized search
#' #' @noRd
#' find_payment_files <- function(base_dir, file_pattern, years, verbose) {
#'   logger::log_info(paste0("Searching for files matching: ", file_pattern))
#'   
#'   all_files <- fs::dir_ls(path = base_dir, recurse = TRUE, regexp = file_pattern, type = "file")
#'   
#'   if (length(all_files) == 0) {
#'     return(tibble::tibble(filepath = character(0), year = character(0), filesize_mb = numeric(0)))
#'   }
#'   
#'   # Process in batches for memory efficiency
#'   batch_size <- 100
#'   num_batches <- ceiling(length(all_files) / batch_size)
#'   result_list <- list()
#'   
#'   for (i in 1:num_batches) {
#'     start_idx <- (i-1) * batch_size + 1
#'     end_idx <- min(i * batch_size, length(all_files))
#'     batch_files <- all_files[start_idx:end_idx]
#'     
#'     # Get file info
#'     file_sizes <- as.numeric(fs::file_size(batch_files))
#'     batch_data <- tibble::tibble(
#'       filepath = as.character(batch_files),
#'       year = NA_character_,
#'       filesize_mb = file_sizes / (1024 * 1024)
#'     )
#'     
#'     # Extract year from filename
#'     for (j in 1:nrow(batch_data)) {
#'       filename <- basename(batch_data$filepath[j])
#'       year_match <- regexpr("PGYR(\\d{4})", filename)
#'       if (year_match > 0) {
#'         batch_data$year[j] <- substr(filename, year_match + 4, year_match + 7)
#'       }
#'     }
#'     
#'     result_list[[i]] <- batch_data
#'     
#'     # Free memory
#'     rm(batch_data, file_sizes, batch_files)
#'     gc()
#'   }
#'   
#'   # Combine results
#'   payment_files <- do.call(rbind, result_list)
#'   
#'   # Filter for years if provided
#'   if (!is.null(years) && length(years) > 0) {
#'     payment_files <- dplyr::filter(payment_files, year %in% years)
#'   }
#'   
#'   # Sort by year
#'   payment_files <- dplyr::arrange(payment_files, year)
#'   
#'   # Log summary
#'   if (nrow(payment_files) > 0) {
#'     total_size_mb <- sum(payment_files$filesize_mb)
#'     logger::log_info(paste0("Found ", nrow(payment_files), " files totaling ",
#'                             round(total_size_mb, 2), " MB"))
#'   } else {
#'     logger::log_warn("No files found for specified years")
#'   }
#'   
#'   return(payment_files)
#' }
#' 
#' #' Check imported files with memory optimization
#' #' @noRd
#' check_imported_files <- function(connection, payment_files, verbose) {
#'   logger::log_info("Checking which files are already imported")
#'   
#'   # Initialize status columns
#'   payment_files$already_imported <- FALSE
#'   payment_files$reimport <- FALSE
#'   payment_files$existing_table <- NA_character_
#'   
#'   if (nrow(payment_files) == 0) {
#'     return(payment_files)
#'   }
#'   
#'   # Process in batches to avoid large IN clauses
#'   batch_size <- 50
#'   num_batches <- ceiling(nrow(payment_files) / batch_size)
#'   
#'   for (i in 1:num_batches) {
#'     start_idx <- (i-1) * batch_size + 1
#'     end_idx <- min(i * batch_size, nrow(payment_files))
#'     batch_files <- payment_files$filepath[start_idx:end_idx]
#'     
#'     files_to_check <- paste0("'", batch_files, "'", collapse = ",")
#'     query <- paste0("SELECT filepath, year, table_name, file_size_bytes, file_hash FROM op_file_metadata ",
#'                     "WHERE filepath IN (", files_to_check, ")")
#'     
#'     imported_files <- tryCatch({
#'       DBI::dbGetQuery(connection, query)
#'     }, error = function(e) {
#'       logger::log_warn(paste0("Error checking batch ", i, ": ", e$message))
#'       return(data.frame())
#'     })
#'     
#'     if (nrow(imported_files) > 0) {
#'       # Check files in this batch
#'       for (j in 1:nrow(imported_files)) {
#'         file_path <- imported_files$filepath[j]
#'         idx <- which(payment_files$filepath == file_path)
#'         
#'         if (length(idx) > 0) {
#'           current_size <- as.numeric(fs::file_size(file_path))
#'           db_size <- imported_files$file_size_bytes[j]
#'           
#'           if (current_size != db_size) {
#'             payment_files$reimport[idx] <- TRUE
#'           } else {
#'             payment_files$already_imported[idx] <- TRUE
#'             payment_files$existing_table[idx] <- imported_files$table_name[j]
#'           }
#'         }
#'       }
#'     }
#'     
#'     # Free memory
#'     rm(imported_files, batch_files)
#'     gc()
#'   }
#'   
#'   # Count files
#'   files_to_import <- sum(!payment_files$already_imported | payment_files$reimport)
#'   files_to_reuse <- sum(payment_files$already_imported & !payment_files$reimport)
#'   logger::log_info(paste0(files_to_import, " files need to be imported, ",
#'                           files_to_reuse, " can be reused"))
#'   
#'   return(payment_files)
#' }
#' 
#' #' Import files to DuckDB with batched processing
#' #' @noRd
#' import_files_to_duckdb_batched <- function(connection, payment_files, verbose, force_reimport, file_pattern) {
#'   logger::log_info("Importing files to DuckDB with batched processing")
#'   
#'   new_tables <- list()
#'   existing_tables <- list()
#'   
#'   # Identify files that need to be imported
#'   files_to_import <- payment_files[!payment_files$already_imported | payment_files$reimport | force_reimport, ]
#'   files_already_imported <- payment_files[payment_files$already_imported & !payment_files$reimport & !force_reimport, ]
#'   
#'   if (nrow(files_to_import) > 0) {
#'     for (i in 1:nrow(files_to_import)) {
#'       file_info <- files_to_import[i, ]
#'       filepath <- file_info$filepath
#'       year <- file_info$year
#'       filesize_mb <- file_info$filesize_mb
#'       
#'       table_name <- paste0("op_temp_", year)
#'       
#'       # Check if table exists
#'       tables <- DBI::dbListTables(connection)
#'       if (table_name %in% tables) {
#'         DBI::dbExecute(connection, paste0("DROP TABLE ", table_name))
#'       }
#'       
#'       logger::log_info(paste0("Importing file ", i, "/", nrow(files_to_import),
#'                               ": ", basename(filepath), " (", round(filesize_mb, 2), " MB)"))
#'       
#'       # Use memory-efficient import method
#'       import_success <- import_file_in_chunks(connection, filepath, table_name, verbose)
#'       
#'       if (import_success) {
#'         new_tables[[year]] <- table_name
#'         
#'         # Update metadata
#'         file_hash <- digest::digest(filepath)
#'         file_size <- as.numeric(fs::file_size(filepath))
#'         
#'         upsert_query <- paste0(
#'           "INSERT OR REPLACE INTO op_file_metadata ",
#'           "(filepath, year, table_name, file_size_bytes, file_hash, import_date, file_pattern) ",
#'           "VALUES ('", filepath, "', '", year, "', '", table_name, "', ",
#'           file_size, ", '", file_hash, "', CURRENT_TIMESTAMP, '",
#'           file_pattern, "')"
#'         )
#'         
#'         tryCatch({
#'           DBI::dbExecute(connection, upsert_query)
#'         }, error = function(e) {
#'           logger::log_error(paste0("Failed to update metadata: ", e$message))
#'         })
#'       }
#'       
#'       # Free memory
#'       gc()
#'     }
#'   }
#'   
#'   # Process already imported files
#'   if (nrow(files_already_imported) > 0) {
#'     logger::log_info(paste0("Using ", nrow(files_already_imported), " previously imported tables"))
#'     
#'     for (i in 1:nrow(files_already_imported)) {
#'       year <- files_already_imported$year[i]
#'       table_name <- files_already_imported$existing_table[i]
#'       
#'       tables <- DBI::dbListTables(connection)
#'       if (table_name %in% tables) {
#'         existing_tables[[year]] <- table_name
#'       }
#'     }
#'   }
#'   
#'   return(list(new_tables = new_tables, existing_tables = existing_tables))
#' }
#' 
#' #' Import file in chunks to reduce memory usage
#' #' @noRd
#' import_file_in_chunks <- function(connection, filepath, table_name, verbose) {
#'   # Create table schema with a sample
#'   schema_query <- paste0(
#'     "CREATE TABLE ", table_name, " AS ",
#'     "SELECT * FROM read_csv_auto('", filepath, "', ",
#'     "all_varchar=TRUE, sample_size=1000, header=TRUE) WHERE 1=0"
#'   )
#'   
#'   tryCatch({
#'     # Create empty table with schema
#'     DBI::dbExecute(connection, schema_query)
#'     logger::log_success("Created table schema")
#'     
#'     # Use efficient COPY FROM command
#'     copy_query <- paste0(
#'       "COPY ", table_name, " FROM '", filepath, "' ",
#'       "(AUTO_DETECT TRUE, HEADER TRUE, IGNORE_ERRORS TRUE)"
#'     )
#'     
#'     tryCatch({
#'       DBI::dbExecute(connection, copy_query)
#'       
#'       # Count rows
#'       count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
#'       count_result <- DBI::dbGetQuery(connection, count_query)
#'       row_count <- count_result$row_count[1]
#'       
#'       logger::log_success(paste0("Successfully imported ", row_count, " rows"))
#'       return(TRUE)
#'       
#'     }, error = function(e) {
#'       logger::log_warn(paste0("COPY command failed: ", e$message))
#'       
#'       # Alternative using INSERT with read_csv_auto for each chunk
#'       # DuckDB will automatically handle the chunking internally
#'       insert_query <- paste0(
#'         "INSERT INTO ", table_name, " ",
#'         "SELECT * FROM read_csv_auto('", filepath, "', ",
#'         "all_varchar=TRUE, header=TRUE, ignore_errors=TRUE)"
#'       )
#'       
#'       tryCatch({
#'         DBI::dbExecute(connection, insert_query)
#'         
#'         # Count rows
#'         count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
#'         count_result <- DBI::dbGetQuery(connection, count_query)
#'         row_count <- count_result$row_count[1]
#'         
#'         logger::log_success(paste0("Successfully imported ", row_count, " rows using INSERT"))
#'         return(TRUE)
#'         
#'       }, error = function(e2) {
#'         logger::log_error(paste0("INSERT method also failed: ", e2$message))
#'         
#'         # Last resort: chunk manually in R
#'         manual_import_success <- manual_chunk_import(connection, filepath, table_name, verbose)
#'         return(manual_import_success)
#'       })
#'     })
#'     
#'   }, error = function(e) {
#'     logger::log_error(paste0("Failed to create schema: ", e$message))
#'     return(FALSE)
#'   })
#' }
#' 
#' #' Manual chunked import as last resort
#' #' @noRd
#' manual_chunk_import <- function(connection, filepath, table_name, verbose) {
#'   logger::log_info("Using manual chunking as last resort")
#'   
#'   # Read header to get schema
#'   header_conn <- file(filepath, "r")
#'   header <- readLines(header_conn, n = 1)
#'   close(header_conn)
#'   
#'   # Create temporary file with just the header
#'   header_file <- tempfile(fileext = ".csv")
#'   writeLines(header, header_file)
#'   
#'   # Create table schema
#'   schema_query <- paste0(
#'     "CREATE TABLE IF NOT EXISTS ", table_name, " AS ",
#'     "SELECT * FROM read_csv_auto('", header_file, "', header=TRUE, all_varchar=TRUE) ",
#'     "WHERE 1=0"
#'   )
#'   
#'   DBI::dbExecute(connection, schema_query)
#'   
#'   # Get column names
#'   cols_query <- paste0("SELECT * FROM ", table_name, " LIMIT 0")
#'   cols_result <- DBI::dbGetQuery(connection, cols_query)
#'   col_names <- colnames(cols_result)
#'   
#'   # Use data.table fread for chunking
#'   chunk_size <- 50000
#'   total_rows <- 0
#'   
#'   # Use R file connection for reading efficiency
#'   con <- file(filepath, "r")
#'   readLines(con, n = 1)  # Skip header
#'   
#'   repeat {
#'     chunk_data <- readLines(con, n = chunk_size)
#'     
#'     if (length(chunk_data) == 0) {
#'       break  # End of file
#'     }
#'     
#'     # Create temp file for this chunk
#'     temp_file <- tempfile(fileext = ".csv")
#'     writeLines(c(header, chunk_data), temp_file)
#'     
#'     # Import chunk
#'     chunk_query <- paste0(
#'       "INSERT INTO ", table_name, " SELECT * FROM read_csv_auto('",
#'       temp_file, "', header=TRUE, all_varchar=TRUE, ignore_errors=TRUE)"
#'     )
#'     
#'     tryCatch({
#'       DBI::dbExecute(connection, chunk_query)
#'       total_rows <- total_rows + length(chunk_data)
#'       
#'       if (verbose) {
#'         logger::log_debug(paste0("Imported ", total_rows, " rows so far"))
#'       }
#'     }, error = function(e) {
#'       logger::log_warn(paste0("Error importing chunk: ", e$message))
#'     })
#'     
#'     # Clean up
#'     unlink(temp_file)
#'     rm(chunk_data)
#'     gc()
#'   }
#'   
#'   close(con)
#'   unlink(header_file)
#'   
#'   logger::log_success(paste0("Manual import completed with approximately ", total_rows, " rows"))
#'   return(total_rows > 0)
#' }
#' 
#' #' Create column mapping with memory optimization
#' #' @noRd
#' create_column_mapping <- function(connection, new_tables, existing_tables, selected_columns, verbose) {
#'   logger::log_info("Creating column mapping across years")
#'   
#'   column_mapping <- list()
#'   all_years <- unique(c(names(new_tables), names(existing_tables)))
#'   
#'   # Common column variations
#'   column_variations <- list(
#'     "Physician_Profile_ID" = c("Physician_Profile_ID", "Covered_Recipient_Profile_ID", "Physician_ID"),
#'     "Physician_First_Name" = c("Physician_First_Name", "Covered_Recipient_First_Name", "First_Name"),
#'     "Physician_Last_Name" = c("Physician_Last_Name", "Covered_Recipient_Last_Name", "Last_Name"),
#'     "Recipient_State" = c("Recipient_State", "State", "Physician_State")
#'   )
#'   
#'   # Check for cached mappings
#'   existing_mappings <- get_cached_mappings(connection, all_years)
#'   
#'   # Process each year
#'   for (year in all_years) {
#'     if (year %in% names(existing_mappings)) {
#'       column_mapping[[year]] <- existing_mappings[[year]]
#'       next
#'     }
#'     
#'     # Get table name
#'     table_name <- NULL
#'     if (year %in% names(new_tables)) {
#'       table_name <- new_tables[[year]]
#'     } else if (year %in% names(existing_tables)) {
#'       table_name <- existing_tables[[year]]
#'     }
#'     
#'     if (is.null(table_name)) {
#'       next
#'     }
#'     
#'     # Get available columns
#'     query <- paste0("SELECT * FROM ", table_name, " LIMIT 1")
#'     sample_data <- DBI::dbGetQuery(connection, query)
#'     available_columns <- colnames(sample_data)
#'     
#'     # Map columns
#'     year_mapping <- list()
#'     
#'     for (std_col in selected_columns) {
#'       # Direct match
#'       if (std_col %in% available_columns) {
#'         year_mapping[[std_col]] <- std_col
#'         next
#'       }
#'       
#'       # Try variations
#'       if (std_col %in% names(column_variations)) {
#'         variations <- column_variations[[std_col]]
#'         for (var in variations) {
#'           if (var %in% available_columns) {
#'             year_mapping[[std_col]] <- var
#'             break
#'           }
#'         }
#'       }
#'       
#'       # Case-insensitive match as fallback
#'       if (is.null(year_mapping[[std_col]])) {
#'         lower_std_col <- tolower(std_col)
#'         lower_available <- tolower(available_columns)
#'         match_idx <- which(lower_available == lower_std_col)
#'         if (length(match_idx) > 0) {
#'           year_mapping[[std_col]] <- available_columns[match_idx[1]]
#'         }
#'       }
#'     }
#'     
#'     column_mapping[[year]] <- year_mapping
#'     
#'     # Cache mapping
#'     cache_mapping(connection, year, year_mapping)
#'   }
#'   
#'   return(column_mapping)
#' }
#' 
#' #' Get cached column mappings
#' #' @noRd
#' get_cached_mappings <- function(connection, years) {
#'   if (length(years) == 0) {
#'     return(list())
#'   }
#'   
#'   years_str <- paste0("'", years, "'", collapse = ",")
#'   query <- paste0("SELECT year, standard_column, actual_column FROM op_column_mappings ",
#'                   "WHERE year IN (", years_str, ")")
#'   
#'   mappings <- tryCatch({
#'     DBI::dbGetQuery(connection, query)
#'   }, error = function(e) {
#'     return(data.frame())
#'   })
#'   
#'   if (nrow(mappings) == 0) {
#'     return(list())
#'   }
#'   
#'   # Convert to nested list
#'   result <- list()
#'   for (year in unique(mappings$year)) {
#'     year_mappings <- mappings[mappings$year == year, ]
#'     result[[year]] <- setNames(
#'       as.list(year_mappings$actual_column),
#'       year_mappings$standard_column
#'     )
#'   }
#'   
#'   return(result)
#' }
#' 
#' #' Cache column mapping
#' #' @noRd
#' cache_mapping <- function(connection, year, mapping) {
#'   # Delete existing mappings
#'   delete_query <- paste0("DELETE FROM op_column_mappings WHERE year = '", year, "'")
#'   DBI::dbExecute(connection, delete_query)
#'   
#'   # Insert new mappings in batches
#'   batch_size <- 10
#'   std_cols <- names(mapping)
#'   num_batches <- ceiling(length(std_cols) / batch_size)
#'   
#'   for (i in 1:num_batches) {
#'     start_idx <- (i-1) * batch_size + 1
#'     end_idx <- min(i * batch_size, length(std_cols))
#'     batch_cols <- std_cols[start_idx:end_idx]
#'     
#'     for (std_col in batch_cols) {
#'       actual_col <- mapping[[std_col]]
#'       if (!is.null(actual_col)) {
#'         insert_query <- paste0(
#'           "INSERT INTO op_column_mappings (year, standard_column, actual_column) ",
#'           "VALUES ('", year, "', '", std_col, "', '", actual_col, "')"
#'         )
#'         DBI::dbExecute(connection, insert_query)
#'       }
#'     }
#'   }
#' }
#' 
#' #' Merge payment data with chunking for memory efficiency
#' #' @noRd
#' merge_payment_data_chunked <- function(connection, new_tables, existing_tables,
#'                                        column_mapping, output_table_name, verbose) {
#'   logger::log_info(paste0("Merging data into table: ", output_table_name))
#'   
#'   # Check if output table exists
#'   tables <- DBI::dbListTables(connection)
#'   if (output_table_name %in% tables) {
#'     DBI::dbExecute(connection, paste0("DROP TABLE ", output_table_name))
#'   }
#'   
#'   # Get all years and columns
#'   all_years <- unique(c(names(new_tables), names(existing_tables)))
#'   all_std_cols <- unique(unlist(lapply(column_mapping, names)))
#'   
#'   # Create empty output table
#'   all_cols <- c(all_std_cols, "Source_Table")
#'   col_defs <- paste0('"', all_cols, '" VARCHAR', collapse = ", ")
#'   create_query <- paste0("CREATE TABLE ", output_table_name, " (", col_defs, ")")
#'   
#'   tryCatch({
#'     DBI::dbExecute(connection, create_query)
#'     
#'     # Process each year in chunks
#'     for (year in all_years) {
#'       table_name <- NULL
#'       if (year %in% names(new_tables)) {
#'         table_name <- new_tables[[year]]
#'       } else if (year %in% names(existing_tables)) {
#'         table_name <- existing_tables[[year]]
#'       }
#'       
#'       if (is.null(table_name) || is.null(column_mapping[[year]])) {
#'         next
#'       }
#'       
#'       # Get row count
#'       count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
#'       count_result <- DBI::dbGetQuery(connection, count_query)
#'       total_rows <- count_result$row_count[1]
#'       
#'       # Set chunk size
#'       chunk_size <- 100000
#'       num_chunks <- ceiling(total_rows / chunk_size)
#'       
#'       logger::log_info(paste0("Merging ", total_rows, " rows from year ", year,
#'                               " in ", num_chunks, " chunks"))
#'       
#'       # Process each chunk
#'       for (chunk in 1:num_chunks) {
#'         offset <- (chunk - 1) * chunk_size
#'         
#'         # Build mapping for INSERT
#'         mapping <- column_mapping[[year]]
#'         output_cols <- paste0('"', all_cols, '"', collapse = ", ")
#'         
#'         # Build select clause
#'         select_parts <- c()
#'         for (std_col in all_std_cols) {
#'           org_col <- NULL
#'           if (std_col %in% names(mapping)) {
#'             org_col <- mapping[[std_col]]
#'           }
#'           
#'           if (!is.null(org_col)) {
#'             select_parts <- c(select_parts, paste0('"', org_col, '" AS "', std_col, '"'))
#'           } else if (std_col == "Program_Year" &&
#'                      (!"Program_Year" %in% names(mapping) || is.null(mapping[["Program_Year"]]))) {
#'             select_parts <- c(select_parts, paste0("'", year, "' AS \"Program_Year\""))
#'           } else {
#'             select_parts <- c(select_parts, paste0('NULL AS "', std_col, '"'))
#'           }
#'         }
#'         
#'         # Add Source_Table
#'         select_parts <- c(select_parts, paste0("'", table_name, "' AS \"Source_Table\""))
#'         
#'         # Build INSERT query
#'         insert_query <- paste0(
#'           "INSERT INTO ", output_table_name, " (", output_cols, ") ",
#'           "SELECT ", paste(select_parts, collapse = ", "),
#'           " FROM ", table_name,
#'           " LIMIT ", chunk_size, " OFFSET ", offset
#'         )
#'         
#'         # Execute INSERT
#'         tryCatch({
#'           DBI::dbExecute(connection, insert_query)
#'           if (verbose || chunk %% 10 == 0 || chunk == num_chunks) {
#'             logger::log_debug(paste0("Merged chunk ", chunk, "/", num_chunks,
#'                                      " for year ", year))
#'           }
#'         }, error = function(e) {
#'           logger::log_error(paste0("Error merging chunk ", chunk, ": ", e$message))
#'         })
#'         
#'         # Free memory
#'         gc()
#'       }
#'     }
#'     
#'     # Get total row count
#'     count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", output_table_name)
#'     count_result <- DBI::dbGetQuery(connection, count_query)
#'     total_rows <- count_result$row_count[1]
#'     
#'     logger::log_success(paste0("Successfully merged ", total_rows, " rows into ",
#'                                output_table_name))
#'     return(output_table_name)
#'     
#'   }, error = function(e) {
#'     logger::log_error(paste0("Failed to merge data: ", e$message))
#'     return(NULL)
#'   })
#' }
#' 
#' #' Export to CSV with chunking for memory efficiency
#' #' @noRd
#' export_to_csv_chunked <- function(connection, table_name, output_path, verbose) {
#'   logger::log_info(paste0("Exporting to CSV: ", output_path))
#'   
#'   # Ensure directory exists
#'   output_dir <- dirname(output_path)
#'   if (!dir.exists(output_dir)) {
#'     dir.create(output_dir, recursive = TRUE)
#'   }
#'   
#'   # Try DuckDB's COPY TO first (most efficient)
#'   tryCatch({
#'     export_query <- paste0(
#'       "COPY (SELECT * FROM ", table_name, ") TO '", output_path,
#'       "' (DELIMITER ',', HEADER, FORMAT CSV)"
#'     )
#'     
#'     DBI::dbExecute(connection, export_query)
#'     logger::log_success(paste0("Successfully exported to ", output_path))
#'     return(TRUE)
#'     
#'   }, error = function(e) {
#'     logger::log_warn(paste0("Direct export failed: ", e$message,
#'                             ". Switching to chunked export."))
#'     
#'     # Get row count
#'     count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
#'     count_result <- DBI::dbGetQuery(connection, count_query)
#'     total_rows <- count_result$row_count[1]
#'     
#'     # Set chunk size
#'     chunk_size <- 100000
#'     num_chunks <- ceiling(total_rows / chunk_size)
#'     
#'     logger::log_info(paste0("Exporting ", total_rows, " rows in ", num_chunks, " chunks"))
#'     
#'     # Get column names
#'     cols_query <- paste0("SELECT * FROM ", table_name, " LIMIT 1")
#'     cols_result <- DBI::dbGetQuery(connection, cols_query)
#'     column_names <- colnames(cols_result)
#'     
#'     # Open file connection
#'     file_conn <- file(output_path, "w")
#'     
#'     # Write header
#'     write.table(data.frame(t(column_names)), file = file_conn,
#'                 row.names = FALSE, col.names = FALSE, sep = ",", quote = TRUE)
#'     
#'     # Process each chunk
#'     for (chunk in 1:num_chunks) {
#'       offset <- (chunk - 1) * chunk_size
#'       
#'       query <- paste0(
#'         "SELECT * FROM ", table_name,
#'         " LIMIT ", chunk_size, " OFFSET ", offset
#'       )
#'       
#'       # Get chunk data
#'       chunk_data <- DBI::dbGetQuery(connection, query)
#'       
#'       # Write to file (without header)
#'       write.table(chunk_data, file = file_conn, row.names = FALSE,
#'                   col.names = FALSE, sep = ",", quote = TRUE, append = TRUE)
#'       
#'       if (verbose || chunk %% 10 == 0 || chunk == num_chunks) {
#'         progress <- min((chunk * chunk_size) / total_rows * 100, 100)
#'         logger::log_debug(paste0("Exported chunk ", chunk, "/", num_chunks,
#'                                  " (", round(progress, 1), "%)"))
#'       }
#'       
#'       # Free memory
#'       rm(chunk_data)
#'       gc()
#'     }
#'     
#'     # Close file
#'     close(file_conn)
#'     
#'     logger::log_success(paste0("Successfully exported ", total_rows,
#'                                " rows to ", output_path))
#'     return(TRUE)
#'   })
#' }
#' 
#' #' Get key columns based on payment type
#' #' @noRd
#' get_key_columns <- function(payment_type) {
#'   if (payment_type == "general") {
#'     return(c(
#'       "Covered_Recipient_Type",
#'       "Teaching_Hospital_Name",
#'       "Covered_Recipient_NPI",
#'       "Covered_Recipient_Profile_ID",
#'       "Covered_Recipient_First_Name",
#'       "Covered_Recipient_Middle_Name",
#'       "Covered_Recipient_Last_Name",
#'       "Covered_Recipient_Name_Suffix",
#'       "Recipient_Primary_Business_Street_Address_Line1",
#'       "Recipient_Primary_Business_Street_Address_Line2",
#'       "Recipient_City",
#'       "Recipient_State",
#'       "Recipient_Zip_Code",
#'       "Recipient_Country",
#'       "Covered_Recipient_Primary_Type_1",
#'       "Covered_Recipient_Specialty_1",
#'       "Covered_Recipient_License_State_code1",
#'       "Covered_Recipient_License_State_code2",
#'       "Covered_Recipient_License_State_code3",
#'       "Covered_Recipient_License_State_code4",
#'       "Covered_Recipient_License_State_code5",
#'       "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
#'       "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
#'       "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
#'       "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
#'       "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
#'       "Form_of_Payment_or_Transfer_of_Value",
#'       "Nature_of_Payment_or_Transfer_of_Value",
#'       "City_of_Travel",
#'       "State_of_Travel",
#'       "Country_of_Travel",
#'       "Physician_Ownership_Indicator",
#'       "Third_Party_Payment_Recipient_Indicator",
#'       "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value",
#'       "Charity_Indicator",
#'       "Third_Party_Equals_Covered_Recipient_Indicator",
#'       "Contextual_Information",
#'       "Delay_in_Publication_Indicator",
#'       "Related_Product_Indicator",
#'       "Covered_or_Noncovered_Indicator_1",
#'       "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
#'       "Product_Category_or_Therapeutic_Area_1",
#'       "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
#'       "Covered_or_Noncovered_Indicator_2",
#'       "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2",
#'       "Product_Category_or_Therapeutic_Area_2",
#'       "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
#'       "Covered_or_Noncovered_Indicator_3",
#'       "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3",
#'       "Product_Category_or_Therapeutic_Area_3",
#'       "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
#'       "Covered_or_Noncovered_Indicator_4",
#'       "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4",
#'       "Product_Category_or_Therapeutic_Area_4",
#'       "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
#'       "Covered_or_Noncovered_Indicator_5",
#'       "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5",
#'       "Product_Category_or_Therapeutic_Area_5",
#'       "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
#'       "Program_Year",
#'       "Payment_Publication_Date",
#'       "Total_Amount_of_Payment_USDollars",
#'       "Number_of_Payments_Included_in_Total_Amount",
#'       "Date_of_Payment",
#'       "Record_ID"
#'     ))
#'   } else if (payment_type == "research") {
#'     # Add all documented columns for research payments here similarly
#'   } else if (payment_type == "ownership") {
#'     # Add all documented columns for ownership payments here similarly
#'   } else {
#'     stop("Invalid payment_type provided to get_key_columns")
#'   }
#' }
#' 
#' # ------------------------------------------------------------------------------
#' # Process Open Payments Data for All Available Years (General Payment Type)
#' # ------------------------------------------------------------------------------
#' 
#' # This call runs the optimized function to:
#' # - Search the `base_dir` directory recursively for Open Payments general payment files
#' # - Import and process those files into the DuckDB database at `db_path`
#' # - Merge tables across years (automatically inferring the year from file names)
#' # - Avoid re-importing files already present in the database unless forced
#' # - Log detailed information about progress and operations
#' # ------------------------------------------------------------------------------
#' 
#' base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
#' db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"
#' 
#' result <- process_open_payments_data_optimized(
#'   base_dir = base_dir,             # Path to folder containing Open Payments CSV files
#'   db_path = db_path,               # Path to DuckDB database file
#'   years = NULL,                    # NULL = automatically detect all years in the files
#'   payment_type = "general",        # Choose "general" payments dataset (vs "research" or "ownership")
#'   output_table_name = "open_payments_merged",  # Final merged DuckDB table name
#'   verbose = TRUE,                  # Enable detailed logging to the console
#'   force_reimport = FALSE           # Avoid re-importing files that were already loaded
#' )
#' 
#' 
#' 
#' # ------------------------------------------------------------------------------
#' # View Contents of the Merged Open Payments Table in DuckDB
#' # ------------------------------------------------------------------------------
#' 
#' # Load required packages for database connection and data manipulation
#' library(DBI)       # For database operations
#' library(duckdb)    # To interface with DuckDB
#' library(dplyr)     # Optional, useful if manipulating the data later
#' 
#' # Connect to the DuckDB database file that stores the merged Open Payments data
#' con <- DBI::dbConnect(
#'   duckdb::duckdb(),
#'   dbdir = "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb",
#'   read_only = FALSE  # Set to FALSE to allow data modifications (TRUE for read-only)
#' )
#' 
#' # List all tables in the database to verify presence of 'open_payments_merged'
#' DBI::dbListTables(con)
#' 
#' # Run a SQL query to preview the first 100 rows of the merged Open Payments table
#' open_payments_preview <- DBI::dbGetQuery(
#'   con,
#'   "SELECT * FROM open_payments_merged LIMIT 100"
#' )
#' 
#' # Print the preview to the console
#' print(open_payments_preview)
#' 
#' # Cleanly disconnect from the DuckDB database and shut it down
#' dbDisconnect(con, shutdown = TRUE)


## zz old simpler function
#' #' Process Open Payments Data with Optimized Database Operations
#' #'
#' #' @description
#' #' Processes Open Payments data files and loads them into a DuckDB database with
#' #' optimized performance. The function handles file discovery, metadata tracking,
#' #' incremental loading, and column mapping across different years of Open Payments data.
#' #'
#' #' @param base_dir Character string specifying the base directory containing Open Payments files.
#' #' @param db_path Character string specifying the path to the DuckDB database file.
#' #' @param years Character vector of years to process (e.g., c("2019", "2020", "2021")).
#' #' @param payment_type Character string specifying the payment type: "general", "research", or "ownership".
#' #' @param output_table_name Character string specifying the name for the output table.
#' #' @param force_reimport Logical, if TRUE will reimport all files even if they exist in the database.
#' #' @param file_pattern Character string with the file pattern to match. Default adapts to payment_type.
#' #' @param verbose Logical, if TRUE (default) will log detailed information.
#' #'
#' #' @return Character string with the name of the created merged table.
#' #'
#' #' @importFrom logger log_info log_success log_error log_warn log_debug log_threshold INFO ERROR
#' #' @importFrom assertthat assert_that
#' #' @importFrom dplyr filter mutate select distinct
#' #' @importFrom duckdb duckdb dbConnect
#' #' @importFrom DBI dbExecute dbGetQuery dbListTables dbDisconnect
#' #' @importFrom purrr map
#' #' @importFrom fs file_size dir_ls
#' #'
#' #' @examples
#' #' # Basic usage with default settings for general payments
#' #' merged_table <- process_open_payments_data_optimized(
#' #'   base_dir = "/path/to/open_payments_data",
#' #'   db_path = "/path/to/output.duckdb",
#' #'   years = c("2021", "2022", "2023"),
#' #'   payment_type = "general",
#' #'   output_table_name = "op_general_payments",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Process research payments with forced reimport
#' #' research_table <- process_open_payments_data_optimized(
#' #'   base_dir = "/path/to/open_payments_data",
#' #'   db_path = "/path/to/output.duckdb",
#' #'   years = c("2021", "2022"),
#' #'   payment_type = "research",
#' #'   output_table_name = "op_research_payments",
#' #'   force_reimport = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Process ownership data with custom file pattern and limited logging
#' #' ownership_table <- process_open_payments_data_optimized(
#' #'   base_dir = "/path/to/open_payments_data",
#' #'   db_path = "/path/to/output.duckdb",
#' #'   years = c("2020", "2021", "2022"),
#' #'   payment_type = "ownership",
#' #'   output_table_name = "op_ownership_interests",
#' #'   file_pattern = ".*OWNRSHP.*\\.csv$",
#' #'   force_reimport = FALSE,
#' #'   verbose = FALSE
#' #' )
#' process_open_payments_data_optimized <- function(base_dir = "/Volumes/Video Projects Muffly 1/open_payments_data",
#'                                                  db_path = "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb",
#'                                                  years = NULL,
#'                                                  payment_type = "general",
#'                                                  output_table_name = "open_payments_merged",
#'                                                  force_reimport = FALSE,
#'                                                  file_pattern = NULL,
#'                                                  verbose = TRUE) {
#'   # Initialize logger
#'   logger::log_threshold(ifelse(verbose, logger::INFO, logger::ERROR))
#'   
#'   # Log function start
#'   logger::log_info("Starting Open Payments data processing")
#'   logger::log_debug("Validating input parameters")
#'   
#'   # Validate input parameters
#'   assertthat::assert_that(is.character(base_dir))
#'   assertthat::assert_that(is.character(db_path))
#'   assertthat::assert_that(is.character(payment_type))
#'   assertthat::assert_that(payment_type %in% PAYMENT_TYPES)
#'   assertthat::assert_that(is.logical(force_reimport))
#'   assertthat::assert_that(is.logical(verbose))
#'   
#'   if (!is.null(years)) {
#'     assertthat::assert_that(is.character(years))
#'   }
#'   
#'   if (is.null(output_table_name)) {
#'     # Generate default output table name based on payment type
#'     output_table_name <- paste0("op_", payment_type, "_all_years")
#'     logger::log_info("Using default output table name: {output_table_name}")
#'   } else {
#'     assertthat::assert_that(is.character(output_table_name))
#'   }
#'   
#'   # Determine file pattern based on payment type if not provided
#'   if (is.null(file_pattern)) {
#'     if (payment_type == "general") {
#'       file_pattern <- ".*_GNRL.*\\.csv$"
#'     } else if (payment_type == "research") {
#'       file_pattern <- ".*_RSRCH.*\\.csv$"
#'     } else if (payment_type == "ownership") {
#'       file_pattern <- ".*_OWNRSHP.*\\.csv$"
#'     }
#'     logger::log_info("Using file pattern: {file_pattern}")
#'   }
#'   
#'   # Connect to DuckDB
#'   logger::log_info("Connecting to DuckDB at {db_path}")
#'   connection <- connect_to_duckdb(db_path, verbose)
#'   
#'   # Initialize database if necessary
#'   initialize_database(connection, verbose)
#'   
#'   # Find Open Payments files
#'   payment_files <- find_payment_files(base_dir, years, file_pattern, verbose)
#'   
#'   # Check which files are already imported
#'   payment_files <- check_imported_files(connection, payment_files, verbose)
#'   
#'   # Import files to DuckDB
#'   imported_tables <- import_files_to_duckdb(connection, payment_files, verbose,
#'                                             force_reimport, file_pattern)
#'   
#'   # Determine which columns to include in the merged data
#'   key_columns <- get_key_columns(payment_type)
#'   
#'   # Create column mapping across years
#'   column_mapping <- create_column_mapping(connection,
#'                                           imported_tables$new_tables,
#'                                           imported_tables$existing_tables,
#'                                           key_columns,
#'                                           verbose)
#'   
#'   # Merge payment data from all years
#'   merged_table <- merge_payment_data(connection,
#'                                      imported_tables$new_tables,
#'                                      imported_tables$existing_tables,
#'                                      column_mapping,
#'                                      output_table_name,
#'                                      verbose)
#'   
#'   # Check for any missing tables
#'   table_names <- c(unlist(imported_tables$new_tables),
#'                    unlist(imported_tables$existing_tables),
#'                    merged_table)
#'   table_names <- table_names[!is.na(table_names)]
#'   
#'   # Verify tables exist
#'   existing_tables <- DBI::dbListTables(connection)
#'   missing_tables <- table_names[!table_names %in% existing_tables]
#'   
#'   # Report results
#'   if (length(missing_tables) == 0) {
#'     logger::log_info("✅ All {length(table_names)} tables were successfully created and are available.")
#'   } else {
#'     logger::log_error("❌ Missing tables detected: {paste(missing_tables, collapse = ', ')}")
#'     DBI::dbDisconnect(connection)
#'     stop("Some tables were not created successfully. Review the errors above.")
#'   }
#'   
#'   # Disconnect from database
#'   logger::log_info("Disconnecting from database")
#'   DBI::dbDisconnect(connection)
#'   
#'   # Return the merged table name
#'   return(merged_table)
#' }
#' 
#' #' Connect to DuckDB
#' #'
#' #' @param db_path Path to DuckDB database file
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @return DuckDB connection
#' #' @noRd
#' connect_to_duckdb <- function(db_path, verbose) {
#'   tryCatch({
#'     connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
#'     logger::log_success("Connected to DuckDB database at {db_path}")
#'     return(connection)
#'   }, error = function(e) {
#'     logger::log_error("Failed to connect to DuckDB: {e$message}")
#'     stop("Database connection failed")
#'   })
#' }
#' 
#' #' Initialize database tables and schema
#' #'
#' #' @param connection DuckDB connection
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @noRd
#' initialize_database <- function(connection, verbose) {
#'   logger::log_info("Initializing database tables and schema")
#'   
#'   # Check if metadata table exists
#'   tables <- DBI::dbListTables(connection)
#'   
#'   if (!"op_file_metadata" %in% tables) {
#'     logger::log_info("Creating file metadata table")
#'     
#'     create_metadata_table <- "
#'       CREATE TABLE op_file_metadata (
#'         filepath VARCHAR PRIMARY KEY,
#'         year VARCHAR,
#'         table_name VARCHAR,
#'         file_size_bytes BIGINT,
#'         file_hash VARCHAR,
#'         import_date TIMESTAMP,
#'         file_pattern VARCHAR
#'       )
#'     "
#'     
#'     DBI::dbExecute(connection, create_metadata_table)
#'     logger::log_success("Created file metadata table")
#'   }
#'   
#'   if (!"op_column_mappings" %in% tables) {
#'     logger::log_info("Creating column mappings table")
#'     
#'     create_mappings_table <- "
#'       CREATE TABLE op_column_mappings (
#'         year VARCHAR,
#'         standard_column VARCHAR,
#'         actual_column VARCHAR,
#'         PRIMARY KEY (year, standard_column)
#'       )
#'     "
#'     
#'     DBI::dbExecute(connection, create_mappings_table)
#'     logger::log_success("Created column mappings table")
#'   }
#' }
#' 
#' #' Find Open Payments files
#' #'
#' #' @param base_dir Base directory containing Open Payments files
#' #' @param years Years to find files for
#' #' @param file_pattern File pattern to match
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @return Tibble of payment files
#' #' @noRd
#' find_payment_files <- function(base_dir, years, file_pattern, verbose) {
#'   logger::log_info("Finding Open Payments files in {base_dir}")
#'   
#'   # Find all files matching the pattern in the base directory and subdirectories
#'   all_files <- tryCatch({
#'     fs::dir_ls(base_dir, recurse = TRUE, regexp = file_pattern)
#'   }, error = function(e) {
#'     logger::log_error("Error finding files: {e$message}")
#'     return(character(0))
#'   })
#'   
#'   # If no files found, return empty tibble
#'   if (length(all_files) == 0) {
#'     logger::log_warn("No files found matching pattern: {file_pattern}")
#'     return(tibble::tibble(
#'       filepath = character(0),
#'       year = character(0),
#'       filesize_mb = numeric(0)
#'     ))
#'   }
#'   
#'   # Create a data frame with file information
#'   payment_files <- tibble::tibble(
#'     filepath = all_files,
#'     year = NA_character_,
#'     filesize_mb = as.numeric(fs::file_size(all_files)) / (1024 * 1024)
#'   )
#'   
#'   # Extract year from file paths
#'   # Common patterns in Open Payments files:
#'   # - PGYR2020_P062022 (Program Year 2020)
#'   # - RY2019 (Reporting Year 2019)
#'   # - _CY2018_ (Calendar Year 2018)
#'   
#'   # Try to extract years from filenames
#'   for (i in 1:nrow(payment_files)) {
#'     file_path <- payment_files$filepath[i]
#'     file_name <- basename(file_path)
#'     
#'     # Try different patterns
#'     year_match <- regmatches(file_name, regexpr("PGYR(20[0-9]{2})", file_name))
#'     if (length(year_match) == 0) {
#'       year_match <- regmatches(file_name, regexpr("RY(20[0-9]{2})", file_name))
#'     }
#'     if (length(year_match) == 0) {
#'       year_match <- regmatches(file_name, regexpr("CY(20[0-9]{2})", file_name))
#'     }
#'     if (length(year_match) == 0) {
#'       year_match <- regmatches(file_name, regexpr("(20[0-9]{2})", file_name))
#'     }
#'     
#'     if (length(year_match) > 0) {
#'       # Extract the year part
#'       year <- gsub("[^0-9]", "", year_match[1])
#'       payment_files$year[i] <- year
#'     }
#'   }
#'   
#'   # Filter by specified years if provided
#'   if (!is.null(years) && length(years) > 0) {
#'     payment_files <- dplyr::filter(payment_files, year %in% years)
#'     
#'     if (nrow(payment_files) > 0) {
#'       logger::log_info("Found {nrow(payment_files)} files for years: {paste(years, collapse = ', ')}")
#'     } else {
#'       logger::log_warn("No files found for specified years")
#'     }
#'   }
#'   
#'   return(payment_files)
#' }
#' 
#' #' Read specific NPI table
#' #'
#' #' @description
#' #' Reads and processes an NPI data table from a CSV file with comprehensive error handling.
#' #'
#' #' @param file_path Character string specifying the path to the CSV file.
#' #' @param column_types Optional list of column types for readr. If NULL, will be guessed.
#' #' @param verbose Logical, if TRUE (default) will log detailed information.
#' #'
#' #' @return A tibble containing the processed NPI data.
#' #'
#' #' @importFrom readr read_csv cols
#' #' @importFrom dplyr select mutate filter distinct
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_error log_debug log_warn
#' #'
#' #' @examples
#' #' # Basic usage with default parameters
#' #' npi_data <- read_npi_table(
#' #'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#' #'   verbose = TRUE
#' #' )
#' #' print(head(npi_data))
#' #'
#' #' # Specifying column types explicitly
#' #' npi_data <- read_npi_table(
#' #'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#' #'   column_types = readr::cols(
#' #'     NPI = readr::col_character(),
#' #'     Provider_Name = readr::col_character(),
#' #'     Score = readr::col_double()
#' #'   ),
#' #'   verbose = TRUE
#' #' )
#' #' print(nrow(npi_data))
#' #'
#' #' # Silent mode with minimal logging
#' #' npi_data <- read_npi_table(
#' #'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#' #'   verbose = FALSE
#' #' )
#' #' str(npi_data)
#' read_npi_table <- function(file_path, column_types = NULL, verbose = TRUE) {
#'   # Initialize logger
#'   logger::log_threshold(ifelse(verbose, logger::INFO, logger::ERROR))
#'   
#'   # Validate inputs
#'   assertthat::assert_that(is.character(file_path))
#'   assertthat::assert_that(length(file_path) == 1)
#'   assertthat::assert_that(is.logical(verbose))
#'   
#'   # Log function start
#'   logger::log_info("Starting to read NPI table from: {file_path}")
#'   
#'   # Check if file exists
#'   if (!file.exists(file_path)) {
#'     logger::log_error("File does not exist: {file_path}")
#'     stop("File not found: ", file_path)
#'   }
#'   
#'   # Attempt to read the file
#'   npi_table <- tryCatch({
#'     logger::log_debug("Attempting to read CSV file")
#'     
#'     # Try different reading methods
#'     try_reading_methods(file_path, column_types, verbose)
#'     
#'   }, error = function(e) {
#'     logger::log_error("All reading methods failed: {conditionMessage(e)}")
#'     stop("Failed to read NPI table: ", conditionMessage(e))
#'   })
#'   
#'   # Log success and return
#'   logger::log_info("Successfully processed NPI table with {nrow(npi_table)} rows and {ncol(npi_table)} columns")
#'   return(npi_table)
#' }
#' 
#' #' Try different methods to read the CSV file
#' #'
#' #' @param file_path Path to the CSV file
#' #' @param column_types Column types specification
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @return Processed tibble
#' #' @noRd
#' try_reading_methods <- function(file_path, column_types, verbose) {
#'   # Method 1: Standard readr::read_csv
#'   npi_data <- tryCatch({
#'     if (verbose) logger::log_debug("Trying standard read_csv method")
#'     
#'     readr_table <- readr::read_csv(
#'       file = file_path,
#'       col_types = column_types,
#'       show_col_types = FALSE
#'     )
#'     
#'     validate_npi_data(readr_table, verbose)
#'     process_npi_data(readr_table, verbose)
#'   }, error = function(e) {
#'     if (verbose) logger::log_warn("Standard read_csv failed: {conditionMessage(e)}")
#'     NULL
#'   })
#'   
#'   if (!is.null(npi_data)) return(npi_data)
#'   
#'   # Method 2: Try with readr but different options
#'   npi_data <- tryCatch({
#'     if (verbose) logger::log_debug("Trying read_csv with different options")
#'     
#'     readr_table <- readr::read_csv(
#'       file = file_path,
#'       col_types = column_types,
#'       show_col_types = FALSE,
#'       lazy = FALSE,
#'       progress = FALSE,
#'       guess_max = 10000
#'     )
#'     
#'     validate_npi_data(readr_table, verbose)
#'     process_npi_data(readr_table, verbose)
#'   }, error = function(e) {
#'     if (verbose) logger::log_warn("Alternative read_csv failed: {conditionMessage(e)}")
#'     NULL
#'   })
#'   
#'   if (!is.null(npi_data)) return(npi_data)
#'   
#'   # Method 3: Try with base R read.csv
#'   npi_data <- tryCatch({
#'     if (verbose) logger::log_debug("Trying base R read.csv")
#'     
#'     base_table <- read.csv(
#'       file = file_path,
#'       stringsAsFactors = FALSE,
#'       check.names = FALSE
#'     )
#'     
#'     # Convert to tibble
#'     base_tibble <- tibble::as_tibble(base_table)
#'     
#'     validate_npi_data(base_tibble, verbose)
#'     process_npi_data(base_tibble, verbose)
#'   }, error = function(e) {
#'     if (verbose) logger::log_warn("Base R read.csv failed: {conditionMessage(e)}")
#'     NULL
#'   })
#'   
#'   if (!is.null(npi_data)) return(npi_data)
#'   
#'   # Method 4: Last resort with data.table
#'   if (requireNamespace("data.table", quietly = TRUE)) {
#'     npi_data <- tryCatch({
#'       if (verbose) logger::log_debug("Trying data.table fread")
#'       
#'       dt_table <- data.table::fread(
#'         file = file_path,
#'         data.table = FALSE,
#'         showProgress = FALSE
#'       )
#'       
#'       # Convert to tibble
#'       dt_tibble <- tibble::as_tibble(dt_table)
#'       
#'       validate_npi_data(dt_tibble, verbose)
#'       process_npi_data(dt_tibble, verbose)
#'     }, error = function(e) {
#'       if (verbose) logger::log_error("data.table fread failed: {conditionMessage(e)}")
#'       NULL
#'     })
#'     
#'     if (!is.null(npi_data)) return(npi_data)
#'   }
#'   
#'   # If all methods failed, throw an error
#'   stop("All reading methods failed for file: ", file_path)
#' }
#' 
#' #' Validate NPI data structure
#' #'
#' #' @param npi_data NPI data to validate
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @return Validated data
#' #' @noRd
#' validate_npi_data <- function(npi_data, verbose) {
#'   if (verbose) logger::log_debug("Validating NPI data structure")
#'   
#'   # Check if data is a data frame
#'   assertthat::assert_that(is.data.frame(npi_data))
#'   
#'   # Check if the data has any rows
#'   if (nrow(npi_data) == 0) {
#'     logger::log_warn("NPI data has 0 rows")
#'   } else {
#'     logger::log_debug("NPI data has {nrow(npi_data)} rows")
#'   }
#'   
#'   # Check expected columns
#'   expected_columns <- c("NPI")
#'   missing_columns <- expected_columns[!expected_columns %in% names(npi_data)]
#'   
#'   if (length(missing_columns) > 0) {
#'     logger::log_warn("Missing expected columns: {paste(missing_columns, collapse=', ')}")
#'   } else if (verbose) {
#'     logger::log_debug("All expected columns present")
#'   }
#'   
#'   return(npi_data)
#' }
#' 
#' 
#' 
#' 
#' #' Process NPI data
#' #'
#' #' @param npi_data NPI data to process
#' #' @param verbose Whether to print verbose output
#' #'
#' #' @return Processed data
#' #' @noRd
#' process_npi_data <- function(npi_data, verbose) {
#'   if (verbose) logger::log_info("Processing NPI data")
#'   
#'   # Handle any transformations for the data
#'   processed_table <- npi_data
#'   
#'   # Example transformations (modify as needed for your actual data)
#'   if ("NPI" %in% names(processed_table)) {
#'     if (verbose) logger::log_debug("Ensuring NPI column is character type")
#'     processed_table <- dplyr::mutate(processed_table, NPI = as.character(NPI))
#'   }
#'   
#'   # Remove any duplicate rows
#'   original_rows <- nrow(processed_table)
#'   processed_table <- dplyr::distinct(processed_table)
#'   if (nrow(processed_table) < original_rows && verbose) {
#'     logger::log_info("Removed {original_rows - nrow(processed_table)} duplicate rows")
#'   }
#'   
#'   # Log completion
#'   if (verbose) logger::log_debug("Data processing complete")
#'   
#'   return(processed_table)
#' }


#### Next Try 1933 hours ####
#' Process Open Payments Data
#'
#' @description
#' Imports, processes, and merges Open Payments data from CSV files into a DuckDB database.
#' Handles CSV parsing issues and ensures NPI data is properly preserved.
#'
#' @param base_dir Character string specifying the directory containing Open Payments CSV files.
#' @param db_path Character string specifying the path for the DuckDB database file.
#' @param years Numeric vector of years to process (e.g., c(2020, 2021)), or NULL to process all available years.
#' @param payment_type Character string specifying the payment type to process: "general", "research", or "ownership".
#' @param output_table_name Character string specifying the name for the final merged output table.
#' @param verbose Logical indicating whether to display detailed logging information.
#' @param force_reimport Logical indicating whether to reimport files even if they've been previously imported.
#' @param preserve_all_columns Logical indicating whether to preserve all columns from the original files.
#' @param max_rows Numeric value specifying the maximum number of rows to import from each file (NULL for all rows).
#'
#' @return A list containing database connection information and summary statistics.
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter select mutate rename inner_join left_join bind_rows
#' @importFrom stringr str_detect str_extract str_replace str_to_lower
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbExistsTable dbListFields dbListTables dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom logger log_info log_debug log_success log_error log_warn
#'
#' @examples
#' # Example 1: Process all general payments with default settings
#' payments_data <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/output/database.duckdb",
#'   payment_type = "general",
#'   output_table_name = "open_payments_merged",
#'   verbose = TRUE,
#'   force_reimport = FALSE,
#'   preserve_all_columns = TRUE,
#'   max_rows = NULL
#' )
#' # The function creates a merged table containing all General Payments data
#' # with NPI numbers properly preserved and all columns from source files
#'
#' # Example 2: Process specific years of research payments with row limitation
#' research_payments <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/output/database.duckdb",
#'   years = c(2020, 2021, 2022),
#'   payment_type = "research",
#'   output_table_name = "research_payments_recent",
#'   verbose = TRUE,
#'   force_reimport = TRUE,
#'   preserve_all_columns = FALSE,
#'   max_rows = 1000
#' )
#' # The function processes only years 2020-2022 research payments data
#' # and imports a maximum of 1000 rows per file for testing purposes
#'
#' # Example 3: Process ownership payments with minimal output
#' ownership_data <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "ownership_payments.duckdb",
#'   payment_type = "ownership",
#'   output_table_name = "ownership_payments",
#'   verbose = FALSE,
#'   force_reimport = FALSE,
#'   preserve_all_columns = TRUE,
#'   max_rows = NULL
#' )
#' # The function silently processes all ownership payments data
#' # with all columns preserved and no row limitations
#'
#' @export
process_open_payments_data <- function(base_dir,
                                       db_path,
                                       years = NULL,
                                       payment_type = "general",
                                       output_table_name = "open_payments_merged",
                                       verbose = TRUE,
                                       force_reimport = FALSE,
                                       preserve_all_columns = TRUE,
                                       max_rows = NULL) {
  
  # Setup logger based on verbose parameter
  setup_open_payments_logger(verbose)
  
  # Validate input parameters
  logger::log_debug("Validating input parameters")
  validate_open_payments_inputs(
    base_dir, 
    db_path, 
    years, 
    payment_type, 
    output_table_name, 
    verbose,
    force_reimport, 
    preserve_all_columns, 
    max_rows
  )
  logger::log_debug("Input validation completed")
  
  # Create file pattern based on payment type
  file_patterns <- list(
    general = "OP_DTL_GNRL_PGYR\\d{4}.*\\.csv$",
    research = "OP_DTL_RSRCH_PGYR\\d{4}.*\\.csv$",
    ownership = "OP_DTL_OWNRSHP_PGYR\\d{4}.*\\.csv$"
  )
  
  # Ensure payment_type is valid and get the corresponding pattern
  assertthat::assert_that(
    payment_type %in% names(file_patterns),
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  file_pattern <- file_patterns[[payment_type]]
  
  # Start processing with info message
  logger::log_info("Starting Open Payments data processing")
  
  # Log max rows limit if set
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Limiting import to %d rows per file", max_rows))
  }
  
  # Log column preservation setting
  if (preserve_all_columns) {
    logger::log_info("Preserving all original columns from source files")
  }
  
  # Establish database connection
  payments_conn <- connect_to_duckdb(db_path)
  
  # Initialize metadata tables if they don't exist
  initialize_metadata_tables(payments_conn)
  
  # Find CSV files matching the pattern
  logger::log_info(sprintf("Searching for files matching: %s", file_pattern))
  csv_files <- find_open_payments_files(base_dir, file_pattern)
  
  # Process each file
  processed_files <- import_open_payments_files(
    csv_files = csv_files,
    conn = payments_conn,
    years = years,
    force_reimport = force_reimport,
    max_rows = max_rows
  )
  
  # Create a single unified table schema from all the imported tables
  unified_schema <- create_unified_schema(
    conn = payments_conn,
    years = processed_files$years,
    preserve_all_columns = preserve_all_columns
  )
  
  # Create final table with the unified schema
  create_final_table(
    conn = payments_conn,
    table_name = output_table_name,
    schema = unified_schema,
    drop_existing = TRUE
  )
  
  # Merge data from all years into the final table
  merged_data <- merge_open_payments_data(
    conn = payments_conn,
    years = processed_files$years,
    output_table_name = output_table_name,
    unified_schema = unified_schema
  )
  
  # Check for NPI data in the final table
  check_npi_data(payments_conn, output_table_name)
  
  # Log success message
  logger::log_success("Open Payments data processing completed successfully")
  
  # Return a list with connection and summary information
  return(list(
    connection = payments_conn,
    output_table = output_table_name,
    years_processed = processed_files$years,
    files_processed = processed_files$files,
    rows_imported = merged_data$total_rows,
    columns_mapped = length(unified_schema$columns)
  ))
}

#' @noRd
setup_open_payments_logger <- function(verbose) {
  # Configure logger based on verbose setting
  if (verbose) {
    logger::log_threshold(logger::INFO)
  } else {
    logger::log_threshold(logger::SUCCESS)
  }
  
  # Set log layout with timestamp
  logger::log_layout(logger::layout_glue_generator(
    format = "{level} [{time}] {msg}"
  ))
}

#' @noRd
validate_open_payments_inputs <- function(base_dir, 
                                          db_path, 
                                          years, 
                                          payment_type, 
                                          output_table_name, 
                                          verbose,
                                          force_reimport, 
                                          preserve_all_columns, 
                                          max_rows) {
  # Validate base_dir
  assertthat::assert_that(
    is.character(base_dir),
    dir.exists(base_dir),
    msg = "base_dir must be a valid directory path"
  )
  
  # Validate db_path
  assertthat::assert_that(
    is.character(db_path),
    msg = "db_path must be a character string"
  )
  
  # Validate years if provided
  if (!is.null(years)) {
    assertthat::assert_that(
      is.numeric(years),
      all(years >= 2013),
      all(years <= as.numeric(format(Sys.Date(), "%Y"))),
      msg = "years must be a numeric vector with valid years (2013 or later)"
    )
  }
  
  # Validate payment_type
  assertthat::assert_that(
    is.character(payment_type),
    payment_type %in% PAYMENT_TYPES,
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  
  # Validate output_table_name
  assertthat::assert_that(
    is.character(output_table_name),
    nchar(output_table_name) > 0,
    msg = "output_table_name must be a non-empty character string"
  )
  
  # Validate verbose
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be a logical value (TRUE or FALSE)"
  )
  
  # Validate force_reimport
  assertthat::assert_that(
    is.logical(force_reimport),
    msg = "force_reimport must be a logical value (TRUE or FALSE)"
  )
  
  # Validate preserve_all_columns
  assertthat::assert_that(
    is.logical(preserve_all_columns),
    msg = "preserve_all_columns must be a logical value (TRUE or FALSE)"
  )
  
  # Validate max_rows if provided
  if (!is.null(max_rows)) {
    assertthat::assert_that(
      is.numeric(max_rows),
      max_rows > 0,
      msg = "max_rows must be a positive numeric value or NULL"
    )
  }
}

#' @noRd
connect_to_duckdb <- function(db_path) {
  # Attempt to connect to DuckDB
  tryCatch({
    conn <- DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db_path,
      read_only = FALSE
    )
    logger::log_success("Successfully connected to DuckDB")
    return(conn)
  }, error = function(e) {
    logger::log_error(sprintf("Failed to connect to DuckDB: %s", e$message))
    stop("Database connection failed")
  })
}

#' @noRd
initialize_metadata_tables <- function(conn) {
  logger::log_info("Initializing database tables")
  
  # Check metadata table schema and recreate if necessary
  if (DBI::dbExistsTable(conn, "op_file_metadata")) {
    # Check if table has the expected columns
    expected_columns <- c("filepath", "file_size", "year", "import_timestamp", "row_count", "status")
    actual_columns <- DBI::dbListFields(conn, "op_file_metadata")
    
    # Check if all expected columns exist
    if (!all(expected_columns %in% actual_columns)) {
      logger::log_warn("Existing op_file_metadata table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_file_metadata")
    }
  }
  
  # Create metadata table for tracking imported files if it doesn't exist
  if (!DBI::dbExistsTable(conn, "op_file_metadata")) {
    DBI::dbExecute(conn, "
      CREATE TABLE op_file_metadata (
        filepath VARCHAR,
        file_size DOUBLE,
        year INTEGER,
        import_timestamp TIMESTAMP,
        row_count INTEGER,
        status VARCHAR
      )
    ")
    logger::log_success("Created op_file_metadata table")
  }
  
  # Check column mapping table schema and recreate if necessary
  if (DBI::dbExistsTable(conn, "op_column_mappings")) {
    # Check if table has the expected columns
    expected_columns <- c("year", "original_column", "standardized_column", "data_type")
    actual_columns <- DBI::dbListFields(conn, "op_column_mappings")
    
    # Check if all expected columns exist
    if (!all(expected_columns %in% actual_columns)) {
      logger::log_warn("Existing op_column_mappings table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_column_mappings")
    }
  }
  
  # Create column mapping table if it doesn't exist
  if (!DBI::dbExistsTable(conn, "op_column_mappings")) {
    DBI::dbExecute(conn, "
      CREATE TABLE op_column_mappings (
        year INTEGER,
        original_column VARCHAR,
        standardized_column VARCHAR,
        data_type VARCHAR
      )
    ")
    logger::log_success("Created op_column_mappings table")
  }
}

#' @noRd
find_open_payments_files <- function(base_dir, file_pattern) {
  # Get all CSV files in the directory
  all_files <- list.files(
    path = base_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )
  
  # Filter for files matching the pattern
  matching_files <- all_files[grep(file_pattern, basename(all_files))]
  
  # Ensure we found at least one file
  assertthat::assert_that(
    length(matching_files) > 0,
    msg = sprintf("No files matching pattern '%s' found in %s", file_pattern, base_dir)
  )
  
  # Get file sizes for logging
  file_sizes <- file.size(matching_files) / (1024 * 1024)  # Convert to MB
  total_size <- sum(file_sizes)
  
  # Log file information
  logger::log_info(sprintf("Found %d files totaling %.2f MB", 
                           length(matching_files), total_size))
  
  # Return data frame with file information
  file_data <- data.frame(
    file_path = matching_files,
    file_size = file_sizes,
    file_name = basename(matching_files),
    stringsAsFactors = FALSE
  )
  
  # Extract year from filename
  file_data$year <- as.integer(stringr::str_extract(
    file_data$file_name, 
    "PGYR(\\d{4})", 
    group = 1
  ))
  
  return(file_data)
}

#' @noRd
import_open_payments_files <- function(csv_files, conn, years, force_reimport, max_rows) {
  # If specific years requested, filter files
  if (!is.null(years)) {
    csv_files <- csv_files[csv_files$year %in% years, ]
    logger::log_info(sprintf("Filtered to %d files for requested years", nrow(csv_files)))
  }
  
  # Sort files by year
  csv_files <- csv_files[order(csv_files$year), ]
  
  # Check if force_reimport is enabled
  if (force_reimport) {
    logger::log_info("Force reimport is enabled. All files will be reimported.")
  } else {
    # Check which files have already been imported
    imported_files <- DBI::dbGetQuery(
      conn,
      "SELECT filepath, year, row_count FROM op_file_metadata WHERE status = 'completed'"
    )
    
    if (nrow(imported_files) > 0) {
      # Filter out already imported files
      csv_files <- csv_files[!csv_files$file_path %in% imported_files$filepath, ]
      logger::log_info(sprintf("%d files already imported, skipping them", 
                               nrow(imported_files)))
    }
  }
  
  # If no files to import, return early
  if (nrow(csv_files) == 0) {
    logger::log_info("No new files to import")
    
    # Get list of imported years
    imported_years <- DBI::dbGetQuery(
      conn,
      "SELECT DISTINCT year FROM op_file_metadata ORDER BY year"
    )$year
    
    return(list(
      years = imported_years,
      files = 0
    ))
  }
  
  # Log import start
  logger::log_info("Importing files to DuckDB with batched processing")
  
  # Process each file
  imported_years <- c()
  for (i in 1:nrow(csv_files)) {
    file_info <- csv_files[i, ]
    logger::log_info(sprintf("Importing file %d/%d: %s (%.2f MB)", 
                             i, nrow(csv_files), 
                             basename(file_info$file_path), 
                             file_info$file_size))
    
    # Import the file
    import_status <- import_single_file(
      conn = conn,
      file_path = file_info$file_path,
      year = file_info$year,
      max_rows = max_rows
    )
    
    if (import_status) {
      # Track imported years
      imported_years <- unique(c(imported_years, file_info$year))
    }
  }
  
  # Return years and count of files imported
  return(list(
    years = sort(imported_years),
    files = nrow(csv_files)
  ))
}

#' @noRd
#' @noRd
import_single_file <- function(conn, file_path, year, max_rows) {
  # Generate a table name for this file
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  table_name <- sprintf("op_data_%d_%s", year, timestamp)
  
  # Create a temporary table name
  temp_table_name <- sprintf("op_temp_%d", year)
  
  # First, sample just a few rows to determine schema
  # This reads only a small portion of the file
  sample_size <- min(1000, max_rows * 2)
  if (is.null(max_rows)) sample_size <- 1000
  
  sample_cmd <- sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=%d, ignore_errors=true, strict_mode=false)",
    temp_table_name,
    file_path,
    sample_size
  )
  
  # Create schema with relaxed parsing using only the sample
  schema_success <- tryCatch({
    DBI::dbExecute(conn, sample_cmd)
    logger::log_success("Created table schema with relaxed parsing")
    TRUE
  }, error = function(e) {
    logger::log_error(sprintf("Failed to create schema: %s", e$message))
    # Try even more relaxed options
    alt_cmd <- sprintf(
      "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=%d, ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
      temp_table_name,
      file_path,
      sample_size
    )
    tryCatch({
      DBI::dbExecute(conn, alt_cmd)
      logger::log_success("Created table schema with all columns as VARCHAR")
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed even with relaxed parsing: %s", e$message))
      FALSE
    })
  })
  
  if (!schema_success) {
    return(FALSE)
  }
  
  # Get column information
  columns <- DBI::dbListFields(conn, temp_table_name)
  logger::log_debug(sprintf("Detected %d columns in the file", length(columns)))
  
  # Create the target table with same schema 
  create_table_cmd <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0",
    table_name,
    temp_table_name
  )
  DBI::dbExecute(conn, create_table_cmd)
  
  # Now, directly import only the number of rows needed
  # This will read only max_rows from the beginning of the file
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Importing first %d rows only", max_rows))
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto(
        '%s', 
        ignore_errors=true, 
        strict_mode=false,
        sample_size=%d,
        HEADER=true
      ) LIMIT %d",
      table_name,
      file_path,
      sample_size,
      max_rows
    )
  } else {
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ignore_errors=true, strict_mode=false)",
      table_name,
      file_path
    )
  }
  
  # Execute the import command
  import_success <- tryCatch({
    DBI::dbExecute(conn, import_cmd)
    TRUE
  }, error = function(e) {
    logger::log_error(sprintf("Failed with regular import, trying with ALL_VARCHAR: %s", e$message))
    
    # Try with all VARCHAR as a fallback
    if (!is.null(max_rows)) {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto(
          '%s', 
          ALL_VARCHAR=true, 
          ignore_errors=true, 
          strict_mode=false,
          sample_size=%d,
          HEADER=true
        ) LIMIT %d",
        table_name,
        file_path,
        sample_size,
        max_rows
      )
    } else {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
        table_name,
        file_path
      )
    }
    
    tryCatch({
      DBI::dbExecute(conn, fallback_cmd)
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed to import file: %s", e$message))
      FALSE
    })
  })
  
  if (!import_success) {
    # Record failure in metadata
    metadata_cmd <- sprintf(
      "INSERT INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
       VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, 0, 'failed')",
      file_path,
      file.size(file_path) / (1024 * 1024),
      year
    )
    DBI::dbExecute(conn, metadata_cmd)
    return(FALSE)
  }
  
  # Check how many rows were imported
  row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", table_name)
  row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
  
  # Log success
  if (!is.null(max_rows)) {
    logger::log_success(sprintf("Successfully imported %s rows (limited to %s)",
                                format_with_commas(row_count),
                                format_with_commas(max_rows)))
  } else {
    logger::log_success(sprintf("Successfully imported %s rows",
                                format_with_commas(row_count)))
  }
  
  # Record in metadata table
  metadata_cmd <- sprintf(
    "INSERT INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
     VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, %d, 'completed')",
    file_path,
    file.size(file_path) / (1024 * 1024),
    year,
    row_count
  )
  DBI::dbExecute(conn, metadata_cmd)
  
  # Record column mappings
  record_column_mappings(conn, year, columns, temp_table_name)
  
  return(TRUE)
}

#' @noRd
record_column_mappings <- function(conn, year, columns, table_name) {
  for (col in columns) {
    # Create standardized column name
    std_col <- stringr::str_replace_all(col, " ", "_")
    std_col <- stringr::str_to_lower(std_col)
    
    # Skip if mapping already exists
    mapping_exists <- DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT COUNT(*) AS cnt FROM op_column_mappings 
         WHERE year = %d AND original_column = '%s'",
        year, col
      )
    )$cnt > 0
    
    if (!mapping_exists) {
      # Get the column type
      col_type_query <- sprintf(
        "SELECT data_type FROM information_schema.columns 
         WHERE table_name = '%s' AND column_name = '%s'",
        table_name, col
      )
      col_type <- tryCatch({
        DBI::dbGetQuery(conn, col_type_query)$data_type[1]
      }, error = function(e) {
        "VARCHAR"
      })
      
      if (is.null(col_type) || length(col_type) == 0) {
        col_type <- "VARCHAR"
      }
      
      # Insert mapping
      mapping_cmd <- sprintf(
        "INSERT INTO op_column_mappings (year, original_column, standardized_column, data_type)
         VALUES (%d, '%s', '%s', '%s')",
        year, col, std_col, col_type
      )
      DBI::dbExecute(conn, mapping_cmd)
    }
  }
}

#' @noRd
create_unified_schema <- function(conn, years, preserve_all_columns) {
  logger::log_info("Creating comprehensive column mapping to preserve all columns")
  
  # Initialize list to store column information for each year
  year_columns <- list()
  
  # Define high-priority columns that should always be included
  high_priority_columns <- c(
    # Identifiers
    "Physician_Profile_ID", "Covered_Recipient_Profile_ID", 
    "Teaching_Hospital_ID", "Record_ID",
    # NPI information
    "Covered_Recipient_NPI", "Physician_NPI", "NPI",
    # Key payment information
    "Program_Year", "Total_Amount_of_Payment_USDollars", "Date_of_Payment",
    "Nature_of_Payment_or_Transfer_of_Value", "Form_of_Payment_or_Transfer_of_Value",
    # Recipient information
    "Physician_First_Name", "Physician_Middle_Name", "Physician_Last_Name",
    "Covered_Recipient_First_Name", "Covered_Recipient_Middle_Name", "Covered_Recipient_Last_Name",
    "Recipient_State", "Recipient_Country", "Covered_Recipient_Type",
    # Payer information
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    # Product information
    "Product_Indicator"
  )
  
  # Get columns for each year
  for (year in years) {
    # Get the most recent table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    
    # Get columns for this year
    columns <- DBI::dbListFields(conn, year_table)
    
    # Store column information
    year_columns[[as.character(year)]] <- columns
    
    # Find and log NPI columns
    npi_cols <- columns[grep("NPI", columns, ignore.case = TRUE)]
    if (length(npi_cols) > 0) {
      logger::log_debug(sprintf("Year %d: Found NPI columns: %s", 
                                year, paste(npi_cols, collapse = ", ")))
      
      # Add all NPI columns to high priority list
      high_priority_columns <- unique(c(high_priority_columns, npi_cols))
    }
    
    # Log column count
    logger::log_debug(sprintf("Year %d: Mapped %d columns", year, length(columns)))
  }
  
  # Create comprehensive list of all unique columns across years
  all_columns <- unique(unlist(year_columns))
  
  # If not preserving all columns, filter to high priority only
  if (!preserve_all_columns) {
    all_columns <- all_columns[all_columns %in% high_priority_columns]
  }
  
  # Ensure Source_Table is included
  all_columns <- unique(c(all_columns, "Source_Table"))
  
  # Create mapping of column types
  column_types <- character(length(all_columns))
  names(column_types) <- all_columns
  
  # Default to VARCHAR
  column_types[] <- "VARCHAR"
  
  # Special types for known columns
  column_types["Total_Amount_of_Payment_USDollars"] <- "DOUBLE"
  column_types["Program_Year"] <- "INTEGER"
  column_types["Record_ID"] <- "VARCHAR"
  
  # Ensure NPI columns are VARCHAR to preserve leading zeros
  npi_columns <- all_columns[grep("NPI", all_columns, ignore.case = TRUE)]
  column_types[npi_columns] <- "VARCHAR"
  
  # Return schema information
  return(list(
    columns = all_columns,
    types = column_types,
    year_columns = year_columns
  ))
}

#' @noRd
create_final_table <- function(conn, table_name, schema, drop_existing = FALSE) {
  # Drop existing table if requested
  if (drop_existing && DBI::dbExistsTable(conn, table_name)) {
    logger::log_warn(sprintf("Dropping existing table %s as requested", table_name))
    DBI::dbExecute(conn, sprintf("DROP TABLE %s", table_name))
  }
  
  # Create the table with unified schema if it doesn't exist
  if (!DBI::dbExistsTable(conn, table_name)) {
    # Generate column definitions with appropriate types
    column_defs <- character(length(schema$columns))
    for (i in seq_along(schema$columns)) {
      col_name <- schema$columns[i]
      col_type <- schema$types[col_name]
      column_defs[i] <- sprintf('"%s" %s', col_name, col_type)
    }
    
    # Create the table
    create_table_cmd <- sprintf(
      "CREATE TABLE %s (%s)",
      table_name,
      paste(column_defs, collapse = ", ")
    )
    
    # Execute create table command
    tryCatch({
      DBI::dbExecute(conn, create_table_cmd)
      logger::log_success(sprintf("Successfully created table %s with unified schema", table_name))
    }, error = function(e) {
      logger::log_error(sprintf("Failed to create table %s: %s", table_name, e$message))
      stop("Table creation failed")
    })
  }
}

#' @noRd
merge_open_payments_data <- function(conn, years, output_table_name, unified_schema) {
  logger::log_info(sprintf("Merging data into table: %s", output_table_name))
  
  # Set batch size for processing
  batch_size <- 100000
  
  # Track total rows merged
  total_rows_merged <- 0
  
  # Process each year
  for (year in years) {
    # Get the most recent table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    
    # Get row count for this year
    row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", year_table)
    year_row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
    
    # Calculate number of batches
    num_batches <- ceiling(year_row_count / batch_size)
    
    logger::log_info(sprintf("Merging %d rows from year %d in %d chunks", 
                             year_row_count, year, num_batches))
    
    # Process each batch
    for (batch in 1:num_batches) {
      # Calculate offsets
      offset <- (batch - 1) * batch_size
      limit <- batch_size
      
      # Get columns for the current year table
      year_columns <- unified_schema$year_columns[[as.character(year)]]
      if (is.null(year_columns)) {
        logger::log_error(sprintf("No column information for year %d", year))
        next
      }
      
      # Build INSERT statement one section at a time
      insert_columns <- paste(sprintf('"%s"', unified_schema$columns), collapse = ", ")
      
      # Build SELECT clause
      select_parts <- character(length(unified_schema$columns))
      for (i in seq_along(unified_schema$columns)) {
        col <- unified_schema$columns[i]
        
        # Special case for Source_Table column
        if (col == "Source_Table") {
          select_parts[i] <- sprintf("'%s' AS \"Source_Table\"", year_table)
          next
        }
        
        # Check if column exists in this year's table
        if (col %in% year_columns) {
          # Special handling for NPI columns
          if (grepl("NPI", col, ignore.case = TRUE)) {
            select_parts[i] <- sprintf('CAST("%s" AS VARCHAR) AS "%s"', col, col)
          } else {
            select_parts[i] <- sprintf('"%s"', col)
          }
        } else {
          select_parts[i] <- sprintf('NULL AS "%s"', col)
        }
      }
      
      select_clause <- paste(select_parts, collapse = ", ")
      
      # Build the complete INSERT statement
      insert_cmd <- sprintf(
        "INSERT INTO %s (%s) SELECT %s FROM %s OFFSET %d LIMIT %d",
        output_table_name,
        insert_columns,
        select_clause,
        year_table,
        offset,
        limit
      )
      
      # Execute the insert
      tryCatch({
        DBI::dbExecute(conn, insert_cmd)
        logger::log_debug(sprintf("Merged chunk %d/%d for year %d", 
                                  batch, num_batches, year))
      }, error = function(e) {
        logger::log_error(sprintf("Failed to merge chunk %d for year %d: %s", 
                                  batch, year, e$message))
        
        # Let's try to provide more helpful error information
        if (grepl("column count", e$message, ignore.case = TRUE)) {
          # Get column count details
          table_cols <- DBI::dbListFields(conn, output_table_name)
          query_cols <- length(select_parts)
          logger::log_error(sprintf("Column count mismatch: Table has %d columns, query has %d columns", 
                                    length(table_cols), query_cols))
        }
      })
    }
    
    # Update total rows
    total_rows_merged <- total_rows_merged + year_row_count
  }
  
  # Log completion
  logger::log_success(sprintf("Successfully merged %s rows into %s",
                              format_with_commas(total_rows_merged),
                              output_table_name))
  
  # Return summary
  return(list(
    table_name = output_table_name,
    total_rows = total_rows_merged,
    years = years
  ))
}

#' @noRd
check_npi_data <- function(conn, table_name) {
  # Get column information
  column_info <- tryCatch({
    DBI::dbGetQuery(conn, sprintf("PRAGMA table_info('%s')", table_name))
  }, error = function(e) {
    return(NULL)
  })
  
  if (is.null(column_info)) {
    logger::log_error("Could not retrieve column information for final table")
    return(FALSE)
  }
  
  # Find NPI columns
  npi_columns <- column_info$name[grep("NPI", column_info$name, ignore.case = TRUE)]
  
  if (length(npi_columns) == 0) {
    logger::log_error("No NPI columns found in the final merged table")
    return(FALSE)
  }
  
  logger::log_info(sprintf("Found %d NPI-related columns in final table: %s", 
                           length(npi_columns), paste(npi_columns, collapse = ", ")))
  
  # Check for non-NULL NPI values
  for (npi_col in npi_columns) {
    query <- sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL", 
                     table_name, npi_col)
    result <- DBI::dbGetQuery(conn, query)
    
    if (result$cnt > 0) {
      logger::log_success(sprintf("Column %s contains %d non-NULL values", 
                                  npi_col, result$cnt))
    } else {
      logger::log_debug(sprintf("Column %s contains only NULL values", npi_col))
      
      # Try to fix NPI data if it's all NULL
      fix_npi_data_from_temp_tables(conn, table_name, npi_col)
    }
  }
  
  return(TRUE)
}

#' @noRd
fix_npi_data_from_temp_tables <- function(conn, output_table_name, npi_column) {
  logger::log_info(sprintf("Attempting to fix missing NPI data for column %s", npi_column))
  
  # Find all temporary tables for each year
  temp_tables <- DBI::dbGetQuery(
    conn, 
    "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_temp_%'"
  )$table_name
  
  if (length(temp_tables) == 0) {
    logger::log_debug("No temporary tables found for NPI data recovery")
    return(FALSE)
  }
  
  updated_rows <- 0
  
  # Try each temp table
  for (temp_table in temp_tables) {
    # Check if temp table has the NPI column
    temp_columns <- DBI::dbListFields(conn, temp_table)
    
    if (npi_column %in% temp_columns) {
      # Check if the temp table has non-NULL NPI values
      check_query <- sprintf(
        "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL",
        temp_table, npi_column
      )
      has_npi_data <- DBI::dbGetQuery(conn, check_query)$cnt > 0
      
      if (has_npi_data) {
        logger::log_debug(sprintf("Found NPI data in %s, attempting to copy to main table", temp_table))
        
        # Extract year from temp table name
        year <- as.integer(stringr::str_extract(temp_table, "\\d{4}"))
        
        # Get a list of columns that both tables have in common to use for joins
        output_columns <- DBI::dbListFields(conn, output_table_name)
        common_columns <- intersect(temp_columns, output_columns)
        common_columns <- common_columns[!grepl("NPI", common_columns, ignore.case = TRUE)]
        
        # Need at least one common column to do a join
        if (length(common_columns) == 0) {
          logger::log_debug("No common columns found for joining")
          next
        }
        
        # Try using Record_ID first if available, as it's most likely to be unique
        join_column <- if ("Record_ID" %in% common_columns) "Record_ID" else common_columns[1]
        
        # Create update query based on matching keys
        update_query <- sprintf(
          "UPDATE %s SET \"%s\" = t.\"%s\" 
           FROM %s t 
           WHERE %s.\"Program_Year\" = %d 
           AND %s.\"%s\" = t.\"%s\"
           AND %s.\"%s\" IS NULL",
          output_table_name, npi_column, npi_column,
          temp_table,
          output_table_name, year,
          output_table_name, join_column, join_column,
          output_table_name, npi_column
        )
        
        # Execute the update
        tryCatch({
          DBI::dbExecute(conn, update_query)
          
          # Check how many rows were updated
          updated_check_query <- sprintf(
            "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL AND \"Program_Year\" = %d",
            output_table_name, npi_column, year
          )
          updated_count <- DBI::dbGetQuery(conn, updated_check_query)$cnt
          
          logger::log_success(sprintf("Updated %d rows with NPI data from year %d", 
                                      updated_count, year))
          
          updated_rows <- updated_rows + updated_count
        }, error = function(e) {
          logger::log_error(sprintf("Failed to update NPI data from %s: %s", 
                                    temp_table, e$message))
        })
      }
    }
  }
  
  if (updated_rows > 0) {
    logger::log_success(sprintf("Successfully recovered %d NPI values in total", updated_rows))
    return(TRUE)
  } else {
    logger::log_debug("Could not recover any NPI values from temporary tables")
    return(FALSE)
  }
}

base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"

# Process the data with the improved function
invisible(gc())

result <- process_open_payments_data(
  base_dir = base_dir,            
  db_path = db_path,              
  payment_type = "general",       
  output_table_name = "open_payments_merged",
  verbose = TRUE,
  force_reimport = TRUE,
  preserve_all_columns = TRUE,
  max_rows = NULL  # Use NULL instead of Inf to import all rows                    
)

# Verify the data in the resulting table
con <- result$connection

# Check if NPI data was properly imported
npi_verification <- DBI::dbGetQuery(
  con,
  "SELECT COUNT(*) as total_rows,
   SUM(CASE WHEN \"Covered_Recipient_NPI\" IS NOT NULL THEN 1 ELSE 0 END) as npi_count,
   (SUM(CASE WHEN \"Covered_Recipient_NPI\" IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as npi_percentage
   FROM open_payments_merged"
)
print(npi_verification)

# Get a sample of rows with NPI data
npi_sample <- DBI::dbGetQuery(
  con,
  "SELECT Program_Year, Physician_Profile_ID, Covered_Recipient_NPI, Total_Amount_of_Payment_USDollars 
   FROM open_payments_merged 
   WHERE Covered_Recipient_NPI IS NOT NULL 
   LIMIT 10"
)
print(npi_sample)


#### function 1608 ----
#' Process Open Payments Data Using DuckDB
#'
#' @description
#' This function reads Open Payments CSV files (General, Research, or Ownership payments)
#' into a DuckDB database, normalizes the schema across years, and merges them into a
#' unified table for analysis. It supports verbose logging, selective column inclusion,
#' and limiting the number of rows per file for testing.
#'
#' @param base_dir Character. Path to the root directory containing Open Payments CSV files.
#'                 This directory may include subdirectories by year.
#' @param db_path Character. Full file path to the DuckDB database (.duckdb file).
#' @param years Numeric vector or NULL. Optional filter for years to process (e.g., c(2019, 2020)).
#'              If NULL, all detected years will be processed.
#' @param payment_type Character. One of "general", "research", or "ownership". Determines which
#'                     file type to import. Must match naming in Open Payments data files.
#' @param output_table_name Character. Name of the merged table to create in the DuckDB database.
#' @param verbose Logical. If TRUE, logs detailed progress using the `logger` package.
#'                If FALSE, only success messages will be printed.
#' @param force_reimport Logical. If TRUE, imports all matching files even if previously processed.
#'                       If FALSE, skips already-imported files based on metadata.
#' @param preserve_all_columns Logical. If TRUE, retains all columns found across years.
#'                             If FALSE, keeps only a consistent subset of key columns.
#' @param max_rows Numeric or NULL. Maximum number of rows to import from each file (useful for testing).
#'                 If NULL, all rows will be imported.
#'
#' @return A named list with the following elements:
#'   \item{connection}{DuckDB connection object}
#'   \item{output_table}{Name of the merged table}
#'   \item{years_processed}{Vector of processed years}
#'   \item{files_processed}{Count of files processed}
#'   \item{rows_imported}{Total number of rows merged into the final table}
#'   \item{columns_mapped}{Count of standardized columns in the final schema}
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_debug log_success log_error log_warn log_threshold layout_glue_generator
#' @importFrom dplyr filter select mutate rename inner_join left_join bind_rows
#' @importFrom stringr str_extract str_replace_all str_to_lower
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbExistsTable dbListFields dbListTables dbGetQuery
#' @importFrom duckdb duckdb
#'
#' @examples
#' # Example 1: Basic usage with default settings for general payments data
#' payment_data <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/output.duckdb",
#'   payment_type = "general",
#'   verbose = TRUE,
#'   preserve_all_columns = TRUE,
#'   max_rows = NULL
#' )
#' # Creates a merged table of all general payment data with all columns preserved
#'
#' # Example 2: Process specific years with row limitation for testing
#' test_data <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/test.duckdb",
#'   years = c(2020, 2021),
#'   payment_type = "research",
#'   output_table_name = "research_payments_test",
#'   verbose = TRUE,
#'   force_reimport = TRUE,
#'   preserve_all_columns = FALSE,
#'   max_rows = 1000
#' )
#' # Reimports 2020-2021 research payments with only key columns, limited to 1000 rows per file
#'
#' # Example 3: Process ownership payments with minimal logging
#' ownership_data <- process_open_payments_data(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "ownership_payments.duckdb",
#'   payment_type = "ownership",
#'   verbose = FALSE,
#'   force_reimport = FALSE,
#'   preserve_all_columns = TRUE,
#'   max_rows = NULL
#' )
#' # Silently processes all ownership payments with all columns preserved
#'
#' @export
process_open_payments_data <- function(base_dir,
                                       db_path,
                                       years = NULL,
                                       payment_type = "general",
                                       output_table_name = "open_payments_merged",
                                       verbose = TRUE,
                                       force_reimport = FALSE,
                                       preserve_all_columns = TRUE,
                                       max_rows = NULL) {
  
  # -------------------------------------------------------------------
  # Set up the logger output (INFO level if verbose, SUCCESS otherwise)
  # -------------------------------------------------------------------
  setup_open_payments_logger(verbose)
  
  # ---------------------------------------------------------------
  # Ensure all arguments are valid and correctly formatted
  # ---------------------------------------------------------------
  logger::log_debug("Validating input parameters")
  validate_open_payments_inputs(
    base_dir, 
    db_path, 
    years, 
    payment_type, 
    output_table_name, 
    verbose,
    force_reimport, 
    preserve_all_columns, 
    max_rows
  )
  logger::log_debug("Input validation completed")
  
  # ---------------------------------------------------------------
  # Define regex patterns for the different types of payment files
  # ---------------------------------------------------------------
  file_patterns <- list(
    general = "OP_DTL_GNRL_PGYR\\d{4}.*\\.csv$",
    research = "OP_DTL_RSRCH_PGYR\\d{4}.*\\.csv$",
    ownership = "OP_DTL_OWNRSHP_PGYR\\d{4}.*\\.csv$"
  )
  
  # ---------------------------------------------------------------
  # Ensure selected payment type is valid and extract its pattern
  # ---------------------------------------------------------------
  assertthat::assert_that(
    payment_type %in% names(file_patterns),
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  file_pattern <- file_patterns[[payment_type]]
  
  # Start processing with info message
  logger::log_info("Starting Open Payments data processing")
  
  # Log max rows limit if set
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Limiting import to %d rows per file", max_rows))
  }
  
  # Log column preservation setting
  if (preserve_all_columns) {
    logger::log_info("Preserving all original columns from source files")
  }
  
  # ---------------------------------------------------------------
  # Connect to or create DuckDB database at provided path
  # ---------------------------------------------------------------
  payments_conn <- connect_to_duckdb(db_path)
  
  # ----------------------------------------------------------------
  # Create metadata tracking tables if they don't exist or are wrong
  # ----------------------------------------------------------------
  initialize_metadata_tables(payments_conn)
  
  # -------------------------------------------------------------------
  # Search for matching Open Payments CSV files recursively
  # -------------------------------------------------------------------
  logger::log_info(sprintf("Searching for files matching: %s", file_pattern))
  csv_files <- find_open_payments_files(base_dir, file_pattern)
  
  
  # -------------------------------------------------------------------
  # Import and track each file, skipping ones already loaded unless forced
  # -------------------------------------------------------------------
  processed_files <- import_open_payments_files(
    csv_files = csv_files,
    conn = payments_conn,
    years = years,
    force_reimport = force_reimport,
    max_rows = max_rows
  )
  
  # -------------------------------------------------------------------
  # Create standardized column mapping for all imported years
  # -------------------------------------------------------------------
  column_mapping <- create_column_mapping(
    conn = payments_conn,
    years = processed_files$years,
    preserve_all_columns = preserve_all_columns,
    payment_type = payment_type
  )
  
  # -------------------------------------------------------------------
  # Merge all yearly tables into one long-format final table
  # -------------------------------------------------------------------
  merged_data <- merge_open_payments_data(
    conn = payments_conn,
    years = processed_files$years,
    output_table_name = output_table_name,
    column_mapping = column_mapping
  )
  
  # -------------------------------------------------------------------
  # Validate presence and content of NPI-related columns in final table
  # -------------------------------------------------------------------
  check_npi_data(payments_conn, output_table_name)
  
  # -------------------------------------------------------------------
  # Signal completion
  # -------------------------------------------------------------------
  logger::log_success("Open Payments data processing completed successfully")
  
  # -------------------------------------------------------------------
  # Return list summarizing the entire import and merge process
  # -------------------------------------------------------------------
  return(list(
    connection = payments_conn,
    output_table = output_table_name,
    years_processed = processed_files$years,
    files_processed = processed_files$files,
    rows_imported = merged_data$total_rows,
    columns_mapped = length(column_mapping$final_columns)
  ))
}

#' Configure logging settings based on verbose parameter
#'
#' @param verbose Logical indicating whether to enable verbose logging
#' @noRd
setup_open_payments_logger <- function(verbose) {
  # Configure logger based on verbose setting
  if (verbose) {
    logger::log_threshold(logger::INFO)
  } else {
    logger::log_threshold(logger::SUCCESS)
  }
  
  # Set log layout with timestamp
  logger::log_layout(logger::layout_glue_generator(
    format = "{level} [{time}] {msg}"
  ))
  
  logger::log_debug("Logger configured successfully")
}

#' Validate all input parameters for the Open Payments processing function
#'
#' @param base_dir Base directory path
#' @param db_path Database file path
#' @param years Years to process
#' @param payment_type Type of payment data
#' @param output_table_name Output table name
#' @param verbose Verbose logging flag
#' @param force_reimport Force reimport flag
#' @param preserve_all_columns Preserve all columns flag
#' @param max_rows Maximum rows to import per file
#' @noRd
validate_open_payments_inputs <- function(base_dir, 
                                          db_path, 
                                          years, 
                                          payment_type, 
                                          output_table_name, 
                                          verbose,
                                          force_reimport, 
                                          preserve_all_columns, 
                                          max_rows) {
  # Validate base_dir
  assertthat::assert_that(
    is.character(base_dir),
    msg = "base_dir must be a character string"
  )
  assertthat::assert_that(
    dir.exists(base_dir),
    msg = sprintf("Directory does not exist: %s", base_dir)
  )
  
  # Validate db_path
  assertthat::assert_that(
    is.character(db_path),
    msg = "db_path must be a character string"
  )
  
  # Validate years if provided
  if (!is.null(years)) {
    assertthat::assert_that(
      is.numeric(years),
      msg = "years must be a numeric vector"
    )
    assertthat::assert_that(
      all(years >= 2013),
      msg = "years must be 2013 or later"
    )
    assertthat::assert_that(
      all(years <= as.numeric(format(Sys.Date(), "%Y"))),
      msg = sprintf("years cannot be in the future (current year: %s)", 
                    format(Sys.Date(), "%Y"))
    )
  }
  
  # Validate payment_type
  assertthat::assert_that(
    is.character(payment_type),
    msg = "payment_type must be a character string"
  )
  assertthat::assert_that(
    payment_type %in% PAYMENT_TYPES,
    msg = "payment_type must be one of 'general', 'research', or 'ownership'"
  )
  
  # Validate output_table_name
  assertthat::assert_that(
    is.character(output_table_name),
    msg = "output_table_name must be a character string"
  )
  assertthat::assert_that(
    nchar(output_table_name) > 0,
    msg = "output_table_name cannot be empty"
  )
  
  # Validate verbose
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be a logical value (TRUE or FALSE)"
  )
  
  # Validate force_reimport
  assertthat::assert_that(
    is.logical(force_reimport),
    msg = "force_reimport must be a logical value (TRUE or FALSE)"
  )
  
  # Validate preserve_all_columns
  assertthat::assert_that(
    is.logical(preserve_all_columns),
    msg = "preserve_all_columns must be a logical value (TRUE or FALSE)"
  )
  
  # Validate max_rows if provided
  if (!is.null(max_rows)) {
    assertthat::assert_that(
      is.numeric(max_rows),
      msg = "max_rows must be numeric or NULL"
    )
    assertthat::assert_that(
      max_rows > 0,
      msg = "max_rows must be a positive number"
    )
  }
  
  logger::log_debug("All input parameters validated successfully")
}

#' Connect to a DuckDB database
#'
#' @param db_path Path to the DuckDB database file
#' @return DuckDB connection object
#' @noRd
connect_to_duckdb <- function(db_path) {
  # Attempt to connect to DuckDB
  tryCatch({
    logger::log_debug(sprintf("Connecting to DuckDB at %s", db_path))
    conn <- DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db_path,
      read_only = FALSE
    )
    logger::log_success("Successfully connected to DuckDB")
    return(conn)
  }, error = function(e) {
    # Safely handle error message that might contain JSON with curly braces
    # which would conflict with the logger's use of glue
    error_msg <- gsub("[{}]", "", e$message)
    logger::log_error(paste0("Failed to connect to DuckDB: ", error_msg))
    stop("Database connection failed")
  })
}

#' Initialize metadata tables in the DuckDB database
#'
#' @param conn DuckDB connection object
#' @noRd
initialize_metadata_tables <- function(conn) {
  logger::log_info("Initializing database metadata tables")
  
  # Check structure of op_file_metadata
  if (DBI::dbExistsTable(conn, "op_file_metadata")) {
    existing_columns <- DBI::dbListFields(conn, "op_file_metadata")
    required_columns <- c("filepath", "file_size", "year",
                          "import_timestamp", "row_count", "status")
    
    if (!all(required_columns %in% existing_columns)) {
      logger::log_warn("Existing op_file_metadata table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_file_metadata")
    }
  }
  
  # Create metadata table for tracking imported files if it doesn't exist
  if (!DBI::dbExistsTable(conn, "op_file_metadata")) {
    DBI::dbExecute(conn, "
      CREATE TABLE op_file_metadata (
        filepath VARCHAR,
        file_size DOUBLE,
        year INTEGER,
        import_timestamp TIMESTAMP,
        row_count INTEGER,
        status VARCHAR,
        PRIMARY KEY (filepath)
      )
    ")
    logger::log_success("Created op_file_metadata table")
  }
  
  # Check and create op_column_mappings with correct schema
  if (DBI::dbExistsTable(conn, "op_column_mappings")) {
    existing_columns <- DBI::dbListFields(conn, "op_column_mappings")
    required_columns <- c("year", "original_column", "standardized_column", "data_type")
    
    if (!all(required_columns %in% existing_columns)) {
      logger::log_warn("Existing op_column_mappings table has incorrect schema. Recreating it.")
      DBI::dbExecute(conn, "DROP TABLE op_column_mappings")
    }
  }
  
  if (!DBI::dbExistsTable(conn, "op_column_mappings")) {
    DBI::dbExecute(conn, "
    CREATE TABLE op_column_mappings (
      year INTEGER,
      original_column VARCHAR,
      standardized_column VARCHAR,
      data_type VARCHAR,
      PRIMARY KEY (year, original_column)
    )
  ")
    logger::log_success("Created op_column_mappings table")
  }
}

#' Find Open Payments CSV files in the provided directory
#'
#' @param base_dir Base directory to search for files
#' @param file_pattern Regex pattern to match file names
#' @return Data frame of file information including paths, sizes, and years
#' @noRd
find_open_payments_files <- function(base_dir, file_pattern) {
  # Get all CSV files in the directory
  logger::log_debug(sprintf("Scanning directory %s for CSV files", base_dir))
  all_files <- list.files(
    path = base_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )
  
  logger::log_debug(sprintf("Found %d total CSV files", length(all_files)))
  
  # Filter for files matching the pattern
  matching_files <- all_files[grep(file_pattern, basename(all_files))]
  
  # Ensure we found at least one file
  assertthat::assert_that(
    length(matching_files) > 0,
    msg = sprintf("No files matching pattern '%s' found in %s", file_pattern, base_dir)
  )
  
  # Get file sizes for logging
  file_sizes <- file.size(matching_files) / (1024 * 1024)  # Convert to MB
  total_size <- sum(file_sizes)
  
  # Log file information
  logger::log_info(sprintf("Found %d matching files totaling %.2f MB", 
                           length(matching_files), total_size))
  
  # Return data frame with file information
  file_data <- data.frame(
    filepath = matching_files,
    file_size = file_sizes,
    filename = basename(matching_files),
    stringsAsFactors = FALSE
  )
  
  # Extract year from filename
  file_data$year <- as.integer(stringr::str_extract(
    file_data$filename, 
    "PGYR(\\d{4})", 
    group = 1
  ))
  
  logger::log_debug("Successfully extracted years from filenames")
  
  # Sort by year
  file_data <- file_data[order(file_data$year), ]
  
  return(file_data)
}

#' Import Open Payments files to DuckDB
#'
#' @param csv_files Data frame of files to import
#' @param conn DuckDB connection object
#' @param years Years to filter for processing
#' @param force_reimport Whether to force reimport of previously imported files
#' @param max_rows Maximum number of rows to import per file
#' @return List containing processed years and file count
#' @noRd
import_open_payments_files <- function(csv_files, conn, years, force_reimport, max_rows) {
  # If specific years requested, filter files
  if (!is.null(years)) {
    csv_files <- dplyr::filter(csv_files, year %in% years)
    logger::log_info(sprintf("Filtered to %d files for requested years", nrow(csv_files)))
  }
  
  # Check if force_reimport is enabled
  if (force_reimport) {
    logger::log_info("Force reimport is enabled. All files will be reimported.")
  } else {
    # Check which files have already been imported
    imported_files <- DBI::dbGetQuery(
      conn,
      "SELECT filepath, year, row_count FROM op_file_metadata WHERE status = 'completed'"
    )
    
    if (nrow(imported_files) > 0) {
      # Filter out already imported files
      csv_files <- dplyr::filter(csv_files, !filepath %in% imported_files$filepath)
      logger::log_info(sprintf("%d files already imported, skipping them", 
                               nrow(imported_files)))
    }
  }
  
  # If no files to import, return early
  if (nrow(csv_files) == 0) {
    logger::log_info("No new files to import")
    
    # Get list of imported years
    imported_years <- DBI::dbGetQuery(
      conn,
      "SELECT DISTINCT year FROM op_file_metadata ORDER BY year"
    )$year
    
    return(list(
      years = imported_years,
      files = 0
    ))
  }
  
  # Log import start
  logger::log_info(sprintf("Importing %d files to DuckDB", nrow(csv_files)))
  
  # Process each file
  imported_years <- c()
  successful_imports <- 0
  
  for (i in 1:nrow(csv_files)) {
    file_info <- csv_files[i, ]
    logger::log_info(sprintf("Importing file %d/%d: %s (%.2f MB)", 
                             i, nrow(csv_files), 
                             basename(file_info$filepath), 
                             file_info$file_size))
    
    # Import the file
    import_status <- import_single_file(
      conn = conn,
      file_path = file_info$filepath,
      year = file_info$year,
      max_rows = max_rows
    )
    
    if (import_status) {
      # Track imported years
      imported_years <- unique(c(imported_years, file_info$year))
      successful_imports <- successful_imports + 1
    }
  }
  
  logger::log_success(sprintf("Successfully imported %d of %d files", 
                              successful_imports, nrow(csv_files)))
  
  # Return years and count of files imported
  return(list(
    years = sort(imported_years),
    files = successful_imports
  ))
}

#' Import a single Open Payments file to DuckDB
#'
#' @param conn DuckDB connection object
#' @param file_path Path to the CSV file
#' @param year Year of the data
#' @param max_rows Maximum number of rows to import
#' @return Logical indicating success
#' @noRd
import_single_file <- function(conn, file_path, year, max_rows) {
  # Generate a table name for this file
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  table_name <- sprintf("op_data_%d_%s", year, timestamp)
  
  # Create a temporary table name for schema detection
  temp_table_name <- sprintf("op_temp_%d", year)
  
  logger::log_debug(sprintf("Creating schema for file: %s", basename(file_path)))
  
  # Try to create a schema with relaxed CSV parsing options
  sample_cmd <- sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=1000, AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false)",
    temp_table_name,
    file_path
  )
  
  # Create schema with relaxed parsing
  schema_success <- tryCatch({
    DBI::dbExecute(conn, sample_cmd)
    logger::log_success("Created table schema with relaxed parsing")
    TRUE
  }, error = function(e) {
    # Handle error message safely
    error_msg <- gsub("[{}]", "", e$message)
    logger::log_error(paste0("Failed to create schema: ", error_msg))
    
    # Try even more relaxed options
    alt_cmd <- sprintf(
      "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=1000, ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
      temp_table_name,
      file_path
    )
    tryCatch({
      DBI::dbExecute(conn, alt_cmd)
      logger::log_success("Created table schema with all columns as VARCHAR")
      TRUE
    }, error = function(e) {
      # Handle error message safely
      error_msg <- gsub("[{}]", "", e$message)
      logger::log_error(paste0("Failed even with relaxed parsing: ", error_msg))
      FALSE
    })
  })
  
  if (!schema_success) {
    record_import_failure(conn, file_path, year)
    return(FALSE)
  }
  
  # Get column information
  columns <- DBI::dbListFields(conn, temp_table_name)
  logger::log_debug(sprintf("Detected %d columns in the file", length(columns)))
  
  # Check for NPI column variations
  npi_columns <- columns[grep("NPI", columns, ignore.case = TRUE)]
  if (length(npi_columns) > 0) {
    logger::log_debug(sprintf("Found NPI-related columns: %s", 
                              paste(npi_columns, collapse = ", ")))
  } else {
    logger::log_debug("No NPI-related columns found in this file")
  }
  
  # Create the new table with the same schema
  create_table_cmd <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0",
    table_name,
    temp_table_name
  )
  DBI::dbExecute(conn, create_table_cmd)
  
  # Import data with row limit if specified and relaxed CSV parsing
  import_success <- import_file_data(conn, file_path, table_name, max_rows)
  
  if (!import_success) {
    record_import_failure(conn, file_path, year)
    return(FALSE)
  }
  
  # Check how many rows were imported
  row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", table_name)
  row_count_result <- DBI::dbGetQuery(conn, row_count_query)
  row_count <- row_count_result$row_count
  
  # Log success
  if (!is.null(max_rows)) {
    logger::log_success(sprintf("Successfully imported %s rows (limited to %s)",
                                format_with_commas(row_count),
                                format_with_commas(max_rows)))
  } else {
    logger::log_success(sprintf("Successfully imported %s rows",
                                format_with_commas(row_count)))
  }
  
  # Record in metadata table
  record_import_success(conn, file_path, year, row_count)
  
  # Record column mappings
  record_column_mappings(conn, year, columns, temp_table_name)
  
  return(TRUE)
}

#' Import data from a CSV file to a DuckDB table
#'
#' @param conn DuckDB connection
#' @param file_path Path to the CSV file
#' @param table_name Name of the target table
#' @param max_rows Maximum number of rows to import
#' @return Boolean indicating success
#' @noRd
import_file_data <- function(conn, file_path, table_name, max_rows) {
  logger::log_debug("Importing data from CSV file")
  
  # Construct import command
  if (!is.null(max_rows)) {
    logger::log_info(sprintf("Importing up to %d rows", max_rows))
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false) LIMIT %d",
      table_name,
      file_path,
      max_rows
    )
  } else {
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', AUTO_DETECT=TRUE, ignore_errors=true, strict_mode=false)",
      table_name,
      file_path
    )
  }
  
  # Execute the import command
  tryCatch({
    DBI::dbExecute(conn, import_cmd)
    logger::log_debug("Import completed successfully using AUTO_DETECT")
    return(TRUE)
  }, error = function(e) {
    # Handle error message safely
    error_msg <- gsub("[{}]", "", e$message)
    logger::log_warn(paste0("Failed with AUTO_DETECT, trying with ALL_VARCHAR: ", error_msg))
    
    # Try with all VARCHAR as a fallback
    if (!is.null(max_rows)) {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, ignore_errors=true, strict_mode=false) LIMIT %d",
        table_name,
        file_path,
        max_rows
      )
    } else {
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, ignore_errors=true, strict_mode=false)",
        table_name,
        file_path
      )
    }
    
    tryCatch({
      DBI::dbExecute(conn, fallback_cmd)
      logger::log_debug("Import completed successfully using ALL_VARCHAR")
      return(TRUE)
    }, error = function(e2) {
      # Handle error message safely
      error_msg2 <- gsub("[{}]", "", e2$message)
      logger::log_error(paste0("Failed to import file: ", error_msg2))
      return(FALSE)
    })
  })
}

#' Record a failed import in the metadata table
#'
#' @param conn DuckDB connection
#' @param file_path Path to the file that failed to import
#' @param year Year of the data
#' @noRd
record_import_failure <- function(conn, file_path, year) {
  logger::log_debug("Recording import failure in metadata")
  
  metadata_cmd <- sprintf(
    "INSERT OR REPLACE INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
     VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, 0, 'failed')",
    file_path,
    file.size(file_path) / (1024 * 1024),
    year
  )
  
  tryCatch({
    DBI::dbExecute(conn, metadata_cmd)
    logger::log_debug("Import failure recorded successfully")
  }, error = function(e) {
    logger::log_error(sprintf("Failed to record import failure: %s", e$message))
  })
}

#' Record a successful import in the metadata table
#'
#' @param conn DuckDB connection
#' @param file_path Path to the successfully imported file
#' @param year Year of the data
#' @param row_count Number of rows imported
#' @noRd
record_import_success <- function(conn, file_path, year, row_count) {
  logger::log_debug("Recording import success in metadata")
  
  metadata_cmd <- sprintf(
    "INSERT OR REPLACE INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
     VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, %d, 'completed')",
    file_path,
    file.size(file_path) / (1024 * 1024),
    year,
    row_count
  )
  
  tryCatch({
    DBI::dbExecute(conn, metadata_cmd)
    logger::log_debug("Import success recorded successfully")
  }, error = function(e) {
    logger::log_error(sprintf("Failed to record import success: %s", e$message))
  })
}

#' Record column mappings in the metadata table
#'
#' @param conn DuckDB connection
#' @param year Year of the data
#' @param columns Vector of column names
#' @param table_name Name of the table with the columns
#' @noRd
record_column_mappings <- function(conn, year, columns, table_name) {
  logger::log_debug("Recording column mappings")
  
  for (col in columns) {
    # Create standardized column name
    std_col <- stringr::str_replace_all(col, " ", "_")
    std_col <- stringr::str_to_lower(std_col)
    
    # Skip if mapping already exists
    mapping_exists <- DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT COUNT(*) AS cnt FROM op_column_mappings 
         WHERE year = %d AND original_column = '%s'",
        year, col
      )
    )$cnt > 0
    
    if (!mapping_exists) {
      # Get the column type
      col_type_query <- sprintf(
        "SELECT data_type FROM information_schema.columns 
         WHERE table_name = '%s' AND column_name = '%s'",
        table_name, col
      )
      col_type <- tryCatch({
        DBI::dbGetQuery(conn, col_type_query)$data_type[1]
      }, error = function(e) {
        "VARCHAR"
      })
      
      if (is.null(col_type) || length(col_type) == 0) {
        col_type <- "VARCHAR"
      }
      
      # Insert mapping
      mapping_cmd <- sprintf(
        "INSERT INTO op_column_mappings (year, original_column, standardized_column, data_type)
         VALUES (%d, '%s', '%s', '%s')",
        year, col, std_col, col_type
      )
      DBI::dbExecute(conn, mapping_cmd)
    }
  }
  
  logger::log_debug(sprintf("Recorded mappings for %d columns", length(columns)))
}

#' Create a comprehensive column mapping across all years
#'
#' @param conn DuckDB connection
#' @param years Vector of years to include
#' @param preserve_all_columns Whether to preserve all columns or just key ones
#' @param payment_type Type of payment data
#' @return List containing column mapping information
#' @noRd
create_column_mapping <- function(conn, years, preserve_all_columns, payment_type) {
  logger::log_info("Creating comprehensive column mapping across years")
  
  # Initialize list to store column information for each year
  year_columns <- list()
  column_types <- list()
  
  # Define high-priority columns
  high_priority_columns <- c(
    "Physician_Profile_ID", "Teaching_Hospital_ID", "Record_ID",
    "Covered_Recipient_NPI", "Physician_NPI", "Recipient_NPI", "NPI",
    "Program_Year", "Total_Amount_of_Payment_USDollars", "Date_of_Payment",
    "Nature_of_Payment_or_Transfer_of_Value", "Form_of_Payment_or_Transfer_of_Value",
    "Physician_First_Name", "Physician_Middle_Name", "Physician_Last_Name",
    "Recipient_State", "Recipient_Country", "Covered_Recipient_Type",
    "Covered_Recipient_Primary_Type_1", "Covered_Recipient_Specialty_1",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Product_Indicator", "Name_of_Associated_Covered_Drug_or_Biological1"
  )
  
  # Get columns for each year
  for (year in years) {
    # Get the table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    
    # Get columns for this year
    columns <- DBI::dbListFields(conn, year_table)
    
    # Get column types
    column_type_query <- sprintf(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'",
      year_table
    )
    column_type_info <- tryCatch({
      DBI::dbGetQuery(conn, column_type_query)
    }, error = function(e) {
      data.frame(column_name = columns, data_type = rep("VARCHAR", length(columns)))
    })
    
    # Store column types
    column_types[[as.character(year)]] <- setNames(
      column_type_info$data_type, 
      column_type_info$column_name
    )
    
    # Store column information
    year_columns[[as.character(year)]] <- columns
    
    # Find and log NPI columns
    npi_cols <- columns[grep("NPI", columns, ignore.case = TRUE)]
    if (length(npi_cols) > 0) {
      logger::log_debug(sprintf("Year %d: Found NPI columns: %s", 
                                year, paste(npi_cols, collapse = ", ")))
    }
    
    # Log column count
    logger::log_debug(sprintf("Year %d: Mapped %d columns", year, length(columns)))
  }
  
  # Create comprehensive list of all unique columns across years
  all_columns <- unique(unlist(year_columns))
  
  # Find all NPI-related columns
  all_npi_columns <- all_columns[grep("NPI", all_columns, ignore.case = TRUE)]
  if (length(all_npi_columns) > 0) {
    logger::log_debug(sprintf("All NPI-related columns across years: %s", 
                              paste(all_npi_columns, collapse = ", ")))
  }
  
  # If not preserving all columns, use a subset of key columns
  if (!preserve_all_columns) {
    # Ensure all NPI fields are included
    core_columns <- unique(c(high_priority_columns, all_npi_columns))
    
    # Filter to only include core columns
    all_columns <- all_columns[all_columns %in% core_columns | 
                                 grepl("NPI", all_columns, ignore.case = TRUE)]
    
    logger::log_info(sprintf("Using %d core columns for output", length(all_columns)))
  } else {
    logger::log_info(sprintf("Preserving all %d unique columns across years", length(all_columns)))
  }
  
  # Return the mapping information
  return(list(
    year_columns = year_columns,
    column_types = column_types,
    final_columns = all_columns,
    npi_columns = all_npi_columns
  ))
}

#' Merge yearly tables into a unified output table
#'
#' @param conn DuckDB connection
#' @param years Vector of years to merge
#' @param output_table_name Name of the output table
#' @param column_mapping Column mapping information
#' @return List with merge results
#' @noRd
merge_open_payments_data <- function(conn, years, output_table_name, column_mapping) {
  logger::log_info(sprintf("Merging data into output table: %s", output_table_name))
  
  # Create final table with all columns if it doesn't exist
  if (!DBI::dbExistsTable(conn, output_table_name)) {
    # Generate column definitions
    column_defs <- character(length(column_mapping$final_columns))
    
    for (i in seq_along(column_mapping$final_columns)) {
      col <- column_mapping$final_columns[i]
      col_type <- "VARCHAR"  # Default to VARCHAR
      
      # Try to find a more specific type from any year
      for (year in names(column_mapping$column_types)) {
        if (!is.null(column_mapping$column_types[[year]][[col]])) {
          col_type <- column_mapping$column_types[[year]][[col]]
          break
        }
      }
      
      # For NPI columns, ensure they're VARCHAR to preserve leading zeros
      if (grepl("NPI", col, ignore.case = TRUE)) {
        col_type <- "VARCHAR"
        logger::log_debug(sprintf("Setting column %s as VARCHAR to preserve NPI format", col))
      }
      
      column_defs[i] <- sprintf('"%s" %s', col, col_type)
    }
    
    # Add source table column
    column_defs <- c(column_defs, "Source_Table VARCHAR")
    
    # Create the table
    create_table_cmd <- sprintf(
      "CREATE TABLE %s (%s)",
      output_table_name,
      paste(column_defs, collapse = ", ")
    )
    DBI::dbExecute(conn, create_table_cmd)
    logger::log_success(sprintf("Created output table %s", output_table_name))
  } else {
    # Clear existing data if table exists
    logger::log_info(sprintf("Clearing existing data from %s", output_table_name))
    DBI::dbExecute(conn, sprintf("DELETE FROM %s", output_table_name))
  }
  
  # Set batch size for processing
  batch_size <- 100000
  logger::log_debug(sprintf("Using batch size of %d rows for merge", batch_size))
  
  # Track total rows merged
  total_rows_merged <- 0
  
  # Process each year
  for (year in years) {
    logger::log_info(sprintf("Processing data for year %d", year))
    
    # Get the most recent table for this year
    year_tables <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_data_%d_%%'", year)
    )$table_name
    
    if (length(year_tables) == 0) {
      logger::log_error(sprintf("No table found for year %d", year))
      next
    }
    
    # Use the most recent table
    year_table <- year_tables[order(year_tables, decreasing = TRUE)][1]
    logger::log_debug(sprintf("Using table %s for year %d", year_table, year))
    
    # Get row count for this year
    row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", year_table)
    year_row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
    
    # Calculate number of batches
    num_batches <- ceiling(year_row_count / batch_size)
    
    logger::log_info(sprintf("Merging %d rows from year %d in %d chunks", 
                             year_row_count, year, num_batches))
    
    # Process each batch
    for (batch in 1:num_batches) {
      # Calculate offsets
      offset <- (batch - 1) * batch_size
      limit <- batch_size
      
      # Generate column list for this year's table
      year_columns <- column_mapping$year_columns[[as.character(year)]]
      
      # Generate SQL SELECT expressions, handling missing columns
      select_expressions <- character(length(column_mapping$final_columns))
      for (i in seq_along(column_mapping$final_columns)) {
        col <- column_mapping$final_columns[i]
        
        if (col %in% year_columns) {
          # Special handling for NPI columns to ensure they're preserved correctly
          if (grepl("NPI", col, ignore.case = TRUE)) {
            select_expressions[i] <- sprintf('CAST("%s" AS VARCHAR) AS "%s"', col, col)
          } else {
            select_expressions[i] <- sprintf('"%s"', col)
          }
        } else {
          select_expressions[i] <- sprintf('NULL AS "%s"', col)
        }
      }
      
      # Create the full SELECT statement
      select_clause <- paste(select_expressions, collapse = ", ")
      
      # Add source table information
      select_clause <- paste(select_clause, sprintf(", '%s' AS Source_Table", year_table), sep = "")
      
      # Create the INSERT statement
      insert_cmd <- sprintf(
        "INSERT INTO %s SELECT %s FROM %s OFFSET %d LIMIT %d",
        output_table_name,
        select_clause,
        year_table,
        offset,
        limit
      )
      
      # Execute the insert
      tryCatch({
        DBI::dbExecute(conn, insert_cmd)
        if (batch %% 10 == 0 || batch == num_batches) {
          logger::log_debug(sprintf("Merged chunk %d/%d for year %d", 
                                    batch, num_batches, year))
        }
      }, error = function(e) {
        logger::log_error(sprintf("Failed to merge chunk %d for year %d: %s", 
                                  batch, year, e$message))
      })
    }
    
    # Update total rows
    total_rows_merged <- total_rows_merged + year_row_count
    logger::log_success(sprintf("Merged %s rows from year %d",
                                format_with_commas(year_row_count), year))
  }
  
  # Log completion
  logger::log_success(sprintf("Successfully merged %s total rows into %s",
                              format_with_commas(total_rows_merged),
                              output_table_name))
  
  # Return summary
  return(list(
    table_name = output_table_name,
    total_rows = total_rows_merged,
    years = years
  ))
}

#' Check and verify NPI data in the final table
#'
#' @param conn DuckDB connection
#' @param table_name Name of the table to check
#' @return Logical indicating whether NPI data was found
#' @noRd
check_npi_data <- function(conn, table_name) {
  logger::log_info("Verifying NPI data in the final table")
  
  # Get column information
  column_info <- tryCatch({
    DBI::dbGetQuery(conn, sprintf("PRAGMA table_info('%s')", table_name))
  }, error = function(e) {
    # Handle error message safely
    error_msg <- gsub("[{}]", "", e$message)
    logger::log_error(paste0("Error getting table info: ", error_msg))
    return(NULL)
  })
  
  if (is.null(column_info)) {
    logger::log_error("Could not retrieve column information for final table")
    return(FALSE)
  }
  
  # Find NPI columns
  npi_columns <- column_info$name[grep("NPI", column_info$name, ignore.case = TRUE)]
  
  if (length(npi_columns) == 0) {
    logger::log_error("No NPI columns found in the final merged table")
    return(FALSE)
  }
  
  logger::log_info(sprintf("Found %d NPI-related columns in final table: %s", 
                           length(npi_columns), paste(npi_columns, collapse = ", ")))
  
  # Check for non-NULL NPI values
  npi_checks <- data.frame(
    column = character(0),
    non_null_count = integer(0),
    status = character(0),
    stringsAsFactors = FALSE
  )
  
  for (npi_col in npi_columns) {
    query <- sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL", 
                     table_name, npi_col)
    result <- tryCatch({
      DBI::dbGetQuery(conn, query)
    }, error = function(e) {
      # Handle error message safely
      error_msg <- gsub("[{}]", "", e$message)
      logger::log_error(paste0("Error checking NPI data: ", error_msg))
      data.frame(cnt = 0)
    })
    
    if (result$cnt > 0) {
      logger::log_success(sprintf("Column %s contains %d non-NULL values", 
                                  npi_col, result$cnt))
      npi_checks <- rbind(npi_checks, data.frame(
        column = npi_col,
        non_null_count = result$cnt,
        status = "OK",
        stringsAsFactors = FALSE
      ))
    } else {
      logger::log_warn(sprintf("Column %s contains only NULL values", npi_col))
      npi_checks <- rbind(npi_checks, data.frame(
        column = npi_col,
        non_null_count = 0,
        status = "empty",
        stringsAsFactors = FALSE
      ))
      
      # Try to fix NPI data if it's all NULL by copying from temporary tables
      fix_success <- fix_npi_data_from_temp_tables(conn, table_name, npi_col)
      
      if (fix_success) {
        # Check again after fix attempt
        query <- sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL", 
                         table_name, npi_col)
        result <- tryCatch({
          DBI::dbGetQuery(conn, query)
        }, error = function(e) {
          data.frame(cnt = 0)
        })
        
        if (result$cnt > 0) {
          logger::log_success(sprintf("Fixed column %s, now contains %d non-NULL values", 
                                      npi_col, result$cnt))
          npi_checks$non_null_count[npi_checks$column == npi_col] <- result$cnt
          npi_checks$status[npi_checks$column == npi_col] <- "fixed"
        }
      }
    }
  }
  
  # Log overall NPI data summary
  total_npi_values <- sum(npi_checks$non_null_count)
  logger::log_info(sprintf("Total NPI values in final table: %d", total_npi_values))
  
  return(total_npi_values > 0)
}

#' Fix missing NPI data by copying from temporary tables
#'
#' @param conn DuckDB connection
#' @param output_table_name Name of the output table
#' @param npi_column Name of the NPI column to fix
#' @return Logical indicating whether any fixes were applied
#' @noRd
fix_npi_data_from_temp_tables <- function(conn, output_table_name, npi_column) {
  logger::log_info(sprintf("Attempting to fix missing NPI data for column %s", npi_column))
  
  # Find all temporary tables for each year
  temp_tables <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_temp_%'")$table_name
  }, error = function(e) {
    # Handle error message safely
    error_msg <- gsub("[{}]", "", e$message)
    logger::log_error(paste0("Failed to query temporary tables: ", error_msg))
    return(character(0))
  })
  
  if (length(temp_tables) == 0) {
    logger::log_debug("No temporary tables found for NPI data recovery")
    return(FALSE)
  }
  
  updated_rows <- 0
  
  # Try each temp table
  for (temp_table in temp_tables) {
    # Check if temp table has the NPI column
    temp_columns <- tryCatch({
      DBI::dbListFields(conn, temp_table)
    }, error = function(e) {
      # Handle error message safely
      error_msg <- gsub("[{}]", "", e$message)
      logger::log_warn(paste0("Failed to get columns for table ", temp_table, ": ", error_msg))
      return(character(0))
    })
    
    if (npi_column %in% temp_columns) {
      # Check if the temp table has non-NULL NPI values
      check_query <- sprintf(
        "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL",
        temp_table, npi_column
      )
      has_npi_data <- tryCatch({
        DBI::dbGetQuery(conn, check_query)$cnt > 0
      }, error = function(e) {
        # Handle error message safely
        error_msg <- gsub("[{}]", "", e$message)
        logger::log_warn(paste0("Failed to check NPI data in ", temp_table, ": ", error_msg))
        return(FALSE)
      })
      
      if (has_npi_data) {
        logger::log_debug(sprintf("Found NPI data in %s, attempting to copy to main table", temp_table))
        
        # Extract year from temp table name
        year <- as.integer(stringr::str_extract(temp_table, "\\d{4}"))
        
        # Create update query based on matching keys
        update_query <- sprintf(
          "UPDATE %s SET \"%s\" = t.\"%s\" 
           FROM %s t 
           WHERE %s.\"Program_Year\" = %d 
           AND %s.\"Physician_Profile_ID\" = t.\"Physician_Profile_ID\"
           AND %s.\"%s\" IS NULL",
          output_table_name, npi_column, npi_column,
          temp_table,
          output_table_name, year,
          output_table_name,
          output_table_name, npi_column
        )
        
        # Execute the update
        tryCatch({
          DBI::dbExecute(conn, update_query)
          
          # Check how many rows were updated
          updated_check_query <- sprintf(
            "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL AND \"Program_Year\" = %d",
            output_table_name, npi_column, year
          )
          updated_count <- DBI::dbGetQuery(conn, updated_check_query)$cnt
          
          logger::log_success(sprintf("Updated %d rows with NPI data from year %d", 
                                      updated_count, year))
          
          updated_rows <- updated_rows + updated_count
        }, error = function(e) {
          # Handle error message safely
          error_msg <- gsub("[{}]", "", e$message)
          logger::log_error(paste0("Failed to update NPI data from ", temp_table, ": ", error_msg))
        })
      }
    }
  }
  
  if (updated_rows > 0) {
    logger::log_success(sprintf("Successfully recovered %d NPI values in total", updated_rows))
    return(TRUE)
  } else {
    logger::log_debug("Could not recover any NPI values from temporary tables")
    return(FALSE)
  }
}

## Execute ----
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"

open_payments_output <- process_open_payments_data(base_dir,
                                       db_path,
                                       years = NULL,
                                       payment_type = "general",
                                       output_table_name = "open_payments_merged",
                                       verbose = TRUE,
                                       force_reimport = FALSE,
                                       preserve_all_columns = TRUE,
                                       max_rows = NULL) 
  
#' Process Open Payments Data with Optimized Database Operations
#'
#' @description
#' Processes Open Payments data files and loads them into a DuckDB database with
#' optimized performance. The function handles file discovery, metadata tracking,
#' incremental loading, and column mapping across different years of Open Payments data.
#'
#' @param base_dir Character string specifying the base directory containing Open Payments files.
#' @param db_path Character string specifying the path to the DuckDB database file.
#' @param years Character vector of years to process (e.g., c("2019", "2020", "2021")).
#' @param payment_type Character string specifying the payment type: "general", "research", or "ownership".
#' @param output_table_name Character string specifying the name for the output table.
#' @param force_reimport Logical, if TRUE will reimport all files even if they exist in the database.
#' @param file_pattern Character string with the file pattern to match. Default adapts to payment_type.
#' @param verbose Logical, if TRUE (default) will log detailed information.
#'
#' @return Character string with the name of the created merged table.
#'
#' @importFrom logger log_info log_success log_error log_warn log_debug log_threshold INFO ERROR
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter mutate select distinct
#' @importFrom duckdb duckdb dbConnect
#' @importFrom DBI dbExecute dbGetQuery dbListTables dbDisconnect
#' @importFrom purrr map
#' @importFrom fs file_size dir_ls
#'
#' @examples
#' # Basic usage with default settings for general payments
#' merged_table <- process_open_payments_data_optimized(
#'   base_dir = "/path/to/open_payments_data",
#'   db_path = "/path/to/output.duckdb",
#'   years = c("2021", "2022", "2023"),
#'   payment_type = "general",
#'   output_table_name = "op_general_payments",
#'   verbose = TRUE
#' )
#'
#' # Process research payments with forced reimport
#' research_table <- process_open_payments_data_optimized(
#'   base_dir = "/path/to/open_payments_data",
#'   db_path = "/path/to/output.duckdb",
#'   years = c("2021", "2022"),
#'   payment_type = "research",
#'   output_table_name = "op_research_payments",
#'   force_reimport = TRUE,
#'   verbose = TRUE
#' )
#'
#' # Process ownership data with custom file pattern and limited logging
#' ownership_table <- process_open_payments_data_optimized(
#'   base_dir = "/path/to/open_payments_data",
#'   db_path = "/path/to/output.duckdb",
#'   years = c("2020", "2021", "2022"),
#'   payment_type = "ownership",
#'   output_table_name = "op_ownership_interests",
#'   file_pattern = ".*OWNRSHP.*\\.csv$",
#'   force_reimport = FALSE,
#'   verbose = FALSE
#' )
process_open_payments_data_optimized <- function(base_dir = "/Volumes/Video Projects Muffly 1/open_payments_data",
                                                 db_path = "/Volumes/Video Projects Muffly 1/open_payments_merged.duckdb",
                                                 years = NULL,
                                                 payment_type = "general",
                                                 output_table_name = "open_payments_merged",
                                                 force_reimport = FALSE,
                                                 file_pattern = NULL,
                                                 verbose = TRUE) {
  # Initialize logger
  logger::log_threshold(ifelse(verbose, logger::INFO, logger::ERROR))

  # Log function start
  logger::log_info("Starting Open Payments data processing")
  logger::log_debug("Validating input parameters")

  # Validate input parameters
  assertthat::assert_that(is.character(base_dir))
  assertthat::assert_that(is.character(db_path))
  assertthat::assert_that(is.character(payment_type))
  assertthat::assert_that(payment_type %in% c("general", "research", "ownership"))
  assertthat::assert_that(is.logical(force_reimport))
  assertthat::assert_that(is.logical(verbose))

  if (!is.null(years)) {
    assertthat::assert_that(is.character(years))
  }

  if (is.null(output_table_name)) {
    # Generate default output table name based on payment type
    output_table_name <- paste0("op_", payment_type, "_all_years")
    logger::log_info("Using default output table name: {output_table_name}")
  } else {
    assertthat::assert_that(is.character(output_table_name))
  }

  # Determine file pattern based on payment type if not provided
  if (is.null(file_pattern)) {
    if (payment_type == "general") {
      file_pattern <- ".*_GNRL.*\\.csv$"
    } else if (payment_type == "research") {
      file_pattern <- ".*_RSRCH.*\\.csv$"
    } else if (payment_type == "ownership") {
      file_pattern <- ".*_OWNRSHP.*\\.csv$"
    }
    logger::log_info("Using file pattern: {file_pattern}")
  }

  # Connect to DuckDB
  logger::log_info("Connecting to DuckDB at {db_path}")
  connection <- connect_to_duckdb(db_path, verbose)

  # Initialize database if necessary
  initialize_database(connection, verbose)

  # Find Open Payments files
  payment_files <- find_payment_files(base_dir, years, file_pattern, verbose)

  # Check which files are already imported
  payment_files <- check_imported_files(connection, payment_files, verbose)

  # Import files to DuckDB
  imported_tables <- import_files_to_duckdb(connection, payment_files, verbose,
                                            force_reimport, file_pattern)

  # Determine which columns to include in the merged data
  key_columns <- get_key_columns(payment_type)

  # Create column mapping across years
  column_mapping <- create_column_mapping(connection,
                                          imported_tables$new_tables,
                                          imported_tables$existing_tables,
                                          key_columns,
                                          verbose)

  # Merge payment data from all years
  merged_table <- merge_payment_data(connection,
                                     imported_tables$new_tables,
                                     imported_tables$existing_tables,
                                     column_mapping,
                                     output_table_name,
                                     verbose)

  # Check for any missing tables
  table_names <- c(unlist(imported_tables$new_tables),
                   unlist(imported_tables$existing_tables),
                   merged_table)
  table_names <- table_names[!is.na(table_names)]

  # Verify tables exist
  existing_tables <- DBI::dbListTables(connection)
  missing_tables <- table_names[!table_names %in% existing_tables]

  # Report results
  if (length(missing_tables) == 0) {
    logger::log_info("✅ All {length(table_names)} tables were successfully created and are available.")
  } else {
    logger::log_error("❌ Missing tables detected: {paste(missing_tables, collapse = ', ')}")
    DBI::dbDisconnect(connection)
    stop("Some tables were not created successfully. Review the errors above.")
  }

  # Disconnect from database
  logger::log_info("Disconnecting from database")
  DBI::dbDisconnect(connection)

  # Return the merged table name
  return(merged_table)
}

#' Connect to DuckDB
#'
#' @param db_path Path to DuckDB database file
#' @param verbose Whether to print verbose output
#'
#' @return DuckDB connection
#' @noRd
connect_to_duckdb <- function(db_path, verbose) {
  tryCatch({
    connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
    logger::log_success("Connected to DuckDB database at {db_path}")
    return(connection)
  }, error = function(e) {
    logger::log_error("Failed to connect to DuckDB: {e$message}")
    stop("Database connection failed")
  })
}

#' Initialize database tables and schema
#'
#' @param connection DuckDB connection
#' @param verbose Whether to print verbose output
#'
#' @noRd
initialize_database <- function(connection, verbose) {
  logger::log_info("Initializing database tables and schema")

  # Check if metadata table exists
  tables <- DBI::dbListTables(connection)

  if (!"op_file_metadata" %in% tables) {
    logger::log_info("Creating file metadata table")

    create_metadata_table <- "
      CREATE TABLE op_file_metadata (
        filepath VARCHAR PRIMARY KEY,
        year VARCHAR,
        table_name VARCHAR,
        file_size_bytes BIGINT,
        file_hash VARCHAR,
        import_date TIMESTAMP,
        file_pattern VARCHAR
      )
    "

    DBI::dbExecute(connection, create_metadata_table)
    logger::log_success("Created file metadata table")
  }

  if (!"op_column_mappings" %in% tables) {
    logger::log_info("Creating column mappings table")

    create_mappings_table <- "
      CREATE TABLE op_column_mappings (
        year VARCHAR,
        standard_column VARCHAR,
        actual_column VARCHAR,
        PRIMARY KEY (year, standard_column)
      )
    "

    DBI::dbExecute(connection, create_mappings_table)
    logger::log_success("Created column mappings table")
  }
}

#' Find Open Payments files
#'
#' @param base_dir Base directory containing Open Payments files
#' @param years Years to find files for
#' @param file_pattern File pattern to match
#' @param verbose Whether to print verbose output
#'
#' @return Tibble of payment files
#' @noRd
find_payment_files <- function(base_dir, years, file_pattern, verbose) {
  logger::log_info("Finding Open Payments files in {base_dir}")

  # Find all files matching the pattern in the base directory and subdirectories
  all_files <- tryCatch({
    fs::dir_ls(base_dir, recurse = TRUE, regexp = file_pattern)
  }, error = function(e) {
    logger::log_error("Error finding files: {e$message}")
    return(character(0))
  })

  # If no files found, return empty tibble
  if (length(all_files) == 0) {
    logger::log_warn("No files found matching pattern: {file_pattern}")
    return(tibble::tibble(
      filepath = character(0),
      year = character(0),
      filesize_mb = numeric(0)
    ))
  }

  # Create a data frame with file information
  payment_files <- tibble::tibble(
    filepath = all_files,
    year = NA_character_,
    filesize_mb = as.numeric(fs::file_size(all_files)) / (1024 * 1024)
  )

  # Extract year from file paths
  # Common patterns in Open Payments files:
  # - PGYR2020_P062022 (Program Year 2020)
  # - RY2019 (Reporting Year 2019)
  # - _CY2018_ (Calendar Year 2018)

  # Try to extract years from filenames
  for (i in 1:nrow(payment_files)) {
    file_path <- payment_files$filepath[i]
    file_name <- basename(file_path)

    # Try different patterns
    year_match <- regmatches(file_name, regexpr("PGYR(20[0-9]{2})", file_name))
    if (length(year_match) == 0) {
      year_match <- regmatches(file_name, regexpr("RY(20[0-9]{2})", file_name))
    }
    if (length(year_match) == 0) {
      year_match <- regmatches(file_name, regexpr("CY(20[0-9]{2})", file_name))
    }
    if (length(year_match) == 0) {
      year_match <- regmatches(file_name, regexpr("(20[0-9]{2})", file_name))
    }

    if (length(year_match) > 0) {
      # Extract the year part
      year <- gsub("[^0-9]", "", year_match[1])
      payment_files$year[i] <- year
    }
  }

  # Filter by specified years if provided
  if (!is.null(years) && length(years) > 0) {
    payment_files <- dplyr::filter(payment_files, year %in% years)

    if (nrow(payment_files) > 0) {
      logger::log_info("Found {nrow(payment_files)} files for years: {paste(years, collapse = ', ')}")
    } else {
      logger::log_warn("No files found for specified years")
    }
  }

  return(payment_files)
}

#' Read specific NPI table
#'
#' @description
#' Reads and processes an NPI data table from a CSV file with comprehensive error handling.
#'
#' @param file_path Character string specifying the path to the CSV file.
#' @param column_types Optional list of column types for readr. If NULL, will be guessed.
#' @param verbose Logical, if TRUE (default) will log detailed information.
#'
#' @return A tibble containing the processed NPI data.
#'
#' @importFrom readr read_csv cols
#' @importFrom dplyr select mutate filter distinct
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_error log_debug log_warn
#'
#' @examples
#' # Basic usage with default parameters
#' npi_data <- read_npi_table(
#'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#'   verbose = TRUE
#' )
#' print(head(npi_data))
#'
#' # Specifying column types explicitly
#' npi_data <- read_npi_table(
#'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#'   column_types = readr::cols(
#'     NPI = readr::col_character(),
#'     Provider_Name = readr::col_character(),
#'     Score = readr::col_double()
#'   ),
#'   verbose = TRUE
#' )
#' print(nrow(npi_data))
#'
#' # Silent mode with minimal logging
#' npi_data <- read_npi_table(
#'   file_path = "data/MUP_DPR_RY23_P04_V10_DY22_NPI_csv_10.csv",
#'   verbose = FALSE
#' )
#' str(npi_data)
read_npi_table <- function(file_path, column_types = NULL, verbose = TRUE) {
  # Initialize logger
  logger::log_threshold(ifelse(verbose, logger::INFO, logger::ERROR))

  # Validate inputs
  assertthat::assert_that(is.character(file_path))
  assertthat::assert_that(length(file_path) == 1)
  assertthat::assert_that(is.logical(verbose))

  # Log function start
  logger::log_info("Starting to read NPI table from: {file_path}")

  # Check if file exists
  if (!file.exists(file_path)) {
    logger::log_error("File does not exist: {file_path}")
    stop("File not found: ", file_path)
  }

  # Attempt to read the file
  npi_table <- tryCatch({
    logger::log_debug("Attempting to read CSV file")

    # Try different reading methods
    try_reading_methods(file_path, column_types, verbose)

  }, error = function(e) {
    logger::log_error("All reading methods failed: {conditionMessage(e)}")
    stop("Failed to read NPI table: ", conditionMessage(e))
  })

  # Log success and return
  logger::log_info("Successfully processed NPI table with {nrow(npi_table)} rows and {ncol(npi_table)} columns")
  return(npi_table)
}

#' Try different methods to read the CSV file
#'
#' @param file_path Path to the CSV file
#' @param column_types Column types specification
#' @param verbose Whether to print verbose output
#'
#' @return Processed tibble
#' @noRd
try_reading_methods <- function(file_path, column_types, verbose) {
  # Method 1: Standard readr::read_csv
  npi_data <- tryCatch({
    if (verbose) logger::log_debug("Trying standard read_csv method")

    readr_table <- readr::read_csv(
      file = file_path,
      col_types = column_types,
      show_col_types = FALSE
    )

    validate_npi_data(readr_table, verbose)
    process_npi_data(readr_table, verbose)
  }, error = function(e) {
    if (verbose) logger::log_warn("Standard read_csv failed: {conditionMessage(e)}")
    NULL
  })

  if (!is.null(npi_data)) return(npi_data)

  # Method 2: Try with readr but different options
  npi_data <- tryCatch({
    if (verbose) logger::log_debug("Trying read_csv with different options")

    readr_table <- readr::read_csv(
      file = file_path,
      col_types = column_types,
      show_col_types = FALSE,
      lazy = FALSE,
      progress = FALSE,
      guess_max = 10000
    )

    validate_npi_data(readr_table, verbose)
    process_npi_data(readr_table, verbose)
  }, error = function(e) {
    if (verbose) logger::log_warn("Alternative read_csv failed: {conditionMessage(e)}")
    NULL
  })

  if (!is.null(npi_data)) return(npi_data)

  # Method 3: Try with base R read.csv
  npi_data <- tryCatch({
    if (verbose) logger::log_debug("Trying base R read.csv")

    base_table <- read.csv(
      file = file_path,
      stringsAsFactors = FALSE,
      check.names = FALSE
    )

    # Convert to tibble
    base_tibble <- tibble::as_tibble(base_table)

    validate_npi_data(base_tibble, verbose)
    process_npi_data(base_tibble, verbose)
  }, error = function(e) {
    if (verbose) logger::log_warn("Base R read.csv failed: {conditionMessage(e)}")
    NULL
  })

  if (!is.null(npi_data)) return(npi_data)

  # Method 4: Last resort with data.table
  if (requireNamespace("data.table", quietly = TRUE)) {
    npi_data <- tryCatch({
      if (verbose) logger::log_debug("Trying data.table fread")

      dt_table <- data.table::fread(
        file = file_path,
        data.table = FALSE,
        showProgress = FALSE
      )

      # Convert to tibble
      dt_tibble <- tibble::as_tibble(dt_table)

      validate_npi_data(dt_tibble, verbose)
      process_npi_data(dt_tibble, verbose)
    }, error = function(e) {
      if (verbose) logger::log_error("data.table fread failed: {conditionMessage(e)}")
      NULL
    })

    if (!is.null(npi_data)) return(npi_data)
  }

  # If all methods failed, throw an error
  stop("All reading methods failed for file: ", file_path)
}

#' Validate NPI data structure
#'
#' @param npi_data NPI data to validate
#' @param verbose Whether to print verbose output
#'
#' @return Validated data
#' @noRd
validate_npi_data <- function(npi_data, verbose) {
  if (verbose) logger::log_debug("Validating NPI data structure")

  # Check if data is a data frame
  assertthat::assert_that(is.data.frame(npi_data))

  # Check if the data has any rows
  if (nrow(npi_data) == 0) {
    logger::log_warn("NPI data has 0 rows")
  } else {
    logger::log_debug("NPI data has {nrow(npi_data)} rows")
  }

  # Check expected columns
  expected_columns <- c("NPI")
  missing_columns <- expected_columns[!expected_columns %in% names(npi_data)]

  if (length(missing_columns) > 0) {
    logger::log_warn("Missing expected columns: {paste(missing_columns, collapse=', ')}")
  } else if (verbose) {
    logger::log_debug("All expected columns present")
  }

  return(npi_data)
}

#' Process NPI data
#'
#' @param npi_data NPI data to process
#' @param verbose Whether to print verbose output
#'
#' @return Processed data
#' @noRd
process_npi_data <- function(npi_data, verbose) {
  if (verbose) logger::log_info("Processing NPI data")

  # Handle any transformations for the data
  processed_table <- npi_data

  # Example transformations (modify as needed for your actual data)
  if ("NPI" %in% names(processed_table)) {
    if (verbose) logger::log_debug("Ensuring NPI column is character type")
    processed_table <- dplyr::mutate(processed_table, NPI = as.character(NPI))
  }

  # Remove any duplicate rows
  original_rows <- nrow(processed_table)
  processed_table <- dplyr::distinct(processed_table)
  if (nrow(processed_table) < original_rows && verbose) {
    logger::log_info("Removed {original_rows - nrow(processed_table)} duplicate rows")
  }

  # Log completion
  if (verbose) logger::log_debug("Data processing complete")

  return(processed_table)
}


# Load required packages
library(DBI)
library(duckdb)
library(dplyr)
library(logger)
library(assertthat)
library(purrr)
library(fs)

# Define paths
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_merged.duckdb"

# Process general payment data for recent years
# Now add 2019-2020 data without reimporting 2021-2023
merged_data_all_years <- process_open_payments_data_optimized(
  base_dir = base_dir,
  db_path = db_path,
  # years = c("2019", "2020", "2021", "2022", "2023"),
  payment_type = "general",
  output_table_name = "op_general_all_years",
  verbose = TRUE
)


#####
# Improving R function robustness and modularity
#####

#' Process and Merge Open Payments Data Using DuckDB with Memory Optimization
#'
#' @param base_dir Character string. Path to base directory with Open Payments data.
#' @param db_path Character string. Path for DuckDB database file.
#' @param years Character vector. Years to process. If NULL, all years.
#' @param payment_type Character string. "general", "research", or "ownership". Default "general".
#' @param file_pattern Character string. Pattern to identify payment files. Default is auto-determined.
#' @param selected_columns Character vector. Columns to include. If NULL, uses default key columns.
#' @param output_table_name Character string. Name for merged table. Default "open_payments_merged".
#' @param save_csv Logical. Whether to save CSV. Default FALSE.
#' @param output_csv_path Character string. Path for CSV. Only if save_csv=TRUE.
#' @param verbose Logical. Whether to print detailed info. Default TRUE.
#' @param force_reimport Logical. Whether to force reimport. Default FALSE.
#'
#' @return List with DuckDB connection and table name.
#'
#' @importFrom DBI dbConnect dbExecute dbDisconnect dbListTables dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom dplyr tbl filter mutate pull select collect arrange
#' @importFrom logger log_info log_success log_error log_debug log_warn
#' @importFrom assertthat assert_that
#' @importFrom purrr map_dfr map
#' @importFrom fs file_size dir_ls
#' @importFrom tibble tibble as_tibble
#' @importFrom tools md5sum
#'
#' @export
process_open_payments_data_optimized <- function(base_dir,
                                                 db_path,
                                                 years = NULL,
                                                 payment_type = "general",
                                                 file_pattern = NULL,
                                                 selected_columns = NULL,
                                                 output_table_name = "open_payments_merged",
                                                 save_csv = FALSE,
                                                 output_csv_path = NULL,
                                                 verbose = TRUE,
                                                 force_reimport = FALSE) {
  # Set up logging
  logger::log_layout(logger::layout_simple)
  log_level <- if (verbose) logger::DEBUG else logger::INFO
  logger::log_threshold(log_level)

  # Validate inputs - NOTE: Fixed by including force_reimport here
  validate_inputs(base_dir, db_path, years, payment_type, file_pattern,
                  output_table_name, save_csv, output_csv_path, verbose, force_reimport)

  # Determine file pattern if not provided
  if (is.null(file_pattern)) {
    type_pattern <- switch(payment_type,
                           "general" = "GNRL",
                           "research" = "RSRCH",
                           "ownership" = "OWNRSHP",
                           "GNRL")
    file_pattern <- paste0("OP_DTL_", type_pattern, "_PGYR\\d{4}.*\\.csv$")
  }

  # Log parameters
  logger::log_info("Starting Open Payments data processing")

  # Check database file permissions
  check_db_permissions(db_path)

  # Set up database with read_only=FALSE
  db_conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  logger::log_success("Successfully connected to DuckDB")

  # Configure for memory efficiency
  DBI::dbExecute(db_conn, "PRAGMA memory_limit='4GB'")
  DBI::dbExecute(db_conn, "PRAGMA threads=2")
  DBI::dbExecute(db_conn, "PRAGMA force_compression='ZSTD'")

  # Initialize tables
  initialize_database_tables(db_conn, verbose)

  # Find payment files
  payment_files <- find_payment_files(base_dir, file_pattern, years, verbose)
  if (nrow(payment_files) == 0) {
    logger::log_error("No payment files found. Process aborted.")
    DBI::dbDisconnect(db_conn)
    return(NULL)
  }

  # Check existing imports
  if (!force_reimport) {
    payment_files <- check_imported_files(db_conn, payment_files, verbose)
  } else {
    logger::log_info("Force reimport is enabled. All files will be reimported.")
    payment_files$already_imported <- FALSE
    payment_files$reimport <- TRUE
    payment_files$existing_table <- NA_character_
  }

  # Import files in batches to reduce memory usage
  imported_tables <- import_files_to_duckdb_batched(
    db_conn, payment_files, verbose, force_reimport, file_pattern
  )

  # Explicitly run garbage collection to free memory after imports
  gc()

  # Get columns and mappings
  if (is.null(selected_columns)) {
    selected_columns <- get_key_columns(payment_type)
  }

  all_years <- unique(c(names(imported_tables$new_tables), names(imported_tables$existing_tables)))
  column_mapping <- create_column_mapping(
    db_conn, imported_tables$new_tables, imported_tables$existing_tables,
    selected_columns, verbose
  )

  # Merge data with chunking for memory efficiency
  merged_table <- merge_payment_data_chunked(
    db_conn, imported_tables$new_tables, imported_tables$existing_tables,
    column_mapping, output_table_name, verbose
  )

  # Free memory
  gc()

  # Export if requested
  if (save_csv && !is.null(output_csv_path)) {
    export_to_csv_chunked(db_conn, output_table_name, output_csv_path, verbose)
  }

  # Return result
  result <- list(connection = db_conn, table_name = output_table_name)
  logger::log_success("Open Payments data processing completed successfully")

  return(result)
}

#' Check database permissions
#' @param db_path Path to the database
#' @noRd
check_db_permissions <- function(db_path) {
  db_dir <- dirname(db_path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE)
  }

  if (file.exists(db_path) && file.access(db_path, 2) != 0) {
    stop("Database file exists but is not writable: ", db_path)
  }
}

#' Validate input parameters - Fixed version with force_reimport
#' @noRd
validate_inputs <- function(base_dir, db_path, years, payment_type, file_pattern,
                            output_table_name, save_csv, output_csv_path, verbose,
                            force_reimport) {
  logger::log_debug("Validating input parameters")

  # Base validations
  assertthat::assert_that(is.character(base_dir),
                          msg = "base_dir must be a character string")
  assertthat::assert_that(dir.exists(base_dir),
                          msg = paste0("Directory not found: ", base_dir))
  assertthat::assert_that(is.character(db_path),
                          msg = "db_path must be a character string")

  # Payment type validation
  assertthat::assert_that(is.character(payment_type),
                          msg = "payment_type must be a character string")
  assertthat::assert_that(payment_type %in% c("general", "research", "ownership"),
                          msg = "payment_type must be one of: general, research, ownership")

  # File pattern validation
  if (!is.null(file_pattern)) {
    assertthat::assert_that(is.character(file_pattern),
                            msg = "file_pattern must be a character string")
  }

  # Output table validation
  assertthat::assert_that(is.character(output_table_name),
                          msg = "output_table_name must be a character string")

  # Years validation
  if (!is.null(years)) {
    assertthat::assert_that(is.character(years),
                            msg = "years must be a character vector")
  }

  # CSV validation
  assertthat::assert_that(is.logical(save_csv),
                          msg = "save_csv must be a logical value")
  if (save_csv) {
    assertthat::assert_that(!is.null(output_csv_path),
                            msg = "output_csv_path must be provided when save_csv is TRUE")
    assertthat::assert_that(is.character(output_csv_path),
                            msg = "output_csv_path must be a character string")
  }

  # Verbose and force_reimport validations
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be a logical value")
  assertthat::assert_that(is.logical(force_reimport),
                          msg = "force_reimport must be a logical value")

  logger::log_debug("Input validation completed")
}

#' Initialize database tables with optimized schema
#' @noRd
initialize_database_tables <- function(connection, verbose) {
  logger::log_info("Initializing database tables")

  tables <- DBI::dbListTables(connection)

  # Create metadata table if needed
  if (!"op_file_metadata" %in% tables) {
    metadata_query <- paste0(
      "CREATE TABLE op_file_metadata (",
      "filepath VARCHAR PRIMARY KEY, ",
      "year VARCHAR, ",
      "table_name VARCHAR, ",
      "file_size_bytes BIGINT, ",
      "file_hash VARCHAR, ",
      "import_date TIMESTAMP, ",
      "file_pattern VARCHAR",
      ")"
    )
    DBI::dbExecute(connection, metadata_query)
    logger::log_success("Created metadata table")
  } else {
    # Check for BIGINT column type
    schema_query <- "PRAGMA table_info('op_file_metadata')"
    schema_info <- DBI::dbGetQuery(connection, schema_query)
    size_col_info <- schema_info[schema_info$name == "file_size_bytes", ]

    if (nrow(size_col_info) > 0 && size_col_info$type == "INTEGER") {
      # Update schema to BIGINT
      update_schema_query <- paste0(
        "CREATE TABLE op_file_metadata_new (",
        "filepath VARCHAR PRIMARY KEY, ",
        "year VARCHAR, ",
        "table_name VARCHAR, ",
        "file_size_bytes BIGINT, ",
        "file_hash VARCHAR, ",
        "import_date TIMESTAMP, ",
        "file_pattern VARCHAR",
        ")"
      )
      DBI::dbExecute(connection, update_schema_query)
      DBI::dbExecute(connection, "INSERT INTO op_file_metadata_new SELECT filepath, year, table_name, CAST(file_size_bytes AS BIGINT), file_hash, import_date, file_pattern FROM op_file_metadata")
      DBI::dbExecute(connection, "DROP TABLE op_file_metadata")
      DBI::dbExecute(connection, "ALTER TABLE op_file_metadata_new RENAME TO op_file_metadata")
    }
  }

  # Create column mappings table if needed
  if (!"op_column_mappings" %in% tables) {
    mapping_query <- paste0(
      "CREATE TABLE op_column_mappings (",
      "year VARCHAR, ",
      "standard_column VARCHAR, ",
      "actual_column VARCHAR, ",
      "PRIMARY KEY (year, standard_column)",
      ")"
    )
    DBI::dbExecute(connection, mapping_query)
    logger::log_success("Created column mappings table")
  }
}

#' Find payment files with optimized search
#' @noRd
find_payment_files <- function(base_dir, file_pattern, years, verbose) {
  logger::log_info(paste0("Searching for files matching: ", file_pattern))

  all_files <- fs::dir_ls(path = base_dir, recurse = TRUE, regexp = file_pattern, type = "file")

  if (length(all_files) == 0) {
    return(tibble::tibble(filepath = character(0), year = character(0), filesize_mb = numeric(0)))
  }

  # Process in batches for memory efficiency
  batch_size <- 100
  num_batches <- ceiling(length(all_files) / batch_size)
  result_list <- list()

  for (i in 1:num_batches) {
    start_idx <- (i-1) * batch_size + 1
    end_idx <- min(i * batch_size, length(all_files))
    batch_files <- all_files[start_idx:end_idx]

    # Get file info
    file_sizes <- as.numeric(fs::file_size(batch_files))
    batch_data <- tibble::tibble(
      filepath = as.character(batch_files),
      year = NA_character_,
      filesize_mb = file_sizes / (1024 * 1024)
    )

    # Extract year from filename
    for (j in 1:nrow(batch_data)) {
      filename <- basename(batch_data$filepath[j])
      year_match <- regexpr("PGYR(\\d{4})", filename)
      if (year_match > 0) {
        batch_data$year[j] <- substr(filename, year_match + 4, year_match + 7)
      }
    }

    result_list[[i]] <- batch_data

    # Free memory
    rm(batch_data, file_sizes, batch_files)
    gc()
  }

  # Combine results
  payment_files <- do.call(rbind, result_list)

  # Filter for years if provided
  if (!is.null(years) && length(years) > 0) {
    payment_files <- dplyr::filter(payment_files, year %in% years)
  }

  # Sort by year
  payment_files <- dplyr::arrange(payment_files, year)

  # Log summary
  if (nrow(payment_files) > 0) {
    total_size_mb <- sum(payment_files$filesize_mb)
    logger::log_info(paste0("Found ", nrow(payment_files), " files totaling ",
                            round(total_size_mb, 2), " MB"))
  } else {
    logger::log_warn("No files found for specified years")
  }

  return(payment_files)
}

#' Check imported files with memory optimization
#' @noRd
check_imported_files <- function(connection, payment_files, verbose) {
  logger::log_info("Checking which files are already imported")

  # Initialize status columns
  payment_files$already_imported <- FALSE
  payment_files$reimport <- FALSE
  payment_files$existing_table <- NA_character_

  if (nrow(payment_files) == 0) {
    return(payment_files)
  }

  # Process in batches to avoid large IN clauses
  batch_size <- 50
  num_batches <- ceiling(nrow(payment_files) / batch_size)

  for (i in 1:num_batches) {
    start_idx <- (i-1) * batch_size + 1
    end_idx <- min(i * batch_size, nrow(payment_files))
    batch_files <- payment_files$filepath[start_idx:end_idx]

    files_to_check <- paste0("'", batch_files, "'", collapse = ",")
    query <- paste0("SELECT filepath, year, table_name, file_size_bytes, file_hash FROM op_file_metadata ",
                    "WHERE filepath IN (", files_to_check, ")")

    imported_files <- tryCatch({
      DBI::dbGetQuery(connection, query)
    }, error = function(e) {
      logger::log_warn(paste0("Error checking batch ", i, ": ", e$message))
      return(data.frame())
    })

    if (nrow(imported_files) > 0) {
      # Check files in this batch
      for (j in 1:nrow(imported_files)) {
        file_path <- imported_files$filepath[j]
        idx <- which(payment_files$filepath == file_path)

        if (length(idx) > 0) {
          current_size <- as.numeric(fs::file_size(file_path))
          db_size <- imported_files$file_size_bytes[j]

          if (current_size != db_size) {
            payment_files$reimport[idx] <- TRUE
          } else {
            payment_files$already_imported[idx] <- TRUE
            payment_files$existing_table[idx] <- imported_files$table_name[j]
          }
        }
      }
    }

    # Free memory
    rm(imported_files, batch_files)
    gc()
  }

  # Count files
  files_to_import <- sum(!payment_files$already_imported | payment_files$reimport)
  files_to_reuse <- sum(payment_files$already_imported & !payment_files$reimport)
  logger::log_info(paste0(files_to_import, " files need to be imported, ",
                          files_to_reuse, " can be reused"))

  return(payment_files)
}

#' Import files to DuckDB with batched processing
#' @noRd
import_files_to_duckdb_batched <- function(connection, payment_files, verbose, force_reimport, file_pattern) {
  logger::log_info("Importing files to DuckDB with batched processing")

  new_tables <- list()
  existing_tables <- list()

  # Identify files that need to be imported
  files_to_import <- payment_files[!payment_files$already_imported | payment_files$reimport | force_reimport, ]
  files_already_imported <- payment_files[payment_files$already_imported & !payment_files$reimport & !force_reimport, ]

  if (nrow(files_to_import) > 0) {
    for (i in 1:nrow(files_to_import)) {
      file_info <- files_to_import[i, ]
      filepath <- file_info$filepath
      year <- file_info$year
      filesize_mb <- file_info$filesize_mb

      table_name <- paste0("op_temp_", year)

      # Check if table exists
      tables <- DBI::dbListTables(connection)
      if (table_name %in% tables) {
        DBI::dbExecute(connection, paste0("DROP TABLE ", table_name))
      }

      logger::log_info(paste0("Importing file ", i, "/", nrow(files_to_import),
                              ": ", basename(filepath), " (", round(filesize_mb, 2), " MB)"))

      # Use memory-efficient import method
      import_success <- import_file_in_chunks(connection, filepath, table_name, verbose)

      if (import_success) {
        new_tables[[year]] <- table_name

        # Update metadata
        file_hash <- digest::digest(filepath)
        file_size <- as.numeric(fs::file_size(filepath))

        upsert_query <- paste0(
          "INSERT OR REPLACE INTO op_file_metadata ",
          "(filepath, year, table_name, file_size_bytes, file_hash, import_date, file_pattern) ",
          "VALUES ('", filepath, "', '", year, "', '", table_name, "', ",
          file_size, ", '", file_hash, "', CURRENT_TIMESTAMP, '",
          file_pattern, "')"
        )

        tryCatch({
          DBI::dbExecute(connection, upsert_query)
        }, error = function(e) {
          logger::log_error(paste0("Failed to update metadata: ", e$message))
        })
      }

      # Free memory
      gc()
    }
  }

  # Process already imported files
  if (nrow(files_already_imported) > 0) {
    logger::log_info(paste0("Using ", nrow(files_already_imported), " previously imported tables"))

    for (i in 1:nrow(files_already_imported)) {
      year <- files_already_imported$year[i]
      table_name <- files_already_imported$existing_table[i]

      tables <- DBI::dbListTables(connection)
      if (table_name %in% tables) {
        existing_tables[[year]] <- table_name
      }
    }
  }

  return(list(new_tables = new_tables, existing_tables = existing_tables))
}

#' Import file in chunks to reduce memory usage
#' @noRd
import_file_in_chunks <- function(connection, filepath, table_name, verbose) {
  # Create table schema with a sample
  schema_query <- paste0(
    "CREATE TABLE ", table_name, " AS ",
    "SELECT * FROM read_csv_auto('", filepath, "', ",
    "all_varchar=TRUE, sample_size=1000, header=TRUE) WHERE 1=0"
  )

  tryCatch({
    # Create empty table with schema
    DBI::dbExecute(connection, schema_query)
    logger::log_success("Created table schema")

    # Use efficient COPY FROM command
    copy_query <- paste0(
      "COPY ", table_name, " FROM '", filepath, "' ",
      "(AUTO_DETECT TRUE, HEADER TRUE, IGNORE_ERRORS TRUE)"
    )

    tryCatch({
      DBI::dbExecute(connection, copy_query)

      # Count rows
      count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
      count_result <- DBI::dbGetQuery(connection, count_query)
      row_count <- count_result$row_count[1]

      logger::log_success(paste0("Successfully imported ", row_count, " rows"))
      return(TRUE)

    }, error = function(e) {
      logger::log_warn(paste0("COPY command failed: ", e$message))

      # Alternative using INSERT with read_csv_auto for each chunk
      # DuckDB will automatically handle the chunking internally
      insert_query <- paste0(
        "INSERT INTO ", table_name, " ",
        "SELECT * FROM read_csv_auto('", filepath, "', ",
        "all_varchar=TRUE, header=TRUE, ignore_errors=TRUE)"
      )

      tryCatch({
        DBI::dbExecute(connection, insert_query)

        # Count rows
        count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
        count_result <- DBI::dbGetQuery(connection, count_query)
        row_count <- count_result$row_count[1]

        logger::log_success(paste0("Successfully imported ", row_count, " rows using INSERT"))
        return(TRUE)

      }, error = function(e2) {
        logger::log_error(paste0("INSERT method also failed: ", e2$message))

        # Last resort: chunk manually in R
        manual_import_success <- manual_chunk_import(connection, filepath, table_name, verbose)
        return(manual_import_success)
      })
    })

  }, error = function(e) {
    logger::log_error(paste0("Failed to create schema: ", e$message))
    return(FALSE)
  })
}

#' Manual chunked import as last resort
#' @noRd
manual_chunk_import <- function(connection, filepath, table_name, verbose) {
  logger::log_info("Using manual chunking as last resort")

  # Read header to get schema
  header_conn <- file(filepath, "r")
  header <- readLines(header_conn, n = 1)
  close(header_conn)

  # Create temporary file with just the header
  header_file <- tempfile(fileext = ".csv")
  writeLines(header, header_file)

  # Create table schema
  schema_query <- paste0(
    "CREATE TABLE IF NOT EXISTS ", table_name, " AS ",
    "SELECT * FROM read_csv_auto('", header_file, "', header=TRUE, all_varchar=TRUE) ",
    "WHERE 1=0"
  )

  DBI::dbExecute(connection, schema_query)

  # Get column names
  cols_query <- paste0("SELECT * FROM ", table_name, " LIMIT 0")
  cols_result <- DBI::dbGetQuery(connection, cols_query)
  col_names <- colnames(cols_result)

  # Use data.table fread for chunking
  chunk_size <- 50000
  total_rows <- 0

  # Use R file connection for reading efficiency
  con <- file(filepath, "r")
  readLines(con, n = 1)  # Skip header

  repeat {
    chunk_data <- readLines(con, n = chunk_size)

    if (length(chunk_data) == 0) {
      break  # End of file
    }

    # Create temp file for this chunk
    temp_file <- tempfile(fileext = ".csv")
    writeLines(c(header, chunk_data), temp_file)

    # Import chunk
    chunk_query <- paste0(
      "INSERT INTO ", table_name, " SELECT * FROM read_csv_auto('",
      temp_file, "', header=TRUE, all_varchar=TRUE, ignore_errors=TRUE)"
    )

    tryCatch({
      DBI::dbExecute(connection, chunk_query)
      total_rows <- total_rows + length(chunk_data)

      if (verbose) {
        logger::log_debug(paste0("Imported ", total_rows, " rows so far"))
      }
    }, error = function(e) {
      logger::log_warn(paste0("Error importing chunk: ", e$message))
    })

    # Clean up
    unlink(temp_file)
    rm(chunk_data)
    gc()
  }

  close(con)
  unlink(header_file)

  logger::log_success(paste0("Manual import completed with approximately ", total_rows, " rows"))
  return(total_rows > 0)
}

#' Create column mapping with memory optimization
#' @noRd
create_column_mapping <- function(connection, new_tables, existing_tables, selected_columns, verbose) {
  logger::log_info("Creating column mapping across years")

  column_mapping <- list()
  all_years <- unique(c(names(new_tables), names(existing_tables)))

  # Common column variations
  column_variations <- list(
    "Physician_Profile_ID" = c("Physician_Profile_ID", "Covered_Recipient_Profile_ID", "Physician_ID"),
    "Physician_First_Name" = c("Physician_First_Name", "Covered_Recipient_First_Name", "First_Name"),
    "Physician_Last_Name" = c("Physician_Last_Name", "Covered_Recipient_Last_Name", "Last_Name"),
    "Recipient_State" = c("Recipient_State", "State", "Physician_State")
  )

  # Check for cached mappings
  existing_mappings <- get_cached_mappings(connection, all_years)

  # Process each year
  for (year in all_years) {
    if (year %in% names(existing_mappings)) {
      column_mapping[[year]] <- existing_mappings[[year]]
      next
    }

    # Get table name
    table_name <- NULL
    if (year %in% names(new_tables)) {
      table_name <- new_tables[[year]]
    } else if (year %in% names(existing_tables)) {
      table_name <- existing_tables[[year]]
    }

    if (is.null(table_name)) {
      next
    }

    # Get available columns
    query <- paste0("SELECT * FROM ", table_name, " LIMIT 1")
    sample_data <- DBI::dbGetQuery(connection, query)
    available_columns <- colnames(sample_data)

    # Map columns
    year_mapping <- list()

    for (std_col in selected_columns) {
      # Direct match
      if (std_col %in% available_columns) {
        year_mapping[[std_col]] <- std_col
        next
      }

      # Try variations
      if (std_col %in% names(column_variations)) {
        variations <- column_variations[[std_col]]
        for (var in variations) {
          if (var %in% available_columns) {
            year_mapping[[std_col]] <- var
            break
          }
        }
      }

      # Case-insensitive match as fallback
      if (is.null(year_mapping[[std_col]])) {
        lower_std_col <- tolower(std_col)
        lower_available <- tolower(available_columns)
        match_idx <- which(lower_available == lower_std_col)
        if (length(match_idx) > 0) {
          year_mapping[[std_col]] <- available_columns[match_idx[1]]
        }
      }
    }

    column_mapping[[year]] <- year_mapping

    # Cache mapping
    cache_mapping(connection, year, year_mapping)
  }

  return(column_mapping)
}

#' Get cached column mappings
#' @noRd
get_cached_mappings <- function(connection, years) {
  if (length(years) == 0) {
    return(list())
  }

  years_str <- paste0("'", years, "'", collapse = ",")
  query <- paste0("SELECT year, standard_column, actual_column FROM op_column_mappings ",
                  "WHERE year IN (", years_str, ")")

  mappings <- tryCatch({
    DBI::dbGetQuery(connection, query)
  }, error = function(e) {
    return(data.frame())
  })

  if (nrow(mappings) == 0) {
    return(list())
  }

  # Convert to nested list
  result <- list()
  for (year in unique(mappings$year)) {
    year_mappings <- mappings[mappings$year == year, ]
    result[[year]] <- setNames(
      as.list(year_mappings$actual_column),
      year_mappings$standard_column
    )
  }

  return(result)
}

#' Cache column mapping
#' @noRd
cache_mapping <- function(connection, year, mapping) {
  # Delete existing mappings
  delete_query <- paste0("DELETE FROM op_column_mappings WHERE year = '", year, "'")
  DBI::dbExecute(connection, delete_query)

  # Insert new mappings in batches
  batch_size <- 10
  std_cols <- names(mapping)
  num_batches <- ceiling(length(std_cols) / batch_size)

            for (i in 1:num_batches) {
              start_idx <- (i-1) * batch_size + 1
              end_idx <- min(i * batch_size, length(std_cols))
              batch_cols <- std_cols[start_idx:end_idx]

              for (std_col in batch_cols) {
                actual_col <- mapping[[std_col]]
                if (!is.null(actual_col)) {
                  insert_query <- paste0(
                    "INSERT INTO op_column_mappings (year, standard_column, actual_column) ",
                    "VALUES ('", year, "', '", std_col, "', '", actual_col, "')"
                  )
                  DBI::dbExecute(connection, insert_query)
                }
              }
            }
          }

          #' Merge payment data with chunking for memory efficiency
          #' @noRd
          merge_payment_data_chunked <- function(connection, new_tables, existing_tables,
                                                 column_mapping, output_table_name, verbose) {
            logger::log_info(paste0("Merging data into table: ", output_table_name))

            # Check if output table exists
            tables <- DBI::dbListTables(connection)
            if (output_table_name %in% tables) {
              DBI::dbExecute(connection, paste0("DROP TABLE ", output_table_name))
            }

            # Get all years and columns
            all_years <- unique(c(names(new_tables), names(existing_tables)))
            all_std_cols <- unique(unlist(lapply(column_mapping, names)))

            # Create empty output table
            all_cols <- c(all_std_cols, "Source_Table")
            col_defs <- paste0('"', all_cols, '" VARCHAR', collapse = ", ")
            create_query <- paste0("CREATE TABLE ", output_table_name, " (", col_defs, ")")

            tryCatch({
              DBI::dbExecute(connection, create_query)

              # Process each year in chunks
              for (year in all_years) {
                table_name <- NULL
                if (year %in% names(new_tables)) {
                  table_name <- new_tables[[year]]
                } else if (year %in% names(existing_tables)) {
                  table_name <- existing_tables[[year]]
                }

                if (is.null(table_name) || is.null(column_mapping[[year]])) {
                  next
                }

                # Get row count
                count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
                count_result <- DBI::dbGetQuery(connection, count_query)
                total_rows <- count_result$row_count[1]

                # Set chunk size
                chunk_size <- 100000
                num_chunks <- ceiling(total_rows / chunk_size)

                logger::log_info(paste0("Merging ", total_rows, " rows from year ", year,
                                        " in ", num_chunks, " chunks"))

                # Process each chunk
                for (chunk in 1:num_chunks) {
                  offset <- (chunk - 1) * chunk_size

                  # Build mapping for INSERT
                  mapping <- column_mapping[[year]]
                  output_cols <- paste0('"', all_cols, '"', collapse = ", ")

                  # Build select clause
                  select_parts <- c()
                  for (std_col in all_std_cols) {
                    org_col <- NULL
                    if (std_col %in% names(mapping)) {
                      org_col <- mapping[[std_col]]
                    }

                    if (!is.null(org_col)) {
                      select_parts <- c(select_parts, paste0('"', org_col, '" AS "', std_col, '"'))
                    } else if (std_col == "Program_Year" &&
                               (!"Program_Year" %in% names(mapping) || is.null(mapping[["Program_Year"]]))) {
                      select_parts <- c(select_parts, paste0("'", year, "' AS \"Program_Year\""))
                    } else {
                      select_parts <- c(select_parts, paste0('NULL AS "', std_col, '"'))
                    }
                  }

                  # Add Source_Table
                  select_parts <- c(select_parts, paste0("'", table_name, "' AS \"Source_Table\""))

                  # Build INSERT query
                  insert_query <- paste0(
                    "INSERT INTO ", output_table_name, " (", output_cols, ") ",
                    "SELECT ", paste(select_parts, collapse = ", "),
                    " FROM ", table_name,
                    " LIMIT ", chunk_size, " OFFSET ", offset
                  )

                  # Execute INSERT
                  tryCatch({
                    DBI::dbExecute(connection, insert_query)
                    if (verbose || chunk %% 10 == 0 || chunk == num_chunks) {
                      logger::log_debug(paste0("Merged chunk ", chunk, "/", num_chunks,
                                               " for year ", year))
                    }
                  }, error = function(e) {
                    logger::log_error(paste0("Error merging chunk ", chunk, ": ", e$message))
                  })

                  # Free memory
                  gc()
                }
              }

              # Get total row count
              count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", output_table_name)
              count_result <- DBI::dbGetQuery(connection, count_query)
              total_rows <- count_result$row_count[1]

              logger::log_success(paste0("Successfully merged ", total_rows, " rows into ",
                                         output_table_name))
              return(output_table_name)

            }, error = function(e) {
              logger::log_error(paste0("Failed to merge data: ", e$message))
              return(NULL)
            })
          }

          #' Export to CSV with chunking for memory efficiency
          #' @noRd
          export_to_csv_chunked <- function(connection, table_name, output_path, verbose) {
            logger::log_info(paste0("Exporting to CSV: ", output_path))

            # Ensure directory exists
            output_dir <- dirname(output_path)
            if (!dir.exists(output_dir)) {
              dir.create(output_dir, recursive = TRUE)
            }

            # Try DuckDB's COPY TO first (most efficient)
            tryCatch({
              export_query <- paste0(
                "COPY (SELECT * FROM ", table_name, ") TO '", output_path,
                "' (DELIMITER ',', HEADER, FORMAT CSV)"
              )

              DBI::dbExecute(connection, export_query)
              logger::log_success(paste0("Successfully exported to ", output_path))
              return(TRUE)

            }, error = function(e) {
              logger::log_warn(paste0("Direct export failed: ", e$message,
                                      ". Switching to chunked export."))

              # Get row count
              count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
              count_result <- DBI::dbGetQuery(connection, count_query)
              total_rows <- count_result$row_count[1]

              # Set chunk size
              chunk_size <- 100000
              num_chunks <- ceiling(total_rows / chunk_size)

              logger::log_info(paste0("Exporting ", total_rows, " rows in ", num_chunks, " chunks"))

              # Get column names
              cols_query <- paste0("SELECT * FROM ", table_name, " LIMIT 1")
              cols_result <- DBI::dbGetQuery(connection, cols_query)
              column_names <- colnames(cols_result)

              # Open file connection
              file_conn <- file(output_path, "w")

              # Write header
              write.table(data.frame(t(column_names)), file = file_conn,
                          row.names = FALSE, col.names = FALSE, sep = ",", quote = TRUE)

              # Process each chunk
              for (chunk in 1:num_chunks) {
                offset <- (chunk - 1) * chunk_size

                query <- paste0(
                  "SELECT * FROM ", table_name,
                  " LIMIT ", chunk_size, " OFFSET ", offset
                )

                # Get chunk data
                chunk_data <- DBI::dbGetQuery(connection, query)

                # Write to file (without header)
                write.table(chunk_data, file = file_conn, row.names = FALSE,
                            col.names = FALSE, sep = ",", quote = TRUE, append = TRUE)

                if (verbose || chunk %% 10 == 0 || chunk == num_chunks) {
                  progress <- min((chunk * chunk_size) / total_rows * 100, 100)
                  logger::log_debug(paste0("Exported chunk ", chunk, "/", num_chunks,
                                           " (", round(progress, 1), "%)"))
                }

                # Free memory
                rm(chunk_data)
                gc()
              }

              # Close file
              close(file_conn)

              logger::log_success(paste0("Successfully exported ", total_rows,
                                         " rows to ", output_path))
              return(TRUE)
            })
          }

          #' Get key columns based on payment type
          #' @noRd
          get_key_columns <- function(payment_type) {
            if (payment_type == "general") {
              return(c(
                "Physician_Profile_ID",
                "Physician_First_Name",
                "Physician_Last_Name",
                "Recipient_State",
                "Total_Amount_of_Payment_USDollars",
                "Date_of_Payment",
                "Nature_of_Payment_or_Transfer_of_Value",
                "Program_Year"
              ))
            } else if (payment_type == "research") {
              return(c(
                "Physician_Profile_ID",
                "Physician_First_Name",
                "Physician_Last_Name",
                "Recipient_State",
                "Research_Institution_Name",
                "Total_Amount_of_Payment_USDollars",
                "Date_of_Payment",
                "Program_Year"
              ))
            } else if (payment_type == "ownership") {
              return(c(
                "Physician_Profile_ID",
                "Physician_First_Name",
                "Physician_Last_Name",
                "Recipient_State",
                "Total_Amount_Invested_USDollars",
                "Value_of_Interest",
                "Program_Year"
              ))
            } else {
              # Default minimal set
              return(c(
                "Physician_Profile_ID",
                "Physician_First_Name",
                "Physician_Last_Name",
                "Total_Amount_of_Payment_USDollars",
                "Program_Year"
              ))
            }
          }

          base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
          db_path <- "/Volumes/Video Projects Muffly 1/open_payments_merged.duckdb"

          result <- process_open_payments_data_optimized(
            base_dir = base_dir,
            db_path = db_path,
            years = NULL,
            payment_type = "general",
            output_table_name = "open_payments_merged",
            verbose = TRUE,
            force_reimport = FALSE
          )

########
# Check the results
########
          # Load necessary libraries
          library(DBI)
          library(duckdb)
          library(dplyr)

          # Connect to the correct DuckDB file
          con <- DBI::dbConnect(
            duckdb::duckdb(),
            dbdir = "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb",
            read_only = FALSE
          )

          # Check if the table exists
          DBI::dbListTables(con)

          # View the first 100 rows from the open_payments_merged table
          open_payments_preview <- DBI::dbGetQuery(con, "SELECT * FROM open_payments_merged LIMIT 100")

          # Display the preview
          print(open_payments_preview)

          # Dsconnect when done
          dbDisconnect(con, shutdown = TRUE)

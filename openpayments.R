library(duckdb)
library(logger)
library(tidyverse)
library(DBI)
library(glue)

#' Process Open Payments Data with Memory Management
#'
#' @description
#' Imports, processes, and merges Open Payments data from CSV files into a DuckDB database
#' with optimized memory management to prevent crashes with large datasets.
#' @param base_dir Character string specifying the directory containing Open Payments CSV files.
#' @param db_path Character string specifying the path for the DuckDB database file.
#' @param years Numeric vector of years to process (e.g., c(2020, 2021)), or NULL to process all available years.
#' @param payment_type Character string specifying the payment type to process: "general", "research", or "ownership".
#' @param output_table_name Character string specifying the name for the final merged output table.
#' @param verbose Logical indicating whether to display detailed logging information.
#' @param force_reimport Logical indicating whether to reimport files even if they've been previously imported.
#' @param preserve_all_columns Logical indicating whether to preserve all columns from the original files.
#' @param max_rows Numeric value specifying the maximum number of rows to import from each file (NULL for all rows).
#' @param memory_limit Numeric value specifying DuckDB memory limit in GB (default 3).
#' @param batch_size Numeric value specifying the batch size for processing (default 50000).
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
#' # Process all general payments with memory optimization
#' payments_data <- process_open_payments_data_memopt(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/output/database.duckdb",
#'   payment_type = "general",
#'   output_table_name = "open_payments_merged",
#'   verbose = TRUE,
#'   force_reimport = FALSE,
#'   preserve_all_columns = TRUE,
#'   max_rows = NULL,
#'   memory_limit = 3,
#'   batch_size = 50000
#' )
#'
#' # Process specific years with reduced memory usage
#' limited_data <- process_open_payments_data_memopt(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "/path/to/output/database.duckdb",
#'   years = c(2020, 2021, 2022),
#'   payment_type = "research",
#'   output_table_name = "research_payments_recent",
#'   verbose = TRUE,
#'   force_reimport = FALSE,
#'   preserve_all_columns = FALSE,
#'   memory_limit = 2,
#'   batch_size = 10000
#' )
#'
#' # Process with minimal memory usage for very large files
#' ownership_data <- process_open_payments_data_memopt(
#'   base_dir = "/path/to/open_payments/files",
#'   db_path = "ownership_payments.duckdb",
#'   payment_type = "ownership",
#'   memory_limit = 1
#' )
#'
#' @export
process_open_payments_data_memopt <- function(base_dir,
                                              db_path,
                                              years = NULL,
                                              payment_type = "general",
                                              output_table_name = "open_payments_merged",
                                              verbose = TRUE,
                                              force_reimport = FALSE,
                                              preserve_all_columns = TRUE,
                                              max_rows = NULL,
                                              memory_limit = 3,
                                              batch_size = 50000) {
  
  # Force garbage collection before starting
  gc(full = TRUE, reset = TRUE)
  
  # Setup logger based on verbose parameter
  setup_open_payments_logger(verbose)
  
  # Validate input parameters
  logger::log_debug("Validating input parameters")
  validate_open_payments_inputs_memopt(
    base_dir, 
    db_path, 
    years, 
    payment_type, 
    output_table_name, 
    verbose,
    force_reimport, 
    preserve_all_columns, 
    max_rows,
    memory_limit,
    batch_size
  )
  logger::log_debug("Input validation completed")
  
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
  
  # Establish database connection with memory configuration
  payments_conn <- connect_to_duckdb_memopt(db_path, memory_limit)
  
  # Initialize metadata tables if they don't exist
  initialize_metadata_tables_memopt(payments_conn)
  
  # Find CSV files matching the pattern
  logger::log_info(sprintf("Searching for files matching: %s", file_pattern))
  csv_files <- find_open_payments_files_memopt(base_dir, file_pattern)
  
  # Process each file
  processed_files <- import_open_payments_files_memopt(
    csv_files = csv_files,
    conn = payments_conn,
    years = years,
    force_reimport = force_reimport,
    max_rows = max_rows,
    batch_size = batch_size
  )
  
  # Free up memory
  gc(full = TRUE)
  
  # Create a single unified table schema from all the imported tables
  unified_schema <- create_unified_schema_memopt(
    conn = payments_conn,
    years = processed_files$years,
    preserve_all_columns = preserve_all_columns
  )
  
  # Create final table with the unified schema
  create_final_table_memopt(
    conn = payments_conn,
    table_name = output_table_name,
    schema = unified_schema,
    drop_existing = TRUE
  )
  
  # Free up memory
  gc(full = TRUE)
  
  # Merge data from all years into the final table
  merged_data <- merge_open_payments_data_memopt(
    conn = payments_conn,
    years = processed_files$years,
    output_table_name = output_table_name,
    unified_schema = unified_schema,
    batch_size = batch_size
  )
  
  # Check for NPI data in the final table
  check_npi_data_memopt(payments_conn, output_table_name)
  
  # Clean up temporary tables to free memory
  cleanup_temp_tables(payments_conn)
  
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
validate_open_payments_inputs_memopt <- function(base_dir, 
                                                 db_path, 
                                                 years, 
                                                 payment_type, 
                                                 output_table_name, 
                                                 verbose,
                                                 force_reimport, 
                                                 preserve_all_columns, 
                                                 max_rows,
                                                 memory_limit,
                                                 batch_size) {
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
    payment_type %in% c("general", "research", "ownership"),
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
  
  # Validate memory_limit
  assertthat::assert_that(
    is.numeric(memory_limit),
    memory_limit > 0,
    msg = "memory_limit must be a positive numeric value"
  )
  
  # Validate batch_size
  assertthat::assert_that(
    is.numeric(batch_size),
    batch_size > 0,
    msg = "batch_size must be a positive numeric value"
  )
}

#' @noRd
#' @noRd
connect_to_duckdb_memopt <- function(db_path, memory_limit) {
  # Attempt to connect to DuckDB with memory management settings
  tryCatch({
    conn <- DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db_path,
      read_only = FALSE
    )
    
    # Set memory limit with proper unit format
    memory_limit_str <- paste0(memory_limit, "GB")
    DBI::dbExecute(conn, paste0("PRAGMA memory_limit='", memory_limit_str, "'"))
    
    # Set thread limit to reduce memory usage
    DBI::dbExecute(conn, "PRAGMA threads=2")
    
    # Enable compression to reduce memory usage
    DBI::dbExecute(conn, "PRAGMA force_compression='ZSTD'")
    
    # For best performance with large CSV files
    DBI::dbExecute(conn, "PRAGMA temp_directory='./'")
    
    logger::log_success(sprintf("Successfully connected to DuckDB with %s memory limit", memory_limit_str))
    return(conn)
  }, error = function(e) {
    logger::log_error(sprintf("Failed to connect to DuckDB: %s", e$message))
    stop("Database connection failed")
  })
}

#' @noRd
initialize_metadata_tables_memopt <- function(conn) {
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
        status VARCHAR,
        PRIMARY KEY (filepath)
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
        data_type VARCHAR,
        PRIMARY KEY (year, original_column)
      )
    ")
    logger::log_success("Created op_column_mappings table")
  }
}

#' @noRd
find_open_payments_files_memopt <- function(base_dir, file_pattern) {
  # Use standard R file search functionality instead of system commands
  logger::log_info(sprintf("Searching for files matching: %s", file_pattern))
  
  # Get all CSV files
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
import_open_payments_files_memopt <- function(csv_files, conn, years, force_reimport, max_rows, batch_size) {
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
    
    # Import the file with optimized memory usage
    import_status <- import_single_file_memopt(
      conn = conn,
      file_path = file_info$file_path,
      year = file_info$year,
      max_rows = max_rows,
      batch_size = batch_size
    )
    
    if (import_status) {
      # Track imported years
      imported_years <- unique(c(imported_years, file_info$year))
    }
    
    # Force garbage collection between files
    gc(full = TRUE)
  }
  
  # Return years and count of files imported
  return(list(
    years = sort(imported_years),
    files = nrow(csv_files)
  ))
}

#' @noRd
import_single_file_memopt <- function(conn, file_path, year, max_rows, batch_size) {
  # Generate a table name for this file
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  table_name <- sprintf("op_data_%d_%s", year, timestamp)
  
  # Create a temporary table name
  temp_table_name <- sprintf("op_temp_%d", year)
  
  # First, sample just a few rows to determine schema
  # This reads only a small portion of the file
  sample_size <- 1000 # Small sample size to determine schema
  
  # Get file size to determine processing approach
  file_size_mb <- file.size(file_path) / (1024 * 1024)
  
  logger::log_debug(sprintf("File size: %.2f MB", file_size_mb))
  
  # Use different approaches based on file size
  if (file_size_mb > 1000) { # For files larger than 1GB
    logger::log_debug("Using stream processing for large file")
    return(import_large_file_by_streaming(conn, file_path, year, max_rows, table_name, batch_size))
  }
  
  # For smaller files, use standard approach with temp tables
  sample_cmd <- sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=%d, ignore_errors=true, strict_mode=false, HEADER=true)",
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
      "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=%d, ALL_VARCHAR=true, ignore_errors=true, strict_mode=false, HEADER=true)",
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
  
  # For memory efficiency, import in batches
  if (!is.null(max_rows) && max_rows < batch_size) {
    # For small imports, do it in one go
    logger::log_info(sprintf("Importing first %d rows only", max_rows))
    import_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto(
        '%s', 
        ignore_errors=true, 
        strict_mode=false,
        HEADER=true
      ) LIMIT %d",
      table_name,
      file_path,
      max_rows
    )
    
    import_success <- tryCatch({
      DBI::dbExecute(conn, import_cmd)
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Import failed: %s", e$message))
      # Try with ALL_VARCHAR as fallback
      fallback_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto(
          '%s', 
          ALL_VARCHAR=true, 
          ignore_errors=true, 
          strict_mode=false,
          HEADER=true
        ) LIMIT %d",
        table_name,
        file_path,
        max_rows
      )
      tryCatch({
        DBI::dbExecute(conn, fallback_cmd)
        TRUE
      }, error = function(e2) {
        logger::log_error(sprintf("Fallback import also failed: %s", e2$message))
        FALSE
      })
    })
  } else {
    # For larger imports, do it in batches
    if (is.null(max_rows)) {
      logger::log_info("Importing all rows in batches")
      total_rows_to_import <- Inf
    } else {
      logger::log_info(sprintf("Importing %d rows in batches", max_rows))
      total_rows_to_import <- max_rows
    }
    
    import_success <- import_in_batches(
      conn = conn,
      file_path = file_path,
      table_name = table_name,
      batch_size = batch_size,
      max_rows = total_rows_to_import
    )
  }
  
  if (!import_success) {
    # Record failure in metadata
    metadata_cmd <- sprintf(
      "INSERT INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
       VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, 0, 'failed')",
      file_path,
      file_size_mb,
      year
    )
    DBI::dbExecute(conn, metadata_cmd)
    
    # Clean up the failed table
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    
    return(FALSE)
  }
  
  # Check how many rows were imported
  row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", table_name)
  row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
  
  # Log success
  if (!is.null(max_rows)) {
    logger::log_success(sprintf("Successfully imported %d rows (limited to %d)", 
                                row_count, max_rows))
  } else {
    logger::log_success(sprintf("Successfully imported %d rows", row_count))
  }
  
  # Record in metadata table
  metadata_cmd <- sprintf(
    "INSERT INTO op_file_metadata (filepath, file_size, year, import_timestamp, row_count, status)
     VALUES ('%s', %f, %d, CURRENT_TIMESTAMP, %d, 'completed')",
    file_path,
    file_size_mb,
    year,
    row_count
  )
  DBI::dbExecute(conn, metadata_cmd)
  
  # Record column mappings
  record_column_mappings_memopt(conn, year, columns, temp_table_name)
  
  return(TRUE)
}

#' @noRd
import_large_file_by_streaming <- function(conn, file_path, year, max_rows, table_name, batch_size) {
  # For very large files, use a streaming approach with COPY
  logger::log_info("Using streaming approach for large file")
  
  # Step 1: Create schema in DuckDB using header only
  header_only_temp <- sprintf("op_header_temp_%d", year)
  
  # Try to create a schema by reading just the header
  header_cmd <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=10, HEADER=true, skip=0) WHERE 1=0",
    header_only_temp,
    file_path
  )
  
  schema_success <- tryCatch({
    DBI::dbExecute(conn, header_cmd)
    TRUE
  }, error = function(e) {
    logger::log_error(sprintf("Failed to create schema from header: %s", e$message))
    # Try with ALL_VARCHAR approach as fallback
    fallback_cmd <- sprintf(
      "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', sample_size=10, ALL_VARCHAR=true, HEADER=true, skip=0) WHERE 1=0",
      header_only_temp,
      file_path
    )
    tryCatch({
      DBI::dbExecute(conn, fallback_cmd)
      TRUE
    }, error = function(e2) {
      logger::log_error(sprintf("Fallback schema creation also failed: %s", e2$message))
      FALSE
    })
  })
  
  if (!schema_success) {
    return(FALSE)
  }
  
  # Get column information
  columns <- DBI::dbListFields(conn, header_only_temp)
  logger::log_debug(sprintf("Detected %d columns in the file", length(columns)))
  
  # Create the target table with same schema 
  create_table_cmd <- sprintf(
    "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0",
    table_name,
    header_only_temp
  )
  DBI::dbExecute(conn, create_table_cmd)
  
  # Record column mappings
  record_column_mappings_memopt(conn, year, columns, header_only_temp)
  
  # Drop the temporary header table to save memory
  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", header_only_temp))
  
  # Step 2: Stream from the CSV directly using COPY with chunking
  rows_imported <- 0
  import_success <- TRUE
  
  # For memory efficiency, import a batch at a time
  if (!is.null(max_rows)) {
    # If max_rows is set, use it as the limit
    copy_cmd <- sprintf(
      "COPY %s FROM '%s' (AUTO_DETECT TRUE, HEADER TRUE, IGNORE_ERRORS TRUE, SAMPLE_SIZE 1000) LIMIT %d",
      table_name,
      file_path,
      max_rows
    )
    
    import_success <- tryCatch({
      DBI::dbExecute(conn, copy_cmd)
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed to import file with COPY: %s", e$message))
      
      # Try with ALL_VARCHAR
      fallback_cmd <- sprintf(
        "COPY %s FROM '%s' (ALL_VARCHAR TRUE, HEADER TRUE, IGNORE_ERRORS TRUE) LIMIT %d",
        table_name,
        file_path,
        max_rows
      )
      
      tryCatch({
        DBI::dbExecute(conn, fallback_cmd)
        TRUE
      }, error = function(e2) {
        logger::log_error(sprintf("Fallback COPY also failed: %s", e2$message))
        FALSE
      })
    })
  } else {
    # If processing the entire file, do it in chunks
    copy_successful <- FALSE
    
    # First, try direct COPY as it's most efficient if it works
    copy_cmd <- sprintf(
      "COPY %s FROM '%s' (AUTO_DETECT TRUE, HEADER TRUE, IGNORE_ERRORS TRUE, SAMPLE_SIZE 1000)",
      table_name,
      file_path
    )
    
    copy_successful <- tryCatch({
      DBI::dbExecute(conn, copy_cmd)
      TRUE
    }, error = function(e) {
      logger::log_warn(sprintf("Direct COPY failed, will try batched import: %s", e$message))
      FALSE
    })
    
    if (!copy_successful) {
      # If direct COPY failed, use batched approach
      import_success <- import_in_batches(
        conn = conn,
        file_path = file_path,
        table_name = table_name,
        batch_size = batch_size,
        max_rows = NULL
      )
    }
  }
  
  # Step 3: Check how many rows were actually imported
  if (import_success) {
    row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", table_name)
    row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
    rows_imported <- row_count
    
    if (!is.null(max_rows)) {
      logger::log_success(sprintf("Successfully imported %d rows (limited to %d)", 
                                  row_count, max_rows))
    } else {
      logger::log_success(sprintf("Successfully imported %d rows", row_count))
    }
  } else {
    rows_imported <- 0
  }
  
  # Return import status
  return(import_success)
}

#' @noRd
#' @noRd
import_in_batches <- function(conn, file_path, table_name, batch_size, max_rows) {
  logger::log_info(sprintf("Importing file in batches of %d rows", batch_size))
  
  total_rows_imported <- 0
  current_offset <- 0
  continue_import <- TRUE
  
  # Set max_total based on max_rows
  max_total <- if(is.null(max_rows)) Inf else max_rows
  
  while(continue_import && total_rows_imported < max_total) {
    # Calculate how many rows to import in this batch
    rows_to_import <- min(batch_size, max_total - total_rows_imported)
    
    # Generate import command
    batch_cmd <- sprintf(
      "INSERT INTO %s SELECT * FROM read_csv_auto('%s', SKIP=%d, HEADER=%s, IGNORE_ERRORS=TRUE, SAMPLE_SIZE=1000) LIMIT %d",
      table_name,
      file_path,
      current_offset + (current_offset > 0), # Skip header only on first batch
      ifelse(current_offset == 0, "TRUE", "FALSE"),
      rows_to_import
    )
    
    # Execute the import
    batch_success <- tryCatch({
      DBI::dbExecute(conn, batch_cmd)
      TRUE
    }, error = function(e) {
      logger::log_warn(sprintf("Batch import failed, trying alternate method: %s", e$message))
      
      # Try with ALL_VARCHAR if the standard approach fails
      alt_cmd <- sprintf(
        "INSERT INTO %s SELECT * FROM read_csv_auto('%s', SKIP=%d, HEADER=%s, ALL_VARCHAR=TRUE, IGNORE_ERRORS=TRUE) LIMIT %d",
        table_name,
        file_path,
        current_offset + (current_offset > 0),
        ifelse(current_offset == 0, "TRUE", "FALSE"),
        rows_to_import
      )
      
      tryCatch({
        DBI::dbExecute(conn, alt_cmd)
        TRUE
      }, error = function(e2) {
        logger::log_error(sprintf("Alternate batch import also failed: %s", e2$message))
        FALSE
      })
    })
    
    if (!batch_success) {
      logger::log_error("Batch import failed, stopping import process")
      break
    }
    
    # Update counts
    current_offset <- current_offset + rows_to_import
    total_rows_imported <- total_rows_imported + rows_to_import
    
    # Check if we need to continue - using %% instead of % for modulo
    if ((current_offset %% (batch_size * 5)) == 0) {
      logger::log_info(sprintf("Imported %d rows so far", total_rows_imported))
      
      # Force garbage collection periodically
      gc(full = TRUE)
    }
    
    # Check if we're done - using %% instead of % for modulo
    check_cmd <- sprintf(
      "SELECT COUNT(*) AS row_count FROM read_csv_auto('%s', SKIP=%d, HEADER=FALSE, SAMPLE_SIZE=10) LIMIT 1",
      file_path, 
      current_offset + 1
    )
    
    # Only check if we've hit the end of file every few batches to save time
    if ((current_offset %% (batch_size * 10)) == 0) {
      check_result <- tryCatch({
        result <- DBI::dbGetQuery(conn, check_cmd)
        if (is.data.frame(result) && nrow(result) > 0) {
          result$row_count > 0
        } else {
          FALSE
        }
      }, error = function(e) {
        # If we get an error, assume we've reached the end
        FALSE
      })
      
      if (!check_result) {
        continue_import <- FALSE
      }
    }
    
    if (is.finite(max_total) && total_rows_imported >= max_total) {
      continue_import <- FALSE
      logger::log_info(sprintf("Reached maximum row limit of %d", max_total))
    }
  }
  
  logger::log_success(sprintf("Completed batch import with %d total rows", total_rows_imported))
  
  # Return TRUE if any rows were imported
  return(total_rows_imported > 0)
}

#' @noRd
record_column_mappings_memopt <- function(conn, year, columns, table_name) {
  # Process in smaller batches to reduce memory usage
  batch_size <- 20
  num_batches <- ceiling(length(columns) / batch_size)
  
  for (batch in 1:num_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, length(columns))
    batch_columns <- columns[start_idx:end_idx]
    
    for (col in batch_columns) {
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
}

#' @noRd
create_unified_schema_memopt <- function(conn, years, preserve_all_columns) {
  logger::log_info("Creating comprehensive column mapping to preserve all columns")
  
  # Initialize empty schema
  all_columns <- character(0)
  column_types <- character(0)
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
  
  # Process each year in batches to reduce memory usage
  for (year in years) {
    # Get column mapping information from database
    mapping_query <- sprintf(
      "SELECT original_column, standardized_column, data_type 
       FROM op_column_mappings 
       WHERE year = %d",
      year
    )
    year_mapping <- DBI::dbGetQuery(conn, mapping_query)
    
    if (nrow(year_mapping) == 0) {
      logger::log_warn(sprintf("No column mapping found for year %d", year))
      next
    }
    
    # Get original column names for this year
    original_cols <- year_mapping$original_column
    
    # Store column information 
    year_columns[[as.character(year)]] <- original_cols
    
    # Find and log NPI columns
    npi_cols <- original_cols[grep("NPI", original_cols, ignore.case = TRUE)]
    if (length(npi_cols) > 0) {
      logger::log_debug(sprintf("Year %d: Found NPI columns: %s", 
                                year, paste(npi_cols, collapse = ", ")))
      
      # Add all NPI columns to high priority list
      high_priority_columns <- unique(c(high_priority_columns, npi_cols))
    }
    
    # Add to unified column list
    all_columns <- unique(c(all_columns, original_cols))
    
    # Store column types
    for (i in 1:nrow(year_mapping)) {
      col <- year_mapping$original_column[i]
      type <- year_mapping$data_type[i]
      
      # Add to types if not already present
      if (!(col %in% names(column_types))) {
        column_types[col] <- type
      }
    }
    
    # Free memory
    rm(year_mapping)
    gc(full = FALSE)
  }
  
  # If not preserving all columns, filter to high priority only
  if (!preserve_all_columns) {
    all_columns <- all_columns[all_columns %in% high_priority_columns]
    column_types <- column_types[names(column_types) %in% all_columns]
  }
  
  # Ensure Source_Table is included
  all_columns <- unique(c(all_columns, "Source_Table"))
  column_types["Source_Table"] <- "VARCHAR"
  
  # Set special types for known columns
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
create_final_table_memopt <- function(conn, table_name, schema, drop_existing = FALSE) {
  # Drop existing table if requested
  if (drop_existing && DBI::dbExistsTable(conn, table_name)) {
    logger::log_warn(sprintf("Dropping existing table %s as requested", table_name))
    DBI::dbExecute(conn, sprintf("DROP TABLE %s", table_name))
  }
  
  # Create the table with unified schema if it doesn't exist
  if (!DBI::dbExistsTable(conn, table_name)) {
    # Generate column definitions with appropriate types
    # Process in batches to reduce memory usage
    batch_size <- 50
    num_batches <- ceiling(length(schema$columns) / batch_size)
    
    all_column_defs <- character(0)
    
    for (batch in 1:num_batches) {
      start_idx <- (batch - 1) * batch_size + 1
      end_idx <- min(batch * batch_size, length(schema$columns))
      
      batch_columns <- schema$columns[start_idx:end_idx]
      column_defs <- character(length(batch_columns))
      
      for (i in seq_along(batch_columns)) {
        col_name <- batch_columns[i]
        col_type <- schema$types[col_name]
        if (is.na(col_type)) col_type <- "VARCHAR"
        column_defs[i] <- sprintf('"%s" %s', col_name, col_type)
      }
      
      all_column_defs <- c(all_column_defs, column_defs)
    }
    
    # Create the table
    create_table_cmd <- sprintf(
      "CREATE TABLE %s (%s)",
      table_name,
      paste(all_column_defs, collapse = ", ")
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
merge_open_payments_data_memopt <- function(conn, years, output_table_name, unified_schema, batch_size) {
  logger::log_info(sprintf("Merging data into table: %s", output_table_name))
  
  # Track total rows merged
  total_rows_merged <- 0
  
  # Process each year sequentially to minimize memory usage
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
    
    # Get row count for this year - this should be memory-efficient
    row_count_query <- sprintf("SELECT COUNT(*) AS row_count FROM %s", year_table)
    year_row_count <- DBI::dbGetQuery(conn, row_count_query)$row_count
    
    # Calculate number of batches
    num_batches <- ceiling(year_row_count / batch_size)
    
    logger::log_info(sprintf("Merging %d rows from year %d in %d chunks", 
                             year_row_count, year, num_batches))
    
    # Get columns for the current year table
    year_columns <- unified_schema$year_columns[[as.character(year)]]
    if (is.null(year_columns)) {
      logger::log_error(sprintf("No column information for year %d", year))
      next
    }
    
    # Process each batch
    for (batch in 1:num_batches) {
      # Calculate offsets
      offset <- (batch - 1) * batch_size
      limit <- batch_size
      
      # For memory efficiency, build SELECT clause in smaller chunks
      max_columns_per_batch <- 50
      total_schema_columns <- length(unified_schema$columns)
      column_batches <- ceiling(total_schema_columns / max_columns_per_batch)
      
      all_select_parts <- character(0)
      
      # Build SELECT clause in batches to avoid memory issues
      for (col_batch in 1:column_batches) {
        col_start <- (col_batch - 1) * max_columns_per_batch + 1
        col_end <- min(col_batch * max_columns_per_batch, total_schema_columns)
        
        batch_columns <- unified_schema$columns[col_start:col_end]
        select_parts <- character(length(batch_columns))
        
        for (i in seq_along(batch_columns)) {
          col <- batch_columns[i]
          
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
        
        all_select_parts <- c(all_select_parts, select_parts)
      }
      
      # Build INSERT columns clause efficiently
      insert_columns <- paste(sprintf('"%s"', unified_schema$columns), collapse = ", ")
      
      # Combine all select parts
      select_clause <- paste(all_select_parts, collapse = ", ")
      
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
      insert_success <- tryCatch({
        DBI::dbExecute(conn, insert_cmd)
        TRUE
      }, error = function(e) {
        logger::log_error(sprintf("Failed to merge chunk %d for year %d: %s", 
                                  batch, year, e$message))
        
        # Let's try to provide more helpful error information
        if (grepl("column count", e$message, ignore.case = TRUE)) {
          # Get column count details
          table_cols <- DBI::dbListFields(conn, output_table_name)
          logger::log_error(sprintf("Column count mismatch: Table has %d columns", length(table_cols)))
        }
        FALSE
      })
      
      if (!insert_success) {
        # Try splitting into even smaller batches
        logger::log_warn("Trying emergency mini-batch approach")
        mini_batch_size <- batch_size / 10
        mini_batches <- 10
        
        mini_batch_success <- TRUE
        for (mini_batch in 1:mini_batches) {
          mini_offset <- offset + (mini_batch - 1) * mini_batch_size
          
          mini_cmd <- sprintf(
            "INSERT INTO %s (%s) SELECT %s FROM %s OFFSET %d LIMIT %d",
            output_table_name,
            insert_columns,
            select_clause,
            year_table,
            mini_offset,
            mini_batch_size
          )
          
          mini_success <- tryCatch({
            DBI::dbExecute(conn, mini_cmd)
            TRUE
          }, error = function(e) {
            logger::log_error(sprintf("Mini-batch %d failed: %s", mini_batch, e$message))
            FALSE
          })
          
          if (!mini_success) {
            mini_batch_success <- FALSE
            break
          }
        }
        
        if (!mini_batch_success) {
          logger::log_error("Failed to merge data even with mini-batches, skipping remaining chunks for this year")
          break
        }
      }
      
      # Log progress periodically
      if (batch %% 5 == 0 || batch == num_batches) {
        logger::log_debug(sprintf("Merged chunk %d/%d for year %d", 
                                  batch, num_batches, year))
      }
      
      # Force garbage collection periodically
      if (batch %% 10 == 0) {
        gc(full = TRUE)
      }
    }
    
    # Update total rows
    total_rows_merged <- total_rows_merged + year_row_count
    
    # Force garbage collection between years
    gc(full = TRUE)
    
    # After successfully merging a year's data, drop its temporary table to free memory
    if (grepl("^op_data_", year_table)) {
      logger::log_debug(sprintf("Dropping temporary table %s to free memory", year_table))
      DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", year_table))
    }
  }
  
  # Log completion
  logger::log_success(sprintf("Successfully merged %d rows into %s", 
                              total_rows_merged, output_table_name))
  
  # Return summary
  return(list(
    table_name = output_table_name,
    total_rows = total_rows_merged,
    years = years
  ))
}

#' @noRd
check_npi_data_memopt <- function(conn, table_name) {
  # Get column information efficiently
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
  
  # Check for non-NULL NPI values efficiently
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
      fix_npi_data_memopt(conn, table_name, npi_col)
    }
  }
  
  return(TRUE)
}

#' @noRd
#' @noRd
fix_npi_data_memopt <- function(conn, output_table_name, npi_column) {
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
  
  # Process each temp table
  for (temp_table in temp_tables) {
    # Check if temp table has the NPI column
    has_column <- tryCatch({
      DBI::dbGetQuery(conn, sprintf("SELECT 1 FROM %s WHERE 0=1", temp_table))
      temp_columns <- DBI::dbListFields(conn, temp_table)
      npi_column %in% temp_columns
    }, error = function(e) {
      logger::log_debug(sprintf("Error checking columns in %s: %s", temp_table, e$message))
      FALSE
    })
    
    if (!has_column) {
      next
    }
    
    # Check if the temp table has non-NULL NPI values
    check_query <- sprintf(
      "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL LIMIT 1",
      temp_table, npi_column
    )
    has_npi_data <- tryCatch({
      result <- DBI::dbGetQuery(conn, check_query)
      result$cnt > 0
    }, error = function(e) {
      logger::log_debug(sprintf("Error checking NPI data in %s: %s", temp_table, e$message))
      FALSE
    })
    
    if (!has_npi_data) {
      next
    }
    
    logger::log_debug(sprintf("Found NPI data in %s, attempting to copy to main table", temp_table))
    
    # Extract year from temp table name
    year <- as.integer(stringr::str_extract(temp_table, "\\d{4}"))
    
    # Get a minimal set of columns from both tables for memory efficiency
    base_columns <- tryCatch({
      DBI::dbGetQuery(conn, sprintf("
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '%s' 
        AND column_name NOT LIKE '%%NPI%%'
        LIMIT 10
      ", temp_table))$column_name
    }, error = function(e) {
      logger::log_debug(sprintf("Error getting columns from %s: %s", temp_table, e$message))
      return(character(0))
    })
    
    if (length(base_columns) == 0) {
      next
    }
    
    # Try using Record_ID first if available, as it's most likely to be unique
    join_column <- if ("Record_ID" %in% base_columns) "Record_ID" else base_columns[1]
    
    # Create update query based on matching keys - use direct SQL for memory efficiency
    # Do the update in smaller batches to avoid memory issues
    update_query <- sprintf(
      "UPDATE %s 
       SET \"%s\" = subq.\"%s\"
       FROM (
         SELECT t.\"%s\", t.\"%s\"
         FROM %s t
         WHERE t.\"%s\" IS NOT NULL
       ) AS subq
       WHERE %s.\"Program_Year\" = %d 
       AND %s.\"%s\" = subq.\"%s\"
       AND %s.\"%s\" IS NULL",
      output_table_name, npi_column, npi_column,
      join_column, npi_column,
      temp_table,
      npi_column,
      output_table_name, year,
      output_table_name, join_column, join_column,
      output_table_name, npi_column
    )
    
    # Execute the update
    update_success <- tryCatch({
      DBI::dbExecute(conn, update_query)
      TRUE
    }, error = function(e) {
      logger::log_error(sprintf("Failed to update NPI data from %s: %s", 
                                temp_table, e$message))
      FALSE
    })
    
    if (update_success) {
      # Check how many rows were updated
      updated_check_query <- sprintf(
        "SELECT COUNT(*) AS cnt FROM %s WHERE \"%s\" IS NOT NULL AND \"Program_Year\" = %d",
        output_table_name, npi_column, year
      )
      updated_count <- DBI::dbGetQuery(conn, updated_check_query)$cnt
      
      logger::log_success(sprintf("Updated %d rows with NPI data from year %d", 
                                  updated_count, year))
      
      updated_rows <- updated_rows + updated_count
    }
    
    # Force garbage collection after each table
    gc(full = TRUE)
  }
  
  if (updated_rows > 0) {
    logger::log_success(sprintf("Successfully recovered %d NPI values in total", updated_rows))
    return(TRUE)
  } else {
    logger::log_debug("Could not recover any NPI values from temporary tables")
    return(FALSE)
  }
}

#' @noRd
cleanup_temp_tables <- function(conn) {
  logger::log_info("Cleaning up temporary tables to free memory")
  
  # Get a list of all temporary tables
  temp_tables <- DBI::dbGetQuery(
    conn,
    "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'op_temp_%' OR table_name LIKE 'op_data_%'"
  )$table_name
  
  if (length(temp_tables) == 0) {
    logger::log_debug("No temporary tables to clean up")
    return(TRUE)
  }
  
  # Drop each temporary table
  for (table in temp_tables) {
    logger::log_debug(sprintf("Dropping temporary table: %s", table))
    drop_success <- tryCatch({
      DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table))
      TRUE
    }, error = function(e) {
      logger::log_warn(sprintf("Failed to drop table %s: %s", table, e$message))
      FALSE
    })
  }
  
  # Force garbage collection
  gc(full = TRUE)
  
  logger::log_success(sprintf("Cleaned up %d temporary tables", length(temp_tables)))
  return(TRUE)
}
#### Trying on 5/9/2025
#' Process Open Payments Data with Simple Memory Optimization
#'
#' @description
#' A simplified, memory-optimized function that processes Open Payments data files
#' into a DuckDB database with basic memory management to prevent crashes.
#'
#' @param base_dir Directory containing Open Payments CSV files
#' @param db_path Path for DuckDB database file
#' @param years Years to process, or NULL for all
#' @param payment_type "general", "research", or "ownership"
#' @param output_table_name Name for final merged output table
#' @param max_rows Maximum rows per file (NULL for all)
#' @param batch_size Batch size for processing
#'
#' @return DuckDB connection and output table name
#'
#' @export
process_open_payments_simple <- function(base_dir,
                                         db_path,
                                         years = NULL,
                                         payment_type = "general",
                                         output_table_name = "open_payments_merged",
                                         max_rows = NULL,
                                         batch_size = 50000) {
  
  # Setup logging
  logger::log_layout(logger::layout_simple)
  logger::log_threshold(logger::INFO)
  
  # Clear memory before we start
  gc(full = TRUE)
  
  # Log start
  message("Starting Open Payments data processing")
  
  # Define file pattern
  file_patterns <- list(
    general = "OP_DTL_GNRL_PGYR\\d{4}.*\\.csv$",
    research = "OP_DTL_RSRCH_PGYR\\d{4}.*\\.csv$",
    ownership = "OP_DTL_OWNRSHP_PGYR\\d{4}.*\\.csv$"
  )
  file_pattern <- file_patterns[[payment_type]]
  
  # Connect to DuckDB
  message("Connecting to DuckDB")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  
  # Set memory-friendly settings
  DBI::dbExecute(conn, "PRAGMA memory_limit='3GB'")
  DBI::dbExecute(conn, "PRAGMA threads=2")
  
  # Create metadata table if it doesn't exist
  if (!DBI::dbExistsTable(conn, "op_file_metadata")) {
    DBI::dbExecute(conn, "
      CREATE TABLE op_file_metadata (
        filepath VARCHAR,
        year INTEGER,
        status VARCHAR
      )
    ")
  }
  
  # Find files
  message("Finding CSV files")
  all_files <- list.files(
    path = base_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )
  
  # Filter by pattern
  matching_files <- all_files[grep(file_pattern, basename(all_files))]
  
  if (length(matching_files) == 0) {
    stop("No matching files found")
  }
  
  # Create data frame with file info
  file_data <- data.frame(
    file_path = matching_files,
    file_name = basename(matching_files),
    stringsAsFactors = FALSE
  )
  
  # Extract year from filename
  file_data$year <- as.integer(gsub(".*PGYR(\\d{4}).*", "\\1", file_data$file_name))
  
  # Filter by years if specified
  if (!is.null(years)) {
    file_data <- file_data[file_data$year %in% years, ]
  }
  
  # Process files
  message(sprintf("Processing %d files", nrow(file_data)))
  
  # Keep track of imported years and tables
  imported_years <- c()
  year_tables <- list()
  
  # Process each file
  for (i in 1:nrow(file_data)) {
    file_info <- file_data[i, ]
    year <- file_info$year
    file_path <- file_info$file_path
    
    message(sprintf("Processing file %d/%d: %s", i, nrow(file_data), basename(file_path)))
    
    # Generate table name
    table_name <- sprintf("op_data_%d_%d", year, i)
    
    # Create table for this file
    message("Creating table schema")
    
    # Try to create table with auto-detect
    success <- try({
      DBI::dbExecute(
        conn, 
        sprintf(
          "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', SAMPLE_SIZE=1000, HEADER=true) WHERE 1=0",
          table_name,
          file_path
        )
      )
      TRUE
    }, silent = TRUE)
    
    # If that fails, try with all VARCHAR
    if (inherits(success, "try-error")) {
      message("Trying with ALL_VARCHAR")
      success <- try({
        DBI::dbExecute(
          conn, 
          sprintf(
            "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=true, HEADER=true) WHERE 1=0",
            table_name,
            file_path
          )
        )
        TRUE
      }, silent = TRUE)
    }
    
    # If still fails, skip this file
    if (inherits(success, "try-error")) {
      message("Skipping file due to schema creation failure")
      next
    }
    
    # Import data
    message("Importing data")
    
    if (!is.null(max_rows)) {
      # Import with row limit
      success <- try({
        DBI::dbExecute(
          conn, 
          sprintf(
            "INSERT INTO %s SELECT * FROM read_csv_auto('%s', HEADER=true) LIMIT %d",
            table_name,
            file_path,
            max_rows
          )
        )
        TRUE
      }, silent = TRUE)
    } else {
      # Try to import in one go first
      success <- try({
        DBI::dbExecute(
          conn, 
          sprintf(
            "COPY %s FROM '%s' (HEADER true, AUTO_DETECT true)",
            table_name,
            file_path
          )
        )
        TRUE
      }, silent = TRUE)
      
      # If that fails, try batched importing
      if (inherits(success, "try-error")) {
        message("Switching to batched import")
        import_batched(conn, file_path, table_name, batch_size)
      }
    }
    
    # Track this table if successful
    if (!inherits(success, "try-error")) {
      imported_years <- unique(c(imported_years, year))
      year_tables[[as.character(year)]] <- table_name
      message("Import successful")
    }
    
    # Clean up memory
    gc(full = TRUE)
  }
  
  # If no tables imported, return early
  if (length(year_tables) == 0) {
    message("No data processed")
    return(list(connection = conn, table_name = NULL))
  }
  
  # Create or replace output table
  message("Creating merged table")
  
  # First, collect all column names across tables
  all_columns <- c()
  
  for (table in unlist(year_tables)) {
    try({
      cols <- DBI::dbListFields(conn, table)
      all_columns <- unique(c(all_columns, cols))
    }, silent = TRUE)
  }
  
  # Add source table column
  all_columns <- c(all_columns, "Source_Table")
  
  # Create output table
  if (DBI::dbExistsTable(conn, output_table_name)) {
    DBI::dbExecute(conn, sprintf("DROP TABLE %s", output_table_name))
  }
  
  # Create table with all VARCHAR columns for simplicity
  column_defs <- paste0('"', all_columns, '" VARCHAR', collapse = ", ")
  
  DBI::dbExecute(
    conn,
    sprintf("CREATE TABLE %s (%s)", output_table_name, column_defs)
  )
  
  # Merge data from all tables
  message("Merging data from all years")
  
  for (year in names(year_tables)) {
    table_name <- year_tables[[year]]
    
    message(sprintf("Merging data from year %s", year))
    
    # Get row count
    row_count <- DBI::dbGetQuery(
      conn,
      sprintf("SELECT COUNT(*) as count FROM %s", table_name)
    )$count
    
    # Process in batches
    num_batches <- ceiling(row_count / batch_size)
    
    for (batch in 1:num_batches) {
      offset <- (batch - 1) * batch_size
      
      # Build column mappings for this table
      table_cols <- DBI::dbListFields(conn, table_name)
      select_expr <- c()
      
      for (col in all_columns) {
        if (col == "Source_Table") {
          select_expr <- c(select_expr, sprintf("'%s' AS \"Source_Table\"", table_name))
        } else if (col %in% table_cols) {
          select_expr <- c(select_expr, sprintf('"%s"', col))
        } else {
          select_expr <- c(select_expr, sprintf('NULL AS "%s"', col))
        }
      }
      
      # Execute merge for this batch
      try({
        DBI::dbExecute(
          conn,
          sprintf(
            "INSERT INTO %s SELECT %s FROM %s OFFSET %d LIMIT %d",
            output_table_name,
            paste(select_expr, collapse = ", "),
            table_name,
            offset,
            batch_size
          )
        )
      }, silent = TRUE)
      
      # Log progress
      if (batch %% 10 == 0 || batch == num_batches) {
        message(sprintf("Merged batch %d/%d for year %s", batch, num_batches, year))
      }
      
      # Clean up memory periodically
      if (batch %% 20 == 0) {
        gc(full = TRUE)
      }
    }
    
    # Clean up memory after each year
    gc(full = TRUE)
  }
  
  message("Open Payments data processing completed")
  
  # Return the connection and table name
  return(list(
    connection = conn,
    table_name = output_table_name
  ))
}

#' Import a file in batches
#'
#' A simple helper function to import CSV data in batches
#'
#' @param conn DuckDB connection
#' @param file_path Path to CSV file
#' @param table_name Target table name
#' @param batch_size Batch size
#'
#' @return TRUE if successful
#'
#' @noRd
import_batched <- function(conn, file_path, table_name, batch_size) {
  # Start at offset 0
  offset <- 0
  continue <- TRUE
  
  while (continue) {
    # Skip header after first batch
    header <- ifelse(offset == 0, "true", "false")
    skip <- offset + ifelse(offset > 0, 1, 0) # Skip header in subsequent batches
    
    # Import batch
    result <- try({
      DBI::dbExecute(
        conn,
        sprintf(
          "INSERT INTO %s SELECT * FROM read_csv_auto('%s', SKIP=%d, HEADER=%s) LIMIT %d",
          table_name,
          file_path,
          skip,
          header,
          batch_size
        )
      )
      TRUE
    }, silent = TRUE)
    
    # Stop if error
    if (inherits(result, "try-error")) {
      continue <- FALSE
    } else {
      # Update offset
      offset <- offset + batch_size
      
      # Check if we've reached the end (very simple check)
      if (offset > 10000000) {  # Arbitrary large number
        continue <- FALSE
      }
    }
  }
  
  return(TRUE)
}


###
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"

result <- process_open_payments_simple(
  base_dir = base_dir,
  db_path = db_path,
  years = NULL,
  payment_type = "general",
  output_table_name = "open_payments_merged",
  max_rows = NULL,
  batch_size = 50000
)

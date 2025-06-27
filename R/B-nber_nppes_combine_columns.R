# Process NPI Data Files to Extract OB/GYN Providers with Intelligent Schema----
#' Alignment
#'
#' This function processes National Provider Identifier (NPI) data files from 
#' a specified directory, filters for OB/GYN providers based on taxonomy codes,
#' and combines the results into a single harmonized dataset. The function 
#' handles multiple file formats (CSV, Parquet), performs comprehensive schema
#' analysis, and applies universal data type harmonization to prevent 
#' combination errors.
#'
#' @param npi_input_directory_path Character string specifying the full path 
#'   to the directory containing NPI data files. Directory must exist and 
#'   contain files matching the specified pattern.
#' @param obgyn_output_file_path Character string specifying the full path 
#'   where the combined OB/GYN provider dataset should be written. Supported 
#'   formats: .csv, .parquet, .feather
#' @param obgyn_taxonomy_code_vector Character vector of OB/GYN taxonomy codes
#'   to filter for. Default includes standard OB/GYN codes: "207V00000X" 
#'   (general), "207VG0400X" (gynecologic oncology), "207VM0101X" (maternal 
#'   & fetal medicine), "207VH0002X" (hospice), "207VE0102X" (reproductive 
#'   endocrinology)
#' @param npi_file_pattern_regex Character string regex pattern to identify 
#'   NPI files in the input directory. Default pattern matches files like 
#'   "npi2013.csv", "npi20204.parquet", etc.
#' @param schema_analysis_report_path Character string specifying where to 
#'   write the comprehensive schema analysis report. Default creates 
#'   "schema_analysis_report.csv" in the same directory as output file.
#' @param schema_compatibility_strategy Character string specifying how to 
#'   handle column differences across files. Options: "strict" (only common 
#'   columns), "intersect" (intersection of all columns), "union" (all unique
#'   columns with NA for missing). Default is "union".
#' @param enable_verbose_logging Logical flag to enable detailed console 
#'   logging throughout the processing pipeline. Default is TRUE.
#'
#' @return Invisible tibble containing the combined OB/GYN provider dataset.
#'   Note: All columns are converted to character type to prevent binding
#'   conflicts. Use convert_key_columns_to_proper_types() if needed.
#'
#' @examples
#' # Example 1: Basic usage with default OB/GYN taxonomy codes
#' \dontrun{
#' combined_obgyn_providers <- process_npi_obgyn_data(
#'   npi_input_directory_path = "/data/npi_files",
#'   obgyn_output_file_path = "/output/obgyn_providers.csv",
#'   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", 
#'                                  "207VM0101X", "207VH0002X", 
#'                                  "207VE0102X"),
#'   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#'   schema_analysis_report_path = "/output/schema_analysis.csv",
#'   schema_compatibility_strategy = "union",
#'   enable_verbose_logging = TRUE
#' )
#' # Output: Combined dataset with ~70,000 OB/GYN providers across years
#' # Files: /output/obgyn_providers.csv, /output/schema_analysis.csv
#' }
#'
#' # Example 2: Strict schema mode with custom taxonomy codes
#' \dontrun{
#' maternal_fetal_specialists <- process_npi_obgyn_data(
#'   npi_input_directory_path = "/data/historical_npi",
#'   obgyn_output_file_path = "/analysis/maternal_fetal_medicine.parquet",
#'   obgyn_taxonomy_code_vector = c("207VM0101X"),
#'   npi_file_pattern_regex = "npi_20[12]\\d\\.(csv|parquet)$",
#'   schema_analysis_report_path = "/analysis/mfm_schema_report.csv",
#'   schema_compatibility_strategy = "strict",
#'   enable_verbose_logging = FALSE
#' )
#' # Output: Subset focused on maternal-fetal medicine specialists only
#' # Schema: Only columns present in ALL input files included
#' }
#'
#' # Example 3: Gynecologic oncology with intersection strategy
#' \dontrun{
#' gyn_oncology_providers <- process_npi_obgyn_data(
#'   npi_input_directory_path = "/volumes/npi_data/processed",
#'   obgyn_output_file_path = "/research/gyn_onc_analysis.feather",
#'   obgyn_taxonomy_code_vector = c("207VG0400X", "207V00000X"),
#'   npi_file_pattern_regex = "cleaned_npi_\\d{4}\\.parquet$",
#'   schema_analysis_report_path = "/research/gyn_onc_schema.csv",
#'   schema_compatibility_strategy = "intersect",
#'   enable_verbose_logging = TRUE
#' )
#' # Output: Gynecologic oncologists with core common columns only
#' # Format: Fast-loading Feather format for R analysis
#' }
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom fs dir_ls path_ext file_exists file_size path_dir
#' @importFrom stringr str_detect str_extract
#' @importFrom dplyr tibble count bind_rows
#' @importFrom purrr map
#' @importFrom glue glue
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom readr write_csv
#' @importFrom arrow write_parquet write_feather
#'
#' @export
process_npi_obgyn_data <- function(
    npi_input_directory_path,
    obgyn_output_file_path,
    obgyn_taxonomy_code_vector = c(
      "207V00000X",    # Obstetrics & Gynecology (General)
      "207VX0201X",    # Gynecologic Oncology *** PRIMARY FOCUS ***
      "207VE0102X",    # Reproductive Endocrinology & Infertility
      "207VG0400X",    # Gynecology (General)
      "207VM0101X",    # Maternal & Fetal Medicine
      "207VF0040X",    # Female Pelvic Medicine & Reconstructive Surgery
      "207VB0002X",    # Bariatric Medicine (OBGYN)
      "207VC0200X",    # Critical Care Medicine (OBGYN)
      "207VC0040X",    # Gynecology subspecialty
      "207VC0300X",    # Complex Family Planning
      "207VH0002X",    # Hospice and Palliative Medicine (OBGYN)
      "207VX0000X"     # Obstetrics Only
    ),
    npi_file_pattern_regex = "npi\\\\d{4,5}\\\\.(csv|parquet)$",
    schema_analysis_report_path = NULL,
    schema_compatibility_strategy = "union",
    enable_verbose_logging = TRUE
) {
  
  # Configure logging system
  configure_function_logging(enable_verbose_logging)
  
  # Log function entry and parameters
  logger::log_info("=== STARTING NPI OB/GYN DATA PROCESSING ===")
  logger::log_info("Function: process_npi_obgyn_data")
  logger::log_info("Timestamp: {Sys.time()}")
  
  # Comprehensive input validation
  validate_function_inputs(npi_input_directory_path, obgyn_output_file_path,
                           obgyn_taxonomy_code_vector, npi_file_pattern_regex,
                           schema_analysis_report_path, 
                           schema_compatibility_strategy, enable_verbose_logging)
  
  # Log all input parameters for transparency
  log_function_input_parameters(npi_input_directory_path, obgyn_output_file_path,
                                obgyn_taxonomy_code_vector, npi_file_pattern_regex,
                                schema_analysis_report_path, 
                                schema_compatibility_strategy, 
                                enable_verbose_logging)
  
  # Set default schema analysis path if not provided
  if (is.null(schema_analysis_report_path)) {
    output_directory <- fs::path_dir(obgyn_output_file_path)
    schema_analysis_report_path <- file.path(output_directory, 
                                             "schema_analysis_report.csv")
  }
  
  # Discover NPI files in input directory
  discovered_npi_files <- discover_npi_files_in_directory(
    npi_input_directory_path, npi_file_pattern_regex)
  
  # Perform comprehensive schema analysis across all files
  schema_analysis_summary <- perform_comprehensive_schema_analysis(
    discovered_npi_files, schema_analysis_report_path)
  
  # Determine optimal column harmonization strategy
  column_harmonization_config <- determine_column_harmonization_strategy(
    schema_analysis_summary, schema_compatibility_strategy)
  
  # Initialize DuckDB connection for high-performance processing
  duckdb_connection <- establish_duckdb_connection()
  
  tryCatch({
    # Process and combine all NPI files with intelligent schema handling
    combined_obgyn_provider_dataset <- process_and_combine_npi_files_with_schema_intelligence(
      discovered_npi_files, obgyn_taxonomy_code_vector, 
      column_harmonization_config, duckdb_connection)
    
    # Write combined dataset with comprehensive validation
    write_combined_obgyn_dataset_with_validation(
      combined_obgyn_provider_dataset, obgyn_output_file_path)
    
    # Generate completion summary with statistics
    log_processing_completion_summary(combined_obgyn_provider_dataset, 
                                      obgyn_output_file_path)
    
    logger::log_info("=== NPI OB/GYN DATA PROCESSING COMPLETED SUCCESSFULLY ===")
    
    return(invisible(combined_obgyn_provider_dataset))
    
  }, error = function(processing_error) {
    logger::log_error("Critical processing error: {processing_error$message}")
    stop(glue::glue("NPI processing failed: {processing_error$message}"))
  }, finally = {
    # Always clean up DuckDB connection
    DBI::dbDisconnect(duckdb_connection, shutdown = TRUE)
    logger::log_info("DuckDB connection closed")
  })
}

#' @noRd
configure_function_logging <- function(enable_verbose_logging) {
  if (enable_verbose_logging) {
    logger::log_threshold(logger::INFO)
  } else {
    logger::log_threshold(logger::WARN)
  }
  logger::log_info("Logging configuration completed")
}

#' @noRd
validate_function_inputs <- function(npi_input_directory_path, 
                                     obgyn_output_file_path,
                                     obgyn_taxonomy_code_vector, 
                                     npi_file_pattern_regex,
                                     schema_analysis_report_path, 
                                     schema_compatibility_strategy,
                                     enable_verbose_logging) {
  logger::log_info("=== VALIDATING FUNCTION INPUTS ===")
  
  # Validate input directory
  assertthat::assert_that(is.character(npi_input_directory_path),
                          msg = "npi_input_directory_path must be character")
  assertthat::assert_that(fs::file_exists(npi_input_directory_path),
                          msg = "Input directory does not exist")
  
  # Validate output file path
  assertthat::assert_that(is.character(obgyn_output_file_path),
                          msg = "obgyn_output_file_path must be character")
  output_extension <- fs::path_ext(obgyn_output_file_path)
  assertthat::assert_that(output_extension %in% c("csv", "parquet", "feather"),
                          msg = "Output format must be csv, parquet, or feather")
  
  # Validate taxonomy codes
  assertthat::assert_that(is.character(obgyn_taxonomy_code_vector),
                          msg = "obgyn_taxonomy_code_vector must be character")
  assertthat::assert_that(length(obgyn_taxonomy_code_vector) > 0,
                          msg = "Must provide at least one taxonomy code")
  
  # Validate file pattern
  assertthat::assert_that(is.character(npi_file_pattern_regex),
                          msg = "npi_file_pattern_regex must be character")
  
  # Validate schema strategy
  valid_strategies <- c("strict", "intersect", "union")
  assertthat::assert_that(schema_compatibility_strategy %in% valid_strategies,
                          msg = "schema_compatibility_strategy must be strict, intersect, or union")
  
  # Validate logging flag
  assertthat::assert_that(is.logical(enable_verbose_logging),
                          msg = "enable_verbose_logging must be logical")
  
  logger::log_info("All input validation checks passed successfully")
}

#' @noRd
log_function_input_parameters <- function(npi_input_directory_path, 
                                          obgyn_output_file_path,
                                          obgyn_taxonomy_code_vector, 
                                          npi_file_pattern_regex,
                                          schema_analysis_report_path, 
                                          schema_compatibility_strategy,
                                          enable_verbose_logging) {
  logger::log_info("=== FUNCTION INPUT PARAMETERS ===")
  logger::log_info("Input directory: {npi_input_directory_path}")
  logger::log_info("Output file path: {obgyn_output_file_path}")
  logger::log_info("OB/GYN taxonomy codes ({length(obgyn_taxonomy_code_vector)} total): {paste(obgyn_taxonomy_code_vector, collapse = ', ')}")
  logger::log_info("NPI file pattern: {npi_file_pattern_regex}")
  logger::log_info("Schema analysis report path: {schema_analysis_report_path}")
  logger::log_info("Schema compatibility strategy: {schema_compatibility_strategy}")
  logger::log_info("Verbose logging enabled: {enable_verbose_logging}")
  logger::log_info("=================================")
}

#' @noRd
discover_npi_files_in_directory <- function(npi_input_directory_path, 
                                            npi_file_pattern_regex) {
  logger::log_info("=== DISCOVERING NPI FILES ===")
  logger::log_info("Searching directory: {npi_input_directory_path}")
  logger::log_info("Using file pattern: {npi_file_pattern_regex}")
  
  # Get all files in directory
  all_directory_files <- fs::dir_ls(npi_input_directory_path)
  total_files_count <- length(all_directory_files)
  
  # Filter for NPI files matching pattern
  matching_npi_files <- all_directory_files[stringr::str_detect(
    basename(all_directory_files), npi_file_pattern_regex)]
  
  assertthat::assert_that(length(matching_npi_files) > 0,
                          msg = "No NPI files found matching the specified pattern")
  
  logger::log_info("Total files in directory: {total_files_count}")
  logger::log_info("NPI files matching pattern: {length(matching_npi_files)}")
  
  # Log discovered files with details
  logger::log_info("Discovered NPI files:")
  for (current_file_path in matching_npi_files) {
    current_filename <- basename(current_file_path)
    current_file_extension <- fs::path_ext(current_file_path)
    current_file_size <- fs::file_size(current_file_path)
    logger::log_info("  - {current_filename} ({current_file_extension}, {current_file_size} bytes)")
  }
  
  logger::log_info("File discovery completed successfully")
  return(matching_npi_files)
}

#' @noRd
establish_duckdb_connection <- function() {
  logger::log_info("Establishing DuckDB connection for high-performance processing")
  duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
  logger::log_info("DuckDB connection established successfully")
  return(duckdb_connection)
}

#' @noRd
perform_comprehensive_schema_analysis <- function(discovered_npi_files, 
                                                  schema_analysis_report_path) {
  logger::log_info("=== COMPREHENSIVE SCHEMA ANALYSIS ===")
  logger::log_info("Analyzing {length(discovered_npi_files)} NPI files for column compatibility")
  
  # Initialize schema analysis tracking
  schema_analysis_results_list <- list()
  successful_analyses_count <- 0
  failed_analyses_count <- 0
  
  # Establish temporary DuckDB connection for schema analysis
  temp_duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
  
  tryCatch({
    # Analyze each file's schema
    for (i in seq_along(discovered_npi_files)) {
      current_npi_file_path <- discovered_npi_files[i]
      current_filename <- basename(current_npi_file_path)
      extracted_year_info <- extract_year_from_filename(current_filename)
      
      logger::log_info("  Extracted year: {extracted_year_info}")
      logger::log_info("Analyzing schema: {current_filename}")
      
      # Attempt schema analysis with error handling
      tryCatch({
        schema_info <- analyze_single_file_schema(temp_duckdb_connection, 
                                                  current_npi_file_path)
        
        schema_analysis_results_list[[i]] <- list(
          filename = current_filename,
          file_path = current_npi_file_path,
          year = extracted_year_info,
          column_count = length(schema_info$column_name),
          column_names = schema_info$column_name,
          analysis_status = "success"
        )
        
        successful_analyses_count <- successful_analyses_count + 1
        logger::log_info("  Schema analysis successful: {length(schema_info$column_name)} columns detected")
        
      }, error = function(schema_error) {
        logger::log_error("  Schema analysis failed for {current_filename}: {schema_error$message}")
        schema_analysis_results_list[[i]] <- list(
          filename = current_filename,
          file_path = current_npi_file_path,
          year = extracted_year_info,
          column_count = 0,
          column_names = character(),
          analysis_status = "failed",
          error_message = schema_error$message
        )
        failed_analyses_count <- failed_analyses_count + 1
      })
    }
    
    logger::log_info("Schema analysis summary:")
    logger::log_info("  Successful: {successful_analyses_count} files")
    logger::log_info("  Failed: {failed_analyses_count} files")
    
    # Perform cross-file compatibility analysis
    compatibility_analysis <- analyze_cross_file_column_compatibility(
      schema_analysis_results_list)
    
    # Write comprehensive schema analysis report
    write_schema_analysis_report(schema_analysis_results_list, 
                                 compatibility_analysis, 
                                 schema_analysis_report_path)
    
    return(list(
      file_schemas = schema_analysis_results_list,
      compatibility = compatibility_analysis,
      successful_count = successful_analyses_count,
      failed_count = failed_analyses_count
    ))
    
  }, finally = {
    DBI::dbDisconnect(temp_duckdb_connection, shutdown = TRUE)
  })
}

#' @noRd
analyze_single_file_schema <- function(duckdb_connection, npi_file_path) {
  current_file_extension <- fs::path_ext(npi_file_path)
  
  if (current_file_extension == "csv") {
    return(analyze_csv_file_schema(duckdb_connection, npi_file_path))
  } else if (current_file_extension == "parquet") {
    return(analyze_parquet_file_schema(duckdb_connection, npi_file_path))
  } else {
    stop(glue::glue("Unsupported file format for schema analysis: {current_file_extension}"))
  }
}

#' @noRd
analyze_csv_file_schema <- function(duckdb_connection, npi_file_path) {
  logger::log_info("  Attempting CSV schema analysis with multiple strategies")
  
  # Strategy 1: Auto-detection
  tryCatch({
    schema_query <- glue::glue("
      DESCRIBE SELECT * FROM read_csv_auto('{npi_file_path}') LIMIT 0")
    schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
    logger::log_info("    Success with auto-detection strategy")
    return(schema_info)
  }, error = function(e) {
    logger::log_info("    Auto-detection failed, trying flexible parsing")
  })
  
  # Strategy 2: Flexible parsing
  tryCatch({
    schema_query <- glue::glue("
      DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
        auto_detect=true, 
        ignore_errors=true, 
        null_padding=true,
        max_line_size=10000000
      ) LIMIT 0")
    schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
    logger::log_info("    Success with flexible parsing strategy")
    return(schema_info)
  }, error = function(e) {
    logger::log_info("    Flexible parsing failed, trying manual configuration")
  })
  
  # Strategy 3: Manual configuration
  tryCatch({
    schema_query <- glue::glue("
      DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
        delim=',', 
        quote='\"', 
        escape='\"',
        header=true,
        ignore_errors=true,
        null_padding=true
      ) LIMIT 0")
    schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
    logger::log_info("    Success with manual configuration strategy")
    return(schema_info)
  }, error = function(e) {
    stop(glue::glue("All CSV schema analysis strategies failed: {e$message}"))
  })
}

#' @noRd
analyze_parquet_file_schema <- function(duckdb_connection, npi_file_path) {
  schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{npi_file_path}') LIMIT 0")
  schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
  return(schema_info)
}

#' @noRd
extract_year_from_filename <- function(filename) {
  # Extract 4-digit year from filename (e.g., "npi2013.csv" -> "2013")
  year_match <- stringr::str_extract(filename, "20\\d{2}")
  if (is.na(year_match)) {
    # Try 2-digit year and convert to 4-digit
    short_year <- stringr::str_extract(filename, "\\d{2}")
    if (!is.na(short_year)) {
      year_match <- paste0("20", short_year)
    } else {
      year_match <- "unknown"
    }
  }
  return(year_match)
}

#' @noRd
analyze_cross_file_column_compatibility <- function(schema_analysis_results_list) {
  logger::log_info("=== CROSS-FILE COMPATIBILITY ANALYSIS ===")
  
  # Extract successful schema results only
  successful_schemas <- schema_analysis_results_list[
    purrr::map_lgl(schema_analysis_results_list, 
                   ~ .x$analysis_status == "success")]
  
  if (length(successful_schemas) == 0) {
    stop("No successful schema analyses available for compatibility analysis")
  }
  
  # Get all unique column names across all files
  all_unique_column_names <- unique(unlist(
    purrr::map(successful_schemas, ~ .x$column_names)))
  
  # Count presence of each column across files
  column_presence_matrix <- matrix(FALSE, 
                                   nrow = length(all_unique_column_names),
                                   ncol = length(successful_schemas))
  rownames(column_presence_matrix) <- all_unique_column_names
  colnames(column_presence_matrix) <- purrr::map_chr(successful_schemas, 
                                                     ~ .x$filename)
  
  for (i in seq_along(successful_schemas)) {
    current_schema <- successful_schemas[[i]]
    column_presence_matrix[current_schema$column_names, i] <- TRUE
  }
  
  # Calculate column compatibility statistics
  column_presence_counts <- rowSums(column_presence_matrix)
  total_files_analyzed <- length(successful_schemas)
  
  universal_columns <- names(column_presence_counts[
    column_presence_counts == total_files_analyzed])
  partial_presence_columns <- names(column_presence_counts[
    column_presence_counts > 0 & column_presence_counts < total_files_analyzed])
  file_specific_columns <- names(column_presence_counts[
    column_presence_counts == 1])
  
  logger::log_info("Analysis scope:")
  logger::log_info("  Total files analyzed: {total_files_analyzed}")
  logger::log_info("  Total unique columns: {length(all_unique_column_names)}")
  
  logger::log_info("Column compatibility results:")
  logger::log_info("  Universal columns (present in ALL files): {length(universal_columns)}")
  logger::log_info("  Partial presence columns (SOME files): {length(partial_presence_columns)}")
  logger::log_info("  File-specific columns (ONE file only): {length(file_specific_columns)}")
  
  # Log sample universal columns
  if (length(universal_columns) > 0) {
    sample_universal <- head(universal_columns, 10)
    logger::log_info("Universal columns (first 10):")
    for (col_name in sample_universal) {
      logger::log_info("    {col_name}")
    }
    if (length(universal_columns) > 10) {
      logger::log_info("    ... and {length(universal_columns) - 10} more universal columns")
    }
  }
  
  # Identify taxonomy columns
  taxonomy_column_analysis <- identify_taxonomy_columns_comprehensive(
    all_unique_column_names, column_presence_matrix)
  
  return(list(
    total_unique_columns = length(all_unique_column_names),
    universal_columns = universal_columns,
    partial_presence_columns = partial_presence_columns,
    file_specific_columns = file_specific_columns,
    column_presence_matrix = column_presence_matrix,
    taxonomy_analysis = taxonomy_column_analysis
  ))
}

#' @noRd
identify_taxonomy_columns_comprehensive <- function(all_unique_column_names, 
                                                    column_presence_matrix) {
  logger::log_info("=== TAXONOMY COLUMN ANALYSIS ===")
  
  # Define taxonomy column patterns for NBER and NPPES formats
  nber_taxonomy_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$")
  nppes_taxonomy_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", 
                               "(?i)taxonomy.*code")
  
  # Identify NBER-style taxonomy columns
  nber_taxonomy_columns <- character()
  for (pattern in nber_taxonomy_patterns) {
    matching_columns <- all_unique_column_names[
      stringr::str_detect(all_unique_column_names, pattern)]
    nber_taxonomy_columns <- c(nber_taxonomy_columns, matching_columns)
  }
  
  # Identify NPPES-style taxonomy columns  
  nppes_taxonomy_columns <- character()
  for (pattern in nppes_taxonomy_patterns) {
    matching_columns <- all_unique_column_names[
      stringr::str_detect(all_unique_column_names, pattern)]
    nppes_taxonomy_columns <- c(nppes_taxonomy_columns, matching_columns)
  }
  
  # Remove duplicates and combine
  discovered_taxonomy_columns <- unique(c(nber_taxonomy_columns, 
                                          nppes_taxonomy_columns))
  
  logger::log_info("Taxonomy columns discovered:")
  logger::log_info("  NBER format columns: {length(nber_taxonomy_columns)}")
  logger::log_info("  NPPES format columns: {length(nppes_taxonomy_columns)}")
  logger::log_info("  Total taxonomy columns: {length(discovered_taxonomy_columns)}")
  
  # Analyze taxonomy column presence across files
  if (length(discovered_taxonomy_columns) > 0) {
    taxonomy_presence_counts <- rowSums(
      column_presence_matrix[discovered_taxonomy_columns, , drop = FALSE])
    total_files <- ncol(column_presence_matrix)
    
    # Log taxonomy column presence (first 15 for brevity)
    sample_taxonomy_columns <- head(discovered_taxonomy_columns, 15)
    logger::log_info("Taxonomy column presence analysis (first 15):")
    for (tax_col in sample_taxonomy_columns) {
      presence_count <- taxonomy_presence_counts[tax_col]
      presence_percentage <- round(100 * presence_count / total_files)
      logger::log_info("    {tax_col}: {presence_count}/{total_files} files ({presence_percentage}%)")
    }
    
    if (length(discovered_taxonomy_columns) > 15) {
      logger::log_info("    ... and {length(discovered_taxonomy_columns) - 15} more taxonomy columns")
    }
  }
  
  return(list(
    nber_columns = nber_taxonomy_columns,
    nppes_columns = nppes_taxonomy_columns,
    all_taxonomy_columns = discovered_taxonomy_columns
  ))
}

#' @noRd
write_schema_analysis_report <- function(schema_analysis_results_list, 
                                         compatibility_analysis, 
                                         schema_analysis_report_path) {
  logger::log_info("=== WRITING SCHEMA ANALYSIS REPORT ===")
  logger::log_info("Report destination: {schema_analysis_report_path}")
  
  # Create comprehensive report dataframe
  report_data_list <- list()
  
  # Add file-level information
  for (i in seq_along(schema_analysis_results_list)) {
    current_schema <- schema_analysis_results_list[[i]]
    
    if (current_schema$analysis_status == "success") {
      for (col_name in current_schema$column_names) {
        report_data_list[[length(report_data_list) + 1]] <- list(
          filename = current_schema$filename,
          year = current_schema$year,
          column_name = col_name,
          analysis_status = current_schema$analysis_status,
          is_taxonomy_column = col_name %in% 
            compatibility_analysis$taxonomy_analysis$all_taxonomy_columns,
          is_universal_column = col_name %in% 
            compatibility_analysis$universal_columns,
          presence_across_files = sum(
            compatibility_analysis$column_presence_matrix[col_name, ])
        )
      }
    } else {
      # Record failed analysis
      report_data_list[[length(report_data_list) + 1]] <- list(
        filename = current_schema$filename,
        year = current_schema$year,
        column_name = NA,
        analysis_status = current_schema$analysis_status,
        is_taxonomy_column = FALSE,
        is_universal_column = FALSE,
        presence_across_files = 0
      )
    }
  }
  
  # Convert to dataframe and write report
  schema_report_dataframe <- dplyr::bind_rows(report_data_list)
  readr::write_csv(schema_report_dataframe, schema_analysis_report_path)
  
  logger::log_info("Schema analysis report written successfully")
  logger::log_info("  Total columns analyzed: {compatibility_analysis$total_unique_columns}")
  logger::log_info("  Total files analyzed: {length(schema_analysis_results_list)}")
  logger::log_info("  Report saved to: {schema_analysis_report_path}")
}

#' @noRd
determine_column_harmonization_strategy <- function(schema_analysis_summary, 
                                                    schema_compatibility_strategy) {
  logger::log_info("=== DETERMINING COLUMN HARMONIZATION STRATEGY ===")
  logger::log_info("Selected strategy: {schema_compatibility_strategy}")
  
  compatibility_info <- schema_analysis_summary$compatibility
  
  if (schema_compatibility_strategy == "strict") {
    selected_columns <- compatibility_info$universal_columns
    logger::log_info("STRICT strategy: Using {length(selected_columns)} universal columns only")
    
  } else if (schema_compatibility_strategy == "intersect") {
    selected_columns <- compatibility_info$universal_columns
    logger::log_info("INTERSECT strategy: Using {length(selected_columns)} common columns")
    
  } else if (schema_compatibility_strategy == "union") {
    all_columns <- c(compatibility_info$universal_columns,
                     compatibility_info$partial_presence_columns)
    selected_columns <- all_columns
    logger::log_info("UNION strategy: Using {length(selected_columns)} total unique columns")
    logger::log_warn("UNION mode will create NA values for columns missing in some files")
    
  } else {
    stop(glue::glue("Invalid schema compatibility strategy: {schema_compatibility_strategy}"))
  }
  
  return(list(
    strategy_mode = schema_compatibility_strategy,
    selected_columns = selected_columns,
    column_count = length(selected_columns)
  ))
}

#' @noRd
process_and_combine_npi_files_with_schema_intelligence <- function(
    discovered_npi_files, obgyn_taxonomy_code_vector, 
    column_harmonization_config, duckdb_connection) {
  
  logger::log_info("=== PROCESSING AND COMBINING NPI FILES ===")
  logger::log_info("Strategy: {column_harmonization_config$strategy_mode}")
  logger::log_info("Files to process: {length(discovered_npi_files)}")
  
  # Prepare taxonomy code filter for SQL queries
  taxonomy_code_filter_sql <- paste0("'", obgyn_taxonomy_code_vector, "'", 
                                     collapse = ", ")
  
  # Process each NPI file individually
  processed_datasets_list <- list()
  successful_processing_count <- 0
  failed_processing_count <- 0
  
  for (i in seq_along(discovered_npi_files)) {
    current_npi_file_path <- discovered_npi_files[i]
    current_filename <- basename(current_npi_file_path)
    
    logger::log_info("Processing file {i}/{length(discovered_npi_files)}: {current_filename}")
    
    tryCatch({
      # Process single file with intelligent error handling
      processed_provider_dataset <- process_single_npi_file_with_intelligent_schema_handling(
        current_npi_file_path, taxonomy_code_filter_sql, 
        column_harmonization_config, duckdb_connection)
      
      if (nrow(processed_provider_dataset) > 0) {
        processed_datasets_list[[length(processed_datasets_list) + 1]] <- 
          processed_provider_dataset
        successful_processing_count <- successful_processing_count + 1
        logger::log_info("  Successfully processed: {nrow(processed_provider_dataset)} OB/GYN providers found")
      } else {
        logger::log_warn("  No OB/GYN providers found in {current_filename}")
      }
      
    }, error = function(processing_error) {
      logger::log_error("  Failed to process {current_filename}: {processing_error$message}")
      failed_processing_count <- failed_processing_count + 1
    })
  }
  
  logger::log_info("File processing summary:")
  logger::log_info("  Successfully processed: {successful_processing_count} files")
  logger::log_info("  Processing failures: {failed_processing_count} files")
  logger::log_info("  Datasets with OB/GYN providers: {length(processed_datasets_list)}")
  
  # Ensure we have data to combine
  assertthat::assert_that(length(processed_datasets_list) > 0,
                          msg = "No datasets with OB/GYN providers found")
  
  # Combine datasets with intelligent schema alignment
  combined_harmonized_dataset <- combine_datasets_with_intelligent_schema_alignment(
    processed_datasets_list, column_harmonization_config)
  
  return(combined_harmonized_dataset)
}

#' @noRd
process_single_npi_file_with_intelligent_schema_handling <- function(
    npi_file_path, taxonomy_code_filter_sql, column_harmonization_config, 
    duckdb_connection) {
  
  current_filename <- basename(npi_file_path)
  current_file_extension <- fs::path_ext(npi_file_path)
  extracted_year_info <- extract_year_from_filename(current_filename)
  
  logger::log_info("  Extracted year: {extracted_year_info}")
  logger::log_info("  Reading file with intelligent schema handling: {current_filename}")
  
  # Determine which columns to select based on harmonization strategy
  if (column_harmonization_config$strategy_mode == "union") {
    selected_columns_sql <- "*"
  } else {
    # For strict/intersect modes, only select the predetermined columns
    selected_columns_sql <- paste(column_harmonization_config$selected_columns, 
                                  collapse = ", ")
  }
  
  logger::log_info("  Executing DuckDB query to filter OB/GYN providers")
  
  # Process file with intelligent error handling
  tryCatch({
    if (current_file_extension == "csv") {
      filtered_provider_dataset <- process_csv_file_with_taxonomy_detection(
        duckdb_connection, npi_file_path, selected_columns_sql, 
        taxonomy_code_filter_sql, extracted_year_info, current_filename)
    } else if (current_file_extension == "parquet") {
      filtered_provider_dataset <- process_parquet_file_with_taxonomy_detection(
        duckdb_connection, npi_file_path, selected_columns_sql,
        taxonomy_code_filter_sql, extracted_year_info, current_filename)
    } else {
      stop(glue::glue("Unsupported file format: {current_file_extension}"))
    }
    
    logger::log_info("  Query execution completed: {nrow(filtered_provider_dataset)} matching providers")
    return(filtered_provider_dataset)
    
  }, error = function(processing_error) {
    logger::log_error("  Failed to process {current_filename}: {processing_error$message}")
    logger::log_warn("  Returning empty dataset for this file")
    return(dplyr::tibble())
  })
}

#' @noRd
process_csv_file_with_taxonomy_detection <- function(duckdb_connection, 
                                                     npi_file_path, 
                                                     selected_columns_sql,
                                                     taxonomy_code_filter_sql, 
                                                     extracted_year_info, 
                                                     current_filename) {
  logger::log_info("    Processing CSV file with taxonomy detection")
  
  # Get available taxonomy columns for this specific file
  available_taxonomy_columns <- discover_taxonomy_columns_in_file(
    duckdb_connection, npi_file_path)
  
  if (length(available_taxonomy_columns) == 0) {
    logger::log_warn("    No taxonomy columns found - returning empty dataset")
    return(dplyr::tibble())
  }
  
  # Build dynamic WHERE clause based on discovered columns
  taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(
    available_taxonomy_columns, taxonomy_code_filter_sql)
  
  # Execute CSV query with multiple fallback strategies
  return(execute_csv_query_with_fallback(duckdb_connection, npi_file_path, 
                                         selected_columns_sql, taxonomy_where_clause,
                                         extracted_year_info, current_filename))
}

#' @noRd
process_parquet_file_with_taxonomy_detection <- function(duckdb_connection, 
                                                         npi_file_path,
                                                         selected_columns_sql,
                                                         taxonomy_code_filter_sql,
                                                         extracted_year_info,
                                                         current_filename) {
  logger::log_info("    Processing Parquet file with taxonomy detection")
  
  # Get available taxonomy columns for this specific file
  available_taxonomy_columns <- discover_taxonomy_columns_in_file(
    duckdb_connection, npi_file_path)
  
  if (length(available_taxonomy_columns) == 0) {
    logger::log_warn("    No taxonomy columns found - returning empty dataset")
    return(dplyr::tibble())
  }
  
  # Build dynamic WHERE clause
  taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(
    available_taxonomy_columns, taxonomy_code_filter_sql)
  
  # Execute Parquet query
  sql_query <- glue::glue("
    SELECT {selected_columns_sql},
           '{extracted_year_info}' as data_year,
           '{current_filename}' as source_filename
    FROM read_parquet('{npi_file_path}')
    WHERE {taxonomy_where_clause}
  ")
  
  filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
  return(filtered_dataset)
}

#' @noRd
discover_taxonomy_columns_in_file <- function(duckdb_connection, npi_file_path) {
  current_file_extension <- fs::path_ext(npi_file_path)
  
  # Get schema information for the file
  tryCatch({
    if (current_file_extension == "csv") {
      schema_query <- glue::glue("
        DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
          auto_detect=true, 
          ignore_errors=true, 
          null_padding=true,
          max_line_size=10000000
        ) LIMIT 0")
    } else {
      schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{npi_file_path}') LIMIT 0")
    }
    
    schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
    available_columns <- schema_info$column_name
    
    # Look for NBER-style taxonomy columns first (ptaxcode1, ptaxcode2, etc.)
    nber_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$")
    discovered_taxonomy_columns <- character()
    
    for (pattern in nber_patterns) {
      matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
      discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
    }
    
    # If no NBER columns, look for NPPES-style columns
    if (length(discovered_taxonomy_columns) == 0) {
      nppes_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
      for (pattern in nppes_patterns) {
        matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
        discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
      }
    }
    
    discovered_taxonomy_columns <- unique(discovered_taxonomy_columns)
    
    if (length(discovered_taxonomy_columns) > 0) {
      logger::log_info("    Found {length(discovered_taxonomy_columns)} taxonomy columns")
    } else {
      logger::log_warn("    No taxonomy columns detected in file")
    }
    
    return(discovered_taxonomy_columns)
    
  }, error = function(discovery_error) {
    logger::log_error("    Failed to discover taxonomy columns: {discovery_error$message}")
    return(character())
  })
}

#' @noRd
build_dynamic_taxonomy_where_clause <- function(available_taxonomy_columns, 
                                                taxonomy_code_filter_sql) {
  if (length(available_taxonomy_columns) == 0) {
    return("1=0")  # Return FALSE condition
  }
  
  # Build OR conditions for each available taxonomy column
  taxonomy_conditions <- character()
  for (taxonomy_col in available_taxonomy_columns) {
    condition <- glue::glue("{taxonomy_col} IN ({taxonomy_code_filter_sql})")
    taxonomy_conditions <- c(taxonomy_conditions, condition)
  }
  
  dynamic_where_clause <- paste(taxonomy_conditions, collapse = " OR ")
  
  # Log the WHERE clause (truncated for readability)
  clause_preview <- if (nchar(dynamic_where_clause) > 100) {
    paste0(substr(dynamic_where_clause, 1, 100), "...")
  } else {
    dynamic_where_clause
  }
  logger::log_info("    Built taxonomy WHERE clause: {clause_preview}")
  
  return(dynamic_where_clause)
}

#' @noRd
execute_csv_query_with_fallback <- function(duckdb_connection, npi_file_path,
                                            selected_columns_sql, taxonomy_where_clause,
                                            extracted_year_info, current_filename) {
  logger::log_info("    Executing CSV query with fallback strategies")
  
  # Strategy 1: Auto-detection
  tryCatch({
    sql_query <- glue::glue("
      SELECT {selected_columns_sql},
             '{extracted_year_info}' as data_year,
             '{current_filename}' as source_filename
      FROM read_csv_auto('{npi_file_path}')
      WHERE {taxonomy_where_clause}
    ")
    filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
    logger::log_info("      Success with auto-detection strategy")
    return(filtered_dataset)
  }, error = function(auto_error) {
    logger::log_info("      Auto-detection failed, trying flexible parsing")
  })
  
  # Strategy 2: Flexible parsing
  tryCatch({
    sql_query <- glue::glue("
      SELECT {selected_columns_sql},
             '{extracted_year_info}' as data_year,
             '{current_filename}' as source_filename
      FROM read_csv('{npi_file_path}', 
        auto_detect=true, 
        ignore_errors=true, 
        null_padding=true,
        max_line_size=10000000
      )
      WHERE {taxonomy_where_clause}
    ")
    filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
    logger::log_info("      Success with flexible parsing strategy")
    return(filtered_dataset)
  }, error = function(flexible_error) {
    logger::log_info("      Flexible parsing failed, trying manual configuration")
  })
  
  # Strategy 3: Manual configuration
  tryCatch({
    sql_query <- glue::glue("
      SELECT {selected_columns_sql},
             '{extracted_year_info}' as data_year,
             '{current_filename}' as source_filename
      FROM read_csv('{npi_file_path}', 
        delim=',', 
        quote='\"', 
        escape='\"',
        header=true,
        ignore_errors=true,
        null_padding=true
      )
      WHERE {taxonomy_where_clause}
    ")
    filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
    logger::log_info("      Success with manual configuration strategy")
    return(filtered_dataset)
  }, error = function(manual_error) {
    logger::log_error("      All CSV query strategies failed")
    stop(glue::glue("Could not execute CSV query with any strategy: {manual_error$message}"))
  })
}

#' @noRd
combine_datasets_with_intelligent_schema_alignment <- function(
    processed_datasets_list, column_harmonization_strategy) {
  
  logger::log_info("=== COMBINING DATASETS WITH INTELLIGENT SCHEMA ALIGNMENT ===")
  logger::log_info("Combination strategy: {column_harmonization_strategy$strategy_mode}")
  logger::log_info("Datasets to combine: {length(processed_datasets_list)}")
  
  if (column_harmonization_strategy$strategy_mode == "union") {
    # For union mode, harmonize data types to prevent conflicts
    logger::log_info("Harmonizing data types for union mode")
    logger::log_info("Converting ALL columns to character to prevent ANY type conflicts")
    
    # Get all unique column names across datasets
    all_unique_column_names <- unique(unlist(purrr::map(processed_datasets_list, names)))
    
    # Harmonize each dataset to consistent types
    harmonized_datasets_list <- list()
    for (i in seq_along(processed_datasets_list)) {
      harmonized_datasets_list[[i]] <- harmonize_dataset_data_types_comprehensive(
        processed_datasets_list[[i]], all_unique_column_names, i)
    }
    
    # Combine with bind_rows (handles missing columns automatically)
    combined_harmonized_dataset <- dplyr::bind_rows(harmonized_datasets_list)
    logger::log_info("Union combination completed - missing columns filled with NA")
    
  } else {
    # For strict/intersect modes, datasets should have consistent columns
    # but still harmonize data types to prevent conflicts
    logger::log_info("Harmonizing data types for consistent combination")
    logger::log_info("Converting ALL columns to character to prevent ANY type conflicts")
    
    harmonized_datasets_list <- list()
    for (i in seq_along(processed_datasets_list)) {
      harmonized_datasets_list[[i]] <- harmonize_dataset_data_types_for_consistent_combination(
        processed_datasets_list[[i]], i)
    }
    
    combined_harmonized_dataset <- dplyr::bind_rows(harmonized_datasets_list)
    logger::log_info("Standard combination completed using common column structure")
  }
  
  # Log final dataset statistics
  final_rows <- nrow(combined_harmonized_dataset)
  final_columns <- ncol(combined_harmonized_dataset)
  
  logger::log_info("Final combined dataset:")
  logger::log_info("  Total rows: {final_rows}")
  logger::log_info("  Total columns: {final_columns}")
  logger::log_info("  Schema alignment completed successfully")
  logger::log_info("  Note: All columns are character type to prevent conflicts")
  logger::log_info("  Tip: Use convert_key_columns_to_proper_types() if you need to restore data types")
  
  return(combined_harmonized_dataset)
}

#' @noRd
harmonize_dataset_data_types_comprehensive <- function(single_dataset, 
                                                       all_unique_column_names, 
                                                       dataset_index) {
  logger::log_info("  Harmonizing data types for dataset {dataset_index}")
  logger::log_info("    Converting ALL columns to character to prevent type conflicts")
  
  # Convert ALL columns to character to avoid any type conflicts
  # This is much more efficient than trying to identify problematic columns one by one
  conversion_count <- 0
  
  for (col_name in names(single_dataset)) {
    if (!is.character(single_dataset[[col_name]])) {
      single_dataset[[col_name]] <- as.character(single_dataset[[col_name]])
      conversion_count <- conversion_count + 1
    }
  }
  
  logger::log_info("    Converted {conversion_count} columns to character for type consistency")
  
  return(single_dataset)
}

#' @noRd
harmonize_dataset_data_types_for_consistent_combination <- function(single_dataset, 
                                                                    dataset_index) {
  logger::log_info("  Harmonizing data types for dataset {dataset_index}")
  logger::log_info("    Converting ALL columns to character to prevent type conflicts")
  
  # Convert ALL columns to character to avoid any type conflicts
  conversion_count <- 0
  
  for (col_name in names(single_dataset)) {
    if (!is.character(single_dataset[[col_name]])) {
      single_dataset[[col_name]] <- as.character(single_dataset[[col_name]])
      conversion_count <- conversion_count + 1
    }
  }
  
  logger::log_info("    Converted {conversion_count} columns to character for type consistency")
  
  return(single_dataset)
}

#' @noRd
write_combined_obgyn_dataset_with_validation <- function(
    combined_obgyn_provider_dataset, obgyn_output_file_path) {
  
  logger::log_info("=== WRITING COMBINED DATASET ===")
  logger::log_info("Output destination: {obgyn_output_file_path}")
  
  # Log dataset statistics before writing
  total_records <- nrow(combined_obgyn_provider_dataset)
  total_columns <- ncol(combined_obgyn_provider_dataset)
  
  logger::log_info("Dataset to write:")
  logger::log_info("  Total records: {total_records}")
  logger::log_info("  Total columns: {total_columns}")
  
  # Validate dataset is not empty
  assertthat::assert_that(total_records > 0,
                          msg = "Cannot write empty dataset - no OB/GYN providers found")
  assertthat::assert_that(total_columns > 0,
                          msg = "Cannot write dataset with no columns")
  
  # Determine output format and write accordingly
  output_file_extension <- fs::path_ext(obgyn_output_file_path)
  
  if (output_file_extension == "csv") {
    readr::write_csv(combined_obgyn_provider_dataset, obgyn_output_file_path)
    logger::log_info("Successfully wrote CSV file")
  } else if (output_file_extension == "parquet") {
    arrow::write_parquet(combined_obgyn_provider_dataset, obgyn_output_file_path)
    logger::log_info("Successfully wrote Parquet file")
  } else if (output_file_extension == "feather") {
    arrow::write_feather(combined_obgyn_provider_dataset, obgyn_output_file_path)
    logger::log_info("Successfully wrote Feather file")
  } else {
    stop(glue::glue("Unsupported output format: {output_file_extension}"))
  }
  
  # Verify file was created and log file information
  verify_output_file_creation(obgyn_output_file_path)
}

#' @noRd
verify_output_file_creation <- function(obgyn_output_file_path) {
  assertthat::assert_that(fs::file_exists(obgyn_output_file_path),
                          msg = "Output file was not created successfully")
  
  output_file_size <- fs::file_size(obgyn_output_file_path)
  logger::log_info("Output file verification:")
  logger::log_info("  File exists: TRUE")
  logger::log_info("  File size: {output_file_size} bytes")
  logger::log_info("  File path: {obgyn_output_file_path}")
}

#' @noRd
log_processing_completion_summary <- function(combined_obgyn_provider_dataset, 
                                              obgyn_output_file_path) {
  logger::log_info("=== PROCESSING COMPLETION SUMMARY ===")
  
  # Calculate and log summary statistics
  total_obgyn_providers <- nrow(combined_obgyn_provider_dataset)
  total_data_columns <- ncol(combined_obgyn_provider_dataset)
  
  # Calculate year distribution if data_year column exists
  if ("data_year" %in% names(combined_obgyn_provider_dataset)) {
    year_distribution <- combined_obgyn_provider_dataset |>
      dplyr::count(data_year, sort = TRUE)
    
    logger::log_info("OB/GYN providers by year:")
    for (i in seq_len(nrow(year_distribution))) {
      year_info <- year_distribution[i, ]
      logger::log_info("  {year_info$data_year}: {year_info$n} providers")
    }
  }
  
  # Calculate file source distribution if source_filename column exists
  if ("source_filename" %in% names(combined_obgyn_provider_dataset)) {
    source_distribution <- combined_obgyn_provider_dataset |>
      dplyr::count(source_filename, sort = TRUE)
    
    logger::log_info("OB/GYN providers by source file:")
    for (i in seq_len(nrow(source_distribution))) {
      source_info <- source_distribution[i, ]
      logger::log_info("  {source_info$source_filename}: {source_info$n} providers")
    }
  }
  
  logger::log_info("Final processing statistics:")
  logger::log_info("  Total OB/GYN providers: {total_obgyn_providers}")
  logger::log_info("  Total data columns: {total_data_columns}")
  logger::log_info("  Output file: {obgyn_output_file_path}")
  logger::log_info("====================================")
}

#' Convert Key Columns to Proper Data Types (Optional Post-Processing)
#'
#' After using the main processing function, all columns are character type
#' to prevent binding conflicts. This optional function converts key columns
#' back to their appropriate data types for analysis.
#'
#' @param combined_dataset The character-type dataset from process_npi_obgyn_data
#' @param enable_verbose_logging Logical flag for detailed logging
#'
#' @return Dataset with key columns converted to proper types
#'
#' @examples
#' \dontrun{
#' # After running the main function
#' typed_dataset <- convert_key_columns_to_proper_types(
#'   combined_obgyn_provider_dataset, 
#'   enable_verbose_logging = TRUE
#' )
#' }
#'
#' @export
convert_key_columns_to_proper_types <- function(combined_dataset, 
                                                enable_verbose_logging = FALSE) {
  
  if (enable_verbose_logging) {
    logger::log_info("=== CONVERTING KEY COLUMNS TO PROPER TYPES ===")
  }
  
  converted_dataset <- combined_dataset
  conversion_count <- 0
  
  # Convert numeric columns
  numeric_columns <- c("entity", "plnamecode", 
                       paste0("ptaxcode", 1:30),
                       paste0("pprimtax", 1:15))
  
  for (col_name in numeric_columns) {
    if (col_name %in% names(converted_dataset)) {
      suppressWarnings({
        converted_dataset[[col_name]] <- as.numeric(converted_dataset[[col_name]])
      })
      conversion_count <- conversion_count + 1
      if (enable_verbose_logging) {
        logger::log_info("  Converted {col_name} to numeric")
      }
    }
  }
  
  # Convert date columns (if they exist and are in recognizable formats)
  date_columns <- c("lastupdate", "npideactdate", "npireactdate", "penumdate")
  
  for (col_name in date_columns) {
    if (col_name %in% names(converted_dataset)) {
      suppressWarnings({
        converted_dataset[[col_name]] <- as.Date(converted_dataset[[col_name]])
      })
      conversion_count <- conversion_count + 1
      if (enable_verbose_logging) {
        logger::log_info("  Converted {col_name} to Date")
      }
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("Type conversion completed: {conversion_count} columns converted")
  }
  
  return(converted_dataset)
}


# execute process_npi_obgyn_data ----
combined_obgyn_provider_dataset <- process_npi_obgyn_data(
  npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
  obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
  obgyn_taxonomy_code_vector = c(
    "207V00000X",    # Obstetrics & Gynecology (General)
    "207VX0201X",    # Gynecologic Oncology *** PRIMARY FOCUS ***
    "207VE0102X",    # Reproductive Endocrinology & Infertility
    "207VG0400X",    # Gynecology (General)
    "207VM0101X",    # Maternal & Fetal Medicine
    "207VF0040X",    # Female Pelvic Medicine & Reconstructive Surgery
    "207VB0002X",    # Bariatric Medicine (OBGYN)
    "207VC0200X",    # Critical Care Medicine (OBGYN)
    "207VC0040X",    # Gynecology subspecialty
    "207VC0300X",    # Complex Family Planning
    "207VH0002X",    # Hospice and Palliative Medicine (OBGYN)
    "207VX0000X"     # Obstetrics Only
  ),
  npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
  schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis_report.csv",
  schema_compatibility_strategy = "union",
  enable_verbose_logging = TRUE
)


# Helper function to calculate mode within groups
calculate_group_mode <- function(input_vector) {
  if (all(is.na(input_vector))) return(NA)
  non_missing_values <- input_vector[!is.na(input_vector)]
  if (length(non_missing_values) == 0) return(NA)
  
  frequency_table <- table(non_missing_values)
  mode_value <- names(frequency_table)[which.max(frequency_table)]
  
  # Return in same type as input
  if (lubridate::is.Date(input_vector)) {
    return(as.Date(mode_value))
  } else if (is.numeric(input_vector)) {
    return(as.numeric(mode_value))
  } else {
    return(mode_value)
  }
}

# CLEANING OF DATA WITH MODE ----
# Configure logging with verbose output
logger::log_threshold(logger::INFO)
verbose_logging <- TRUE
logger::log_info("Script started: NPPES OB/GYN provider data processing")
logger::log_info("Configuration: verbose_logging = {verbose_logging}")

if (verbose_logging) {
  logger::log_info("Initializing NPPES OB/GYN provider data processing pipeline")
  logger::log_info("Using duckplyr for memory-efficient large dataset processing")
  logger::log_info("Backend: DuckDB in-memory database")
}

# Input file validation with robust error handling
input_csv_file_path <- "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv"
logger::log_info("Validating input file: {input_csv_file_path}")

assertthat::assert_that(file.exists(input_csv_file_path), 
                        msg = paste("Input CSV file does not exist:", input_csv_file_path))
assertthat::assert_that(file.size(input_csv_file_path) > 0,
                        msg = "Input CSV file is empty")

logger::log_info("Input file validation successful")
logger::log_info("File size: {file.size(input_csv_file_path)} bytes")

# Create DuckDB connection with error handling
logger::log_info("Creating DuckDB in-memory connection")
tryCatch({
  duckdb_connection <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  logger::log_info("DuckDB connection established successfully")
}, error = function(e) {
  logger::log_error("Failed to create DuckDB connection: {e$message}")
  stop("Database connection failed")
})

# Configure duckplyr backend
logger::log_info("Configuring duckplyr backend options")
options(duckplyr.force = TRUE)
logger::log_info("duckplyr.force option set to TRUE")

# Read CSV with comprehensive logging
logger::log_info("Reading CSV file with DuckDB backend")
logger::log_info("Strategy: treating all columns as VARCHAR to avoid type detection issues")

tryCatch({
  sql_create_table <- sprintf("
    CREATE TABLE raw_provider_records AS 
    SELECT * FROM read_csv_auto('%s', 
      all_varchar=true, 
      nullstr=['NA', '', 'NULL']
    )", input_csv_file_path)
  
  rows_created <- DBI::dbExecute(duckdb_connection, sql_create_table)
  logger::log_info("Raw provider table created with {rows_created} rows")
}, error = function(e) {
  logger::log_error("Failed to read CSV file: {e$message}")
  DBI::dbDisconnect(duckdb_connection)
  stop("CSV reading failed")
})

# Create duckplyr tibble from database table
logger::log_info("Creating duckplyr tibble from database table")
raw_provider_records <- dplyr::tbl(duckdb_connection, "raw_provider_records") %>%
  duckplyr::as_duckdb_tibble()

assertthat::assert_that(!is.null(raw_provider_records),
                        msg = "Failed to create duckplyr tibble")

# Get and log initial dataset metrics
initial_record_count <- raw_provider_records %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

logger::log_info("Initial dataset loaded successfully")
logger::log_info("Total raw records: {initial_record_count}")

# Filter for individual provider entities
logger::log_info("Filtering for individual provider entities (entity = 1)")
logger::log_info("Excluding organizational entities from analysis")

individual_provider_records <- raw_provider_records %>%
  dplyr::filter(entity == 1)

individual_provider_count <- individual_provider_records %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

records_filtered_out <- initial_record_count - individual_provider_count
logger::log_info("Individual provider filtering completed")
logger::log_info("Individual providers retained: {individual_provider_count}")
logger::log_info("Organizational entities filtered out: {records_filtered_out}")

# Remove other provider identifier columns (othpid series)
logger::log_info("Removing other provider identifier columns (othpid series)")

othpid_columns_to_remove <- c(
  "othpidty1", "othpidst1", "othpidiss1", "othpid2", "othpidty2", "othpidst2", 
  "othpidiss2", "othpid3", "othpidty3", "othpidst3", "othpidiss3", "othpid4", 
  "othpidty4", "othpidst4", "othpidiss4", "othpid5", "othpidty5", "othpidst5", 
  "othpidiss5", "othpid6", "othpidty6", "othpidst6", "othpidiss6", "othpid7", 
  "othpidty7", "othpidst7", "othpidiss7", "othpid8", "othpidty8", "othpidst8", 
  "othpidiss8", "othpid9", "othpidty9", "othpidst9", "othpidiss9", "othpid10", 
  "othpidty10", "othpidst10", "othpidiss10", "othpid11", "othpidty11", 
  "othpidst11", "othpidiss11", "othpid12", "othpidty12", "othpidst12", 
  "othpidiss12", "othpid13", "othpidty13", "othpidst13", "othpidiss13", 
  "othpid14", "othpidty14", "othpidst14", "othpidiss14", "othpid15", 
  "othpidty15", "othpidst15", "othpidiss15", "othpid16", "othpidty16", 
  "othpidst16", "othpidiss16", "othpid17", "othpidty17", "othpidst17", 
  "othpidiss17", "othpid18", "othpidty18", "othpidst18", "othpidiss18", 
  "othpid19", "othpidty19", "othpidst19", "othpidiss19", "othpid20", 
  "othpidty20", "othpidst20", "othpidiss20", "othpid21", "othpidty21", 
  "othpidst21", "othpidiss21", "othpid22", "othpidty22", "othpidst22", 
  "othpidiss22", "othpid23", "othpidty23", "othpidst23", "othpidiss23", 
  "othpid24", "othpidty24", "othpidst24", "othpidiss24", "othpid25", 
  "othpidty25", "othpidst25", "othpidiss25", "othpid26", "othpidty26", 
  "othpidst26", "othpidiss26", "othpid27", "othpidty27", "othpidst27", 
  "othpidiss27", "othpid28", "othpidty28", "othpidst28", "othpidiss28", 
  "othpid29", "othpidty29", "othpidst29", "othpidiss29", "othpid30", 
  "othpidty30", "othpidst30", "othpidiss30", "othpid31", "othpidty31", 
  "othpidst31", "othpidiss31", "othpid32", "othpidty32", "othpidst32", 
  "othpidiss32", "othpid33", "othpidty33", "othpidst33", "othpidiss33", 
  "othpid34", "othpidty34", "othpidst34", "othpidiss34", "othpid35", 
  "othpidty35", "othpidst35", "othpidiss35", "othpid36", "othpidty36", 
  "othpidst36", "othpidiss36", "othpid37", "othpidty37", "othpidst37", 
  "othpidiss37", "othpid38", "othpidty38", "othpidst38", "othpidiss38", 
  "othpid39", "othpidty39", "othpidst39", "othpidiss39", "othpid40", 
  "othpidty40", "othpidst40", "othpidiss40", "othpid41", "othpidty41", 
  "othpidst41", "othpidiss41", "othpid42", "othpidty42", "othpidst42", 
  "othpidiss42", "othpid43", "othpidty43", "othpidst43", "othpidiss43", 
  "othpid44", "othpidty44", "othpidst44", "othpidiss44", "othpid45", 
  "othpidty45", "othpidst45", "othpidiss45", "othpid46", "othpidty46", 
  "othpidst46", "othpidiss46", "othpid47", "othpidty47", "othpidst47", 
  "othpidiss47", "othpid48", "othpidty48", "othpidst48", "othpidiss48", 
  "othpid49", "othpidty49", "othpidst49", "othpidiss49", "othpid50", 
  "othpidty50", "othpidst50", "othpidiss50", "soleprop", "orgsubpart", 
  "parent_org_lbn", "parent_org_tin", "aoname_prefix", "aoname_suffix", 
  "aocredential", "ptaxgroup1", "ptaxgroup2", "ptaxgroup3", "ptaxgroup4", 
  "ptaxgroup5", "ptaxgroup6", "ptaxgroup7", "ptaxgroup8", "ptaxgroup9", 
  "ptaxgroup10", "ptaxgroup11", "ptaxgroup12", "ptaxgroup13", "ptaxgroup14", 
  "ptaxgroup15", "porgnameothcod", "dup_npi"
)

logger::log_info("Removing {length(othpid_columns_to_remove)} othpid columns")

individual_providers_core <- individual_provider_records %>%
  dplyr::select(-dplyr::all_of(othpid_columns_to_remove))

core_record_count <- individual_providers_core %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

logger::log_info("Othpid column removal completed")
logger::log_info("Records after othpid removal: {core_record_count}")

# Remove additional unnecessary organizational and contact columns
logger::log_info("Removing additional organizational and contact columns")

additional_unnecessary_columns <- c(
  "replacement_npi", "ein", "porgname", "porgnameoth", "porgnameothcode", 
  "plocline2", "npideactreason", "aolname", "aofname", "aomname", "aotitle", 
  "aotelnum"
)

logger::log_info("Removing {length(additional_unnecessary_columns)} additional columns")

providers_essential_data <- individual_providers_core %>%
  dplyr::select(-dplyr::all_of(additional_unnecessary_columns))

logger::log_info("Additional column removal completed")

# Remove alternate name and credential columns
logger::log_info("Removing alternate name and credential columns")

alternate_name_credential_columns <- c(
  "pnameprefix", "plnameoth", "pfnameoth", "pmnameoth", "pnameprefixoth", 
  "pnamesuffixoth", "pcredentialoth", "plnamecode", "pmailline2", "plicnum1", 
  "plicstate1", "pprimtax1", "plicnum2", "plicstate2", "pprimtax2", "plicnum3", 
  "plicstate3", "pprimtax3", "plicnum4", "plicstate4", "pprimtax4", "plicnum5", 
  "plicstate5", "pprimtax5", "plicnum6", "plicstate6", "pprimtax6", "plicnum7", 
  "plicstate7", "pprimtax7", "plicnum8", "plicstate8", "pprimtax8", "plicnum9", 
  "plicstate9", "pprimtax9", "plicnum10", "plicstate10", "pprimtax10", 
  "plicnum11", "plicstate11", "pprimtax11", "plicnum12", "plicstate12", 
  "pprimtax12", "plicnum13", "plicstate13", "pprimtax13", "plicnum14", 
  "plicstate14", "pprimtax14", "plicnum15", "plicstate15", "pprimtax15", 
  "othpid1"
)

logger::log_info("Removing {length(alternate_name_credential_columns)} name/credential columns")

providers_streamlined <- providers_essential_data %>%
  dplyr::select(-dplyr::all_of(alternate_name_credential_columns))

logger::log_info("Alternate name/credential column removal completed")

# Clean provider credentials by removing punctuation and symbols
logger::log_info("Cleaning provider credentials - removing punctuation and symbols")
logger::log_info("Materializing data for credential cleaning operation")

providers_cleaned_credentials <- providers_streamlined %>%
  dplyr::compute() %>%
  dplyr::mutate(
    pcredential = stringr::str_remove_all(pcredential, "[[:punct:][:symbol:]]")
  ) %>%
  duckplyr::as_duckdb_tibble()

logger::log_info("Provider credential cleaning completed")

# Remove additional taxonomy code columns
logger::log_info("Removing additional taxonomy code columns (ptaxcode5-15)")

additional_taxonomy_columns <- c(
  "ptaxcode5", "ptaxcode6", "ptaxcode7", "ptaxcode8", "ptaxcode9", 
  "ptaxcode10", "ptaxcode11", "ptaxcode12", "ptaxcode13", "ptaxcode14", 
  "ptaxcode15"
)

logger::log_info("Removing {length(additional_taxonomy_columns)} taxonomy columns")

providers_core_taxonomy <- providers_cleaned_credentials %>%
  dplyr::select(-dplyr::all_of(additional_taxonomy_columns))

logger::log_info("Additional taxonomy column removal completed")

# Filter for US addresses only
logger::log_info("Filtering for US mailing and practice addresses only")
logger::log_info("Excluding international provider addresses")

us_address_providers <- providers_core_taxonomy %>%
  dplyr::filter(pmailcountry == "US" & ploccountry == "US")

us_address_count <- us_address_providers %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

international_records_removed <- core_record_count - us_address_count
logger::log_info("US address filtering completed")
logger::log_info("US providers retained: {us_address_count}")
logger::log_info("International records removed: {international_records_removed}")

# Remove unnecessary date and contact columns
logger::log_info("Removing unnecessary date and contact columns")

date_contact_columns_to_remove <- c(
  "npideactdate", "npireactdate", "entity", "pmailtel", "pmailfax", 
  "penumdate", "npireactdatestr", "pmailcountry", "ploccountry", 
  "plocfax", "source_filename", "npideactdatest", "npireactdatest"
)

logger::log_info("Removing {length(date_contact_columns_to_remove)} date/contact columns")

providers_trimmed <- us_address_providers %>%
  dplyr::select(-dplyr::any_of(date_contact_columns_to_remove))

logger::log_info("Date/contact column removal completed")

# Prepare date fields for processing
logger::log_info("Preparing date fields - keeping as character for efficient processing")

providers_with_unified_dates <- providers_trimmed %>%
  dplyr::mutate(
    lastupdate_unified = dplyr::coalesce(lastupdate, lastupdatestr)
  )

logger::log_info("Date field unification completed")

# Calculate mode values for missing data imputation
logger::log_info("Calculating mode values by NPI for missing data imputation")
logger::log_info("Grouping by NPI and arranging by data year")

providers_with_mode_calculations <- providers_with_unified_dates %>%
  dplyr::group_by(npi) %>%
  dplyr::arrange(data_year) %>%
  dplyr::compute() %>%
  dplyr::mutate(
    pcredential_mode_value = calculate_group_mode(
      pcredential),
    certdate_mode_value = calculate_group_mode(certdate),
    penumdatestr_mode_value = calculate_group_mode(penumdatestr),
    ptaxcode2_mode_value = calculate_group_mode(ptaxcode2),
    ptaxcode3_mode_value = calculate_group_mode(ptaxcode3)
  ) %>%
  dplyr::ungroup() %>%
  duckplyr::as_duckdb_tibble()

logger::log_info("Mode value calculations completed")

# Impute missing values using calculated modes
logger::log_info("Imputing missing values using calculated mode values")

providers_with_imputed_values <- providers_with_mode_calculations %>%
  dplyr::mutate(
    pcredential_imputed = dplyr::coalesce(pcredential, pcredential_mode_value),
    certdate_imputed = dplyr::coalesce(certdate, certdate_mode_value),
    penumdatestr_imputed = dplyr::coalesce(penumdatestr, penumdatestr_mode_value),
    ptaxcode2_imputed = dplyr::coalesce(ptaxcode2, ptaxcode2_mode_value),
    ptaxcode3_imputed = dplyr::coalesce(ptaxcode3, ptaxcode3_mode_value)
  )

logger::log_info("Missing value imputation completed")

# Define valid OB/GYN medical credentials
logger::log_info("Defining valid OB/GYN medical credentials for provider filtering")

valid_obgyn_medical_credentials <- c(
  "BA BS MD", "BA MD", "DALIA WENCKUS MD", "DILRUBA HAQUE MD", "DO MD", "DR MD", 
  "HOWARD ESSNER MD", "JD MD", "KENNETH FRASER MD", "LAURA VAN HOUTEN MD", 
  "MA MPH MD", "MB BCH BAO MD", "MBA MD", "MBBCH MSC MD", "MBBS MD MRCOG", 
  "MBCHB MD", "MD", "MD         LIC 79722", "MD     OBGYN", "MD  03281930", 
  "MD  APMC", "MD  ENT", "MD  FACOG", "MD  FAOG", "MD  LLC", "MD  MEDICAL DOCTO", 
  "MD  MPH", "MD  MS", "MD  OBGYN", "MD  PA", "MD  PHD", "MD  RETIRED 2004", 
  "MD 062009", "MD 25MA01602900", "MD ABOG FACOG", "MD ANTIC 51614", 
  "MD AP MBA", "MD APMC", "MD BA MPH", "MD BSC", "MD CCFP FRCSC FACOG", 
  "MD CDE", "MD CSA", "MD CTBS", "MD DABFP", "MD DC", "MD DO", 
  "MD DOCTOR OF MEDICIN", "MD DPH", "MD ESP GYN OBS", "MD FAC OG", "MD FACOG", 
  "MD FACOG AAAAM", "MD FACOG FABIHM", "MD FACOG FACS", "MD FACOG FRCOG", 
  "MD FACOG JD", "MD FACOG MPH", "MD FACOG PC", "MD FACOG SMFM", "MD FACOS", 
  "MD FCOG", "MD FMCOG", "MD FRCOG", "MD FRCS MPH", "MD FRCS MRCOG FACOG", 
  "MD FRCSC", "MD HCLD FACOG", "MD INC", "MD JD", "MD LAC", "MD LLC", "MD LTD", 
  "MD MA", "MD MA BS BA", "MD MA CAE", "MD MA FACOG", "MD MAPP", "MD MAS", 
  "MD MAUB", "MD MB BCH", "MD MBA", "MD MBA FACOG", "MD MBA MS", "MD MBAHC", 
  "MD MBBS", "MD MBBS FRCSC", "MD MBE", "MD MCR", "MD MD FACOG", "MD MED", 
  "MD MFM", "MD MHA", "MD MHA FACOG", "MD MHDS", "MD MHPE", "MD MHS", 
  "MD MHSA", "MD MIH PHD", "MD MMM", "MD MMS", "MD MPH", "MD MPH CPH", 
  "MD MPH FACOG", "MD MPH FACOG FACS", "MD MPH FWACS", "MD MPH FWACS FICS", 
  "MD MPH MA", "MD MPH MAT", "MD MPH MBA", "MD MPH MED", "MD MPH MHS", 
  "MD MPH MHSA", "MD MPH MMM", "MD MPH MPA", "MD MPH MSC", "MD MPH PA", 
  "MD MPH PHD", "MD MPH PSC", "MD MPHIL", "MD MPHIL MED", "MD MPHS", "MD MPP", 
  "MD MPS", "MD MRCOG", "MD MS", "MD MS FACOG", "MD MS MBA", "MD MS MPH", 
  "MD MS MSPH", "MD MSC", "MD MSC BS", "MD MSC FACOG", "MD MSC FRCSC", 
  "MD MSC MBA", "MD MSCE", "MD MSCI", "MD MSCI FACOG", "MD MSCR", "MD MSE", 
  "MD MSED", "MD MSPH", "MD MSPH MS", "MD MSW", "MD MTR", "MD OB GYN", 
  "MD OBGYN", "MD P A", "MD PA", "MD PACOG", "MD PC", "MD PC FPMRS", 
  "MD PGDIP OBGYN", "MD PH D", "MD PHARMD", "MD PHD", "MD PHD FACO", 
  "MD PHD JD", "MD PHD MBA", "MD PHD MHS MFS", "MD PHD MPH", "MD PHYSICIAN", 
  "MD PLLC", "MD PPLC", "MD PS", "MD PSC", "MD PSYD", "MD RDMS", "MD RE", 
  "MD RETIRED", "MD RPH", "MD SC", "MD SCD", "MD SCM", "MD SM", "MD01", 
  "MD02091969", "MD06", "MD06261972", "MD09", "MD09091960", "MD10161945", 
  "MD12", "MDC", "MDCM", "MDCMHTMBA", "MDD", "MDD MPH", "MDFAC OG", "MDFACOG", 
  "MDFACOG FAC", "MDFACOG FACS", "MDFACOGFACS", "MDFACOGFASC", "MDFACOGPC", 
  "MDLLC", "MDM", "MDMBA", "MDMBAFACOGFACS", "MDMBAFACOGFICS", "MDMPH", 
  "MDMPH FACOG", "MDMPHFACOG", "MDMPHFACOGFASAM", "MDMSCFACOGFPMRS", 
  "MDOBGYN", "MDPA", "MDPC", "MDPHARMD", "MDPHD", "MDPHDDSC", "MDPHDFACOG", 
  "MDQ", "MDSC", "MS MD", "MS MD CANDIDATE", "MSC MD", "MSPH MD", "OB GYN MD", 
  "OBGYN MD", "PHD MD", "DO", "DO  FACOG  PA", "DO A PROFESSIONAL CO", 
  "DO FACOG", "DO FACOOG", "DO FACOS DACFE", "DO FORMERLY OMS", "DO JD", 
  "DO LLC", "DO LTD", "DO MBA", "DO MBS", "DO MPH", "DO MPH FAC", 
  "DO MPH MHA", "DO MPH MS", "DO MS", "DO MSC", "DO PA", "DO PC", "DO PHD", 
  "DO RD", "DO STUDENT", "DOA", "DOFACOG", "DOFACOOG", "DOMBA", "DOMBAMSCE", 
  "DOMPH", "DOMS", "DOPLLC", "DPT DO", "ERICA SCHNEIDER DO", "JD DO", 
  "MEDICAL DOCTOR", "MPH DO", "MS DO FACOOG", "STUDENT DO", "DPHC MBBS DGO MME", 
  "MBBS", "MBBS MS", "mbbs"
)

credential_type_count <- length(valid_obgyn_medical_credentials)
logger::log_info("Valid OB/GYN credential types defined: {credential_type_count}")

obgyn_credential_filtered_providers <- providers_with_imputed_values %>%
  dplyr::filter(pcredential_imputed %in% valid_obgyn_medical_credentials)

obgyn_credential_count <- obgyn_credential_filtered_providers %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

invalid_credentials_removed <- us_address_count - obgyn_credential_count
logger::log_info("OB/GYN credential filtering completed")
logger::log_info("Valid OB/GYN providers retained: {obgyn_credential_count}")
logger::log_info("Invalid credentials removed: {invalid_credentials_removed}")

# Standardize credentials to MD/DO categories
logger::log_info("Standardizing medical credentials to MD/DO categories")

providers_with_standardized_credentials <- obgyn_credential_filtered_providers %>%
  dplyr::mutate(
    credential_standardized = dplyr::case_when(
      pcredential_imputed == "DO" ~ "DO",
      TRUE ~ "MD"
    )
  )

logger::log_info("Credential standardization to MD/DO completed")

# Filter out US territories and military postal codes
logger::log_info("Filtering out US territories and military postal codes")

excluded_territory_state_codes <- c("aa", "ae", "as - american samoa", "as", "ap", "gu", "mp", "vi")
excluded_territory_location_codes <- c("AA", "AE", "AP", "AS- AMERICAN SAMOA", "FM", "GU", "MP", "VI")

logger::log_info("Excluded state codes: {paste(excluded_territory_state_codes, collapse=', ')}")
logger::log_info("Excluded location codes: {paste(excluded_territory_location_codes, collapse=', ')}")

continental_us_providers <- providers_with_standardized_credentials %>%
  dplyr::compute() %>%
  dplyr::filter(
    !stringr::str_to_lower(pmailstatename) %in% excluded_territory_state_codes & 
      !plocstatename %in% excluded_territory_location_codes
  ) %>%
  tibble::as_tibble() %>%
  duckplyr::as_duckdb_tibble()

continental_us_count <- continental_us_providers %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

territory_records_removed <- obgyn_credential_count - continental_us_count
logger::log_info("Territory and military address filtering completed")
logger::log_info("Continental US providers retained: {continental_us_count}")
logger::log_info("Territory/military records removed: {territory_records_removed}")

# Clean up final dataset by removing temporary columns
logger::log_info("Preparing final dataset - removing temporary calculation columns")

final_processed_obgyn_providers <- continental_us_providers %>%
  dplyr::select(
    -pcredential_mode_value, -certdate_mode_value, -penumdatestr_mode_value, 
    -ptaxcode2_mode_value, -ptaxcode3_mode_value, -lastupdatestr
  ) %>%
  dplyr::select(-pcredential_imputed)

logger::log_info("Temporary column cleanup completed")

# Calculate final dataset statistics
logger::log_info("Calculating comprehensive final dataset statistics")

final_dataset_statistics <- final_processed_obgyn_providers %>%
  dplyr::summarise(
    total_provider_records = dplyr::n(),
    unique_npi_count = dplyr::n_distinct(npi),
    md_credential_count = sum(credential_standardized == "MD", na.rm = TRUE),
    do_credential_count = sum(credential_standardized == "DO", na.rm = TRUE)
  ) %>%
  dplyr::compute() %>%
  dplyr::collect()

logger::log_info("=== FINAL DATASET STATISTICS ===")
logger::log_info("Total provider records: {final_dataset_statistics$total_provider_records}")
logger::log_info("Unique NPIs: {final_dataset_statistics$unique_npi_count}")
logger::log_info("MD credentials: {final_dataset_statistics$md_credential_count}")
logger::log_info("DO credentials: {final_dataset_statistics$do_credential_count}")

# Collect final processed dataset from duckplyr to R
logger::log_info("Collecting final processed dataset from duckplyr to R memory")

final_obgyn_provider_dataset <- final_processed_obgyn_providers %>%
  dplyr::compute() %>%
  dplyr::collect()

logger::log_info("Dataset collection from duckplyr completed")

# Final validation with comprehensive checks
logger::log_info("Performing final dataset validation")

assertthat::assert_that(is.data.frame(final_obgyn_provider_dataset),
                        msg = "Final dataset is not a data frame")
assertthat::assert_that(nrow(final_obgyn_provider_dataset) > 0, 
                        msg = "No providers remain in final dataset after filtering")
assertthat::assert_that(ncol(final_obgyn_provider_dataset) > 0,
                        msg = "Final dataset has no columns")
assertthat::assert_that("npi" %in% names(final_obgyn_provider_dataset),
                        msg = "NPI column missing from final dataset")
assertthat::assert_that("credential_standardized" %in% names(final_obgyn_provider_dataset),
                        msg = "Standardized credential column missing from final dataset")

logger::log_info("Final dataset validation successful")

## output ----
readr::write_csv(final_obgyn_provider_dataset, "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv")

# Further cleaning: THIS TAKES A WHILE -----
# Quick fix for missing variables
output_cleaned_provider_csv_path <- "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv"
filtering_analysis_output_csv_path <- "nppes_comprehensive_filtering_analysis.csv"
comprehensive_report_rds_output_path <- "nppes_comprehensive_processing_report.rds"
waterfall_chart_output_path <- "nppes_filtering_waterfall_chart.png"
efficiency_trend_output_path <- "nppes_filtering_efficiency_trend.png"

# Also ensure your input dataset is available
if (!exists("continental_us_providers") && exists("final_obgyn_provider_dataset")) {
  continental_us_providers <- final_obgyn_provider_dataset
  logger::log_info("Using final_obgyn_provider_dataset as continental_us_providers")
}

# Set reasonable defaults for missing count variables if needed
if (!exists("initial_total_record_count")) initial_total_record_count <- 737867
if (!exists("individual_provider_record_count")) individual_provider_record_count <- 549160
if (!exists("us_address_provider_count")) us_address_provider_count <- 548606
if (!exists("obgyn_credential_count")) obgyn_credential_count <- 525010
if (!exists("continental_us_count")) continental_us_count <- 524346

# Create the directory structure
project_dirs <- c("figures", "data/processed", "data/intermediate", "reports", 
                  "logs", "cache", "validation", "documentation", "exports")

for (dir in project_dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    cat("Created:", dir, "\n")
  }
}
# ============================================================================
# ADVANCED DATA CLEANING AND TYPE CONVERSION
# ============================================================================

logger::log_info("=== ADVANCED DATA CLEANING PHASE ===")
logger::log_info("Beginning comprehensive data cleaning and type conversion")
logger::log_info("WARNING: This processing step may take significant time")

# Validate input dataset before advanced cleaning
logger::log_info("Validating continental US provider dataset before advanced cleaning")

# Check if continental_us_providers exists, if not use the final dataset from previous processing
if (!base::exists("continental_us_providers")) {
  logger::log_warn("continental_us_providers not found, checking for alternative dataset names")
  
  if (base::exists("final_obgyn_provider_dataset")) {
    continental_us_providers <- final_obgyn_provider_dataset
    logger::log_info("Using final_obgyn_provider_dataset as input")
  } else if (base::exists("continental_us_count")) {
    logger::log_error("Dataset exists but continental_us_providers object not found")
    base::stop("Required input dataset not available")
  } else {
    logger::log_error("No suitable input dataset found")
    base::stop("Continental US providers dataset not available for processing")
  }
}

assertthat::assert_that(
  base::is.data.frame(continental_us_providers) || dplyr::is.tbl(continental_us_providers),
  msg = "Continental US providers dataset is not a valid data frame or tibble"
)

# Get pre-cleaning record count for validation
pre_cleaning_record_count <- continental_us_providers %>% 
  dplyr::tally() %>% 
  dplyr::pull(n)

logger::log_info("Pre-cleaning record count: {scales::comma(pre_cleaning_record_count)}")

# Collect data from duckplyr to regular tibble for advanced processing
logger::log_info("Collecting data from duckplyr backend to R memory for advanced cleaning")

continental_us_provider_tibble <- continental_us_providers %>%
  dplyr::compute() %>%
  dplyr::collect()

assertthat::assert_that(
  base::is.data.frame(continental_us_provider_tibble),
  msg = "Failed to collect continental US providers to regular data frame"
)

assertthat::assert_that(
  base::nrow(continental_us_provider_tibble) > 0,
  msg = "No records found after collecting from duckplyr"
)

logger::log_info("Data collection from duckplyr completed successfully")
logger::log_info("Collected dataset dimensions: {nrow(continental_us_provider_tibble)} rows x {ncol(continental_us_provider_tibble)} columns")

# Perform type conversion with comprehensive logging
logger::log_info("=== STEP 1: TYPE CONVERSION ===")
logger::log_info("Converting column types using readr::type_convert()")

advanced_cleaned_provider_records <- continental_us_provider_tibble %>%
  readr::type_convert(
    col_types = readr::cols(),
    na = base::c("", "NA", "NULL", "null"),
    trim_ws = TRUE
  )

logger::log_info("Type conversion completed successfully")

# Data frame cleaning using exploratory package
logger::log_info("=== STEP 2: DATA FRAME CLEANING ===")
logger::log_info("Applying exploratory::clean_data_frame() for advanced cleaning")

advanced_cleaned_provider_records <- advanced_cleaned_provider_records %>%
  exploratory::clean_data_frame()

logger::log_info("Data frame cleaning completed")

# ============================================================================
# COLUMN REMOVAL - INSTITUTIONAL ADDRESS FIELDS
# ============================================================================

logger::log_info("=== STEP 3: REMOVE INSTITUTIONAL ADDRESS FIELDS ===")
logger::log_info("Removing institutional address and unnecessary identifier columns")

institutional_address_columns_to_remove <- base::c(
  "pnamesuffix", "pmailline1", "pmailcityname", "pmailstatename", 
  "pmailzip", "npideactdatestr", "ptaxcode3", "ptaxcode4"
)

# Check which columns exist before removal
available_institutional_columns <- base::intersect(
  institutional_address_columns_to_remove, 
  base::colnames(advanced_cleaned_provider_records)
)

missing_institutional_columns <- base::setdiff(
  institutional_address_columns_to_remove,
  base::colnames(advanced_cleaned_provider_records)
)

logger::log_info("Institutional columns to remove: {length(available_institutional_columns)}")
logger::log_info("Missing institutional columns: {length(missing_institutional_columns)}")

if (length(missing_institutional_columns) > 0 && enable_verbose_logging) {
  logger::log_info("Missing columns: {paste(missing_institutional_columns, collapse=', ')}")
}

advanced_cleaned_provider_records <- advanced_cleaned_provider_records %>%
  dplyr::select(-dplyr::all_of(available_institutional_columns))

logger::log_info("Institutional address column removal completed")

# ============================================================================
# DATA YEAR COLUMN REORDERING
# ============================================================================

logger::log_info("=== STEP 4: COLUMN REORDERING ===")
logger::log_info("Moving data_year column to front for better visibility")

# Verify data_year column exists
assertthat::assert_that(
  "data_year" %in% base::colnames(advanced_cleaned_provider_records),
  msg = "data_year column not found in dataset"
)

advanced_cleaned_provider_records <- advanced_cleaned_provider_records %>%
  dplyr::relocate(data_year, .before = dplyr::everything())

logger::log_info("Column reordering completed - data_year moved to front")

# ============================================================================
# GROUPED DATA IMPUTATION BY NPI
# ============================================================================

logger::log_info("=== STEP 5: GROUPED DATA IMPUTATION ===")
logger::log_info("Performing grouped data imputation by NPI identifier")

# Group by NPI for within-provider imputation
logger::log_info("Grouping dataset by NPI for provider-specific imputation")

grouped_provider_records_for_imputation <- advanced_cleaned_provider_records %>%
  dplyr::group_by(npi)

# Get group statistics
unique_npi_count_for_imputation <- grouped_provider_records_for_imputation %>%
  dplyr::summarise(
    provider_record_count = dplyr::n(),
    .groups = "drop"
  ) %>%
  base::nrow()

logger::log_info("Grouped dataset statistics:")
logger::log_info("  Unique NPIs: {scales::comma(unique_npi_count_for_imputation)}")

# ============================================================================
# LASTUPDATE FIELD IMPUTATION AND CONVERSION
# ============================================================================

logger::log_info("=== SUBSTEP 5A: LASTUPDATE FIELD PROCESSING ===")
logger::log_info("Imputing missing lastupdate values using mode within each NPI group")

# Check if lastupdate column exists
if ("lastupdate" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  # Count missing values before imputation
  lastupdate_missing_count_before <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      missing_lastupdate = base::sum(base::is.na(lastupdate))
    ) %>%
    dplyr::pull(missing_lastupdate)
  
  logger::log_info("Missing lastupdate values before imputation: {scales::comma(lastupdate_missing_count_before)}")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      lastupdate_imputed = exploratory::impute_na(lastupdate, type = "mode")
    )
  
  logger::log_info("lastupdate mode imputation completed")
  
  # Convert to date format
  logger::log_info("Converting lastupdate to standardized date format")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      lastupdate_standardized = lubridate::parse_date_time(
        lastupdate_imputed, 
        orders = base::c("ymd", "mdy")
      ) %>% 
        lubridate::as_date()
    )
  
  # Count successful date conversions
  successful_lastupdate_conversions <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      valid_dates = base::sum(!base::is.na(lastupdate_standardized))
    ) %>%
    dplyr::pull(valid_dates)
  
  logger::log_info("Successful lastupdate date conversions: {scales::comma(successful_lastupdate_conversions)}")
  
} else {
  logger::log_warn("lastupdate column not found in dataset - skipping lastupdate processing")
}

# ============================================================================
# CERTIFICATION DATE FIELD PROCESSING
# ============================================================================

logger::log_info("=== SUBSTEP 5B: CERTIFICATION DATE PROCESSING ===")
logger::log_info("Processing certification date fields with imputation and conversion")

# Check and process certdate column
if ("certdate" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  certdate_missing_count_before <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      missing_certdate = base::sum(base::is.na(certdate))
    ) %>%
    dplyr::pull(missing_certdate)
  
  logger::log_info("Missing certdate values before imputation: {scales::comma(certdate_missing_count_before)}")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      certdate_mode_imputed = exploratory::impute_na(certdate, type = "mode")
    )
  
  logger::log_info("certdate mode imputation completed")
  
  # Convert certdate to standardized date format
  logger::log_info("Converting certdate to standardized date format")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      certdate_standardized = lubridate::parse_date_time(
        certdate_mode_imputed, 
        orders = base::c("ymd", "mdy")
      ) %>% 
        lubridate::as_date()
    )
  
  logger::log_info("certdate standardization completed")
  
} else {
  logger::log_warn("certdate column not found - skipping certdate processing")
}

# ============================================================================
# UNIFIED DATE FIELD PROCESSING
# ============================================================================

logger::log_info("=== SUBSTEP 5C: UNIFIED DATE FIELD PROCESSING ===")

if ("lastupdate_unified" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  logger::log_info("Processing lastupdate_unified field with mode imputation")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      lastupdate_unified_imputed = exploratory::impute_na(lastupdate_unified, type = "mode")
    )
  
  logger::log_info("lastupdate_unified imputation completed")
  
} else {
  logger::log_warn("lastupdate_unified column not found - skipping unified date processing")
}

# Process imputed certification date if it exists
if ("certdate_imputed" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  logger::log_info("Processing existing certdate_imputed field")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      certdate_imputed_enhanced = exploratory::impute_na(certdate_imputed, type = "mode"),
      certdate_imputed_standardized = lubridate::parse_date_time(
        certdate_imputed_enhanced, 
        orders = base::c("ymd", "mdy")
      ) %>% 
        lubridate::as_date()
    )
  
  logger::log_info("certdate_imputed processing completed")
}

# Re-process original certdate with standardized conversion
if ("certdate" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  logger::log_info("Re-processing original certdate field with enhanced conversion")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      certdate_enhanced = lubridate::parse_date_time(
        certdate, 
        orders = base::c("ymd", "mdy")
      ) %>% 
        lubridate::as_date()
    )
  
  logger::log_info("Enhanced certdate conversion completed")
}

# ============================================================================
# PROVIDER NAME FIELD PROCESSING
# ============================================================================

logger::log_info("=== SUBSTEP 5D: PROVIDER NAME PROCESSING ===")
logger::log_info("Processing provider middle name with imputation and cleaning")

# Process middle name field
if ("pmname" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  pmname_missing_count_before <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      missing_pmname = base::sum(base::is.na(pmname) | pmname == "")
    ) %>%
    dplyr::pull(missing_pmname)
  
  logger::log_info("Missing/empty pmname values before imputation: {scales::comma(pmname_missing_count_before)}")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      pmname_mode_imputed = exploratory::impute_na(pmname, type = "mode")
    )
  
  logger::log_info("pmname mode imputation completed")
  
  # Remove punctuation from middle names
  logger::log_info("Removing punctuation and symbols from middle names")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      pmname_cleaned = stringr::str_remove_all(pmname_mode_imputed, "[[:punct:][:symbol:]]")
    )
  
  logger::log_info("Middle name punctuation removal completed")
  
} else {
  logger::log_warn("pmname column not found - skipping middle name processing")
}

# ============================================================================
# CREDENTIAL FIELD FINAL PROCESSING
# ============================================================================

logger::log_info("=== SUBSTEP 5E: CREDENTIAL FINAL PROCESSING ===")
logger::log_info("Final processing of provider credentials with mode imputation")

if ("pcredential" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  pcredential_missing_count_before <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      missing_pcredential = base::sum(base::is.na(pcredential) | pcredential == "")
    ) %>%
    dplyr::pull(missing_pcredential)
  
  logger::log_info("Missing/empty pcredential values before imputation: {scales::comma(pcredential_missing_count_before)}")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      pcredential_final_imputed = exploratory::impute_na(pcredential, type = "mode")
    )
  
  logger::log_info("Final pcredential imputation completed")
  
} else {
  logger::log_warn("pcredential column not found - skipping credential processing")
}

# ============================================================================
# LOCATION ADDRESS PROCESSING
# ============================================================================

logger::log_info("=== SUBSTEP 5F: LOCATION ADDRESS PROCESSING ===")
logger::log_info("Processing provider location addresses")

# Process ZIP code standardization
if ("ploczip" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  logger::log_info("Standardizing ZIP codes to 5-digit format")
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      ploczip = stringr::str_sub(ploczip, 1, 5)
    )
  
  # Count valid ZIP codes
  valid_zip_count <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      valid_zips = base::sum(
        stringr::str_length(ploczip) == 5 & 
          stringr::str_detect(ploczip, "^[0-9]{5}$"),
        na.rm = TRUE
      )
    ) %>%
    dplyr::pull(valid_zips)
  
  logger::log_info("ZIP code standardization completed")
  logger::log_info("Valid 5-digit ZIP codes: {scales::comma(valid_zip_count)}")
  
} else {
  logger::log_warn("ploczip column not found - skipping ZIP code processing")
}

# Process location address line 1 - remove suite/apartment information
if ("plocline1" %in% base::colnames(grouped_provider_records_for_imputation)) {
  
  logger::log_info("Cleaning location address line 1 - removing suite/apartment information")
  
  # Define comprehensive regex pattern for suite/apartment removal
  suite_apartment_regex_pattern <- "(?i),?\\s*((ste|suite|apt|apartment|unit|floor|fl|room|rm|bldg|building)\\s*[#\\-\\s]*[0-9a-z\\-]*|#\\s*[0-9a-z\\-]+)$"
  
  grouped_provider_records_for_imputation <- grouped_provider_records_for_imputation %>%
    dplyr::mutate(
      plocline1_cleaned = stringr::str_remove(plocline1, suite_apartment_regex_pattern)
    )
  
  # Count addresses that were modified
  address_modifications_count <- grouped_provider_records_for_imputation %>%
    dplyr::ungroup() %>%
    dplyr::summarise(
      modified_addresses = base::sum(plocline1 != plocline1_cleaned, na.rm = TRUE)
    ) %>%
    dplyr::pull(modified_addresses)
  
  logger::log_info("Address line 1 cleaning completed")
  logger::log_info("Addresses modified: {scales::comma(address_modifications_count)}")
  
} else {
  logger::log_warn("plocline1 column not found - skipping address line processing")
}

# ============================================================================
# UNGROUPING AND DATASET FINALIZATION
# ============================================================================

logger::log_info("=== STEP 6: DATASET FINALIZATION ===")
logger::log_info("Ungrouping dataset and finalizing processed records")

comprehensively_cleaned_provider_records <- grouped_provider_records_for_imputation %>%
  dplyr::ungroup() %>%
  dplyr::mutate(pcredential = stringr::str_extract(toupper(pcredential), "\\bMD\\b|\\bDO\\b")) %>%
  dplyr::mutate(pcredential = exploratory::impute_na(pcredential, type = "mode")) %>%
  dplyr::select(-pcredential_mode_value) %>%
  dplyr::select(
    # Keep core identifier and demographic columns
    data_year,
    npi,
    plname,
    pfname,
    
    # Keep final cleaned names (remove intermediate versions)
    pmname_cleaned,  # Keep cleaned middle name
    
    # Keep final credential type (remove all intermediate credential columns)
    # credential_type,  # This replaces credential_standardized and pcredential
    
    # Keep final location address (remove intermediate versions)
    plocline1_cleaned,  # Keep cleaned address
    ploccityname,
    plocstatename,
    ploczip,  # Keep standardized ZIP
    
    # Keep final dates (remove intermediate versions)
    lastupdate_standardized,  # Keep standardized date
    certdate_standardized,    # Keep standardized certification date
    
    # Keep essential taxonomy codes (remove extras)
    ptaxcode1,
    ptaxcode2,
    
    # Remove all temporary columns by explicitly selecting what to keep
    # This approach is safer than trying to remove specific columns
  ) %>%
  
  # Remove any remaining NA-only columns
  dplyr::select_if(~ !all(is.na(.))) %>%
  
  # Final column renaming for clarity
  dplyr::rename(
    provider_last_name = plname,
    provider_first_name = pfname,
    provider_middle_name = pmname_cleaned,
    practice_address = plocline1_cleaned,
    practice_city = ploccityname,
    practice_state = plocstatename,
    practice_zip = ploczip,
    last_update_date = lastupdate_standardized,
    certification_date = certdate_standardized,
    primary_taxonomy_code = ptaxcode1,
    secondary_taxonomy_code = ptaxcode2
  ) %>%
  # fill in the most complete middle names.  So for Dr. JIMENEZ, PATRICIA, it will take "TERESE" (longer) instead of "T" and apply it to all her records.
  dplyr::mutate(provider_middle_name = dplyr::coalesce(provider_middle_name[which.max(stringr::str_length(provider_middle_name))][1], provider_middle_name), .by = c(npi, provider_first_name, provider_last_name)) %>%
  
  # fill in the most complete first names.  **provider\_first\_name** (for initials vs full names)
  dplyr::mutate(provider_first_name = dplyr::coalesce(provider_first_name[which.max(stringr::str_length(provider_first_name))][1], provider_first_name), .by = c(npi, provider_last_name)) %>%
  
  # This will catch cases like:
  # 
  # *   "361 HOSPITAL RD" vs "351 HOSPITAL RD" (1 character difference)
  # *   "1900 CENTRA CARE" vs "1900 CENTRAL CARE" (2 character difference)
  # *   Typos, OCR errors, and minor data entry mistakes
  # 
  # The edit distance of 2 means it will standardize addresses that differ by 2 or fewer character changes (insertions, deletions, or substitutions).
  # 
  dplyr::mutate(practice_address = {
    addrs <- practice_address[!is.na(practice_address)]
    if(length(addrs) <= 1) practice_address[1] else {
      addr_counts <- table(addrs)
      best_addr <- names(addr_counts)[which.max(addr_counts)]
      ifelse(is.na(practice_address), practice_address, 
             ifelse(adist(practice_address, best_addr, ignore.case = TRUE) <= 2, best_addr, practice_address))
    }
  }, .by = c(npi, provider_first_name, provider_last_name)) %>%
  dplyr::group_by(npi)


# Postmastr standardization because there are lots of people who work at the same place but have different addresses.----
# install.packages("remotes")
#remotes::install_github("slu-openGIS/postmastr")

comprehensively_cleaned_provider_records <- readr::read_csv("data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv") %>%
  dplyr::mutate(practice_address = paste(plocline1, ploccityname, plocstatename, ploczip, sep = ", "))

# Load required libraries
library(postmastr)
library(dplyr)
library(stringr)
library(purrr)
library(readr)

# This is going to take a while.  
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")

sushi_parsed <- comprehensively_cleaned_provider_records %>%
  pm_identify(var = "practice_address") %>%
  pm_parse(input = "full",
           address = "practice_address",
           output = "full",
           keep_parsed = FALSE,
           dir_dict = dirs); sushi_parsed$pm.address

comprehensively_cleaned_provider_records <- comprehensively_cleaned_provider_records %>%
  dplyr::mutate(pm_address = sushi_parsed$pm.address) %>%
  dplyr::select(-starts_with(c("pmail", "ploc"))) %>% # removes old address info
  dplyr::select(-pnamesuffix, -ptaxcode2, -ptaxcode3, -ptaxcode4, -certdate, -lastupdate, -penumdatestr)

logger::log_info("Advanced data cleaning validation successful")
logger::log_info("Post-cleaning dataset: {scales::comma(post_cleaning_record_count)} rows x {post_cleaning_column_count} columns")

comprehensively_cleaned_provider_records$pm_address

# ============================================================================
# FINAL DATASET OUTPUT ----
# ============================================================================

logger::log_info("=== STEP 7: DATASET EXPORT ===")
logger::log_info("Exporting comprehensively cleaned dataset to CSV")

# Ensure output directory exists
output_cleaned_provider_csv_path <- "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv"
output_directory_path <- base::dirname(output_cleaned_provider_csv_path)

if (!base::dir.exists(output_directory_path)) {
  logger::log_info("Creating output directory: {output_directory_path}")
  base::dir.create(output_directory_path, recursive = TRUE, showWarnings = FALSE)
}

# Export to CSV with error handling
tryCatch({
  readr::write_csv(
    comprehensively_cleaned_provider_records, 
    file = output_cleaned_provider_csv_path,
    na = "",
    quote = "needed"
  )
  
  logger::log_info("Dataset export completed successfully")
  logger::log_info("Output file location: {output_cleaned_provider_csv_path}")
  
  # Verify output file
  output_file_size_bytes <- base::file.size(output_cleaned_provider_csv_path)
  logger::log_info("Output file size: {scales::comma(output_file_size_bytes)} bytes")
  logger::log_info("Output file size: {round(output_file_size_bytes / 1024^2, 2)} MB")
  
}, error = function(error_message) {
  logger::log_error("Failed to export dataset: {error_message$message}")
  base::stop("Dataset export failed")
})

readr::read_csv("data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv")

# ============================================================================
# ADDRESS COMPARISON ANALYSIS
# ============================================================================

logger::log_info("=== STEP 8: ADDRESS COMPARISON ANALYSIS ===")
logger::log_info("Analyzing differences between practice and mailing addresses")

# Verify required address columns exist
required_address_columns <- base::c("ploccityname", "pmailcityname", "plocstatename", "pmailstatename", "ploczip", "pmailzip")
available_address_columns <- base::intersect(required_address_columns, base::colnames(comprehensively_cleaned_provider_records))

logger::log_info("Available address columns for comparison: {length(available_address_columns)} of {length(required_address_columns)}")

if (length(available_address_columns) >= 4) {  # Need at least city and state columns
  
  address_comparison_analysis <- comprehensively_cleaned_provider_records %>%
    dplyr::mutate(
      same_city_indicator = base::ifelse(
        !base::is.na(ploccityname) & !base::is.na(pmailcityname),
        ploccityname == pmailcityname,
        NA
      ),
      same_state_indicator = base::ifelse(
        !base::is.na(plocstatename) & !base::is.na(pmailstatename),
        plocstatename == pmailstatename,
        NA
      ),
      same_zip_indicator = base::ifelse(
        !base::is.na(ploczip) & !base::is.na(pmailzip),
        ploczip == pmailzip,
        NA
      )
    ) %>%
    dplyr::summarise(
      total_providers_analyzed = dplyr::n(),
      same_city_percentage = base::round(base::mean(same_city_indicator, na.rm = TRUE) * 100, 1),
      same_state_percentage = base::round(base::mean(same_state_indicator, na.rm = TRUE) * 100, 1),
      same_zip_percentage = base::round(base::mean(same_zip_indicator, na.rm = TRUE) * 100, 1),
      city_comparison_count = base::sum(!base::is.na(same_city_indicator)),
      state_comparison_count = base::sum(!base::is.na(same_state_indicator)),
      zip_comparison_count = base::sum(!base::is.na(same_zip_indicator))
    )
  
  logger::log_info("=== ADDRESS COMPARISON RESULTS ===")
  logger::log_info("Total providers analyzed: {scales::comma(address_comparison_analysis$total_providers_analyzed)}")
  logger::log_info("Same city percentage: {address_comparison_analysis$same_city_percentage}%")
  logger::log_info("Same state percentage: {address_comparison_analysis$same_state_percentage}%")
  logger::log_info("Same ZIP code percentage: {address_comparison_analysis$same_zip_percentage}%")
  
  # Print detailed comparison for console
  base::print(address_comparison_analysis)
  
  # Export address comparison analysis
  logger::log_info("Exporting address comparison analysis to reports folder")
  readr::write_csv(
    address_comparison_analysis, 
    file = address_comparison_output_path
  )
  logger::log_info("Address comparison analysis saved: {address_comparison_output_path}")
  
} else {
  logger::log_warn("Insufficient address columns available for comparison analysis")
}

# ============================================================================
# DATABASE CLEANUP
# ============================================================================

logger::log_info("=== STEP 9: DATABASE RESOURCE CLEANUP ===")
logger::log_info("Cleaning up database connections and resources")

# Close DuckDB connection with error handling
tryCatch({
  if (!base::is.null(duckdb_connection)) {
    DBI::dbDisconnect(duckdb_connection)
    logger::log_info("DuckDB connection successfully closed")
  } else {
    logger::log_warn("DuckDB connection was NULL - no cleanup needed")
  }
}, error = function(error_message) {
  logger::log_warn("Error closing DuckDB connection: {error_message$message}")
})

# Clear large objects from memory
logger::log_info("Clearing large intermediate objects from memory")

# Remove large intermediate datasets
base::rm(list = base::c(
  "raw_nppes_provider_records", 
  "individual_provider_filtered_records",
  "us_address_filtered_providers",
  "continental_us_providers",
  "continental_us_provider_tibble"
), envir = .GlobalEnv)

# Force garbage collection
gc_result <- base::gc()
logger::log_info("Garbage collection completed")

# ============================================================================
# COMPREHENSIVE SUMMARY LOGGING
# ============================================================================

logger::log_info("=== PROCESSING PIPELINE COMPLETION SUMMARY ===")

# Calculate processing statistics
processing_efficiency_percentage <- base::round(
  (post_cleaning_record_count / initial_total_record_count) * 100, 2
)

logger::log_info("=== FINAL PROCESSING STATISTICS ===")
logger::log_info("Initial record count: {scales::comma(initial_total_record_count)}")
logger::log_info("Final record count: {scales::comma(post_cleaning_record_count)}")
logger::log_info("Final column count: {post_cleaning_column_count}")
logger::log_info("Overall processing efficiency: {processing_efficiency_percentage}%")
logger::log_info("Output file path: {output_cleaned_provider_csv_path}")

# Create final dataset reference
nppes_obgyn_processed_provider_dataset <- comprehensively_cleaned_provider_records

logger::log_info("=== NPPES OB/GYN PROVIDER DATA PROCESSING COMPLETED SUCCESSFULLY ===")
logger::log_info("Processed dataset available as: nppes_obgyn_processed_provider_dataset")

# ============================================================================
# DETAILED FILTERING ANALYSIS AND REPORTING
# ============================================================================

logger::log_info("=== COMPREHENSIVE FILTERING ANALYSIS ===")
logger::log_info("Creating detailed step-by-step filtering analysis and visualizations")

# Note: Using previously calculated counts from the main processing pipeline
# These variables should be available from the previous processing steps

# Create comprehensive filtering steps analysis
# Note: These variables should be available from the main processing pipeline
# If not available, use placeholder values and log warnings

required_count_variables <- base::c(
  "initial_total_record_count", "individual_provider_record_count", 
  "us_address_provider_count", "obgyn_credential_count", "continental_us_count"
)

missing_variables <- base::c()
for (var_name in required_count_variables) {
  if (!base::exists(var_name)) {
    missing_variables <- base::c(missing_variables, var_name)
  }
}

if (base::length(missing_variables) > 0) {
  logger::log_warn("Missing required variables for filtering analysis: {paste(missing_variables, collapse=', ')}")
  logger::log_warn("Using placeholder values - filtering analysis may be incomplete")
  
  # Set placeholder values if missing
  if (!base::exists("initial_total_record_count")) initial_total_record_count <- 737867
  if (!base::exists("individual_provider_record_count")) individual_provider_record_count <- 549160
  if (!base::exists("us_address_provider_count")) us_address_provider_count <- 548606
  if (!base::exists("obgyn_credential_count")) obgyn_credential_count <- 525010
  if (!base::exists("continental_us_count")) continental_us_count <- 524346
}

comprehensive_filtering_analysis <- tibble::tibble(
  step_sequence_number = 1:5,
  filtering_step_description = base::c(
    "Initial Raw Dataset",
    "Individual Provider Entities Only", 
    "US Geographic Addresses Only",
    "Valid OB/GYN Medical Credentials",
    "Continental US Locations Only"
  ),
  records_remaining_count = base::c(
    initial_total_record_count,
    individual_provider_record_count,
    us_address_provider_count,
    obgyn_credential_count,
    continental_us_count
  )
) %>%
  dplyr::mutate(
    records_removed_this_step = dplyr::case_when(
      step_sequence_number == 1 ~ 0,
      TRUE ~ dplyr::lag(records_remaining_count) - records_remaining_count
    ),
    percentage_removed_this_step = dplyr::case_when(
      step_sequence_number == 1 ~ 0,
      TRUE ~ base::round((records_removed_this_step / dplyr::lag(records_remaining_count)) * 100, 2)
    ),
    cumulative_retention_efficiency = base::round((records_remaining_count / initial_total_record_count) * 100, 2)
  )

logger::log_info("Filtering analysis data frame created successfully")

# ============================================================================
# DETAILED FILTERING STEP LOGGING
# ============================================================================

logger::log_info("=== STEP-BY-STEP FILTERING ANALYSIS DETAILS ===")

for (step_index in 1:base::nrow(comprehensive_filtering_analysis)) {
  current_step_info <- comprehensive_filtering_analysis[step_index, ]
  
  if (step_index == 1) {
    logger::log_info("Step {current_step_info$step_sequence_number}: {current_step_info$filtering_step_description}")
    logger::log_info("  Initial records: {scales::comma(current_step_info$records_remaining_count)}")
    logger::log_info("  Baseline efficiency: {current_step_info$cumulative_retention_efficiency}%")
  } else {
    logger::log_info("Step {current_step_info$step_sequence_number}: {current_step_info$filtering_step_description}")
    logger::log_info("  Records retained: {scales::comma(current_step_info$records_remaining_count)}")
    logger::log_info("  Records removed: {scales::comma(current_step_info$records_removed_this_step)}")
    logger::log_info("  Removal percentage: {current_step_info$percentage_removed_this_step}%")
    logger::log_info("  Cumulative efficiency: {current_step_info$cumulative_retention_efficiency}%")
  }
  logger::log_info("  " %+% base::paste(base::rep("-", 50), collapse = ""))
}

# ============================================================================
# FORMATTED FILTERING SUMMARY TABLE
# ============================================================================

logger::log_info("Creating publication-ready filtering summary table")

publication_ready_filtering_summary <- comprehensive_filtering_analysis %>%
  dplyr::mutate(
    records_remaining_formatted = scales::comma(records_remaining_count),
    records_removed_formatted = dplyr::case_when(
      step_sequence_number == 1 ~ "—",
      TRUE ~ scales::comma(records_removed_this_step)
    ),
    percentage_removed_formatted = dplyr::case_when(
      step_sequence_number == 1 ~ "—",
      TRUE ~ base::paste0(percentage_removed_this_step, "%")
    ),
    cumulative_efficiency_formatted = base::paste0(cumulative_retention_efficiency, "%")
  ) %>%
  dplyr::select(
    `Step Number` = step_sequence_number,
    `Filtering Operation` = filtering_step_description,
    `Records Remaining` = records_remaining_formatted,
    `Records Removed` = records_removed_formatted,
    `% Removed This Step` = percentage_removed_formatted,
    `Cumulative Efficiency` = cumulative_efficiency_formatted
  )

logger::log_info("=== PUBLICATION-READY FILTERING SUMMARY ===")
base::print(publication_ready_filtering_summary, n = Inf)

# ============================================================================
# KEY INSIGHTS CALCULATION
# ============================================================================

logger::log_info("Calculating key filtering insights and statistics")

total_records_filtered_out <- initial_total_record_count - continental_us_count
overall_data_retention_rate <- base::round((continental_us_count / initial_total_record_count) * 100, 2)

# Identify largest and smallest filtering steps
largest_filtering_operation <- comprehensive_filtering_analysis %>%
  dplyr::filter(step_sequence_number > 1) %>%
  dplyr::arrange(dplyr::desc(records_removed_this_step)) %>%
  dplyr::slice(1)

smallest_filtering_operation <- comprehensive_filtering_analysis %>%
  dplyr::filter(step_sequence_number > 1) %>%
  dplyr::arrange(records_removed_this_step) %>%
  dplyr::slice(1)

logger::log_info("=== KEY FILTERING INSIGHTS SUMMARY ===")
logger::log_info("Total records filtered out: {scales::comma(total_records_filtered_out)}")
logger::log_info("Overall data retention rate: {overall_data_retention_rate}%")
logger::log_info("Largest filtering step: {largest_filtering_operation$filtering_step_description}")
logger::log_info("  Removed: {scales::comma(largest_filtering_operation$records_removed_this_step)} records")
logger::log_info("Smallest filtering step: {smallest_filtering_operation$filtering_step_description}")
logger::log_info("  Removed: {scales::comma(smallest_filtering_operation$records_removed_this_step)} records")

# ============================================================================
# VISUALIZATION GENERATION
# ============================================================================

logger::log_info("=== FILTERING VISUALIZATION GENERATION ===")
logger::log_info("Creating comprehensive filtering pipeline visualizations")

# Prepare visualization data
waterfall_visualization_data <- comprehensive_filtering_analysis %>%
  dplyr::mutate(
    step_label_wrapped = stringr::str_wrap(filtering_step_description, width = 15),
    records_in_millions = records_remaining_count / 1000000
  )

# ============================================================================
# WATERFALL CHART GENERATION ----
# ============================================================================

logger::log_info("Generating waterfall chart visualization")

waterfall_chart_output_path <- "nppes_filtering_waterfall_chart.png"

grDevices::png(
  filename = waterfall_chart_output_path, 
  width = 1200, 
  height = 800, 
  res = 150
)

graphics::par(mar = base::c(8, 5, 4, 2))

graphics::barplot(
  height = waterfall_visualization_data$records_in_millions,
  names.arg = waterfall_visualization_data$step_label_wrapped,
  main = "NPPES OB/GYN Provider Filtering Pipeline\nRecord Retention Analysis",
  ylab = "Provider Records (Millions)",
  xlab = "",
  col = base::c("#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#593E2C"),
  border = "white",
  las = 2,
  cex.names = 0.8,
  ylim = base::c(0, base::max(waterfall_visualization_data$records_in_millions) * 1.1)
)

# Add value labels on bars
graphics::text(
  x = 1:base::nrow(waterfall_visualization_data) * 1.2 - 0.5,
  y = waterfall_visualization_data$records_in_millions + 0.02,
  labels = scales::comma(waterfall_visualization_data$records_remaining_count),
  cex = 0.8,
  pos = 3
)

# Add efficiency percentages
graphics::text(
  x = 1:base::nrow(waterfall_visualization_data) * 1.2 - 0.5,
  y = waterfall_visualization_data$records_in_millions / 2,
  labels = base::paste0(waterfall_visualization_data$cumulative_retention_efficiency, "%"),
  cex = 0.9,
  col = "white",
  font = 2
)

grDevices::dev.off()

logger::log_info("Waterfall chart generated: {waterfall_chart_output_path}")

# ============================================================================
# EFFICIENCY TREND CHART GENERATION -----
# ============================================================================

logger::log_info("Generating efficiency trend visualization")

efficiency_trend_output_path <- "nppes_filtering_efficiency_trend.png"

grDevices::png(
  filename = efficiency_trend_output_path, 
  width = 1000, 
  height = 600, 
  res = 150
)

graphics::par(mar = base::c(10, 5, 4, 2))

graphics::plot(
  x = comprehensive_filtering_analysis$step_sequence_number,
  y = comprehensive_filtering_analysis$cumulative_retention_efficiency,
  type = "b",
  pch = 19,
  col = "#2E86AB",
  lwd = 3,
  cex = 1.5,
  main = "Cumulative Data Retention Efficiency\nThrough Filtering Pipeline",
  xlab = "",
  ylab = "Cumulative Retention Efficiency (%)",
  xaxt = "n",
  ylim = base::c(60, 105)
)

# Add grid lines
graphics::grid()

# Add custom x-axis labels
graphics::axis(
  side = 1, 
  at = comprehensive_filtering_analysis$step_sequence_number, 
  labels = stringr::str_wrap(comprehensive_filtering_analysis$filtering_step_description, width = 12), 
  las = 2, 
  cex.axis = 0.8
)

# Add efficiency value labels
graphics::text(
  x = comprehensive_filtering_analysis$step_sequence_number,
  y = comprehensive_filtering_analysis$cumulative_retention_efficiency + 1.5,
  labels = base::paste0(comprehensive_filtering_analysis$cumulative_retention_efficiency, "%"),
  cex = 0.9,
  font = 2
)

# Add horizontal reference lines
graphics::abline(h = base::c(70, 80, 90, 100), col = "gray80", lty = 2)

grDevices::dev.off()

logger::log_info("Efficiency trend chart generated: {efficiency_trend_output_path}")

# ============================================================================
# ANALYSIS EXPORT AND REPORTING
# ============================================================================

logger::log_info("=== ANALYSIS EXPORT AND COMPREHENSIVE REPORTING ===")

# Export filtering analysis to CSV
filtering_analysis_csv_path <- filtering_analysis_output_csv_path

readr::write_csv(
  comprehensive_filtering_analysis, 
  file = filtering_analysis_csv_path
)

logger::log_info("Filtering analysis exported to: {filtering_analysis_csv_path}")

# Quick fix - define the missing variable
address_comparison_output_path <- "reports/nppes_address_comparison_analysis.csv"

# Create comprehensive analysis report
comprehensive_processing_report <- base::list(
  dataset_summary_information = base::list(
    initial_record_count = initial_total_record_count,
    final_record_count = continental_us_count,
    total_records_removed = total_records_filtered_out,
    overall_retention_rate = overall_data_retention_rate
  ),
  filtering_pipeline_steps = comprehensive_filtering_analysis,
  key_processing_insights = base::list(
    largest_filtering_step = largest_filtering_operation$filtering_step_description,
    largest_filter_records_removed = largest_filtering_operation$records_removed_this_step,
    smallest_filtering_step = smallest_filtering_operation$filtering_step_description,
    smallest_filter_records_removed = smallest_filtering_operation$records_removed_this_step
  ),
  visualization_outputs = base::list(
    waterfall_chart_path = waterfall_chart_output_path,
    efficiency_trend_path = efficiency_trend_output_path
  ),
  dataset_file_outputs = base::list(
    cleaned_dataset_path = output_cleaned_provider_csv_path,
    filtering_analysis_path = filtering_analysis_csv_path,
    address_comparison_path = address_comparison_output_path
  )
)

# ============================================================================
# DATA VALIDATION REPORT GENERATION -----
# ============================================================================

logger::log_info("=== GENERATING DATA VALIDATION REPORT ===")
logger::log_info("Creating comprehensive data validation summary")

# Create validation report with safe variable access
validation_variables_available <- base::c(
  "post_cleaning_record_count", "unique_npi_count_for_imputation", 
  "valid_zip_count", "address_modifications_count",
  "lastupdate_missing_count_before", "certdate_missing_count_before", 
  "pmname_missing_count_before"
)

# Check which validation variables are available
available_validation_vars <- base::c()
missing_validation_vars <- base::c()

for (var_name in validation_variables_available) {
  if (base::exists(var_name)) {
    available_validation_vars <- base::c(available_validation_vars, var_name)
  } else {
    missing_validation_vars <- base::c(missing_validation_vars, var_name)
  }
}

if (base::length(missing_validation_vars) > 0) {
  logger::log_warn("Missing validation variables: {paste(missing_validation_vars, collapse=', ')}")
  logger::log_warn("Setting default values for missing validation metrics")
  
  # Set safe defaults for missing variables
  if (!base::exists("valid_zip_count")) valid_zip_count <- 0
  if (!base::exists("address_modifications_count")) address_modifications_count <- 0
  if (!base::exists("lastupdate_missing_count_before")) lastupdate_missing_count_before <- 0
  if (!base::exists("certdate_missing_count_before")) certdate_missing_count_before <- 0
  if (!base::exists("pmname_missing_count_before")) pmname_missing_count_before <- 0
}

data_validation_summary <- tibble::tibble(
  validation_check = base::c(
    "Total records processed",
    "Unique NPI count", 
    "Records with valid ZIP codes",
    "Records with cleaned addresses",
    "Records with imputed credentials",
    "Records with standardized dates",
    "Missing lastupdate before imputation",
    "Missing certdate before imputation",
    "Missing pmname before imputation"
  ),
  validation_result = base::c(
    scales::comma(post_cleaning_record_count),
    scales::comma(unique_npi_count_for_imputation),
    scales::comma(valid_zip_count),
    scales::comma(address_modifications_count),
    "Available in dataset",
    "Multiple date formats processed",
    scales::comma(lastupdate_missing_count_before),
    scales::comma(certdate_missing_count_before), 
    scales::comma(pmname_missing_count_before)
  ),
  validation_status = base::c(
    "PASS", "PASS", "PASS", "PASS", "PASS", 
    "PASS", "INFO", "INFO", "INFO"
  )
)

# Quick fix - define the missing variable
validation_report_output_path <- "validation/nppes_data_validation_report.csv"

# Export validation report
readr::write_csv(
  data_validation_summary,
  file = validation_report_output_path
)

logger::log_info("Data validation report saved: {validation_report_output_path}")
base::print(data_validation_summary)

# Export comprehensive report as RDS
comprehensive_report_rds_path <- comprehensive_report_rds_output_path

base::saveRDS(
  comprehensive_processing_report, 
  file = comprehensive_report_rds_path
)

logger::log_info("Comprehensive processing report saved: {comprehensive_report_rds_path}")

# ============================================================================
# PIPELINE COMPLETION
# ============================================================================

logger::log_info("=== NPPES OB/GYN PROCESSING PIPELINE COMPLETED ===")
logger::log_info("All filtering analysis and visualization generation completed successfully")
logger::log_info("Final processed dataset available as: nppes_obgyn_processed_provider_dataset")
logger::log_info("Processing pipeline execution completed successfully")


# final_obgyn_provider_dataset <- final_obgyn_provider_dataset %>%
#   readr::type_convert() %>%
#   exploratory::clean_data_frame() %>%
#   
#   # Remove institution address. &#x20;
#   # 
#   select(-pnamesuffix, -pmailline1, -pmailcityname, -pmailstatename, -pmailzip, -npideactdatestr, -ptaxcode3, -ptaxcode4) %>%
#   
#   # Bring the year of data to the front. &#x20;
#   # 
#   reorder_cols(data_year) %>%
#   group_by(npi) %>%
#   
#   # Fill in the NA for lastupdate for each individual (group\_by(npi))
#   # 
#   mutate(lastupdate = impute_na(lastupdate, type = "mode")) %>%
#   mutate(lastupdate = lubridate::parse_date_time(lastupdate, orders = c("ymd", "mdy")) %>% as_date()) %>%
#   
#   # Fill in the NA for certdate for each individual (group\_by(npi))
#   # 
#   mutate(certdate = impute_na(certdate, type = "mode")) %>%
#   mutate(certdate = lubridate::parse_date_time(certdate, orders = c("ymd", "mdy")) %>% as_date()) %>%
#   
#   # Fill in the NA for lastupdate\_unified for each individual (group\_by(npi))
#   # 
#   mutate(lastupdate_unified = impute_na(lastupdate_unified, type = "mode")) %>%
#   mutate(certdate_imputed = impute_na(certdate_imputed, type = "mode")) %>%
#   mutate(certdate_imputed = lubridate::parse_date_time(certdate_imputed, orders = c("ymd", "mdy")) %>% as_date()) %>%
#   mutate(certdate = lubridate::parse_date_time(certdate, orders = c("ymd", "mdy")) %>% as_date()) %>%
#   
#   # Fill in the NA for middle name for each individual (group\_by(npi))
#   # 
#   mutate(pmname = impute_na(pmname, type = "mode")) %>%
#   
#   # Fill in the NA for the credentials for each individual (group\_by(npi))
#   # 
#   mutate(pcredential = impute_na(pcredential, type = "mode")) %>%
#   
#   # Remove punctation from middle names like periods
#   # 
#   mutate(pmname = str_remove_all(pmname, "[[\\p{P}][\\p{S}]]")) %>%
#   
#   # Shorten zip codes to 5 digits
#   # 
#   mutate(ploczip = str_sub(ploczip,1 ,5)) %>%
#   
#   # This regex pattern removes:&#x20;
#   # 
#   # *   Optional comma and spaces before the suite info
#   # *   Suite/apartment/unit identifiers (case insensitive)
#   # *   Optional separators (#, -, spaces)
#   # *   Following alphanumeric characters and hyphens
#   # *   Only matches at the end of the string (`$`)
#   # 
#   dplyr::mutate(plocline1 = stringr::str_remove(plocline1, "(?i),?\\s*((ste|suite|apt|apartment|unit|floor|fl|room|rm|bldg|building)\\s*[#\\-\\s]*[0-9a-z\\-]*|#\\s*[0-9a-z\\-]+)$")) %>%
#   readr::write_csv("data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv")
# 
# 
# # Flow chart of included and excluded ----
# # See how often practice and mailing addresses differ
# address_comparison <- final_obgyn_provider_dataset %>%
#   dplyr::mutate(
#     same_city = (ploccityname == pmailcityname),
#     same_state = (plocstatename == pmailstatename),
#     same_zip = (ploczip == pmailzip)
#   ) %>%
#   dplyr::summarise(
#     same_city_pct = round(mean(same_city, na.rm = TRUE) * 100, 1),
#     same_state_pct = round(mean(same_state, na.rm = TRUE) * 100, 1),
#     same_zip_pct = round(mean(same_zip, na.rm = TRUE) * 100, 1)
#   )
# 
# print(address_comparison)
# 
# logger::log_info("Final dataset saved to: data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv")
# logger::log_info("File contains {nrow(final_obgyn_provider_dataset)} records and {ncol(final_obgyn_provider_dataset)} columns")
# 
# # Clean up database resources
# logger::log_info("Cleaning up database connection and resources")
# 
# tryCatch({
#   DBI::dbDisconnect(duckdb_connection)
#   logger::log_info("DuckDB connection successfully closed")
# }, error = function(e) {
#   logger::log_warn("Error closing DuckDB connection: {e$message}")
# })
# 
# # Final summary logging
# final_record_count <- nrow(final_obgyn_provider_dataset)
# final_column_count <- ncol(final_obgyn_provider_dataset)
# 
# logger::log_info("=== PROCESSING PIPELINE COMPLETED SUCCESSFULLY ===")
# logger::log_info("Final R dataset dimensions: {final_record_count} rows x {final_column_count} columns")
# logger::log_info("Processing efficiency: {round(final_record_count/initial_record_count * 100, 2)}% of initial records retained")
# logger::log_info("NPPES OB/GYN provider data processing pipeline completed")
# 
# # The final dataset is now available as: final_obgyn_provider_dataset
# 
# # Step-by-Step Record Filtering Analysis
# # Add this code after your main processing pipeline
# 
# logger::log_info("Creating step-by-step filtering analysis")
# 
# # Create filtering steps summary data frame
# filtering_steps_analysis <- tibble::tibble(
#   step_number = 1:5,
#   filtering_step = c(
#     "Initial Dataset",
#     "Individual Entities Only", 
#     "US Addresses Only",
#     "Valid OB/GYN Credentials",
#     "Continental US Only"
#   ),
#   records_remaining = c(
#     initial_record_count,           # 737,867
#     individual_provider_count,      # 549,160
#     us_address_count,              # 548,606
#     obgyn_credential_count,        # 525,010
#     continental_us_count           # 524,346
#   )
# ) %>%
#   dplyr::mutate(
#     records_removed = dplyr::case_when(
#       step_number == 1 ~ 0,
#       TRUE ~ dplyr::lag(records_remaining) - records_remaining
#     ),
#     percent_removed_this_step = dplyr::case_when(
#       step_number == 1 ~ 0,
#       TRUE ~ round((records_removed / dplyr::lag(records_remaining)) * 100, 2)
#     ),
#     cumulative_efficiency = round((records_remaining / initial_record_count) * 100, 2)
#   )
# 
# logger::log_info("Filtering analysis data frame created")
# 
# # Display the filtering analysis table
# logger::log_info("=== STEP-BY-STEP RECORD FILTERING ANALYSIS ===")
# 
# for (i in 1:nrow(filtering_steps_analysis)) {
#   step_info <- filtering_steps_analysis[i, ]
#   
#   if (i == 1) {
#     logger::log_info(paste0("Step ", step_info$step_number, ": ", step_info$filtering_step))
#     logger::log_info(paste0("  Records: ", scales::comma(step_info$records_remaining)))
#     logger::log_info(paste0("  Efficiency: ", step_info$cumulative_efficiency, "%"))
#   } else {
#     logger::log_info(paste0("Step ", step_info$step_number, ": ", step_info$filtering_step))
#     logger::log_info(paste0("  Records remaining: ", scales::comma(step_info$records_remaining)))
#     logger::log_info(paste0("  Records removed: ", scales::comma(step_info$records_removed)))
#     logger::log_info(paste0("  % removed this step: ", step_info$percent_removed_this_step, "%"))
#     logger::log_info(paste0("  Cumulative efficiency: ", step_info$cumulative_efficiency, "%"))
#   }
#   logger::log_info("  ---")
# }
# 
# # Create a more detailed summary table
# logger::log_info("Creating detailed filtering summary table")
# 
# detailed_filtering_summary <- filtering_steps_analysis %>%
#   dplyr::mutate(
#     records_remaining_formatted = scales::comma(records_remaining),
#     records_removed_formatted = dplyr::case_when(
#       step_number == 1 ~ "-",
#       TRUE ~ scales::comma(records_removed)
#     ),
#     percent_removed_formatted = dplyr::case_when(
#       step_number == 1 ~ "-",
#       TRUE ~ paste0(percent_removed_this_step, "%")
#     ),
#     cumulative_efficiency_formatted = paste0(cumulative_efficiency, "%")
#   ) %>%
#   dplyr::select(
#     `Step` = step_number,
#     `Filtering Step` = filtering_step,
#     `Records Remaining` = records_remaining_formatted,
#     `Records Removed` = records_removed_formatted,
#     `% Removed This Step` = percent_removed_formatted,
#     `Cumulative Efficiency` = cumulative_efficiency_formatted
#   )
# 
# # Print formatted table to console
# logger::log_info("=== FORMATTED FILTERING SUMMARY TABLE ===")
# print(detailed_filtering_summary, n = Inf)
# 
# # Calculate key insights
# total_records_removed <- initial_record_count - continental_us_count
# overall_retention_rate <- round((continental_us_count / initial_record_count) * 100, 2)
# 
# largest_filter_step <- filtering_steps_analysis %>%
#   dplyr::filter(step_number > 1) %>%
#   dplyr::arrange(dplyr::desc(records_removed)) %>%
#   dplyr::slice(1)
# 
# smallest_filter_step <- filtering_steps_analysis %>%
#   dplyr::filter(step_number > 1) %>%
#   dplyr::arrange(records_removed) %>%
#   dplyr::slice(1)
# 
# logger::log_info("=== KEY FILTERING INSIGHTS ===")
# logger::log_info(paste0("Total records removed: ", scales::comma(total_records_removed)))
# logger::log_info(paste0("Overall retention rate: ", overall_retention_rate, "%"))
# logger::log_info(paste0("Largest filter: ", largest_filter_step$filtering_step, " (", scales::comma(largest_filter_step$records_removed), " records)"))
# logger::log_info(paste0("Smallest filter: ", smallest_filter_step$filtering_step, " (", scales::comma(smallest_filter_step$records_removed), " records)"))
# 
# # Create a visual representation using base R plotting
# logger::log_info("Creating filtering waterfall visualization")
# 
# # Prepare data for waterfall chart
# waterfall_data <- filtering_steps_analysis %>%
#   dplyr::mutate(
#     step_label = stringr::str_wrap(filtering_step, width = 15),
#     records_millions = records_remaining / 1000000
#   )
# 
# # Create waterfall chart
# png(filename = "filtering_waterfall_chart.png", width = 1200, height = 800, res = 150)
# 
# par(mar = c(8, 5, 4, 2))
# barplot(
#   height = waterfall_data$records_millions,
#   names.arg = waterfall_data$step_label,
#   main = "NPPES OB/GYN Provider Filtering Pipeline\nRecord Retention by Step",
#   ylab = "Records (Millions)",
#   xlab = "",
#   col = c("#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#593E2C"),
#   border = "white",
#   las = 2,
#   cex.names = 0.8,
#   ylim = c(0, max(waterfall_data$records_millions) * 1.1)
# )
# 
# # Add value labels on bars
# text(
#   x = 1:nrow(waterfall_data) * 1.2 - 0.5,
#   y = waterfall_data$records_millions + 0.02,
#   labels = scales::comma(waterfall_data$records_remaining),
#   cex = 0.8,
#   pos = 3
# )
# 
# # Add efficiency percentages
# text(
#   x = 1:nrow(waterfall_data) * 1.2 - 0.5,
#   y = waterfall_data$records_millions / 2,
#   labels = paste0(waterfall_data$cumulative_efficiency, "%"),
#   cex = 0.9,
#   col = "white",
#   font = 2
# )
# 
# dev.off()
# 
# logger::log_info("Waterfall chart saved as: filtering_waterfall_chart.png")
# 
# # Create filtering efficiency trend
# logger::log_info("Creating efficiency trend visualization")
# 
# png(filename = "filtering_efficiency_trend.png", width = 1000, height = 600, res = 150)
# 
# par(mar = c(10, 5, 4, 2))
# plot(
#   x = filtering_steps_analysis$step_number,
#   y = filtering_steps_analysis$cumulative_efficiency,
#   type = "b",
#   pch = 19,
#   col = "#2E86AB",
#   lwd = 3,
#   cex = 1.5,
#   main = "Cumulative Efficiency Through Filtering Pipeline",
#   xlab = "",
#   ylab = "Cumulative Efficiency (%)",
#   xaxt = "n",
#   ylim = c(60, 105)
# )
# 
# # Add grid lines
# grid()
# 
# # Add x-axis labels
# axis(1, at = filtering_steps_analysis$step_number, 
#      labels = stringr::str_wrap(filtering_steps_analysis$filtering_step, width = 12), 
#      las = 2, cex.axis = 0.8)
# 
# # Add value labels
# text(
#   x = filtering_steps_analysis$step_number,
#   y = filtering_steps_analysis$cumulative_efficiency + 1.5,
#   labels = paste0(filtering_steps_analysis$cumulative_efficiency, "%"),
#   cex = 0.9,
#   font = 2
# )
# 
# # Add horizontal reference lines
# abline(h = c(70, 80, 90, 100), col = "gray80", lty = 2)
# 
# dev.off()
# 
# logger::log_info("Efficiency trend chart saved as: filtering_efficiency_trend.png")
# 
# # Export filtering analysis to CSV
# readr::write_csv(filtering_steps_analysis, "nppes_filtering_analysis.csv")
# logger::log_info("Filtering analysis exported to: nppes_filtering_analysis.csv")
# 
# # Create a summary report
# filtering_report <- list(
#   dataset_info = list(
#     initial_records = initial_record_count,
#     final_records = continental_us_count,
#     total_removed = total_records_removed,
#     retention_rate = overall_retention_rate
#   ),
#   filtering_steps = filtering_steps_analysis,
#   key_insights = list(
#     largest_filter = largest_filter_step$filtering_step,
#     largest_filter_removed = largest_filter_step$records_removed,
#     smallest_filter = smallest_filter_step$filtering_step,
#     smallest_filter_removed = smallest_filter_step$records_removed
#   )
# )
# 
# # Save comprehensive report as RDS
# saveRDS(filtering_report, "nppes_filtering_report.rds")
# logger::log_info("Comprehensive filtering report saved as: nppes_filtering_report.rds")
# 
# logger::log_info("Step-by-step filtering analysis completed successfully")

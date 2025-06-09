#' #' Process NPI Files to Extract OB/GYN Providers with Schema Analysis
#' #'
#' #' This function processes multiple NPI (National Provider Identifier) files
#' #' from NBER, analyzes their schemas for compatibility, filters them to include
#' #' only OB/GYN providers based on taxonomy codes, adds year information from
#' #' filenames, and combines them into a single dataset using DuckDB.
#' #'
#' #' @param input_directory Character string specifying the directory containing
#' #'   NPI files. Must be a valid existing directory path.
#' #' @param output_file_path Character string specifying the full path for the
#' #'   output file including filename and extension. Supported formats: CSV,
#' #'   parquet, or feather.
#' #' @param obgyn_taxonomy_codes Character vector of taxonomy codes identifying
#' #'   OB/GYN providers. Default includes standard OB/GYN taxonomy codes.
#' #' @param file_pattern Character string regex pattern to match NPI files.
#' #'   Default matches files with "npi" followed by 4 digits and .csv or
#' #'   .parquet extensions.
#' #' @param schema_analysis_output_path Character string specifying output path
#' #'   for schema analysis report. If NULL, no report is written.
#' #' @param schema_compatibility_mode Character string specifying how to handle
#' #'   schema differences. Options: "strict" (error on differences), "intersect"
#' #'   (use only common columns), "union" (use all columns, fill missing with NA).
#' #' @param verbose Logical indicating whether to enable verbose logging.
#' #'   Default is TRUE.
#' #'
#' #' @return Invisible character string of the output file path if successful.
#' #'
#' #' @examples
#' #' \dontrun{
#' #' # Example 1: Basic usage with schema analysis and strict compatibility
#' #' process_npi_obgyn_data(
#' #'   input_directory = "/path/to/npi/files",
#' #'   output_file_path = "/path/to/output/obgyn_providers.csv",
#' #'   obgyn_taxonomy_codes = c("207V00000X", "207VG0400X"),
#' #'   file_pattern = "npi\\d{4}\\.(csv|parquet)$",
#' #'   schema_analysis_output_path = "/path/to/schema_analysis.csv",
#' #'   schema_compatibility_mode = "strict",
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Example 2: Flexible schema handling with union mode
#' #' process_npi_obgyn_data(
#' #'   input_directory = "/Volumes/data/npi_historical",
#' #'   output_file_path = "/Volumes/output/obgyn_combined.parquet",
#' #'   obgyn_taxonomy_codes = c("207V00000X", "207VG0400X", "207VM0101X"),
#' #'   file_pattern = "npi\\d{4}\\.(csv|parquet)$",
#' #'   schema_analysis_output_path = "/Volumes/output/schema_report.csv",
#' #'   schema_compatibility_mode = "union",
#' #'   verbose = FALSE
#' #' )
#' #'
#' #' # Example 3: Conservative approach using only common columns
#' #' process_npi_obgyn_data(
#' #'   input_directory = "./npi_data",
#' #'   output_file_path = "./output/obgyn_conservative.feather",
#' #'   obgyn_taxonomy_codes = c("207V00000X", "207VG0400X", "207VM0101X",
#' #'                           "207VH0002X", "207VE0102X"),
#' #'   file_pattern = "npi\\d{4}\\.(csv|parquet)$",
#' #'   schema_analysis_output_path = "./output/schema_analysis.csv",
#' #'   schema_compatibility_mode = "intersect",
#' #'   verbose = TRUE
#' #' )
#' #' }
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom dplyr filter mutate bind_rows select all_of group_by summarise n_distinct arrange desc pull
#' #' @importFrom stringr str_extract str_detect str_replace_all
#' #' @importFrom readr write_csv
#' #' @importFrom arrow write_parquet write_feather
#' #' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' #' @importFrom duckdb duckdb
#' #' @importFrom glue glue
#' #' @importFrom fs dir_exists file_exists path_ext
#' #' @importFrom purrr map_dfr
#' #' @importFrom tidyr pivot_longer
#' #'
#' #' @export
#' process_npi_obgyn_data <- function(input_directory,
#'                                    output_file_path,
#'                                    obgyn_taxonomy_codes = c("207V00000X",
#'                                                             "207VG0400X",
#'                                                             "207VM0101X",
#'                                                             "207VH0002X",
#'                                                             "207VE0102X"),
#'                                    file_pattern = "npi\\d{4}\\.(csv|parquet)$",
#'                                    schema_analysis_output_path = NULL,
#'                                    schema_compatibility_mode = "union",
#'                                    verbose = TRUE) {
#'   
#'   # Configure logger based on verbose setting
#'   if (verbose) {
#'     logger::log_threshold(logger::INFO)
#'   } else {
#'     logger::log_threshold(logger::WARN)
#'   }
#'   
#'   logger::log_info("Starting NPI OB/GYN data processing with schema analysis")
#'   
#'   # Validate inputs
#'   validate_function_inputs(input_directory, output_file_path,
#'                            obgyn_taxonomy_codes, file_pattern,
#'                            schema_analysis_output_path,
#'                            schema_compatibility_mode, verbose)
#'   
#'   # Log function inputs
#'   log_function_inputs(input_directory, output_file_path,
#'                       obgyn_taxonomy_codes, file_pattern,
#'                       schema_analysis_output_path,
#'                       schema_compatibility_mode, verbose)
#'   
#'   # Find NPI files
#'   npi_file_paths <- find_npi_files(input_directory, file_pattern)
#'   
#'   # Analyze schemas across all files
#'   schema_analysis_results <- analyze_file_schemas(npi_file_paths)
#'   
#'   # Write schema analysis if requested
#'   if (!is.null(schema_analysis_output_path)) {
#'     write_schema_analysis_report(schema_analysis_results,
#'                                  schema_analysis_output_path)
#'   }
#'   
#'   # Determine common column strategy
#'   column_mapping_strategy <- determine_column_strategy(
#'     schema_analysis_results, schema_compatibility_mode)
#'   
#'   # Process files and combine data with schema handling
#'   combined_obgyn_providers <- process_and_combine_npi_files_with_schema(
#'     npi_file_paths, obgyn_taxonomy_codes, column_mapping_strategy)
#'   
#'   # Write output file
#'   write_combined_dataset(combined_obgyn_providers, output_file_path)
#'   
#'   logger::log_info("NPI OB/GYN data processing completed successfully")
#'   logger::log_info(glue::glue("Output file written to: {output_file_path}"))
#'   
#'   return(invisible(output_file_path))
#' }
#' 
#' #' @noRd
#' validate_function_inputs <- function(input_directory, output_file_path,
#'                                      obgyn_taxonomy_codes, file_pattern,
#'                                      schema_analysis_output_path,
#'                                      schema_compatibility_mode, verbose) {
#'   logger::log_info("Validating function inputs")
#'   
#'   assertthat::assert_that(is.character(input_directory),
#'                           msg = "input_directory must be a character string")
#'   assertthat::assert_that(length(input_directory) == 1,
#'                           msg = "input_directory must be a single value")
#'   assertthat::assert_that(fs::dir_exists(input_directory),
#'                           msg = "input_directory must be an existing directory")
#'   
#'   assertthat::assert_that(is.character(output_file_path),
#'                           msg = "output_file_path must be a character string")
#'   assertthat::assert_that(length(output_file_path) == 1,
#'                           msg = "output_file_path must be a single value")
#'   
#'   output_extension <- fs::path_ext(output_file_path)
#'   assertthat::assert_that(output_extension %in% c("csv", "parquet", "feather"),
#'                           msg = "output_file_path must have .csv, .parquet, or .feather extension")
#'   
#'   assertthat::assert_that(is.character(obgyn_taxonomy_codes),
#'                           msg = "obgyn_taxonomy_codes must be a character vector")
#'   assertthat::assert_that(length(obgyn_taxonomy_codes) > 0,
#'                           msg = "obgyn_taxonomy_codes must contain at least one code")
#'   
#'   assertthat::assert_that(is.character(file_pattern),
#'                           msg = "file_pattern must be a character string")
#'   assertthat::assert_that(length(file_pattern) == 1,
#'                           msg = "file_pattern must be a single value")
#'   
#'   if (!is.null(schema_analysis_output_path)) {
#'     assertthat::assert_that(is.character(schema_analysis_output_path),
#'                             msg = "schema_analysis_output_path must be character or NULL")
#'   }
#'   
#'   assertthat::assert_that(schema_compatibility_mode %in% c("strict", "intersect", "union"),
#'                           msg = "schema_compatibility_mode must be 'strict', 'intersect', or 'union'")
#'   
#'   assertthat::assert_that(is.logical(verbose),
#'                           msg = "verbose must be logical (TRUE/FALSE)")
#'   assertthat::assert_that(length(verbose) == 1,
#'                           msg = "verbose must be a single logical value")
#'   
#'   logger::log_info("All input validation checks passed")
#' }
#' 
#' #' @noRd
#' log_function_inputs <- function(input_directory, output_file_path,
#'                                 obgyn_taxonomy_codes, file_pattern,
#'                                 schema_analysis_output_path,
#'                                 schema_compatibility_mode, verbose) {
#'   logger::log_info("=== FUNCTION INPUTS ===")
#'   logger::log_info(glue::glue("Input directory: {input_directory}"))
#'   logger::log_info(glue::glue("Output file path: {output_file_path}"))
#'   logger::log_info(glue::glue("OB/GYN taxonomy codes: {paste(obgyn_taxonomy_codes, collapse = ', ')}"))
#'   # Escape curly braces in file pattern for glue formatting
#'   escaped_pattern <- stringr::str_replace_all(file_pattern, "\\{", "{{")
#'   escaped_pattern <- stringr::str_replace_all(escaped_pattern, "\\}", "}}")
#'   logger::log_info(glue::glue("File pattern: {escaped_pattern}"))
#'   logger::log_info(glue::glue("Schema analysis output: {schema_analysis_output_path %||% 'None'}"))
#'   logger::log_info(glue::glue("Schema compatibility mode: {schema_compatibility_mode}"))
#'   logger::log_info(glue::glue("Verbose logging: {verbose}"))
#'   logger::log_info("========================")
#' }
#' 
#' #' @noRd
#' find_npi_files <- function(input_directory, file_pattern) {
#'   logger::log_info("Searching for NPI files in input directory")
#'   # Escape curly braces in file pattern for glue formatting
#'   escaped_pattern <- stringr::str_replace_all(file_pattern, "\\{", "{{")
#'   escaped_pattern <- stringr::str_replace_all(escaped_pattern, "\\}", "}}")
#'   logger::log_info(glue::glue("Using file pattern: {escaped_pattern}"))
#'   
#'   all_files <- list.files(input_directory, full.names = TRUE)
#'   npi_file_paths <- all_files[stringr::str_detect(basename(all_files), file_pattern)]
#'   
#'   logger::log_info(glue::glue("Found {length(npi_file_paths)} NPI files"))
#'   
#'   if (length(npi_file_paths) == 0) {
#'     logger::log_error("No NPI files found matching the specified pattern")
#'     stop("No NPI files found in the specified directory")
#'   }
#'   
#'   logger::log_info("NPI files found:")
#'   for (file_path in npi_file_paths) {
#'     logger::log_info(glue::glue("  - {basename(file_path)}"))
#'   }
#'   
#'   return(npi_file_paths)
#' }
#' 
#' #' @noRd
#' analyze_file_schemas <- function(npi_file_paths) {
#'   logger::log_info("=== SCHEMA ANALYSIS ===")
#'   logger::log_info("Analyzing column schemas across all NPI files")
#'   
#'   # Connect to DuckDB for schema analysis
#'   duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   on.exit(DBI::dbDisconnect(duckdb_connection))
#'   
#'   schema_analysis_list <- list()
#'   successful_files <- character()
#'   failed_files <- character()
#'   
#'   for (file_path in npi_file_paths) {
#'     current_filename <- basename(file_path)
#'     extracted_year <- extract_year_from_filename(current_filename)
#'     
#'     logger::log_info(glue::glue("Analyzing schema for: {current_filename}"))
#'     
#'     file_extension <- fs::path_ext(file_path)
#'     
#'     # Try different CSV reading strategies for better compatibility
#'     tryCatch({
#'       if (file_extension == "csv") {
#'         # Try multiple CSV reading strategies
#'         schema_info <- try_csv_schema_strategies(duckdb_connection, file_path)
#'       } else if (file_extension == "parquet") {
#'         schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 0")
#'         schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'       } else {
#'         logger::log_warn(glue::glue("Skipping unsupported file format: {file_extension}"))
#'         next
#'       }
#'       
#'       schema_analysis_list[[current_filename]] <- data.frame(
#'         filename = current_filename,
#'         year = extracted_year,
#'         column_name = schema_info$column_name,
#'         column_type = schema_info$column_type,
#'         stringsAsFactors = FALSE
#'       )
#'       
#'       successful_files <- c(successful_files, current_filename)
#'       logger::log_info(glue::glue("  Found {nrow(schema_info)} columns"))
#'       
#'     }, error = function(e) {
#'       failed_files <- c(failed_files, current_filename)
#'       logger::log_error(glue::glue("Failed to analyze schema for {current_filename}: {e$message}"))
#'       logger::log_warn(glue::glue("Continuing with remaining files..."))
#'     })
#'   }
#'   
#'   # Log summary of schema analysis
#'   logger::log_info(glue::glue("Schema analysis completed: {length(successful_files)} successful, {length(failed_files)} failed"))
#'   if (length(failed_files) > 0) {
#'     logger::log_warn("Failed files:")
#'     for (failed_file in failed_files) {
#'       logger::log_warn(glue::glue("  - {failed_file}"))
#'     }
#'   }
#'   
#'   if (length(schema_analysis_list) == 0) {
#'     logger::log_error("No files successfully analyzed")
#'     stop("Schema analysis failed for all files")
#'   }
#'   
#'   # Combine all schema information
#'   combined_schema_analysis <- dplyr::bind_rows(schema_analysis_list)
#'   
#'   # Analyze column consistency across files
#'   analyze_schema_consistency(combined_schema_analysis)
#'   
#'   logger::log_info("Schema analysis completed")
#'   return(combined_schema_analysis)
#' }
#' 
#' #' @noRd
#' try_csv_schema_strategies <- function(duckdb_connection, file_path) {
#'   logger::log_info("Attempting CSV schema analysis with multiple strategies")
#'   
#'   # Strategy 1: Standard auto-detection
#'   tryCatch({
#'     schema_query <- glue::glue("DESCRIBE SELECT * FROM read_csv_auto('{file_path}') LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("  Success with auto-detection")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("  Auto-detection failed, trying flexible mode")
#'   })
#'   
#'   # Strategy 2: Flexible mode with relaxed parsing
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("  Success with flexible mode")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("  Flexible mode failed, trying manual settings")
#'   })
#'   
#'   # Strategy 3: Manual CSV settings
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("  Success with manual CSV settings")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_error("  All CSV strategies failed")
#'     stop(glue::glue("Could not parse CSV file with any strategy: {e$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' analyze_schema_consistency <- function(combined_schema_analysis) {
#'   logger::log_info("Analyzing schema consistency across files")
#'   
#'   # Count unique files
#'   unique_files_count <- length(unique(combined_schema_analysis$filename))
#'   logger::log_info(glue::glue("Total files analyzed: {unique_files_count}"))
#'   
#'   # Find columns that appear in all files
#'   column_file_counts <- combined_schema_analysis |>
#'     dplyr::group_by(column_name) |>
#'     dplyr::summarise(
#'       files_with_column = dplyr::n_distinct(filename),
#'       files_percentage = round(files_with_column / unique_files_count * 100, 1),
#'       .groups = "drop"
#'     ) |>
#'     dplyr::arrange(dplyr::desc(files_with_column))
#'   
#'   universal_columns <- column_file_counts |>
#'     dplyr::filter(files_with_column == unique_files_count) |>
#'     dplyr::pull(column_name)
#'   
#'   partial_columns <- column_file_counts |>
#'     dplyr::filter(files_with_column < unique_files_count & files_with_column > 1) |>
#'     dplyr::pull(column_name)
#'   
#'   unique_columns <- column_file_counts |>
#'     dplyr::filter(files_with_column == 1) |>
#'     dplyr::pull(column_name)
#'   
#'   logger::log_info(glue::glue("Columns in ALL files: {length(universal_columns)}"))
#'   logger::log_info(glue::glue("Columns in SOME files: {length(partial_columns)}"))
#'   logger::log_info(glue::glue("Columns in ONE file only: {length(unique_columns)}"))
#'   
#'   if (length(universal_columns) > 0) {
#'     logger::log_info("Universal columns (first 10):")
#'     for (col in head(universal_columns, 10)) {
#'       logger::log_info(glue::glue("  - {col}"))
#'     }
#'   }
#'   
#'   if (length(partial_columns) > 0) {
#'     logger::log_warn("Columns with inconsistent presence (first 10):")
#'     for (col in head(partial_columns, 10)) {
#'       file_count <- column_file_counts[column_file_counts$column_name == col, "files_with_column"]
#'       percentage <- column_file_counts[column_file_counts$column_name == col, "files_percentage"]
#'       logger::log_warn(glue::glue("  - {col} (in {file_count}/{unique_files_count} files, {percentage}%)"))
#'     }
#'   }
#'   
#'   # Check for NBER taxonomy columns specifically
#'   all_columns <- unique(combined_schema_analysis$column_name)
#'   
#'   # Look for NBER-style taxonomy columns
#'   nber_taxonomy_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$", "^ptaxgroup\\d+$")
#'   found_taxonomy_columns <- character()
#'   
#'   for (pattern in nber_taxonomy_patterns) {
#'     matching_cols <- all_columns[stringr::str_detect(all_columns, pattern)]
#'     found_taxonomy_columns <- c(found_taxonomy_columns, matching_cols)
#'   }
#'   
#'   # If no NBER columns, try original NPPES patterns
#'   if (length(found_taxonomy_columns) == 0) {
#'     nppes_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
#'     for (pattern in nppes_patterns) {
#'       matching_cols <- all_columns[stringr::str_detect(all_columns, pattern)]
#'       found_taxonomy_columns <- c(found_taxonomy_columns, matching_cols)
#'     }
#'   }
#'   
#'   found_taxonomy_columns <- unique(found_taxonomy_columns)
#'   
#'   if (length(found_taxonomy_columns) > 0) {
#'     logger::log_info("Taxonomy-related columns found:")
#'     for (col in head(found_taxonomy_columns, 15)) {  # Show first 15
#'       file_count <- column_file_counts[column_file_counts$column_name == col, "files_with_column"]
#'       percentage <- column_file_counts[column_file_counts$column_name == col, "files_percentage"]
#'       logger::log_info(glue::glue("  - {col} (in {file_count}/{unique_files_count} files, {percentage}%)"))
#'     }
#'     if (length(found_taxonomy_columns) > 15) {
#'       logger::log_info(glue::glue("  ... and {length(found_taxonomy_columns) - 15} more taxonomy columns"))
#'     }
#'   } else {
#'     logger::log_error("No taxonomy-related columns found in any files!")
#'     logger::log_info("Showing first 20 column names for debugging:")
#'     for (col in head(sort(all_columns), 20)) {
#'       logger::log_info(glue::glue("  - {col}"))
#'     }
#'   }
#' }
#' 
#' #' @noRd
#' determine_column_strategy <- function(schema_analysis_results,
#'                                       schema_compatibility_mode) {
#'   logger::log_info(glue::glue("Determining column strategy using mode: {schema_compatibility_mode}"))
#'   
#'   unique_files_count <- length(unique(schema_analysis_results$filename))
#'   
#'   if (schema_compatibility_mode == "strict") {
#'     # Only use columns that exist in ALL files
#'     universal_columns <- schema_analysis_results |>
#'       dplyr::group_by(column_name) |>
#'       dplyr::summarise(file_count = dplyr::n_distinct(filename), .groups = "drop") |>
#'       dplyr::filter(file_count == unique_files_count) |>
#'       dplyr::pull(column_name)
#'     
#'     logger::log_info(glue::glue("STRICT mode: Using {length(universal_columns)} universal columns"))
#'     return(list(mode = "strict", columns = universal_columns))
#'     
#'   } else if (schema_compatibility_mode == "intersect") {
#'     # Use columns that exist in ALL files (same as strict)
#'     universal_columns <- schema_analysis_results |>
#'       dplyr::group_by(column_name) |>
#'       dplyr::summarise(file_count = dplyr::n_distinct(filename), .groups = "drop") |>
#'       dplyr::filter(file_count == unique_files_count) |>
#'       dplyr::pull(column_name)
#'     
#'     logger::log_info(glue::glue("INTERSECT mode: Using {length(universal_columns)} universal columns"))
#'     return(list(mode = "intersect", columns = universal_columns))
#'     
#'   } else if (schema_compatibility_mode == "union") {
#'     # Use ALL columns from ALL files
#'     all_unique_columns <- unique(schema_analysis_results$column_name)
#'     
#'     logger::log_info(glue::glue("UNION mode: Using {length(all_unique_columns)} total unique columns"))
#'     logger::log_warn("UNION mode will create NA values for missing columns in some files")
#'     return(list(mode = "union", columns = all_unique_columns))
#'   }
#' }
#' 
#' #' @noRd
#' write_schema_analysis_report <- function(schema_analysis_results,
#'                                          schema_analysis_output_path) {
#'   logger::log_info("Writing schema analysis report")
#'   logger::log_info(glue::glue("Report path: {schema_analysis_output_path}"))
#'   
#'   # Create summary by column
#'   column_summary <- schema_analysis_results |>
#'     dplyr::group_by(column_name) |>
#'     dplyr::summarise(
#'       files_with_column = dplyr::n_distinct(filename),
#'       unique_types = dplyr::n_distinct(column_type),
#'       most_common_type = names(sort(table(column_type), decreasing = TRUE))[1],
#'       files_list = paste(unique(filename), collapse = "; "),
#'       .groups = "drop"
#'     ) |>
#'     dplyr::arrange(dplyr::desc(files_with_column), column_name)
#'   
#'   readr::write_csv(column_summary, schema_analysis_output_path)
#'   logger::log_info("Schema analysis report written successfully")
#' }
#' 
#' #' @noRd
#' process_and_combine_npi_files_with_schema <- function(npi_file_paths,
#'                                                       obgyn_taxonomy_codes,
#'                                                       column_mapping_strategy) {
#'   logger::log_info("Starting file processing with schema-aware combination")
#'   
#'   # Connect to DuckDB
#'   duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   on.exit(DBI::dbDisconnect(duckdb_connection))
#'   
#'   processed_datasets <- list()
#'   successful_files <- character()
#'   failed_files <- character()
#'   
#'   for (i in seq_along(npi_file_paths)) {
#'     current_file_path <- npi_file_paths[i]
#'     current_filename <- basename(current_file_path)
#'     
#'     logger::log_info(glue::glue("Processing file {i}/{length(npi_file_paths)}: {current_filename}"))
#'     
#'     # Extract year from filename
#'     extracted_year <- extract_year_from_filename(current_filename)
#'     
#'     # Process current file with schema handling and error recovery
#'     tryCatch({
#'       filtered_provider_data <- process_single_npi_file_with_schema(
#'         duckdb_connection, current_file_path, obgyn_taxonomy_codes,
#'         extracted_year, column_mapping_strategy)
#'       
#'       if (nrow(filtered_provider_data) > 0) {
#'         processed_datasets[[length(processed_datasets) + 1]] <- filtered_provider_data
#'         successful_files <- c(successful_files, current_filename)
#'         logger::log_info(glue::glue("Added {nrow(filtered_provider_data)} OB/GYN providers from {current_filename}"))
#'       } else {
#'         logger::log_warn(glue::glue("No OB/GYN providers found in {current_filename}"))
#'         successful_files <- c(successful_files, current_filename)  # Still count as successful
#'       }
#'       
#'     }, error = function(e) {
#'       failed_files <- c(failed_files, current_filename)
#'       logger::log_error(glue::glue("Failed to process {current_filename}: {e$message}"))
#'       logger::log_warn("Continuing with remaining files...")
#'     })
#'   }
#'   
#'   # Log processing summary
#'   logger::log_info(glue::glue("File processing completed: {length(successful_files)} successful, {length(failed_files)} failed"))
#'   if (length(failed_files) > 0) {
#'     logger::log_warn("Failed files:")
#'     for (failed_file in failed_files) {
#'       logger::log_warn(glue::glue("  - {failed_file}"))
#'     }
#'   }
#'   
#'   # Combine all datasets with schema alignment
#'   if (length(processed_datasets) > 0) {
#'     combined_obgyn_providers <- combine_datasets_with_schema_alignment(
#'       processed_datasets, column_mapping_strategy)
#'     logger::log_info("Successfully combined all processed datasets")
#'     logger::log_info(glue::glue("Total OB/GYN providers in combined dataset: {nrow(combined_obgyn_providers)}"))
#'   } else {
#'     logger::log_error("No OB/GYN providers found in any successfully processed files")
#'     stop("No data to combine - no OB/GYN providers found in any successfully processed files")
#'   }
#'   
#'   return(combined_obgyn_providers)
#' }
#' 
#' #' @noRd
#' extract_year_from_filename <- function(filename) {
#'   extracted_year <- stringr::str_extract(filename, "\\d{4}")
#'   
#'   if (is.na(extracted_year)) {
#'     logger::log_warn(glue::glue("Could not extract year from filename: {filename}"))
#'     extracted_year <- "unknown"
#'   } else {
#'     logger::log_info(glue::glue("Extracted year {extracted_year} from filename: {filename}"))
#'   }
#'   
#'   return(extracted_year)
#' }
#' 
#' #' @noRd
#' process_single_npi_file_with_schema <- function(duckdb_connection, file_path,
#'                                                 obgyn_taxonomy_codes,
#'                                                 extracted_year,
#'                                                 column_mapping_strategy) {
#'   current_filename <- basename(file_path)
#'   logger::log_info(glue::glue("Reading file with schema handling: {current_filename}"))
#'   
#'   # Create taxonomy code filter condition for SQL
#'   taxonomy_filter_condition <- paste0("'", obgyn_taxonomy_codes, "'", collapse = ", ")
#'   
#'   # Build SQL query based on file type and schema strategy
#'   file_extension <- fs::path_ext(file_path)
#'   
#'   # Determine which columns to select
#'   if (column_mapping_strategy$mode %in% c("strict", "intersect")) {
#'     column_selection <- paste(column_mapping_strategy$columns, collapse = ", ")
#'   } else {
#'     column_selection <- "*"
#'   }
#'   
#'   logger::log_info("Executing DuckDB query to filter OB/GYN providers")
#'   
#'   # Execute query with robust CSV handling
#'   tryCatch({
#'     if (file_extension == "csv") {
#'       filtered_provider_data <- try_csv_data_strategies(
#'         duckdb_connection, file_path, column_selection, 
#'         taxonomy_filter_condition, extracted_year, current_filename)
#'     } else if (file_extension == "parquet") {
#'       sql_query <- glue::glue("
#'         SELECT {column_selection},
#'                '{extracted_year}' as data_year,
#'                '{current_filename}' as source_filename
#'         FROM read_parquet('{file_path}')
#'         WHERE Healthcare_Provider_Taxonomy_Code_1 IN ({taxonomy_filter_condition})
#'            OR Healthcare_Provider_Taxonomy_Code_2 IN ({taxonomy_filter_condition})
#'            OR Healthcare_Provider_Taxonomy_Code_3 IN ({taxonomy_filter_condition})
#'            OR Healthcare_Provider_Taxonomy_Code_4 IN ({taxonomy_filter_condition})
#'            OR Healthcare_Provider_Taxonomy_Code_5 IN ({taxonomy_filter_condition})
#'       ")
#'       filtered_provider_data <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     } else {
#'       logger::log_error(glue::glue("Unsupported file format: {file_extension}"))
#'       stop(glue::glue("Unsupported file format: {file_extension}"))
#'     }
#'     
#'     logger::log_info(glue::glue("Query completed. Found {nrow(filtered_provider_data)} matching providers"))
#'     return(filtered_provider_data)
#'     
#'   }, error = function(e) {
#'     logger::log_error(glue::glue("Failed to process file {current_filename}: {e$message}"))
#'     logger::log_warn("Returning empty dataset for this file")
#'     return(data.frame())
#'   })
#' }
#' 
#' #' @noRd
#' try_csv_data_strategies <- function(duckdb_connection, file_path, column_selection,
#'                                     taxonomy_filter_condition, extracted_year, 
#'                                     current_filename) {
#'   logger::log_info("Attempting CSV data processing with multiple strategies")
#'   
#'   # Get discovered taxonomy columns
#'   taxonomy_columns <- get_taxonomy_columns_for_file(duckdb_connection, file_path)
#'   
#'   if (length(taxonomy_columns) == 0) {
#'     logger::log_warn("No taxonomy columns found for this file - returning empty dataset")
#'     return(data.frame())
#'   }
#'   
#'   # Build WHERE clause with discovered taxonomy columns
#'   where_clause <- build_taxonomy_where_clause(taxonomy_columns, taxonomy_filter_condition)
#'   
#'   # Strategy 1: Standard auto-detection
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {column_selection},
#'              '{extracted_year}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv_auto('{file_path}')
#'       WHERE {where_clause}
#'     ")
#'     filtered_data <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("  Success with auto-detection")
#'     return(filtered_data)
#'   }, error = function(e) {
#'     logger::log_info("  Auto-detection failed, trying flexible mode")
#'   })
#'   
#'   # Strategy 2: Flexible mode with relaxed parsing
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {column_selection},
#'              '{extracted_year}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       )
#'       WHERE {where_clause}
#'     ")
#'     filtered_data <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("  Success with flexible mode")
#'     return(filtered_data)
#'   }, error = function(e) {
#'     logger::log_info("  Flexible mode failed, trying manual settings")
#'   })
#'   
#'   # Strategy 3: Manual CSV settings
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {column_selection},
#'              '{extracted_year}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       )
#'       WHERE {where_clause}
#'     ")
#'     filtered_data <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("  Success with manual CSV settings")
#'     return(filtered_data)
#'   }, error = function(e) {
#'     logger::log_error("  All CSV strategies failed")
#'     stop(glue::glue("Could not process CSV file with any strategy: {e$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' get_taxonomy_columns_for_file <- function(duckdb_connection, file_path) {
#'   logger::log_info("Discovering taxonomy columns for current file")
#'   
#'   file_extension <- fs::path_ext(file_path)
#'   
#'   # Get column names for this specific file
#'   tryCatch({
#'     if (file_extension == "csv") {
#'       # Try to get column names with flexible parsing
#'       schema_query <- glue::glue("
#'         DESCRIBE SELECT * FROM read_csv('{file_path}', 
#'           auto_detect=true, 
#'           ignore_errors=true, 
#'           null_padding=true,
#'           max_line_size=10000000
#'         ) LIMIT 0")
#'     } else {
#'       schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 0")
#'     }
#'     
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     all_columns <- schema_info$column_name
#'     
#'     # Look for NBER-style taxonomy columns (ptaxcode1, ptaxcode2, etc.)
#'     nber_taxonomy_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$", "^ptaxgroup\\d+$")
#'     found_taxonomy_columns <- character()
#'     
#'     for (pattern in nber_taxonomy_patterns) {
#'       matching_cols <- all_columns[stringr::str_detect(all_columns, pattern)]
#'       found_taxonomy_columns <- c(found_taxonomy_columns, matching_cols)
#'     }
#'     
#'     # If no NBER columns found, try original NPPES column patterns
#'     if (length(found_taxonomy_columns) == 0) {
#'       nppes_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
#'       for (pattern in nppes_patterns) {
#'         matching_cols <- all_columns[stringr::str_detect(all_columns, pattern)]
#'         found_taxonomy_columns <- c(found_taxonomy_columns, matching_cols)
#'       }
#'     }
#'     
#'     found_taxonomy_columns <- unique(found_taxonomy_columns)
#'     
#'     logger::log_info(glue::glue("Found {length(found_taxonomy_columns)} taxonomy columns in this file:"))
#'     for (col in found_taxonomy_columns) {
#'       logger::log_info(glue::glue("  - {col}"))
#'     }
#'     
#'     return(found_taxonomy_columns)
#'     
#'   }, error = function(e) {
#'     logger::log_error(glue::glue("Failed to discover taxonomy columns: {e$message}"))
#'     return(character())
#'   })
#' }
#' 
#' #' @noRd  
#' build_taxonomy_where_clause <- function(taxonomy_columns, taxonomy_filter_condition) {
#'   if (length(taxonomy_columns) == 0) {
#'     return("1=0")  # Return condition that evaluates to FALSE
#'   }
#'   
#'   # Build OR conditions for each taxonomy column
#'   conditions <- character()
#'   for (col in taxonomy_columns) {
#'     conditions <- c(conditions, glue::glue("{col} IN ({taxonomy_filter_condition})"))
#'   }
#'   
#'   where_clause <- paste(conditions, collapse = " OR ")
#'   logger::log_info(glue::glue("Built WHERE clause: {substr(where_clause, 1, 100)}..."))
#'   
#'   return(where_clause)
#' }
#' 
#' #' @noRd
#' combine_datasets_with_schema_alignment <- function(processed_datasets,
#'                                                    column_mapping_strategy) {
#'   logger::log_info("Combining datasets with schema alignment")
#'   
#'   if (column_mapping_strategy$mode == "union") {
#'     # For union mode, handle data type mismatches carefully
#'     logger::log_info("Harmonizing data types before combining datasets")
#'     
#'     # Get all unique column names across all datasets
#'     all_column_names <- unique(unlist(lapply(processed_datasets, names)))
#'     
#'     # Harmonize each dataset to have consistent data types
#'     harmonized_datasets <- list()
#'     for (i in seq_along(processed_datasets)) {
#'       harmonized_datasets[[i]] <- harmonize_dataset_types(processed_datasets[[i]], all_column_names, i)
#'     }
#'     
#'     combined_dataset <- dplyr::bind_rows(harmonized_datasets)
#'     logger::log_info("Used bind_rows for union mode with type harmonization - missing columns filled with NA")
#'   } else {
#'     # For strict/intersect modes, all datasets should have the same columns
#'     combined_dataset <- dplyr::bind_rows(processed_datasets)
#'     logger::log_info("Combined datasets using common column structure")
#'   }
#'   
#'   logger::log_info(glue::glue("Combined dataset dimensions: {nrow(combined_dataset)} x {ncol(combined_dataset)}"))
#'   
#'   return(combined_dataset)
#' }
#' 
#' #' @noRd
#' harmonize_dataset_types <- function(single_dataset, all_column_names, dataset_index) {
#'   logger::log_info(glue::glue("Harmonizing types for dataset {dataset_index}"))
#'   
#'   # Convert ALL potentially problematic columns to character to avoid type conflicts
#'   # This is a comprehensive list of columns that can vary in type across years
#'   problematic_columns <- c(
#'     # Fax and phone numbers
#'     "pmailfax", "plocfax", "aofax", "aotelnum", "ploctelnum",
#'     
#'     # Other provider IDs (all of them)
#'     paste0("othpid", 1:50),
#'     paste0("othpidiss", 1:50), 
#'     paste0("othpidst", 1:50),
#'     paste0("othpidty", 1:50),
#'     
#'     # Provider license numbers
#'     paste0("plicnum", 1:15),
#'     paste0("plicstate", 1:15),
#'     
#'     # Other identifiers
#'     "certdate", "dup_npi", "npi", "ein",
#'     
#'     # Dates that might be inconsistent
#'     "lastupdate", "npideactdate", "npireactdate", "penumdate"
#'   )
#'   
#'   for (col in problematic_columns) {
#'     if (col %in% names(single_dataset)) {
#'       if (!is.character(single_dataset[[col]])) {
#'         single_dataset[[col]] <- as.character(single_dataset[[col]])
#'         logger::log_info(glue::glue("  Converted {col} to character"))
#'       }
#'     }
#'   }
#'   
#'   return(single_dataset)
#' }
#' 
#' #' @noRd
#' write_combined_dataset <- function(combined_obgyn_providers, output_file_path) {
#'   logger::log_info("Writing combined dataset to output file")
#'   logger::log_info(glue::glue("Output path: {output_file_path}"))
#'   logger::log_info(glue::glue("Number of records to write: {nrow(combined_obgyn_providers)}"))
#'   logger::log_info(glue::glue("Number of columns: {ncol(combined_obgyn_providers)}"))
#'   
#'   output_extension <- fs::path_ext(output_file_path)
#'   
#'   if (output_extension == "csv") {
#'     readr::write_csv(combined_obgyn_providers, output_file_path)
#'     logger::log_info("Successfully wrote CSV file")
#'   } else if (output_extension == "parquet") {
#'     arrow::write_parquet(combined_obgyn_providers, output_file_path)
#'     logger::log_info("Successfully wrote Parquet file")
#'   } else if (output_extension == "feather") {
#'     arrow::write_feather(combined_obgyn_providers, output_file_path)
#'     logger::log_info("Successfully wrote Feather file")
#'   } else {
#'     logger::log_error(glue::glue("Unsupported output format: {output_extension}"))
#'     stop(glue::glue("Unsupported output format: {output_extension}"))
#'   }
#'   
#'   # Verify file was created
#'   if (fs::file_exists(output_file_path)) {
#'     file_size <- file.size(output_file_path)
#'     logger::log_info(glue::glue("Output file created successfully. File size: {file_size} bytes"))
#'   } else {
#'     logger::log_error("Failed to create output file")
#'     stop("Output file was not created")
#'   }
#' }
#' 
#' 
#' # Execute ----
#' # Then paste the ENTIRE updated function code from the artifact
#' # (All the code from #' Process NPI Files... to the very end)
#' 
#' # Then run your processing command
#' process_npi_obgyn_data(
#'   input_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
#'   schema_analysis_output_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis.csv",
#'   file_pattern = "npi(\\d{4,5}|_.*?)\\.(csv|parquet)$",
#'   schema_compatibility_mode = "union",
#'   verbose = TRUE
#' )
#' 
#' 
#' # Fucntion at 1413 ----
#' #' Process NPI Files to Extract OB/GYN Providers with Robust Schema Analysis
#' #'
#' #' This function processes multiple NPI (National Provider Identifier) files
#' #' from NBER, automatically detects and handles schema differences between file 
#' #' formats, filters records to include only OB/GYN providers based on taxonomy 
#' #' codes, adds year information extracted from filenames, and combines them into 
#' #' a single harmonized dataset using DuckDB for efficient processing.
#' #'
#' #' @param npi_input_directory Character string specifying the directory containing
#' #'   NPI files. Must be a valid existing directory path. Default is current 
#' #'   working directory.
#' #' @param obgyn_output_file_path Character string specifying the full path for the
#' #'   output file including filename and extension. Supported formats: CSV,
#' #'   parquet, or feather. Default writes to "combined_obgyn_providers.csv" in 
#' #'   current directory.
#' #' @param obgyn_taxonomy_code_vector Character vector of taxonomy codes identifying
#' #'   OB/GYN providers. Default includes comprehensive OB/GYN taxonomy codes 
#' #'   covering general obstetrics & gynecology, reproductive endocrinology, 
#' #'   maternal-fetal medicine, gynecologic oncology, and female pelvic medicine.
#' #' @param npi_file_pattern Character string regex pattern to match NPI files.
#' #'   Default matches files with "npi" followed by 4-5 digits and .csv or
#' #'   .parquet extensions.
#' #' @param schema_analysis_report_path Character string specifying output path
#' #'   for detailed schema analysis report. If NULL (default), no report is written.
#' #'   When specified, creates a comprehensive CSV report of column compatibility.
#' #' @param schema_compatibility_strategy Character string specifying how to handle
#' #'   schema differences between files. Options: "strict" (error on differences), 
#' #'   "intersect" (use only universal columns), "union" (use all columns, fill 
#' #'   missing with NA). Default is "union" for maximum data preservation.
#' #' @param enable_verbose_logging Logical indicating whether to enable verbose 
#' #'   logging to console. When TRUE, logs all operations, data transformations, 
#' #'   and file processing details. When FALSE, only warnings and errors are shown.
#' #'   Default is TRUE.
#' #'
#' #' @return Invisible character string of the output file path if successful.
#' #'   Function completes successfully and returns path, or stops with informative
#' #'   error message if processing fails.
#' #'
#' #' @examples
#' #' \dontrun{
#' #' # Example 1: Basic usage with comprehensive OB/GYN taxonomy codes
#' #' # and automatic schema detection for mixed file formats
#' #' processed_file_path <- process_npi_obgyn_data(
#' #'   npi_input_directory = "/path/to/npi/historical/files",
#' #'   obgyn_output_file_path = "/path/to/output/obgyn_providers_2024.csv",
#' #'   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", 
#' #'                                  "207VM0101X", "207VH0002X"),
#' #'   npi_file_pattern = "npi\\d{4,5}\\.(csv|parquet)$",
#' #'   schema_analysis_report_path = "/path/to/schema_compatibility.csv",
#' #'   schema_compatibility_strategy = "union",
#' #'   enable_verbose_logging = TRUE
#' #' )
#' #' # Output: Processed 11 files, combined 275,347 OB/GYN providers
#' #' # Schema report shows column compatibility across years
#' #'
#' #' # Example 2: Conservative processing using only common columns
#' #' # across all file formats with detailed schema validation
#' #' conservative_output <- process_npi_obgyn_data(
#' #'   npi_input_directory = "/Volumes/data/npi_historical_downloads",
#' #'   obgyn_output_file_path = "/Volumes/output/obgyn_intersect.parquet",
#' #'   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", 
#' #'                                  "207VM0101X", "207VH0002X", 
#' #'                                  "207VE0102X"),
#' #'   npi_file_pattern = "npi\\d{4}\\.(csv|parquet)$",
#' #'   schema_analysis_report_path = "/Volumes/output/detailed_schema.csv",
#' #'   schema_compatibility_strategy = "intersect", 
#' #'   enable_verbose_logging = FALSE
#' #' )
#' #' # Output: Uses only columns present in ALL files
#' #' # Ensures maximum compatibility but may lose some data fields
#' #'
#' #' # Example 3: High-performance processing with strict validation
#' #' # for production environments requiring error-free execution
#' #' production_result <- process_npi_obgyn_data(
#' #'   npi_input_directory = "./data/npi_yearly_extracts",
#' #'   obgyn_output_file_path = "./output/production_obgyn.feather",
#' #'   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", 
#' #'                                  "207VM0101X", "207VH0002X", 
#' #'                                  "207VE0102X", "207VX0000X"),
#' #'   npi_file_pattern = "npi_\\d{4}_quarterly\\.(csv|parquet)$",
#' #'   schema_analysis_report_path = "./logs/schema_validation.csv",
#' #'   schema_compatibility_strategy = "strict",
#' #'   enable_verbose_logging = TRUE
#' #' )
#' #' # Output: Fails fast if any schema incompatibilities detected
#' #' # Generates comprehensive validation report for troubleshooting
#' #' }
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error log_threshold INFO WARN
#' #' @importFrom dplyr filter mutate bind_rows select all_of group_by summarise 
#' #' @importFrom dplyr n_distinct arrange desc pull case_when
#' #' @importFrom stringr str_extract str_detect str_replace_all str_c
#' #' @importFrom readr write_csv
#' #' @importFrom arrow write_parquet write_feather
#' #' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' #' @importFrom duckdb duckdb
#' #' @importFrom glue glue
#' #' @importFrom fs dir_exists file_exists path_ext file_size
#' #' @importFrom purrr map_dfr map_chr
#' #' @importFrom tidyr pivot_longer
#' #'
#' #' @export
#' process_npi_obgyn_data <- function(npi_input_directory = ".",
#'                                    obgyn_output_file_path = "combined_obgyn_providers.csv",
#'                                    obgyn_taxonomy_code_vector = c("207V00000X",
#'                                                                   "207VG0400X", 
#'                                                                   "207VM0101X",
#'                                                                   "207VH0002X",
#'                                                                   "207VE0102X"),
#'                                    npi_file_pattern = "npi\\d{4,5}\\.(csv|parquet)$",
#'                                    schema_analysis_report_path = NULL,
#'                                    schema_compatibility_strategy = "union",
#'                                    enable_verbose_logging = TRUE) {
#'   
#'   # Configure logging based on verbose setting
#'   configure_function_logging(enable_verbose_logging)
#'   
#'   logger::log_info("=== STARTING NPI OB/GYN DATA PROCESSING ===")
#'   logger::log_info("Function: process_npi_obgyn_data")
#'   logger::log_info("Timestamp: {Sys.time()}")
#'   
#'   # Validate all function inputs with detailed assertions
#'   validate_npi_function_inputs(npi_input_directory, obgyn_output_file_path,
#'                                obgyn_taxonomy_code_vector, npi_file_pattern,
#'                                schema_analysis_report_path, 
#'                                schema_compatibility_strategy, 
#'                                enable_verbose_logging)
#'   
#'   # Log comprehensive function inputs for debugging
#'   log_npi_function_inputs(npi_input_directory, obgyn_output_file_path,
#'                           obgyn_taxonomy_code_vector, npi_file_pattern,
#'                           schema_analysis_report_path, 
#'                           schema_compatibility_strategy, 
#'                           enable_verbose_logging)
#'   
#'   # Discover and validate NPI files in input directory  
#'   discovered_npi_file_paths <- discover_npi_files_with_validation(
#'     npi_input_directory, npi_file_pattern)
#'   
#'   # Perform comprehensive schema analysis across all files
#'   comprehensive_schema_analysis <- analyze_npi_file_schemas_comprehensive(
#'     discovered_npi_file_paths)
#'   
#'   # Write detailed schema analysis report if requested
#'   if (!is.null(schema_analysis_report_path)) {
#'     write_comprehensive_schema_analysis_report(comprehensive_schema_analysis,
#'                                                schema_analysis_report_path)
#'   }
#'   
#'   # Determine column harmonization strategy based on compatibility analysis
#'   column_harmonization_strategy <- determine_column_harmonization_strategy(
#'     comprehensive_schema_analysis, schema_compatibility_strategy)
#'   
#'   # Process all files and combine with intelligent schema handling
#'   combined_obgyn_provider_dataset <- process_and_combine_npi_files_with_schema_intelligence(
#'     discovered_npi_file_paths, obgyn_taxonomy_code_vector, 
#'     column_harmonization_strategy)
#'   
#'   # Write final combined dataset with format validation
#'   write_combined_obgyn_dataset_with_validation(combined_obgyn_provider_dataset, 
#'                                                obgyn_output_file_path)
#'   
#'   # Log successful completion with summary statistics
#'   log_processing_completion_summary(combined_obgyn_provider_dataset, 
#'                                     obgyn_output_file_path)
#'   
#'   logger::log_info("=== NPI OB/GYN DATA PROCESSING COMPLETED SUCCESSFULLY ===")
#'   
#'   return(invisible(obgyn_output_file_path))
#' }
#' 
#' #' @noRd
#' configure_function_logging <- function(enable_verbose_logging) {
#'   if (enable_verbose_logging) {
#'     logger::log_threshold(logger::INFO)
#'   } else {
#'     logger::log_threshold(logger::WARN)
#'   }
#'   logger::log_info("Logging configuration completed")
#' }
#' 
#' #' @noRd
#' validate_npi_function_inputs <- function(npi_input_directory, obgyn_output_file_path,
#'                                          obgyn_taxonomy_code_vector, npi_file_pattern,
#'                                          schema_analysis_report_path,
#'                                          schema_compatibility_strategy, 
#'                                          enable_verbose_logging) {
#'   logger::log_info("=== VALIDATING FUNCTION INPUTS ===")
#'   
#'   # Validate input directory
#'   assertthat::assert_that(is.character(npi_input_directory),
#'                           msg = "npi_input_directory must be a character string")
#'   assertthat::assert_that(length(npi_input_directory) == 1,
#'                           msg = "npi_input_directory must be a single value")
#'   assertthat::assert_that(fs::dir_exists(npi_input_directory),
#'                           msg = "npi_input_directory must be an existing directory")
#'   
#'   # Validate output file path  
#'   assertthat::assert_that(is.character(obgyn_output_file_path),
#'                           msg = "obgyn_output_file_path must be a character string")
#'   assertthat::assert_that(length(obgyn_output_file_path) == 1,
#'                           msg = "obgyn_output_file_path must be a single value")
#'   
#'   output_file_extension <- fs::path_ext(obgyn_output_file_path)
#'   assertthat::assert_that(output_file_extension %in% c("csv", "parquet", "feather"),
#'                           msg = "obgyn_output_file_path must have .csv, .parquet, or .feather extension")
#'   
#'   # Validate taxonomy codes
#'   assertthat::assert_that(is.character(obgyn_taxonomy_code_vector),
#'                           msg = "obgyn_taxonomy_code_vector must be a character vector")
#'   assertthat::assert_that(length(obgyn_taxonomy_code_vector) > 0,
#'                           msg = "obgyn_taxonomy_code_vector must contain at least one code")
#'   assertthat::assert_that(all(nchar(obgyn_taxonomy_code_vector) == 10),
#'                           msg = "All taxonomy codes must be exactly 10 characters")
#'   
#'   # Validate file pattern
#'   assertthat::assert_that(is.character(npi_file_pattern),
#'                           msg = "npi_file_pattern must be a character string")
#'   assertthat::assert_that(length(npi_file_pattern) == 1,
#'                           msg = "npi_file_pattern must be a single value")
#'   
#'   # Validate schema analysis output path if provided
#'   if (!is.null(schema_analysis_report_path)) {
#'     assertthat::assert_that(is.character(schema_analysis_report_path),
#'                             msg = "schema_analysis_report_path must be character or NULL")
#'     assertthat::assert_that(length(schema_analysis_report_path) == 1,
#'                             msg = "schema_analysis_report_path must be a single value")
#'   }
#'   
#'   # Validate schema compatibility strategy
#'   valid_strategies <- c("strict", "intersect", "union")
#'   assertthat::assert_that(schema_compatibility_strategy %in% valid_strategies,
#'                           msg = glue::glue("schema_compatibility_strategy must be one of: {paste(valid_strategies, collapse = ', ')}"))
#'   
#'   # Validate verbose logging flag
#'   assertthat::assert_that(is.logical(enable_verbose_logging),
#'                           msg = "enable_verbose_logging must be logical (TRUE/FALSE)")
#'   assertthat::assert_that(length(enable_verbose_logging) == 1,
#'                           msg = "enable_verbose_logging must be a single logical value")
#'   
#'   logger::log_info("All input validation checks passed successfully")
#' }
#' 
#' #' @noRd
#' log_npi_function_inputs <- function(npi_input_directory, obgyn_output_file_path,
#'                                     obgyn_taxonomy_code_vector, npi_file_pattern,
#'                                     schema_analysis_report_path,
#'                                     schema_compatibility_strategy, 
#'                                     enable_verbose_logging) {
#'   logger::log_info("=== FUNCTION INPUT PARAMETERS ===")
#'   logger::log_info("Input directory: {npi_input_directory}")
#'   logger::log_info("Output file path: {obgyn_output_file_path}")
#'   logger::log_info("OB/GYN taxonomy codes ({length(obgyn_taxonomy_code_vector)} total): {paste(obgyn_taxonomy_code_vector, collapse = ', ')}")
#'   
#'   # Escape curly braces in file pattern for glue formatting
#'   escaped_file_pattern <- stringr::str_replace_all(npi_file_pattern, "\\{", "{{")
#'   escaped_file_pattern <- stringr::str_replace_all(escaped_file_pattern, "\\}", "}}")
#'   logger::log_info("NPI file pattern: {escaped_file_pattern}")
#'   
#'   logger::log_info("Schema analysis report path: {schema_analysis_report_path %||% 'None specified'}")
#'   logger::log_info("Schema compatibility strategy: {schema_compatibility_strategy}")
#'   logger::log_info("Verbose logging enabled: {enable_verbose_logging}")
#'   logger::log_info("=================================")
#' }
#' 
#' #' @noRd
#' discover_npi_files_with_validation <- function(npi_input_directory, npi_file_pattern) {
#'   logger::log_info("=== DISCOVERING NPI FILES ===")
#'   logger::log_info("Searching directory: {npi_input_directory}")
#'   
#'   # Escape curly braces in file pattern for logging
#'   escaped_pattern <- stringr::str_replace_all(npi_file_pattern, "\\{", "{{")
#'   escaped_pattern <- stringr::str_replace_all(escaped_pattern, "\\}", "}}")
#'   logger::log_info("Using file pattern: {escaped_pattern}")
#'   
#'   # Get all files in directory
#'   all_directory_files <- list.files(npi_input_directory, full.names = TRUE)
#'   logger::log_info("Total files in directory: {length(all_directory_files)}")
#'   
#'   # Filter files matching the NPI pattern
#'   matching_npi_file_paths <- all_directory_files[stringr::str_detect(basename(all_directory_files), npi_file_pattern)]
#'   
#'   logger::log_info("NPI files matching pattern: {length(matching_npi_file_paths)}")
#'   
#'   # Validate that files were found
#'   assertthat::assert_that(length(matching_npi_file_paths) > 0,
#'                           msg = glue::glue("No NPI files found matching pattern '{npi_file_pattern}' in directory '{npi_input_directory}'"))
#'   
#'   # Log discovered files with details
#'   logger::log_info("Discovered NPI files:")
#'   for (file_path in matching_npi_file_paths) {
#'     file_name <- basename(file_path)
#'     file_size_bytes <- fs::file_size(file_path)
#'     file_extension <- fs::path_ext(file_path)
#'     logger::log_info("  - {file_name} ({file_extension}, {file_size_bytes} bytes)")
#'   }
#'   
#'   logger::log_info("File discovery completed successfully")
#'   return(matching_npi_file_paths)
#' }
#' 
#' #' @noRd
#' analyze_npi_file_schemas_comprehensive <- function(discovered_npi_file_paths) {
#'   logger::log_info("=== COMPREHENSIVE SCHEMA ANALYSIS ===")
#'   logger::log_info("Analyzing {length(discovered_npi_file_paths)} NPI files for column compatibility")
#'   
#'   # Connect to DuckDB for schema analysis
#'   duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   on.exit(DBI::dbDisconnect(duckdb_connection), add = TRUE)
#'   
#'   schema_analysis_results_list <- list()
#'   successful_analysis_files <- character()
#'   failed_analysis_files <- character()
#'   
#'   for (current_file_path in discovered_npi_file_paths) {
#'     current_file_name <- basename(current_file_path)
#'     extracted_file_year <- extract_year_from_npi_filename(current_file_name)
#'     
#'     logger::log_info("Analyzing schema: {current_file_name}")
#'     
#'     # Analyze schema with error handling
#'     tryCatch({
#'       file_schema_info <- analyze_single_file_schema_robust(duckdb_connection, current_file_path)
#'       
#'       schema_analysis_results_list[[current_file_name]] <- dplyr::tibble(
#'         filename = current_file_name,
#'         extracted_year = extracted_file_year,
#'         column_name = file_schema_info$column_name,
#'         column_type = file_schema_info$column_type,
#'         file_format = fs::path_ext(current_file_path)
#'       )
#'       
#'       successful_analysis_files <- c(successful_analysis_files, current_file_name)
#'       logger::log_info("  Schema analysis successful: {nrow(file_schema_info)} columns detected")
#'       
#'     }, error = function(e) {
#'       failed_analysis_files <- c(failed_analysis_files, current_file_name)
#'       logger::log_error("Schema analysis failed for {current_file_name}: {e$message}")
#'       logger::log_warn("Continuing with remaining files...")
#'     })
#'   }
#'   
#'   # Validate schema analysis results
#'   assertthat::assert_that(length(schema_analysis_results_list) > 0,
#'                           msg = "Schema analysis failed for all files - no usable data")
#'   
#'   # Log analysis summary
#'   logger::log_info("Schema analysis summary:")
#'   logger::log_info("  Successful: {length(successful_analysis_files)} files")
#'   logger::log_info("  Failed: {length(failed_analysis_files)} files")
#'   
#'   if (length(failed_analysis_files) > 0) {
#'     logger::log_warn("Files with schema analysis failures:")
#'     for (failed_file in failed_analysis_files) {
#'       logger::log_warn("  - {failed_file}")
#'     }
#'   }
#'   
#'   # Combine all schema information into comprehensive dataset
#'   combined_schema_analysis <- dplyr::bind_rows(schema_analysis_results_list)
#'   
#'   # Perform cross-file compatibility analysis
#'   perform_cross_file_compatibility_analysis(combined_schema_analysis)
#'   
#'   logger::log_info("Comprehensive schema analysis completed")
#'   return(combined_schema_analysis)
#' }
#' 
#' #' @noRd  
#' extract_year_from_npi_filename <- function(npi_filename) {
#'   extracted_year <- stringr::str_extract(npi_filename, "\\d{4}")
#'   
#'   if (is.na(extracted_year)) {
#'     logger::log_warn("Could not extract year from filename: {npi_filename}")
#'     extracted_year <- "unknown"
#'   } else {
#'     logger::log_info("  Extracted year: {extracted_year}")
#'   }
#'   
#'   return(extracted_year)
#' }
#' 
#' #' @noRd
#' analyze_single_file_schema_robust <- function(duckdb_connection, file_path) {
#'   current_file_extension <- fs::path_ext(file_path)
#'   
#'   if (current_file_extension == "csv") {
#'     schema_info <- analyze_csv_schema_with_fallback(duckdb_connection, file_path)
#'   } else if (current_file_extension == "parquet") {
#'     schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'   } else {
#'     stop(glue::glue("Unsupported file format: {current_file_extension}"))
#'   }
#'   
#'   return(schema_info)
#' }
#' 
#' #' @noRd
#' analyze_csv_schema_with_fallback <- function(duckdb_connection, file_path) {
#'   logger::log_info("  Attempting CSV schema analysis with multiple strategies")
#'   
#'   # Strategy 1: Auto-detection
#'   tryCatch({
#'     schema_query <- glue::glue("DESCRIBE SELECT * FROM read_csv_auto('{file_path}') LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with auto-detection strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("    Auto-detection failed, trying flexible parsing")
#'   })
#'   
#'   # Strategy 2: Flexible parsing
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with flexible parsing strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("    Flexible parsing failed, trying manual configuration")
#'   })
#'   
#'   # Strategy 3: Manual configuration
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with manual configuration strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_error("    All CSV parsing strategies failed")
#'     stop(glue::glue("Could not analyze CSV schema with any strategy: {e$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' perform_cross_file_compatibility_analysis <- function(combined_schema_analysis) {
#'   logger::log_info("=== CROSS-FILE COMPATIBILITY ANALYSIS ===")
#'   
#'   # Calculate file and column statistics
#'   total_files_analyzed <- length(unique(combined_schema_analysis$filename))
#'   total_unique_columns <- length(unique(combined_schema_analysis$column_name))
#'   
#'   logger::log_info("Analysis scope:")
#'   logger::log_info("  Total files analyzed: {total_files_analyzed}")
#'   logger::log_info("  Total unique columns: {total_unique_columns}")
#'   
#'   # Analyze column presence across files
#'   column_presence_analysis <- combined_schema_analysis |>
#'     dplyr::group_by(column_name) |>
#'     dplyr::summarise(
#'       files_containing_column = dplyr::n_distinct(filename),
#'       presence_percentage = round(files_containing_column / total_files_analyzed * 100, 1),
#'       file_formats = paste(unique(file_format), collapse = ", "),
#'       .groups = "drop"
#'     ) |>
#'     dplyr::arrange(dplyr::desc(files_containing_column), column_name)
#'   
#'   # Categorize columns by presence
#'   universal_columns <- column_presence_analysis |>
#'     dplyr::filter(files_containing_column == total_files_analyzed) |>
#'     dplyr::pull(column_name)
#'   
#'   partial_presence_columns <- column_presence_analysis |>
#'     dplyr::filter(files_containing_column < total_files_analyzed & files_containing_column > 1) |>
#'     dplyr::pull(column_name)
#'   
#'   file_specific_columns <- column_presence_analysis |>
#'     dplyr::filter(files_containing_column == 1) |>
#'     dplyr::pull(column_name)
#'   
#'   # Log compatibility analysis results
#'   logger::log_info("Column compatibility results:")
#'   logger::log_info("  Universal columns (present in ALL files): {length(universal_columns)}")
#'   logger::log_info("  Partial presence columns (SOME files): {length(partial_presence_columns)}")
#'   logger::log_info("  File-specific columns (ONE file only): {length(file_specific_columns)}")
#'   
#'   # Log details for universal columns
#'   if (length(universal_columns) > 0) {
#'     logger::log_info("Universal columns (first 10):")
#'     for (col in head(universal_columns, 10)) {
#'       logger::log_info("    {col}")
#'     }
#'     if (length(universal_columns) > 10) {
#'       logger::log_info("    ... and {length(universal_columns) - 10} more universal columns")
#'     }
#'   }
#'   
#'   # Analyze taxonomy column presence specifically
#'   analyze_taxonomy_column_presence(combined_schema_analysis, total_files_analyzed)
#' }
#' 
#' #' @noRd
#' analyze_taxonomy_column_presence <- function(combined_schema_analysis, total_files_analyzed) {
#'   logger::log_info("=== TAXONOMY COLUMN ANALYSIS ===")
#'   
#'   all_discovered_columns <- unique(combined_schema_analysis$column_name)
#'   
#'   # Define taxonomy column patterns for different file formats
#'   nber_taxonomy_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$", "^ptaxgroup\\d+$")
#'   nppes_taxonomy_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
#'   
#'   # Find NBER-style taxonomy columns
#'   nber_taxonomy_columns <- character()
#'   for (pattern in nber_taxonomy_patterns) {
#'     matching_columns <- all_discovered_columns[stringr::str_detect(all_discovered_columns, pattern)]
#'     nber_taxonomy_columns <- c(nber_taxonomy_columns, matching_columns)
#'   }
#'   
#'   # Find NPPES-style taxonomy columns  
#'   nppes_taxonomy_columns <- character()
#'   for (pattern in nppes_taxonomy_patterns) {
#'     matching_columns <- all_discovered_columns[stringr::str_detect(all_discovered_columns, pattern)]
#'     nppes_taxonomy_columns <- c(nppes_taxonomy_columns, matching_columns)
#'   }
#'   
#'   all_taxonomy_columns <- unique(c(nber_taxonomy_columns, nppes_taxonomy_columns))
#'   
#'   # Log taxonomy column analysis
#'   if (length(all_taxonomy_columns) > 0) {
#'     logger::log_info("Taxonomy columns discovered:")
#'     logger::log_info("  NBER format columns: {length(nber_taxonomy_columns)}")
#'     logger::log_info("  NPPES format columns: {length(nppes_taxonomy_columns)}")
#'     logger::log_info("  Total taxonomy columns: {length(all_taxonomy_columns)}")
#'     
#'     # Show detailed taxonomy column presence
#'     taxonomy_presence <- combined_schema_analysis |>
#'       dplyr::filter(column_name %in% all_taxonomy_columns) |>
#'       dplyr::group_by(column_name) |>
#'       dplyr::summarise(
#'         files_with_column = dplyr::n_distinct(filename),
#'         presence_percentage = round(files_with_column / total_files_analyzed * 100, 1),
#'         .groups = "drop"
#'       ) |>
#'       dplyr::arrange(dplyr::desc(files_with_column))
#'     
#'     logger::log_info("Taxonomy column presence analysis (first 15):")
#'     for (i in seq_len(min(15, nrow(taxonomy_presence)))) {
#'       col_info <- taxonomy_presence[i, ]
#'       logger::log_info("    {col_info$column_name}: {col_info$files_with_column}/{total_files_analyzed} files ({col_info$presence_percentage}%)")
#'     }
#'     
#'     if (nrow(taxonomy_presence) > 15) {
#'       logger::log_info("    ... and {nrow(taxonomy_presence) - 15} more taxonomy columns")
#'     }
#'     
#'   } else {
#'     logger::log_error("CRITICAL: No taxonomy columns found in any files!")
#'     logger::log_info("Available columns for debugging (first 20):")
#'     for (col in head(sort(all_discovered_columns), 20)) {
#'       logger::log_info("    {col}")
#'     }
#'     stop("No taxonomy columns detected - cannot filter OB/GYN providers")
#'   }
#' }
#' 
#' #' @noRd
#' determine_column_harmonization_strategy <- function(comprehensive_schema_analysis, 
#'                                                     schema_compatibility_strategy) {
#'   logger::log_info("=== DETERMINING COLUMN HARMONIZATION STRATEGY ===")
#'   logger::log_info("Selected strategy: {schema_compatibility_strategy}")
#'   
#'   total_analyzed_files <- length(unique(comprehensive_schema_analysis$filename))
#'   
#'   if (schema_compatibility_strategy == "strict") {
#'     # Use only columns present in ALL files
#'     universal_column_set <- comprehensive_schema_analysis |>
#'       dplyr::group_by(column_name) |>
#'       dplyr::summarise(file_count = dplyr::n_distinct(filename), .groups = "drop") |>
#'       dplyr::filter(file_count == total_analyzed_files) |>
#'       dplyr::pull(column_name)
#'     
#'     logger::log_info("STRICT strategy: Using {length(universal_column_set)} universal columns")
#'     
#'     # Validate that essential columns are present
#'     assertthat::assert_that(length(universal_column_set) > 0,
#'                             msg = "STRICT mode failed: No universal columns found across all files")
#'     
#'     return(list(strategy_mode = "strict", selected_columns = universal_column_set))
#'     
#'   } else if (schema_compatibility_strategy == "intersect") {
#'     # Use only columns present in ALL files (same as strict)
#'     intersect_column_set <- comprehensive_schema_analysis |>
#'       dplyr::group_by(column_name) |>
#'       dplyr::summarise(file_count = dplyr::n_distinct(filename), .groups = "drop") |>
#'       dplyr::filter(file_count == total_analyzed_files) |>
#'       dplyr::pull(column_name)
#'     
#'     logger::log_info("INTERSECT strategy: Using {length(intersect_column_set)} common columns")
#'     
#'     # Validate intersection is not empty
#'     assertthat::assert_that(length(intersect_column_set) > 0,
#'                             msg = "INTERSECT mode failed: No common columns found across all files")
#'     
#'     return(list(strategy_mode = "intersect", selected_columns = intersect_column_set))
#'     
#'   } else if (schema_compatibility_strategy == "union") {
#'     # Use ALL columns from ALL files
#'     union_column_set <- unique(comprehensive_schema_analysis$column_name)
#'     
#'     logger::log_info("UNION strategy: Using {length(union_column_set)} total unique columns")
#'     logger::log_warn("UNION mode will create NA values for columns missing in some files")
#'     
#'     return(list(strategy_mode = "union", selected_columns = union_column_set))
#'   }
#'   
#'   stop(glue::glue("Invalid schema compatibility strategy: {schema_compatibility_strategy}"))
#' }
#' 
#' #' @noRd
#' write_comprehensive_schema_analysis_report <- function(comprehensive_schema_analysis,
#'                                                        schema_analysis_report_path) {
#'   logger::log_info("=== WRITING SCHEMA ANALYSIS REPORT ===")
#'   logger::log_info("Report destination: {schema_analysis_report_path}")
#'   
#'   # Create detailed column summary with compatibility metrics
#'   column_compatibility_summary <- comprehensive_schema_analysis |>
#'     dplyr::group_by(column_name) |>
#'     dplyr::summarise(
#'       files_containing_column = dplyr::n_distinct(filename),
#'       unique_data_types = dplyr::n_distinct(column_type),
#'       most_common_data_type = names(sort(table(column_type), decreasing = TRUE))[1],
#'       file_formats_present = paste(unique(file_format), collapse = "; "),
#'       files_list = paste(unique(filename), collapse = "; "),
#'       years_covered = paste(unique(extracted_year), collapse = "; "),
#'       .groups = "drop"
#'     ) |>
#'     dplyr::arrange(dplyr::desc(files_containing_column), column_name)
#'   
#'   # Write comprehensive report
#'   readr::write_csv(column_compatibility_summary, schema_analysis_report_path)
#'   
#'   # Log report statistics
#'   total_columns_analyzed <- nrow(column_compatibility_summary)
#'   total_files_in_analysis <- length(unique(comprehensive_schema_analysis$filename))
#'   
#'   logger::log_info("Schema analysis report written successfully")
#'   logger::log_info("  Total columns analyzed: {total_columns_analyzed}")
#'   logger::log_info("  Total files analyzed: {total_files_in_analysis}")
#'   logger::log_info("  Report saved to: {schema_analysis_report_path}")
#' }
#' 
#' #' @noRd
#' process_and_combine_npi_files_with_schema_intelligence <- function(discovered_npi_file_paths,
#'                                                                    obgyn_taxonomy_code_vector,
#'                                                                    column_harmonization_strategy) {
#'   logger::log_info("=== PROCESSING AND COMBINING NPI FILES ===")
#'   logger::log_info("Strategy: {column_harmonization_strategy$strategy_mode}")
#'   logger::log_info("Files to process: {length(discovered_npi_file_paths)}")
#'   
#'   # Connect to DuckDB for data processing
#'   duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   on.exit(DBI::dbDisconnect(duckdb_connection), add = TRUE)
#'   
#'   processed_datasets_list <- list()
#'   successful_processing_files <- character()
#'   failed_processing_files <- character()
#'   
#'   for (i in seq_along(discovered_npi_file_paths)) {
#'     current_npi_file_path <- discovered_npi_file_paths[i]
#'     current_npi_filename <- basename(current_npi_file_path)
#'     
#'     logger::log_info("Processing file {i}/{length(discovered_npi_file_paths)}: {current_npi_filename}")
#'     
#'     # Extract year and process with error handling
#'     extracted_year_info <- extract_year_from_npi_filename(current_npi_filename)
#'     
#'     tryCatch({
#'       filtered_obgyn_data <- process_single_npi_file_with_intelligent_schema(
#'         duckdb_connection, current_npi_file_path, obgyn_taxonomy_code_vector,
#'         extracted_year_info, column_harmonization_strategy)
#'       
#'       if (nrow(filtered_obgyn_data) > 0) {
#'         processed_datasets_list[[length(processed_datasets_list) + 1]] <- filtered_obgyn_data
#'         successful_processing_files <- c(successful_processing_files, current_npi_filename)
#'         logger::log_info("  Successfully processed: {nrow(filtered_obgyn_data)} OB/GYN providers found")
#'       } else {
#'         logger::log_warn("  No OB/GYN providers found in {current_npi_filename}")
#'         successful_processing_files <- c(successful_processing_files, current_npi_filename)
#'       }
#'       
#'     }, error = function(e) {
#'       failed_processing_files <- c(failed_processing_files, current_npi_filename)
#'       logger::log_error("  Processing failed for {current_npi_filename}: {e$message}")
#'       logger::log_warn("  Continuing with remaining files...")
#'     })
#'   }
#'   
#'   # Validate processing results
#'   assertthat::assert_that(length(processed_datasets_list) > 0,
#'                           msg = "No OB/GYN providers found in any successfully processed files")
#'   
#'   # Log processing summary
#'   logger::log_info("File processing summary:")
#'   logger::log_info("  Successfully processed: {length(successful_processing_files)} files")
#'   logger::log_info("  Processing failures: {length(failed_processing_files)} files")
#'   logger::log_info("  Datasets with OB/GYN data: {length(processed_datasets_list)}")
#'   
#'   # Combine datasets with intelligent schema alignment
#'   combined_harmonized_dataset <- combine_datasets_with_intelligent_schema_alignment(
#'     processed_datasets_list, column_harmonization_strategy)
#'   
#'   logger::log_info("Dataset combination completed successfully")
#'   return(combined_harmonized_dataset)
#' }
#' 
#' #' @noRd
#' process_single_npi_file_with_intelligent_schema <- function(duckdb_connection, 
#'                                                             npi_file_path,
#'                                                             obgyn_taxonomy_code_vector,
#'                                                             extracted_year_info,
#'                                                             column_harmonization_strategy) {
#'   current_filename <- basename(npi_file_path)
#'   logger::log_info("  Reading file with intelligent schema handling: {current_filename}")
#'   
#'   # Create SQL-safe taxonomy code filter
#'   taxonomy_code_filter_sql <- paste0("'", obgyn_taxonomy_code_vector, "'", collapse = ", ")
#'   
#'   # Determine file format and build appropriate query
#'   current_file_extension <- fs::path_ext(npi_file_path)
#'   
#'   # Determine column selection based on strategy
#'   if (column_harmonization_strategy$strategy_mode %in% c("strict", "intersect")) {
#'     selected_columns_sql <- paste(column_harmonization_strategy$selected_columns, collapse = ", ")
#'   } else {
#'     selected_columns_sql <- "*"
#'   }
#'   
#'   logger::log_info("  Executing DuckDB query to filter OB/GYN providers")
#'   
#'   # Process file with intelligent error handling
#'   tryCatch({
#'     if (current_file_extension == "csv") {
#'       filtered_provider_dataset <- process_csv_file_with_taxonomy_detection(
#'         duckdb_connection, npi_file_path, selected_columns_sql, 
#'         taxonomy_code_filter_sql, extracted_year_info, current_filename)
#'     } else if (current_file_extension == "parquet") {
#'       filtered_provider_dataset <- process_parquet_file_with_taxonomy_detection(
#'         duckdb_connection, npi_file_path, selected_columns_sql,
#'         taxonomy_code_filter_sql, extracted_year_info, current_filename)
#'     } else {
#'       stop(glue::glue("Unsupported file format: {current_file_extension}"))
#'     }
#'     
#'     logger::log_info("  Query execution completed: {nrow(filtered_provider_dataset)} matching providers")
#'     return(filtered_provider_dataset)
#'     
#'   }, error = function(e) {
#'     logger::log_error("  Failed to process {current_filename}: {e$message}")
#'     logger::log_warn("  Returning empty dataset for this file")
#'     return(dplyr::tibble())
#'   })
#' }
#' 
#' #' @noRd
#' process_csv_file_with_taxonomy_detection <- function(duckdb_connection, npi_file_path, 
#'                                                      selected_columns_sql,
#'                                                      taxonomy_code_filter_sql, 
#'                                                      extracted_year_info, 
#'                                                      current_filename) {
#'   logger::log_info("    Processing CSV file with taxonomy detection")
#'   
#'   # Get available taxonomy columns for this specific file
#'   available_taxonomy_columns <- discover_taxonomy_columns_in_file(duckdb_connection, npi_file_path)
#'   
#'   if (length(available_taxonomy_columns) == 0) {
#'     logger::log_warn("    No taxonomy columns found - returning empty dataset")
#'     return(dplyr::tibble())
#'   }
#'   
#'   # Build dynamic WHERE clause based on discovered columns
#'   taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(available_taxonomy_columns, 
#'                                                                taxonomy_code_filter_sql)
#'   
#'   # Execute CSV query with multiple fallback strategies
#'   return(execute_csv_query_with_fallback(duckdb_connection, npi_file_path, 
#'                                          selected_columns_sql, taxonomy_where_clause,
#'                                          extracted_year_info, current_filename))
#' }
#' 
#' #' @noRd
#' process_parquet_file_with_taxonomy_detection <- function(duckdb_connection, npi_file_path,
#'                                                          selected_columns_sql,
#'                                                          taxonomy_code_filter_sql,
#'                                                          extracted_year_info,
#'                                                          current_filename) {
#'   logger::log_info("    Processing Parquet file with taxonomy detection")
#'   
#'   # Get available taxonomy columns for this specific file
#'   available_taxonomy_columns <- discover_taxonomy_columns_in_file(duckdb_connection, npi_file_path)
#'   
#'   if (length(available_taxonomy_columns) == 0) {
#'     logger::log_warn("    No taxonomy columns found - returning empty dataset")
#'     return(dplyr::tibble())
#'   }
#'   
#'   # Build dynamic WHERE clause
#'   taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(available_taxonomy_columns,
#'                                                                taxonomy_code_filter_sql)
#'   
#'   # Execute Parquet query
#'   sql_query <- glue::glue("
#'     SELECT {selected_columns_sql},
#'            '{extracted_year_info}' as data_year,
#'            '{current_filename}' as source_filename
#'     FROM read_parquet('{npi_file_path}')
#'     WHERE {taxonomy_where_clause}
#'   ")
#'   
#'   filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'   return(filtered_dataset)
#' }
#' 
#' #' @noRd
#' discover_taxonomy_columns_in_file <- function(duckdb_connection, npi_file_path) {
#'   current_file_extension <- fs::path_ext(npi_file_path)
#'   
#'   # Get schema information for the file
#'   tryCatch({
#'     if (current_file_extension == "csv") {
#'       schema_query <- glue::glue("
#'         DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
#'           auto_detect=true, 
#'           ignore_errors=true, 
#'           null_padding=true,
#'           max_line_size=10000000
#'         ) LIMIT 0")
#'     } else {
#'       schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{npi_file_path}') LIMIT 0")
#'     }
#'     
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     available_columns <- schema_info$column_name
#'     
#'     # Look for NBER-style taxonomy columns first (ptaxcode1, ptaxcode2, etc.)
#'     nber_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$")
#'     discovered_taxonomy_columns <- character()
#'     
#'     for (pattern in nber_patterns) {
#'       matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
#'       discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
#'     }
#'     
#'     # If no NBER columns, look for NPPES-style columns
#'     if (length(discovered_taxonomy_columns) == 0) {
#'       nppes_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
#'       for (pattern in nppes_patterns) {
#'         matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
#'         discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
#'       }
#'     }
#'     
#'     discovered_taxonomy_columns <- unique(discovered_taxonomy_columns)
#'     
#'     if (length(discovered_taxonomy_columns) > 0) {
#'       logger::log_info("    Found {length(discovered_taxonomy_columns)} taxonomy columns")
#'     } else {
#'       logger::log_warn("    No taxonomy columns detected in file")
#'     }
#'     
#'     return(discovered_taxonomy_columns)
#'     
#'   }, error = function(e) {
#'     logger::log_error("    Failed to discover taxonomy columns: {e$message}")
#'     return(character())
#'   })
#' }
#' 
#' #' @noRd
#' build_dynamic_taxonomy_where_clause <- function(available_taxonomy_columns, 
#'                                                 taxonomy_code_filter_sql) {
#'   if (length(available_taxonomy_columns) == 0) {
#'     return("1=0")  # Return FALSE condition
#'   }
#'   
#'   # Build OR conditions for each available taxonomy column
#'   taxonomy_conditions <- character()
#'   for (taxonomy_col in available_taxonomy_columns) {
#'     condition <- glue::glue("{taxonomy_col} IN ({taxonomy_code_filter_sql})")
#'     taxonomy_conditions <- c(taxonomy_conditions, condition)
#'   }
#'   
#'   dynamic_where_clause <- paste(taxonomy_conditions, collapse = " OR ")
#'   
#'   # Log the WHERE clause (truncated for readability)
#'   clause_preview <- if (nchar(dynamic_where_clause) > 100) {
#'     paste0(substr(dynamic_where_clause, 1, 100), "...")
#'   } else {
#'     dynamic_where_clause
#'   }
#'   logger::log_info("    Built taxonomy WHERE clause: {clause_preview}")
#'   
#'   return(dynamic_where_clause)
#' }
#' 
#' #' @noRd
#' execute_csv_query_with_fallback <- function(duckdb_connection, npi_file_path,
#'                                             selected_columns_sql, taxonomy_where_clause,
#'                                             extracted_year_info, current_filename) {
#'   logger::log_info("    Executing CSV query with fallback strategies")
#'   
#'   # Strategy 1: Auto-detection
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv_auto('{npi_file_path}')
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with auto-detection strategy")
#'     return(filtered_dataset)
#'   }, error = function(e) {
#'     logger::log_info("      Auto-detection failed, trying flexible parsing")
#'   })
#'   
#'   # Strategy 2: Flexible parsing
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{npi_file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       )
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with flexible parsing strategy")
#'     return(filtered_dataset)
#'   }, error = function(e) {
#'     logger::log_info("      Flexible parsing failed, trying manual configuration")
#'   })
#'   
#'   # Strategy 3: Manual configuration
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{npi_file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       )
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with manual configuration strategy")
#'     return(filtered_dataset)
#'   }, error = function(e) {
#'     logger::log_error("      All CSV query strategies failed")
#'     stop(glue::glue("Could not execute CSV query with any strategy: {e$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' combine_datasets_with_intelligent_schema_alignment <- function(processed_datasets_list,
#'                                                                column_harmonization_strategy) {
#'   logger::log_info("=== COMBINING DATASETS WITH INTELLIGENT SCHEMA ALIGNMENT ===")
#'   logger::log_info("Combination strategy: {column_harmonization_strategy$strategy_mode}")
#'   logger::log_info("Datasets to combine: {length(processed_datasets_list)}")
#'   
#'   if (column_harmonization_strategy$strategy_mode == "union") {
#'     # For union mode, harmonize data types to prevent conflicts
#'     logger::log_info("Harmonizing data types for union mode")
#'     
#'     # Get all unique column names across datasets
#'     all_unique_column_names <- unique(unlist(purrr::map(processed_datasets_list, names)))
#'     
#'     # Harmonize each dataset to consistent types
#'     harmonized_datasets_list <- list()
#'     for (i in seq_along(processed_datasets_list)) {
#'       harmonized_datasets_list[[i]] <- harmonize_dataset_data_types_comprehensive(
#'         processed_datasets_list[[i]], all_unique_column_names, i)
#'     }
#'     
#'     # Combine with bind_rows (handles missing columns automatically)
#'     combined_harmonized_dataset <- dplyr::bind_rows(harmonized_datasets_list)
#'     logger::log_info("Union combination completed - missing columns filled with NA")
#'     
#'   } else {
#'     # For strict/intersect modes, datasets should have consistent columns
#'     combined_harmonized_dataset <- dplyr::bind_rows(processed_datasets_list)
#'     logger::log_info("Standard combination completed using common column structure")
#'   }
#'   
#'   # Log final dataset statistics
#'   final_rows <- nrow(combined_harmonized_dataset)
#'   final_columns <- ncol(combined_harmonized_dataset)
#'   
#'   logger::log_info("Final combined dataset:")
#'   logger::log_info("  Total rows: {final_rows}")
#'   logger::log_info("  Total columns: {final_columns}")
#'   logger::log_info("  Schema alignment completed successfully")
#'   
#'   return(combined_harmonized_dataset)
#' }
#' 
#' #' @noRd
#' harmonize_dataset_data_types_comprehensive <- function(single_dataset, 
#'                                                        all_unique_column_names, 
#'                                                        dataset_index) {
#'   logger::log_info("  Harmonizing data types for dataset {dataset_index}")
#'   
#'   # Define columns that commonly have type conflicts across years/formats
#'   type_problematic_columns <- c(
#'     # Phone and fax numbers (can be numeric or character)
#'     "pmailfax", "plocfax", "aofax", "aotelnum", "ploctelnum", "pmailtel",
#'     
#'     # Provider identifiers (various formats)
#'     paste0("othpid", 1:50),
#'     paste0("othpidiss", 1:50), 
#'     paste0("othpidst", 1:50),
#'     paste0("othpidty", 1:50),
#'     
#'     # License numbers (can be numeric or alphanumeric)
#'     paste0("plicnum", 1:15),
#'     paste0("plicstate", 1:15),
#'     
#'     # Core identifiers
#'     "npi", "ein", "replacement_npi",
#'     
#'     # Date fields (can have inconsistent formats)
#'     "lastupdate", "npideactdate", "npireactdate", "penumdate",
#'     "penumdate", "certificationdate"
#'   )
#'   
#'   # Convert problematic columns to character to avoid type conflicts
#'   for (col_name in type_problematic_columns) {
#'     if (col_name %in% names(single_dataset)) {
#'       if (!is.character(single_dataset[[col_name]])) {
#'         single_dataset[[col_name]] <- as.character(single_dataset[[col_name]])
#'         logger::log_info("    Converted {col_name} to character for type consistency")
#'       }
#'     }
#'   }
#'   
#'   return(single_dataset)
#' }
#' 
#' #' @noRd
#' write_combined_obgyn_dataset_with_validation <- function(combined_obgyn_provider_dataset, 
#'                                                          obgyn_output_file_path) {
#'   logger::log_info("=== WRITING COMBINED DATASET ===")
#'   logger::log_info("Output destination: {obgyn_output_file_path}")
#'   
#'   # Log dataset statistics before writing
#'   total_records <- nrow(combined_obgyn_provider_dataset)
#'   total_columns <- ncol(combined_obgyn_provider_dataset)
#'   
#'   logger::log_info("Dataset to write:")
#'   logger::log_info("  Total records: {total_records}")
#'   logger::log_info("  Total columns: {total_columns}")
#'   
#'   # Validate dataset is not empty
#'   assertthat::assert_that(total_records > 0,
#'                           msg = "Cannot write empty dataset - no OB/GYN providers found")
#'   assertthat::assert_that(total_columns > 0,
#'                           msg = "Cannot write dataset with no columns")
#'   
#'   # Determine output format and write accordingly
#'   output_file_extension <- fs::path_ext(obgyn_output_file_path)
#'   
#'   if (output_file_extension == "csv") {
#'     readr::write_csv(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote CSV file")
#'   } else if (output_file_extension == "parquet") {
#'     arrow::write_parquet(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote Parquet file")
#'   } else if (output_file_extension == "feather") {
#'     arrow::write_feather(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote Feather file")
#'   } else {
#'     stop(glue::glue("Unsupported output format: {output_file_extension}"))
#'   }
#'   
#'   # Verify file was created and log file information
#'   verify_output_file_creation(obgyn_output_file_path)
#' }
#' 
#' #' @noRd
#' verify_output_file_creation <- function(obgyn_output_file_path) {
#'   assertthat::assert_that(fs::file_exists(obgyn_output_file_path),
#'                           msg = "Output file was not created successfully")
#'   
#'   output_file_size <- fs::file_size(obgyn_output_file_path)
#'   logger::log_info("Output file verification:")
#'   logger::log_info("  File exists: TRUE")
#'   logger::log_info("  File size: {output_file_size} bytes")
#'   logger::log_info("  File path: {obgyn_output_file_path}")
#' }
#' 
#' #' @noRd
#' log_processing_completion_summary <- function(combined_obgyn_provider_dataset, 
#'                                               obgyn_output_file_path) {
#'   logger::log_info("=== PROCESSING COMPLETION SUMMARY ===")
#'   
#'   # Calculate and log summary statistics
#'   total_obgyn_providers <- nrow(combined_obgyn_provider_dataset)
#'   total_data_columns <- ncol(combined_obgyn_provider_dataset)
#'   
#'   # Calculate year distribution if data_year column exists
#'   if ("data_year" %in% names(combined_obgyn_provider_dataset)) {
#'     year_distribution <- combined_obgyn_provider_dataset |>
#'       dplyr::count(data_year, sort = TRUE)
#'     
#'     logger::log_info("OB/GYN providers by year:")
#'     for (i in seq_len(nrow(year_distribution))) {
#'       year_info <- year_distribution[i, ]
#'       logger::log_info("  {year_info$data_year}: {year_info$n} providers")
#'     }
#'   }
#'   
#'   # Calculate file source distribution if source_filename column exists
#'   if ("source_filename" %in% names(combined_obgyn_provider_dataset)) {
#'     source_distribution <- combined_obgyn_provider_dataset |>
#'       dplyr::count(source_filename, sort = TRUE)
#'     
#'     logger::log_info("OB/GYN providers by source file:")
#'     for (i in seq_len(nrow(source_distribution))) {
#'       source_info <- source_distribution[i, ]
#'       logger::log_info("  {source_info$source_filename}: {source_info$n} providers")
#'     }
#'   }
#'   
#'   logger::log_info("Final processing statistics:")
#'   logger::log_info("  Total OB/GYN providers: {total_obgyn_providers}")
#'   logger::log_info("  Total data columns: {total_data_columns}")
#'   logger::log_info("  Output file: {obgyn_output_file_path}")
#'   logger::log_info("====================================")
#' }
#' 
#' # execute ----
#' process_npi_obgyn_data (npi_input_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'                                    obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
#'                                    obgyn_taxonomy_code_vector = c("207V00000X",
#'                                                                   "207VG0400X", 
#'                                                                   "207VM0101X",
#'                                                                   "207VH0002X",
#'                                                                   "207VE0102X"),
#'                                    npi_file_pattern = "npi\\d{4,5}\\.(csv|parquet)$",
#'                                    schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis.csv",
#'                                    schema_compatibility_strategy = "union",
#'                                    enable_verbose_logging = TRUE)
#' 
#' process_npi_obgyn_data(
#'   input_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
#'   schema_analysis_output_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis.csv",
#'   file_pattern = "npi(\\d{4,5}|_.*?)\\.(csv|parquet)$",
#'   schema_compatibility_mode = "union",
#'   verbose = TRUE
#' )
#' 
#' # Function 1435 ----
#' #' Process NPI Data Files to Extract OB/GYN Providers with Intelligent Schema 
#' #' Alignment
#' #'
#' #' This function processes National Provider Identifier (NPI) data files from 
#' #' a specified directory, filters for OB/GYN providers based on taxonomy codes,
#' #' and combines the results into a single harmonized dataset. The function 
#' #' handles multiple file formats (CSV, Parquet), performs comprehensive schema
#' #' analysis, and applies intelligent data type harmonization to prevent 
#' #' combination errors.
#' #'
#' #' @param npi_input_directory_path Character string specifying the full path 
#' #'   to the directory containing NPI data files. Directory must exist and 
#' #'   contain files matching the specified pattern.
#' #' @param obgyn_output_file_path Character string specifying the full path 
#' #'   where the combined OB/GYN provider dataset should be written. Supported 
#' #'   formats: .csv, .parquet, .feather
#' #' @param obgyn_taxonomy_code_vector Character vector of OB/GYN taxonomy codes
#' #'   to filter for. Default includes standard OB/GYN codes: "207V00000X" 
#' #'   (general), "207VG0400X" (gynecologic oncology), "207VM0101X" (maternal 
#' #'   & fetal medicine), "207VH0002X" (hospice), "207VE0102X" (reproductive 
#' #'   endocrinology)
#' #' @param npi_file_pattern_regex Character string regex pattern to identify 
#' #'   NPI files in the input directory. Default pattern matches files like 
#' #'   "npi2013.csv", "npi20204.parquet", etc.
#' #' @param schema_analysis_report_path Character string specifying where to 
#' #'   write the comprehensive schema analysis report. Default creates 
#' #'   "schema_analysis_report.csv" in the same directory as output file.
#' #' @param schema_compatibility_strategy Character string specifying how to 
#' #'   handle column differences across files. Options: "strict" (only common 
#' #'   columns), "intersect" (intersection of all columns), "union" (all unique
#' #'   columns with NA for missing). Default is "union".
#' #' @param enable_verbose_logging Logical flag to enable detailed console 
#' #'   logging throughout the processing pipeline. Default is TRUE.
#' #'
#' #' @return Invisible tibble containing the combined OB/GYN provider dataset.
#' #'   The function primarily works by side-effect (writing output file and 
#' #'   generating logs).
#' #'
#' #' @examples
#' #' # Example 1: Basic usage with default OB/GYN taxonomy codes
#' #' \dontrun{
#' #' combined_obgyn_providers <- process_npi_obgyn_data(
#' #'   npi_input_directory_path = "/data/npi_files",
#' #'   obgyn_output_file_path = "/output/obgyn_providers.csv",
#' #'   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", 
#' #'                                  "207VM0101X", "207VH0002X", 
#' #'                                  "207VE0102X"),
#' #'   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#' #'   schema_analysis_report_path = "/output/schema_analysis.csv",
#' #'   schema_compatibility_strategy = "union",
#' #'   enable_verbose_logging = TRUE
#' #' )
#' #' # Output: Combined dataset with ~70,000 OB/GYN providers across years
#' #' # Files: /output/obgyn_providers.csv, /output/schema_analysis.csv
#' #' }
#' #'
#' #' # Example 2: Strict schema mode with custom taxonomy codes
#' #' \dontrun{
#' #' maternal_fetal_specialists <- process_npi_obgyn_data(
#' #'   npi_input_directory_path = "/data/historical_npi",
#' #'   obgyn_output_file_path = "/analysis/maternal_fetal_medicine.parquet",
#' #'   obgyn_taxonomy_code_vector = c("207VM0101X"),
#' #'   npi_file_pattern_regex = "npi_20[12]\\d\\.(csv|parquet)$",
#' #'   schema_analysis_report_path = "/analysis/mfm_schema_report.csv",
#' #'   schema_compatibility_strategy = "strict",
#' #'   enable_verbose_logging = FALSE
#' #' )
#' #' # Output: Subset focused on maternal-fetal medicine specialists only
#' #' # Schema: Only columns present in ALL input files included
#' #' }
#' #'
#' #' # Example 3: Gynecologic oncology with intersection strategy
#' #' \dontrun{
#' #' gyn_oncology_providers <- process_npi_obgyn_data(
#' #'   npi_input_directory_path = "/volumes/npi_data/processed",
#' #'   obgyn_output_file_path = "/research/gyn_onc_analysis.feather",
#' #'   obgyn_taxonomy_code_vector = c("207VG0400X", "207V00000X"),
#' #'   npi_file_pattern_regex = "cleaned_npi_\\d{4}\\.parquet$",
#' #'   schema_analysis_report_path = "/research/gyn_onc_schema.csv",
#' #'   schema_compatibility_strategy = "intersect",
#' #'   enable_verbose_logging = TRUE
#' #' )
#' #' # Output: Gynecologic oncologists with core common columns only
#' #' # Format: Fast-loading Feather format for R analysis
#' #' }
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom fs dir_ls path_ext file_exists file_size path_dir
#' #' @importFrom stringr str_detect str_extract
#' #' @importFrom dplyr tibble count bind_rows
#' #' @importFrom purrr map
#' #' @importFrom glue glue
#' #' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' #' @importFrom duckdb duckdb
#' #' @importFrom readr write_csv
#' #' @importFrom arrow write_parquet write_feather
#' #'
#' #' @export
#' process_npi_obgyn_data <- function(
#'     npi_input_directory_path,
#'     obgyn_output_file_path,
#'     obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", "207VM0101X", 
#'                                    "207VH0002X", "207VE0102X"),
#'     npi_file_pattern_regex = "npi\\\\d{4,5}\\\\.(csv|parquet)$",
#'     schema_analysis_report_path = NULL,
#'     schema_compatibility_strategy = "union",
#'     enable_verbose_logging = TRUE
#' ) {
#'   
#'   # Configure logging system
#'   configure_function_logging(enable_verbose_logging)
#'   
#'   # Log function entry and parameters
#'   logger::log_info("=== STARTING NPI OB/GYN DATA PROCESSING ===")
#'   logger::log_info("Function: process_npi_obgyn_data")
#'   logger::log_info("Timestamp: {Sys.time()}")
#'   
#'   # Comprehensive input validation
#'   validate_function_inputs(npi_input_directory_path, obgyn_output_file_path,
#'                            obgyn_taxonomy_code_vector, npi_file_pattern_regex,
#'                            schema_analysis_report_path, 
#'                            schema_compatibility_strategy, enable_verbose_logging)
#'   
#'   # Log all input parameters for transparency
#'   log_function_input_parameters(npi_input_directory_path, obgyn_output_file_path,
#'                                 obgyn_taxonomy_code_vector, npi_file_pattern_regex,
#'                                 schema_analysis_report_path, 
#'                                 schema_compatibility_strategy, 
#'                                 enable_verbose_logging)
#'   
#'   # Set default schema analysis path if not provided
#'   if (is.null(schema_analysis_report_path)) {
#'     output_directory <- fs::path_dir(obgyn_output_file_path)
#'     schema_analysis_report_path <- file.path(output_directory, 
#'                                              "schema_analysis_report.csv")
#'   }
#'   
#'   # Discover NPI files in input directory
#'   discovered_npi_files <- discover_npi_files_in_directory(
#'     npi_input_directory_path, npi_file_pattern_regex)
#'   
#'   # Perform comprehensive schema analysis across all files
#'   schema_analysis_summary <- perform_comprehensive_schema_analysis(
#'     discovered_npi_files, schema_analysis_report_path)
#'   
#'   # Determine optimal column harmonization strategy
#'   column_harmonization_config <- determine_column_harmonization_strategy(
#'     schema_analysis_summary, schema_compatibility_strategy)
#'   
#'   # Initialize DuckDB connection for high-performance processing
#'   duckdb_connection <- establish_duckdb_connection()
#'   
#'   tryCatch({
#'     # Process and combine all NPI files with intelligent schema handling
#'     combined_obgyn_provider_dataset <- process_and_combine_npi_files_with_schema_intelligence(
#'       discovered_npi_files, obgyn_taxonomy_code_vector, 
#'       column_harmonization_config, duckdb_connection)
#'     
#'     # Write combined dataset with comprehensive validation
#'     write_combined_obgyn_dataset_with_validation(
#'       combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     
#'     # Generate completion summary with statistics
#'     log_processing_completion_summary(combined_obgyn_provider_dataset, 
#'                                       obgyn_output_file_path)
#'     
#'     logger::log_info("=== NPI OB/GYN DATA PROCESSING COMPLETED SUCCESSFULLY ===")
#'     
#'     return(invisible(combined_obgyn_provider_dataset))
#'     
#'   }, error = function(processing_error) {
#'     logger::log_error("Critical processing error: {processing_error$message}")
#'     stop(glue::glue("NPI processing failed: {processing_error$message}"))
#'   }, finally = {
#'     # Always clean up DuckDB connection
#'     DBI::dbDisconnect(duckdb_connection, shutdown = TRUE)
#'     logger::log_info("DuckDB connection closed")
#'   })
#' }
#' 
#' #' @noRd
#' configure_function_logging <- function(enable_verbose_logging) {
#'   if (enable_verbose_logging) {
#'     logger::log_threshold(logger::INFO)
#'   } else {
#'     logger::log_threshold(logger::WARN)
#'   }
#'   logger::log_info("Logging configuration completed")
#' }
#' 
#' #' @noRd
#' validate_function_inputs <- function(npi_input_directory_path, 
#'                                      obgyn_output_file_path,
#'                                      obgyn_taxonomy_code_vector, 
#'                                      npi_file_pattern_regex,
#'                                      schema_analysis_report_path, 
#'                                      schema_compatibility_strategy,
#'                                      enable_verbose_logging) {
#'   logger::log_info("=== VALIDATING FUNCTION INPUTS ===")
#'   
#'   # Validate input directory
#'   assertthat::assert_that(is.character(npi_input_directory_path),
#'                           msg = "npi_input_directory_path must be character")
#'   assertthat::assert_that(fs::file_exists(npi_input_directory_path),
#'                           msg = "Input directory does not exist")
#'   
#'   # Validate output file path
#'   assertthat::assert_that(is.character(obgyn_output_file_path),
#'                           msg = "obgyn_output_file_path must be character")
#'   output_extension <- fs::path_ext(obgyn_output_file_path)
#'   assertthat::assert_that(output_extension %in% c("csv", "parquet", "feather"),
#'                           msg = "Output format must be csv, parquet, or feather")
#'   
#'   # Validate taxonomy codes
#'   assertthat::assert_that(is.character(obgyn_taxonomy_code_vector),
#'                           msg = "obgyn_taxonomy_code_vector must be character")
#'   assertthat::assert_that(length(obgyn_taxonomy_code_vector) > 0,
#'                           msg = "Must provide at least one taxonomy code")
#'   
#'   # Validate file pattern
#'   assertthat::assert_that(is.character(npi_file_pattern_regex),
#'                           msg = "npi_file_pattern_regex must be character")
#'   
#'   # Validate schema strategy
#'   valid_strategies <- c("strict", "intersect", "union")
#'   assertthat::assert_that(schema_compatibility_strategy %in% valid_strategies,
#'                           msg = "schema_compatibility_strategy must be strict, intersect, or union")
#'   
#'   # Validate logging flag
#'   assertthat::assert_that(is.logical(enable_verbose_logging),
#'                           msg = "enable_verbose_logging must be logical")
#'   
#'   logger::log_info("All input validation checks passed successfully")
#' }
#' 
#' #' @noRd
#' log_function_input_parameters <- function(npi_input_directory_path, 
#'                                           obgyn_output_file_path,
#'                                           obgyn_taxonomy_code_vector, 
#'                                           npi_file_pattern_regex,
#'                                           schema_analysis_report_path, 
#'                                           schema_compatibility_strategy,
#'                                           enable_verbose_logging) {
#'   logger::log_info("=== FUNCTION INPUT PARAMETERS ===")
#'   logger::log_info("Input directory: {npi_input_directory_path}")
#'   logger::log_info("Output file path: {obgyn_output_file_path}")
#'   logger::log_info("OB/GYN taxonomy codes ({length(obgyn_taxonomy_code_vector)} total): {paste(obgyn_taxonomy_code_vector, collapse = ', ')}")
#'   logger::log_info("NPI file pattern: {npi_file_pattern_regex}")
#'   logger::log_info("Schema analysis report path: {schema_analysis_report_path}")
#'   logger::log_info("Schema compatibility strategy: {schema_compatibility_strategy}")
#'   logger::log_info("Verbose logging enabled: {enable_verbose_logging}")
#'   logger::log_info("=================================")
#' }
#' 
#' #' @noRd
#' discover_npi_files_in_directory <- function(npi_input_directory_path, 
#'                                             npi_file_pattern_regex) {
#'   logger::log_info("=== DISCOVERING NPI FILES ===")
#'   logger::log_info("Searching directory: {npi_input_directory_path}")
#'   logger::log_info("Using file pattern: {npi_file_pattern_regex}")
#'   
#'   # Get all files in directory
#'   all_directory_files <- fs::dir_ls(npi_input_directory_path)
#'   total_files_count <- length(all_directory_files)
#'   
#'   # Filter for NPI files matching pattern
#'   matching_npi_files <- all_directory_files[stringr::str_detect(
#'     basename(all_directory_files), npi_file_pattern_regex)]
#'   
#'   assertthat::assert_that(length(matching_npi_files) > 0,
#'                           msg = "No NPI files found matching the specified pattern")
#'   
#'   logger::log_info("Total files in directory: {total_files_count}")
#'   logger::log_info("NPI files matching pattern: {length(matching_npi_files)}")
#'   
#'   # Log discovered files with details
#'   logger::log_info("Discovered NPI files:")
#'   for (current_file_path in matching_npi_files) {
#'     current_filename <- basename(current_file_path)
#'     current_file_extension <- fs::path_ext(current_file_path)
#'     current_file_size <- fs::file_size(current_file_path)
#'     logger::log_info("  - {current_filename} ({current_file_extension}, {current_file_size} bytes)")
#'   }
#'   
#'   logger::log_info("File discovery completed successfully")
#'   return(matching_npi_files)
#' }
#' 
#' #' @noRd
#' establish_duckdb_connection <- function() {
#'   logger::log_info("Establishing DuckDB connection for high-performance processing")
#'   duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   logger::log_info("DuckDB connection established successfully")
#'   return(duckdb_connection)
#' }
#' 
#' #' @noRd
#' perform_comprehensive_schema_analysis <- function(discovered_npi_files, 
#'                                                   schema_analysis_report_path) {
#'   logger::log_info("=== COMPREHENSIVE SCHEMA ANALYSIS ===")
#'   logger::log_info("Analyzing {length(discovered_npi_files)} NPI files for column compatibility")
#'   
#'   # Initialize schema analysis tracking
#'   schema_analysis_results_list <- list()
#'   successful_analyses_count <- 0
#'   failed_analyses_count <- 0
#'   
#'   # Establish temporary DuckDB connection for schema analysis
#'   temp_duckdb_connection <- DBI::dbConnect(duckdb::duckdb())
#'   
#'   tryCatch({
#'     # Analyze each file's schema
#'     for (i in seq_along(discovered_npi_files)) {
#'       current_npi_file_path <- discovered_npi_files[i]
#'       current_filename <- basename(current_npi_file_path)
#'       extracted_year_info <- extract_year_from_filename(current_filename)
#'       
#'       logger::log_info("  Extracted year: {extracted_year_info}")
#'       logger::log_info("Analyzing schema: {current_filename}")
#'       
#'       # Attempt schema analysis with error handling
#'       tryCatch({
#'         schema_info <- analyze_single_file_schema(temp_duckdb_connection, 
#'                                                   current_npi_file_path)
#'         
#'         schema_analysis_results_list[[i]] <- list(
#'           filename = current_filename,
#'           file_path = current_npi_file_path,
#'           year = extracted_year_info,
#'           column_count = length(schema_info$column_name),
#'           column_names = schema_info$column_name,
#'           analysis_status = "success"
#'         )
#'         
#'         successful_analyses_count <- successful_analyses_count + 1
#'         logger::log_info("  Schema analysis successful: {length(schema_info$column_name)} columns detected")
#'         
#'       }, error = function(schema_error) {
#'         logger::log_error("  Schema analysis failed for {current_filename}: {schema_error$message}")
#'         schema_analysis_results_list[[i]] <- list(
#'           filename = current_filename,
#'           file_path = current_npi_file_path,
#'           year = extracted_year_info,
#'           column_count = 0,
#'           column_names = character(),
#'           analysis_status = "failed",
#'           error_message = schema_error$message
#'         )
#'         failed_analyses_count <- failed_analyses_count + 1
#'       })
#'     }
#'     
#'     logger::log_info("Schema analysis summary:")
#'     logger::log_info("  Successful: {successful_analyses_count} files")
#'     logger::log_info("  Failed: {failed_analyses_count} files")
#'     
#'     # Perform cross-file compatibility analysis
#'     compatibility_analysis <- analyze_cross_file_column_compatibility(
#'       schema_analysis_results_list)
#'     
#'     # Write comprehensive schema analysis report
#'     write_schema_analysis_report(schema_analysis_results_list, 
#'                                  compatibility_analysis, 
#'                                  schema_analysis_report_path)
#'     
#'     return(list(
#'       file_schemas = schema_analysis_results_list,
#'       compatibility = compatibility_analysis,
#'       successful_count = successful_analyses_count,
#'       failed_count = failed_analyses_count
#'     ))
#'     
#'   }, finally = {
#'     DBI::dbDisconnect(temp_duckdb_connection, shutdown = TRUE)
#'   })
#' }
#' 
#' #' @noRd
#' analyze_single_file_schema <- function(duckdb_connection, npi_file_path) {
#'   current_file_extension <- fs::path_ext(npi_file_path)
#'   
#'   if (current_file_extension == "csv") {
#'     return(analyze_csv_file_schema(duckdb_connection, npi_file_path))
#'   } else if (current_file_extension == "parquet") {
#'     return(analyze_parquet_file_schema(duckdb_connection, npi_file_path))
#'   } else {
#'     stop(glue::glue("Unsupported file format for schema analysis: {current_file_extension}"))
#'   }
#' }
#' 
#' #' @noRd
#' analyze_csv_file_schema <- function(duckdb_connection, npi_file_path) {
#'   logger::log_info("  Attempting CSV schema analysis with multiple strategies")
#'   
#'   # Strategy 1: Auto-detection
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv_auto('{npi_file_path}') LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with auto-detection strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("    Auto-detection failed, trying flexible parsing")
#'   })
#'   
#'   # Strategy 2: Flexible parsing
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with flexible parsing strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     logger::log_info("    Flexible parsing failed, trying manual configuration")
#'   })
#'   
#'   # Strategy 3: Manual configuration
#'   tryCatch({
#'     schema_query <- glue::glue("
#'       DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       ) LIMIT 0")
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     logger::log_info("    Success with manual configuration strategy")
#'     return(schema_info)
#'   }, error = function(e) {
#'     stop(glue::glue("All CSV schema analysis strategies failed: {e$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' analyze_parquet_file_schema <- function(duckdb_connection, npi_file_path) {
#'   schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{npi_file_path}') LIMIT 0")
#'   schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'   return(schema_info)
#' }
#' 
#' #' @noRd
#' extract_year_from_filename <- function(filename) {
#'   # Extract 4-digit year from filename (e.g., "npi2013.csv" -> "2013")
#'   year_match <- stringr::str_extract(filename, "20\\d{2}")
#'   if (is.na(year_match)) {
#'     # Try 2-digit year and convert to 4-digit
#'     short_year <- stringr::str_extract(filename, "\\d{2}")
#'     if (!is.na(short_year)) {
#'       year_match <- paste0("20", short_year)
#'     } else {
#'       year_match <- "unknown"
#'     }
#'   }
#'   return(year_match)
#' }
#' 
#' #' @noRd
#' analyze_cross_file_column_compatibility <- function(schema_analysis_results_list) {
#'   logger::log_info("=== CROSS-FILE COMPATIBILITY ANALYSIS ===")
#'   
#'   # Extract successful schema results only
#'   successful_schemas <- schema_analysis_results_list[
#'     purrr::map_lgl(schema_analysis_results_list, 
#'                    ~ .x$analysis_status == "success")]
#'   
#'   if (length(successful_schemas) == 0) {
#'     stop("No successful schema analyses available for compatibility analysis")
#'   }
#'   
#'   # Get all unique column names across all files
#'   all_unique_column_names <- unique(unlist(
#'     purrr::map(successful_schemas, ~ .x$column_names)))
#'   
#'   # Count presence of each column across files
#'   column_presence_matrix <- matrix(FALSE, 
#'                                    nrow = length(all_unique_column_names),
#'                                    ncol = length(successful_schemas))
#'   rownames(column_presence_matrix) <- all_unique_column_names
#'   colnames(column_presence_matrix) <- purrr::map_chr(successful_schemas, 
#'                                                      ~ .x$filename)
#'   
#'   for (i in seq_along(successful_schemas)) {
#'     current_schema <- successful_schemas[[i]]
#'     column_presence_matrix[current_schema$column_names, i] <- TRUE
#'   }
#'   
#'   # Calculate column compatibility statistics
#'   column_presence_counts <- rowSums(column_presence_matrix)
#'   total_files_analyzed <- length(successful_schemas)
#'   
#'   universal_columns <- names(column_presence_counts[
#'     column_presence_counts == total_files_analyzed])
#'   partial_presence_columns <- names(column_presence_counts[
#'     column_presence_counts > 0 & column_presence_counts < total_files_analyzed])
#'   file_specific_columns <- names(column_presence_counts[
#'     column_presence_counts == 1])
#'   
#'   logger::log_info("Analysis scope:")
#'   logger::log_info("  Total files analyzed: {total_files_analyzed}")
#'   logger::log_info("  Total unique columns: {length(all_unique_column_names)}")
#'   
#'   logger::log_info("Column compatibility results:")
#'   logger::log_info("  Universal columns (present in ALL files): {length(universal_columns)}")
#'   logger::log_info("  Partial presence columns (SOME files): {length(partial_presence_columns)}")
#'   logger::log_info("  File-specific columns (ONE file only): {length(file_specific_columns)}")
#'   
#'   # Log sample universal columns
#'   if (length(universal_columns) > 0) {
#'     sample_universal <- head(universal_columns, 10)
#'     logger::log_info("Universal columns (first 10):")
#'     for (col_name in sample_universal) {
#'       logger::log_info("    {col_name}")
#'     }
#'     if (length(universal_columns) > 10) {
#'       logger::log_info("    ... and {length(universal_columns) - 10} more universal columns")
#'     }
#'   }
#'   
#'   # Identify taxonomy columns
#'   taxonomy_column_analysis <- identify_taxonomy_columns_comprehensive(
#'     all_unique_column_names, column_presence_matrix)
#'   
#'   return(list(
#'     total_unique_columns = length(all_unique_column_names),
#'     universal_columns = universal_columns,
#'     partial_presence_columns = partial_presence_columns,
#'     file_specific_columns = file_specific_columns,
#'     column_presence_matrix = column_presence_matrix,
#'     taxonomy_analysis = taxonomy_column_analysis
#'   ))
#' }
#' 
#' #' @noRd
#' identify_taxonomy_columns_comprehensive <- function(all_unique_column_names, 
#'                                                     column_presence_matrix) {
#'   logger::log_info("=== TAXONOMY COLUMN ANALYSIS ===")
#'   
#'   # Define taxonomy column patterns for NBER and NPPES formats
#'   nber_taxonomy_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$")
#'   nppes_taxonomy_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", 
#'                                "(?i)taxonomy.*code")
#'   
#'   # Identify NBER-style taxonomy columns
#'   nber_taxonomy_columns <- character()
#'   for (pattern in nber_taxonomy_patterns) {
#'     matching_columns <- all_unique_column_names[
#'       stringr::str_detect(all_unique_column_names, pattern)]
#'     nber_taxonomy_columns <- c(nber_taxonomy_columns, matching_columns)
#'   }
#'   
#'   # Identify NPPES-style taxonomy columns  
#'   nppes_taxonomy_columns <- character()
#'   for (pattern in nppes_taxonomy_patterns) {
#'     matching_columns <- all_unique_column_names[
#'       stringr::str_detect(all_unique_column_names, pattern)]
#'     nppes_taxonomy_columns <- c(nppes_taxonomy_columns, matching_columns)
#'   }
#'   
#'   # Remove duplicates and combine
#'   discovered_taxonomy_columns <- unique(c(nber_taxonomy_columns, 
#'                                           nppes_taxonomy_columns))
#'   
#'   logger::log_info("Taxonomy columns discovered:")
#'   logger::log_info("  NBER format columns: {length(nber_taxonomy_columns)}")
#'   logger::log_info("  NPPES format columns: {length(nppes_taxonomy_columns)}")
#'   logger::log_info("  Total taxonomy columns: {length(discovered_taxonomy_columns)}")
#'   
#'   # Analyze taxonomy column presence across files
#'   if (length(discovered_taxonomy_columns) > 0) {
#'     taxonomy_presence_counts <- rowSums(
#'       column_presence_matrix[discovered_taxonomy_columns, , drop = FALSE])
#'     total_files <- ncol(column_presence_matrix)
#'     
#'     # Log taxonomy column presence (first 15 for brevity)
#'     sample_taxonomy_columns <- head(discovered_taxonomy_columns, 15)
#'     logger::log_info("Taxonomy column presence analysis (first 15):")
#'     for (tax_col in sample_taxonomy_columns) {
#'       presence_count <- taxonomy_presence_counts[tax_col]
#'       presence_percentage <- round(100 * presence_count / total_files)
#'       logger::log_info("    {tax_col}: {presence_count}/{total_files} files ({presence_percentage}%)")
#'     }
#'     
#'     if (length(discovered_taxonomy_columns) > 15) {
#'       logger::log_info("    ... and {length(discovered_taxonomy_columns) - 15} more taxonomy columns")
#'     }
#'   }
#'   
#'   return(list(
#'     nber_columns = nber_taxonomy_columns,
#'     nppes_columns = nppes_taxonomy_columns,
#'     all_taxonomy_columns = discovered_taxonomy_columns
#'   ))
#' }
#' 
#' #' @noRd
#' write_schema_analysis_report <- function(schema_analysis_results_list, 
#'                                          compatibility_analysis, 
#'                                          schema_analysis_report_path) {
#'   logger::log_info("=== WRITING SCHEMA ANALYSIS REPORT ===")
#'   logger::log_info("Report destination: {schema_analysis_report_path}")
#'   
#'   # Create comprehensive report dataframe
#'   report_data_list <- list()
#'   
#'   # Add file-level information
#'   for (i in seq_along(schema_analysis_results_list)) {
#'     current_schema <- schema_analysis_results_list[[i]]
#'     
#'     if (current_schema$analysis_status == "success") {
#'       for (col_name in current_schema$column_names) {
#'         report_data_list[[length(report_data_list) + 1]] <- list(
#'           filename = current_schema$filename,
#'           year = current_schema$year,
#'           column_name = col_name,
#'           analysis_status = current_schema$analysis_status,
#'           is_taxonomy_column = col_name %in% 
#'             compatibility_analysis$taxonomy_analysis$all_taxonomy_columns,
#'           is_universal_column = col_name %in% 
#'             compatibility_analysis$universal_columns,
#'           presence_across_files = sum(
#'             compatibility_analysis$column_presence_matrix[col_name, ])
#'         )
#'       }
#'     } else {
#'       # Record failed analysis
#'       report_data_list[[length(report_data_list) + 1]] <- list(
#'         filename = current_schema$filename,
#'         year = current_schema$year,
#'         column_name = NA,
#'         analysis_status = current_schema$analysis_status,
#'         is_taxonomy_column = FALSE,
#'         is_universal_column = FALSE,
#'         presence_across_files = 0
#'       )
#'     }
#'   }
#'   
#'   # Convert to dataframe and write report
#'   schema_report_dataframe <- dplyr::bind_rows(report_data_list)
#'   readr::write_csv(schema_report_dataframe, schema_analysis_report_path)
#'   
#'   logger::log_info("Schema analysis report written successfully")
#'   logger::log_info("  Total columns analyzed: {compatibility_analysis$total_unique_columns}")
#'   logger::log_info("  Total files analyzed: {length(schema_analysis_results_list)}")
#'   logger::log_info("  Report saved to: {schema_analysis_report_path}")
#' }
#' 
#' #' @noRd
#' determine_column_harmonization_strategy <- function(schema_analysis_summary, 
#'                                                     schema_compatibility_strategy) {
#'   logger::log_info("=== DETERMINING COLUMN HARMONIZATION STRATEGY ===")
#'   logger::log_info("Selected strategy: {schema_compatibility_strategy}")
#'   
#'   compatibility_info <- schema_analysis_summary$compatibility
#'   
#'   if (schema_compatibility_strategy == "strict") {
#'     selected_columns <- compatibility_info$universal_columns
#'     logger::log_info("STRICT strategy: Using {length(selected_columns)} universal columns only")
#'     
#'   } else if (schema_compatibility_strategy == "intersect") {
#'     selected_columns <- compatibility_info$universal_columns
#'     logger::log_info("INTERSECT strategy: Using {length(selected_columns)} common columns")
#'     
#'   } else if (schema_compatibility_strategy == "union") {
#'     all_columns <- c(compatibility_info$universal_columns,
#'                      compatibility_info$partial_presence_columns)
#'     selected_columns <- all_columns
#'     logger::log_info("UNION strategy: Using {length(selected_columns)} total unique columns")
#'     logger::log_warn("UNION mode will create NA values for columns missing in some files")
#'     
#'   } else {
#'     stop(glue::glue("Invalid schema compatibility strategy: {schema_compatibility_strategy}"))
#'   }
#'   
#'   return(list(
#'     strategy_mode = schema_compatibility_strategy,
#'     selected_columns = selected_columns,
#'     column_count = length(selected_columns)
#'   ))
#' }
#' 
#' #' @noRd
#' process_and_combine_npi_files_with_schema_intelligence <- function(
#'     discovered_npi_files, obgyn_taxonomy_code_vector, 
#'     column_harmonization_config, duckdb_connection) {
#'   
#'   logger::log_info("=== PROCESSING AND COMBINING NPI FILES ===")
#'   logger::log_info("Strategy: {column_harmonization_config$strategy_mode}")
#'   logger::log_info("Files to process: {length(discovered_npi_files)}")
#'   
#'   # Prepare taxonomy code filter for SQL queries
#'   taxonomy_code_filter_sql <- paste0("'", obgyn_taxonomy_code_vector, "'", 
#'                                      collapse = ", ")
#'   
#'   # Process each NPI file individually
#'   processed_datasets_list <- list()
#'   successful_processing_count <- 0
#'   failed_processing_count <- 0
#'   
#'   for (i in seq_along(discovered_npi_files)) {
#'     current_npi_file_path <- discovered_npi_files[i]
#'     current_filename <- basename(current_npi_file_path)
#'     
#'     logger::log_info("Processing file {i}/{length(discovered_npi_files)}: {current_filename}")
#'     
#'     tryCatch({
#'       # Process single file with intelligent error handling
#'       processed_provider_dataset <- process_single_npi_file_with_intelligent_schema_handling(
#'         current_npi_file_path, taxonomy_code_filter_sql, 
#'         column_harmonization_config, duckdb_connection)
#'       
#'       if (nrow(processed_provider_dataset) > 0) {
#'         processed_datasets_list[[length(processed_datasets_list) + 1]] <- 
#'           processed_provider_dataset
#'         successful_processing_count <- successful_processing_count + 1
#'         logger::log_info("  Successfully processed: {nrow(processed_provider_dataset)} OB/GYN providers found")
#'       } else {
#'         logger::log_warn("  No OB/GYN providers found in {current_filename}")
#'       }
#'       
#'     }, error = function(processing_error) {
#'       logger::log_error("  Failed to process {current_filename}: {processing_error$message}")
#'       failed_processing_count <- failed_processing_count + 1
#'     })
#'   }
#'   
#'   logger::log_info("File processing summary:")
#'   logger::log_info("  Successfully processed: {successful_processing_count} files")
#'   logger::log_info("  Processing failures: {failed_processing_count} files")
#'   logger::log_info("  Datasets with OB/GYN providers: {length(processed_datasets_list)}")
#'   
#'   # Ensure we have data to combine
#'   assertthat::assert_that(length(processed_datasets_list) > 0,
#'                           msg = "No datasets with OB/GYN providers found")
#'   
#'   # Combine datasets with intelligent schema alignment
#'   combined_harmonized_dataset <- combine_datasets_with_intelligent_schema_alignment(
#'     processed_datasets_list, column_harmonization_config)
#'   
#'   return(combined_harmonized_dataset)
#' }
#' 
#' #' @noRd
#' process_single_npi_file_with_intelligent_schema_handling <- function(
#'     npi_file_path, taxonomy_code_filter_sql, column_harmonization_config, 
#'     duckdb_connection) {
#'   
#'   current_filename <- basename(npi_file_path)
#'   current_file_extension <- fs::path_ext(npi_file_path)
#'   extracted_year_info <- extract_year_from_filename(current_filename)
#'   
#'   logger::log_info("  Extracted year: {extracted_year_info}")
#'   logger::log_info("  Reading file with intelligent schema handling: {current_filename}")
#'   
#'   # Determine which columns to select based on harmonization strategy
#'   if (column_harmonization_config$strategy_mode == "union") {
#'     selected_columns_sql <- "*"
#'   } else {
#'     # For strict/intersect modes, only select the predetermined columns
#'     selected_columns_sql <- paste(column_harmonization_config$selected_columns, 
#'                                   collapse = ", ")
#'   }
#'   
#'   logger::log_info("  Executing DuckDB query to filter OB/GYN providers")
#'   
#'   # Process file with intelligent error handling
#'   tryCatch({
#'     if (current_file_extension == "csv") {
#'       filtered_provider_dataset <- process_csv_file_with_taxonomy_detection(
#'         duckdb_connection, npi_file_path, selected_columns_sql, 
#'         taxonomy_code_filter_sql, extracted_year_info, current_filename)
#'     } else if (current_file_extension == "parquet") {
#'       filtered_provider_dataset <- process_parquet_file_with_taxonomy_detection(
#'         duckdb_connection, npi_file_path, selected_columns_sql,
#'         taxonomy_code_filter_sql, extracted_year_info, current_filename)
#'     } else {
#'       stop(glue::glue("Unsupported file format: {current_file_extension}"))
#'     }
#'     
#'     logger::log_info("  Query execution completed: {nrow(filtered_provider_dataset)} matching providers")
#'     return(filtered_provider_dataset)
#'     
#'   }, error = function(processing_error) {
#'     logger::log_error("  Failed to process {current_filename}: {processing_error$message}")
#'     logger::log_warn("  Returning empty dataset for this file")
#'     return(dplyr::tibble())
#'   })
#' }
#' 
#' #' @noRd
#' process_csv_file_with_taxonomy_detection <- function(duckdb_connection, 
#'                                                      npi_file_path, 
#'                                                      selected_columns_sql,
#'                                                      taxonomy_code_filter_sql, 
#'                                                      extracted_year_info, 
#'                                                      current_filename) {
#'   logger::log_info("    Processing CSV file with taxonomy detection")
#'   
#'   # Get available taxonomy columns for this specific file
#'   available_taxonomy_columns <- discover_taxonomy_columns_in_file(
#'     duckdb_connection, npi_file_path)
#'   
#'   if (length(available_taxonomy_columns) == 0) {
#'     logger::log_warn("    No taxonomy columns found - returning empty dataset")
#'     return(dplyr::tibble())
#'   }
#'   
#'   # Build dynamic WHERE clause based on discovered columns
#'   taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(
#'     available_taxonomy_columns, taxonomy_code_filter_sql)
#'   
#'   # Execute CSV query with multiple fallback strategies
#'   return(execute_csv_query_with_fallback(duckdb_connection, npi_file_path, 
#'                                          selected_columns_sql, taxonomy_where_clause,
#'                                          extracted_year_info, current_filename))
#' }
#' 
#' #' @noRd
#' process_parquet_file_with_taxonomy_detection <- function(duckdb_connection, 
#'                                                          npi_file_path,
#'                                                          selected_columns_sql,
#'                                                          taxonomy_code_filter_sql,
#'                                                          extracted_year_info,
#'                                                          current_filename) {
#'   logger::log_info("    Processing Parquet file with taxonomy detection")
#'   
#'   # Get available taxonomy columns for this specific file
#'   available_taxonomy_columns <- discover_taxonomy_columns_in_file(
#'     duckdb_connection, npi_file_path)
#'   
#'   if (length(available_taxonomy_columns) == 0) {
#'     logger::log_warn("    No taxonomy columns found - returning empty dataset")
#'     return(dplyr::tibble())
#'   }
#'   
#'   # Build dynamic WHERE clause
#'   taxonomy_where_clause <- build_dynamic_taxonomy_where_clause(
#'     available_taxonomy_columns, taxonomy_code_filter_sql)
#'   
#'   # Execute Parquet query
#'   sql_query <- glue::glue("
#'     SELECT {selected_columns_sql},
#'            '{extracted_year_info}' as data_year,
#'            '{current_filename}' as source_filename
#'     FROM read_parquet('{npi_file_path}')
#'     WHERE {taxonomy_where_clause}
#'   ")
#'   
#'   filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'   return(filtered_dataset)
#' }
#' 
#' #' @noRd
#' discover_taxonomy_columns_in_file <- function(duckdb_connection, npi_file_path) {
#'   current_file_extension <- fs::path_ext(npi_file_path)
#'   
#'   # Get schema information for the file
#'   tryCatch({
#'     if (current_file_extension == "csv") {
#'       schema_query <- glue::glue("
#'         DESCRIBE SELECT * FROM read_csv('{npi_file_path}', 
#'           auto_detect=true, 
#'           ignore_errors=true, 
#'           null_padding=true,
#'           max_line_size=10000000
#'         ) LIMIT 0")
#'     } else {
#'       schema_query <- glue::glue("DESCRIBE SELECT * FROM read_parquet('{npi_file_path}') LIMIT 0")
#'     }
#'     
#'     schema_info <- DBI::dbGetQuery(duckdb_connection, schema_query)
#'     available_columns <- schema_info$column_name
#'     
#'     # Look for NBER-style taxonomy columns first (ptaxcode1, ptaxcode2, etc.)
#'     nber_patterns <- c("^ptaxcode\\d+$", "^pprimtax\\d+$")
#'     discovered_taxonomy_columns <- character()
#'     
#'     for (pattern in nber_patterns) {
#'       matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
#'       discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
#'     }
#'     
#'     # If no NBER columns, look for NPPES-style columns
#'     if (length(discovered_taxonomy_columns) == 0) {
#'       nppes_patterns <- c("(?i)healthcare.*provider.*taxonomy.*code", "(?i)taxonomy.*code")
#'       for (pattern in nppes_patterns) {
#'         matching_columns <- available_columns[stringr::str_detect(available_columns, pattern)]
#'         discovered_taxonomy_columns <- c(discovered_taxonomy_columns, matching_columns)
#'       }
#'     }
#'     
#'     discovered_taxonomy_columns <- unique(discovered_taxonomy_columns)
#'     
#'     if (length(discovered_taxonomy_columns) > 0) {
#'       logger::log_info("    Found {length(discovered_taxonomy_columns)} taxonomy columns")
#'     } else {
#'       logger::log_warn("    No taxonomy columns detected in file")
#'     }
#'     
#'     return(discovered_taxonomy_columns)
#'     
#'   }, error = function(discovery_error) {
#'     logger::log_error("    Failed to discover taxonomy columns: {discovery_error$message}")
#'     return(character())
#'   })
#' }
#' 
#' #' @noRd
#' build_dynamic_taxonomy_where_clause <- function(available_taxonomy_columns, 
#'                                                 taxonomy_code_filter_sql) {
#'   if (length(available_taxonomy_columns) == 0) {
#'     return("1=0")  # Return FALSE condition
#'   }
#'   
#'   # Build OR conditions for each available taxonomy column
#'   taxonomy_conditions <- character()
#'   for (taxonomy_col in available_taxonomy_columns) {
#'     condition <- glue::glue("{taxonomy_col} IN ({taxonomy_code_filter_sql})")
#'     taxonomy_conditions <- c(taxonomy_conditions, condition)
#'   }
#'   
#'   dynamic_where_clause <- paste(taxonomy_conditions, collapse = " OR ")
#'   
#'   # Log the WHERE clause (truncated for readability)
#'   clause_preview <- if (nchar(dynamic_where_clause) > 100) {
#'     paste0(substr(dynamic_where_clause, 1, 100), "...")
#'   } else {
#'     dynamic_where_clause
#'   }
#'   logger::log_info("    Built taxonomy WHERE clause: {clause_preview}")
#'   
#'   return(dynamic_where_clause)
#' }
#' 
#' #' @noRd
#' execute_csv_query_with_fallback <- function(duckdb_connection, npi_file_path,
#'                                             selected_columns_sql, taxonomy_where_clause,
#'                                             extracted_year_info, current_filename) {
#'   logger::log_info("    Executing CSV query with fallback strategies")
#'   
#'   # Strategy 1: Auto-detection
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv_auto('{npi_file_path}')
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with auto-detection strategy")
#'     return(filtered_dataset)
#'   }, error = function(auto_error) {
#'     logger::log_info("      Auto-detection failed, trying flexible parsing")
#'   })
#'   
#'   # Strategy 2: Flexible parsing
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{npi_file_path}', 
#'         auto_detect=true, 
#'         ignore_errors=true, 
#'         null_padding=true,
#'         max_line_size=10000000
#'       )
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with flexible parsing strategy")
#'     return(filtered_dataset)
#'   }, error = function(flexible_error) {
#'     logger::log_info("      Flexible parsing failed, trying manual configuration")
#'   })
#'   
#'   # Strategy 3: Manual configuration
#'   tryCatch({
#'     sql_query <- glue::glue("
#'       SELECT {selected_columns_sql},
#'              '{extracted_year_info}' as data_year,
#'              '{current_filename}' as source_filename
#'       FROM read_csv('{npi_file_path}', 
#'         delim=',', 
#'         quote='\"', 
#'         escape='\"',
#'         header=true,
#'         ignore_errors=true,
#'         null_padding=true
#'       )
#'       WHERE {taxonomy_where_clause}
#'     ")
#'     filtered_dataset <- DBI::dbGetQuery(duckdb_connection, sql_query)
#'     logger::log_info("      Success with manual configuration strategy")
#'     return(filtered_dataset)
#'   }, error = function(manual_error) {
#'     logger::log_error("      All CSV query strategies failed")
#'     stop(glue::glue("Could not execute CSV query with any strategy: {manual_error$message}"))
#'   })
#' }
#' 
#' #' @noRd
#' combine_datasets_with_intelligent_schema_alignment <- function(
#'     processed_datasets_list, column_harmonization_strategy) {
#'   
#'   logger::log_info("=== COMBINING DATASETS WITH INTELLIGENT SCHEMA ALIGNMENT ===")
#'   logger::log_info("Combination strategy: {column_harmonization_strategy$strategy_mode}")
#'   logger::log_info("Datasets to combine: {length(processed_datasets_list)}")
#'   
#'   if (column_harmonization_strategy$strategy_mode == "union") {
#'     # For union mode, harmonize data types to prevent conflicts
#'     logger::log_info("Harmonizing data types for union mode")
#'     
#'     # Get all unique column names across datasets
#'     all_unique_column_names <- unique(unlist(purrr::map(processed_datasets_list, names)))
#'     
#'     # Harmonize each dataset to consistent types
#'     harmonized_datasets_list <- list()
#'     for (i in seq_along(processed_datasets_list)) {
#'       harmonized_datasets_list[[i]] <- harmonize_dataset_data_types_comprehensive(
#'         processed_datasets_list[[i]], all_unique_column_names, i)
#'     }
#'     
#'     # Combine with bind_rows (handles missing columns automatically)
#'     combined_harmonized_dataset <- dplyr::bind_rows(harmonized_datasets_list)
#'     logger::log_info("Union combination completed - missing columns filled with NA")
#'     
#'   } else {
#'     # For strict/intersect modes, datasets should have consistent columns
#'     # but still harmonize data types to prevent conflicts
#'     logger::log_info("Harmonizing data types for consistent combination")
#'     
#'     harmonized_datasets_list <- list()
#'     for (i in seq_along(processed_datasets_list)) {
#'       harmonized_datasets_list[[i]] <- harmonize_dataset_data_types_for_consistent_combination(
#'         processed_datasets_list[[i]], i)
#'     }
#'     
#'     combined_harmonized_dataset <- dplyr::bind_rows(harmonized_datasets_list)
#'     logger::log_info("Standard combination completed using common column structure")
#'   }
#'   
#'   # Log final dataset statistics
#'   final_rows <- nrow(combined_harmonized_dataset)
#'   final_columns <- ncol(combined_harmonized_dataset)
#'   
#'   logger::log_info("Final combined dataset:")
#'   logger::log_info("  Total rows: {final_rows}")
#'   logger::log_info("  Total columns: {final_columns}")
#'   logger::log_info("  Schema alignment completed successfully")
#'   
#'   return(combined_harmonized_dataset)
#' }
#' 
#' #' @noRd
#' harmonize_dataset_data_types_comprehensive <- function(single_dataset, 
#'                                                        all_unique_column_names, 
#'                                                        dataset_index) {
#'   logger::log_info("  Harmonizing data types for dataset {dataset_index}")
#'   
#'   # Define columns that commonly have type conflicts across years/formats
#'   type_problematic_columns <- c(
#'     # Phone and fax numbers (can be numeric or character)
#'     "pmailfax", "plocfax", "aofax", "aotelnum", "ploctelnum", "pmailtel", "plnamecode", 
#'     
#'     # Provider identifiers (various formats)
#'     paste0("othpid", 1:50),
#'     paste0("othpidiss", 1:50), 
#'     paste0("othpidst", 1:50),
#'     paste0("othpidty", 1:50),
#'     
#'     # License numbers (can be numeric or alphanumeric)
#'     paste0("plicnum", 1:15),
#'     paste0("plicstate", 1:15),
#'     
#'     # Core identifiers
#'     "npi", "ein", "replacement_npi",
#'     
#'     # Date fields (can have inconsistent formats)
#'     "lastupdate", "npideactdate", "npireactdate", "penumdate",
#'     "certificationdate",
#'     
#'     # Entity type (the problematic column from your error)
#'     "entity"
#'   )
#'   
#'   # Convert problematic columns to character to avoid type conflicts
#'   for (col_name in type_problematic_columns) {
#'     if (col_name %in% names(single_dataset)) {
#'       if (!is.character(single_dataset[[col_name]])) {
#'         single_dataset[[col_name]] <- as.character(single_dataset[[col_name]])
#'         logger::log_info("    Converted {col_name} to character for type consistency")
#'       }
#'     }
#'   }
#'   
#'   return(single_dataset)
#' }
#' 
#' #' @noRd
#' harmonize_dataset_data_types_for_consistent_combination <- function(single_dataset, 
#'                                                                     dataset_index) {
#'   logger::log_info("  Harmonizing data types for dataset {dataset_index}")
#'   
#'   # Apply the same harmonization as comprehensive but for consistent schema
#'   type_problematic_columns <- c(
#'     "pmailfax", "plocfax", "aofax", "aotelnum", "ploctelnum", "pmailtel",
#'     paste0("othpid", 1:50), paste0("othpidty", 1:50),
#'     paste0("plicnum", 1:15), "npi", "ein", "replacement_npi",
#'     "lastupdate", "npideactdate", "npireactdate", "penumdate",
#'     "certificationdate", "entity"
#'   )
#'   
#'   for (col_name in type_problematic_columns) {
#'     if (col_name %in% names(single_dataset)) {
#'       if (!is.character(single_dataset[[col_name]])) {
#'         single_dataset[[col_name]] <- as.character(single_dataset[[col_name]])
#'         logger::log_info("    Converted {col_name} to character for type consistency")
#'       }
#'     }
#'   }
#'   
#'   return(single_dataset)
#' }
#' 
#' #' @noRd
#' write_combined_obgyn_dataset_with_validation <- function(
#'     combined_obgyn_provider_dataset, obgyn_output_file_path) {
#'   
#'   logger::log_info("=== WRITING COMBINED DATASET ===")
#'   logger::log_info("Output destination: {obgyn_output_file_path}")
#'   
#'   # Log dataset statistics before writing
#'   total_records <- nrow(combined_obgyn_provider_dataset)
#'   total_columns <- ncol(combined_obgyn_provider_dataset)
#'   
#'   logger::log_info("Dataset to write:")
#'   logger::log_info("  Total records: {total_records}")
#'   logger::log_info("  Total columns: {total_columns}")
#'   
#'   # Validate dataset is not empty
#'   assertthat::assert_that(total_records > 0,
#'                           msg = "Cannot write empty dataset - no OB/GYN providers found")
#'   assertthat::assert_that(total_columns > 0,
#'                           msg = "Cannot write dataset with no columns")
#'   
#'   # Determine output format and write accordingly
#'   output_file_extension <- fs::path_ext(obgyn_output_file_path)
#'   
#'   if (output_file_extension == "csv") {
#'     readr::write_csv(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote CSV file")
#'   } else if (output_file_extension == "parquet") {
#'     arrow::write_parquet(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote Parquet file")
#'   } else if (output_file_extension == "feather") {
#'     arrow::write_feather(combined_obgyn_provider_dataset, obgyn_output_file_path)
#'     logger::log_info("Successfully wrote Feather file")
#'   } else {
#'     stop(glue::glue("Unsupported output format: {output_file_extension}"))
#'   }
#'   
#'   # Verify file was created and log file information
#'   verify_output_file_creation(obgyn_output_file_path)
#' }
#' 
#' #' @noRd
#' verify_output_file_creation <- function(obgyn_output_file_path) {
#'   assertthat::assert_that(fs::file_exists(obgyn_output_file_path),
#'                           msg = "Output file was not created successfully")
#'   
#'   output_file_size <- fs::file_size(obgyn_output_file_path)
#'   logger::log_info("Output file verification:")
#'   logger::log_info("  File exists: TRUE")
#'   logger::log_info("  File size: {output_file_size} bytes")
#'   logger::log_info("  File path: {obgyn_output_file_path}")
#' }
#' 
#' #' @noRd
#' log_processing_completion_summary <- function(combined_obgyn_provider_dataset, 
#'                                               obgyn_output_file_path) {
#'   logger::log_info("=== PROCESSING COMPLETION SUMMARY ===")
#'   
#'   # Calculate and log summary statistics
#'   total_obgyn_providers <- nrow(combined_obgyn_provider_dataset)
#'   total_data_columns <- ncol(combined_obgyn_provider_dataset)
#'   
#'   # Calculate year distribution if data_year column exists
#'   if ("data_year" %in% names(combined_obgyn_provider_dataset)) {
#'     year_distribution <- combined_obgyn_provider_dataset |>
#'       dplyr::count(data_year, sort = TRUE)
#'     
#'     logger::log_info("OB/GYN providers by year:")
#'     for (i in seq_len(nrow(year_distribution))) {
#'       year_info <- year_distribution[i, ]
#'       logger::log_info("  {year_info$data_year}: {year_info$n} providers")
#'     }
#'   }
#'   
#'   # Calculate file source distribution if source_filename column exists
#'   if ("source_filename" %in% names(combined_obgyn_provider_dataset)) {
#'     source_distribution <- combined_obgyn_provider_dataset |>
#'       dplyr::count(source_filename, sort = TRUE)
#'     
#'     logger::log_info("OB/GYN providers by source file:")
#'     for (i in seq_len(nrow(source_distribution))) {
#'       source_info <- source_distribution[i, ]
#'       logger::log_info("  {source_info$source_filename}: {source_info$n} providers")
#'     }
#'   }
#'   
#'   logger::log_info("Final processing statistics:")
#'   logger::log_info("  Total OB/GYN providers: {total_obgyn_providers}")
#'   logger::log_info("  Total data columns: {total_data_columns}")
#'   logger::log_info("  Output file: {obgyn_output_file_path}")
#'   logger::log_info("====================================")
#' }
#' 
#' # Execute ----
#' # ==============================================================================
#' # EXECUTION: NPI OB/GYN DATA PROCESSING
#' # ==============================================================================
#' 
#' # Execute the robust NPI OB/GYN data processing function
#' combined_obgyn_provider_dataset <- process_npi_obgyn_data(
#'   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
#'   obgyn_taxonomy_code_vector = c("207V00000X",    # Obstetrics & Gynecology
#'                                  "207VG0400X",    # Gynecologic Oncology  
#'                                  "207VM0101X",    # Maternal & Fetal Medicine
#'                                  "207VH0002X",    # Hospice and Palliative Medicine
#'                                  "207VE0102X"),   # Reproductive Endocrinology
#'   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#'   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis_report.csv",
#'   schema_compatibility_strategy = "union",
#'   enable_verbose_logging = TRUE
#' )
#' 
#' # ==============================================================================
#' # OPTIONAL: ADDITIONAL ANALYSIS AND VALIDATION
#' # ==============================================================================
#' 
#' # Display summary statistics of the combined dataset
#' if (exists("combined_obgyn_provider_dataset") && !is.null(combined_obgyn_provider_dataset)) {
#'   
#'   cat("\n=== EXECUTION COMPLETION SUMMARY ===\n")
#'   cat("Total OB/GYN providers processed:", nrow(combined_obgyn_provider_dataset), "\n")
#'   cat("Total columns in final dataset:", ncol(combined_obgyn_provider_dataset), "\n")
#'   
#'   # Show year distribution if available
#'   if ("data_year" %in% names(combined_obgyn_provider_dataset)) {
#'     cat("\nProviders by year:\n")
#'     year_summary <- combined_obgyn_provider_dataset |> 
#'       dplyr::count(data_year, sort = TRUE)
#'     print(year_summary)
#'   }
#'   
#'   # Show file source distribution if available  
#'   if ("source_filename" %in% names(combined_obgyn_provider_dataset)) {
#'     cat("\nProviders by source file:\n")
#'     source_summary <- combined_obgyn_provider_dataset |>
#'       dplyr::count(source_filename, sort = TRUE)
#'     print(source_summary)
#'   }
#'   
#'   # Show sample of the data structure
#'   cat("\nSample of combined dataset structure:\n")
#'   print(str(combined_obgyn_provider_dataset))
#'   
#'   cat("\nFirst few rows of key columns:\n")
#'   key_columns <- c("npi", "entity", "data_year", "source_filename")
#'   available_key_columns <- key_columns[key_columns %in% names(combined_obgyn_provider_dataset)]
#'   
#'   if (length(available_key_columns) > 0) {
#'     print(head(combined_obgyn_provider_dataset[, available_key_columns]))
#'   }
#'   
#'   cat("\n=== EXECUTION COMPLETED SUCCESSFULLY ===\n")
#'   
#' } else {
#'   cat("\nWARNING: Function execution may have failed - no dataset returned\n")
#' }
#' 
#' # ==============================================================================
#' # ALTERNATIVE EXECUTION EXAMPLES
#' # ==============================================================================
#' 
#' # Example 1: Process with strict schema compatibility (only common columns)
#' # combined_obgyn_strict <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/obgyn_strict_schema.csv",
#' #   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", "207VM0101X", "207VH0002X", "207VE0102X"),
#' #   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis_strict.csv",
#' #   schema_compatibility_strategy = "strict",
#' #   enable_verbose_logging = TRUE
#' # )
#' 
#' # Example 2: Focus on specific OB/GYN specialties only
#' # maternal_fetal_specialists <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/maternal_fetal_medicine.parquet",
#' #   obgyn_taxonomy_code_vector = c("207VM0101X"),  # Maternal & Fetal Medicine only
#' #   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/mfm_schema_analysis.csv",
#' #   schema_compatibility_strategy = "union",
#' #   enable_verbose_logging = FALSE
#' # )
#' 
#' # Example 3: Process only recent years (modify file pattern)
#' # recent_obgyn_providers <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/recent_obgyn_providers.feather",
#' #   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", "207VM0101X", "207VH0002X", "207VE0102X"),
#' #   npi_file_pattern_regex = "npi20[2-9]\\d\\.(csv|parquet)$",  # 2020s only
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/recent_schema_analysis.csv",
#' #   schema_compatibility_strategy = "intersect",
#' #   enable_verbose_logging = TRUE
#' # )
#' 
#' # Execute ----
#' # ==============================================================================
#' # EXECUTION: NPI OB/GYN DATA PROCESSING (UPDATED WITH plnamecode FIX)
#' # ==============================================================================
#' 
#' # Execute the robust NPI OB/GYN data processing function
#' combined_obgyn_provider_dataset <- process_npi_obgyn_data(
#'   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv",
#'   obgyn_taxonomy_code_vector = c("207V00000X",    # Obstetrics & Gynecology
#'                                  "207VG0400X",    # Gynecologic Oncology  
#'                                  "207VM0101X",    # Maternal & Fetal Medicine
#'                                  "207VH0002X",    # Hospice and Palliative Medicine
#'                                  "207VE0102X"),   # Reproductive Endocrinology
#'   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#'   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis_report.csv",
#'   schema_compatibility_strategy = "union",
#'   enable_verbose_logging = TRUE
#' )
#' 
#' # ==============================================================================
#' # OPTIONAL: ADDITIONAL ANALYSIS AND VALIDATION
#' # ==============================================================================
#' 
#' # Display summary statistics of the combined dataset
#' if (exists("combined_obgyn_provider_dataset") && !is.null(combined_obgyn_provider_dataset)) {
#'   
#'   cat("\n=== EXECUTION COMPLETION SUMMARY ===\n")
#'   cat("Total OB/GYN providers processed:", nrow(combined_obgyn_provider_dataset), "\n")
#'   cat("Total columns in final dataset:", ncol(combined_obgyn_provider_dataset), "\n")
#'   
#'   # Show year distribution if available
#'   if ("data_year" %in% names(combined_obgyn_provider_dataset)) {
#'     cat("\nProviders by year:\n")
#'     year_summary <- combined_obgyn_provider_dataset |> 
#'       dplyr::count(data_year, sort = TRUE)
#'     print(year_summary)
#'   }
#'   
#'   # Show file source distribution if available  
#'   if ("source_filename" %in% names(combined_obgyn_provider_dataset)) {
#'     cat("\nProviders by source file:\n")
#'     source_summary <- combined_obgyn_provider_dataset |>
#'       dplyr::count(source_filename, sort = TRUE)
#'     print(source_summary)
#'   }
#'   
#'   # Show sample of the data structure
#'   cat("\nSample of combined dataset structure:\n")
#'   print(str(combined_obgyn_provider_dataset))
#'   
#'   cat("\nFirst few rows of key columns:\n")
#'   key_columns <- c("npi", "entity", "data_year", "source_filename")
#'   available_key_columns <- key_columns[key_columns %in% names(combined_obgyn_provider_dataset)]
#'   
#'   if (length(available_key_columns) > 0) {
#'     print(head(combined_obgyn_provider_dataset[, available_key_columns]))
#'   }
#'   
#'   cat("\n=== EXECUTION COMPLETED SUCCESSFULLY ===\n")
#'   
#' } else {
#'   cat("\nWARNING: Function execution may have failed - no dataset returned\n")
#' }
#' 
#' # ==============================================================================
#' # ALTERNATIVE EXECUTION EXAMPLES
#' # ==============================================================================
#' 
#' # Example 1: Process with strict schema compatibility (only common columns)
#' # combined_obgyn_strict <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/obgyn_strict_schema.csv",
#' #   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", "207VM0101X", "207VH0002X", "207VE0102X"),
#' #   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/schema_analysis_strict.csv",
#' #   schema_compatibility_strategy = "strict",
#' #   enable_verbose_logging = TRUE
#' # )
#' 
#' # Example 2: Focus on specific OB/GYN specialties only
#' # maternal_fetal_specialists <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/maternal_fetal_medicine.parquet",
#' #   obgyn_taxonomy_code_vector = c("207VM0101X"),  # Maternal & Fetal Medicine only
#' #   npi_file_pattern_regex = "npi\\d{4,5}\\.(csv|parquet)$",
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/mfm_schema_analysis.csv",
#' #   schema_compatibility_strategy = "union",
#' #   enable_verbose_logging = FALSE
#' # )
#' 
#' # Example 3: Process only recent years (modify file pattern)
#' # recent_obgyn_providers <- process_npi_obgyn_data(
#' #   npi_input_directory_path = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#' #   obgyn_output_file_path = "/Volumes/MufflyNew/nppes_historical_downloads/recent_obgyn_providers.feather",
#' #   obgyn_taxonomy_code_vector = c("207V00000X", "207VG0400X", "207VM0101X", "207VH0002X", "207VE0102X"),
#' #   npi_file_pattern_regex = "npi20[2-9]\\d\\.(csv|parquet)$",  # 2020s only
#' #   schema_analysis_report_path = "/Volumes/MufflyNew/nppes_historical_downloads/recent_schema_analysis.csv",
#' #   schema_compatibility_strategy = "intersect",
#' #   enable_verbose_logging = TRUE
#' # )

# Function at 1539 ----
#' Process NPI Data Files to Extract OB/GYN Providers with Intelligent Schema 
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


# execute ----
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



# Funmction at 0644 ----
# Load required packages
library(duckplyr)
library(duckdb)
library(DBI)
library(logger)
library(assertthat)
library(janitor)
library(lubridate)
library(tidyr)
library(stringr)
library(readr)
library(forcats)
library(dplyr)
library(tibble)
library(purrr)
library(crayon)

# Helper function to calculate mode within groups
calculate_group_mode <- function(x) {
  if (all(is.na(x))) return(NA)
  non_na_vals <- x[!is.na(x)]
  if (length(non_na_vals) == 0) return(NA)
  
  freq_table <- table(non_na_vals)
  mode_val <- names(freq_table)[which.max(freq_table)]
  
  # Return in same type as input
  if (lubridate::is.Date(x)) {
    return(as.Date(mode_val))
  } else if (is.numeric(x)) {
    return(as.numeric(mode_val))
  } else {
    return(mode_val)
  }
}

# Configure logging
logger::log_threshold(INFO)
verbose_logging <- TRUE

if (verbose_logging) {
  logger::log_info("Starting NPPES OB/GYN provider data processing with duckplyr")
  logger::log_info("Using duckplyr verbs for memory-efficient large dataset processing")
}

# Input file validation
input_csv_path <- "/Volumes/MufflyNew/nppes_historical_downloads/combined_obgyn_providers.csv"
assertthat::assert_that(file.exists(input_csv_path), 
                        msg = "Input CSV file does not exist")
logger::log_info("Input file validated")

# Create DuckDB connection and configure duckplyr
logger::log_info("Creating DuckDB connection for large dataset processing")
con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

# Configure duckplyr to use DuckDB backend
options(duckplyr.force = TRUE)

# Read CSV directly with DuckDB using explicit column types to avoid auto-detection issues
logger::log_info("Reading CSV directly with DuckDB, treating date columns as VARCHAR")
DBI::dbExecute(con, sprintf("
  CREATE TABLE raw_providers AS 
  SELECT * FROM read_csv_auto('%s', 
    all_varchar=true, 
    nullstr=['NA', '', 'NULL']
  )", input_csv_path))

# Create duckplyr data frame from the table  
raw_provider_dataset <- dplyr::tbl(con, "raw_providers") %>%
  duckplyr::as_duckdb_tibble()

# Get initial row count
initial_row_count <- raw_provider_dataset %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("Raw dataset loaded - rows:", initial_row_count)

# Filter for individual entities (not organizations)
logger::log_info("Filtering for individual provider entities (entity = 1)")
individual_providers <- raw_provider_dataset %>%
  dplyr::filter(entity == 1)

individual_count <- individual_providers %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("Individual providers filtered - rows:", individual_count)

# Remove unnecessary identifier columns
logger::log_info("Removing other provider identifier columns (othpid series)")
columns_to_remove_othpid <- c(
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

providers_without_othpid <- individual_providers %>%
  dplyr::select(-dplyr::all_of(columns_to_remove_othpid))

core_count <- providers_without_othpid %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("Removed othpid columns - rows:", core_count)

# Remove additional unnecessary columns
logger::log_info("Removing additional unnecessary organizational and contact columns")
additional_columns_to_remove <- c(
  "replacement_npi", "ein", "porgname", "porgnameoth", "porgnameothcode", 
  "plocline2", "npideactreason", "aolname", "aofname", "aomname", "aotitle", 
  "aotelnum"
)

providers_core_columns <- providers_without_othpid %>%
  dplyr::select(-dplyr::all_of(additional_columns_to_remove))

# Remove name and credential alternate columns
logger::log_info("Removing alternate name and credential columns")
name_credential_columns_to_remove <- c(
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

providers_essential_columns <- providers_core_columns %>%
  dplyr::select(-dplyr::all_of(name_credential_columns_to_remove))

logger::log_info("Removed name/credential columns")

# Clean provider credentials - materialize for R processing
logger::log_info("Cleaning provider credentials - removing punctuation and symbols")
providers_clean_credentials <- providers_essential_columns %>%
  dplyr::compute() %>%  # Standard compute since we avoided auto-detection issues
  dplyr::mutate(
    pcredential = stringr::str_remove_all(pcredential, "[[:punct:][:symbol:]]")
  ) %>%
  duckplyr::as_duckdb_tibble()  # Convert back to duckplyr for continued lazy evaluation

logger::log_info("Provider credentials cleaned")

# Remove additional taxonomy code columns
logger::log_info("Removing additional taxonomy code columns (ptaxcode5-15)")
taxonomy_columns_to_remove <- c(
  "ptaxcode5", "ptaxcode6", "ptaxcode7", "ptaxcode8", "ptaxcode9", 
  "ptaxcode10", "ptaxcode11", "ptaxcode12", "ptaxcode13", "ptaxcode14", 
  "ptaxcode15"
)

providers_core_taxonomy <- providers_clean_credentials %>%
  dplyr::select(-dplyr::all_of(taxonomy_columns_to_remove))

logger::log_info("Removed taxonomy columns")

# Filter for US addresses first, then remove date-related columns and clean up
logger::log_info("Filtering for US mailing and practice addresses only")
us_providers_filtered <- providers_core_taxonomy %>%
  dplyr::filter(pmailcountry == "US" & ploccountry == "US")

us_count <- us_providers_filtered %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("US address filter applied - rows:", us_count)

logger::log_info("Removing unnecessary date and contact columns")
final_columns_to_remove <- c(
  "npideactdate", "npireactdate", "entity", "pmailtel", "pmailfax", 
  "penumdate", "npireactdatestr", "pmailcountry", "ploccountry", 
  "plocfax", "source_filename", "npideactdatest", "npireactdatest"
)

providers_cleaned <- us_providers_filtered %>%
  dplyr::select(-dplyr::any_of(final_columns_to_remove))

# The us_providers_only step is now just the cleaned data
us_providers_only <- providers_cleaned

# Keep dates as character for now - convert at the end
logger::log_info("Keeping dates as character for efficient DuckDB processing")
providers_with_dates <- us_providers_only %>%
  dplyr::mutate(
    lastupdate_final = dplyr::coalesce(lastupdate, lastupdatestr)
  )

logger::log_info("Date field preparation completed")

logger::log_info("Date conversions completed")

# Group by NPI and calculate modes - all on character data for efficiency
logger::log_info("Grouping providers by NPI and calculating modes for imputation")
providers_with_modes <- providers_with_dates %>%
  dplyr::group_by(npi) %>%
  dplyr::arrange(data_year) %>%
  dplyr::compute() %>%  # Materialize for R mode calculations
  dplyr::mutate(
    pcredential_mode = calculate_group_mode(pcredential),
    certdate_mode = calculate_group_mode(certdate),
    penumdatestr_mode = calculate_group_mode(penumdatestr),
    ptaxcode2_mode = calculate_group_mode(ptaxcode2),
    ptaxcode3_mode = calculate_group_mode(ptaxcode3)
  ) %>%
  dplyr::ungroup() %>%
  duckplyr::as_duckdb_tibble()  # Convert back to duckplyr

logger::log_info("Mode calculations completed")

# Impute missing values using character data
logger::log_info("Imputing missing values using calculated modes")
providers_imputed <- providers_with_modes %>%
  dplyr::mutate(
    pcredential_final = dplyr::coalesce(pcredential, pcredential_mode),
    certdate_final = dplyr::coalesce(certdate, certdate_mode),
    penumdatestr_final = dplyr::coalesce(penumdatestr, penumdatestr_mode),
    ptaxcode2_final = dplyr::coalesce(ptaxcode2, ptaxcode2_mode),
    ptaxcode3_final = dplyr::coalesce(ptaxcode3, ptaxcode3_mode)
  )

logger::log_info("Data imputation completed")

# Define valid medical credentials for OB/GYN providers
logger::log_info("Defining valid OB/GYN medical credentials for filtering")
valid_obgyn_credentials <- c(
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

credential_count <- length(valid_obgyn_credentials)
logger::log_info("Filtering for valid OB/GYN credentials - credential types:", credential_count)

obgyn_providers_filtered <- providers_imputed %>%
  dplyr::filter(pcredential_final %in% valid_obgyn_credentials)

obgyn_count <- obgyn_providers_filtered %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("Credential filtering completed - rows:", obgyn_count)

# Standardize credentials to MD/DO
logger::log_info("Standardizing credentials to MD/DO categories")
standardized_credential_providers <- obgyn_providers_filtered %>%
  dplyr::mutate(
    pcredential_standardized = dplyr::case_when(
      pcredential_final == "DO" ~ "DO",
      TRUE ~ "MD"
    )
  )

logger::log_info("Credential standardization completed")

# Filter out US territories and military addresses - materialize for R processing
logger::log_info("Filtering out US territories and military postal codes")
excluded_state_codes <- c("aa", "ae", "as - american samoa", "as", "ap", "gu", "mp", "vi")
excluded_location_codes <- c("AA", "AE", "AP", "AS- AMERICAN SAMOA", "FM", "GU", "MP", "VI")

final_filtered_providers <- standardized_credential_providers %>%
  dplyr::compute() %>%  # Materialize to allow R string processing
  dplyr::filter(
    !stringr::str_to_lower(pmailstatename) %in% excluded_state_codes & 
      !plocstatename %in% excluded_location_codes
  ) %>%
  as_tibble() %>%
  duckplyr::as_duckdb_tibble() # Convert back to duckplyr

final_count <- final_filtered_providers %>% dplyr::tally() %>% dplyr::pull(n)
logger::log_info("Territory filtering completed - rows:", final_count)

# Clean up final dataset by removing temporary columns
logger::log_info("Cleaning up final dataset and selecting final columns")
processed_obgyn_providers_lazy <- final_filtered_providers %>%
  dplyr::select(
    -pcredential_mode, -certdate_mode, -penumdatestr_mode, 
    -ptaxcode2_mode, -ptaxcode3_mode, -lastupdatestr
  ) %>%
  # dplyr::rename(
  #   pcredential = pcredential_standardized,
  #   certdate = certdate_final,
  #   penumdatestr = penumdatestr_final,
  #   lastupdate = lastupdate_final,
  #   ptaxcode2 = ptaxcode2_final,
  #   ptaxcode3 = ptaxcode3_final
  # ) %>%
  dplyr::select(-pcredential_final)

# Collect final statistics using duckplyr
logger::log_info("Calculating final dataset statistics")
final_stats <- processed_obgyn_providers_lazy %>%
  dplyr::summarise(
    total_providers = dplyr::n(),
    unique_npis = dplyr::n_distinct(npi),
    md_count = sum(pcredential == "MD", na.rm = TRUE),
    do_count = sum(pcredential == "DO", na.rm = TRUE)
  ) %>%
  dplyr::collect()

logger::log_info("Final dataset statistics:")
logger::log_info("Total providers:", final_stats$total_providers)
logger::log_info("Unique NPIs:", final_stats$unique_npis)
logger::log_info("MD credentials:", final_stats$md_count)
logger::log_info("DO credentials:", final_stats$do_count)

# Retrieve final processed dataset into R and convert dates
logger::log_info("Collecting final processed dataset from duckplyr")
processed_obgyn_providers_raw <- processed_obgyn_providers_lazy %>%
  dplyr::collect()

# Final validation
assertthat::assert_that(is.data.frame(processed_obgyn_providers))
assertthat::assert_that(nrow(processed_obgyn_providers) > 0, 
                        msg = "No providers remain after filtering")

# Clean up DuckDB connection
DBI::dbDisconnect(con)

logger::log_info("DuckDB connection closed")
logger::log_info("NPPES OB/GYN provider processing pipeline completed using duckplyr")

final_rows <- nrow(processed_obgyn_providers)
final_cols <- ncol(processed_obgyn_providers)
logger::log_info("Final R dataset - rows:", final_rows, "cols:", final_cols)

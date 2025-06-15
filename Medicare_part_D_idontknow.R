#' Process Medicare Part D Prescribers Data with Improved Retirement Detection
#'
#' This function processes Medicare Part D Prescribers data from CSV files, 
#' filters to specific specialties, and calculates the last consecutive year 
#' of Medicare Part D prescribing for each provider using an improved retirement
#' detection algorithm that avoids the artificial end-of-data spike.
#'
#' @param directory_path Character. Path to directory containing Medicare Part D CSV files.
#'        Default: "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers"
#' @param duckdb_path Character. Path to DuckDB database file.
#'        Default: "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"
#' @param output_dir Character. Directory where output files will be saved.
#'        Default: A subdirectory "output" within directory_path
#' @param specialties Character vector. Provider specialties to include 
#'        Default: c("Gynecological Oncology", "Obstetrics & Gynecology")
#' @param country_code Character. Country code to filter by. Default: "US"
#' @param max_claims Numeric. Maximum claim threshold. Default: 50000
#' @param min_gap_years Numeric. Minimum years of absence to consider a provider retired.
#'        Default: 2
#' @param verbose Logical. Whether to print detailed logging messages. Default: TRUE
#'
#' @return A list containing four elements:
#'   - combined_data: Data frame of all filtered provider records
#'   - final_dataset: Data frame with standardized year values 
#'   - consecutive_years: Data frame with last consecutive year using original method
#'   - retirement_analysis: Data frame with improved retirement detection
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_threshold INFO log_layout layout_glue_generator log_info
#'   log_warn log_error log_debug
#' @importFrom conflicted conflicts_prefer
#' @importFrom dplyr filter select mutate distinct bind_rows arrange group_by
#'   summarise ungroup count pull row_number desc n_distinct
#' @importFrom tidyr unnest
#' @importFrom stringr str_extract str_remove
#' @importFrom tools file_path_sans_ext
#' @importFrom readr write_csv read_csv
#' @importFrom DBI dbConnect dbDisconnect dbListTables dbExecute dbReadTable
#' @importFrom ggplot2 ggplot aes geom_bar geom_histogram labs theme_minimal
#'   scale_fill_manual ggsave
#'
#' @examples
#' # Example 1: Process Medicare Part D data with default settings
#' medicare_results <- process_medicare_part_d_improved()
#' # The function returns a list with four data frames:
#' # - combined_data: All filtered provider records
#' # - final_dataset: Dataset with standardized year values
#' # - consecutive_years: Last consecutive year using original method
#' # - retirement_analysis: Improved retirement detection (requires 2-year gap)
#'
#' # Example 2: Process data for different specialties with custom thresholds
#' medicare_results <- process_medicare_part_d_improved(
#'   specialties = c("Hematology", "Oncology"),
#'   max_claims = 100000,
#'   min_gap_years = 3
#' )
#' # This processes the data for Hematology and Oncology specialties,
#' # sets a higher claims threshold of 100,000, and requires a 3-year gap
#' # before considering a provider retired
#'
#' # Example 3: Process data with minimal logging output
#' medicare_results <- process_medicare_part_d_improved(
#'   output_dir = "/Volumes/Video Projects Muffly 1/Medicare_part_D_results",
#'   verbose = FALSE
#' )
#' # This runs the processing with minimal logging output
#' # Only critical messages will be displayed
process_medicare_part_d_improved <- function(
    directory_path = "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers",
    duckdb_path = "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb",
    output_dir = file.path(directory_path, "output"),
    specialties = c("Gynecological Oncology", "Obstetrics & Gynecology"),
    country_code = "US",
    max_claims = 50000,
    min_gap_years = 2,
    verbose = TRUE
) {
  # Validate inputs
  assertthat::assert_that(
    is.character(directory_path),
    msg = "directory_path must be a character string"
  )
  assertthat::assert_that(
    is.character(duckdb_path),
    msg = "duckdb_path must be a character string"
  )
  assertthat::assert_that(
    is.character(output_dir),
    msg = "output_dir must be a character string"
  )
  assertthat::assert_that(
    is.character(specialties) && length(specialties) > 0,
    msg = "specialties must be a non-empty character vector"
  )
  assertthat::assert_that(
    is.character(country_code) && length(country_code) == 1,
    msg = "country_code must be a single character string"
  )
  assertthat::assert_that(
    is.numeric(max_claims) && max_claims > 0,
    msg = "max_claims must be a positive number"
  )
  assertthat::assert_that(
    is.numeric(min_gap_years) && min_gap_years > 0,
    msg = "min_gap_years must be a positive number"
  )
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be a logical value"
  )
  
  # Set up paths
  assertthat::assert_that(
    dir.exists(directory_path),
    msg = paste("Directory does not exist:", directory_path)
  )
  
  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  output_paths <- setup_output_paths(output_dir)
  
  # Configure logging
  setup_logging(verbose)
  
  # Process the Medicare data
  process_result <- tryCatch({
    # Set conflict preferences
    set_conflict_preferences()
    
    # Connect to database and process tables
    database_connection <- connect_to_database(duckdb_path)
    
    # Create/verify tables
    medicare_tables <- create_database_tables(
      database_connection, 
      directory_path
    )
    
    # Process the data
    processed_data <- process_medicare_tables(
      database_connection,
      medicare_tables,
      specialties,
      country_code,
      max_claims,
      output_paths$intermediate
    )
    
    # Create the final datasets
    final_datasets <- create_final_datasets(
      processed_data,
      output_paths
    )
    
    # Create improved retirement analysis
    retirement_analysis <- analyze_retirement_patterns(
      final_datasets$medicare_prescribers_final,
      min_gap_years,
      output_paths$retirement
    )
    
    # Generate comparison visualizations
    generate_comparison_visualizations(
      final_datasets$last_consecutive_year,
      retirement_analysis,
      output_dir
    )
    
    # Disconnect from database
    DBI::dbDisconnect(database_connection)
    logger::log_info("Database connection closed")
    
    # Return the results
    list(
      combined_data = processed_data$combined_data,
      final_dataset = final_datasets$medicare_prescribers_final,
      consecutive_years = final_datasets$last_consecutive_year,
      retirement_analysis = retirement_analysis
    )
  }, 
  error = function(e) {
    logger::log_error("Error in Medicare data processing: {e$message}")
    stop(paste("Medicare data processing failed:", e$message))
  })
  
  return(process_result)
}

#' Setup output file paths
#'
#' @param output_dir Character. Base output directory
#' @return List of output file paths
#' @noRd
setup_output_paths <- function(output_dir) {
  intermediate_path <- file.path(output_dir, "Medicare_part_D_prescribers_merged_data.csv")
  final_path <- file.path(output_dir, "end_Medicare_part_D_prescribers_combined_df.csv")
  consecutive_path <- file.path(output_dir, "end_Medicare_part_D_prescribers_last_consecutive_year.csv")
  retirement_path <- file.path(output_dir, "improved_Medicare_part_D_retirement_analysis.csv")
  
  return(list(
    intermediate = intermediate_path,
    final = final_path,
    consecutive = consecutive_path,
    retirement = retirement_path
  ))
}

#' Configure logging settings
#'
#' @param verbose Logical. Whether to use verbose logging
#' @noRd
setup_logging <- function(verbose) {
  log_level <- if (verbose) logger::INFO else logger::WARN
  logger::log_threshold(log_level)
  logger::log_layout(logger::layout_glue_generator(
    format = "{level} [{time}] {msg}"
  ))
  logger::log_info("Logging initialized with level: {log_level}")
}

#' Set function conflict preferences
#'
#' @noRd
set_conflict_preferences <- function() {
  logger::log_info("Setting function conflict preferences")
  tryCatch({
    conflicted::conflicts_prefer(stringr::str_remove_all)
    conflicted::conflicts_prefer(dplyr::left_join)
    conflicted::conflicts_prefer(dplyr::case_when)
    conflicted::conflicts_prefer(dplyr::filter)
    conflicted::conflicts_prefer(lubridate::year)
  }, error = function(e) {
    logger::log_warn("Error setting conflict preferences: {e$message}")
  })
}

#' Connect to the DuckDB database
#'
#' @param duckdb_path Character. Path to DuckDB database file
#' @return Database connection object
#' @noRd
connect_to_database <- function(duckdb_path) {
  logger::log_info("Connecting to DuckDB at: {duckdb_path}")
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
    assertthat::assert_that(!is.null(con), msg = "Database connection failed")
    return(con)
  }, error = function(e) {
    logger::log_error("Failed to connect to database: {e$message}")
    stop(paste("Database connection failed:", e$message))
  })
}

#' Create tables in database from CSV files
#'
#' @param connection Database connection object
#' @param directory_path Character. Path to directory with CSV files
#' @return Character vector of table names to process
#' @noRd
create_database_tables <- function(connection, directory_path) {
  logger::log_info("Checking existing tables in database")
  existing_tables <- DBI::dbListTables(connection)
  
  file_names <- list.files(directory_path, pattern = "\\.csv$")
  logger::log_info("Found {length(file_names)} CSV files in directory")
  
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
    
    sql_command <- sprintf(
      "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
      table_name, full_path
    )
    
    tryCatch({
      DBI::dbExecute(connection, sql_command)
      logger::log_info("Table '{table_name}' created successfully")
      created_tables <- c(created_tables, table_name)
    }, error = function(e) {
      logger::log_error("Failed to create table '{table_name}': {e$message}")
    })
  }
  
  logger::log_info("Created/verified {length(created_tables)} tables")
  
  # If no tables could be created, raise an error
  assertthat::assert_that(
    length(created_tables) > 0,
    msg = "No tables could be created from the provided directory"
  )
  
  # Return the list of tables to process
  medicare_tables <- identify_medicare_tables(created_tables)
  
  # Validate required tables exist
  validate_medicare_tables(connection, medicare_tables)
  
  return(medicare_tables)
}

#' Identify Medicare Part D tables to process
#'
#' @param available_tables Character vector of available table names
#' @return Character vector of Medicare table names to process
#' @noRd
identify_medicare_tables <- function(available_tables) {
  # Define patterns to match Medicare Part D tables
  pattern <- "MUP_DPR_RY\\d+_P04_V10_DY\\d+_NPI"
  
  # Find tables matching the pattern
  medicare_tables <- grep(pattern, available_tables, value = TRUE)
  
  if (length(medicare_tables) == 0) {
    # Try a more flexible approach if no tables match
    pattern <- ".*DY\\d+.*NPI.*"
    medicare_tables <- grep(pattern, available_tables, value = TRUE)
  }
  
  # If still no tables found, use all available tables
  if (length(medicare_tables) == 0) {
    logger::log_warn("No Medicare Part D tables identified by pattern, using all available tables")
    medicare_tables <- available_tables
  }
  
  logger::log_info("Identified {length(medicare_tables)} Medicare Part D tables for processing")
  return(medicare_tables)
}

#' Validate that required tables exist
#'
#' @param connection Database connection
#' @param tables Character vector of table names to validate
#' @noRd
validate_medicare_tables <- function(connection, tables) {
  existing_tables <- DBI::dbListTables(connection)
  missing_tables <- setdiff(tables, existing_tables)
  
  if (length(missing_tables) > 0) {
    logger::log_error("Missing required tables: {paste(missing_tables, collapse=', ')}")
    stop("Some required Medicare Part D tables are missing from the database")
  }
  
  logger::log_info("All required Medicare Part D tables are available")
  
  # Validate schema consistency (optional but recommended)
  validate_table_schemas(connection, tables)
}

#' Validate table schemas for consistency
#'
#' @param connection Database connection
#' @param tables Character vector of table names to check
#' @noRd
validate_table_schemas <- function(connection, tables) {
  logger::log_info("Validating schema consistency across tables")
  
  required_columns <- c("PRSCRBR_NPI", "Prscrbr_Type", "Prscrbr_Cntry", "Tot_Clms")
  schema_issues <- character(0)
  
  for (table in tables) {
    tryCatch({
      # Get a sample row to check columns
      sample_data <- DBI::dbReadTable(connection, table, n = 1)
      table_columns <- names(sample_data)
      
      # Check for required columns
      missing_columns <- setdiff(required_columns, table_columns)
      
      if (length(missing_columns) > 0) {
        issue <- paste0("Table '", table, "' is missing columns: ", 
                        paste(missing_columns, collapse = ", "))
        schema_issues <- c(schema_issues, issue)
      }
    }, error = function(e) {
      schema_issues <- c(
        schema_issues, 
        paste0("Error reading schema for table '", table, "': ", e$message)
      )
    })
  }
  
  if (length(schema_issues) > 0) {
    logger::log_warn("Schema validation issues found:")
    for (issue in schema_issues) {
      logger::log_warn("  - {issue}")
    }
  } else {
    logger::log_info("Schema validation passed for all tables")
  }
}

#' Process Medicare tables with filtering criteria
#'
#' @param connection Database connection
#' @param tables Character vector of table names to process
#' @param specialties Character vector of specialties to include
#' @param country_code Character. Country code to filter by
#' @param max_claims Numeric. Maximum claim threshold
#' @param output_path Character. Path to write intermediate output
#' @return List containing processed data
#' @noRd
process_medicare_tables <- function(
    connection, 
    tables, 
    specialties, 
    country_code, 
    max_claims,
    output_path
) {
  logger::log_info("Processing {length(tables)} Medicare tables")
  logger::log_info("Filtering criteria: specialties={paste(specialties, collapse=', ')}, country={country_code}, max_claims={max_claims}")
  
  processed_records <- list()
  combined_data <- data.frame()
  
  for (i in seq_along(tables)) {
    table_name <- tables[i]
    logger::log_info("Processing table {i}/{length(tables)}: {table_name}")
    
    tryCatch({
      # Apply filtering to table
      processed_table <- filter_medicare_table(
        connection, 
        table_name, 
        specialties, 
        country_code, 
        max_claims
      )
      
      # Collect results
      processed_data_df <- processed_table %>% dplyr::collect()
      record_count <- nrow(processed_data_df)
      logger::log_info("Collected {record_count} records from {table_name}")
      
      # Store results
      combined_data <- dplyr::bind_rows(combined_data, processed_data_df)
      processed_records[[table_name]] <- processed_table
      
    }, error = function(e) {
      logger::log_error("Failed to process table {table_name}: {e$message}")
    })
  }
  
  total_records <- nrow(combined_data)
  logger::log_info("Total records in combined dataset: {total_records}")
  
  # Write intermediate result to CSV
  write_intermediate_data(combined_data, output_path)
  
  return(list(
    processed_records = processed_records,
    combined_data = combined_data
  ))
}

#' Filter a Medicare table based on criteria
#'
#' @param connection Database connection
#' @param table_name Character. Name of table to filter
#' @param specialties Character vector of specialties to include
#' @param country_code Character. Country code to filter by
#' @param max_claims Numeric. Maximum claim threshold
#' @return Filtered database table reference
#' @noRd
filter_medicare_table <- function(
    connection, 
    table_name, 
    specialties, 
    country_code, 
    max_claims
) {
  logger::log_info("Applying filters to table: {table_name}")
  
  # Create reference to the table
  table_ref <- dplyr::tbl(connection, table_name)
  
  # Step 1: Filter by specialty and country
  logger::log_debug("Filtering by specialty and country")
  filtered_by_specialty <- table_ref %>%
    dplyr::filter(Prscrbr_Type %in% specialties & Prscrbr_Cntry == country_code)
  
  # Step 2: Select key columns and filter by claims
  logger::log_debug("Selecting columns and filtering by claims threshold")
  filtered_by_claims <- filtered_by_specialty %>%
    dplyr::select(PRSCRBR_NPI, Tot_Clms) %>%
    dplyr::filter(Tot_Clms < max_claims)
  
  # Step 3: Add prescription indicator and year information
  logger::log_debug("Adding prescription indicator and year")
  processed_data <- filtered_by_claims %>%
    dplyr::mutate(Prescribed = "Prescription written") %>%
    dplyr::distinct(PRSCRBR_NPI, .keep_all = TRUE) %>%
    dplyr::mutate(year = table_name)
  
  return(processed_data)
}

#' Write intermediate data to CSV
#'
#' @param combined_data Data frame with combined data
#' @param output_path Character. Path to write the data
#' @noRd
write_intermediate_data <- function(combined_data, output_path) {
  logger::log_info("Writing intermediate data to: {output_path}")
  
  tryCatch({
    readr::write_csv(combined_data, output_path)
    logger::log_info("Successfully wrote {nrow(combined_data)} records to {output_path}")
  }, error = function(e) {
    logger::log_error("Failed to write intermediate CSV: {e$message}")
    stop(paste("Failed to write intermediate data:", e$message))
  })
}

#' Create final datasets with standardized format
#'
#' @param processed_data List containing processed records
#' @param output_paths List of output file paths
#' @return List containing final datasets
#' @noRd
create_final_datasets <- function(processed_data, output_paths) {
  # Create finalized dataset with clean year values
  logger::log_info("Creating final dataset with standardized year format")
  
  medicare_prescribers_final <- create_standardized_dataset(
    processed_data$processed_records
  )
  
  # Write final combined dataset
  write_final_dataset(
    medicare_prescribers_final, 
    output_paths$final
  )
  
  # Calculate consecutive years analysis (original method)
  logger::log_info("Calculating consecutive years using original method")
  
  last_consecutive_year <- calculate_consecutive_years(
    medicare_prescribers_final,
    output_paths$consecutive
  )
  
  return(list(
    medicare_prescribers_final = medicare_prescribers_final,
    last_consecutive_year = last_consecutive_year
  ))
}

#' Create standardized dataset with clean year values
#'
#' @param processed_records List of processed table references
#' @return Data frame with standardized format
#' @noRd
create_standardized_dataset <- function(processed_records) {
  logger::log_info("Standardizing year format across all records")
  
  standardized_data <- lapply(processed_records, function(table_ref) {
    table_ref %>% dplyr::collect() %>% as.data.frame()
  }) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(year = stringr::str_extract(year, "DY\\d+")) %>%
    dplyr::mutate(year = stringr::str_remove(year, "DY")) %>%
    dplyr::mutate(year = factor(year)) %>%
    dplyr::distinct(PRSCRBR_NPI, year, .keep_all = TRUE) %>%
    dplyr::ungroup()
  
  logger::log_info("Created standardized dataset with {nrow(standardized_data)} records")
  
  return(standardized_data)
}

#' Write final dataset to CSV
#'
#' @param final_data Data frame with final data
#' @param output_path Character. Path to write the data
#' @noRd
write_final_dataset <- function(final_data, output_path) {
  logger::log_info("Writing final dataset to: {output_path}")
  
  tryCatch({
    readr::write_csv(final_data, output_path)
    logger::log_info("Final dataset successfully written to {output_path}")
  }, error = function(e) {
    logger::log_error("Failed to write final dataset: {e$message}")
    stop(paste("Failed to write final dataset:", e$message))
  })
}

#' Calculate consecutive years of prescribing (original method)
#'
#' @param final_data Data frame with standardized data
#' @param output_path Character. Path to write consecutive year data
#' @return Data frame with consecutive year analysis
#' @noRd
calculate_consecutive_years <- function(final_data, output_path) {
  logger::log_info("Loading final dataset for consecutive year analysis")
  
  consecutive_year_data <- final_data
  
  # Extract and standardize year format for calculation
  logger::log_info("Standardizing year format for consecutive year calculation")
  
  consecutive_year_data <- consecutive_year_data %>%
    dplyr::mutate(
      year = as.character(year),
      year = sub(".*RY(\\d+).*", "\\1", year),
      year = paste0("20", year),
      year = sub("_.*", "", year),
      year = as.numeric(year)
    )
  
  # Calculate last consecutive year for each provider
  logger::log_info("Computing last consecutive year for each provider")
  
  last_consecutive_year <- consecutive_year_data %>%
    dplyr::arrange(PRSCRBR_NPI, year) %>%
    dplyr::group_by(PRSCRBR_NPI) %>%
    dplyr::summarise(
      last_consecutive_year_Medicare_part_D_prescribers = 
        max(base::cumsum(c(0, diff(year) != 1)) + year)
    )
  
  logger::log_info("Identified last consecutive year for {nrow(last_consecutive_year)} providers")
  
  # Write consecutive year analysis to CSV
  logger::log_info("Writing original consecutive year analysis to: {output_path}")
  
  tryCatch({
    readr::write_csv(last_consecutive_year, output_path)
    logger::log_info("Original consecutive year analysis successfully written to {output_path}")
  }, error = function(e) {
    logger::log_error("Failed to write consecutive year analysis: {e$message}")
  })
  
  return(last_consecutive_year)
}

#' Analyze retirement patterns with improved methodology
#'
#' @param final_data Data frame with standardized data
#' @param min_gap_years Numeric. Minimum years of absence to consider a provider retired
#' @param output_path Character. Path to write retirement analysis data
#' @return Data frame with improved retirement analysis
#' @noRd
analyze_retirement_patterns <- function(final_data, min_gap_years, output_path) {
  logger::log_info("Implementing improved retirement detection with {min_gap_years}-year gap")
  
  # Convert final data to proper format for analysis
  retirement_data <- final_data %>%
    dplyr::mutate(
      year = as.character(year),
      year = sub(".*RY(\\d+).*", "\\1", year),
      year = paste0("20", year),
      year = sub("_.*", "", year),
      year = as.numeric(year)
    )
  
  # Get years in the dataset
  years_in_data <- retirement_data %>%
    dplyr::summarise(
      min_year = min(year),
      max_year = max(year)
    )
  
  min_year <- years_in_data$min_year
  max_year <- years_in_data$max_year
  
  logger::log_info("Dataset spans years {min_year} to {max_year}")
  
  # For each provider, identify all years in which they prescribed
  provider_years <- retirement_data %>%
    dplyr::group_by(PRSCRBR_NPI) %>%
    dplyr::summarise(
      years_present = list(year),
      min_year_present = min(year),
      max_year_present = max(year),
      total_years_present = n_distinct(year),
      # Calculate the expected number of years if provider was present every year
      expected_years = max_year_present - min_year_present + 1,
      # Calculate the proportion of expected years that provider was actually present
      coverage = total_years_present / expected_years
    )
  
  # Define providers who have "retired" as those who were missing for at 
  # least min_gap_years before the end of the dataset
  retirement_year <- max_year - min_gap_years
  
  retired_providers <- provider_years %>%
    dplyr::filter(
      # Provider must have been absent for at least min_gap_years
      max_year_present <= retirement_year,
      # Must have at least one year of data
      total_years_present > 0
    ) %>%
    dplyr::mutate(
      retirement_status = "Retired",
      retirement_year = max_year_present
    )
  
  logger::log_info("Identified {nrow(retired_providers)} providers who stopped prescribing")
  logger::log_info("Retirement definition: absent from data for at least {min_gap_years} years")
  
  # Create visualization of retirement patterns
  retirement_plot <- ggplot2::ggplot(retired_providers, 
                                     ggplot2::aes(x = retirement_year)) +
    ggplot2::geom_bar(fill = "darkred") +
    ggplot2::labs(
      title = paste0("Providers Who Stopped Prescribing (with ", 
                     min_gap_years, "-year gap)"),
      x = "Last Year of Prescribing",
      y = "Number of Physicians"
    ) +
    ggplot2::theme_minimal() +
    # Add note about methodology
    ggplot2::labs(caption = paste0("Note: 'Retired' defined as absent from data for at least ", 
                                   min_gap_years, " years"))
  
  # Save visualization if possible
  tryCatch({
    visualization_path <- file.path(dirname(output_path), "retirement_pattern.png")
    ggplot2::ggsave(visualization_path, retirement_plot, width = 10, height = 6)
    logger::log_info("Retirement visualization saved to: {visualization_path}")
  }, error = function(e) {
    logger::log_warn("Could not save retirement visualization: {e$message}")
  })
  
  # Create additional analysis for consistent prescribers
  consistent_providers <- provider_years %>%
    dplyr::filter(
      # Provider was present in most recent year
      max_year_present == max_year,
      # Provider has been present for at least 3 years
      total_years_present >= 3
    ) %>%
    dplyr::arrange(desc(coverage)) %>%
    dplyr::mutate(consistency_rank = row_number())
  
  # Log summary statistics
  logger::log_info("Identified {nrow(consistent_providers)} consistently prescribing providers")
  logger::log_info("Average coverage ratio: {mean(consistent_providers$coverage)}")
  
  # Write retirement analysis to CSV
  logger::log_info("Writing improved retirement analysis to: {output_path}")
  
  tryCatch({
    readr::write_csv(retired_providers, output_path)
    logger::log_info("Improved retirement analysis successfully written to {output_path}")
    
    # Also write consistency analysis
    consistency_path <- file.path(dirname(output_path), "consistent_providers_analysis.csv")
    readr::write_csv(consistent_providers, consistency_path)
    logger::log_info("Consistent providers analysis written to: {consistency_path}")
  }, error = function(e) {
    logger::log_error("Failed to write retirement analysis: {e$message}")
  })
  
  return(retired_providers)
}

# Updated visualization functions to both display and save graphs

#' Generate visualizations comparing original and improved methods
#'
#' @param original_data Data frame with original consecutive year analysis
#' @param improved_data Data frame with improved retirement analysis
#' @param output_dir Character. Directory to save visualizations
#' @return Invisibly returns TRUE if successful
#' @noRd
generate_comparison_visualizations <- function(original_data, improved_data, output_dir) {
  logger::log_info("Generating comparison visualizations")
  
  # Prepare comparison data
  original_years <- original_data %>%
    dplyr::rename(last_year = last_consecutive_year_Medicare_part_D_prescribers) %>%
    dplyr::count(last_year) %>%
    dplyr::mutate(method = "Original")
  
  improved_years <- improved_data %>%
    dplyr::count(retirement_year) %>%
    dplyr::rename(last_year = retirement_year) %>%
    dplyr::mutate(method = "Improved")
  
  # Combine for plotting
  comparison <- dplyr::bind_rows(original_years, improved_years)
  
  # Create comparison plot
  comparison_plot <- ggplot2::ggplot(comparison, 
                                     ggplot2::aes(x = last_year, y = n, fill = method)) +
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::labs(
      title = "Comparison of Retirement Detection Methods",
      x = "Last Year of Prescribing",
      y = "Number of Physicians",
      fill = "Method"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::scale_fill_manual(values = c("Original" = "darkgreen", "Improved" = "darkblue"))
  
  # Display the plot
  print(comparison_plot)
  
  # Save comparison visualization
  tryCatch({
    comparison_path <- file.path(output_dir, "retirement_method_comparison.png")
    ggplot2::ggsave(comparison_path, comparison_plot, width = 10, height = 6)
    logger::log_info("Comparison visualization saved to: {comparison_path}")
  }, error = function(e) {
    logger::log_warn("Could not save comparison visualization: {e$message}")
  })
  
  # Create a data distribution visualization
  data_distribution <- improved_data %>%
    dplyr::mutate(
      years_active = total_years_present,
      retirement_year = retirement_year
    )
  
  if (nrow(data_distribution) > 0) {
    # Only create if we have data
    distribution_plot <- ggplot2::ggplot(data_distribution) +
      ggplot2::geom_histogram(ggplot2::aes(x = years_active), binwidth = 1, fill = "steelblue") +
      ggplot2::labs(
        title = "Distribution of Years Active in Medicare Part D",
        x = "Number of Years Active",
        y = "Number of Physicians"
      ) +
      ggplot2::theme_minimal()
    
    # Display the plot
    print(distribution_plot)
    
    tryCatch({
      distribution_path <- file.path(output_dir, "years_active_distribution.png")
      ggplot2::ggsave(distribution_path, distribution_plot, width = 10, height = 6)
      logger::log_info("Years active distribution saved to: {distribution_path}")
    }, error = function(e) {
      logger::log_warn("Could not save years active distribution: {e$message}")
    })
  }
  
  # Create year-by-year count visualization
  year_count_data <- improved_data %>%
    tidyr::unnest(years_present) %>%
    dplyr::count(years_present) %>%
    dplyr::rename(year = years_present)
  
  if (nrow(year_count_data) > 0) {
    year_count_plot <- ggplot2::ggplot(year_count_data) +
      ggplot2::geom_bar(ggplot2::aes(x = year, y = n), stat = "identity", fill = "darkred") +
      ggplot2::labs(
        title = "Number of Physicians Active by Year",
        x = "Year",
        y = "Number of Physicians"
      ) +
      ggplot2::theme_minimal()
    
    # Display the plot
    print(year_count_plot)
    
    tryCatch({
      year_count_path <- file.path(output_dir, "physicians_by_year.png")
      ggplot2::ggsave(year_count_path, year_count_plot, width = 10, height = 6)
      logger::log_info("Physicians by year visualization saved to: {year_count_path}")
    }, error = function(e) {
      logger::log_warn("Could not save physicians by year visualization: {e$message}")
    })
  }
  
  return(invisible(TRUE))
}

#' Analyze retirement patterns with improved methodology (with plot display)
#'
#' @param final_data Data frame with standardized data
#' @param min_gap_years Numeric. Minimum years of absence to consider a provider retired
#' @param output_path Character. Path to write retirement analysis data
#' @return Data frame with improved retirement analysis
#' @noRd
analyze_retirement_patterns <- function(final_data, min_gap_years, output_path) {
  logger::log_info("Implementing improved retirement detection with {min_gap_years}-year gap")
  
  # Convert final data to proper format for analysis
  retirement_data <- final_data %>%
    dplyr::mutate(
      year = as.character(year),
      year = sub(".*RY(\\d+).*", "\\1", year),
      year = paste0("20", year),
      year = sub("_.*", "", year),
      year = as.numeric(year)
    )
  
  # Get years in the dataset
  years_in_data <- retirement_data %>%
    dplyr::summarise(
      min_year = min(year),
      max_year = max(year)
    )
  
  min_year <- years_in_data$min_year
  max_year <- years_in_data$max_year
  
  logger::log_info("Dataset spans years {min_year} to {max_year}")
  
  # For each provider, identify all years in which they prescribed
  provider_years <- retirement_data %>%
    dplyr::group_by(PRSCRBR_NPI) %>%
    dplyr::summarise(
      years_present = list(year),
      min_year_present = min(year),
      max_year_present = max(year),
      total_years_present = n_distinct(year),
      # Calculate the expected number of years if provider was present every year
      expected_years = max_year_present - min_year_present + 1,
      # Calculate the proportion of expected years that provider was actually present
      coverage = total_years_present / expected_years
    )
  
  # Define providers who have "retired" as those who were missing for at 
  # least min_gap_years before the end of the dataset
  retirement_year <- max_year - min_gap_years
  
  retired_providers <- provider_years %>%
    dplyr::filter(
      # Provider must have been absent for at least min_gap_years
      max_year_present <= retirement_year,
      # Must have at least one year of data
      total_years_present > 0
    ) %>%
    dplyr::mutate(
      retirement_status = "Retired",
      retirement_year = max_year_present
    )
  
  logger::log_info("Identified {nrow(retired_providers)} providers who stopped prescribing")
  logger::log_info("Retirement definition: absent from data for at least {min_gap_years} years")
  
  # Create visualization of retirement patterns
  retirement_plot <- ggplot2::ggplot(retired_providers, 
                                     ggplot2::aes(x = retirement_year)) +
    ggplot2::geom_bar(fill = "darkred") +
    ggplot2::labs(
      title = paste0("Providers Who Stopped Prescribing (with ", 
                     min_gap_years, "-year gap)"),
      x = "Last Year of Prescribing",
      y = "Number of Physicians"
    ) +
    ggplot2::theme_minimal() +
    # Add note about methodology
    ggplot2::labs(caption = paste0("Note: 'Retired' defined as absent from data for at least ", 
                                   min_gap_years, " years"))
  
  # Display the plot
  print(retirement_plot)
  
  # Save visualization if possible
  tryCatch({
    visualization_path <- file.path(dirname(output_path), "retirement_pattern.png")
    ggplot2::ggsave(visualization_path, retirement_plot, width = 10, height = 6)
    logger::log_info("Retirement visualization saved to: {visualization_path}")
  }, error = function(e) {
    logger::log_warn("Could not save retirement visualization: {e$message}")
  })
  
  # Create additional analysis for consistent prescribers
  consistent_providers <- provider_years %>%
    dplyr::filter(
      # Provider was present in most recent year
      max_year_present == max_year,
      # Provider has been present for at least 3 years
      total_years_present >= 3
    ) %>%
    dplyr::arrange(desc(coverage)) %>%
    dplyr::mutate(consistency_rank = row_number())
  
  # Log summary statistics
  logger::log_info("Identified {nrow(consistent_providers)} consistently prescribing providers")
  logger::log_info("Average coverage ratio: {mean(consistent_providers$coverage)}")
  
  # Write retirement analysis to CSV
  logger::log_info("Writing improved retirement analysis to: {output_path}")
  
  tryCatch({
    readr::write_csv(retired_providers, output_path)
    logger::log_info("Improved retirement analysis successfully written to {output_path}")
    
    # Also write consistency analysis
    consistency_path <- file.path(dirname(output_path), "consistent_providers_analysis.csv")
    readr::write_csv(consistent_providers, consistency_path)
    logger::log_info("Consistent providers analysis written to: {consistency_path}")
  }, error = function(e) {
    logger::log_error("Failed to write retirement analysis: {e$message}")
  })
  
  return(retired_providers)
}



####
# First, tell conflicted which setdiff to use
conflicted::conflicts_prefer(base::setdiff)

# Then run the function
results <- process_medicare_part_d_improved(
  directory_path = "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files",
  duckdb_path = "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb",
  output_dir = "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final_improved",
  specialties = c("Gynecological Oncology", "Obstetrics & Gynecology"),
  country_code = "US",
  max_claims = 50000,
  min_gap_years = 2,
  verbose = TRUE
)


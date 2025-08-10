

#***********
# GOBA_Scrape_subspecialists_only.csv has Guntupalli.  

# Setup and Configuration ----
source("R/01-setup.R")

# File Path Constants ----
INPUT_DIR <- "data/04-geocode/input"
INPUT_NPI_SUBSPECIALISTS_FILE <- "data/03-search_and_process_npi/output/end_complete_npi_for_subspecialists.rds"
INPUT_OBGYN_PROVIDER_DATASET <- "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv"
INTERMEDIATE_DIR <- "data/04-geocode/intermediate"
OUTPUT_DIR <- "data/04-geocode/output"
CITY_DICTIONARY_PATH <- "data/05-geocode-cleaning/city.rds"

# Helper Functions ----

#' Format numbers with commas for logging
#' @noRd
format_number_with_commas <- function(number_value) {
  format(number_value, big.mark = ",", scientific = FALSE)
}

#' Validate file existence and readability
#' @noRd
validate_input_file <- function(file_path, file_description = "Input file") {
  assertthat::assert_that(
    is.character(file_path),
    msg = glue::glue("{file_description} path must be a character string")
  )
  
  assertthat::assert_that(
    file.exists(file_path),
    msg = glue::glue("{file_description} does not exist: {file_path}")
  )
  
  assertthat::assert_that(
    file.size(file_path) > 0,
    msg = glue::glue("{file_description} is empty: {file_path}")
  )
}

#' Create directory if it doesn't exist
#' @noRd
ensure_directory_exists <- function(directory_path, verbose = TRUE) {
  if (!dir.exists(directory_path)) {
    dir.create(directory_path, recursive = TRUE)
    if (verbose) {
      logger::log_info("Created directory: {directory_path}")
    }
  }
}

# Main Processing Functions ----

#' Prepare Combined Address Data for Geocoding
#'
#' This function reads a dataset with pre-combined address fields and prepares
#' it for geocoding. It handles datasets where address components are already
#' combined into single fields. For maximum cost efficiency, use 
#' deduplicate_by_address_only=TRUE to geocode each unique address only once.
#'
#' @param input_file_path Character string. Path to input file (CSV or RDS format).
#'   Default uses INPUT_OBGYN_PROVIDER_DATASET constant
#' @param address_column_name Character string. Name of the combined address column.
#'   Default is "practice_address"
#' @param output_csv_path Character string. Path for output CSV file
#' @param deduplicate_by_npi Logical. Whether to keep only one record per unique
#'   NPI-address combination. Default is TRUE
#' @param deduplicate_by_address_only Logical. Whether to keep only one record 
#'   per unique address (ignoring NPI). Default is FALSE. When TRUE, this 
#'   minimizes geocoding costs by geocoding each address only once
#' @param state_filter Character string. Optional state filter for testing
#'   (e.g., "CO" for Colorado). Default is NULL to include all states
#' @param verbose Logical. Whether to enable verbose logging. Default is TRUE
#'
#' @return A tibble with the processed data ready for geocoding
#'
#' @examples
#' # Example 1: Most cost-effective - geocode each unique address only once
#' prepared_addresses <- prepare_combined_addresses_for_geocoding(
#'   deduplicate_by_address_only = TRUE,
#'   output_csv_path = file.path(INTERMEDIATE_DIR, "unique_addresses_only.csv")
#' )
#'
#' # Example 2: Test with Colorado addresses only
#' prepared_addresses <- prepare_combined_addresses_for_geocoding(
#'   state_filter = "CO",
#'   deduplicate_by_address_only = TRUE
#' )
#'
#' # Example 3: Use mailing addresses instead of practice addresses
#' prepared_addresses <- prepare_combined_addresses_for_geocoding(
#'   address_column_name = "pm_address",
#'   deduplicate_by_address_only = TRUE
#' )
#'
#' @export
prepare_combined_addresses_for_geocoding <- function(input_file_path = INPUT_OBGYN_PROVIDER_DATASET,
                                                     address_column_name = "practice_address",
                                                     output_csv_path = file.path(INTERMEDIATE_DIR, "prepared_addresses_for_geocoding.csv"),
                                                     deduplicate_by_npi = TRUE,
                                                     deduplicate_by_address_only = FALSE,
                                                     state_filter = NULL,
                                                     verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting address preparation for geocoding (combined addresses)")
    logger::log_info("Input file: {input_file_path}")
    logger::log_info("Output file: {output_csv_path}")
    logger::log_info("Address column: {address_column_name}")
  }
  
  # Validate input parameters
  validate_input_file(input_file_path, "Input provider dataset")
  
  assertthat::assert_that(
    is.character(address_column_name),
    msg = "address_column_name must be a character string"
  )
  
  assertthat::assert_that(
    is.character(output_csv_path),
    msg = "output_csv_path must be a character string"
  )
  
  if (verbose) {
    logger::log_info("Parameter validation completed successfully")
  }
  
  # Determine file type and read data
  input_file_extension <- tools::file_ext(input_file_path)
  
  if (verbose) {
    logger::log_info("Detected file type: {input_file_extension}")
  }
  
  tryCatch({
    if (tolower(input_file_extension) == "rds") {
      if (verbose) logger::log_info("Reading RDS file")
      provider_dataset <- readr::read_rds(input_file_path)
    } else if (tolower(input_file_extension) == "csv") {
      if (verbose) logger::log_info("Reading CSV file")
      provider_dataset <- readr::read_csv(input_file_path, show_col_types = FALSE)
    } else {
      stop(glue::glue("Unsupported file type: {input_file_extension} - only CSV and RDS files are supported"))
    }
  }, error = function(e) {
    logger::log_error("Failed to read input file: {e$message}")
    stop(glue::glue("Error reading file: {e$message}"))
  })
  
  # Convert to tibble and validate
  provider_dataset <- tibble::as_tibble(provider_dataset)
  
  assertthat::assert_that(
    is.data.frame(provider_dataset),
    msg = "Input data is not a valid data frame"
  )
  
  assertthat::assert_that(
    nrow(provider_dataset) > 0,
    msg = "Input dataset contains no rows"
  )
  
  # Check if address column exists
  assertthat::assert_that(
    address_column_name %in% names(provider_dataset),
    msg = glue::glue("Address column '{address_column_name}' not found. Available columns: {paste(names(provider_dataset), collapse = ', ')}")
  )
  
  initial_row_count <- nrow(provider_dataset)
  initial_column_count <- ncol(provider_dataset)
  
  if (verbose) {
    logger::log_info("Dataset loaded successfully")
    logger::log_info("Initial dimensions: {format_number_with_commas(initial_row_count)} rows x {initial_column_count} columns")
    logger::log_info("Address column found: {address_column_name}")
    
    # Show sample addresses
    sample_addresses <- head(provider_dataset[[address_column_name]], 3)
    sample_addresses <- sample_addresses[!is.na(sample_addresses)]
    if (length(sample_addresses) > 0) {
      logger::log_info("Sample addresses:")
      for (i in seq_along(sample_addresses)) {
        logger::log_info("  {i}: {sample_addresses[i]}")
      }
    }
  }
  
  # Rename address column to standardized name "address"
  if (address_column_name != "address") {
    provider_dataset <- provider_dataset %>%
      dplyr::rename(address = !!rlang::sym(address_column_name))
    
    if (verbose) {
      logger::log_info("Renamed '{address_column_name}' column to 'address'")
    }
  }
  
  # Filter by state if requested
  if (!is.null(state_filter)) {
    if (verbose) {
      logger::log_info("Filtering dataset for state: {state_filter}")
    }
    
    pre_filter_count <- nrow(provider_dataset)
    
    provider_dataset <- provider_dataset %>%
      dplyr::filter(stringr::str_detect(address, paste0("\\b", state_filter, "\\b")))
    
    post_filter_count <- nrow(provider_dataset)
    records_filtered_out <- pre_filter_count - post_filter_count
    
    if (verbose) {
      logger::log_info("State filtering completed")
      logger::log_info("Records before state filter: {pre_filter_count}")
      logger::log_info("Records after filtering for {state_filter}: {post_filter_count}")
      logger::log_info("Records filtered out: {records_filtered_out}")
    }
    
    assertthat::assert_that(
      post_filter_count > 0,
      msg = glue::glue("No records found for state: {state_filter}")
    )
  }
  
  # Remove records with missing addresses
  missing_addresses <- sum(is.na(provider_dataset$address) | provider_dataset$address == "")
  if (missing_addresses > 0) {
    if (verbose) {
      logger::log_warn("Removing {missing_addresses} records with missing addresses")
    }
    provider_dataset <- provider_dataset %>%
      dplyr::filter(!is.na(address) & address != "")
  }
  
  # Deduplicate if requested
  if (deduplicate_by_npi || deduplicate_by_address_only) {
    
    pre_dedup_count <- nrow(provider_dataset)
    
    if (deduplicate_by_address_only) {
      if (verbose) {
        logger::log_info("Deduplicating dataset by unique address only (most cost-effective)")
        logger::log_info("This minimizes geocoding costs by geocoding each address only once")
      }
      
      provider_dataset <- provider_dataset %>%
        dplyr::group_by(address) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()
      
      combination_type <- "unique addresses"
      
    } else if (deduplicate_by_npi && "npi" %in% names(provider_dataset)) {
      if (verbose) {
        logger::log_info("Deduplicating dataset by NPI and address combination")
      }
      
      provider_dataset <- provider_dataset %>%
        dplyr::group_by(npi, address) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()
      
      combination_type <- "unique NPI-address combinations"
      
    } else {
      if (verbose) {
        logger::log_info("NPI column not found - falling back to address-only deduplication")
      }
      
      provider_dataset <- provider_dataset %>%
        dplyr::group_by(address) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()
      
      combination_type <- "unique addresses"
    }
    
    post_dedup_count <- nrow(provider_dataset)
    records_removed <- pre_dedup_count - post_dedup_count
    
    if (verbose) {
      logger::log_info("Deduplication completed")
      logger::log_info("Records before deduplication: {pre_dedup_count}")
      logger::log_info("{stringr::str_to_title(combination_type)}: {post_dedup_count}")
      logger::log_info("Duplicate records removed: {records_removed}")
      
      if (deduplicate_by_address_only) {
        estimated_cost_after_free <- max(0, post_dedup_count - 10000) * 0.005
        logger::log_info("Estimated geocoding cost (after 10k free): ${round(estimated_cost_after_free, 2)}")
      }
    }
  }
  
  final_row_count <- nrow(provider_dataset)
  final_column_count <- ncol(provider_dataset)
  
  if (verbose) {
    logger::log_info("Address preparation completed")
    logger::log_info("Final dimensions: {format_number_with_commas(final_row_count)} rows x {final_column_count} columns")
    
    if ((deduplicate_by_npi || deduplicate_by_address_only) && exists("records_removed")) {
      deduplication_efficiency <- round((final_row_count / initial_row_count) * 100, 1)
      logger::log_info("Deduplication efficiency: {deduplication_efficiency}% of original records retained")
    }
  }
  
  # Create output directory and save
  ensure_directory_exists(dirname(output_csv_path))
  
  tryCatch({
    readr::write_csv(provider_dataset, output_csv_path)
  }, error = function(e) {
    logger::log_error("Failed to write output file: {e$message}")
    stop(glue::glue("Error writing CSV file: {e$message}"))
  })
  
  assertthat::assert_that(
    file.exists(output_csv_path),
    msg = "Output CSV file was not created"
  )
  
  output_file_size_bytes <- file.size(output_csv_path)
  output_file_size_mb <- round(output_file_size_bytes / (1024^2), 2)
  
  if (verbose) {
    logger::log_info("Geocoding preparation completed successfully")
    logger::log_info("Output file: {output_csv_path}")
    logger::log_info("Output file size: {output_file_size_mb} MB")
    logger::log_info("Records prepared for geocoding: {format_number_with_commas(final_row_count)}")
  }
  
  return(provider_dataset)
}

#' Process and Filter Subspecialist Data for Geocoding
#'
#' This function combines the prepared geocoding data with subspecialist information,
#' filters for specific subspecialties, and prepares the final dataset for geocoding.
#' It includes comprehensive logging and validation at each step.
#'
#' @param geocoding_prepared_data Data frame. The prepared geocoding dataset from
#'   prepare_combined_addresses_for_geocoding()
#' @param subspecialists_file_path Character string. Path to the subspecialists
#'   RDS file containing NPI and subspecialty information
#' @param subspecialty_filter Character string. Subspecialty code to filter for
#'   (e.g., "FPM" for Family Planning Medicine/Gynecologic Oncology)
#' @param output_subspecialist_path Character string. Path for the complete
#'   subspecialist geocoding dataset
#' @param output_filtered_path Character string. Path for the filtered
#'   subspecialty-specific dataset
#' @param verbose Logical. Whether to enable verbose logging. Default is TRUE
#'
#' @return A list containing:
#'   \item{all_subspecialists}{Data frame with all subspecialist geocoding data}
#'   \item{filtered_subspecialists}{Data frame with filtered subspecialty data}
#'   \item{subspecialty_summary}{Summary statistics of subspecialties}
#'
#' @examples
#' # Example 1: Process all subspecialists and filter for Family Planning Medicine
#' subspecialist_results <- process_subspecialist_geocoding_data(
#'   geocoding_prepared_data = prepared_geocoding_dataset,
#'   subspecialists_file_path = "data/subspecialists.rds",
#'   subspecialty_filter = "FPM",
#'   output_subspecialist_path = file.path(INTERMEDIATE_DIR, 
#'     "subspecialist_geocoding_data.csv"),
#'   output_filtered_path = file.path(INTERMEDIATE_DIR, 
#'     "FPM_only_subspecialist_geocoding_data.csv")
#' )
#'
#' # Example 2: Process with different subspecialty filter
#' subspecialist_results <- process_subspecialist_geocoding_data(
#'   geocoding_prepared_data = prepared_geocoding_dataset,
#'   subspecialty_filter = "REI",
#'   subspecialists_file_path = "data/subspecialists.rds",
#'   verbose = TRUE
#' )
#'
#' # Example 3: Process without filtering (keep all subspecialties)
#' subspecialist_results <- process_subspecialist_geocoding_data(
#'   geocoding_prepared_data = prepared_geocoding_dataset,
#'   subspecialty_filter = NULL,
#'   subspecialists_file_path = "data/subspecialists.rds"
#' )
#'
#' @export
process_subspecialist_geocoding_data <- function(geocoding_prepared_data,
                                                 subspecialists_file_path = INPUT_NPI_SUBSPECIALISTS_FILE,
                                                 subspecialty_filter = "FPM",
                                                 output_subspecialist_path = file.path(INTERMEDIATE_DIR, "subspecialist_geocoding_data.csv"),
                                                 output_filtered_path = file.path(INTERMEDIATE_DIR, "filtered_subspecialist_geocoding_data.csv"),
                                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting subspecialist geocoding data processing")
    logger::log_info("Subspecialists file: {subspecialists_file_path}")
    logger::log_info("Subspecialty filter: {ifelse(is.null(subspecialty_filter), 'None (all subspecialties)', subspecialty_filter)}")
  }
  
  # Input validation
  assertthat::assert_that(
    is.data.frame(geocoding_prepared_data),
    msg = "geocoding_prepared_data must be a data frame"
  )
  
  assertthat::assert_that(
    nrow(geocoding_prepared_data) > 0,
    msg = "geocoding_prepared_data cannot be empty"
  )
  
  validate_input_file(subspecialists_file_path, "Subspecialists file")
  
  if (verbose) {
    logger::log_info("Input validation completed")
    logger::log_info("Geocoding prepared data: {format_number_with_commas(nrow(geocoding_prepared_data))} rows")
  }
  
  # Load subspecialists data
  tryCatch({
    npi_subspecialists_data <- readr::read_rds(subspecialists_file_path) %>%
      dplyr::distinct(NPI, .keep_all = TRUE)
  }, error = function(e) {
    logger::log_error("Failed to read subspecialists file: {e$message}")
    stop(glue::glue("Error reading subspecialists file: {e$message}"))
  })
  
  if (verbose) {
    logger::log_info("Subspecialists data loaded: {format_number_with_commas(nrow(npi_subspecialists_data))} unique NPIs")
    logger::log_info("Available columns: {paste(names(npi_subspecialists_data), collapse = ', ')}")
  }
  
  # Validate required columns exist
  required_subspecialist_columns <- c("NPI", "sub1", "first_name", "last_name")
  missing_columns <- setdiff(required_subspecialist_columns, names(npi_subspecialists_data))
  
  assertthat::assert_that(
    length(missing_columns) == 0,
    msg = glue::glue("Missing required columns in subspecialists data: {paste(missing_columns, collapse = ', ')}")
  )
  
  # Perform inner join
  if (verbose) {
    logger::log_info("Performing inner join between geocoding data and subspecialists data")
  }
  
  subspecialist_geocoding_combined <- geocoding_prepared_data %>%
    dplyr::inner_join(
      npi_subspecialists_data %>% 
        dplyr::select(NPI, sub1, first_name, last_name),
      by = c("npi" = "NPI")
    ) %>%
    dplyr::relocate(sub1, .before = 1) %>%
    dplyr::arrange(npi)
  
  # Log join results
  if (verbose) {
    logger::log_info("Inner join completed")
    logger::log_info("Original geocoding dataset: {format_number_with_commas(nrow(geocoding_prepared_data))} rows")
    logger::log_info("Subspecialists dataset: {format_number_with_commas(nrow(npi_subspecialists_data))} rows")
    logger::log_info("After inner join: {format_number_with_commas(nrow(subspecialist_geocoding_combined))} rows")
  }
  
  # Generate subspecialty summary
  subspecialty_counts <- table(subspecialist_geocoding_combined$sub1, useNA = "ifany")
  
  if (verbose) {
    logger::log_info("Subspecialty breakdown:")
    for (specialty_name in names(subspecialty_counts)) {
      logger::log_info("  {specialty_name}: {subspecialty_counts[specialty_name]} records")
    }
  }
  
  # Save complete subspecialist dataset
  ensure_directory_exists(dirname(output_subspecialist_path))
  
  tryCatch({
    readr::write_csv(subspecialist_geocoding_combined, output_subspecialist_path)
    if (verbose) {
      logger::log_info("Complete subspecialist dataset saved: {output_subspecialist_path}")
    }
  }, error = function(e) {
    logger::log_error("Failed to save complete subspecialist dataset: {e$message}")
    stop(glue::glue("Error saving subspecialist dataset: {e$message}"))
  })
  
  # Filter for specific subspecialty if requested
  filtered_subspecialist_data <- NULL
  if (!is.null(subspecialty_filter)) {
    
    if (verbose) {
      logger::log_info("Filtering for subspecialty: {subspecialty_filter}")
    }
    
    # Check if subspecialty exists in data
    available_subspecialties <- unique(subspecialist_geocoding_combined$sub1)
    if (!subspecialty_filter %in% available_subspecialties) {
      logger::log_warn("Subspecialty '{subspecialty_filter}' not found in data")
      logger::log_warn("Available subspecialties: {paste(available_subspecialties, collapse = ', ')}")
      
      # Suggest alternatives for common subspecialty code mappings
      suggestions <- list(
        "GO" = c("ONC", "FPM"),  # Gynecologic Oncology might be coded as ONC or FPM
        "GYN" = c("FPM", "MIG"), # General Gynecology 
        "OBGYN" = c("FPM", "MFM", "REI", "MIG", "ONC")
      )
      
      if (subspecialty_filter %in% names(suggestions)) {
        suggested_codes <- intersect(suggestions[[subspecialty_filter]], available_subspecialties)
        if (length(suggested_codes) > 0) {
          logger::log_info("Suggested alternative subspecialty codes: {paste(suggested_codes, collapse = ', ')}")
        }
      }
    }
    
    filtered_subspecialist_data <- subspecialist_geocoding_combined %>%
      dplyr::filter(sub1 == subspecialty_filter)
    
    records_after_filter <- nrow(filtered_subspecialist_data)
    
    if (verbose) {
      logger::log_info("Records after filtering for {subspecialty_filter}: {format_number_with_commas(records_after_filter)}")
    }
    
    # Handle empty filtered dataset
    if (records_after_filter == 0) {
      logger::log_warn("No records found for subspecialty '{subspecialty_filter}' - using complete subspecialist dataset instead")
      filtered_subspecialist_data <- subspecialist_geocoding_combined
      records_after_filter <- nrow(filtered_subspecialist_data)
      
      if (verbose) {
        logger::log_info("Continuing with all {format_number_with_commas(records_after_filter)} subspecialist records")
      }
    }
    
    # Save filtered dataset
    ensure_directory_exists(dirname(output_filtered_path))
    
    tryCatch({
      readr::write_csv(filtered_subspecialist_data, output_filtered_path)
      if (verbose) {
        logger::log_info("Filtered subspecialist dataset saved: {output_filtered_path}")
      }
    }, error = function(e) {
      logger::log_error("Failed to save filtered subspecialist dataset: {e$message}")
      stop(glue::glue("Error saving filtered dataset: {e$message}"))
    })
    
  } else {
    filtered_subspecialist_data <- subspecialist_geocoding_combined
    if (verbose) {
      logger::log_info("No subspecialty filter applied - using complete dataset")
    }
  }
  
  # Show sample records
  if (verbose && nrow(filtered_subspecialist_data) > 0) {
    logger::log_info("Sample records from final dataset:")
    sample_records <- filtered_subspecialist_data %>%
      dplyr::select(npi, plname, pfname, sub1, address) %>%
      head(3)
    
    for (i in seq_len(nrow(sample_records))) {
      logger::log_info("  {i}. {sample_records$pfname[i]} {sample_records$plname[i]} (NPI: {sample_records$npi[i]}, Specialty: {sample_records$sub1[i]})")
      logger::log_info("     Address: {sample_records$address[i]}")
    }
  }
  
  if (verbose) {
    logger::log_info("Subspecialist geocoding data processing completed")
  }
  
  # Return results
  return(list(
    all_subspecialists = subspecialist_geocoding_combined,
    filtered_subspecialists = filtered_subspecialist_data,
    subspecialty_summary = subspecialty_counts
  ))
}

#' Clean and Standardize Addresses for Matching
#'
#' This function performs address cleaning operations to prepare addresses for
#' matching with geocoded results. It removes country suffixes, handles address
#' components after dashes, and standardizes state/ZIP code formatting.
#'
#' @param input_address_data Data frame. Dataset containing addresses to clean
#' @param address_column Character string. Name of the address column to clean.
#'   Default is "address"
#' @param output_file_path Character string. Path for the cleaned address dataset
#' @param verbose Logical. Whether to enable verbose logging. Default is TRUE
#'
#' @return Data frame with cleaned addresses in the 'address_cleaned' column
#'
#' @examples
#' # Example 1: Clean addresses from filtered subspecialist data
#' cleaned_addresses <- clean_addresses_for_matching(
#'   input_address_data = subspecialist_filtered_data,
#'   output_file_path = file.path(INTERMEDIATE_DIR, 
#'     "cleaned_addresses_for_matching.csv")
#' )
#'
#' # Example 2: Clean addresses with custom address column name
#' cleaned_addresses <- clean_addresses_for_matching(
#'   input_address_data = address_dataset,
#'   address_column = "practice_address",
#'   output_file_path = "cleaned_addresses.csv"
#' )
#'
#' # Example 3: Clean addresses without verbose logging
#' cleaned_addresses <- clean_addresses_for_matching(
#'   input_address_data = address_dataset,
#'   verbose = FALSE
#' )
#'
#' @export
clean_addresses_for_matching <- function(input_address_data,
                                         address_column = "address",
                                         output_file_path = file.path(INTERMEDIATE_DIR, "cleaned_addresses_for_matching.csv"),
                                         verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting address cleaning for matching")
    logger::log_info("Address column: {address_column}")
    logger::log_info("Output file: {output_file_path}")
  }
  
  # Input validation
  assertthat::assert_that(
    is.data.frame(input_address_data),
    msg = "input_address_data must be a data frame"
  )
  
  assertthat::assert_that(
    nrow(input_address_data) > 0,
    msg = "input_address_data cannot be empty"
  )
  
  assertthat::assert_that(
    address_column %in% names(input_address_data),
    msg = glue::glue("Address column '{address_column}' not found in input data")
  )
  
  initial_record_count <- nrow(input_address_data)
  
  if (verbose) {
    logger::log_info("Input validation completed")
    logger::log_info("Records to process: {format_number_with_commas(initial_record_count)}")
  }
  
  # Perform address cleaning operations
  tryCatch({
    cleaned_address_data <- input_address_data %>%
      dplyr::mutate(
        address_cleaned = exploratory::str_remove(
          .data[[address_column]], 
          stringr::regex(", United States$", ignore_case = TRUE), 
          remove_extra_space = TRUE
        ),
        .after = ifelse(address_column %in% names(.), address_column, dplyr::last_col())
      ) %>%
      dplyr::mutate(
        address_cleaned = exploratory::str_remove_after(address_cleaned, sep = "\\-")
      ) %>%
      dplyr::mutate(
        address_cleaned = stringr::str_replace(
          address_cleaned, 
          "([A-Z]{2}) ([0-9]{5})", 
          "\\1, \\2"
        )
      )
    
  }, error = function(e) {
    logger::log_error("Error during address cleaning: {e$message}")
    stop(glue::glue("Address cleaning failed: {e$message}"))
  })
  
  final_record_count <- nrow(cleaned_address_data)
  
  if (verbose) {
    logger::log_info("Address cleaning completed")
    logger::log_info("Records after cleaning: {format_number_with_commas(final_record_count)}")
    
    # Show sample of cleaned addresses
    sample_cleaned_addresses <- head(cleaned_address_data$address_cleaned[!is.na(cleaned_address_data$address_cleaned)], 3)
    if (length(sample_cleaned_addresses) > 0) {
      logger::log_info("Sample cleaned addresses:")
      for (i in seq_along(sample_cleaned_addresses)) {
        logger::log_info("  {i}: {sample_cleaned_addresses[i]}")
      }
    }
  }
  
  # Save cleaned data
  ensure_directory_exists(dirname(output_file_path))
  
  tryCatch({
    readr::write_csv(cleaned_address_data, output_file_path)
    if (verbose) {
      logger::log_info("Cleaned address data saved: {output_file_path}")
    }
  }, error = function(e) {
    logger::log_error("Failed to save cleaned address data: {e$message}")
    stop(glue::glue("Error saving cleaned addresses: {e$message}"))
  })
  
  return(cleaned_address_data)
}

#' Parse and Standardize Addresses Using Postmastr
#'
#' This function performs comprehensive address parsing and standardization using
#' the postmastr package. It follows the required order of operations: prep,
#' postal code, state, city, house number, street suffix, street direction,
#' and street name parsing.
#'
#' @param input_data_path Character string. Path to the CSV file containing
#'   addresses to parse
#' @param city_dictionary_path Character string. Path to the city dictionary
#'   RDS file. Default uses the predefined constant
#' @param output_file_path Character string. Path for the parsed address dataset
#' @param verbose Logical. Whether to enable verbose logging. Default is TRUE
#'
#' @return Data frame with parsed address components and ACOG district assignments
#'
#' @examples
#' # Example 1: Parse addresses with default city dictionary
#' parsed_addresses <- parse_addresses_with_postmastr(
#'   input_data_path = file.path(INTERMEDIATE_DIR, "addresses_to_parse.csv"),
#'   output_file_path = file.path(OUTPUT_DIR, "parsed_addresses.csv")
#' )
#'
#' # Example 2: Parse addresses with custom city dictionary
#' parsed_addresses <- parse_addresses_with_postmastr(
#'   input_data_path = "addresses.csv",
#'   city_dictionary_path = "custom_city_dictionary.rds",
#'   output_file_path = "parsed_output.csv"
#' )
#'
#' # Example 3: Parse addresses with minimal logging
#' parsed_addresses <- parse_addresses_with_postmastr(
#'   input_data_path = "addresses.csv",
#'   verbose = FALSE
#' )
#'
#' @export
parse_addresses_with_postmastr <- function(input_data_path,
                                           city_dictionary_path = CITY_DICTIONARY_PATH,
                                           output_file_path = file.path(OUTPUT_DIR, "parsed_addresses_postmastr.csv"),
                                           verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting address parsing with postmastr")
    logger::log_info("Input file: {input_data_path}")
    logger::log_info("City dictionary: {city_dictionary_path}")
    logger::log_info("Output file: {output_file_path}")
  }
  
  # Validate input files
  validate_input_file(input_data_path, "Input address data file")
  validate_input_file(city_dictionary_path, "City dictionary file")
  
  # Load input data
  tryCatch({
    address_data_for_parsing <- readr::read_csv(input_data_path, show_col_types = FALSE)
  }, error = function(e) {
    logger::log_error("Failed to read input address data: {e$message}")
    stop(glue::glue("Error reading input data: {e$message}"))
  })
  
  initial_record_count <- nrow(address_data_for_parsing)
  
  if (verbose) {
    logger::log_info("Address data loaded: {format_number_with_commas(initial_record_count)} records")
  }
  
  # Step 1: Initialize postmastr parsing
  if (verbose) {
    logger::log_info("Step 1: Initializing postmastr parsing (pm_identify)")
  }
  
  tryCatch({
    postmastr_identified_data <- address_data_for_parsing %>% 
      postmastr::pm_identify(var = "address")
  }, error = function(e) {
    logger::log_error("Failed during pm_identify: {e$message}")
    stop(glue::glue("Postmastr identification failed: {e$message}"))
  })
  
  # Step 2: Prepare addresses for parsing
  if (verbose) {
    logger::log_info("Step 2: Preparing addresses for parsing (pm_prep)")
  }
  
  tryCatch({
    postmastr_prepared_data <- postmastr::pm_prep(postmastr_identified_data, var = "address", type = "street") %>%
      dplyr::mutate(pm.address = stringr::str_squish(pm.address))
  }, error = function(e) {
    logger::log_error("Failed during pm_prep: {e$message}")
    stop(glue::glue("Address preparation failed: {e$message}"))
  })
  
  # Step 3: Parse postal codes
  if (verbose) {
    logger::log_info("Step 3: Parsing postal codes (pm_postal_parse)")
  }
  
  tryCatch({
    postmastr_prepared_data <- postmastr::pm_postal_parse(postmastr_prepared_data)
  }, error = function(e) {
    logger::log_error("Failed during postal code parsing: {e$message}")
    stop(glue::glue("Postal code parsing failed: {e$message}"))
  })
  
  # Step 4: Parse states
  if (verbose) {
    logger::log_info("Step 4: Creating state dictionary and parsing states")
  }
  
  tryCatch({
    state_dictionary <- postmastr::pm_dictionary(locale = "us", type = "state", case = c("title", "upper"))
    
    # Validate state matches
    state_validation_result <- postmastr::pm_state_all(postmastr_prepared_data, dictionary = state_dictionary)
    if (verbose) {
      logger::log_info("State validation result: {state_validation_result}")
    }
    
    postmastr_prepared_data <- postmastr::pm_state_parse(postmastr_prepared_data, dictionary = state_dictionary)
    
  }, error = function(e) {
    logger::log_error("Failed during state parsing: {e$message}")
    stop(glue::glue("State parsing failed: {e$message}"))
  })
  
  # Step 5: Parse cities
  if (verbose) {
    logger::log_info("Step 5: Loading city dictionary and parsing cities")
  }
  
  tryCatch({
    city_dictionary <- readr::read_rds(city_dictionary_path)
    
    # Validate city matches
    city_validation_result <- postmastr::pm_city_all(postmastr_prepared_data, dictionary = city_dictionary)
    if (verbose) {
      logger::log_info("City validation result: {city_validation_result}")
    }
    
    postmastr_prepared_data <- postmastr::pm_city_parse(postmastr_prepared_data, 
                                                        dictionary = city_dictionary, 
                                                        locale = "us")
    
  }, error = function(e) {
    logger::log_error("Failed during city parsing: {e$message}")
    stop(glue::glue("City parsing failed: {e$message}"))
  })
  
  # Step 6: Parse house numbers
  if (verbose) {
    logger::log_info("Step 6: Parsing house numbers")
  }
  
  tryCatch({
    house_validation_result <- postmastr::pm_house_all(postmastr_prepared_data)
    if (verbose) {
      logger::log_info("House number validation result: {house_validation_result}")
    }
    
    postmastr_prepared_data <- postmastr::pm_house_parse(postmastr_prepared_data, locale = "us")
    
  }, error = function(e) {
    logger::log_error("Failed during house number parsing: {e$message}")
    stop(glue::glue("House number parsing failed: {e$message}"))
  })
  
  # Step 7: Parse street suffixes and directions
  if (verbose) {
    logger::log_info("Step 7: Parsing street suffixes and directions")
  }
  
  tryCatch({
    postmastr_prepared_data <- postmastr::pm_streetSuf_parse(postmastr_prepared_data, locale = "us")
    postmastr_prepared_data <- postmastr::pm_streetDir_parse(postmastr_prepared_data, locale = "us")
    
  }, error = function(e) {
    logger::log_error("Failed during street suffix/direction parsing: {e$message}")
    stop(glue::glue("Street suffix/direction parsing failed: {e$message}"))
  })
  
  # Step 8: Parse street names
  if (verbose) {
    logger::log_info("Step 8: Parsing street names")
  }
  
  tryCatch({
    postmastr_prepared_data <- postmastr::pm_street_parse(postmastr_prepared_data, 
                                                          ordinal = TRUE, 
                                                          drop = TRUE, 
                                                          locale = "us")
  }, error = function(e) {
    logger::log_error("Failed during street name parsing: {e$message}")
    stop(glue::glue("Street name parsing failed: {e$message}"))
  })
  
  # Step 9: Reconstruct and add ACOG districts
  if (verbose) {
    logger::log_info("Step 9: Reconstructing addresses and adding ACOG districts")
  }
  
  tryCatch({
    acog_districts_reference <- tyler::ACOG_Districts
    
    final_parsed_addresses <- postmastr::pm_replace(street = postmastr_prepared_data, 
                                                    source = postmastr_identified_data) %>%
      exploratory::left_join(acog_districts_reference, 
                             by = dplyr::join_by(pm.state == State_Abbreviations))
    
    # Reorder ACOG district factor levels
    final_parsed_addresses$ACOG_District <- factor(
      final_parsed_addresses$ACOG_District,
      levels = c("District I", "District II", "District III", "District IV", "District V",
                 "District VI", "District VII", "District VIII", "District IX",
                 "District XI", "District XII")
    )
    
  }, error = function(e) {
    logger::log_error("Failed during address reconstruction or ACOG district assignment: {e$message}")
    stop(glue::glue("Address reconstruction failed: {e$message}"))
  })
  
  final_record_count <- nrow(final_parsed_addresses)
  
  if (verbose) {
    logger::log_info("Address parsing completed")
    logger::log_info("Records after parsing: {format_number_with_commas(final_record_count)}")
    
    # Show ACOG district distribution
    acog_district_counts <- table(final_parsed_addresses$ACOG_District, useNA = "ifany")
    logger::log_info("ACOG District distribution:")
    for (district_name in names(acog_district_counts)) {
      logger::log_info("  {district_name}: {acog_district_counts[district_name]} records")
    }
  }
  
  # Save parsed data
  ensure_directory_exists(dirname(output_file_path))
  
  tryCatch({
    readr::write_csv(final_parsed_addresses, output_file_path)
    if (verbose) {
      logger::log_info("Parsed address data saved: {output_file_path}")
    }
  }, error = function(e) {
    logger::log_error("Failed to save parsed address data: {e$message}")
    stop(glue::glue("Error saving parsed addresses: {e$message}"))
  })
  
  if (verbose) {
    logger::log_info("Address parsing with postmastr completed successfully")
  }
  
  return(final_parsed_addresses)
}

# Main Workflow Execution ----
#' Execute Complete Geocoding Workflow
#'
#' This function orchestrates the complete geocoding workflow by calling all
#' processing functions in the correct sequence. It follows a structured folder
#' organization with input, intermediate, and output directories.
#'
#' @param subspecialty_filter Character string. Subspecialty code to filter for
#'   (e.g., "FPM" for Family Planning Medicine). Default is "FPM"
#' @param deduplicate_by_address_only Logical. Whether to deduplicate by unique
#'   addresses only (most cost-effective for geocoding). Default is TRUE
#' @param state_filter Character string. Optional state filter for testing.
#'   Default is NULL
#' @param output_filename Character string. Name of the final output file.
#'   Default is "final_parsed_addresses_with_acog_districts.csv"
#' @param input_obgyn_dataset Character string. Path to OBGYN provider dataset.
#'   Default uses INPUT_OBGYN_PROVIDER_DATASET constant
#' @param input_subspecialists_file Character string. Path to subspecialists file.
#'   Default uses INPUT_NPI_SUBSPECIALISTS_FILE constant  
#' @param input_city_dictionary Character string. Path to city dictionary file.
#'   Default uses CITY_DICTIONARY_PATH constant
#' @param verbose Logical. Whether to enable verbose logging. Default is TRUE
#'
#' @return A tibble containing the final parsed addresses with ACOG districts.
#'   All intermediate results are saved to INTERMEDIATE_DIR.
#'   Final output is saved to OUTPUT_DIR.
#'
#' @section Folder Structure:
#' The function uses the following folder organization:
#' \describe{
#'   \item{INPUT_DIR}{Reserved for input files (currently uses external sources)}
#'   \item{INTERMEDIATE_DIR}{All intermediate processing files}
#'   \item{OUTPUT_DIR}{Final output files}
#' }
#'
#' @examples
#' # Example 1: Basic usage with default folder structure
#' final_addresses <- execute_complete_geocoding_workflow(
#'   subspecialty_filter = "FPM",
#'   deduplicate_by_address_only = TRUE
#' )
#'
#' # Example 2: State filter with custom output filename
#' colorado_addresses <- execute_complete_geocoding_workflow(
#'   subspecialty_filter = "FPM",
#'   state_filter = "CO",
#'   output_filename = "colorado_fpm_specialists.csv"
#' )
#'
#' # Example 3: Different subspecialty with custom inputs
#' rei_addresses <- execute_complete_geocoding_workflow(
#'   subspecialty_filter = "REI",
#'   output_filename = "rei_specialists.csv",
#'   input_obgyn_dataset = file.path(INPUT_DIR, "custom_obgyn_data.csv")
#' )
#'
#' @export
execute_complete_geocoding_workflow <- function(subspecialty_filter = "FPM",
                                                deduplicate_by_address_only = TRUE,
                                                state_filter = NULL,
                                                output_filename = "final_parsed_addresses_with_acog_districts.csv",
                                                input_obgyn_dataset = INPUT_OBGYN_PROVIDER_DATASET,
                                                input_subspecialists_file = INPUT_NPI_SUBSPECIALISTS_FILE,
                                                input_city_dictionary = CITY_DICTIONARY_PATH,
                                                verbose = TRUE) {
  
  workflow_start_time <- Sys.time()
  
  # Construct full output path using the existing OUTPUT_DIR constant
  final_output_path <- file.path(OUTPUT_DIR, output_filename)
  
  if (verbose) {
    logger::log_info("=== STARTING COMPLETE GEOCODING WORKFLOW ===")
    logger::log_info("Workflow started at: {workflow_start_time}")
    logger::log_info("Subspecialty filter: {subspecialty_filter}")
    logger::log_info("Address-only deduplication: {deduplicate_by_address_only}")
    logger::log_info("State filter: {ifelse(is.null(state_filter), 'None', state_filter)}")
    logger::log_info("")
    logger::log_info("=== FOLDER STRUCTURE ===")
    logger::log_info("Input directory: {INPUT_DIR}")
    logger::log_info("Intermediate directory: {INTERMEDIATE_DIR}")
    logger::log_info("Output directory: {OUTPUT_DIR}")
    logger::log_info("Final output: {final_output_path}")
  }
  
  # Validate inputs and ensure directories exist
  assertthat::assert_that(
    is.character(output_filename),
    msg = "output_filename must be a character string"
  )
  
  # Create all directories using existing constants
  ensure_directory_exists(INPUT_DIR, verbose)
  ensure_directory_exists(INTERMEDIATE_DIR, verbose)
  ensure_directory_exists(OUTPUT_DIR, verbose)
  
  # Phase 1: Prepare combined addresses for geocoding
  if (verbose) {
    logger::log_info("=== PHASE 1: PREPARING ADDRESSES FOR GEOCODING ===")
  }
  
  tryCatch({
    prepared_geocoding_data <- prepare_combined_addresses_for_geocoding(
      input_file_path = input_obgyn_dataset,
      deduplicate_by_address_only = deduplicate_by_address_only,
      deduplicate_by_npi = TRUE,
      state_filter = state_filter,
      output_csv_path = file.path(INTERMEDIATE_DIR, "prepared_addresses_for_geocoding.csv"),
      verbose = verbose
    )
  }, error = function(e) {
    logger::log_error("PHASE 1 FAILED: {e$message}")
    stop(glue::glue("Address preparation failed: {e$message}"))
  })
  
  # Phase 2: Process subspecialist data
  if (verbose) {
    logger::log_info("=== PHASE 2: PROCESSING SUBSPECIALIST DATA ===")
  }
  
  tryCatch({
    subspecialist_results <- process_subspecialist_geocoding_data(
      geocoding_prepared_data = prepared_geocoding_data,
      subspecialists_file_path = input_subspecialists_file,
      subspecialty_filter = subspecialty_filter,
      output_subspecialist_path = file.path(INTERMEDIATE_DIR, "subspecialist_geocoding_data.csv"),
      output_filtered_path = file.path(INTERMEDIATE_DIR, glue::glue("{subspecialty_filter}_only_subspecialist_geocoding_data.csv")),
      verbose = verbose
    )
  }, error = function(e) {
    logger::log_error("PHASE 2 FAILED: {e$message}")
    stop(glue::glue("Subspecialist processing failed: {e$message}"))
  })
  
  # Phase 3: Clean addresses for matching
  if (verbose) {
    logger::log_info("=== PHASE 3: CLEANING ADDRESSES FOR MATCHING ===")
  }
  
  tryCatch({
    cleaned_addresses <- clean_addresses_for_matching(
      input_address_data = subspecialist_results$filtered_subspecialists,
      output_file_path = file.path(INTERMEDIATE_DIR, "cleaned_addresses_for_matching.csv"),
      verbose = verbose
    )
  }, error = function(e) {
    logger::log_error("PHASE 3 FAILED: {e$message}")
    stop(glue::glue("Address cleaning failed: {e$message}"))
  })
  
  # Phase 4: Parse addresses with postmastr
  if (verbose) {
    logger::log_info("=== PHASE 4: PARSING ADDRESSES WITH POSTMASTR ===")
  }
  
  tryCatch({
    # Save filtered data for postmastr input in INTERMEDIATE_DIR
    filtered_subspecialist_path <- file.path(INTERMEDIATE_DIR, glue::glue("{subspecialty_filter}_filtered_for_postmastr.csv"))
    readr::write_csv(subspecialist_results$filtered_subspecialists, filtered_subspecialist_path)
    
    final_parsed_addresses <- parse_addresses_with_postmastr(
      input_data_path = filtered_subspecialist_path,
      city_dictionary_path = input_city_dictionary,
      output_file_path = final_output_path,  # Save to OUTPUT_DIR
      verbose = verbose
    )
  }, error = function(e) {
    logger::log_error("PHASE 4 FAILED: {e$message}")
    stop(glue::glue("Address parsing with postmastr failed: {e$message}"))
  })
  
  # Generate and log workflow summary
  workflow_end_time <- Sys.time()
  workflow_duration <- difftime(workflow_end_time, workflow_start_time, units = "mins")
  
  # Calculate summary statistics
  initial_addresses <- nrow(prepared_geocoding_data)
  subspecialist_matches <- nrow(subspecialist_results$all_subspecialists)
  filtered_subspecialists <- nrow(subspecialist_results$filtered_subspecialists)
  final_record_count <- nrow(final_parsed_addresses)
  acog_coverage <- sum(!is.na(final_parsed_addresses$ACOG_District))
  acog_coverage_percent <- round((acog_coverage / final_record_count) * 100, 1)
  
  if (verbose) {
    logger::log_info("=== WORKFLOW COMPLETED SUCCESSFULLY ===")
    logger::log_info("Total processing time: {round(workflow_duration, 2)} minutes")
    logger::log_info("Final output file: {final_output_path}")
    logger::log_info("")
    logger::log_info("=== PROCESSING SUMMARY ===")
    logger::log_info("Initial addresses: {format_number_with_commas(initial_addresses)}")
    logger::log_info("Subspecialist matches: {format_number_with_commas(subspecialist_matches)}")
    logger::log_info("Filtered subspecialists: {format_number_with_commas(filtered_subspecialists)}")
    logger::log_info("Final parsed addresses: {format_number_with_commas(final_record_count)}")
    logger::log_info("Records with ACOG District: {format_number_with_commas(acog_coverage)} ({acog_coverage_percent}%)")
    
    # Log final subspecialty distribution
    if (!is.null(subspecialty_filter)) {
      final_specialty_count <- sum(final_parsed_addresses$sub1 == subspecialty_filter, na.rm = TRUE)
      logger::log_info("Records with {subspecialty_filter} subspecialty: {format_number_with_commas(final_specialty_count)}")
    }
    
    # Log subspecialty breakdown
    logger::log_info("")
    logger::log_info("=== SUBSPECIALTY BREAKDOWN ===")
    for (specialty_name in names(subspecialist_results$subspecialty_summary)) {
      count <- subspecialist_results$subspecialty_summary[specialty_name]
      logger::log_info("  {specialty_name}: {count} records")
    }
    
    # Log all file locations using existing constants
    logger::log_info("")
    logger::log_info("=== FILE LOCATIONS ===")
    logger::log_info("INPUT FILES:")
    logger::log_info("  OBGYN dataset: {input_obgyn_dataset}")
    logger::log_info("  Subspecialists: {input_subspecialists_file}")
    logger::log_info("  City dictionary: {input_city_dictionary}")
    logger::log_info("")
    logger::log_info("INTERMEDIATE FILES ({INTERMEDIATE_DIR}):")
    logger::log_info("  Prepared addresses: prepared_addresses_for_geocoding.csv")
    logger::log_info("  All subspecialists: subspecialist_geocoding_data.csv")
    logger::log_info("  Filtered subspecialists: {subspecialty_filter}_only_subspecialist_geocoding_data.csv")
    logger::log_info("  Cleaned addresses: cleaned_addresses_for_matching.csv")
    logger::log_info("  Postmastr input: {subspecialty_filter}_filtered_for_postmastr.csv")
    logger::log_info("")
    logger::log_info("OUTPUT FILES ({OUTPUT_DIR}):")
    logger::log_info("  Final dataset: {output_filename}")
  }
  
  # Return just the final dataset - clean and simple!
  return(final_parsed_addresses)
}

# run ----
workflow_results <- execute_complete_geocoding_workflow(
  subspecialty_filter = 'ONC',
  deduplicate_by_address_only = TRUE,
  state_filter = 'CO',
  output_filename = "data/04-geocode/output/final_parsed_addresses_with_acog_districts.csv",
  input_obgyn_dataset = INPUT_OBGYN_PROVIDER_DATASET,
  input_subspecialists_file = INPUT_NPI_SUBSPECIALISTS_FILE,
  input_city_dictionary = CITY_DICTIONARY_PATH,
  verbose = TRUE
)

#View(workflow_results)

# QC ----
# Load the subspecialists data
npi_subspecialists_data <- readr::read_rds(INPUT_NPI_SUBSPECIALISTS_FILE)

# Quick search for Guntapalli in the subspecialists data
npi_subspecialists_data %>%
  dplyr::filter(stringr::str_detect(stringr::str_to_upper(last_name), "GUNTUPALLI")) %>%
  dplyr::select(NPI, first_name, last_name, sub1, state, city)

# Also search in first_name in case the name is in a different field
npi_subspecialists_data %>%
  dplyr::filter(stringr::str_detect(stringr::str_to_upper(first_name), "SAKETH")) %>%
  dplyr::select(NPI, first_name, last_name, sub1, state, city)

# Check if he's in the original OBGYN dataset
obgyn_data <- readr::read_csv(INPUT_OBGYN_PROVIDER_DATASET, show_col_types = FALSE)
obgyn_data %>%
  dplyr::filter(stringr::str_detect(stringr::str_to_upper(plname), "GUNTUPALLI")) %>%
  dplyr::select(npi, plname, pfname, practice_address)

# Search by first name in OBGYN data too
obgyn_data %>%
  dplyr::filter(stringr::str_detect(stringr::str_to_upper(pfname), "SAKETH")) %>%
  dplyr::select(npi, plname, pfname, practice_address)

###
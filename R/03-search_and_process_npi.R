
# Setup and Configuration ----
source("R/01-setup.R")

# File Path Constants for NPI Processing Pipeline ----

# Directory Structure
INPUT_DIR <- "data/03-search_and_process_npi/input"
INTERMEDIATE_DIR <- "data/03-search_and_process_npi/intermediate"
OUTPUT_DIR <- "data/03-search_and_process_npi/output"

# Primary Input Files
GOBA_SUBSPECIALISTS_FILE <- file.path(INPUT_DIR, "GOBA_Scrape_subspecialists.csv")
SUBSPECIALISTS_ONLY_FILE <- "data/03-search_and_process_npi/subspecialists_only.csv"

# Step 1: Filter Missing NPIs
FILTERED_SUBSPECIALISTS_OUTPUT <- file.path(INTERMEDIATE_DIR, "step01_filtered_subspecialists_only.csv")

# Step 2: NPPES API Search Results
SEARCHED_NPI_OUTPUT <- file.path(INTERMEDIATE_DIR, "step02_searched_npi_numbers.csv")

# Step 4: Optional Taxonomy Search Data (from 02-search_taxonomy.R)
TAXONOMY_SEARCH_DATA_FILE <- "data/03-search_and_process_npi/all_taxonomy_search_data.csv"

# Final Output
FINAL_NPI_OUTPUT_FILE <- file.path(OUTPUT_DIR, "end_complete_npi_for_subspecialists.rds")

# External Dependencies (from other processing steps)
EXTERNAL_OBGYN_PROVIDER_DATASET <- "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv"
EXTERNAL_CITY_DICTIONARY_PATH <- "data/05-geocode-cleaning/city.rds"

# Geocoding Pipeline Input/Output (for downstream processing)
GEOCODING_INPUT_DIR <- "data/04-geocode/input"
GEOCODING_INTERMEDIATE_DIR <- "data/04-geocode/intermediate"
GEOCODING_OUTPUT_DIR <- "data/04-geocode/output"

# Data Validation Constants
VALID_CREDENTIALS <- c("MD", "DO")
GYNECOLOGY_TAXONOMY_PATTERN <- "Gyn"


#' Process National Provider Identifier (NPI) Database for Gynecologic Oncologists
#'
#' This script identifies gynecologic oncologists from multiple data sources:
#' 1. Existing subspecialists database with missing NPI numbers
#' 2. NPPES API search for missing providers
#' 3. Taxonomy-based search results
#' 
#' The final output combines all sources to create a comprehensive database
#' of gynecologic oncologists with complete NPI and location information.
#'
#' @importFrom readr read_csv write_csv write_rds
#' @importFrom dplyr rename filter mutate distinct select bind_rows coalesce
#' @importFrom tidyr unite
#' @importFrom stringr str_remove_all str_to_upper str_detect str_sub
#' @importFrom logger log_info log_threshold
#' @importFrom assertthat assert_that

# Set up logging
verbose <- TRUE
if (verbose) {
  logger::log_threshold(logger::INFO)
  logger::log_info("Starting NPI database processing for gynecologic oncologists")
}

# Create all necessary directories
ensure_directory_exists(INPUT_DIR, verbose)
ensure_directory_exists(INTERMEDIATE_DIR, verbose)
ensure_directory_exists(OUTPUT_DIR, verbose)

# Helper Functions ----
#' @noRd
ensure_directory_exists <- function(directory_path, verbose_logging = FALSE) {
  if (!dir.exists(directory_path)) {
    dir.create(directory_path, recursive = TRUE)
    if (verbose_logging) {
      logger::log_info("Created directory: {directory_path}")
    }
  } else if (verbose_logging) {
    logger::log_info("Directory already exists: {directory_path}")
  }
}

#' @noRd
validate_subspecialists_data <- function(subspecialists_data, verbose_logging = FALSE) {
  assertthat::assert_that(is.data.frame(subspecialists_data))
  assertthat::assert_that(nrow(subspecialists_data) > 0)
  assertthat::assert_that("NPI" %in% names(subspecialists_data))
  
  if (verbose_logging) {
    missing_npi_count <- sum(is.na(subspecialists_data$NPI))
    logger::log_info("Data validation passed - {nrow(subspecialists_data)} records with {missing_npi_count} missing NPIs")
  }
  
  return(subspecialists_data)
}

#' @noRd
filter_missing_npi_records <- function(subspecialists_data, verbose_logging = FALSE) {
  filtered_subspecialists <- subspecialists_data %>%
    dplyr::rename(
      first = first_name,
      last = last_name
    ) %>%
    dplyr::filter(is.na(NPI))
  
  assertthat::assert_that(nrow(filtered_subspecialists) > 0)
  
  if (verbose_logging) {
    logger::log_info("Filtered to {nrow(filtered_subspecialists)} subspecialists missing NPI numbers")
  }
  
  return(filtered_subspecialists)
}

#' @noRd
process_api_search_results <- function(api_search_results, valid_credentials, gynecology_pattern, verbose_logging = FALSE) {
  if (verbose_logging) {
    logger::log_info("Starting API search results processing with {nrow(api_search_results)} initial records")
  }
  
  # Step 1: Remove duplicate NPI numbers
  deduplicated_api_results <- api_search_results %>%
    dplyr::distinct(npi, .keep_all = TRUE)
  
  if (verbose_logging) {
    logger::log_info("After deduplication: {nrow(deduplicated_api_results)} unique NPI records")
  }
  
  # Step 2: Clean name and credential fields
  cleaned_api_results <- deduplicated_api_results %>%
    dplyr::mutate(
      dplyr::across(
        c(basic_first_name, basic_last_name, basic_credential),
        .fns = ~stringr::str_remove_all(., "[[\\p{P}][\\p{S}]]")
      )
    )
  
  # Step 3: Filter to physicians only
  physician_records <- cleaned_api_results %>%
    dplyr::mutate(basic_credential = stringr::str_to_upper(basic_credential)) %>%
    dplyr::filter(stringr::str_detect(basic_credential, "MD|DO")) %>%
    dplyr::mutate(basic_credential = stringr::str_sub(basic_credential, 1, 2)) %>%
    dplyr::filter(basic_credential %in% valid_credentials)
  
  if (verbose_logging) {
    credential_counts <- table(physician_records$basic_credential)
    md_count <- ifelse("MD" %in% names(credential_counts), credential_counts[["MD"]], 0)
    do_count <- ifelse("DO" %in% names(credential_counts), credential_counts[["DO"]], 0)
    logger::log_info("After physician filtering: {nrow(physician_records)} records (MD={md_count}, DO={do_count})")
  }
  
  # Step 4: Filter to gynecology specialists
  gynecology_specialists <- physician_records %>%
    dplyr::filter(stringr::str_detect(taxonomies_desc, fixed(gynecology_pattern, ignore_case = TRUE)))
  
  if (verbose_logging) {
    unique_taxonomies <- unique(gynecology_specialists$taxonomies_desc)
    logger::log_info("After gynecology filtering: {nrow(gynecology_specialists)} specialists")
    logger::log_info("Taxonomy codes found: {paste(unique_taxonomies, collapse = ', ')}")
  }
  
  # Step 5: Final deduplication and conversion
  validated_npi_records <- gynecology_specialists %>%
    dplyr::distinct(npi, .keep_all = TRUE) %>%
    dplyr::mutate(npi = as.numeric(npi))
  
  assertthat::assert_that(nrow(validated_npi_records) > 0)
  assertthat::assert_that(all(!is.na(validated_npi_records$npi)))
  
  if (verbose_logging) {
    final_credential_counts <- table(validated_npi_records$basic_credential)
    final_md_count <- ifelse("MD" %in% names(final_credential_counts), final_credential_counts[["MD"]], 0)
    final_do_count <- ifelse("DO" %in% names(final_credential_counts), final_credential_counts[["DO"]], 0)
    logger::log_info("Final validation: {nrow(validated_npi_records)} gynecologic oncologists (MD={final_md_count}, DO={final_do_count})")
  }
  
  return(validated_npi_records)
}

#' @noRd
merge_npi_with_original_database <- function(original_subspecialists, validated_npi_records, verbose_logging = FALSE) {
  # Prepare original database
  original_with_numeric_npi <- original_subspecialists %>%
    dplyr::mutate(NPI = as.numeric(NPI))
  
  assertthat::assert_that(class(original_with_numeric_npi$NPI) == class(validated_npi_records$npi))
  
  if (verbose_logging) {
    original_na_count <- sum(is.na(original_with_numeric_npi$NPI))
    logger::log_info("Starting merge with {original_na_count} missing NPIs in original database")
  }
  
  # Perform join
  joined_subspecialists <- original_with_numeric_npi %>%
    exploratory::left_join(
      validated_npi_records,
      by = c("first_name" = "basic_first_name", "last_name" = "basic_last_name"),
      ignorecase = TRUE
    )
  
  # Fill missing NPIs and clean up
  complete_npi_subspecialists <- joined_subspecialists %>%
    dplyr::mutate(NPI = dplyr::coalesce(NPI, npi)) %>%
    {
      api_columns_to_remove <- c(
        "npi", "basic_credential", "basic_sole_proprietor", "basic_gender",
        "basic_enumeration_date", "basic_last_updated", "basic_status",
        "basic_name_prefix", "taxonomies_code", "taxonomies_taxonomy_group",
        "taxonomies_desc", "taxonomies_state", "taxonomies_license",
        "taxonomies_primary", "basic_middle_name", "basic_name_suffix",
        "basic_certification_date"
      )
      
      existing_columns_to_remove <- intersect(api_columns_to_remove, names(.))
      
      if (verbose_logging && length(existing_columns_to_remove) > 0) {
        logger::log_info("Removing API columns: {paste(existing_columns_to_remove, collapse = ', ')}")
      }
      
      if (length(existing_columns_to_remove) > 0) {
        dplyr::select(., -all_of(existing_columns_to_remove))
      } else {
        .
      }
    }
  
  if (verbose_logging) {
    original_missing <- sum(is.na(original_with_numeric_npi$NPI))
    final_missing <- sum(is.na(complete_npi_subspecialists$NPI))
    logger::log_info("Merge complete: {original_missing - final_missing} new NPIs added, {final_missing} still missing")
  }
  
  return(complete_npi_subspecialists)
}

#' @noRd
combine_with_taxonomy_data <- function(current_subspecialists, taxonomy_search_data = NULL, verbose_logging = FALSE) {
  if (!is.null(taxonomy_search_data) && is.data.frame(taxonomy_search_data)) {
    if (verbose_logging) {
      logger::log_info("Combining with taxonomy search data ({nrow(taxonomy_search_data)} records)")
    }
    
    comprehensive_database <- current_subspecialists %>%
      exploratory::bind_rows(
        taxonomy_search_data,
        id_column_name = "ID",
        current_df_name = "subspecialists_only",
        force_data_type = TRUE
      ) %>%
      dplyr::distinct(NPI, .keep_all = TRUE)
    
    if (verbose_logging) {
      logger::log_info("After taxonomy combination: {nrow(comprehensive_database)} total records")
    }
    
    return(comprehensive_database)
  } else {
    if (verbose_logging) {
      logger::log_warn("No taxonomy search data available - proceeding with current database only")
    }
    return(current_subspecialists)
  }
}

#' @noRd
perform_final_data_cleaning <- function(comprehensive_subspecialists, verbose_logging = FALSE) {
  if (verbose_logging) {
    logger::log_info("Starting final data cleaning and standardization")
  }
  
  cleaned_final_database <- comprehensive_subspecialists %>%
    # Remove unnecessary columns
    {
      potential_columns_to_remove <- c(
        "ID", "userid", "startDate", "certStatus", "sub1startDate", "sub1certStatus",
        "honorrific_end", "Medical school namePhysicianCompare",
        "Graduation yearPhysicianCompare", "Organization legal namePhysicianCompare",
        "Number of Group Practice membersPhysicianCompare",
        "Professional accepts Medicare AssignmentPhysicianCompare",
        "search_term", "basic_sole_proprietor", "basic_enumeration_date",
        "taxonomies_primary", "addresses_address_1", "addresses_telephone_number",
        "npi", "name.x", "basic_first_name", "basic_last_name", "basic_middle_name",
        "basic_gender", "taxonomies_desc", "addresses_city", "addresses_state",
        "addresses_postal_code", "full_name"
      )
      
      existing_columns_to_remove <- intersect(potential_columns_to_remove, names(.))
      
      if (verbose_logging && length(existing_columns_to_remove) > 0) {
        logger::log_info("Removing {length(existing_columns_to_remove)} unnecessary columns")
      }
      
      if (length(existing_columns_to_remove) > 0) {
        dplyr::select(., -all_of(existing_columns_to_remove))
      } else {
        .
      }
    } %>%
    
    # Add unique identifier
    dplyr::mutate(row_number = dplyr::row_number()) %>%
    
    # Standardize ZIP codes
    {
      if ("Zip CodePhysicianCompare" %in% names(.)) {
        dplyr::mutate(.,
                      zip = stringr::str_sub(`Zip CodePhysicianCompare`, 1, 5),
                      .after = "Zip CodePhysicianCompare"
        )
      } else {
        if (verbose_logging) {
          logger::log_warn("ZIP code column not found - creating empty zip column")
        }
        dplyr::mutate(., zip = "")
      }
    } %>%
    
    # Filter complete location information
    dplyr::filter(!is.na(state) & !is.na(city)) %>%
    
    # Handle missing ZIP codes and create address
    dplyr::mutate(zip = ifelse(is.na(zip) | zip == "", "", zip)) %>%
    tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
    
    # Remove duplicate addresses
    dplyr::distinct(address, .keep_all = TRUE)
  
  # Final validation
  assertthat::assert_that(nrow(cleaned_final_database) > 0)
  assertthat::assert_that(all(!is.na(cleaned_final_database$address)))
  
  if (verbose_logging) {
    logger::log_info("Final database statistics:")
    logger::log_info("- Total gynecologic oncologists: {nrow(cleaned_final_database)}")
    logger::log_info("- Unique addresses: {length(unique(cleaned_final_database$address))}")
    logger::log_info("- States represented: {length(unique(cleaned_final_database$state))}")
    logger::log_info("- Complete NPI numbers: {sum(!is.na(cleaned_final_database$NPI))}")
  }
  
  return(cleaned_final_database)
}

# Process National Provider Identifier (NPI) Database for Gynecologic Oncologists ----
#'
#' This function processes and combines multiple data sources to create a comprehensive 
#' database of gynecologic oncologists with complete NPI and location information. 
#' The process includes identifying subspecialists with missing NPI numbers, searching 
#' the NPPES API for missing providers, validating and filtering results, and performing 
#' final data cleaning and standardization.
#'
#' @param input_subspecialists_file_path Character string. Path to the input CSV file 
#'   containing subspecialists data with potential missing NPI numbers. Must contain 
#'   columns: NPI, first_name, last_name, state, city.
#' @param output_directory_path Character string. Directory path where all output files 
#'   will be saved. Directory will be created if it doesn't exist.
#' @param taxonomy_search_data_frame Data frame or NULL. Optional taxonomy search data 
#'   to combine with subspecialists data for comprehensive coverage. Default is NULL.
#' @param verbose_logging Logical. If TRUE, detailed logging information will be printed 
#'   to console throughout the processing. Default is TRUE.
#' @param valid_physician_credentials Character vector. Valid physician credentials to 
#'   filter API results. Default is c("MD", "DO").
#' @param gynecology_taxonomy_pattern Character string. Pattern to identify gynecology-related 
#'   specialties in taxonomy descriptions. Default is "Gyn".
#'
#' @return Data frame containing the final comprehensive gynecologic oncologists database 
#'   with complete NPI numbers, standardized addresses, and cleaned data.
#'
#' @examples
#' # Example 1: Basic usage with all default parameters
#' gynecologic_oncologists_database <- process_npi_for_gynecologic_oncologists(
#'   input_subspecialists_file_path = "data/input/subspecialists.csv",
#'   output_directory_path = "data/output/npi_processing",
#'   taxonomy_search_data_frame = NULL,
#'   verbose_logging = TRUE,
#'   valid_physician_credentials = c("MD", "DO"),
#'   gynecology_taxonomy_pattern = "Gyn"
#' )
#'
#' # Example 2: Including taxonomy search data with custom credentials
#' comprehensive_database <- process_npi_for_gynecologic_oncologists(
#'   input_subspecialists_file_path = "data/GOBA_subspecialists.csv",
#'   output_directory_path = "results/comprehensive_npi_analysis",
#'   taxonomy_search_data_frame = existing_taxonomy_search_results,
#'   verbose_logging = FALSE,
#'   valid_physician_credentials = c("MD", "DO", "MBBS"),
#'   gynecology_taxonomy_pattern = "Gynecologic"
#' )
#'
#' # Example 3: Silent processing with minimal taxonomy pattern matching
#' final_oncologists_dataset <- process_npi_for_gynecologic_oncologists(
#'   input_subspecialists_file_path = "/path/to/subspecialists_input.csv",
#'   output_directory_path = "/path/to/npi_output_directory",
#'   taxonomy_search_data_frame = combined_taxonomy_results,
#'   verbose_logging = FALSE,
#'   valid_physician_credentials = c("MD", "DO"),
#'   gynecology_taxonomy_pattern = "Gyn"
#' )
#'
#' @importFrom readr read_csv write_csv write_rds
#' @importFrom dplyr rename filter mutate distinct select bind_rows coalesce across 
#'   row_number left_join
#' @importFrom tidyr unite
#' @importFrom stringr str_remove_all str_to_upper str_detect str_sub
#' @importFrom logger log_info log_threshold log_warn INFO
#' @importFrom assertthat assert_that
#' @importFrom exploratory left_join bind_rows
#' 
#' @export
process_npi_for_gynecologic_oncologists <- function(input_subspecialists_file_path,
                                                    output_directory_path,
                                                    taxonomy_search_data_frame = NULL,
                                                    verbose_logging = TRUE,
                                                    valid_physician_credentials = c("MD", "DO"),
                                                    gynecology_taxonomy_pattern = "Gyn") {
  
  # Input validation
  assertthat::assert_that(is.character(input_subspecialists_file_path))
  assertthat::assert_that(is.character(output_directory_path))
  assertthat::assert_that(is.logical(verbose_logging))
  assertthat::assert_that(is.character(valid_physician_credentials))
  assertthat::assert_that(is.character(gynecology_taxonomy_pattern))
  assertthat::assert_that(file.exists(input_subspecialists_file_path))
  
  # Set up logging
  if (verbose_logging) {
    logger::log_threshold(logger::INFO)
    logger::log_info("Starting NPI database processing for gynecologic oncologists")
    logger::log_info("Input file: {input_subspecialists_file_path}")
    logger::log_info("Output directory: {output_directory_path}")
  }
  
  # Define file paths
  intermediate_directory_path <- file.path(output_directory_path, "intermediate")
  filtered_subspecialists_output_path <- file.path(intermediate_directory_path, "filtered_subspecialists.csv")
  searched_npi_output_path <- file.path(intermediate_directory_path, "searched_npi_numbers.csv")
  final_npi_output_file_path <- file.path(output_directory_path, "final_gynecologic_oncologists.rds")
  
  # Create directories
  ensure_directory_exists(output_directory_path, verbose_logging)
  ensure_directory_exists(intermediate_directory_path, verbose_logging)
  
  # STEP 1: Read and validate input data ----
  if (verbose_logging) {
    logger::log_info("Step 1: Reading and validating subspecialists database")
  }
  
  original_subspecialists_database <- readr::read_csv(input_subspecialists_file_path, show_col_types = FALSE)
  validated_subspecialists <- validate_subspecialists_data(original_subspecialists_database, verbose_logging)
  
  # STEP 2: Filter subspecialists with missing NPI numbers ----
  if (verbose_logging) {
    logger::log_info("Step 2: Identifying subspecialists with missing NPI numbers")
  }
  
  subspecialists_missing_npi <- filter_missing_npi_records(validated_subspecialists, verbose_logging)
  
  # Save filtered data
  subspecialists_missing_npi %>%
    readr::write_csv(filtered_subspecialists_output_path)
  
  if (verbose_logging) {
    logger::log_info("Saved filtered subspecialists to: {filtered_subspecialists_output_path}")
  }
  
  # STEP 3: Search NPPES API (assuming search_and_process_npi function exists) ----
  if (verbose_logging) {
    logger::log_info("Step 3: Searching NPPES API for missing NPI numbers")
  }
  
  # Note: This assumes search_and_process_npi function exists in the environment
  api_search_results <- search_and_process_npi(filtered_subspecialists_output_path)
  
  assertthat::assert_that(is.data.frame(api_search_results))
  
  if (verbose_logging) {
    logger::log_info("API search returned {nrow(api_search_results)} potential matches")
  }
  
  # STEP 4: Process and validate API results ----
  if (verbose_logging) {
    logger::log_info("Step 4: Processing and validating API search results")
  }
  
  validated_npi_records <- process_api_search_results(
    api_search_results, 
    valid_physician_credentials, 
    gynecology_taxonomy_pattern, 
    verbose_logging
  )
  
  # Save validated results
  validated_npi_records %>%
    readr::write_csv(searched_npi_output_path)
  
  if (verbose_logging) {
    logger::log_info("Saved validated NPI search results to: {searched_npi_output_path}")
  }
  
  # STEP 5: Merge with original database ----
  if (verbose_logging) {
    logger::log_info("Step 5: Merging new NPI numbers with original database")
  }
  
  subspecialists_with_complete_npi <- merge_npi_with_original_database(
    validated_subspecialists, 
    validated_npi_records, 
    verbose_logging
  )
  
  # STEP 6: Combine with taxonomy data if available ----
  if (verbose_logging) {
    logger::log_info("Step 6: Combining with taxonomy search data (if available)")
  }
  
  comprehensive_subspecialists_database <- combine_with_taxonomy_data(
    subspecialists_with_complete_npi, 
    taxonomy_search_data_frame, 
    verbose_logging
  )
  
  # STEP 7: Final data cleaning and standardization ----
  if (verbose_logging) {
    logger::log_info("Step 7: Final data cleaning and standardization")
  }
  
  final_gynecologic_oncologists_database <- perform_final_data_cleaning(
    comprehensive_subspecialists_database, 
    verbose_logging
  )
  
  # Save final database
  final_gynecologic_oncologists_database %>%
    readr::write_rds(final_npi_output_file_path)
  
  if (verbose_logging) {
    logger::log_info("Final database saved to: {final_npi_output_file_path}")
    logger::log_info("NPI processing complete!")
  }
  
  return(final_gynecologic_oncologists_database)
}

# run ----
final_database <- process_npi_for_gynecologic_oncologists(
  input_subspecialists_file_path = "data/03-search_and_process_npi/input/GOBA_Scrape_subspecialists.csv",
  output_directory_path = "data/03-search_and_process_npi/output"
)


# # 
# # STEP 1: IDENTIFY SUBSPECIALISTS WITH MISSING NPI NUMBERS ----
# #
# 
# if (verbose) {
#   logger::log_info("Step 1: Reading subspecialists database and identifying missing NPI numbers")
# }
# 
# # Read the original subspecialists database
# original_subspecialists_database <- readr::read_csv(GOBA_SUBSPECIALISTS_FILE)
# 
# # Validate input data
# assertthat::assert_that(is.data.frame(original_subspecialists_database))
# assertthat::assert_that(nrow(original_subspecialists_database) > 0)
# assertthat::assert_that("NPI" %in% names(original_subspecialists_database))
# 
# if (verbose) {
#   logger::log_info("Original database contains {nrow(original_subspecialists_database)} subspecialists")
#   logger::log_info("Missing NPI count: {sum(is.na(original_subspecialists_database$NPI))}")
# }
# 
# # Filter to subspecialists without NPI numbers for API search
# subspecialists_missing_npi <- original_subspecialists_database %>%
#   dplyr::rename(
#     first = first_name,
#     last = last_name
#   ) %>%
#   dplyr::filter(is.na(NPI))
# 
# # Validate filtered data
# assertthat::assert_that(nrow(subspecialists_missing_npi) > 0)
# 
# if (verbose) {
#   logger::log_info("Found {nrow(subspecialists_missing_npi)} subspecialists without NPI numbers")
# }
# 
# # Save filtered data for API processing
# subspecialists_missing_npi %>%
#   readr::write_csv(FILTERED_SUBSPECIALISTS_OUTPUT)
# 
# if (verbose) {
#   logger::log_info("Saved filtered subspecialists to: {FILTERED_SUBSPECIALISTS_OUTPUT}")
# }
# 
# #
# # STEP 2: SEARCH NPPES API FOR MISSING NPI NUMBERS OF GOBA DATA ----
# #
# 
# if (verbose) {
#   logger::log_info("Step 2: Searching NPPES API for missing NPI numbers")
# }
# 
# # Search NPPES API using custom function
# api_search_results <- search_and_process_npi(FILTERED_SUBSPECIALISTS_OUTPUT)
# 
# # Validate API results
# assertthat::assert_that(is.data.frame(api_search_results))
# 
# if (verbose) {
#   logger::log_info("API search returned {nrow(api_search_results)} potential matches")
# }
# 
# # Process and filter API search results with detailed validation
# if (verbose) {
#   logger::log_info("Processing API search results through validation pipeline")
# }
# 
# # Step 2a: Remove duplicate NPI numbers from API results
# api_results_deduplicated <- api_search_results %>%
#   dplyr::distinct(npi, .keep_all = TRUE)
# 
# if (verbose) {
#   logger::log_info("After deduplication: {nrow(api_results_deduplicated)} unique NPI records")
# }
# 
# # Step 2b: Clean name and credential fields by removing punctuation
# api_results_cleaned <- api_results_deduplicated %>%
#   dplyr::mutate(
#     dplyr::across(
#       c(basic_first_name, basic_last_name, basic_credential),
#       .fns = ~stringr::str_remove_all(., "[[\\p{P}][\\p{S}]]")
#     )
#   )
# 
# if (verbose) {
#   logger::log_info("Name and credential fields cleaned of punctuation")
# }
# 
# # Step 2c: Standardize credentials and filter to physicians only
# physician_records_only <- api_results_cleaned %>%
#   # Convert credentials to uppercase for standardization
#   dplyr::mutate(basic_credential = stringr::str_to_upper(basic_credential)) %>%
#   
#   # Initial filter for MD/DO pattern
#   dplyr::filter(stringr::str_detect(basic_credential, "MD|DO")) %>%
#   
#   # Extract first 2 characters for precise matching
#   dplyr::mutate(basic_credential = stringr::str_sub(basic_credential, 1, 2)) %>%
#   
#   # Keep only exact MD or DO matches
#   dplyr::filter(basic_credential %in% VALID_CREDENTIALS)
# 
# if (verbose) {
#   logger::log_info("After physician credential filtering: {nrow(physician_records_only)} records")
#   credential_counts <- table(physician_records_only$basic_credential)
#   md_count <- ifelse("MD" %in% names(credential_counts), credential_counts[["MD"]], 0)
#   do_count <- ifelse("DO" %in% names(credential_counts), credential_counts[["DO"]], 0)
#   logger::log_info("Credential breakdown: MD={md_count}, DO={do_count}")
# }
# 
# # Step 2d: Filter to gynecology-related specialties
# gynecology_specialists <- physician_records_only %>%
#   dplyr::filter(stringr::str_detect(taxonomies_desc, fixed(GYNECOLOGY_TAXONOMY_PATTERN, ignore_case = TRUE)))
# 
# if (verbose) {
#   logger::log_info("After gynecology taxonomy filtering: {nrow(gynecology_specialists)} specialists")
#   unique_taxonomies <- unique(gynecology_specialists$taxonomies_desc)
#   logger::log_info("Taxonomy codes found: {paste(unique_taxonomies, collapse = ', ')}")
# }
# 
# # Step 2e: Final deduplication and data type conversion
# validated_npi_numbers <- gynecology_specialists %>%
#   # Final deduplication by NPI
#   dplyr::distinct(npi, .keep_all = TRUE) %>%
#   
#   # Convert NPI to numeric for database consistency
#   dplyr::mutate(npi = as.numeric(npi))
# 
# # Validate processed results
# assertthat::assert_that(nrow(validated_npi_numbers) > 0)
# assertthat::assert_that(all(!is.na(validated_npi_numbers$npi)))
# 
# if (verbose) {
#   logger::log_info("After validation: {nrow(validated_npi_numbers)} gynecologic oncologists found")
#   credential_counts <- table(validated_npi_numbers$basic_credential)
#   md_count <- ifelse("MD" %in% names(credential_counts), credential_counts[["MD"]], 0)
#   do_count <- ifelse("DO" %in% names(credential_counts), credential_counts[["DO"]], 0)
#   logger::log_info("Credential distribution: MD={md_count}, DO={do_count}")
# }
# 
# # Save validated NPI search results
# validated_npi_numbers %>%
#   readr::write_csv(SEARCHED_NPI_OUTPUT)
# 
# if (verbose) {
#   logger::log_info("Saved validated NPI search results to: {SEARCHED_NPI_OUTPUT}")
# }
# 
# #
# # STEP 3: MERGE NPI NUMBERS WITH ORIGINAL DATABASE ----
# #
# 
# if (verbose) {
#   logger::log_info("Step 3: Merging new NPI numbers with original subspecialists database")
# }
# 
# # Prepare original database for merging (keeping original column names)
# original_subspecialists_numeric_npi <- original_subspecialists_database %>%
#   dplyr::mutate(NPI = as.numeric(NPI))
# 
# # Verify data types match for joining
# assertthat::assert_that(class(original_subspecialists_numeric_npi$NPI) == class(validated_npi_numbers$npi))
# 
# if (verbose) {
#   logger::log_info("Data types validated for NPI joining")
# }
# 
# # Merge original database with newly found NPI numbers
# if (verbose) {
#   logger::log_info("Starting left join between original database and validated NPI numbers")
#   logger::log_info("Original database columns: {paste(names(original_subspecialists_numeric_npi), collapse = ', ')}")
#   logger::log_info("Validated NPI columns: {paste(names(validated_npi_numbers), collapse = ', ')}")
# }
# 
# # Perform the join
# subspecialists_after_join <- original_subspecialists_numeric_npi %>%
#   # Left join to preserve all original records
#   exploratory::left_join(
#     validated_npi_numbers,
#     by = c("first_name" = "basic_first_name", "last_name" = "basic_last_name"),
#     ignorecase = TRUE
#   )
# 
# if (verbose) {
#   logger::log_info("After join: {nrow(subspecialists_after_join)} records")
#   logger::log_info("Columns after join: {paste(names(subspecialists_after_join), collapse = ', ')}")
#   
#   # Check how many NPI numbers were filled
#   original_na_count <- sum(is.na(original_subspecialists_numeric_npi$NPI))
#   joined_na_count <- sum(is.na(subspecialists_after_join$NPI))
#   logger::log_info("NPI filling check: Original NA count = {original_na_count}")
# }
# 
# # Fill missing NPI numbers with newly found ones and clean up columns
# subspecialists_with_complete_npi <- subspecialists_after_join %>%
#   # Fill missing NPI numbers with newly found ones
#   dplyr::mutate(NPI = dplyr::coalesce(NPI, npi)) %>%
#   
#   # Identify which API-added columns actually exist to remove
#   {
#     api_columns_to_remove <- c(
#       "npi", "basic_credential", "basic_sole_proprietor", "basic_gender",
#       "basic_enumeration_date", "basic_last_updated", "basic_status",
#       "basic_name_prefix", "taxonomies_code", "taxonomies_taxonomy_group",
#       "taxonomies_desc", "taxonomies_state", "taxonomies_license",
#       "taxonomies_primary", "basic_middle_name", "basic_name_suffix",
#       "basic_certification_date"
#     )
#     
#     # Only remove columns that actually exist
#     existing_columns_to_remove <- intersect(api_columns_to_remove, names(.))
#     
#     if (verbose) {
#       logger::log_info("API columns to remove: {paste(existing_columns_to_remove, collapse = ', ')}")
#     }
#     
#     # Remove only existing columns
#     if (length(existing_columns_to_remove) > 0) {
#       dplyr::select(., -all_of(existing_columns_to_remove))
#     } else {
#       . # Return unchanged if no columns to remove
#     }
#   }
# 
# if (verbose) {
#   original_missing <- sum(is.na(original_subspecialists_numeric_npi$NPI))
#   final_missing <- sum(is.na(subspecialists_with_complete_npi$NPI))
#   logger::log_info("NPI completion: {original_missing - final_missing} new NPI numbers added")
#   logger::log_info("Remaining missing NPIs: {final_missing}")
# }
# 
# #
# # STEP 4: COMBINE WITH TAXONOMY SEARCH DATA (IF AVAILABLE) ----
# #
# 
# if (verbose) {
#   logger::log_info("Step 4: Checking for taxonomy search data to combine")
# }
# 
# # Check if taxonomy search data exists from previous processing steps
# if (exists("all_taxonomy_search_data") && is.data.frame(all_taxonomy_search_data)) {
#   
#   if (verbose) {
#     logger::log_info("Taxonomy search data found with {nrow(all_taxonomy_search_data)} records")
#     logger::log_info("Combining with taxonomy search data for comprehensive coverage")
#   }
#   
#   # Combine with taxonomy search results to include younger subspecialists
#   comprehensive_subspecialists_database <- subspecialists_with_complete_npi %>%
#     exploratory::bind_rows(
#       all_taxonomy_search_data,
#       id_column_name = "ID",
#       current_df_name = "subspecialists_only",
#       force_data_type = TRUE
#     ) %>%
#     
#     # Remove duplicates based on NPI number
#     dplyr::distinct(NPI, .keep_all = TRUE)
#   
#   if (verbose) {
#     logger::log_info("After combining with taxonomy data: {nrow(comprehensive_subspecialists_database)} total records")
#   }
#   
# } else {
#   
#   if (verbose) {
#     logger::log_warn("Taxonomy search data not found - skipping taxonomy combination step")
#     logger::log_info("To include taxonomy search data, run R/02-search_taxonomy.R first")
#     logger::log_info("Proceeding with current subspecialists database only")
#   }
#   
#   # Use the current database without taxonomy additions
#   comprehensive_subspecialists_database <- subspecialists_with_complete_npi
#   
#   if (verbose) {
#     logger::log_info("Current database contains: {nrow(comprehensive_subspecialists_database)} records")
#   }
# }
# 
# #
# # STEP 5: FINAL DATA CLEANING AND STANDARDIZATION ----
# #
# 
# if (verbose) {
#   logger::log_info("Step 5: Final data cleaning and address standardization")
# }
# 
# final_gynecologic_oncologists_database <- comprehensive_subspecialists_database %>%
#   # Remove unnecessary columns from various data sources (only if they exist)
#   {
#     # Define all potential columns to remove from various processing steps
#     potential_columns_to_remove <- c(
#       "ID", "userid", "startDate", "certStatus", "sub1startDate", "sub1certStatus",
#       "honorrific_end", "Medical school namePhysicianCompare",
#       "Graduation yearPhysicianCompare", "Organization legal namePhysicianCompare",
#       "Number of Group Practice membersPhysicianCompare",
#       "Professional accepts Medicare AssignmentPhysicianCompare",
#       "search_term", "basic_sole_proprietor", "basic_enumeration_date",
#       "taxonomies_primary", "addresses_address_1", "addresses_telephone_number",
#       "npi", "name.x", "basic_first_name", "basic_last_name", "basic_middle_name",
#       "basic_gender", "taxonomies_desc", "addresses_city", "addresses_state",
#       "addresses_postal_code", "full_name"
#     )
#     
#     # Only remove columns that actually exist
#     existing_columns_to_remove <- intersect(potential_columns_to_remove, names(.))
#     
#     if (verbose) {
#       logger::log_info("Available columns to clean: {paste(existing_columns_to_remove, collapse = ', ')}")
#       logger::log_info("Current column count: {ncol(.)}")
#     }
#     
#     # Remove only existing columns
#     if (length(existing_columns_to_remove) > 0) {
#       dplyr::select(., -all_of(existing_columns_to_remove))
#     } else {
#       if (verbose) {
#         logger::log_info("No unnecessary columns found to remove")
#       }
#       . # Return unchanged if no columns to remove
#     }
#   } %>%
#   
#   # Add unique row identifier
#   dplyr::mutate(row_number = dplyr::row_number()) %>%
#   
#   # Standardize ZIP codes to 5 digits (if ZIP code column exists)
#   {
#     if ("Zip CodePhysicianCompare" %in% names(.)) {
#       dplyr::mutate(.,
#                     zip = stringr::str_sub(`Zip CodePhysicianCompare`, 1, 5),
#                     .after = "Zip CodePhysicianCompare"
#       )
#     } else {
#       if (verbose) {
#         logger::log_warn("Zip CodePhysicianCompare column not found - skipping ZIP standardization")
#       }
#       # Create empty zip column for consistency
#       dplyr::mutate(., zip = "")
#     }
#   } %>%
#   
#   # Filter to records with complete location information
#   dplyr::filter(!is.na(state) & !is.na(city)) %>%
#   
#   # Handle missing ZIP codes
#   dplyr::mutate(zip = ifelse(is.na(zip) | zip == "", "", zip)) %>%
#   
#   # Create standardized address field for geocoding
#   tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
#   
#   # Remove duplicate addresses (same practice location)
#   dplyr::distinct(address, .keep_all = TRUE)
# 
# # Final validation
# assertthat::assert_that(nrow(final_gynecologic_oncologists_database) > 0)
# assertthat::assert_that(all(!is.na(final_gynecologic_oncologists_database$address)))
# 
# if (verbose) {
#   logger::log_info("Final database statistics:")
#   logger::log_info("- Total gynecologic oncologists: {nrow(final_gynecologic_oncologists_database)}")
#   logger::log_info("- Unique addresses: {length(unique(final_gynecologic_oncologists_database$address))}")
#   logger::log_info("- States represented: {length(unique(final_gynecologic_oncologists_database$state))}")
#   logger::log_info("- Complete NPI numbers: {sum(!is.na(final_gynecologic_oncologists_database$NPI))}")
# }
# 
# # Save final comprehensive database
# final_gynecologic_oncologists_database %>%
#   readr::write_rds(FINAL_NPI_OUTPUT_FILE)
# 
# if (verbose) {
#   logger::log_info("Final gynecologic oncologists database saved to: {FINAL_NPI_OUTPUT_FILE}")
#   logger::log_info("NPI processing complete!")
# }


#' ##' PROCESSING PIPELINE FLOWCHART
#' #' ================================================================================================
#' #'
#' #'  ┌─────────────────────────────────┐
#' #'  │ GOBA_Scrape_subspecialists.csv  │  ← Board-certified subspecialists
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'  ┌─────────────────────────────────┐
#' #'  │ STEP 1: Filter Missing NPIs     │  → filtered_subspecialists_only.csv
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'  ┌─────────────────────────────────┐
#' #'  │ STEP 2: NPPES API Search        │  ← search_and_process_npi()
#' #'  │  • Remove duplicates            │
#' #'  │  • Clean names/credentials      │
#' #'  │  • Filter MD/DO only            │
#' #'  │  • Filter Gyn taxonomy          │
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'               → searched_npi_numbers.csv
#' #'               ▼
#' #'  ┌─────────────────────────────────┐
#' #'  │ STEP 3: Merge NPI Data          │  ← Left join on names
#' #'  │  • Coalesce NPIs                │
#' #'  │  • Remove API columns           │
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'  ┌─────────────────────────────────┐     ┌──────────────────────────┐
#' #'  │ STEP 4: Combine Sources         │ ← ← │ all_taxonomy_search_data │ (optional)
#' #'  │  • bind_rows()                  │     └──────────────────────────┘
#' #'  │  • distinct(NPI)                │
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'  ┌─────────────────────────────────┐
#' #'  │ STEP 5: Final Cleaning          │
#' #'  │  • Remove unnecessary columns   │
#' #'  │  • Standardize ZIP codes        │
#' #'  │  • Filter complete locations    │
#' #'  │  • Create address field         │
#' #'  │  • Remove duplicate addresses   │
#' #'  └────────────┬────────────────────┘
#' #'               ▼
#' #'  ┌─────────────────────────────────┐
#' #'  │ end_complete_npi_for_           │  ← FINAL OUTPUT (RDS)
#' #'  │ subspecialists.rds              │
#' #'  └─────────────────────────────────┘
#' #'
#' #' ================================================================================================
#' # File: 03-search_and_process_npi.R
#' # Title: Search and Process Missing NPI Numbers for Subspecialists
#' #
#' # Purpose:
#' #   This script identifies gynecologic subspecialists missing NPI numbers
#' #   and retrieves them using the NPPES NPI Registry API. It merges new
#' #   NPI data with the original file, and optionally incorporates results
#' #   from taxonomy-based searches to produce a complete, deduplicated list.
#' #
#' # Workflow Overview:
#' #   1. Load the GOBA subspecialists CSV file
#' #   2. Filter to those with missing NPI numbers
#' #   3. Query the NPPES API using `search_and_process_npi()` to find likely matches
#' #   4. Clean and filter to MD/DO physicians with gynecology-related taxonomy
#' #   5. Join NPI data back into original dataset
#' #   6. Optionally combine with results from `02-search_taxonomy.R`
#' #   7. Clean addresses, filter incomplete entries, and export as a clean RDS
#' #
#' # data/
#' #   └── 03-search_and_process_npi/
#' #   ├── input/
#' #   ├── intermediate/
#' #   ├── output/
#' #   └── logs/ (optional)
#' #
#' # Input Files:
#' #   - "data/03-search_and_process_npi/GOBA_Scrape_subspecialists_only.csv"
#' #       ▸ File created from manual or semi-automated scraping of subspecialists
#' #         from the ABOG board certification verification tool.
#' #         Contains board-certified subspecialists with first/last name, state,
#' #         and subspecialty designation (e.g., MFM, ONC, REI).
#' #
#' #   - "data/03-search_and_process_npi/filtered_subspecialists_only.csv"
#' #       ▸ Generated by this script in Step 1.
#' #         Created by filtering `GOBA_Scrape_subspecialists_only.csv` to rows
#' #         where NPI is NA.
#' #
#' #   - "data/03-search_and_process_npi/all_taxonomy_search_data.csv" (optional)
#' #       ▸ Output from "02-search_taxonomy.R" script.
#' #         Contains additional subspecialists identified using NPPES taxonomy search
#' #         rather than board certification. Helps capture early-career physicians
#' #         who haven’t yet completed certification.
#' #
#' # Output Files:
#' #   - "data/03-search_and_process_npi/searched_npi_numbers.csv"
#' #       ▸ API results for missing NPIs. Filtered to MD/DO with gynecology taxonomy.
#' #
#' #   - "data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds"
#' #       ▸ Final dataset with all validated NPI numbers merged into the original list.
#' #         Combines board-certified and taxonomy-based subspecialists into a single file.
#' #
#' # Requirements:
#' #   - `search_and_process_npi()` function must exist and access the NPPES API
#' #   - If combining with taxonomy-based search results, `all_taxonomy_search_data`
#' #     must exist in the environment or be read in manually
#' #
#' # Dependencies:
#' #   - Packages: readr, dplyr, tidyr, stringr, logger, assertthat, exploratory
#' #
#' # Notes:
#' #   - Filters strictly to MD or DO credentials only
#' #   - Includes fuzzy join back to original file (case-insensitive name matching)
#' #   - Removes many temporary API-derived columns for a clean final export
#' #
#' # Author: Tyler Muffly
#' # Date: 2025-07-27
#' #
#' 
#' 
#' #
#' # tyler::search_and_process_npi
#' # Filter the subspecialists_only.csv file/goba file to those without NPI numbers.  Then send those names into the search_and_process_npi to get NPI numbers.  
#' 
#' # Setup ----
#' source("R/01-setup.R")
#' #
#' 
#' 
#' #' Process National Provider Identifier (NPI) Database for Gynecologic Oncologists
#' #'
#' #' This script identifies gynecologic oncologists from multiple data sources:
#' #' 1. Existing subspecialists database with missing NPI numbers
#' #' 2. NPPES API search for missing providers
#' #' 3. Taxonomy-based search results
#' #' 
#' #' The final output combines all sources to create a comprehensive database
#' #' of gynecologic oncologists with complete NPI and location information.
#' #'
#' #' @importFrom readr read_csv write_csv write_rds
#' #' @importFrom dplyr rename filter mutate distinct select bind_rows coalesce
#' #' @importFrom tidyr unite
#' #' @importFrom stringr str_remove_all str_to_upper str_detect str_sub
#' #' @importFrom logger log_info log_threshold
#' #' @importFrom assertthat assert_that
#' 
#' # Set up logging
#' verbose <- TRUE
#' if (verbose) {
#'   logger::log_threshold(logger::INFO)
#'   logger::log_info("Starting NPI database processing for gynecologic oncologists")
#' }
#' 
#' # Define file paths as constants for easier maintenance ----
#' GOBA <- "data/03-search_and_process_npi/input/GOBA_Scrape_subspecialists.csv"
#' FILTERED_SUBSPECIALISTS_OUTPUT <- "data/03-search_and_process_npi/step01_intermediate/filtered_subspecialists_only.csv"
#' SEARCHED_NPI_OUTPUT <- "data/03-search_and_process_npi/intermediate/step02_searched_npi_numbers.csv"
#' FINAL_OUTPUT_FILE <- "data/03-search_and_process_npi/output/end_complete_npi_for_subspecialists.rds"
#' 
#' # Define credential patterns for validation ----
#' VALID_CREDENTIALS <- c("MD", "DO")
#' GYNECOLOGY_TAXONOMY_PATTERN <- "Gyn"
#' 
#' 
#' # 
#' # STEP 1: IDENTIFY SUBSPECIALISTS WITH MISSING NPI NUMBERS ----
#' #
#' 
#' if (verbose) {
#'   logger::log_info("Step 1: Reading subspecialists database and identifying missing NPI numbers")
#' }
#' 
#' # Read the original subspecialists database
#' original_subspecialists_database <- readr::read_csv(GOBA)
#' 
#' # Validate input data
#' assertthat::assert_that(is.data.frame(original_subspecialists_database))
#' assertthat::assert_that(nrow(original_subspecialists_database) > 0)
#' assertthat::assert_that("NPI" %in% names(original_subspecialists_database))
#' 
#' if (verbose) {
#'   logger::log_info("Original database contains {nrow(original_subspecialists_database)} subspecialists")
#'   logger::log_info("Missing NPI count: {sum(is.na(original_subspecialists_database$NPI))}")
#' }
#' 
#' # Filter to subspecialists without NPI numbers for API search
#' subspecialists_missing_npi <- original_subspecialists_database %>%
#'   dplyr::rename(
#'     first = first_name,
#'     last = last_name
#'   ) %>%
#'   dplyr::filter(is.na(NPI))
#' 
#' # Validate filtered data
#' assertthat::assert_that(nrow(subspecialists_missing_npi) > 0)
#' 
#' if (verbose) {
#'   logger::log_info("Found {nrow(subspecialists_missing_npi)} subspecialists without NPI numbers")
#' }
#' 
#' # Save filtered data for API processing
#' subspecialists_missing_npi %>%
#'   readr::write_csv(FILTERED_SUBSPECIALISTS_OUTPUT)
#' 
#' if (verbose) {
#'   logger::log_info("Saved filtered subspecialists to: {FILTERED_SUBSPECIALISTS_OUTPUT}")
#' }
#' 
#' #
#' # STEP 2: SEARCH NPPES API FOR MISSING NPI NUMBERS OF GOBA DATA ----
#' #
#' 
#' if (verbose) {
#'   logger::log_info("Step 2: Searching NPPES API for missing NPI numbers")
#' }
#' 
#' # Search NPPES API using custom function
#' api_search_results <- search_and_process_npi(FILTERED_SUBSPECIALISTS_OUTPUT)
#' 
#' # Validate API results
#' assertthat::assert_that(is.data.frame(api_search_results))
#' 
#' if (verbose) {
#'   logger::log_info("API search returned {nrow(api_search_results)} potential matches")
#' }
#' 
#' # Process and filter API search results with detailed validation
#' if (verbose) {
#'   logger::log_info("Processing API search results through validation pipeline")
#' }
#' 
#' # Step 2a: Remove duplicate NPI numbers from API results
#' api_results_deduplicated <- api_search_results %>%
#'   dplyr::distinct(npi, .keep_all = TRUE)
#' 
#' if (verbose) {
#'   logger::log_info("After deduplication: {nrow(api_results_deduplicated)} unique NPI records")
#' }
#' 
#' # Step 2b: Clean name and credential fields by removing punctuation
#' api_results_cleaned <- api_results_deduplicated %>%
#'   dplyr::mutate(
#'     dplyr::across(
#'       c(basic_first_name, basic_last_name, basic_credential),
#'       .fns = ~stringr::str_remove_all(., "[[\\p{P}][\\p{S}]]")  # Fixed: was sstringr::tr_remove_all
#'     )
#'   )
#' 
#' if (verbose) {
#'   logger::log_info("Name and credential fields cleaned of punctuation")
#' }
#' 
#' # Step 2c: Standardize credentials and filter to physicians only
#' physician_records_only <- api_results_cleaned %>%
#'   # Convert credentials to uppercase for standardization
#'   dplyr::mutate(basic_credential = stringr::str_to_upper(basic_credential)) %>%
#'   
#'   # Initial filter for MD/DO pattern
#'   dplyr::filter(stringr::str_detect(basic_credential, "MD|DO")) %>%
#'   
#'   # Extract first 2 characters for precise matching
#'   dplyr::mutate(basic_credential = stringr::str_sub(basic_credential, 1, 2)) %>%
#'   
#'   # Keep only exact MD or DO matches
#'   dplyr::filter(basic_credential %in% VALID_CREDENTIALS)
#' 
#' if (verbose) {
#'   logger::log_info("After physician credential filtering: {nrow(physician_records_only)} records")
#'   credential_counts <- table(physician_records_only$basic_credential)
#'   md_count <- ifelse("MD" %in% names(credential_counts), credential_counts[["MD"]], 0)
#'   do_count <- ifelse("DO" %in% names(credential_counts), credential_counts[["DO"]], 0)
#'   logger::log_info("Credential breakdown: MD={md_count}, DO={do_count}")
#' }
#' 
#' # Step 2d: Filter to gynecology-related specialties
#' gynecology_specialists <- physician_records_only %>%
#'   dplyr::filter(stringr::str_detect(taxonomies_desc, fixed(GYNECOLOGY_TAXONOMY_PATTERN, ignore_case = TRUE)))
#' 
#' if (verbose) {
#'   logger::log_info("After gynecology taxonomy filtering: {nrow(gynecology_specialists)} specialists")
#'   unique_taxonomies <- unique(gynecology_specialists$taxonomies_desc)
#'   logger::log_info("Taxonomy codes found: {paste(unique_taxonomies, collapse = ', ')}")
#' }
#' 
#' # Step 2e: Final deduplication and data type conversion
#' validated_npi_numbers <- gynecology_specialists %>%
#'   # Final deduplication by NPI
#'   dplyr::distinct(npi, .keep_all = TRUE) %>%
#'   
#'   # Convert NPI to numeric for database consistency
#'   dplyr::mutate(npi = as.numeric(npi))
#' 
#' # Validate processed results
#' assertthat::assert_that(nrow(validated_npi_numbers) > 0)
#' assertthat::assert_that(all(!is.na(validated_npi_numbers$npi)))
#' 
#' if (verbose) {
#'   logger::log_info("After validation: {nrow(validated_npi_numbers)} gynecologic oncologists found")
#'   credential_counts <- table(validated_npi_numbers$basic_credential)
#'   md_count <- ifelse("MD" %in% names(credential_counts), credential_counts[["MD"]], 0)
#'   do_count <- ifelse("DO" %in% names(credential_counts), credential_counts[["DO"]], 0)
#'   logger::log_info("Credential distribution: MD={md_count}, DO={do_count}")
#' }
#' 
#' # Save validated NPI search results
#' validated_npi_numbers %>%
#'   readr::write_csv(SEARCHED_NPI_OUTPUT)
#' 
#' if (verbose) {
#'   logger::log_info("Saved validated NPI search results to: {SEARCHED_NPI_OUTPUT}")
#' }
#' 
#' #
#' # STEP 3: MERGE NPI NUMBERS WITH ORIGINAL DATABASE ----
#' #
#' 
#' if (verbose) {
#'   logger::log_info("Step 3: Merging new NPI numbers with original subspecialists database")
#' }
#' 
#' # Prepare original database for merging (keeping original column names)
#' original_subspecialists_numeric_npi <- original_subspecialists_database %>%
#'   dplyr::mutate(NPI = as.numeric(NPI))
#' 
#' # Note: original database has 'first_name'/'last_name' columns
#' # validated_npi_numbers has 'basic_first_name'/'basic_last_name' columns
#' 
#' # Verify data types match for joining
#' assertthat::assert_that(class(original_subspecialists_numeric_npi$NPI) == class(validated_npi_numbers$npi))
#' 
#' if (verbose) {
#'   logger::log_info("Data types validated for NPI joining")
#' }
#' 
#' # Merge original database with newly found NPI numbers
#' if (verbose) {
#'   logger::log_info("Starting left join between original database and validated NPI numbers")
#'   logger::log_info("Original database columns: {paste(names(original_subspecialists_numeric_npi), collapse = ', ')}")
#'   logger::log_info("Validated NPI columns: {paste(names(validated_npi_numbers), collapse = ', ')}")
#' }
#' 
#' # Perform the join
#' subspecialists_after_join <- original_subspecialists_numeric_npi %>%
#'   # Left join to preserve all original records
#'   # Note: original database has 'first_name'/'last_name', validated data has 'basic_first_name'/'basic_last_name'
#'   exploratory::left_join(
#'     validated_npi_numbers,
#'     by = c("first_name" = "basic_first_name", "last_name" = "basic_last_name"),
#'     ignorecase = TRUE
#'   )
#' 
#' if (verbose) {
#'   logger::log_info("After join: {nrow(subspecialists_after_join)} records")
#'   logger::log_info("Columns after join: {paste(names(subspecialists_after_join), collapse = ', ')}")
#'   
#'   # Check how many NPI numbers were filled
#'   original_na_count <- sum(is.na(original_subspecialists_numeric_npi$NPI))
#'   joined_na_count <- sum(is.na(subspecialists_after_join$NPI))
#'   logger::log_info("NPI filling check: Original NA count = {original_na_count}")
#' }
#' 
#' # Fill missing NPI numbers with newly found ones and clean up columns
#' subspecialists_with_complete_npi <- subspecialists_after_join %>%
#'   # Fill missing NPI numbers with newly found ones
#'   dplyr::mutate(NPI = dplyr::coalesce(NPI, npi)) %>%
#'   
#'   # Identify which API-added columns actually exist to remove
#'   {
#'     api_columns_to_remove <- c(
#'       "npi", "basic_credential", "basic_sole_proprietor", "basic_gender",
#'       "basic_enumeration_date", "basic_last_updated", "basic_status",
#'       "basic_name_prefix", "taxonomies_code", "taxonomies_taxonomy_group",
#'       "taxonomies_desc", "taxonomies_state", "taxonomies_license",
#'       "taxonomies_primary", "basic_middle_name", "basic_name_suffix",
#'       "basic_certification_date"
#'     )
#'     
#'     # Only remove columns that actually exist
#'     existing_columns_to_remove <- intersect(api_columns_to_remove, names(.))
#'     
#'     if (verbose) {
#'       logger::log_info("API columns to remove: {paste(existing_columns_to_remove, collapse = ', ')}")
#'     }
#'     
#'     # Remove only existing columns
#'     if (length(existing_columns_to_remove) > 0) {
#'       dplyr::select(., -all_of(existing_columns_to_remove))
#'     } else {
#'       . # Return unchanged if no columns to remove
#'     }
#'   }
#' 
#' if (verbose) {
#'   original_missing <- sum(is.na(original_subspecialists_numeric_npi$NPI))
#'   final_missing <- sum(is.na(subspecialists_with_complete_npi$NPI))
#'   logger::log_info("NPI completion: {original_missing - final_missing} new NPI numbers added")
#'   logger::log_info("Remaining missing NPIs: {final_missing}")
#' }
#' 
#' #
#' # STEP 4: COMBINE WITH TAXONOMY SEARCH DATA (IF AVAILABLE) ----
#' #
#' 
#' if (verbose) {
#'   logger::log_info("Step 4: Checking for taxonomy search data to combine")
#' }
#' 
#' # Check if taxonomy search data exists from previous processing steps
#' if (exists("all_taxonomy_search_data") && is.data.frame(all_taxonomy_search_data)) {
#'   
#'   if (verbose) {
#'     logger::log_info("Taxonomy search data found with {nrow(all_taxonomy_search_data)} records")
#'     logger::log_info("Combining with taxonomy search data for comprehensive coverage")
#'   }
#'   
#'   # Combine with taxonomy search results to include younger subspecialists
#'   # (Those who have taxonomy codes but may not yet have board certification)
#'   comprehensive_subspecialists_database <- subspecialists_with_complete_npi %>%
#'     exploratory::bind_rows(
#'       all_taxonomy_search_data,
#'       id_column_name = "ID",
#'       current_df_name = "subspecialists_only",
#'       force_data_type = TRUE
#'     ) %>%
#'     
#'     # Remove duplicates based on NPI number
#'     dplyr::distinct(NPI, .keep_all = TRUE)
#'   
#'   if (verbose) {
#'     logger::log_info("After combining with taxonomy data: {nrow(comprehensive_subspecialists_database)} total records")
#'   }
#'   
#' } else {
#'   
#'   if (verbose) {
#'     logger::log_warn("Taxonomy search data not found - skipping taxonomy combination step")
#'     logger::log_info("To include taxonomy search data, run R/02-search_taxonomy.R first")
#'     logger::log_info("Proceeding with current subspecialists database only")
#'   }
#'   
#'   # Use the current database without taxonomy additions
#'   comprehensive_subspecialists_database <- subspecialists_with_complete_npi
#'   
#'   if (verbose) {
#'     logger::log_info("Current database contains: {nrow(comprehensive_subspecialists_database)} records")
#'   }
#' }
#' 
#' #
#' # STEP 5: FINAL DATA CLEANING AND STANDARDIZATION ----
#' #
#' 
#' if (verbose) {
#'   logger::log_info("Step 5: Final data cleaning and address standardization")
#' }
#' 
#' final_gynecologic_oncologists_database <- comprehensive_subspecialists_database %>%
#'   # Remove unnecessary columns from various data sources (only if they exist)
#'   {
#'     # Define all potential columns to remove from various processing steps
#'     potential_columns_to_remove <- c(
#'       "ID", "userid", "startDate", "certStatus", "sub1startDate", "sub1certStatus",
#'       "honorrific_end", "Medical school namePhysicianCompare",
#'       "Graduation yearPhysicianCompare", "Organization legal namePhysicianCompare",
#'       "Number of Group Practice membersPhysicianCompare",
#'       "Professional accepts Medicare AssignmentPhysicianCompare",
#'       "search_term", "basic_sole_proprietor", "basic_enumeration_date",
#'       "taxonomies_primary", "addresses_address_1", "addresses_telephone_number",
#'       "npi", "name.x", "basic_first_name", "basic_last_name", "basic_middle_name",
#'       "basic_gender", "taxonomies_desc", "addresses_city", "addresses_state",
#'       "addresses_postal_code", "full_name"
#'     )
#'     
#'     # Only remove columns that actually exist
#'     existing_columns_to_remove <- intersect(potential_columns_to_remove, names(.))
#'     
#'     if (verbose) {
#'       logger::log_info("Available columns to clean: {paste(existing_columns_to_remove, collapse = ', ')}")
#'       logger::log_info("Current column count: {ncol(.)}")
#'     }
#'     
#'     # Remove only existing columns
#'     if (length(existing_columns_to_remove) > 0) {
#'       dplyr::select(., -all_of(existing_columns_to_remove))
#'     } else {
#'       if (verbose) {
#'         logger::log_info("No unnecessary columns found to remove")
#'       }
#'       . # Return unchanged if no columns to remove
#'     }
#'   } %>%
#'   
#'   # Add unique row identifier
#'   dplyr::mutate(row_number = dplyr::row_number()) %>%
#'   
#'   # Standardize ZIP codes to 5 digits (if ZIP code column exists)
#'   {
#'     if ("Zip CodePhysicianCompare" %in% names(.)) {
#'       dplyr::mutate(.,
#'                     zip = stringr::str_sub(`Zip CodePhysicianCompare`, 1, 5),
#'                     .after = "Zip CodePhysicianCompare"
#'       )
#'     } else {
#'       if (verbose) {
#'         logger::log_warn("Zip CodePhysicianCompare column not found - skipping ZIP standardization")
#'       }
#'       # Create empty zip column for consistency
#'       dplyr::mutate(., zip = "")
#'     }
#'   } %>%
#'   
#'   # Filter to records with complete location information
#'   dplyr::filter(!is.na(state) & !is.na(city)) %>%
#'   
#'   # Handle missing ZIP codes (temporary solution - TODO: improve this)
#'   dplyr::mutate(zip = ifelse(is.na(zip) | zip == "", "", zip)) %>%
#'   
#'   # Create standardized address field for geocoding
#'   tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
#'   
#'   # Remove duplicate addresses (same practice location)
#'   dplyr::distinct(address, .keep_all = TRUE)
#' 
#' # Final validation
#' assertthat::assert_that(nrow(final_gynecologic_oncologists_database) > 0)
#' assertthat::assert_that(all(!is.na(final_gynecologic_oncologists_database$address)))
#' 
#' if (verbose) {
#'   logger::log_info("Final database statistics:")
#'   logger::log_info("- Total gynecologic oncologists: {nrow(final_gynecologic_oncologists_database)}")
#'   logger::log_info("- Unique addresses: {length(unique(final_gynecologic_oncologists_database$address))}")
#'   logger::log_info("- States represented: {length(unique(final_gynecologic_oncologists_database$state))}")
#'   logger::log_info("- Complete NPI numbers: {sum(!is.na(final_gynecologic_oncologists_database$NPI))}")
#' }
#' 
#' # Save final comprehensive database
#' final_gynecologic_oncologists_database %>%
#'   readr::write_rds(FINAL_OUTPUT_FILE)
#' 
#' if (verbose) {
#'   logger::log_info("Final gynecologic oncologists database saved to: {FINAL_OUTPUT_FILE}")
#'   logger::log_info("NPI processing complete!")
#' }
#' 
#' # Read and Rename Columns: It begins by reading a CSV file that contains information about subspecialists. The code renames specific columns in this dataset to more convenient names.
#' # 
#' # Filter Rows: Rows in the dataset are filtered to select only those where the National Provider Identifier (NPI) number is missing (NA), indicating subspecialists without NPI numbers. The filtered data is then saved as a new CSV file.
#' # 
#' # Retrieve NPI Numbers: A custom function (search_and_process_npi) is used to retrieve NPI numbers for the subspecialists who lack them. The retrieved NPI numbers are processed, including removing punctuation, converting credential information to uppercase, and filtering based on specific criteria. The processed NPI data is saved as another CSV file.
#' # 
#' # Read Original Subspecialty Data: The original dataset containing subspecialist information is read again, and the NPI column is converted to a numeric format.
#' # 
#' # Coalesce NPI Numbers: The missing NPI numbers in the original subspecialist dataset are filled in (coalesced) with the NPI numbers from the previous step. Unnecessary columns from the NPI dataset are removed.
#' # 
#' # Combine DataFrames: The code combines the datasets related to NPI numbers and taxonomy information into a single dataset. This merged dataset likely contains comprehensive information about subspecialists, including their NPI numbers and taxonomy data.
#' # 
#' # Merge Rows: Rows are merged from the taxonomy dataset and the NPI dataset to include younger subspecialists who have a taxonomy code but not board certification yet. Additional data transformations, filtering, and column manipulation are performed during this step.
#' # 
#' # Save Results: The final merged dataset is saved in RDS (R Data Store) format for further analysis or use in other R scripts.
#' 
#' #**************************
#' # GETS CURRENT DATA DATA  ----
#' #**************************
#' ### Read in file and clean it up
#' # File Provenance: "/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/goba/subspecialists_only.csv"
#' filtered_subspecialists <- readr::read_csv("data/03-search_and_process_npi/subspecialists_only.csv") %>%
#'   dplyr::rename(first = first_name) %>%
#'   dplyr::rename(last = last_name) %>%
#'   dplyr::filter(is.na(NPI)) %>%
#'   readr::write_csv("data/03-search_and_process_npi/filtered_subspecialists_only.csv")
#' 
#' #**************************
#' # RUN THE API FOR MORE DOCTOR DEMOGRAPHICS ----
#' # GET NPI NUMBERS for those that do not have any in subspecialists.csv, using search_and_process_npi
#' #**************************
#' input_file <- "data/03-search_and_process_npi/filtered_subspecialists_only.csv"
#' output_result <- search_and_process_npi(input_file) #Runs the function to get data from the NPPES website
#' 
#' searched_npi_numbers <- output_result %>%
#'   dplyr::distinct(npi, .keep_all = TRUE) %>%
#'   dplyr::mutate(across(
#'     c(basic_first_name, basic_last_name, basic_credential),
#'     .fns = ~stringr::str_remove_all(., "[[\\p{P}][\\p{S}]]")
#'   )) %>%
#'   dplyr::mutate(basic_credential = stringr::str_to_upper(basic_credential)) %>%
#'   dplyr::filter(stringr::str_detect(basic_credential, "MD|DO")) %>%
#'   dplyr::mutate(basic_credential = stringr::str_sub(basic_credential,1 ,2)) %>%
#'   dplyr::filter(basic_credential %in% c("DO", "MD")) %>%
#'   dplyr::filter(stringr::str_detect(taxonomies_desc, fixed("Gyn", ignore_case=TRUE))) %>%
#'   dplyr::distinct(npi, .keep_all = TRUE) %>%
#'   dplyr::mutate(npi = as.numeric(npi)) %>%
#'   readr::write_csv("data/03-search_and_process_npi/searched_npi_numbers.csv") ### File with NPI numbers to complete subspecialty.csv file.    We need to merge the results of new NPI numbers in `searched_npi_numbers` with subspecialty.csv
#' 
#' #
#' # DATA DICTIONARY: Searched NPI Numbers Dataset (searched_npi_numbers.csv)
#' # 
#' # This dataset contains validated NPI numbers retrieved from the NPPES API
#' # for gynecologic subspecialists who were missing NPI identification in the
#' # original GOBA dataset. Used as intermediate processing file before final
#' # merge with comprehensive subspecialists database.
#' #
#' 
#' #
#' # CORE PHYSICIAN IDENTIFICATION
#' #
#' # npi                         - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              National Provider Identifier number (numeric) 
#' #                              retrieved from NPPES National Provider database
#' #
#' # basic_first_name            - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              First name as registered in NPPES database,
#' #                              cleaned of punctuation and special characters
#' #
#' # basic_last_name             - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Last name as registered in NPPES database,
#' #                              cleaned of punctuation and special characters
#' #
#' # basic_credential            - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Medical credential (MD or DO only), standardized
#' #                              to uppercase and truncated to first 2 characters
#' 
#' #
#' # NPPES REGISTRATION DATA
#' #
#' # basic_sole_proprietor       - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Whether provider is registered as sole proprietor
#' #                              in NPPES database
#' #
#' # basic_gender                - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Gender as registered in NPPES database
#' #
#' # basic_enumeration_date      - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Date when NPI was first assigned to this provider
#' #
#' # basic_last_updated          - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Date when NPPES record was last updated
#' #
#' # basic_status                - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Current status of NPI registration (Active/Inactive)
#' #
#' # basic_name_prefix           - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Name prefix (Dr., Mr., Ms., etc.) from NPPES
#' #
#' # basic_middle_name           - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Middle name or initial from NPPES registration
#' #
#' # basic_name_suffix           - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Name suffix (Jr., Sr., III, etc.) from NPPES
#' #
#' # basic_certification_date    - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Date of medical certification/graduation
#' 
#' #
#' # SPECIALTY/TAXONOMY CLASSIFICATION
#' #
#' # taxonomies_code             - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Healthcare provider taxonomy code from NPPES
#' #                              (used to identify gynecology specialists)
#' #
#' # taxonomies_taxonomy_group   - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Taxonomy group classification from NPPES
#' #
#' # taxonomies_desc             - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Taxonomy description (filtered to contain "Gyn" 
#' #                              pattern to identify gynecology-related specialties)
#' #
#' # taxonomies_state            - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              State where taxonomy/license is valid
#' #
#' # taxonomies_license          - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Medical license number associated with taxonomy
#' #
#' # taxonomies_primary          - Source: NPPES API search results
#' #                              File: R/03-search_and_process_npi.R
#' #                              Whether this is the provider's primary taxonomy
#' 
#' #
#' # DATA PROCESSING PIPELINE FOR THIS FILE
#' #
#' # 1. R/03-search_and_process_npi.R identifies subspecialists with missing NPIs
#' #    from GOBA_Scrape_subspecialists_only.csv
#' # 2. Filtered subspecialists sent to search_and_process_npi() function
#' # 3. NPPES API returns potential matches with full provider details
#' # 4. Results filtered through validation pipeline:
#' #    - Deduplicated by NPI number
#' #    - Cleaned of punctuation in names/credentials
#' #    - Filtered to MD/DO credentials only
#' #    - Filtered to gynecology-related taxonomy codes (contains "Gyn")
#' #    - Final deduplication and numeric conversion of NPI
#' # 5. Validated results saved as searched_npi_numbers.csv
#' 
#' #
#' # DATASET CHARACTERISTICS
#' #
#' # Purpose: Intermediate file containing validated NPI numbers for subspecialists
#' #          missing identification in original GOBA dataset
#' # Record Count: Variable (depends on how many NPIs were missing in GOBA)
#' # Data Quality: High - multiple validation filters applied to ensure accuracy
#' # Coverage: Only subspecialists with missing NPIs in original dataset
#' # Validation Criteria: MD/DO credentials + gynecology taxonomy + active status
#' 
#' #
#' # FILTERING CRITERIA APPLIED
#' #
#' # 1. Credential Validation: Only MD or DO physicians included
#' # 2. Specialty Validation: Only providers with "Gyn" in taxonomy description
#' # 3. Deduplication: Unique by NPI number to prevent duplicates
#' # 4. Data Quality: Punctuation removed, credentials standardized
#' # 5. Completeness: Only records with valid NPI numbers included
#' 
#' #
#' # USAGE NOTES
#' #
#' # - This file serves as input for merging with original GOBA dataset
#' # - Use npi field to join with NPI column in main subspecialists database
#' # - Join on basic_first_name/basic_last_name to first_name/last_name in GOBA
#' # - Taxonomy fields can be used for specialty validation and filtering
#' # - File is temporary - final analysis should use end_complete_npi_for_subspecialists.rds
#' # - Contains only providers that were missing from original GOBA dataset
#' 
#' 
#' 
#' # Read in the original subspecialty.csv file.  This file is given and is not calculated earlier in the workflow.   ----
#' # File Provenance: "/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/goba/subspecialists_only.csv"
#' subspecialists_only <- read_csv("data/03-search_and_process_npi/subspecialists_only.csv") %>%
#'   mutate(NPI = as.numeric(NPI))
#' 
#' # Coalesce NPI numbers to fill NA gaps in `subspecialists_only`
#' all_NPI_numbers_we_will_ever_find <-
#'   subspecialists_only %>%
#'   exploratory::left_join(searched_npi_numbers, by =
#'                            c("first" = "basic_first_name",
#'                              "last" = "basic_last_name"), ignorecase=TRUE) %>%
#'   dplyr::mutate(NPI = dplyr::coalesce(NPI, npi)) %>% #Fills in the NPI numbers with NA
#'   dplyr::select(-npi, -basic_credential, -basic_sole_proprietor, -basic_gender, - basic_enumeration_date, -basic_last_updated, -basic_status, -basic_name_prefix, -taxonomies_code,  -taxonomies_taxonomy_group, -taxonomies_desc, -taxonomies_state, -taxonomies_license, -taxonomies_primary, -basic_middle_name, -basic_name_suffix, -basic_certification_date) # removes the unneeded rows from `searched_npi_numbers`
#' 
#' ### Bring together the rows of the taxonomy data and the rows of the goba NPI numbers
#' taxonomy_plus_NPI <- all_NPI_numbers_we_will_ever_find %>%
#'   exploratory::bind_rows(all_taxonomy_search_data, id_column_name = "ID", current_df_name = "subspecialists_only",         force_data_type = TRUE) %>%
#'   dplyr::distinct(NPI, .keep_all = TRUE)
#' 
#' ### Merge rows of `all_taxonomy_search_data` from 02-search_taxonomy and `searched_npi_numbers` from 03-search_and_process_npi  ----
#' # Brings in the younger subspecialists who have a taxonomy code but not board-certification yet.  Board certification for OBGYN usually takes 2 years after graduating from fellowship.  
#' complete_npi_for_subspecialists <- all_NPI_numbers_we_will_ever_find %>%
#'   exploratory::bind_rows(all_taxonomy_search_data, id_column_name = "ID", current_df_name = "subspecialists_only", force_data_type = TRUE) %>%
#'   dplyr::distinct(NPI, .keep_all = TRUE) %>%
#'   dplyr::select(-ID, -userid, -startDate, -certStatus, -sub1startDate, -sub1certStatus, -honorrific_end, -`Medical school namePhysicianCompare`, -`Graduation yearPhysicianCompare`, -`Organization legal namePhysicianCompare`, -`Number of Group Practice membersPhysicianCompare`, -`Professional accepts Medicare AssignmentPhysicianCompare`, -search_term, -basic_sole_proprietor, -basic_enumeration_date, -taxonomies_primary, -addresses_address_1, -addresses_telephone_number, -npi, -name.x, -basic_first_name, -basic_last_name, -basic_middle_name, -basic_gender, -taxonomies_desc, -addresses_city, -addresses_state, -addresses_postal_code, -full_name) %>%
#'   dplyr::mutate(row_number = dplyr::row_number()) %>%
#'   dplyr::mutate(zip = stringr::str_sub(`Zip CodePhysicianCompare`,1 ,5), .after = ifelse("Zip CodePhysicianCompare" %in% names(.), "Zip CodePhysicianCompare", last_col())) %>%
#'   dplyr::filter(!is.na(state) & !is.na(city)) %>%
#' 
#'   # TODO: i need to change this back eventually.
#'   dplyr::mutate(zip = exploratory::impute_na(zip, type = "value", val = "")) %>%
#'   tidyr::unite(address, city, state, zip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
#'   dplyr::distinct(address, .keep_all = TRUE) %>%
#'   readr::write_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds")
#' 
#' #
#' # DATA DICTIONARY: Final Subspecialists Database (end_complete_npi_for_subspecialists.rds)
#' # 
#' # This comprehensive database combines gynecologic subspecialists from multiple
#' # authoritative sources to support geographic accessibility analysis following
#' # Desjardins et al. (2023) methodology.
#' #
#' 
#' #
#' # CORE PHYSICIAN IDENTIFICATION
#' #
#' # sub1                        - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Subspecialty abbreviation extracted from ABOG 
#' #                              verification tool PDFs (MFM=Maternal-Fetal Medicine, 
#' #                              ONC=Gynecologic Oncology, REI=Reproductive Endocrinology, 
#' #                              FPM=Female Pelvic Medicine, MIG=Minimally Invasive Gynecology)
#' #
#' # first_name                  - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Physician's first name from ABOG board certification 
#' #                              verification tool PDFs
#' #
#' # last_name                   - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Physician's last name from ABOG board certification 
#' #                              verification tool PDFs
#' #
#' # NPI                         - Source: NPPES API search + original GOBA data
#' #                              File: R/03-search_and_process_npi.R
#' #                              National Provider Identifier retrieved via 
#' #                              search_and_process_npi() function for missing NPIs, 
#' #                              coalesced with existing NPI numbers from GOBA
#' #
#' # standardized_physician_name - Source: Computed field
#' #                              File: Current update function
#' #                              Created by combining and cleaning first_name + 
#' #                              last_name for cross-dataset matching
#' 
#' #
#' # GEOGRAPHIC/LOCATION DATA
#' #
#' # address                     - Source: Computed field
#' #                              File: R/03-search_and_process_npi.R
#' #                              Created by combining city, state, and ZIP using 
#' #                              tidyr::unite() for geocoding purposes
#' #
#' # state                       - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              State from original ABOG verification tool PDFs
#' #
#' # city                        - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              City from original ABOG verification tool PDFs
#' #
#' # zip                         - Source: Computed from PhysicianCompare
#' #                              File: R/03-search_and_process_npi.R
#' #                              5-digit ZIP extracted from Zip CodePhysicianCompare 
#' #                              using stringr::str_sub()
#' #
#' # Zip CodePhysicianCompare    - Source: CMS PhysicianCompare database
#' #                              File: R/0-goba_search_update_goba.R
#' #                              Full ZIP+4 code from Medicare PhysicianCompare lookup
#' 
#' #
#' # CERTIFICATION AND STATUS
#' #
#' # mocStatus                   - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Maintenance of Certification status from ABOG 
#' #                              verification tool PDFs
#' #
#' # sub1mocStatus               - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Subspecialty Maintenance of Certification status 
#' #                              from ABOG verification tool PDFs
#' #
#' # clinicallyActive            - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Clinical activity status from ABOG verification 
#' #                              tool PDFs
#' 
#' #
#' # DATA PROVENANCE AND TIMESTAMPS
#' #
#' # DateTime                    - Source: GOBA scraping process
#' #                              File: R/0-Download and extract PDF.R
#' #                              Timestamp when record was extracted from ABOG 
#' #                              verification tool PDFs
#' #
#' # ID.new                      - Source: GOBA scraping batch process
#' #                              File: R/0-Download and extract PDF.R
#' #                              Batch identifier showing which PDF extraction run 
#' #                              produced this record (e.g., "~Recent_Grads_GOBA_NPI_2022")
#' #
#' # id                          - Source: GOBA scraping file management
#' #                              File: R/0-Download and extract PDF.R
#' #                              Original CSV filename from ABOG batch PDF processing
#' #
#' # data_source                 - Source: Script processing logic
#' #                              File: Current update function
#' #                              Added during merge process to track whether record 
#' #                              came from "GOBA", "Combined_Extractions", or taxonomy search
#' 
#' #
#' # HISTORICAL CERTIFICATION DATES
#' #
#' # orig_sub                    - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Original subspecialty certification date from ABOG 
#' #                              verification tool PDFs
#' #
#' # x_sub_orig                  - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Additional/secondary subspecialty certification date 
#' #                              from ABOG verification tool PDFs
#' #
#' # orig_bas                    - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Original basic OBGYN board certification date from 
#' #                              ABOG verification tool PDFs
#' 
#' #
#' # ADMINISTRATIVE FIELDS
#' #
#' # app_no                      - Source: GOBA board certification scraping
#' #                              File: R/0-Download and extract PDF.R
#' #                              Application number from ABOG PDFs (mostly NA/unused 
#' #                              legacy field)
#' #
#' # Input.name                  - Source: GOBA processing
#' #                              File: R/0-Download and extract PDF.R
#' #                              Original input name format during GOBA PDF processing 
#' #                              (mostly NA)
#' #
#' # row_number                  - Source: Final processing script
#' #                              File: R/03-search_and_process_npi.R
#' #                              Sequential identifier added during final dataset assembly
#' 
#' #
#' # DEMOGRAPHICS
#' #
#' # name.y                      - Source: CMS PhysicianCompare database
#' #                              File: R/0-goba_search_update_goba.R
#' #                              Full formal name with credentials from Medicare 
#' #                              PhysicianCompare lookup
#' #
#' # GenderPhysicianCompare      - Source: CMS PhysicianCompare database
#' #                              File: R/0-goba_search_update_goba.R
#' #                              Gender from Medicare PhysicianCompare database
#' #
#' # basic_sex                   - Source: NPPES API search
#' #                              File: R/03-search_and_process_npi.R
#' #                              Gender from NPPES National Provider database 
#' #                              (mostly NA due to limited availability)
#' 
#' #
#' # DATA SOURCE SUMMARY
#' #
#' # 1. GOBA (Primary Source): Board certification scraping from ABOG verification 
#' #    tool - provides subspecialty, names, certification status, dates
#' #    Primary File: R/0-Download and extract PDF.R
#' #
#' # 2. NPPES API: National Provider database searches - provides missing NPI 
#' #    numbers and some demographic data
#' #    Primary File: R/03-search_and_process_npi.R
#' #
#' # 3. CMS PhysicianCompare: Medicare provider database - provides formal names, 
#' #    gender, ZIP codes
#' #    Primary File: R/0-goba_search_update_goba.R
#' #
#' # 4. Computed Fields: Created during processing for standardization and geocoding
#' #    Various Files: Throughout processing pipeline
#' #
#' # 5. Combined Extractions: Additional subspecialists from 
#' #    combined_subspecialty_extractions.csv added via the update function
#' #    Primary File: Current update function
#' #
#' # 6. Taxonomy Search (if available): Additional providers identified through 
#' #    NPPES taxonomy codes rather than board certification
#' #    Primary File: R/02-search_taxonomy.R
#' 
#' #
#' # FILE PROCESSING PIPELINE
#' #
#' # 1. R/0-Download and extract PDF.R    → Creates GOBA_Scrape_subspecialists_only.csv 
#' #                                        with core physician data from ABOG PDFs
#' # 2. R/0-goba_search_update_goba.R     → Enhances records with PhysicianCompare 
#' #                                        demographic and location data
#' # 3. R/0-goba_search_this one works_adaptive_binary_search.R → Optimizes 
#' #                                        searching/matching across datasets
#' # 4. Current update function           → Merges additional subspecialists from 
#' #                                        combined_subspecialty_extractions.csv
#' # 5. R/02-search_taxonomy.R            → Adds taxonomy-based subspecialists 
#' #                                        (creates all_taxonomy_search_data)
#' # 6. R/03-search_and_process_npi.R     → Fills missing NPIs, merges all sources, 
#' #                                        creates final RDS file
#' 
#' #
#' # DATASET CHARACTERISTICS
#' #
#' # Total Records: ~4,014 subspecialists
#' # Data Sources: Board certification records (GOBA), NPPES API searches, 
#' #               and taxonomy-based identification
#' # Geographic Coverage: All U.S. states with practicing gynecologic subspecialists
#' # Time Period: 2019-2023 (based on extraction timestamps)
#' 
#' #
#' # DATA QUALITY NOTES
#' #
#' # Primary Source: ABOG board certification verification tool (most authoritative 
#' #                 for subspecialty status)
#' # NPI Completion: NPPES API used to fill ~721 missing NPI numbers through 
#' #                 name-based fuzzy matching
#' # Geographic Standardization: ZIP codes standardized to 5 digits, addresses 
#' #                             formatted for geocoding
#' # Deduplication: Final dataset deduplicated by NPI to prevent double-counting 
#' #                across data sources
#' # Coverage: Captures both board-certified specialists and early-career physicians 
#' #           with taxonomy codes but pending certification
#' 
#' #
#' # KEY USAGE NOTES FOR ACCESSIBILITY ANALYSIS
#' #
#' # - Use NPI as the primary unique identifier for physicians
#' # - Use address field for geocoding and geographic accessibility analysis
#' # - Filter by clinicallyActive == "Yes" for active practitioners only
#' # - sub1 field categorizes subspecialty types for workforce distribution analysis
#' # - Combine with county-level demographic data for accessibility studies 
#' #   following Desjardins et al. (2023) methodology
#' #
#' 
#' # complete_npi_for_subspecialists <- readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds") # for testing
#' 
#' 
#' # QC: Trace Marisa Moroney ----
#' #
#' # TRACING MARISA MORONEY THROUGH THE NPI PIPELINE
#' # ABOG ID: 9033995
#' #
#' 
#' # Let's load the necessary libraries and functions first
#' source("R/01-setup.R")
#' library(tidyverse)
#' library(logger)
#' 
#' # Set up tracking
#' physician_to_track <- "Moroney"
#' abog_id <- "9033995"
#' 
#' logger::log_info("Starting trace for Marisa Moroney (ABOG ID: {abog_id})")
#' 
#' #
#' # CHECKPOINT 1: Initial GOBA Database
#' #
#' logger::log_info("CHECKPOINT 1: Checking initial GOBA database")
#' 
#' # Load the original GOBA scraped data
#' goba_data <- readr::read_csv("data/03-search_and_process_npi/GOBA_Scrape_subspecialists_only.csv")
#' 
#' # Check if Marisa is in the initial dataset
#' marisa_initial <- goba_data %>%
#'   filter(str_detect(last_name, "Moroney") | 
#'            str_detect(first_name, "Marisa") |
#'            userid == "9033995")
#' 
#' if(nrow(marisa_initial) > 0) {
#'   logger::log_info("✓ Found in initial GOBA database")
#'   print(marisa_initial %>% 
#'           select(first_name, last_name, NPI, state, city, sub1, 
#'                  mocStatus, clinicallyActive))
#'   
#'   # Check if she has an NPI
#'   has_npi <- !is.na(marisa_initial$NPI)
#'   logger::log_info("NPI Status: {ifelse(has_npi, paste('Has NPI:', marisa_initial$NPI), 'Missing NPI')}")
#' } else {
#'   logger::log_warn("✗ Not found in initial GOBA database")
#' }
#' 
#' #
#' # CHECKPOINT 2: Missing NPI Filter
#' #
#' logger::log_info("CHECKPOINT 2: Checking filtered missing NPI list")
#' 
#' # Check if she's in the missing NPI file
#' missing_npi_list <- readr::read_csv("data/03-search_and_process_npi/filtered_subspecialists_only.csv")
#' 
#' marisa_missing <- missing_npi_list %>%
#'   filter(str_detect(last, "Moroney") | str_detect(first, "Marisa"))
#' 
#' if(nrow(marisa_missing) > 0) {
#'   logger::log_info("✓ Found in missing NPI list - will search NPPES")
#'   print(marisa_missing %>% select(first, last, state, city, sub1))
#' } else {
#'   logger::log_info("✗ Not in missing NPI list (already has NPI or not in database)")
#' }
#' 
#' #
#' # CHECKPOINT 3: NPPES API Search Results
#' #
#' logger::log_info("CHECKPOINT 3: Checking NPPES API search results")
#' 
#' # If she needed NPI search, check the results
#' if(nrow(marisa_missing) > 0) {
#'   # Check searched NPI results
#'   api_results <- readr::read_csv("data/03-search_and_process_npi/searched_npi_numbers.csv")
#'   
#'   marisa_api <- api_results %>%
#'     filter(str_detect(basic_last_name, "Moroney") | 
#'              str_detect(basic_first_name, "Marisa"))
#'   
#'   if(nrow(marisa_api) > 0) {
#'     logger::log_info("✓ Found in NPPES API results")
#'     print(marisa_api %>% 
#'             select(npi, basic_first_name, basic_last_name, basic_credential,
#'                    taxonomies_desc, basic_gender))
#'     
#'     # Check validation criteria
#'     logger::log_info("Validation checks:")
#'     logger::log_info("  - Credential: {marisa_api$basic_credential} (MD/DO required)")
#'     logger::log_info("  - Taxonomy: {marisa_api$taxonomies_desc}")
#'     logger::log_info("  - Has 'Gyn' in taxonomy: {str_detect(marisa_api$taxonomies_desc, 'Gyn')}")
#'   } else {
#'     logger::log_warn("✗ Not found in NPPES API results")
#'     logger::log_info("Possible reasons:")
#'     logger::log_info("  - Name mismatch in NPPES")
#'     logger::log_info("  - Not MD/DO credential")
#'     logger::log_info("  - No gynecology taxonomy")
#'   }
#' }
#' 
#' #
#' # CHECKPOINT 4: After NPI Merge
#' #
#' logger::log_info("CHECKPOINT 4: Checking after NPI merge")
#' 
#' # This would be in your intermediate data after the left join
#' # Since we don't save this intermediate step, we'll simulate it
#' goba_with_npi <- goba_data %>%
#'   mutate(NPI = as.numeric(NPI))
#' 
#' if(exists("marisa_api") && nrow(marisa_api) > 0) {
#'   # Simulate the merge
#'   logger::log_info("Simulating NPI merge:")
#'   logger::log_info("  - Original NPI: {ifelse(is.na(marisa_initial$NPI), 'NA', marisa_initial$NPI)}")
#'   logger::log_info("  - API found NPI: {marisa_api$npi}")
#'   logger::log_info("  - After coalesce: {coalesce(marisa_initial$NPI, marisa_api$npi)}")
#' }
#' 
#' #
#' # CHECKPOINT 5: Taxonomy Search Data (if exists)
#' #
#' logger::log_info("CHECKPOINT 5: Checking taxonomy search data")
#' 
#' taxonomy_file <- "data/03-search_and_process_npi/all_taxonomy_search_data.csv"
#' if(file.exists(taxonomy_file)) {
#'   taxonomy_data <- readr::read_csv(taxonomy_file)
#'   
#'   marisa_taxonomy <- taxonomy_data %>%
#'     filter(str_detect(last_name, "Moroney") | 
#'              str_detect(first_name, "Marisa") |
#'              NPI == marisa_initial$NPI)
#'   
#'   if(nrow(marisa_taxonomy) > 0) {
#'     logger::log_info("✓ Also found in taxonomy search data")
#'     print(marisa_taxonomy %>% 
#'             select(first_name, last_name, NPI, taxonomies_desc))
#'   } else {
#'     logger::log_info("✗ Not in taxonomy search data")
#'   }
#' }
#' 
#' #
#' # CHECKPOINT 6: Final Combined Database
#' #
#' logger::log_info("CHECKPOINT 6: Checking final database")
#' 
#' final_data <- readr::read_rds("data/03-search_and_process_npi/end_complete_npi_for_subspecialists.rds")
#' 
#' marisa_final <- final_data %>%
#'   filter(str_detect(last_name, "Moroney") | 
#'            str_detect(first_name, "Marisa") |
#'            NPI == marisa_initial$NPI)
#' 
#' if(nrow(marisa_final) > 0) {
#'   logger::log_info("✓ Found in final database")
#'   
#'   # Show her final record
#'   logger::log_info("Final record for Marisa Moroney:")
#'   print(marisa_final %>% 
#'           select(first_name, last_name, NPI, address, state, city, zip,
#'                  sub1, mocStatus, clinicallyActive))
#'   
#'   # Check data completeness
#'   logger::log_info("Data completeness:")
#'   logger::log_info("  - Has NPI: {!is.na(marisa_final$NPI)}")
#'   logger::log_info("  - Has address: {!is.na(marisa_final$address)}")
#'   logger::log_info("  - Has state: {!is.na(marisa_final$state)}")
#'   logger::log_info("  - Has city: {!is.na(marisa_final$city)}")
#'   logger::log_info("  - Subspecialty: {marisa_final$sub1}")
#'   
#'   # Check if duplicate addresses were removed
#'   n_addresses <- marisa_final %>% 
#'     distinct(address) %>% 
#'     nrow()
#'   logger::log_info("  - Number of unique addresses: {n_addresses}")
#'   
#' } else {
#'   logger::log_error("✗ NOT FOUND in final d
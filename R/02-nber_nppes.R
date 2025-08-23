# Pull all the subspecialties from the NBER NPPES data.

# File Path Constants for NBER NPPES ----
ABOG_INPUT_DIR <- "data/0-Download/output"
NPI_OUTPUT_DIR <- "data/02.33-nber_nppes_data/output"

ABOG_PROVIDER_DATA_FILE <- file.path(ABOG_INPUT_DIR, "best_abog_provider_dataframe_8_17_2025_2053.csv")
OBGYN_TAXONOMY_OUTPUT_FILE <- file.path(NPI_OUTPUT_DIR, "obgyn_taxonomy_abog_npi_matched_8_18_2025.csv")
UROLOGY_TAXONOMY_OUTPUT_FILE <- file.path(NPI_OUTPUT_DIR, "urology_taxonomy_abog_npi_matched_8_18_2025.csv")

# API and processing constants
API_RATE_LIMIT_DELAY_DEFAULT <- 0.1
API_RATE_LIMIT_DELAY_SLOW <- 0.2
MAX_RESULTS_PER_QUERY <- 200
HTTP_OK <- 200

# Pull all the subspecialties from the NBER NPPES data.  
#source("R/B-nber_nppes_combine_columns.R")
source("R/01-setup.R")

# Function at 1723 ----
# Add these missing helper functions to your script:

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
validate_npi_api_inputs <- function(physician_names_input, specialty_filter,
                                    output_file_path, api_rate_limit_delay,
                                    max_results_per_query, enable_verbose_logging) {
  
  assertthat::assert_that(
    !is.null(physician_names_input),
    msg = "physician_names_input cannot be NULL"
  )
  
  assertthat::assert_that(
    is.character(specialty_filter),
    msg = "specialty_filter must be character string"
  )
  
  assertthat::assert_that(
    is.numeric(api_rate_limit_delay) && api_rate_limit_delay >= 0,
    msg = "api_rate_limit_delay must be non-negative numeric"
  )
  
  assertthat::assert_that(
    is.numeric(max_results_per_query) && max_results_per_query > 0,
    msg = "max_results_per_query must be positive numeric"
  )
  
  assertthat::assert_that(
    is.logical(enable_verbose_logging),
    msg = "enable_verbose_logging must be logical"
  )
  
  logger::log_info("Input validation completed successfully")
}

#' @noRd
process_physician_names_for_api_lookup <- function(physician_names_input, 
                                                   enable_verbose_logging) {
  
  logger::log_info("Processing physician names for API lookup")
  
  if (is.character(physician_names_input)) {
    if (length(physician_names_input) == 1 && file.exists(physician_names_input)) {
      # CSV file input
      logger::log_info("Reading names from CSV file: {physician_names_input}")
      names_data <- readr::read_csv(physician_names_input, show_col_types = FALSE)
    } else {
      # Character vector of full names
      logger::log_info("Processing character vector of {length(physician_names_input)} names")
      names_data <- parse_full_names_for_api_query(physician_names_input)
    }
  } else if (is.data.frame(physician_names_input)) {
    # Data frame input
    logger::log_info("Processing data frame with {nrow(physician_names_input)} rows")
    names_data <- physician_names_input
  } else {
    stop("Invalid input type. Use character vector, data frame, or CSV file path.")
  }
  
  # Simple, safe standardization
  standardized_names <- names_data %>%
    dplyr::mutate(
      api_first_name = stringr::str_trim(first_name),
      api_last_name = stringr::str_trim(last_name),
      query_id = dplyr::row_number()
    )
  
  # Add original_input safely
  if ("original_input" %in% names(standardized_names)) {
    # Keep existing original_input
    standardized_names <- standardized_names
  } else if ("physician_name" %in% names(standardized_names)) {
    # Use physician_name if available
    standardized_names <- standardized_names %>%
      dplyr::mutate(original_input = physician_name)
  } else {
    # Create from first and last names
    standardized_names <- standardized_names %>%
      dplyr::mutate(original_input = paste(first_name, last_name))
  }
  
  # Filter for valid names
  standardized_names <- standardized_names %>%
    dplyr::filter(
      !is.na(api_first_name) & !is.na(api_last_name) &
        api_first_name != "" & api_last_name != ""
    )
  
  logger::log_info("Standardized {nrow(standardized_names)} valid names for API queries")
  
  return(standardized_names)
}

#' @noRd
parse_full_names_for_api_query <- function(full_names) {
  
  parsed_names_list <- purrr::map(full_names, function(full_name) {
    # Remove common titles and suffixes
    cleaned_name <- stringr::str_remove_all(full_name, 
                                            "(?i)\\b(dr\\.?|doctor|md|do)\\b")
    
    name_parts <- stringr::str_split(stringr::str_trim(cleaned_name), "\\s+")[[1]]
    name_parts <- name_parts[name_parts != ""]
    
    if (length(name_parts) >= 2) {
      tibble::tibble(
        first_name = name_parts[1],
        last_name = name_parts[length(name_parts)],
        middle_name = if (length(name_parts) > 2) {
          paste(name_parts[2:(length(name_parts)-1)], collapse = " ")
        } else {
          NA_character_
        },
        original_input = full_name
      )
    } else if (length(name_parts) == 1) {
      tibble::tibble(
        first_name = NA_character_,
        last_name = name_parts[1],
        middle_name = NA_character_,
        original_input = full_name
      )
    } else {
      tibble::tibble(
        first_name = NA_character_,
        last_name = NA_character_,
        middle_name = NA_character_,
        original_input = full_name
      )
    }
  })
  
  dplyr::bind_rows(parsed_names_list)
}

#' @noRd  
write_npi_lookup_results <- function(npi_results, output_file_path, enable_verbose_logging) {
  
  logger::log_info("Writing NPI lookup results to: {output_file_path}")
  
  # Ensure output directory exists
  output_dir <- dirname(output_file_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    logger::log_info("Created output directory: {output_dir}")
  }
  
  # Write results
  readr::write_csv(npi_results, output_file_path, na = "")
  
  # Verify output
  if (file.exists(output_file_path)) {
    file_size <- file.size(output_file_path)
    logger::log_info("Results written successfully")
    logger::log_info("Output file size: {scales::comma(file_size)} bytes")
  } else {
    logger::log_error("Failed to write output file")
  }
}

# Helper operator for NULL coalescing
`%||%` <- function(x, y) if (is.null(x)) y else x

# Replace the entire batch_npi_api_lookup function with this corrected version
batch_npi_api_lookup <- function(
    physician_names_input,
    specialty_filter = "Obstetrics & Gynecology",
    output_file_path = NULL,
    api_rate_limit_delay = API_RATE_LIMIT_DELAY_DEFAULT,
    max_results_per_query = MAX_RESULTS_PER_QUERY,
    enable_verbose_logging = TRUE
) {
  
  # Configure logging
  configure_function_logging(enable_verbose_logging)
  
  # Input validation
  validate_npi_api_inputs(physician_names_input, specialty_filter, 
                          output_file_path, api_rate_limit_delay, 
                          max_results_per_query, enable_verbose_logging)
  
  logger::log_info("=== STARTING BATCH NPI API LOOKUP ===")
  logger::log_info("Specialty filter: {specialty_filter}")
  logger::log_info("API rate limit delay: {api_rate_limit_delay} seconds")
  
  # Process input names
  processed_physician_names <- process_physician_names_for_api_lookup(
    physician_names_input, enable_verbose_logging)
  
  logger::log_info("Processed {nrow(processed_physician_names)} names for API lookup")
  
  # Perform API lookups with FIXED version parameter
  api_lookup_results <- perform_batch_nppes_api_queries_fixed(
    processed_physician_names, specialty_filter, api_rate_limit_delay,
    max_results_per_query, enable_verbose_logging)
  
  # Process and clean API results
  cleaned_npi_results <- clean_and_standardize_api_results(
    api_lookup_results, enable_verbose_logging)
  
  # Write results if output path specified
  if (!is.null(output_file_path)) {
    write_npi_lookup_results(cleaned_npi_results, output_file_path, 
                             enable_verbose_logging)
  }
  
  logger::log_info("=== BATCH NPI API LOOKUP COMPLETED ===")
  logger::log_info("Total NPIs found: {nrow(cleaned_npi_results)}")
  
  return(cleaned_npi_results)
}

#' @noRd
clean_and_standardize_api_results <- function(api_results, enable_verbose_logging) {
  
  if (nrow(api_results) == 0) {
    logger::log_warn("No API results to clean")
    return(tibble::tibble())
  }
  
  logger::log_info("Cleaning and standardizing {nrow(api_results)} API results")
  
  cleaned_results <- api_results %>%
    dplyr::mutate(
      # Standardize ZIP codes
      practice_zip_cleaned = stringr::str_sub(practice_zip, 1, 5),
      
      # Create full name for matching
      api_full_name = paste(provider_first_name, provider_last_name),
      
      # Create combined practice address
      practice_address_combined = paste(
        ifelse(is.na(practice_address_1), "", practice_address_1),
        ifelse(is.na(practice_address_2), "", practice_address_2),
        ifelse(is.na(practice_city), "", practice_city),
        ifelse(is.na(practice_state), "", practice_state),
        ifelse(is.na(practice_zip), "", practice_zip),
        sep = ", "
      ) %>% stringr::str_remove_all("^,\\s*|,\\s*$|,\\s*,"),
      
      # Clean credentials
      credential_cleaned = stringr::str_remove_all(
        stringr::str_to_upper(provider_credential %||% ""), 
        "[[:punct:][:symbol:]]"
      ),
      
      # Add result quality indicators
      has_practice_address = !is.na(practice_address_1) | !is.na(practice_city),
      has_taxonomy = !is.na(taxonomy_code),
      is_primary_specialty = taxonomy_primary == TRUE,
      
      # Add subspecialty indicator
      is_subspecialist = stringr::str_detect(
        taxonomy_description %||% "", 
        "(?i)(oncology|endocrinology|maternal|fetal|urogynecology|reconstructive)"
      )
    ) %>%
    dplyr::arrange(query_id, desc(is_primary_specialty), provider_last_name)
  
  logger::log_info("API results cleaning completed")
  
  return(cleaned_results)
}

#' @noRd
perform_batch_nppes_api_queries_fixed <- function(processed_names, specialty_filter,
                                                  api_rate_limit_delay, max_results_per_query,
                                                  enable_verbose_logging) {
  
  logger::log_info("Starting batch API queries for {nrow(processed_names)} physicians")
  
  api_base_url <- "https://npiregistry.cms.hhs.gov/api/"
  all_api_results <- list()
  successful_queries <- 0
  failed_queries <- 0
  
  for (i in seq_len(nrow(processed_names))) {
    current_physician <- processed_names[i, ]
    
    if (enable_verbose_logging && i %% 10 == 0) {
      logger::log_info("Processing query {i}/{nrow(processed_names)}")
    }
    
    # Build API query parameters with REQUIRED version parameter
    api_query_params <- list(
      version = "2.1",  # CRITICAL - was missing before!
      first_name = current_physician$api_first_name,
      last_name = current_physician$api_last_name,
      limit = max_results_per_query,
      skip = 0
    )
    
    # Add specialty filter only if not empty
    if (specialty_filter != "") {
      api_query_params$taxonomy_description <- specialty_filter
    }
    
    # Execute API query with error handling
    tryCatch({
      api_response <- httr::GET(
        url = api_base_url,
        query = api_query_params,
        timeout = 30
      )
      
      if (httr::status_code(api_response) == HTTP_OK) {
        response_content <- httr::content(api_response, "parsed")
        
        if (!is.null(response_content$results) && length(response_content$results) > 0) {
          processed_results <- process_single_api_response_fixed(
            response_content$results, current_physician)
          
          if (nrow(processed_results) > 0) {
            all_api_results[[length(all_api_results) + 1]] <- processed_results
          }
        }
        
        successful_queries <- successful_queries + 1
        
      } else {
        logger::log_warn("API query failed for {current_physician$original_input}: HTTP {httr::status_code(api_response)}")
        failed_queries <- failed_queries + 1
      }
      
    }, error = function(e) {
      logger::log_error("API query error for {current_physician$original_input}: {e$message}")
      failed_queries <- failed_queries + 1
    })
    
    # Rate limiting
    if (api_rate_limit_delay > 0) {
      Sys.sleep(api_rate_limit_delay)
    }
  }
  
  logger::log_info("API queries completed:")
  logger::log_info("  Successful: {successful_queries}")
  logger::log_info("  Failed: {failed_queries}")
  
  if (length(all_api_results) > 0) {
    combined_results <- dplyr::bind_rows(all_api_results)
  } else {
    combined_results <- tibble::tibble()
  }
  
  return(combined_results)
}

#' @noRd
process_single_api_response_fixed <- function(api_results, query_physician) {
  
  processed_results_list <- purrr::map(api_results, function(single_result) {
    
    # Extract basic provider information safely
    basic_info <- single_result$basic %||% list()
    
    # Extract addresses safely
    addresses <- single_result$addresses %||% list()
    practice_address <- NULL
    
    if (length(addresses) > 0) {
      # Find practice location address
      for (addr in addresses) {
        if (!is.null(addr$address_purpose) && addr$address_purpose == "LOCATION") {
          practice_address <- addr
          break
        }
      }
    }
    
    # Extract taxonomies safely
    taxonomies <- single_result$taxonomies %||% list()
    primary_taxonomy <- NULL
    if (length(taxonomies) > 0) {
      # Find primary taxonomy
      for (tax in taxonomies) {
        if (!is.null(tax$primary) && tax$primary == TRUE) {
          primary_taxonomy <- tax
          break
        }
      }
      # If no primary found, use first one
      if (is.null(primary_taxonomy)) {
        primary_taxonomy <- taxonomies[[1]]
      }
    }
    
    # Create standardized result with safe extraction
    tibble::tibble(
      query_id = query_physician$query_id %||% NA_real_,
      original_input_name = query_physician$original_input %||% NA_character_,
      abog_id = if ("abog_id" %in% names(query_physician)) query_physician$abog_id else NA_real_,
      npi = single_result$number %||% NA_character_,
      provider_first_name = basic_info$first_name %||% NA_character_,
      provider_last_name = basic_info$last_name %||% NA_character_,
      provider_middle_name = basic_info$middle_name %||% NA_character_,
      provider_credential = basic_info$credential %||% NA_character_,
      provider_sex = basic_info$sex %||% NA_character_,
      enumeration_date = basic_info$enumeration_date %||% NA_character_,
      last_updated = basic_info$last_updated %||% NA_character_,
      status = basic_info$status %||% NA_character_,
      practice_address_1 = if (!is.null(practice_address)) practice_address$address_1 %||% NA_character_ else NA_character_,
      practice_address_2 = if (!is.null(practice_address)) practice_address$address_2 %||% NA_character_ else NA_character_,
      practice_city = if (!is.null(practice_address)) practice_address$city %||% NA_character_ else NA_character_,
      practice_state = if (!is.null(practice_address)) practice_address$state %||% NA_character_ else NA_character_,
      practice_zip = if (!is.null(practice_address)) practice_address$postal_code %||% NA_character_ else NA_character_,
      practice_phone = if (!is.null(practice_address)) practice_address$telephone_number %||% NA_character_ else NA_character_,
      taxonomy_code = if (!is.null(primary_taxonomy)) primary_taxonomy$code %||% NA_character_ else NA_character_,
      taxonomy_description = if (!is.null(primary_taxonomy)) primary_taxonomy$desc %||% NA_character_ else NA_character_,
      taxonomy_state = if (!is.null(primary_taxonomy)) primary_taxonomy$state %||% NA_character_ else NA_character_,
      taxonomy_license = if (!is.null(primary_taxonomy)) primary_taxonomy$license %||% NA_character_ else NA_character_,
      taxonomy_primary = if (!is.null(primary_taxonomy)) primary_taxonomy$primary %||% FALSE else FALSE,
      api_query_timestamp = Sys.time()
    )
  })
  
  # Safely bind rows
  tryCatch({
    dplyr::bind_rows(processed_results_list)
  }, error = function(e) {
    logger::log_error("Error binding API results: {e$message}")
    return(tibble::tibble())
  })
}

# run ----
# Better approach - preserve ABOG IDs for matching
# abog_names <- "data/0-Download/output/best_abog_provider_dataframe_8_17_2025_2053.csv"
abog_names <- ABOG_PROVIDER_DATA_FILE

# Read the full dataset first to see what columns we have
abog_full_data <- read_csv(abog_names, show_col_types = FALSE)

# Check the column names to see what's available
glimpse(abog_full_data)

# Prepare names with ABOG IDs, keeping the certification column for filtering
names_with_abog_ids <- abog_full_data %>%
  mutate(abog_id = as.numeric(abog_id)) %>%
  select(abog_id, physician_name, first, last, primary_certification) %>%
  rename(
    first_name = first,
    last_name = last,
    original_input = physician_name
  )

# Run NPI lookup for OB/GYN certified physicians only
obgyn_taxonomy_full_abog_with_ids <- batch_npi_api_lookup(
  physician_names_input = names_with_abog_ids,
  specialty_filter = "Obstetrics & Gynecology", 
  output_file_path = OBGYN_TAXONOMY_OUTPUT_FILE,
  api_rate_limit_delay = API_RATE_LIMIT_DELAY_SLOW,
  enable_verbose_logging = TRUE
)

urology_taxonomy_full_abog_with_ids <- batch_npi_api_lookup(
  physician_names_input = names_with_abog_ids,
  specialty_filter = "Urology",
  output_file_path = UROLOGY_TAXONOMY_OUTPUT_FILE,
  api_rate_limit_delay = API_RATE_LIMIT_DELAY_SLOW,
  enable_verbose_logging = TRUE
)


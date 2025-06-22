#######################
source("R/01-setup.R")


#````````````````
# I believe that the API from NPPES rate limits so I can only get 800 unique NPIs despite doing thousands of searches.  Best to use the database approach to download the latest version of NPPES and parse it out that way.  
#````````````````


# ---------------------------------------------------------------------------
# Constants for subspecialty searches and taxonomy descriptions
# ---------------------------------------------------------------------------
SUBSPECIALTIES <- c(
  "Gynecologic Oncology",
  "Urogynecology and Reconstructive Pelvic Surgery",
  "Reproductive Endocrinology",
  "Maternal & Fetal Medicine"
)

TAXONOMY_DESCRIPTIONS <- c(
  "Obstetrics & Gynecology, Female Pelvic Medicine and Reconstructive Surgery",
  "Obstetrics & Gynecology, Gynecologic Oncology",
  "Obstetrics & Gynecology, Maternal & Fetal Medicine",
  "Obstetrics & Gynecology, Reproductive Endocrinology",
  "Obstetrics & Gynecology, Urogynecology and Reconstructive Pelvic Surgery"
)

TAXONOMY_RECODE_MAP <- c(
  "Obstetrics & Gynecology, Female Pelvic Medicine and Reconstructive Surgery" = "FPM",
  "Obstetrics & Gynecology, Gynecologic Oncology" = "ONC",
  "Obstetrics & Gynecology, Maternal & Fetal Medicine" = "MFM",
  "Obstetrics & Gynecology, Reproductive Endocrinology" = "REI"
)
#######################

# The code you provided appears to be R code that performs the following tasks:
# 1. It sources an R script called "01-setup.R," which likely contains some setup or configuration code.
# 2. It defines a function called "search_by_taxonomy" to retrieve data related to different subspecialties in the field of Obstetrics & Gynecology (OBGYN).
# 3. It calls the "search_by_taxonomy" function four times with different subspecialties: "Gynecologic Oncology," "Female Pelvic Medicine and Reconstructive Surgery," "Reproductive Endocrinology," and "Maternal & Fetal Medicine."
# 4. It then merges the data obtained from the four calls into a single data frame called "all_taxonomy_search_data" using the "bind_rows" function from the dplyr package.
# 5. The code cleans and processes the "all_taxonomy_search_data" data frame. It filters the data to keep only rows with specific taxonomy descriptions related to OBGYN subspecialties, extracts the first five characters of the postal code, converts the "basic_enumeration_date" to a date object and extracts the year, shortens the "basic_middle_name" to a single character, and removes punctuation from names.
# 6. It renames and recodes some columns for clarity and consistency.
# 7. Finally, it writes the cleaned data frame to an RDS file named "end_cleaned_all_taxonomy_search_data.rds" in the "data/02-search_taxonomy" directory.

##########################################################################
#' Search National Provider Identifier (NPI) Registry
#'
#' This function searches the CMS NPI Registry API for healthcare providers
#' based on various criteria and returns a cleaned tibble with provider
#' information.
#'
#' @param taxonomy_description Character string. The taxonomy description to
#'   search for (e.g., "Obstetrics & Gynecology", "Family Medicine").
#' @param taxonomy Character string or NULL. The primary taxonomy code to
#'   search for (e.g., "207V00000X", "208000000X"). Default is NULL.
#' @param enumeration_type Character string or NULL. Type of enumeration
#'   ("NPI-1" for individuals, "NPI-2" for organizations). Default is "NPI-1"
#'   to search only individual providers.
#' @param first_name Character string or NULL. First name of provider for
#'   individual searches. Default is NULL.
#' @param last_name Character string or NULL. Last name of provider for
#'   individual searches. Default is NULL.
#' @param organization_name Character string or NULL. Organization name for
#'   organization searches. Default is NULL.
#' @param state Character string or NULL. Two-letter state abbreviation
#'   (e.g., "CA", "NY"). Default is NULL.
#' @param city Character string or NULL. City name. Default is NULL.
#' @param postal_code Character string or NULL. ZIP code or postal code.
#'   Default is NULL.
#' @param country_code Character string or NULL. Two-letter country code
#'   (e.g., "US"). Default is "US".
#' @param delay_seconds Numeric. Number of seconds to wait between API
#'   requests as a courtesy to the server. Default is 0.5 seconds.
#' @param max_records Numeric. Maximum number of records to retrieve for
#'   safety. Default is 10000 to prevent long processing times.
#' @param output_csv Character string or NULL. File path to save results as
#'   CSV. If NULL (default), no CSV file is written. Example: "npi_results.csv"
#'   or "/path/to/output.csv".
#' @param filter_mode Character string. How to filter taxonomy descriptions:
#'   "starts_with" (default) requires exact match at beginning, "contains" 
#'   allows the search term anywhere in the taxonomy description.
#' @param debug_structure Logical. Whether to examine and log the structure
#'   of the first API response for debugging. Default is FALSE.
#' @param verbose Logical. Whether to enable detailed logging to console.
#'   Default is TRUE.
#'
#' @return A tibble containing provider information with columns for NPI,
#'   basic information, addresses, and taxonomies.
#'
#' @examples
#' # Example 1: Search by taxonomy description with "contains" filter
#' gynecologic_oncology <- npi_search_all(
#'   taxonomy_description = "Gynecologic Oncology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   state = "CA",
#'   delay_seconds = 0.5,
#'   max_records = 5000,
#'   output_csv = "gynecologic_oncology_providers.csv",
#'   filter_mode = "contains",
#'   debug_structure = FALSE,
#'   verbose = TRUE
#' )
#' # Finds providers with "Gynecologic Oncology" anywhere in taxonomy
#'
#' # Example 2: Search by specific taxonomy code
#' obgyn_by_code <- npi_search_all(
#'   taxonomy_description = "Obstetrics & Gynecology",
#'   taxonomy = "207V00000X",
#'   enumeration_type = "NPI-1",
#'   city = "Boston",
#'   state = "MA",
#'   country_code = "US",
#'   delay_seconds = 0.2,
#'   max_records = 1000,
#'   output_csv = "data/obgyn_boston.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = FALSE
#' )
#' # Searches for specific taxonomy code 207V00000X (Obstetrics & Gynecology)
#'
#' # Example 3: Large search with flexible matching
#' timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#' oncology_broad <- npi_search_all(
#'   taxonomy_description = "Oncology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   state = "TX",
#'   country_code = "US",
#'   delay_seconds = 1.0,
#'   max_records = 25000,
#'   output_csv = paste0("oncology_providers_", timestamp, ".csv"),
#'   filter_mode = "contains",
#'   debug_structure = FALSE,
#'   verbose = TRUE
#' )
#' # Finds all providers with "Oncology" anywhere in their taxonomy
#'
#' @importFrom httr GET stop_for_status content
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows filter
#' @importFrom purrr map_dfr
#' @importFrom stringr str_starts str_detect fixed
#' @importFrom readr write_csv
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom logger log_info log_warn log_error
#' @export
npi_search_all <- function(taxonomy_description,
                           taxonomy = NULL,
                           enumeration_type = "NPI-1",
                           first_name = NULL,
                           last_name = NULL,
                           organization_name = NULL,
                           state = NULL,
                           city = NULL,
                           postal_code = NULL,
                           country_code = "US",
                           delay_seconds = 0.5,
                           max_records = 10000,
                           output_csv = NULL,
                           filter_mode = "starts_with",
                           debug_structure = FALSE,
                           verbose = TRUE) {
  
  # Input validation
  assertthat::assert_that(assertthat::is.string(taxonomy_description))
  assertthat::assert_that(is.null(taxonomy) || 
                            assertthat::is.string(taxonomy))
  assertthat::assert_that(is.null(enumeration_type) || 
                            assertthat::is.string(enumeration_type))
  assertthat::assert_that(is.null(first_name) || 
                            assertthat::is.string(first_name))
  assertthat::assert_that(is.null(last_name) || 
                            assertthat::is.string(last_name))
  assertthat::assert_that(is.null(organization_name) || 
                            assertthat::is.string(organization_name))
  assertthat::assert_that(is.null(state) || 
                            assertthat::is.string(state))
  assertthat::assert_that(is.null(city) || 
                            assertthat::is.string(city))
  assertthat::assert_that(is.null(postal_code) || 
                            assertthat::is.string(postal_code))
  assertthat::assert_that(is.null(country_code) || 
                            assertthat::is.string(country_code))
  assertthat::assert_that(is.numeric(delay_seconds) && delay_seconds >= 0)
  assertthat::assert_that(is.numeric(max_records) && max_records > 0)
  assertthat::assert_that(is.null(output_csv) || 
                            assertthat::is.string(output_csv))
  assertthat::assert_that(assertthat::is.string(filter_mode))
  assertthat::assert_that(filter_mode %in% c("starts_with", "contains"))
  assertthat::assert_that(assertthat::is.flag(debug_structure))
  assertthat::assert_that(assertthat::is.flag(verbose))
  
  if (verbose) {
    logger::log_info("Starting NPI registry search")
    logger::log_info("Input parameters:")
    logger::log_info("  taxonomy_description: {taxonomy_description}")
    logger::log_info("  taxonomy: {taxonomy %||% 'NULL'}")
    logger::log_info("  enumeration_type: {enumeration_type}")
    logger::log_info("  first_name: {first_name %||% 'NULL'}")
    logger::log_info("  last_name: {last_name %||% 'NULL'}")
    logger::log_info("  organization_name: {organization_name %||% 'NULL'}")
    logger::log_info("  state: {state %||% 'NULL'}")
    logger::log_info("  city: {city %||% 'NULL'}")
    logger::log_info("  postal_code: {postal_code %||% 'NULL'}")
    logger::log_info("  country_code: {country_code %||% 'NULL'}")
    logger::log_info("  delay_seconds: {delay_seconds}")
    logger::log_info("  max_records: {max_records}")
    logger::log_info("  output_csv: {output_csv %||% 'NULL'}")
    logger::log_info("  filter_mode: {filter_mode}")
    logger::log_info("  debug_structure: {debug_structure}")
  }
  
  # Build API parameters
  api_parameters <- .build_api_parameters(
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code,
    verbose = verbose
  )
  
  # Fetch all provider records
  provider_records <- .fetch_all_provider_records(
    api_parameters = api_parameters,
    delay_seconds = delay_seconds,
    max_records = max_records,
    debug_structure = debug_structure,
    verbose = verbose
  )
  
  # Process and clean the data
  cleaned_provider_data <- .process_provider_records(
    provider_records = provider_records,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("NPI search completed successfully")
    logger::log_info("Final dataset dimensions: {nrow(cleaned_provider_data)} rows, {ncol(cleaned_provider_data)} columns")
    logger::log_info("Output columns: {paste(colnames(cleaned_provider_data), collapse = ', ')}")
  }
  
  # Write to CSV if output path is provided
  if (!is.null(output_csv)) {
    if (verbose) {
      logger::log_info("Writing results to CSV file: {output_csv}")
    }
    
    # Create directory if it doesn't exist
    output_dir <- dirname(output_csv)
    if (!dir.exists(output_dir) && output_dir != ".") {
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      if (verbose) {
        logger::log_info("Created directory: {output_dir}")
      }
    }
    
    # Write the CSV file
    readr::write_csv(cleaned_provider_data, output_csv, na = "")
    
    if (verbose) {
      logger::log_info("Successfully saved {nrow(cleaned_provider_data)} records to: {normalizePath(output_csv, mustWork = FALSE)}")
      logger::log_info("CSV file size: {file.size(output_csv)} bytes")
    }
  }
  
  # Print API specification information
  cat("https://npiregistry.cms.hhs.gov/api-page contains the API specifications.\n")
  
  return(cleaned_provider_data)
}

#' Build API parameters for NPI registry search
#' @noRd
.build_api_parameters <- function(taxonomy_description,
                                  taxonomy,
                                  enumeration_type,
                                  first_name,
                                  last_name,
                                  organization_name,
                                  state,
                                  city,
                                  postal_code,
                                  country_code,
                                  verbose) {
  
  api_params <- list(
    version = "2.1",
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code
  )
  
  # Remove NULL parameters
  api_params <- api_params[!sapply(api_params, is.null)]
  
  if (verbose) {
    logger::log_info("Built API parameters with {length(api_params)} non-null values")
  }
  
  return(api_params)
}

#' Fetch all provider records from NPI API with pagination
#' @noRd
.fetch_all_provider_records <- function(api_parameters, delay_seconds, max_records, debug_structure, verbose) {
  
  npi_base_url <- "https://npiregistry.cms.hhs.gov/api/"
  records_per_request <- 200
  current_skip <- 0
  all_provider_records <- list()
  request_count <- 0
  
  if (verbose) {
    logger::log_info("Starting API data retrieval with pagination")
    logger::log_info("Using {delay_seconds} second delay between requests")
    logger::log_info("Maximum records limit set to: {max_records}")
  }
  
  repeat {
    request_count <- request_count + 1
    api_parameters$limit <- records_per_request
    api_parameters$skip <- current_skip
    
    # Add delay before request (except for the first request)
    if (request_count > 1 && delay_seconds > 0) {
      if (verbose) {
        logger::log_info("Waiting {delay_seconds} seconds before next request...")
      }
      Sys.sleep(delay_seconds)
    }
    
    if (verbose) {
      logger::log_info("Making API request #{request_count}, skip={current_skip}, limit={records_per_request}")
    }
    
    # Make API request
    api_response <- httr::GET(npi_base_url, query = api_parameters)
    httr::stop_for_status(api_response)
    
    parsed_response <- httr::content(api_response, as = "parsed", type = "application/json")
    
    if (verbose) {
      logger::log_info("API response metadata - result_count: {parsed_response$result_count %||% 'NULL'}")
    }
    
    # Debug structure on first request if requested
    if (debug_structure && request_count == 1 && !is.null(parsed_response$results) && length(parsed_response$results) > 0) {
      .debug_npi_structure(parsed_response$results[[1]], verbose)
    }
    
    # Check if we have results
    if (is.null(parsed_response$results) || length(parsed_response$results) == 0) {
      if (verbose) {
        logger::log_info("No results found in this request, stopping pagination")
      }
      break
    }
    
    current_batch_size <- length(parsed_response$results)
    
    # Add results to our collection
    all_provider_records <- append(all_provider_records, parsed_response$results)
    current_skip <- current_skip + records_per_request
    
    if (verbose) {
      logger::log_info("Retrieved {current_batch_size} records in this batch, total accumulated: {length(all_provider_records)}")
    }
    
    # Stop pagination if we got fewer results than requested (indicates end of data)
    if (current_batch_size < records_per_request) {
      if (verbose) {
        logger::log_info("Received {current_batch_size} records (less than requested {records_per_request}), indicating end of available data")
      }
      break
    }
    
    # Safety check to prevent processing too many records
    if (length(all_provider_records) >= max_records) {
      if (verbose) {
        logger::log_warn("Reached maximum record limit ({max_records}), stopping pagination for performance")
      }
      break
    }
  }
  
  if (verbose) {
    logger::log_info("API retrieval completed: {length(all_provider_records)} total provider records fetched in {request_count} requests")
  }
  
  return(all_provider_records)
}

#' Debug function to examine NPI record structure
#' @noRd
.debug_npi_structure <- function(sample_record, verbose) {
  
  if (verbose) {
    logger::log_info("=== DEBUGGING: NPI RECORD STRUCTURE ===")
    logger::log_info("Top-level fields in NPI record:")
    
    # Show all top-level field names
    top_level_fields <- names(sample_record)
    for (field in top_level_fields) {
      field_type <- class(sample_record[[field]])[1]
      logger::log_info("  {field}: {field_type}")
    }
    
    # Look for NPI in various locations
    logger::log_info("=== SEARCHING FOR NPI NUMBER ===")
    
    # Check top level
    if (!is.null(sample_record$npi)) {
      logger::log_info("Found NPI at top level: {sample_record$npi}")
    } else {
      logger::log_info("No NPI field at top level")
    }
    
    if (!is.null(sample_record$number)) {
      logger::log_info("Found 'number' at top level: {sample_record$number}")
    } else {
      logger::log_info("No 'number' field at top level")
    }
    
    # Check basic section
    if (!is.null(sample_record$basic)) {
      logger::log_info("Basic section fields:")
      basic_fields <- names(sample_record$basic)
      for (field in basic_fields) {
        value <- sample_record$basic[[field]]
        if (is.character(value) && length(value) == 1) {
          logger::log_info("  basic${field}: {value}")
        } else {
          logger::log_info("  basic${field}: {class(value)[1]}")
        }
      }
    }
    
    # Show a sample of each major section
    if (!is.null(sample_record$addresses) && length(sample_record$addresses) > 0) {
      logger::log_info("Address section fields (first address):")
      addr_fields <- names(sample_record$addresses[[1]])
      for (field in addr_fields) {
        value <- sample_record$addresses[[1]][[field]]
        if (is.character(value) && length(value) == 1) {
          logger::log_info("  addresses[[1]]${field}: {value}")
        } else {
          logger::log_info("  addresses[[1]]${field}: {class(value)[1]}")
        }
      }
    }
    
    if (!is.null(sample_record$taxonomies) && length(sample_record$taxonomies) > 0) {
      logger::log_info("Taxonomy section fields (first taxonomy):")
      tax_fields <- names(sample_record$taxonomies[[1]])
      for (field in tax_fields) {
        value <- sample_record$taxonomies[[1]][[field]]
        if (is.character(value) && length(value) == 1) {
          logger::log_info("  taxonomies[[1]]${field}: {value}")
        } else {
          logger::log_info("  taxonomies[[1]]${field}: {class(value)[1]}")
        }
      }
    }
    
    logger::log_info("=== END DEBUG STRUCTURE ===")
  }
}

#' Process and clean provider records into a tibble
#' @noRd
.process_provider_records <- function(provider_records, taxonomy_description, filter_mode, verbose) {
  
  if (length(provider_records) == 0) {
    if (verbose) {
      logger::log_warn("No provider records to process, returning empty tibble")
    }
    return(tibble::tibble())
  }
  
  total_records <- length(provider_records)
  
  if (verbose) {
    logger::log_info("Processing {total_records} provider records into tibble")
    logger::log_info("This may take a moment for large datasets...")
  }
  
  # Process in batches for better performance and progress tracking
  batch_size <- 1000
  processed_batches <- list()
  
  for (batch_start in seq(1, total_records, by = batch_size)) {
    batch_end <- min(batch_start + batch_size - 1, total_records)
    
    if (verbose) {
      logger::log_info("Processing records {batch_start} to {batch_end} ({round((batch_end/total_records)*100, 1)}% complete)")
    }
    
    current_batch <- provider_records[batch_start:batch_end]
    
    # Process current batch (removed .id parameter to eliminate record_index)
    batch_tibble <- purrr::map_dfr(current_batch, .extract_provider_info)
    processed_batches[[length(processed_batches) + 1]] <- batch_tibble
  }
  
  # Combine all batches
  if (verbose) {
    logger::log_info("Combining all processed batches into final tibble...")
  }
  
  final_processed_data <- dplyr::bind_rows(processed_batches)
  
  # Filter taxonomy descriptions based on selected mode
  if (verbose) {
    original_count <- nrow(final_processed_data)
    filter_desc <- if (filter_mode == "starts_with") {
      "must start with"
    } else {
      "must contain"
    }
    logger::log_info("Applying taxonomy filter ({filter_mode}): {filter_desc} '{taxonomy_description}'")
    
    # Debug: Show sample taxonomy descriptions before filtering
    if (original_count > 0) {
      sample_taxonomies <- final_processed_data$primary_taxonomy_description[1:min(10, original_count)]
      sample_taxonomies <- sample_taxonomies[!is.na(sample_taxonomies)]
      if (length(sample_taxonomies) > 0) {
        logger::log_info("Sample taxonomy descriptions found in data:")
        for (i in seq_along(sample_taxonomies)) {
          logger::log_info("  {i}: '{sample_taxonomies[i]}'")
        }
      }
    }
  }
  
  # Apply the appropriate filter based on mode
  if (filter_mode == "starts_with") {
    filtered_provider_data <- final_processed_data |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_starts(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  } else if (filter_mode == "contains") {
    filtered_provider_data <- final_processed_data |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_detect(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  }
  
  if (verbose) {
    filtered_count <- nrow(filtered_provider_data)
    removed_count <- original_count - filtered_count
    logger::log_info("Taxonomy filtering completed: kept {filtered_count} records, removed {removed_count} records")
    
    # If no matches found, suggest alternatives
    if (filtered_count == 0 && original_count > 0) {
      logger::log_warn("No records matched the taxonomy filter!")
      logger::log_info("Consider using a different filter_mode or check taxonomy spelling")
      
      # Show unique taxonomy descriptions that contain the search term (regardless of current mode)
      containing_taxonomies <- final_processed_data |>
        dplyr::filter(!is.na(primary_taxonomy_description) & 
                        stringr::str_detect(primary_taxonomy_description, 
                                            stringr::fixed(taxonomy_description))) |>
        dplyr::distinct(primary_taxonomy_description) |>
        dplyr::slice_head(n = 5)
      
      if (nrow(containing_taxonomies) > 0) {
        logger::log_info("Found {nrow(containing_taxonomies)} unique taxonomy descriptions CONTAINING '{taxonomy_description}':")
        for (i in 1:nrow(containing_taxonomies)) {
          logger::log_info("  - '{containing_taxonomies$primary_taxonomy_description[i]}'")
        }
        if (filter_mode == "starts_with") {
          logger::log_info("Try using filter_mode = 'contains' to capture these records")
        }
      }
    }
    
    logger::log_info("Provider records processed into {nrow(filtered_provider_data)} rows")
    logger::log_info("Data transformation completed - extracted key provider information")
  }
  
  return(filtered_provider_data)
}

#' Extract key information from a single provider record
#' @noRd
.extract_provider_info <- function(single_provider_record) {
  
  # Extract basic information
  basic_info <- single_provider_record$basic %||% list()
  
  # Extract primary address (usually the first address)
  primary_address <- if (length(single_provider_record$addresses) > 0) {
    single_provider_record$addresses[[1]]
  } else {
    list()
  }
  
  # Extract primary taxonomy (usually the first taxonomy)
  primary_taxonomy <- if (length(single_provider_record$taxonomies) > 0) {
    single_provider_record$taxonomies[[1]]
  } else {
    list()
  }
  
  # Build the flattened record - try multiple possible locations for NPI
  flattened_provider <- tibble::tibble(
    npi = single_provider_record$number %||% 
      basic_info$npi %||% 
      single_provider_record$npi %||% 
      NA_character_,
    entity_type_code = basic_info$enumeration_type %||% NA_character_,
    entity_type = if (!is.null(basic_info$enumeration_type)) {
      if (basic_info$enumeration_type == "NPI-1") "Individual" else "Organization"
    } else {
      NA_character_
    },
    first_name = basic_info$first_name %||% NA_character_,
    last_name = basic_info$last_name %||% NA_character_,
    middle_name = basic_info$middle_name %||% NA_character_,
    organization_name = basic_info$organization_name %||% NA_character_,
    gender = basic_info$gender %||% NA_character_,
    sole_proprietor = basic_info$sole_proprietor %||% NA_character_,
    primary_address_line1 = primary_address$address_1 %||% NA_character_,
    primary_address_line2 = primary_address$address_2 %||% NA_character_,
    primary_city = primary_address$city %||% NA_character_,
    primary_state = primary_address$state %||% NA_character_,
    primary_postal_code = primary_address$postal_code %||% NA_character_,
    primary_country_code = primary_address$country_code %||% NA_character_,
    primary_telephone = primary_address$telephone_number %||% NA_character_,
    primary_fax = primary_address$fax_number %||% NA_character_,
    primary_taxonomy_code = primary_taxonomy$code %||% NA_character_,
    primary_taxonomy_description = primary_taxonomy$desc %||% NA_character_,
    primary_taxonomy_license = primary_taxonomy$license %||% NA_character_,
    primary_taxonomy_state = primary_taxonomy$state %||% NA_character_,
    is_primary_taxonomy = primary_taxonomy$primary %||% NA
  )
  
  return(flattened_provider)
}


# Execute ----
generalists <- npi_search_all(
  taxonomy_description = "Obstetrics & Gynecology",
  debug_structure = TRUE,
  max_records = 50000L,
  verbose = TRUE,
  delay_seconds = 0.5,
  enumeration_type = "NPI-1",
  country_code = "US",
  output_csv = paste0("data/02-search_taxonomy/nppes_npi_search_all_obgyn.csv")
)

# Saves each subspecialty as an individual CSV and dataframe
go_search_by_taxonomy_data <- npi_search_all(
  taxonomy_description = "Gynecologic Oncology",
  filter_mode = "contains",
  output_csv = paste0(
    "data/02-search_taxonomy/go_nppes_npi_search_all_obgyn.csv"
  )
)

fpmrs_search_by_taxonomy_data <- npi_search_all(
  taxonomy_description = "Urogynecology and Reconstructive Pelvic Surgery",
  filter_mode = "contains",
  output_csv = paste0(
    "data/02-search_taxonomy/fprms_nppes_npi_search_all_obgyn.csv"
  )
)
rei_search_by_taxonomy_data <- npi_search_all(
  taxonomy_description = "Reproductive Endocrinology",
  filter_mode = "contains",
  output_csv = paste0(
    "data/02-search_taxonomy/rei_nppes_npi_search_all_obgyn.csv"
  )
)

mfm_search_by_taxonomy_data <- npi_search_all(
  taxonomy = "207VM0101X",
  output_csv = paste0(
    "data/02-search_taxonomy/mfm_nppes_npi_search_all_obgyn.csv"
  )
)



# Merge all data frames of each of the subspecialties into one
all_taxonomy_search_data <- dplyr::bind_rows(generalists,
                                             go_search_by_taxonomy_data,
                                             #fpmrs_search_by_taxonomy_data,
                                             #rei_search_by_taxonomy_data,
                                             #mfm_search_by_taxonomy_data
                                             ) %>%
 dplyr::distinct(npi, .keep_all = TRUE)


cleaned_all_taxonomy_search_data <-
  all_taxonomy_search_data %>%
  distinct(npi, .keep_all = TRUE) %>%
  # Keep only the OBGYN subspecialist taxonomy descriptions.
  filter(
    taxonomies_desc %in% TAXONOMY_DESCRIPTIONS
  ) %>%
  mutate(addresses_postal_code = str_sub(addresses_postal_code,1 ,5)) %>% # Extract the first five of the zip code
  mutate(basic_enumeration_date = ymd(basic_enumeration_date)) %>%
  mutate(basic_enumeration_date_year = year(basic_enumeration_date), .after = ifelse("basic_enumeration_date" %in% names(.), "basic_enumeration_date", last_col())) %>% # Pull the year out of the enumeration full data.
  mutate(basic_middle_name = str_sub(basic_middle_name,1 ,1)) %>%
  mutate(across(c(basic_first_name, basic_last_name, basic_middle_name), .fns = ~str_remove_all(., "[[\\p{P}][\\p{S}]]"))) %>%
  # Get data ready to add these taxonomy rows to the `search_npi`/GOBA data set.

  rename(NPI = npi, first_name = basic_first_name, last_name = basic_last_name, middle_name = basic_middle_name, GenderPhysicianCompare = basic_gender, sub1 = taxonomies_desc, city = addresses_city, state = addresses_state, name.x = full_name, `Zip CodePhysicianCompare` = addresses_postal_code) %>%
  mutate(GenderPhysicianCompare = recode(GenderPhysicianCompare, "F" = "Female", "M" = "Male")) %>%

  # Show the subspecialty from goba.
  mutate(sub1 = recode(sub1, !!!TAXONOMY_RECODE_MAP))

 
write_rds(cleaned_all_taxonomy_search_data, "data/02-search_taxonomy/end_cleaned_all_taxonomy_search_data.rds")

#**********************************************
# SANITY CHECK
#**********************************************
#* cleaned_all_taxonomy_search_data <- read_rds("data/02-search_taxonomy/end_cleaned_all_taxonomy_search_data.rds")
dim(cleaned_all_taxonomy_search_data)
glimpse(cleaned_all_taxonomy_search_data)
janitor::tabyl(cleaned_all_taxonomy_search_data$sub1)
janitor::tabyl(cleaned_all_taxonomy_search_data$GenderPhysicianCompare)
janitor::tabyl(cleaned_all_taxonomy_search_data$state)


# function at 1601 ----
##########################################################################
#' Search National Provider Identifier (NPI) Registry with Deduplication
#'
#' This function searches the CMS NPI Registry API for healthcare providers
#' based on various criteria and returns a cleaned tibble with provider
#' information. Automatically deduplicates providers by NPI number to prevent
#' duplicate entries from multiple taxonomies or practice locations.
#'
#' @param taxonomy_description Character string. The taxonomy description to
#'   search for (e.g., "Obstetrics & Gynecology", "Family Medicine").
#' @param taxonomy Character string or NULL. The primary taxonomy code to
#'   search for (e.g., "207V00000X", "208000000X"). Default is NULL.
#' @param enumeration_type Character string or NULL. Type of enumeration
#'   ("NPI-1" for individuals, "NPI-2" for organizations). Default is "NPI-1"
#'   to search only individual providers.
#' @param first_name Character string or NULL. First name of provider for
#'   individual searches. Default is NULL.
#' @param last_name Character string or NULL. Last name of provider for
#'   individual searches. Default is NULL.
#' @param organization_name Character string or NULL. Organization name for
#'   organization searches. Default is NULL.
#' @param state Character string or NULL. Two-letter state abbreviation
#'   (e.g., "CA", "NY"). Default is NULL.
#' @param city Character string or NULL. City name. Default is NULL.
#' @param postal_code Character string or NULL. ZIP code or postal code.
#'   Default is NULL.
#' @param country_code Character string or NULL. Two-letter country code
#'   (e.g., "US"). Default is "US".
#' @param delay_seconds Numeric. Number of seconds to wait between API
#'   requests as a courtesy to the server. Default is 0.5 seconds.
#' @param max_records Numeric. Maximum number of records to retrieve for
#'   safety. Default is 10000 to prevent long processing times.
#' @param output_csv Character string or NULL. File path to save results as
#'   CSV. If NULL (default), no CSV file is written.
#' @param filter_mode Character string. How to filter taxonomy descriptions:
#'   "starts_with" (default) requires exact match at beginning, "contains" 
#'   allows the search term anywhere in the taxonomy description.
#' @param debug_structure Logical. Whether to examine and log the structure
#'   of the first API response for debugging. Default is FALSE.
#' @param verbose Logical. Whether to enable detailed logging to console.
#'   Default is TRUE.
#'
#' @return A tibble containing unique provider information (deduplicated by
#'   NPI number) with columns for basic information, addresses, and taxonomies.
#'
#' @examples
#' # Example 1: Search gynecologic oncology with contains filter and CSV output
#' gynecologic_oncology_providers <- npi_search_all(
#'   taxonomy_description = "Gynecologic Oncology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   state = "CA",
#'   delay_seconds = 0.5,
#'   max_records = 5000,
#'   output_csv = "gynecologic_oncology_providers.csv",
#'   filter_mode = "contains",
#'   debug_structure = FALSE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with unique providers containing "Gynecologic Oncology"
#' # Output: CSV file saved to working directory
#'
#' # Example 2: Search specific taxonomy code with city filter
#' obgyn_boston_providers <- npi_search_all(
#'   taxonomy_description = "Obstetrics & Gynecology",
#'   taxonomy = "207V00000X",
#'   enumeration_type = "NPI-1",
#'   city = "Boston",
#'   state = "MA",
#'   country_code = "US",
#'   delay_seconds = 0.2,
#'   max_records = 1000,
#'   output_csv = "data/obgyn_boston.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = FALSE
#' )
#' # Returns: tibble with unique OB/GYN providers in Boston
#' # Output: CSV saved to data/ subdirectory
#'
#' # Example 3: Large oncology search with timestamp and debugging
#' timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#' oncology_providers_broad <- npi_search_all(
#'   taxonomy_description = "Oncology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   state = "TX",
#'   country_code = "US",
#'   delay_seconds = 1.0,
#'   max_records = 25000,
#'   output_csv = paste0("oncology_providers_", timestamp, ".csv"),
#'   filter_mode = "contains",
#'   debug_structure = TRUE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with unique providers containing "Oncology" in Texas
#' # Output: Timestamped CSV file with debug structure logging
#'
#' @importFrom httr GET stop_for_status content
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows filter distinct arrange slice_head
#' @importFrom purrr map_dfr
#' @importFrom stringr str_starts str_detect fixed
#' @importFrom readr write_csv
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom logger log_info log_warn log_error
#' @export
npi_search_all <- function(taxonomy_description,
                           taxonomy = NULL,
                           enumeration_type = "NPI-1",
                           first_name = NULL,
                           last_name = NULL,
                           organization_name = NULL,
                           state = NULL,
                           city = NULL,
                           postal_code = NULL,
                           country_code = "US",
                           delay_seconds = 0.5,
                           max_records = 10000,
                           output_csv = NULL,
                           filter_mode = "starts_with",
                           debug_structure = FALSE,
                           verbose = TRUE) {
  
  # Input validation with assertthat
  assertthat::assert_that(assertthat::is.string(taxonomy_description),
                          msg = "taxonomy_description must be a single character string")
  assertthat::assert_that(is.null(taxonomy) || assertthat::is.string(taxonomy),
                          msg = "taxonomy must be NULL or a single character string")
  assertthat::assert_that(is.null(enumeration_type) || assertthat::is.string(enumeration_type),
                          msg = "enumeration_type must be NULL or a single character string")
  assertthat::assert_that(is.null(first_name) || assertthat::is.string(first_name),
                          msg = "first_name must be NULL or a single character string")
  assertthat::assert_that(is.null(last_name) || assertthat::is.string(last_name),
                          msg = "last_name must be NULL or a single character string")
  assertthat::assert_that(is.null(organization_name) || assertthat::is.string(organization_name),
                          msg = "organization_name must be NULL or a single character string")
  assertthat::assert_that(is.null(state) || assertthat::is.string(state),
                          msg = "state must be NULL or a single character string")
  assertthat::assert_that(is.null(city) || assertthat::is.string(city),
                          msg = "city must be NULL or a single character string")
  assertthat::assert_that(is.null(postal_code) || assertthat::is.string(postal_code),
                          msg = "postal_code must be NULL or a single character string")
  assertthat::assert_that(is.null(country_code) || assertthat::is.string(country_code),
                          msg = "country_code must be NULL or a single character string")
  assertthat::assert_that(is.numeric(delay_seconds) && delay_seconds >= 0,
                          msg = "delay_seconds must be a non-negative numeric value")
  assertthat::assert_that(is.numeric(max_records) && max_records > 0,
                          msg = "max_records must be a positive numeric value")
  assertthat::assert_that(is.null(output_csv) || assertthat::is.string(output_csv),
                          msg = "output_csv must be NULL or a single character string")
  assertthat::assert_that(assertthat::is.string(filter_mode),
                          msg = "filter_mode must be a single character string")
  assertthat::assert_that(filter_mode %in% c("starts_with", "contains"),
                          msg = "filter_mode must be either 'starts_with' or 'contains'")
  assertthat::assert_that(assertthat::is.flag(debug_structure),
                          msg = "debug_structure must be TRUE or FALSE")
  assertthat::assert_that(assertthat::is.flag(verbose),
                          msg = "verbose must be TRUE or FALSE")
  
  if (verbose) {
    logger::log_info("=== STARTING NPI REGISTRY SEARCH ===")
    logger::log_info("Input parameters received:")
    logger::log_info("  taxonomy_description: '{taxonomy_description}'")
    logger::log_info("  taxonomy: '{taxonomy %||% 'NULL'}'")
    logger::log_info("  enumeration_type: '{enumeration_type %||% 'NULL'}'")
    logger::log_info("  first_name: '{first_name %||% 'NULL'}'")
    logger::log_info("  last_name: '{last_name %||% 'NULL'}'")
    logger::log_info("  organization_name: '{organization_name %||% 'NULL'}'")
    logger::log_info("  state: '{state %||% 'NULL'}'")
    logger::log_info("  city: '{city %||% 'NULL'}'")
    logger::log_info("  postal_code: '{postal_code %||% 'NULL'}'")
    logger::log_info("  country_code: '{country_code %||% 'NULL'}'")
    logger::log_info("  delay_seconds: {delay_seconds}")
    logger::log_info("  max_records: {max_records}")
    logger::log_info("  output_csv: '{output_csv %||% 'NULL'}'")
    logger::log_info("  filter_mode: '{filter_mode}'")
    logger::log_info("  debug_structure: {debug_structure}")
    logger::log_info("  verbose: {verbose}")
  }
  
  # Build API parameters
  constructed_api_params <- .construct_api_query_parameters(
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code,
    verbose = verbose
  )
  
  # Fetch all provider records from API
  retrieved_provider_records <- .retrieve_paginated_provider_records(
    constructed_api_params = constructed_api_params,
    delay_seconds = delay_seconds,
    max_records = max_records,
    debug_structure = debug_structure,
    verbose = verbose
  )
  
  # Process and clean the provider data
  processed_provider_tibble <- .transform_provider_records_to_tibble(
    retrieved_provider_records = retrieved_provider_records,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  # CRITICAL: Deduplicate by NPI to prevent duplicate entries
  deduplicated_provider_tibble <- .deduplicate_providers_by_npi(
    processed_provider_tibble = processed_provider_tibble,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("=== SEARCH PROCESS COMPLETED ===")
    logger::log_info("Final deduplicated dataset dimensions: {nrow(deduplicated_provider_tibble)} rows, {ncol(deduplicated_provider_tibble)} columns")
    logger::log_info("Column names in final tibble: {paste(colnames(deduplicated_provider_tibble), collapse = ', ')}")
  }
  
  # Write to CSV if output path specified
  if (!is.null(output_csv)) {
    .write_provider_tibble_to_csv(
      deduplicated_provider_tibble = deduplicated_provider_tibble,
      output_csv_path = output_csv,
      verbose = verbose
    )
  }
  
  # Display API reference information
  cat("API documentation: https://npiregistry.cms.hhs.gov/api-page\n")
  
  return(deduplicated_provider_tibble)
}

#' Construct API query parameters for NPI registry search
#' @noRd
.construct_api_query_parameters <- function(taxonomy_description,
                                            taxonomy,
                                            enumeration_type,
                                            first_name,
                                            last_name,
                                            organization_name,
                                            state,
                                            city,
                                            postal_code,
                                            country_code,
                                            verbose) {
  
  constructed_params <- list(
    version = "2.1",
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code
  )
  
  # Remove NULL parameters to clean the query
  non_null_params <- constructed_params[!sapply(constructed_params, is.null)]
  
  if (verbose) {
    logger::log_info("Constructed API parameters with {length(non_null_params)} non-null values")
    logger::log_info("API parameter names: {paste(names(non_null_params), collapse = ', ')}")
  }
  
  return(non_null_params)
}

#' Retrieve all provider records using API pagination
#' @noRd
.retrieve_paginated_provider_records <- function(constructed_api_params, 
                                                 delay_seconds, 
                                                 max_records, 
                                                 debug_structure, 
                                                 verbose) {
  
  npi_registry_base_url <- "https://npiregistry.cms.hhs.gov/api/"
  records_per_api_request <- 200
  current_skip_value <- 0
  accumulated_provider_records <- list()
  api_request_counter <- 0
  
  if (verbose) {
    logger::log_info("=== STARTING API DATA RETRIEVAL ===")
    logger::log_info("Base URL: {npi_registry_base_url}")
    logger::log_info("Records per request: {records_per_api_request}")
    logger::log_info("Delay between requests: {delay_seconds} seconds")
    logger::log_info("Maximum records limit: {max_records}")
  }
  
  repeat {
    api_request_counter <- api_request_counter + 1
    constructed_api_params$limit <- records_per_api_request
    constructed_api_params$skip <- current_skip_value
    
    # Apply courtesy delay between requests (except first)
    if (api_request_counter > 1 && delay_seconds > 0) {
      if (verbose) {
        logger::log_info("Applying {delay_seconds} second delay before request #{api_request_counter}")
      }
      Sys.sleep(delay_seconds)
    }
    
    if (verbose) {
      logger::log_info("Making API request #{api_request_counter}: skip={current_skip_value}, limit={records_per_api_request}")
    }
    
    # Execute API request
    current_api_response <- httr::GET(npi_registry_base_url, query = constructed_api_params)
    httr::stop_for_status(current_api_response)
    
    parsed_api_response <- httr::content(current_api_response, as = "parsed", type = "application/json")
    
    if (verbose) {
      response_result_count <- parsed_api_response$result_count %||% 0
      logger::log_info("API response #{api_request_counter} - result_count: {response_result_count}")
    }
    
    # Debug structure on first request if requested
    if (debug_structure && api_request_counter == 1 && 
        !is.null(parsed_api_response$results) && 
        length(parsed_api_response$results) > 0) {
      .examine_npi_record_structure(parsed_api_response$results[[1]], verbose)
    }
    
    # Check for valid results
    if (is.null(parsed_api_response$results) || length(parsed_api_response$results) == 0) {
      if (verbose) {
        logger::log_info("No results in API response #{api_request_counter}, ending pagination")
      }
      break
    }
    
    current_batch_record_count <- length(parsed_api_response$results)
    
    # Accumulate results
    accumulated_provider_records <- append(accumulated_provider_records, parsed_api_response$results)
    current_skip_value <- current_skip_value + records_per_api_request
    
    if (verbose) {
      total_accumulated <- length(accumulated_provider_records)
      logger::log_info("Batch #{api_request_counter}: retrieved {current_batch_record_count} records, total accumulated: {total_accumulated}")
    }
    
    # Stop if we received fewer records than requested (end of data)
    if (current_batch_record_count < records_per_api_request) {
      if (verbose) {
        logger::log_info("Received {current_batch_record_count} records (less than {records_per_api_request}), indicating end of available data")
      }
      break
    }
    
    # Safety check for maximum records limit
    if (length(accumulated_provider_records) >= max_records) {
      if (verbose) {
        logger::log_warn("Reached maximum record limit ({max_records}), stopping retrieval for performance")
      }
      break
    }
  }
  
  total_retrieved <- length(accumulated_provider_records)
  if (verbose) {
    logger::log_info("=== API RETRIEVAL COMPLETED ===")
    logger::log_info("Total provider records retrieved: {total_retrieved} across {api_request_counter} API requests")
  }
  
  return(accumulated_provider_records)
}

#' Examine and debug NPI record structure
#' @noRd
.examine_npi_record_structure <- function(sample_provider_record, verbose) {
  
  if (verbose) {
    logger::log_info("=== DEBUGGING: EXAMINING NPI RECORD STRUCTURE ===")
    
    # Show top-level fields
    top_level_field_names <- names(sample_provider_record)
    logger::log_info("Top-level fields in provider record:")
    for (field_name in top_level_field_names) {
      field_class <- class(sample_provider_record[[field_name]])[1]
      logger::log_info("  {field_name}: {field_class}")
    }
    
    # Search for NPI number in various locations
    logger::log_info("=== SEARCHING FOR NPI NUMBER FIELD ===")
    
    if (!is.null(sample_provider_record$number)) {
      logger::log_info("Found NPI as 'number': {sample_provider_record$number}")
    }
    
    if (!is.null(sample_provider_record$npi)) {
      logger::log_info("Found NPI as 'npi': {sample_provider_record$npi}")
    }
    
    # Examine basic section
    if (!is.null(sample_provider_record$basic)) {
      logger::log_info("=== BASIC SECTION FIELDS ===")
      basic_field_names <- names(sample_provider_record$basic)
      for (field_name in basic_field_names) {
        field_value <- sample_provider_record$basic[[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  basic${field_name}: '{field_value}'")
        } else {
          logger::log_info("  basic${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine addresses section
    if (!is.null(sample_provider_record$addresses) && length(sample_provider_record$addresses) > 0) {
      logger::log_info("=== ADDRESSES SECTION (FIRST ADDRESS) ===")
      first_address_fields <- names(sample_provider_record$addresses[[1]])
      for (field_name in first_address_fields) {
        field_value <- sample_provider_record$addresses[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  addresses[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  addresses[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine taxonomies section
    if (!is.null(sample_provider_record$taxonomies) && length(sample_provider_record$taxonomies) > 0) {
      logger::log_info("=== TAXONOMIES SECTION (FIRST TAXONOMY) ===")
      first_taxonomy_fields <- names(sample_provider_record$taxonomies[[1]])
      for (field_name in first_taxonomy_fields) {
        field_value <- sample_provider_record$taxonomies[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  taxonomies[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  taxonomies[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    logger::log_info("=== END STRUCTURE DEBUGGING ===")
  }
}

#' Transform provider records into a cleaned tibble
#' @noRd
.transform_provider_records_to_tibble <- function(retrieved_provider_records, 
                                                  taxonomy_description, 
                                                  filter_mode, 
                                                  verbose) {
  
  total_provider_records <- length(retrieved_provider_records)
  
  if (total_provider_records == 0) {
    if (verbose) {
      logger::log_warn("No provider records to process, returning empty tibble")
    }
    return(tibble::tibble())
  }
  
  if (verbose) {
    logger::log_info("=== PROCESSING PROVIDER RECORDS ===")
    logger::log_info("Total provider records to process: {total_provider_records}")
  }
  
  # Process in batches for performance and progress tracking
  processing_batch_size <- 1000
  processed_batch_list <- list()
  
  for (batch_start_index in seq(1, total_provider_records, by = processing_batch_size)) {
    batch_end_index <- min(batch_start_index + processing_batch_size - 1, total_provider_records)
    
    if (verbose) {
      completion_percent <- round((batch_end_index / total_provider_records) * 100, 1)
      logger::log_info("Processing records {batch_start_index} to {batch_end_index} ({completion_percent}% complete)")
    }
    
    current_batch_records <- retrieved_provider_records[batch_start_index:batch_end_index]
    
    # Transform current batch to tibble
    current_batch_tibble <- purrr::map_dfr(current_batch_records, .extract_provider_information)
    processed_batch_list[[length(processed_batch_list) + 1]] <- current_batch_tibble
  }
  
  # Combine all processed batches
  if (verbose) {
    logger::log_info("Combining {length(processed_batch_list)} processed batches into unified tibble")
  }
  
  combined_provider_tibble <- dplyr::bind_rows(processed_batch_list)
  
  if (verbose) {
    logger::log_info("Combined tibble dimensions: {nrow(combined_provider_tibble)} rows, {ncol(combined_provider_tibble)} columns")
  }
  
  # Apply taxonomy description filtering
  filtered_provider_tibble <- .apply_taxonomy_filtering(
    combined_provider_tibble = combined_provider_tibble,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  return(filtered_provider_tibble)
}

#' Apply taxonomy description filtering based on mode
#' @noRd
.apply_taxonomy_filtering <- function(combined_provider_tibble, 
                                      taxonomy_description, 
                                      filter_mode, 
                                      verbose) {
  
  original_record_count <- nrow(combined_provider_tibble)
  
  if (verbose) {
    logger::log_info("=== APPLYING TAXONOMY FILTERING ===")
    logger::log_info("Original record count before filtering: {original_record_count}")
    filter_description <- if (filter_mode == "starts_with") {
      "taxonomy description must start with"
    } else {
      "taxonomy description must contain"
    }
    logger::log_info("Filter mode: {filter_mode} ('{filter_description}' '{taxonomy_description}')")
    
    # Show sample taxonomy descriptions for debugging
    if (original_record_count > 0) {
      sample_taxonomies <- combined_provider_tibble$primary_taxonomy_description[1:min(10, original_record_count)]
      valid_sample_taxonomies <- sample_taxonomies[!is.na(sample_taxonomies)]
      if (length(valid_sample_taxonomies) > 0) {
        logger::log_info("Sample taxonomy descriptions found in retrieved data:")
        for (i in seq_along(valid_sample_taxonomies)) {
          logger::log_info("  {i}: '{valid_sample_taxonomies[i]}'")
        }
      }
    }
  }
  
  # Apply filtering based on selected mode
  if (filter_mode == "starts_with") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_starts(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  } else if (filter_mode == "contains") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_detect(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  }
  
  filtered_record_count <- nrow(filtered_provider_tibble)
  removed_record_count <- original_record_count - filtered_record_count
  
  if (verbose) {
    logger::log_info("Taxonomy filtering completed:")
    logger::log_info("  Records kept: {filtered_record_count}")
    logger::log_info("  Records removed: {removed_record_count}")
    
    # Provide suggestions if no matches found
    if (filtered_record_count == 0 && original_record_count > 0) {
      logger::log_warn("No records matched the taxonomy filter criteria!")
      logger::log_info("Consider trying filter_mode = 'contains' or checking taxonomy spelling")
      
      # Show taxonomy descriptions that contain the search term
      alternative_matches <- combined_provider_tibble |>
        dplyr::filter(!is.na(primary_taxonomy_description) & 
                        stringr::str_detect(primary_taxonomy_description, 
                                            stringr::fixed(taxonomy_description))) |>
        dplyr::distinct(primary_taxonomy_description) |>
        dplyr::slice_head(n = 5)
      
      if (nrow(alternative_matches) > 0) {
        logger::log_info("Found {nrow(alternative_matches)} taxonomy descriptions CONTAINING '{taxonomy_description}':")
        for (i in 1:nrow(alternative_matches)) {
          logger::log_info("  - '{alternative_matches$primary_taxonomy_description[i]}'")
        }
      }
    }
  }
  
  return(filtered_provider_tibble)
}

#' Extract key information from a single provider record
#' @noRd
.extract_provider_information <- function(single_provider_record) {
  
  # Extract sections from provider record
  basic_section <- single_provider_record$basic %||% list()
  primary_address_section <- if (length(single_provider_record$addresses) > 0) {
    single_provider_record$addresses[[1]]
  } else {
    list()
  }
  primary_taxonomy_section <- if (length(single_provider_record$taxonomies) > 0) {
    single_provider_record$taxonomies[[1]]
  } else {
    list()
  }
  
  # Build flattened provider information tibble
  provider_information_tibble <- tibble::tibble(
    npi = single_provider_record$number %||% 
      basic_section$npi %||% 
      single_provider_record$npi %||% 
      NA_character_,
    entity_type_code = basic_section$enumeration_type %||% NA_character_,
    entity_type = if (!is.null(basic_section$enumeration_type)) {
      if (basic_section$enumeration_type == "NPI-1") "Individual" else "Organization"
    } else {
      NA_character_
    },
    first_name = basic_section$first_name %||% NA_character_,
    last_name = basic_section$last_name %||% NA_character_,
    middle_name = basic_section$middle_name %||% NA_character_,
    organization_name = basic_section$organization_name %||% NA_character_,
    gender = basic_section$gender %||% NA_character_,
    sole_proprietor = basic_section$sole_proprietor %||% NA_character_,
    primary_address_line1 = primary_address_section$address_1 %||% NA_character_,
    primary_address_line2 = primary_address_section$address_2 %||% NA_character_,
    primary_city = primary_address_section$city %||% NA_character_,
    primary_state = primary_address_section$state %||% NA_character_,
    primary_postal_code = primary_address_section$postal_code %||% NA_character_,
    primary_country_code = primary_address_section$country_code %||% NA_character_,
    primary_telephone = primary_address_section$telephone_number %||% NA_character_,
    primary_fax = primary_address_section$fax_number %||% NA_character_,
    primary_taxonomy_code = primary_taxonomy_section$code %||% NA_character_,
    primary_taxonomy_description = primary_taxonomy_section$desc %||% NA_character_,
    primary_taxonomy_license = primary_taxonomy_section$license %||% NA_character_,
    primary_taxonomy_state = primary_taxonomy_section$state %||% NA_character_,
    is_primary_taxonomy = primary_taxonomy_section$primary %||% NA
  )
  
  return(provider_information_tibble)
}

#' Deduplicate providers by NPI number (CRITICAL for preventing duplicates)
#' @noRd
.deduplicate_providers_by_npi <- function(processed_provider_tibble, verbose) {
  
  original_row_count <- nrow(processed_provider_tibble)
  
  if (verbose) {
    logger::log_info("=== DEDUPLICATING PROVIDERS BY NPI ===")
    logger::log_info("Records before deduplication: {original_row_count}")
  }
  
  # Check for missing NPIs
  missing_npi_count <- sum(is.na(processed_provider_tibble$npi))
  if (missing_npi_count > 0 && verbose) {
    logger::log_warn("Found {missing_npi_count} records with missing NPI numbers")
  }
  
  # Remove duplicates, keeping first occurrence for each NPI
  # Arrange by NPI first to ensure consistent ordering
  deduplicated_provider_tibble <- processed_provider_tibble |>
    dplyr::arrange(npi) |>
    dplyr::distinct(npi, .keep_all = TRUE)
  
  deduplicated_row_count <- nrow(deduplicated_provider_tibble)
  duplicate_count <- original_row_count - deduplicated_row_count
  
  if (verbose) {
    logger::log_info("Deduplication completed:")
    logger::log_info("  Unique providers after deduplication: {deduplicated_row_count}")
    logger::log_info("  Duplicate records removed: {duplicate_count}")
    
    if (duplicate_count > 0) {
      logger::log_info("Duplicates were likely caused by providers having multiple:")
      logger::log_info("  - Practice locations (addresses)")
      logger::log_info("  - Taxonomy codes/specialties")
      logger::log_info("  - Database entries in the NPI registry")
    }
  }
  
  return(deduplicated_provider_tibble)
}

#' Write provider tibble to CSV file
#' @noRd
.write_provider_tibble_to_csv <- function(deduplicated_provider_tibble, 
                                          output_csv_path, 
                                          verbose) {
  
  if (verbose) {
    logger::log_info("=== WRITING RESULTS TO CSV ===")
    logger::log_info("Output CSV file path: '{output_csv_path}'")
  }
  
  # Create directory if it doesn't exist
  output_directory <- dirname(output_csv_path)
  if (!dir.exists(output_directory) && output_directory != ".") {
    dir.create(output_directory, recursive = TRUE, showWarnings = FALSE)
    if (verbose) {
      logger::log_info("Created output directory: '{output_directory}'")
    }
  }
  
  # Write CSV file
  readr::write_csv(deduplicated_provider_tibble, output_csv_path, na = "")
  
  # Verify file was written and get details
  if (file.exists(output_csv_path)) {
    file_size_bytes <- file.size(output_csv_path)
    normalized_path <- normalizePath(output_csv_path, mustWork = FALSE)
    
    if (verbose) {
      logger::log_info("Successfully wrote CSV file:")
      logger::log_info("  Records written: {nrow(deduplicated_provider_tibble)}")
      logger::log_info("  Full file path: '{normalized_path}'")
      logger::log_info("  File size: {file_size_bytes} bytes")
    }
  } else {
    logger::log_error("Failed to write CSV file: '{output_csv_path}'")
  }
}

generalist <- npi_search_all(
  taxonomy = "207V00000X", # Specific physician code
  debug_structure = TRUE,
  max_records = 10,
  verbose = TRUE,
  delay_seconds = 0.5,
  enumeration_type = "NPI-1",
  country_code = "US",
  output_csv = "data/02-search_taxonomy/nppes_npi_search_all_obgyn_deduplicated.csv"
)

# function at 1617 ----
##########################################################################
#' Search National Provider Identifier (NPI) Registry with Deduplication
#'
#' This function searches the CMS NPI Registry API for healthcare providers
#' based on various criteria and returns a cleaned tibble with provider
#' information. Automatically deduplicates providers by NPI number to prevent
#' duplicate entries from multiple taxonomies or practice locations. Either
#' taxonomy_description OR taxonomy must be provided to search for providers.
#'
#' @param taxonomy_description Character string or NULL. The taxonomy description to
#'   search for (e.g., "Obstetrics & Gynecology", "Family Medicine"). Default is NULL.
#'   Either taxonomy_description OR taxonomy must be provided.
#' @param taxonomy Character string or NULL. The primary taxonomy code to
#'   search for (e.g., "207V00000X", "208000000X"). Default is NULL.
#'   Either taxonomy_description OR taxonomy must be provided.
#' @param enumeration_type Character string or NULL. Type of enumeration
#'   ("NPI-1" for individuals, "NPI-2" for organizations). Default is "NPI-1"
#'   to search only individual providers.
#' @param first_name Character string or NULL. First name of provider for
#'   individual searches. Default is NULL.
#' @param last_name Character string or NULL. Last name of provider for
#'   individual searches. Default is NULL.
#' @param organization_name Character string or NULL. Organization name for
#'   organization searches. Default is NULL.
#' @param state Character string or NULL. Two-letter state abbreviation
#'   (e.g., "CA", "NY"). Default is NULL.
#' @param city Character string or NULL. City name. Default is NULL.
#' @param postal_code Character string or NULL. ZIP code or postal code.
#'   Default is NULL.
#' @param country_code Character string or NULL. Two-letter country code
#'   (e.g., "US"). Default is "US".
#' @param delay_seconds Numeric. Number of seconds to wait between API
#'   requests as a courtesy to the server. Default is 0.5 seconds.
#' @param max_records Numeric. Maximum number of records to retrieve for
#'   safety. Default is 10000 to prevent long processing times.
#' @param output_csv Character string or NULL. File path to save results as
#'   CSV. If NULL (default), no CSV file is written.
#' @param filter_mode Character string. How to filter taxonomy descriptions:
#'   "starts_with" (default) requires exact match at beginning, "contains" 
#'   allows the search term anywhere in the taxonomy description.
#' @param debug_structure Logical. Whether to examine and log the structure
#'   of the first API response for debugging. Default is FALSE.
#' @param verbose Logical. Whether to enable detailed logging to console.
#'   Default is TRUE.
#'
#' @return A tibble containing unique provider information (deduplicated by
#'   NPI number) with columns for basic information, addresses, and taxonomies.
#'
#' @examples
#' # Example 1: Search by specific taxonomy code (physicians only)
#' obgyn_physicians_by_code <- npi_search_all(
#'   taxonomy_description = NULL,
#'   taxonomy = "207V00000X",
#'   enumeration_type = "NPI-1",
#'   state = "CA",
#'   delay_seconds = 0.5,
#'   max_records = 5000,
#'   output_csv = "obgyn_physicians_only.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with OB/GYN physicians only (no nurse practitioners)
#' # Output: CSV file saved to working directory
#'
#' # Example 2: Search by taxonomy description with city filter
#' obgyn_boston_providers <- npi_search_all(
#'   taxonomy_description = "Obstetrics & Gynecology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   city = "Boston",
#'   state = "MA",
#'   country_code = "US",
#'   delay_seconds = 0.2,
#'   max_records = 1000,
#'   output_csv = "data/obgyn_boston.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = FALSE
#' )
#' # Returns: tibble with unique OB/GYN providers in Boston
#' # Output: CSV saved to data/ subdirectory
#'
#' # Example 3: Large search combining both taxonomy code and description
#' timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#' oncology_providers_comprehensive <- npi_search_all(
#'   taxonomy_description = "Oncology",
#'   taxonomy = "207RX0202X",
#'   enumeration_type = "NPI-1",
#'   state = "TX",
#'   country_code = "US",
#'   delay_seconds = 1.0,
#'   max_records = 25000,
#'   output_csv = paste0("oncology_providers_", timestamp, ".csv"),
#'   filter_mode = "contains",
#'   debug_structure = TRUE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with unique oncology providers in Texas
#' # Output: Timestamped CSV file with debug structure logging
#'
#' @importFrom httr GET stop_for_status content
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows filter distinct arrange slice_head
#' @importFrom purrr map_dfr
#' @importFrom stringr str_starts str_detect fixed
#' @importFrom readr write_csv
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom logger log_info log_warn log_error
#' @export
npi_search_all <- function(taxonomy_description = NULL,
                           taxonomy = NULL,
                           enumeration_type = "NPI-1",
                           first_name = NULL,
                           last_name = NULL,
                           organization_name = NULL,
                           state = NULL,
                           city = NULL,
                           postal_code = NULL,
                           country_code = "US",
                           delay_seconds = 0.5,
                           max_records = 10000,
                           output_csv = NULL,
                           filter_mode = "starts_with",
                           debug_structure = FALSE,
                           verbose = TRUE) {
  
  # Input validation with assertthat
  assertthat::assert_that(is.null(taxonomy_description) || assertthat::is.string(taxonomy_description),
                          msg = "taxonomy_description must be NULL or a single character string")
  assertthat::assert_that(is.null(taxonomy) || assertthat::is.string(taxonomy),
                          msg = "taxonomy must be NULL or a single character string")
  assertthat::assert_that(!is.null(taxonomy_description) || !is.null(taxonomy),
                          msg = "Either taxonomy_description OR taxonomy must be provided (both cannot be NULL)")
  assertthat::assert_that(is.null(enumeration_type) || assertthat::is.string(enumeration_type),
                          msg = "enumeration_type must be NULL or a single character string")
  assertthat::assert_that(is.null(first_name) || assertthat::is.string(first_name),
                          msg = "first_name must be NULL or a single character string")
  assertthat::assert_that(is.null(last_name) || assertthat::is.string(last_name),
                          msg = "last_name must be NULL or a single character string")
  assertthat::assert_that(is.null(organization_name) || assertthat::is.string(organization_name),
                          msg = "organization_name must be NULL or a single character string")
  assertthat::assert_that(is.null(state) || assertthat::is.string(state),
                          msg = "state must be NULL or a single character string")
  assertthat::assert_that(is.null(city) || assertthat::is.string(city),
                          msg = "city must be NULL or a single character string")
  assertthat::assert_that(is.null(postal_code) || assertthat::is.string(postal_code),
                          msg = "postal_code must be NULL or a single character string")
  assertthat::assert_that(is.null(country_code) || assertthat::is.string(country_code),
                          msg = "country_code must be NULL or a single character string")
  assertthat::assert_that(is.numeric(delay_seconds) && delay_seconds >= 0,
                          msg = "delay_seconds must be a non-negative numeric value")
  assertthat::assert_that(is.numeric(max_records) && max_records > 0,
                          msg = "max_records must be a positive numeric value")
  assertthat::assert_that(is.null(output_csv) || assertthat::is.string(output_csv),
                          msg = "output_csv must be NULL or a single character string")
  assertthat::assert_that(assertthat::is.string(filter_mode),
                          msg = "filter_mode must be a single character string")
  assertthat::assert_that(filter_mode %in% c("starts_with", "contains"),
                          msg = "filter_mode must be either 'starts_with' or 'contains'")
  assertthat::assert_that(assertthat::is.flag(debug_structure),
                          msg = "debug_structure must be TRUE or FALSE")
  assertthat::assert_that(assertthat::is.flag(verbose),
                          msg = "verbose must be TRUE or FALSE")
  
  if (verbose) {
    logger::log_info("=== STARTING NPI REGISTRY SEARCH ===")
    logger::log_info("Input parameters received:")
    logger::log_info("  taxonomy_description: '{taxonomy_description %||% 'NULL'}'")
    logger::log_info("  taxonomy: '{taxonomy %||% 'NULL'}'")
    logger::log_info("  enumeration_type: '{enumeration_type %||% 'NULL'}'")
    logger::log_info("  first_name: '{first_name %||% 'NULL'}'")
    logger::log_info("  last_name: '{last_name %||% 'NULL'}'")
    logger::log_info("  organization_name: '{organization_name %||% 'NULL'}'")
    logger::log_info("  state: '{state %||% 'NULL'}'")
    logger::log_info("  city: '{city %||% 'NULL'}'")
    logger::log_info("  postal_code: '{postal_code %||% 'NULL'}'")
    logger::log_info("  country_code: '{country_code %||% 'NULL'}'")
    logger::log_info("  delay_seconds: {delay_seconds}")
    logger::log_info("  max_records: {max_records}")
    logger::log_info("  output_csv: '{output_csv %||% 'NULL'}'")
    logger::log_info("  filter_mode: '{filter_mode}'")
    logger::log_info("  debug_structure: {debug_structure}")
    logger::log_info("  verbose: {verbose}")
  }
  
  # Build API parameters
  constructed_api_params <- .construct_api_query_parameters(
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code,
    verbose = verbose
  )
  
  # Fetch all provider records from API
  retrieved_provider_records <- .retrieve_paginated_provider_records(
    constructed_api_params = constructed_api_params,
    delay_seconds = delay_seconds,
    max_records = max_records,
    debug_structure = debug_structure,
    verbose = verbose
  )
  
  # Process and clean the provider data
  processed_provider_tibble <- .transform_provider_records_to_tibble(
    retrieved_provider_records = retrieved_provider_records,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  # CRITICAL: Deduplicate by NPI to prevent duplicate entries
  deduplicated_provider_tibble <- .deduplicate_providers_by_npi(
    processed_provider_tibble = processed_provider_tibble,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("=== SEARCH PROCESS COMPLETED ===")
    logger::log_info("Final deduplicated dataset dimensions: {nrow(deduplicated_provider_tibble)} rows, {ncol(deduplicated_provider_tibble)} columns")
    logger::log_info("Column names in final tibble: {paste(colnames(deduplicated_provider_tibble), collapse = ', ')}")
  }
  
  # Write to CSV if output path specified
  if (!is.null(output_csv)) {
    .write_provider_tibble_to_csv(
      deduplicated_provider_tibble = deduplicated_provider_tibble,
      output_csv_path = output_csv,
      verbose = verbose
    )
  }
  
  # Display API reference information
  cat("API documentation: https://npiregistry.cms.hhs.gov/api-page\n")
  
  return(deduplicated_provider_tibble)
}

#' Construct API query parameters for NPI registry search
#' @noRd
.construct_api_query_parameters <- function(taxonomy_description,
                                            taxonomy,
                                            enumeration_type,
                                            first_name,
                                            last_name,
                                            organization_name,
                                            state,
                                            city,
                                            postal_code,
                                            country_code,
                                            verbose) {
  
  constructed_params <- list(
    version = "2.1",
    taxonomy_description = taxonomy_description,
    taxonomy_code = taxonomy,  # Changed from taxonomy to taxonomy_code
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code
  )
  
  # Remove NULL parameters to clean the query
  non_null_params <- constructed_params[!sapply(constructed_params, is.null)]
  
  if (verbose) {
    logger::log_info("Constructed API parameters with {length(non_null_params)} non-null values")
    logger::log_info("API parameter names: {paste(names(non_null_params), collapse = ', ')}")
  }
  
  return(non_null_params)
}

#' Retrieve all provider records using API pagination
#' @noRd
.retrieve_paginated_provider_records <- function(constructed_api_params, 
                                                 delay_seconds, 
                                                 max_records, 
                                                 debug_structure, 
                                                 verbose) {
  
  npi_registry_base_url <- "https://npiregistry.cms.hhs.gov/api/"
  records_per_api_request <- 200
  current_skip_value <- 0
  accumulated_provider_records <- list()
  api_request_counter <- 0
  
  if (verbose) {
    logger::log_info("=== STARTING API DATA RETRIEVAL ===")
    logger::log_info("Base URL: {npi_registry_base_url}")
    logger::log_info("Records per request: {records_per_api_request}")
    logger::log_info("Delay between requests: {delay_seconds} seconds")
    logger::log_info("Maximum records limit: {max_records}")
  }
  
  repeat {
    api_request_counter <- api_request_counter + 1
    constructed_api_params$limit <- records_per_api_request
    constructed_api_params$skip <- current_skip_value
    
    # Apply courtesy delay between requests (except first)
    if (api_request_counter > 1 && delay_seconds > 0) {
      if (verbose) {
        logger::log_info("Applying {delay_seconds} second delay before request #{api_request_counter}")
      }
      Sys.sleep(delay_seconds)
    }
    
    if (verbose) {
      logger::log_info("Making API request #{api_request_counter}: skip={current_skip_value}, limit={records_per_api_request}")
    }
    
    # Execute API request
    current_api_response <- httr::GET(npi_registry_base_url, query = constructed_api_params)
    httr::stop_for_status(current_api_response)
    
    parsed_api_response <- httr::content(current_api_response, as = "parsed", type = "application/json")
    
    if (verbose) {
      response_result_count <- parsed_api_response$result_count %||% 0
      logger::log_info("API response #{api_request_counter} - result_count: {response_result_count}")
    }
    
    # Debug structure on first request if requested
    if (debug_structure && api_request_counter == 1 && 
        !is.null(parsed_api_response$results) && 
        length(parsed_api_response$results) > 0) {
      .examine_npi_record_structure(parsed_api_response$results[[1]], verbose)
    }
    
    # Check for valid results
    if (is.null(parsed_api_response$results) || length(parsed_api_response$results) == 0) {
      if (verbose) {
        logger::log_info("No results in API response #{api_request_counter}, ending pagination")
      }
      break
    }
    
    current_batch_record_count <- length(parsed_api_response$results)
    
    # Accumulate results
    accumulated_provider_records <- append(accumulated_provider_records, parsed_api_response$results)
    current_skip_value <- current_skip_value + records_per_api_request
    
    if (verbose) {
      total_accumulated <- length(accumulated_provider_records)
      logger::log_info("Batch #{api_request_counter}: retrieved {current_batch_record_count} records, total accumulated: {total_accumulated}")
    }
    
    # Stop if we received fewer records than requested (end of data)
    if (current_batch_record_count < records_per_api_request) {
      if (verbose) {
        logger::log_info("Received {current_batch_record_count} records (less than {records_per_api_request}), indicating end of available data")
      }
      break
    }
    
    # Safety check for maximum records limit
    if (length(accumulated_provider_records) >= max_records) {
      if (verbose) {
        logger::log_warn("Reached maximum record limit ({max_records}), stopping retrieval for performance")
      }
      break
    }
  }
  
  total_retrieved <- length(accumulated_provider_records)
  if (verbose) {
    logger::log_info("=== API RETRIEVAL COMPLETED ===")
    logger::log_info("Total provider records retrieved: {total_retrieved} across {api_request_counter} API requests")
  }
  
  return(accumulated_provider_records)
}

#' Examine and debug NPI record structure
#' @noRd
.examine_npi_record_structure <- function(sample_provider_record, verbose) {
  
  if (verbose) {
    logger::log_info("=== DEBUGGING: EXAMINING NPI RECORD STRUCTURE ===")
    
    # Show top-level fields
    top_level_field_names <- names(sample_provider_record)
    logger::log_info("Top-level fields in provider record:")
    for (field_name in top_level_field_names) {
      field_class <- class(sample_provider_record[[field_name]])[1]
      logger::log_info("  {field_name}: {field_class}")
    }
    
    # Search for NPI number in various locations
    logger::log_info("=== SEARCHING FOR NPI NUMBER FIELD ===")
    
    if (!is.null(sample_provider_record$number)) {
      logger::log_info("Found NPI as 'number': {sample_provider_record$number}")
    }
    
    if (!is.null(sample_provider_record$npi)) {
      logger::log_info("Found NPI as 'npi': {sample_provider_record$npi}")
    }
    
    # Examine basic section
    if (!is.null(sample_provider_record$basic)) {
      logger::log_info("=== BASIC SECTION FIELDS ===")
      basic_field_names <- names(sample_provider_record$basic)
      for (field_name in basic_field_names) {
        field_value <- sample_provider_record$basic[[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  basic${field_name}: '{field_value}'")
        } else {
          logger::log_info("  basic${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine addresses section
    if (!is.null(sample_provider_record$addresses) && length(sample_provider_record$addresses) > 0) {
      logger::log_info("=== ADDRESSES SECTION (FIRST ADDRESS) ===")
      first_address_fields <- names(sample_provider_record$addresses[[1]])
      for (field_name in first_address_fields) {
        field_value <- sample_provider_record$addresses[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  addresses[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  addresses[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine taxonomies section
    if (!is.null(sample_provider_record$taxonomies) && length(sample_provider_record$taxonomies) > 0) {
      logger::log_info("=== TAXONOMIES SECTION (FIRST TAXONOMY) ===")
      first_taxonomy_fields <- names(sample_provider_record$taxonomies[[1]])
      for (field_name in first_taxonomy_fields) {
        field_value <- sample_provider_record$taxonomies[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  taxonomies[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  taxonomies[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    logger::log_info("=== END STRUCTURE DEBUGGING ===")
  }
}

#' Transform provider records into a cleaned tibble
#' @noRd
.transform_provider_records_to_tibble <- function(retrieved_provider_records, 
                                                  taxonomy_description, 
                                                  filter_mode, 
                                                  verbose) {
  
  total_provider_records <- length(retrieved_provider_records)
  
  if (total_provider_records == 0) {
    if (verbose) {
      logger::log_warn("No provider records to process, returning empty tibble")
    }
    return(tibble::tibble())
  }
  
  if (verbose) {
    logger::log_info("=== PROCESSING PROVIDER RECORDS ===")
    logger::log_info("Total provider records to process: {total_provider_records}")
  }
  
  # Process in batches for performance and progress tracking
  processing_batch_size <- 1000
  processed_batch_list <- list()
  
  for (batch_start_index in seq(1, total_provider_records, by = processing_batch_size)) {
    batch_end_index <- min(batch_start_index + processing_batch_size - 1, total_provider_records)
    
    if (verbose) {
      completion_percent <- round((batch_end_index / total_provider_records) * 100, 1)
      logger::log_info("Processing records {batch_start_index} to {batch_end_index} ({completion_percent}% complete)")
    }
    
    current_batch_records <- retrieved_provider_records[batch_start_index:batch_end_index]
    
    # Transform current batch to tibble
    current_batch_tibble <- purrr::map_dfr(current_batch_records, .extract_provider_information)
    processed_batch_list[[length(processed_batch_list) + 1]] <- current_batch_tibble
  }
  
  # Combine all processed batches
  if (verbose) {
    logger::log_info("Combining {length(processed_batch_list)} processed batches into unified tibble")
  }
  
  combined_provider_tibble <- dplyr::bind_rows(processed_batch_list)
  
  if (verbose) {
    logger::log_info("Combined tibble dimensions: {nrow(combined_provider_tibble)} rows, {ncol(combined_provider_tibble)} columns")
  }
  
  # Apply taxonomy description filtering
  filtered_provider_tibble <- .apply_taxonomy_filtering(
    combined_provider_tibble = combined_provider_tibble,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  return(filtered_provider_tibble)
}

#' Apply taxonomy description filtering based on mode
#' @noRd
.apply_taxonomy_filtering <- function(combined_provider_tibble, 
                                      taxonomy_description, 
                                      filter_mode, 
                                      verbose) {
  
  original_record_count <- nrow(combined_provider_tibble)
  
  # If no taxonomy_description provided, skip filtering
  if (is.null(taxonomy_description)) {
    if (verbose) {
      logger::log_info("=== SKIPPING TAXONOMY FILTERING ===")
      logger::log_info("No taxonomy_description provided, returning all {original_record_count} records")
    }
    return(combined_provider_tibble)
  }
  
  if (verbose) {
    logger::log_info("=== APPLYING TAXONOMY FILTERING ===")
    logger::log_info("Original record count before filtering: {original_record_count}")
    filter_description <- if (filter_mode == "starts_with") {
      "taxonomy description must start with"
    } else {
      "taxonomy description must contain"
    }
    logger::log_info("Filter mode: {filter_mode} ('{filter_description}' '{taxonomy_description}')")
    
    # Show sample taxonomy descriptions for debugging
    if (original_record_count > 0) {
      sample_taxonomies <- combined_provider_tibble$primary_taxonomy_description[1:min(10, original_record_count)]
      valid_sample_taxonomies <- sample_taxonomies[!is.na(sample_taxonomies)]
      if (length(valid_sample_taxonomies) > 0) {
        logger::log_info("Sample taxonomy descriptions found in retrieved data:")
        for (i in seq_along(valid_sample_taxonomies)) {
          logger::log_info("  {i}: '{valid_sample_taxonomies[i]}'")
        }
      }
    }
  }
  
  # Apply filtering based on selected mode
  if (filter_mode == "starts_with") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_starts(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  } else if (filter_mode == "contains") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_detect(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  }
  
  filtered_record_count <- nrow(filtered_provider_tibble)
  removed_record_count <- original_record_count - filtered_record_count
  
  if (verbose) {
    logger::log_info("Taxonomy filtering completed:")
    logger::log_info("  Records kept: {filtered_record_count}")
    logger::log_info("  Records removed: {removed_record_count}")
    
    # Provide suggestions if no matches found
    if (filtered_record_count == 0 && original_record_count > 0) {
      logger::log_warn("No records matched the taxonomy filter criteria!")
      logger::log_info("Consider trying filter_mode = 'contains' or checking taxonomy spelling")
      
      # Show taxonomy descriptions that contain the search term
      alternative_matches <- combined_provider_tibble |>
        dplyr::filter(!is.na(primary_taxonomy_description) & 
                        stringr::str_detect(primary_taxonomy_description, 
                                            stringr::fixed(taxonomy_description))) |>
        dplyr::distinct(primary_taxonomy_description) |>
        dplyr::slice_head(n = 5)
      
      if (nrow(alternative_matches) > 0) {
        logger::log_info("Found {nrow(alternative_matches)} taxonomy descriptions CONTAINING '{taxonomy_description}':")
        for (i in 1:nrow(alternative_matches)) {
          logger::log_info("  - '{alternative_matches$primary_taxonomy_description[i]}'")
        }
      }
    }
  }
  
  return(filtered_provider_tibble)
}

#' Extract key information from a single provider record
#' @noRd
.extract_provider_information <- function(single_provider_record) {
  
  # Extract sections from provider record
  basic_section <- single_provider_record$basic %||% list()
  primary_address_section <- if (length(single_provider_record$addresses) > 0) {
    single_provider_record$addresses[[1]]
  } else {
    list()
  }
  primary_taxonomy_section <- if (length(single_provider_record$taxonomies) > 0) {
    single_provider_record$taxonomies[[1]]
  } else {
    list()
  }
  
  # Build flattened provider information tibble
  provider_information_tibble <- tibble::tibble(
    npi = single_provider_record$number %||% 
      basic_section$npi %||% 
      single_provider_record$npi %||% 
      NA_character_,
    entity_type_code = basic_section$enumeration_type %||% NA_character_,
    entity_type = if (!is.null(basic_section$enumeration_type)) {
      if (basic_section$enumeration_type == "NPI-1") "Individual" else "Organization"
    } else {
      NA_character_
    },
    first_name = basic_section$first_name %||% NA_character_,
    last_name = basic_section$last_name %||% NA_character_,
    middle_name = basic_section$middle_name %||% NA_character_,
    organization_name = basic_section$organization_name %||% NA_character_,
    gender = basic_section$gender %||% NA_character_,
    sole_proprietor = basic_section$sole_proprietor %||% NA_character_,
    primary_address_line1 = primary_address_section$address_1 %||% NA_character_,
    primary_address_line2 = primary_address_section$address_2 %||% NA_character_,
    primary_city = primary_address_section$city %||% NA_character_,
    primary_state = primary_address_section$state %||% NA_character_,
    primary_postal_code = primary_address_section$postal_code %||% NA_character_,
    primary_country_code = primary_address_section$country_code %||% NA_character_,
    primary_telephone = primary_address_section$telephone_number %||% NA_character_,
    primary_fax = primary_address_section$fax_number %||% NA_character_,
    primary_taxonomy_code = primary_taxonomy_section$code %||% NA_character_,
    primary_taxonomy_description = primary_taxonomy_section$desc %||% NA_character_,
    primary_taxonomy_license = primary_taxonomy_section$license %||% NA_character_,
    primary_taxonomy_state = primary_taxonomy_section$state %||% NA_character_,
    is_primary_taxonomy = primary_taxonomy_section$primary %||% NA
  )
  
  return(provider_information_tibble)
}

#' Deduplicate providers by NPI number (CRITICAL for preventing duplicates)
#' @noRd
.deduplicate_providers_by_npi <- function(processed_provider_tibble, verbose) {
  
  original_row_count <- nrow(processed_provider_tibble)
  
  if (verbose) {
    logger::log_info("=== DEDUPLICATING PROVIDERS BY NPI ===")
    logger::log_info("Records before deduplication: {original_row_count}")
  }
  
  # Handle empty tibble case
  if (original_row_count == 0) {
    if (verbose) {
      logger::log_info("No records to deduplicate, returning empty tibble")
    }
    return(processed_provider_tibble)
  }
  
  # Check for missing NPIs
  missing_npi_count <- sum(is.na(processed_provider_tibble$npi))
  if (missing_npi_count > 0 && verbose) {
    logger::log_warn("Found {missing_npi_count} records with missing NPI numbers")
  }
  
  # Remove duplicates, keeping first occurrence for each NPI
  # Arrange by NPI first to ensure consistent ordering
  deduplicated_provider_tibble <- processed_provider_tibble |>
    dplyr::arrange(npi) |>
    dplyr::distinct(npi, .keep_all = TRUE)
  
  deduplicated_row_count <- nrow(deduplicated_provider_tibble)
  duplicate_count <- original_row_count - deduplicated_row_count
  
  if (verbose) {
    logger::log_info("Deduplication completed:")
    logger::log_info("  Unique providers after deduplication: {deduplicated_row_count}")
    logger::log_info("  Duplicate records removed: {duplicate_count}")
    
    if (duplicate_count > 0) {
      logger::log_info("Duplicates were likely caused by providers having multiple:")
      logger::log_info("  - Practice locations (addresses)")
      logger::log_info("  - Taxonomy codes/specialties")
      logger::log_info("  - Database entries in the NPI registry")
    }
  }
  
  return(deduplicated_provider_tibble)
}

#' Write provider tibble to CSV file
#' @noRd
.write_provider_tibble_to_csv <- function(deduplicated_provider_tibble, 
                                          output_csv_path, 
                                          verbose) {
  
  if (verbose) {
    logger::log_info("=== WRITING RESULTS TO CSV ===")
    logger::log_info("Output CSV file path: '{output_csv_path}'")
  }
  
  # Create directory if it doesn't exist
  output_directory <- dirname(output_csv_path)
  if (!dir.exists(output_directory) && output_directory != ".") {
    dir.create(output_directory, recursive = TRUE, showWarnings = FALSE)
    if (verbose) {
      logger::log_info("Created output directory: '{output_directory}'")
    }
  }
  
  # Write CSV file
  readr::write_csv(deduplicated_provider_tibble, output_csv_path, na = "")
  
  # Verify file was written and get details
  if (file.exists(output_csv_path)) {
    file_size_bytes <- file.size(output_csv_path)
    normalized_path <- normalizePath(output_csv_path, mustWork = FALSE)
    
    if (verbose) {
      logger::log_info("Successfully wrote CSV file:")
      logger::log_info("  Records written: {nrow(deduplicated_provider_tibble)}")
      logger::log_info("  Full file path: '{normalized_path}'")
      logger::log_info("  File size: {file_size_bytes} bytes")
    }
  } else {
    logger::log_error("Failed to write CSV file: '{output_csv_path}'")
  }
}

# Search by taxonomy code only (physicians only)
obgyn_physicians <- npi_search_all(
  taxonomy = "207V00000X",
  debug_structure = TRUE,
  max_records = 150000L,  # Increased limit
  verbose = TRUE,
  delay_seconds = 0.5,
  enumeration_type = "NPI-1",
  country_code = "US",
  output_csv = "data/02-search_taxonomy/nppes_obgyn_physicians_only.csv"
)

# execute -----
# Test with a small search using taxonomy_description to see if it works
test_small <- npi_search_all(
  taxonomy_description = "Obstetrics & Gynecology",
  max_records = 5,
  verbose = TRUE,
  delay_seconds = 0.5,
  enumeration_type = "NPI-1",
  country_code = "US"
)

# Alternative: Use taxonomy code as taxonomy_description
obgyn_physicians_alt <- npi_search_all(
  taxonomy_description = "207V00000X",  # Put taxonomy code here
  taxonomy = NULL,
  debug_structure = TRUE,
  max_records = 10,
  verbose = TRUE,
  delay_seconds = 0.5,
  enumeration_type = "NPI-1",
  country_code = "US"
)


# function at 1624 ----
##########################################################################
#' Search National Provider Identifier (NPI) Registry with Deduplication
#'
#' This function searches the CMS NPI Registry API for healthcare providers
#' based on various criteria and returns a cleaned tibble with provider
#' information. Automatically deduplicates providers by NPI number to prevent
#' duplicate entries from multiple taxonomies or practice locations. Either
#' taxonomy_description OR taxonomy must be provided to search for providers.
#'
#' @param taxonomy_description Character string or NULL. The taxonomy description to
#'   search for (e.g., "Obstetrics & Gynecology", "Family Medicine"). Default is NULL.
#'   Either taxonomy_description OR taxonomy must be provided.
#' @param taxonomy Character string or NULL. The primary taxonomy code to
#'   search for (e.g., "207V00000X", "208000000X"). Default is NULL.
#'   Either taxonomy_description OR taxonomy must be provided.
#' @param enumeration_type Character string or NULL. Type of enumeration
#'   ("NPI-1" for individuals, "NPI-2" for organizations). Default is "NPI-1"
#'   to search only individual providers.
#' @param first_name Character string or NULL. First name of provider for
#'   individual searches. Default is NULL.
#' @param last_name Character string or NULL. Last name of provider for
#'   individual searches. Default is NULL.
#' @param organization_name Character string or NULL. Organization name for
#'   organization searches. Default is NULL.
#' @param state Character string or NULL. Two-letter state abbreviation
#'   (e.g., "CA", "NY"). Default is NULL.
#' @param city Character string or NULL. City name. Default is NULL.
#' @param postal_code Character string or NULL. ZIP code or postal code.
#'   Default is NULL.
#' @param country_code Character string or NULL. Two-letter country code
#'   (e.g., "US"). Default is "US".
#' @param delay_seconds Numeric. Number of seconds to wait between API
#'   requests as a courtesy to the server. Default is 0.5 seconds.
#' @param max_records Numeric. Maximum number of records to retrieve for
#'   safety. Default is 10000 to prevent long processing times.
#' @param output_csv Character string or NULL. File path to save results as
#'   CSV. If NULL (default), no CSV file is written.
#' @param filter_mode Character string. How to filter taxonomy descriptions:
#'   "starts_with" (default) requires exact match at beginning, "contains" 
#'   allows the search term anywhere in the taxonomy description.
#' @param debug_structure Logical. Whether to examine and log the structure
#'   of the first API response for debugging. Default is FALSE.
#' @param verbose Logical. Whether to enable detailed logging to console.
#'   Default is TRUE.
#'
#' @return A tibble containing unique provider information (deduplicated by
#'   NPI number) with columns for basic information, addresses, and taxonomies.
#'
#' @examples
#' # Example 1: Search by specific taxonomy code (physicians only)
#' obgyn_physicians_by_code <- npi_search_all(
#'   taxonomy_description = NULL,
#'   taxonomy = "207V00000X",
#'   enumeration_type = "NPI-1",
#'   state = "CA",
#'   delay_seconds = 0.5,
#'   max_records = 5000,
#'   output_csv = "obgyn_physicians_only.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with OB/GYN physicians only (no nurse practitioners)
#' # Output: CSV file saved to working directory
#'
#' # Example 2: Search by taxonomy description with city filter
#' obgyn_boston_providers <- npi_search_all(
#'   taxonomy_description = "Obstetrics & Gynecology",
#'   taxonomy = NULL,
#'   enumeration_type = "NPI-1",
#'   city = "Boston",
#'   state = "MA",
#'   country_code = "US",
#'   delay_seconds = 0.2,
#'   max_records = 1000,
#'   output_csv = "data/obgyn_boston.csv",
#'   filter_mode = "starts_with",
#'   debug_structure = FALSE,
#'   verbose = FALSE
#' )
#' # Returns: tibble with unique OB/GYN providers in Boston
#' # Output: CSV saved to data/ subdirectory
#'
#' # Example 3: Large search combining both taxonomy code and description
#' timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#' oncology_providers_comprehensive <- npi_search_all(
#'   taxonomy_description = "Oncology",
#'   taxonomy = "207RX0202X",
#'   enumeration_type = "NPI-1",
#'   state = "TX",
#'   country_code = "US",
#'   delay_seconds = 1.0,
#'   max_records = 25000,
#'   output_csv = paste0("oncology_providers_", timestamp, ".csv"),
#'   filter_mode = "contains",
#'   debug_structure = TRUE,
#'   verbose = TRUE
#' )
#' # Returns: tibble with unique oncology providers in Texas
#' # Output: Timestamped CSV file with debug structure logging
#'
#' @importFrom httr GET stop_for_status content
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows filter distinct arrange slice_head
#' @importFrom purrr map_dfr
#' @importFrom stringr str_starts str_detect fixed
#' @importFrom readr write_csv
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom logger log_info log_warn log_error
#' @export
npi_search_all <- function(taxonomy_description = NULL,
                           taxonomy = NULL,
                           enumeration_type = "NPI-1",
                           first_name = NULL,
                           last_name = NULL,
                           organization_name = NULL,
                           state = NULL,
                           city = NULL,
                           postal_code = NULL,
                           country_code = "US",
                           delay_seconds = 0.5,
                           max_records = 10000,
                           output_csv = NULL,
                           filter_mode = "starts_with",
                           debug_structure = FALSE,
                           verbose = TRUE) {
  
  # Input validation with assertthat
  assertthat::assert_that(is.null(taxonomy_description) || assertthat::is.string(taxonomy_description),
                          msg = "taxonomy_description must be NULL or a single character string")
  assertthat::assert_that(is.null(taxonomy) || assertthat::is.string(taxonomy),
                          msg = "taxonomy must be NULL or a single character string")
  assertthat::assert_that(!is.null(taxonomy_description) || !is.null(taxonomy),
                          msg = "Either taxonomy_description OR taxonomy must be provided (both cannot be NULL)")
  assertthat::assert_that(is.null(enumeration_type) || assertthat::is.string(enumeration_type),
                          msg = "enumeration_type must be NULL or a single character string")
  assertthat::assert_that(is.null(first_name) || assertthat::is.string(first_name),
                          msg = "first_name must be NULL or a single character string")
  assertthat::assert_that(is.null(last_name) || assertthat::is.string(last_name),
                          msg = "last_name must be NULL or a single character string")
  assertthat::assert_that(is.null(organization_name) || assertthat::is.string(organization_name),
                          msg = "organization_name must be NULL or a single character string")
  assertthat::assert_that(is.null(state) || assertthat::is.string(state),
                          msg = "state must be NULL or a single character string")
  assertthat::assert_that(is.null(city) || assertthat::is.string(city),
                          msg = "city must be NULL or a single character string")
  assertthat::assert_that(is.null(postal_code) || assertthat::is.string(postal_code),
                          msg = "postal_code must be NULL or a single character string")
  assertthat::assert_that(is.null(country_code) || assertthat::is.string(country_code),
                          msg = "country_code must be NULL or a single character string")
  assertthat::assert_that(is.numeric(delay_seconds) && delay_seconds >= 0,
                          msg = "delay_seconds must be a non-negative numeric value")
  assertthat::assert_that(is.numeric(max_records) && max_records > 0,
                          msg = "max_records must be a positive numeric value")
  assertthat::assert_that(is.null(output_csv) || assertthat::is.string(output_csv),
                          msg = "output_csv must be NULL or a single character string")
  assertthat::assert_that(assertthat::is.string(filter_mode),
                          msg = "filter_mode must be a single character string")
  assertthat::assert_that(filter_mode %in% c("starts_with", "contains"),
                          msg = "filter_mode must be either 'starts_with' or 'contains'")
  assertthat::assert_that(assertthat::is.flag(debug_structure),
                          msg = "debug_structure must be TRUE or FALSE")
  assertthat::assert_that(assertthat::is.flag(verbose),
                          msg = "verbose must be TRUE or FALSE")
  
  if (verbose) {
    logger::log_info("=== STARTING NPI REGISTRY SEARCH ===")
    logger::log_info("Input parameters received:")
    logger::log_info("  taxonomy_description: '{taxonomy_description %||% 'NULL'}'")
    logger::log_info("  taxonomy: '{taxonomy %||% 'NULL'}'")
    logger::log_info("  enumeration_type: '{enumeration_type %||% 'NULL'}'")
    logger::log_info("  first_name: '{first_name %||% 'NULL'}'")
    logger::log_info("  last_name: '{last_name %||% 'NULL'}'")
    logger::log_info("  organization_name: '{organization_name %||% 'NULL'}'")
    logger::log_info("  state: '{state %||% 'NULL'}'")
    logger::log_info("  city: '{city %||% 'NULL'}'")
    logger::log_info("  postal_code: '{postal_code %||% 'NULL'}'")
    logger::log_info("  country_code: '{country_code %||% 'NULL'}'")
    logger::log_info("  delay_seconds: {delay_seconds}")
    logger::log_info("  max_records: {max_records}")
    logger::log_info("  output_csv: '{output_csv %||% 'NULL'}'")
    logger::log_info("  filter_mode: '{filter_mode}'")
    logger::log_info("  debug_structure: {debug_structure}")
    logger::log_info("  verbose: {verbose}")
  }
  
  # Build API parameters
  constructed_api_params <- .construct_api_query_parameters(
    taxonomy_description = taxonomy_description,
    taxonomy = taxonomy,
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code,
    verbose = verbose
  )
  
  # Fetch all provider records from API
  retrieved_provider_records <- .retrieve_paginated_provider_records(
    constructed_api_params = constructed_api_params,
    delay_seconds = delay_seconds,
    max_records = max_records,
    debug_structure = debug_structure,
    verbose = verbose
  )
  
  # Process and clean the provider data
  processed_provider_tibble <- .transform_provider_records_to_tibble(
    retrieved_provider_records = retrieved_provider_records,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  # CRITICAL: Deduplicate by NPI to prevent duplicate entries
  deduplicated_provider_tibble <- .deduplicate_providers_by_npi(
    processed_provider_tibble = processed_provider_tibble,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("=== SEARCH PROCESS COMPLETED ===")
    logger::log_info("Final deduplicated dataset dimensions: {nrow(deduplicated_provider_tibble)} rows, {ncol(deduplicated_provider_tibble)} columns")
    logger::log_info("Column names in final tibble: {paste(colnames(deduplicated_provider_tibble), collapse = ', ')}")
  }
  
  # Write to CSV if output path specified
  if (!is.null(output_csv)) {
    .write_provider_tibble_to_csv(
      deduplicated_provider_tibble = deduplicated_provider_tibble,
      output_csv_path = output_csv,
      verbose = verbose
    )
  }
  
  # Display API reference information
  cat("API documentation: https://npiregistry.cms.hhs.gov/api-page\n")
  
  return(deduplicated_provider_tibble)
}

#' Construct API query parameters for NPI registry search
#' @noRd
.construct_api_query_parameters <- function(taxonomy_description,
                                            taxonomy,
                                            enumeration_type,
                                            first_name,
                                            last_name,
                                            organization_name,
                                            state,
                                            city,
                                            postal_code,
                                            country_code,
                                            verbose) {
  
  constructed_params <- list(
    version = "2.1",
    taxonomy_description = taxonomy_description,
    taxonomy_code = taxonomy,  # Changed from taxonomy to taxonomy_code
    enumeration_type = enumeration_type,
    first_name = first_name,
    last_name = last_name,
    organization_name = organization_name,
    state = state,
    city = city,
    postal_code = postal_code,
    country_code = country_code
  )
  
  # Remove NULL parameters to clean the query
  non_null_params <- constructed_params[!sapply(constructed_params, is.null)]
  
  if (verbose) {
    logger::log_info("Constructed API parameters with {length(non_null_params)} non-null values")
    logger::log_info("API parameter names: {paste(names(non_null_params), collapse = ', ')}")
  }
  
  return(non_null_params)
}

#' Retrieve all provider records using API pagination
#' @noRd
.retrieve_paginated_provider_records <- function(constructed_api_params, 
                                                 delay_seconds, 
                                                 max_records, 
                                                 debug_structure, 
                                                 verbose) {
  
  npi_registry_base_url <- "https://npiregistry.cms.hhs.gov/api/"
  records_per_api_request <- 200
  current_skip_value <- 0
  accumulated_provider_records <- list()
  api_request_counter <- 0
  
  if (verbose) {
    logger::log_info("=== STARTING API DATA RETRIEVAL ===")
    logger::log_info("Base URL: {npi_registry_base_url}")
    logger::log_info("Records per request: {records_per_api_request}")
    logger::log_info("Delay between requests: {delay_seconds} seconds")
    logger::log_info("Maximum records limit: {max_records}")
  }
  
  repeat {
    api_request_counter <- api_request_counter + 1
    constructed_api_params$limit <- records_per_api_request
    constructed_api_params$skip <- current_skip_value
    
    # Apply courtesy delay between requests (except first)
    if (api_request_counter > 1 && delay_seconds > 0) {
      if (verbose) {
        logger::log_info("Applying {delay_seconds} second delay before request #{api_request_counter}")
      }
      Sys.sleep(delay_seconds)
    }
    
    if (verbose) {
      logger::log_info("Making API request #{api_request_counter}: skip={current_skip_value}, limit={records_per_api_request}")
    }
    
    # Execute API request
    current_api_response <- httr::GET(npi_registry_base_url, query = constructed_api_params)
    httr::stop_for_status(current_api_response)
    
    parsed_api_response <- httr::content(current_api_response, as = "parsed", type = "application/json")
    
    if (verbose) {
      response_result_count <- parsed_api_response$result_count %||% 0
      logger::log_info("API response #{api_request_counter} - result_count: {response_result_count}")
    }
    
    # Debug structure on first request if requested
    if (debug_structure && api_request_counter == 1 && 
        !is.null(parsed_api_response$results) && 
        length(parsed_api_response$results) > 0) {
      .examine_npi_record_structure(parsed_api_response$results[[1]], verbose)
    }
    
    # Check for valid results
    if (is.null(parsed_api_response$results) || length(parsed_api_response$results) == 0) {
      if (verbose) {
        logger::log_info("No results in API response #{api_request_counter}, ending pagination")
      }
      break
    }
    
    current_batch_record_count <- length(parsed_api_response$results)
    
    # Accumulate results
    accumulated_provider_records <- append(accumulated_provider_records, parsed_api_response$results)
    current_skip_value <- current_skip_value + records_per_api_request
    
    if (verbose) {
      total_accumulated <- length(accumulated_provider_records)
      logger::log_info("Batch #{api_request_counter}: retrieved {current_batch_record_count} records, total accumulated: {total_accumulated}")
    }
    
    # Stop if we received fewer records than requested (end of data)
    if (current_batch_record_count < records_per_api_request) {
      if (verbose) {
        logger::log_info("Received {current_batch_record_count} records (less than {records_per_api_request}), indicating end of available data")
      }
      break
    }
    
    # Safety check for maximum records limit
    if (length(accumulated_provider_records) >= max_records) {
      if (verbose) {
        logger::log_warn("Reached maximum record limit ({max_records}), stopping retrieval for performance")
      }
      break
    }
  }
  
  total_retrieved <- length(accumulated_provider_records)
  if (verbose) {
    logger::log_info("=== API RETRIEVAL COMPLETED ===")
    logger::log_info("Total provider records retrieved: {total_retrieved} across {api_request_counter} API requests")
  }
  
  return(accumulated_provider_records)
}

#' Examine and debug NPI record structure
#' @noRd
.examine_npi_record_structure <- function(sample_provider_record, verbose) {
  
  if (verbose) {
    logger::log_info("=== DEBUGGING: EXAMINING NPI RECORD STRUCTURE ===")
    
    # Show top-level fields
    top_level_field_names <- names(sample_provider_record)
    logger::log_info("Top-level fields in provider record:")
    for (field_name in top_level_field_names) {
      field_class <- class(sample_provider_record[[field_name]])[1]
      logger::log_info("  {field_name}: {field_class}")
    }
    
    # Search for NPI number in various locations
    logger::log_info("=== SEARCHING FOR NPI NUMBER FIELD ===")
    
    if (!is.null(sample_provider_record$number)) {
      logger::log_info("Found NPI as 'number': {sample_provider_record$number}")
    }
    
    if (!is.null(sample_provider_record$npi)) {
      logger::log_info("Found NPI as 'npi': {sample_provider_record$npi}")
    }
    
    # Examine basic section
    if (!is.null(sample_provider_record$basic)) {
      logger::log_info("=== BASIC SECTION FIELDS ===")
      basic_field_names <- names(sample_provider_record$basic)
      for (field_name in basic_field_names) {
        field_value <- sample_provider_record$basic[[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  basic${field_name}: '{field_value}'")
        } else {
          logger::log_info("  basic${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine addresses section
    if (!is.null(sample_provider_record$addresses) && length(sample_provider_record$addresses) > 0) {
      logger::log_info("=== ADDRESSES SECTION (FIRST ADDRESS) ===")
      first_address_fields <- names(sample_provider_record$addresses[[1]])
      for (field_name in first_address_fields) {
        field_value <- sample_provider_record$addresses[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  addresses[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  addresses[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    # Examine taxonomies section
    if (!is.null(sample_provider_record$taxonomies) && length(sample_provider_record$taxonomies) > 0) {
      logger::log_info("=== TAXONOMIES SECTION (FIRST TAXONOMY) ===")
      first_taxonomy_fields <- names(sample_provider_record$taxonomies[[1]])
      for (field_name in first_taxonomy_fields) {
        field_value <- sample_provider_record$taxonomies[[1]][[field_name]]
        if (is.character(field_value) && length(field_value) == 1) {
          logger::log_info("  taxonomies[[1]]${field_name}: '{field_value}'")
        } else {
          logger::log_info("  taxonomies[[1]]${field_name}: {class(field_value)[1]}")
        }
      }
    }
    
    logger::log_info("=== END STRUCTURE DEBUGGING ===")
  }
}

#' Transform provider records into a cleaned tibble
#' @noRd
.transform_provider_records_to_tibble <- function(retrieved_provider_records, 
                                                  taxonomy_description, 
                                                  filter_mode, 
                                                  verbose) {
  
  total_provider_records <- length(retrieved_provider_records)
  
  if (total_provider_records == 0) {
    if (verbose) {
      logger::log_warn("No provider records to process, returning empty tibble with correct structure")
    }
    # Return empty tibble with correct column structure
    return(tibble::tibble(
      npi = character(0),
      entity_type_code = character(0),
      entity_type = character(0),
      first_name = character(0),
      last_name = character(0),
      middle_name = character(0),
      organization_name = character(0),
      gender = character(0),
      sole_proprietor = character(0),
      primary_address_line1 = character(0),
      primary_address_line2 = character(0),
      primary_city = character(0),
      primary_state = character(0),
      primary_postal_code = character(0),
      primary_country_code = character(0),
      primary_telephone = character(0),
      primary_fax = character(0),
      primary_taxonomy_code = character(0),
      primary_taxonomy_description = character(0),
      primary_taxonomy_license = character(0),
      primary_taxonomy_state = character(0),
      is_primary_taxonomy = logical(0)
    ))
  }
  
  if (verbose) {
    logger::log_info("=== PROCESSING PROVIDER RECORDS ===")
    logger::log_info("Total provider records to process: {total_provider_records}")
  }
  
  # Process in batches for performance and progress tracking
  processing_batch_size <- 1000
  processed_batch_list <- list()
  
  for (batch_start_index in seq(1, total_provider_records, by = processing_batch_size)) {
    batch_end_index <- min(batch_start_index + processing_batch_size - 1, total_provider_records)
    
    if (verbose) {
      completion_percent <- round((batch_end_index / total_provider_records) * 100, 1)
      logger::log_info("Processing records {batch_start_index} to {batch_end_index} ({completion_percent}% complete)")
    }
    
    current_batch_records <- retrieved_provider_records[batch_start_index:batch_end_index]
    
    # Transform current batch to tibble
    current_batch_tibble <- purrr::map_dfr(current_batch_records, .extract_provider_information)
    processed_batch_list[[length(processed_batch_list) + 1]] <- current_batch_tibble
  }
  
  # Combine all processed batches
  if (verbose) {
    logger::log_info("Combining {length(processed_batch_list)} processed batches into unified tibble")
  }
  
  combined_provider_tibble <- dplyr::bind_rows(processed_batch_list)
  
  if (verbose) {
    logger::log_info("Combined tibble dimensions: {nrow(combined_provider_tibble)} rows, {ncol(combined_provider_tibble)} columns")
  }
  
  # Apply taxonomy description filtering
  filtered_provider_tibble <- .apply_taxonomy_filtering(
    combined_provider_tibble = combined_provider_tibble,
    taxonomy_description = taxonomy_description,
    filter_mode = filter_mode,
    verbose = verbose
  )
  
  return(filtered_provider_tibble)
}

#' Apply taxonomy description filtering based on mode
#' @noRd
.apply_taxonomy_filtering <- function(combined_provider_tibble, 
                                      taxonomy_description, 
                                      filter_mode, 
                                      verbose) {
  
  original_record_count <- nrow(combined_provider_tibble)
  
  # If no taxonomy_description provided, skip filtering
  if (is.null(taxonomy_description)) {
    if (verbose) {
      logger::log_info("=== SKIPPING TAXONOMY FILTERING ===")
      logger::log_info("No taxonomy_description provided, returning all {original_record_count} records")
    }
    return(combined_provider_tibble)
  }
  
  if (verbose) {
    logger::log_info("=== APPLYING TAXONOMY FILTERING ===")
    logger::log_info("Original record count before filtering: {original_record_count}")
    filter_description <- if (filter_mode == "starts_with") {
      "taxonomy description must start with"
    } else {
      "taxonomy description must contain"
    }
    logger::log_info("Filter mode: {filter_mode} ('{filter_description}' '{taxonomy_description}')")
    
    # Show sample taxonomy descriptions for debugging
    if (original_record_count > 0) {
      sample_taxonomies <- combined_provider_tibble$primary_taxonomy_description[1:min(10, original_record_count)]
      valid_sample_taxonomies <- sample_taxonomies[!is.na(sample_taxonomies)]
      if (length(valid_sample_taxonomies) > 0) {
        logger::log_info("Sample taxonomy descriptions found in retrieved data:")
        for (i in seq_along(valid_sample_taxonomies)) {
          logger::log_info("  {i}: '{valid_sample_taxonomies[i]}'")
        }
      }
    }
  }
  
  # Apply filtering based on selected mode
  if (filter_mode == "starts_with") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_starts(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  } else if (filter_mode == "contains") {
    filtered_provider_tibble <- combined_provider_tibble |>
      dplyr::filter(
        !is.na(primary_taxonomy_description) &
          stringr::str_detect(primary_taxonomy_description, 
                              stringr::fixed(taxonomy_description))
      )
  }
  
  filtered_record_count <- nrow(filtered_provider_tibble)
  removed_record_count <- original_record_count - filtered_record_count
  
  if (verbose) {
    logger::log_info("Taxonomy filtering completed:")
    logger::log_info("  Records kept: {filtered_record_count}")
    logger::log_info("  Records removed: {removed_record_count}")
    
    # Provide suggestions if no matches found
    if (filtered_record_count == 0 && original_record_count > 0) {
      logger::log_warn("No records matched the taxonomy filter criteria!")
      logger::log_info("Consider trying filter_mode = 'contains' or checking taxonomy spelling")
      
      # Show taxonomy descriptions that contain the search term
      alternative_matches <- combined_provider_tibble |>
        dplyr::filter(!is.na(primary_taxonomy_description) & 
                        stringr::str_detect(primary_taxonomy_description, 
                                            stringr::fixed(taxonomy_description))) |>
        dplyr::distinct(primary_taxonomy_description) |>
        dplyr::slice_head(n = 5)
      
      if (nrow(alternative_matches) > 0) {
        logger::log_info("Found {nrow(alternative_matches)} taxonomy descriptions CONTAINING '{taxonomy_description}':")
        for (i in 1:nrow(alternative_matches)) {
          logger::log_info("  - '{alternative_matches$primary_taxonomy_description[i]}'")
        }
      }
    }
  }
  
  return(filtered_provider_tibble)
}

#' Extract key information from a single provider record
#' @noRd
.extract_provider_information <- function(single_provider_record) {
  
  # Extract sections from provider record
  basic_section <- single_provider_record$basic %||% list()
  primary_address_section <- if (length(single_provider_record$addresses) > 0) {
    single_provider_record$addresses[[1]]
  } else {
    list()
  }
  primary_taxonomy_section <- if (length(single_provider_record$taxonomies) > 0) {
    single_provider_record$taxonomies[[1]]
  } else {
    list()
  }
  
  # Build flattened provider information tibble
  provider_information_tibble <- tibble::tibble(
    npi = single_provider_record$number %||% 
      basic_section$npi %||% 
      single_provider_record$npi %||% 
      NA_character_,
    entity_type_code = basic_section$enumeration_type %||% NA_character_,
    entity_type = if (!is.null(basic_section$enumeration_type)) {
      if (basic_section$enumeration_type == "NPI-1") "Individual" else "Organization"
    } else {
      NA_character_
    },
    first_name = basic_section$first_name %||% NA_character_,
    last_name = basic_section$last_name %||% NA_character_,
    middle_name = basic_section$middle_name %||% NA_character_,
    organization_name = basic_section$organization_name %||% NA_character_,
    gender = basic_section$gender %||% NA_character_,
    sole_proprietor = basic_section$sole_proprietor %||% NA_character_,
    primary_address_line1 = primary_address_section$address_1 %||% NA_character_,
    primary_address_line2 = primary_address_section$address_2 %||% NA_character_,
    primary_city = primary_address_section$city %||% NA_character_,
    primary_state = primary_address_section$state %||% NA_character_,
    primary_postal_code = primary_address_section$postal_code %||% NA_character_,
    primary_country_code = primary_address_section$country_code %||% NA_character_,
    primary_telephone = primary_address_section$telephone_number %||% NA_character_,
    primary_fax = primary_address_section$fax_number %||% NA_character_,
    primary_taxonomy_code = primary_taxonomy_section$code %||% NA_character_,
    primary_taxonomy_description = primary_taxonomy_section$desc %||% NA_character_,
    primary_taxonomy_license = primary_taxonomy_section$license %||% NA_character_,
    primary_taxonomy_state = primary_taxonomy_section$state %||% NA_character_,
    is_primary_taxonomy = primary_taxonomy_section$primary %||% NA
  )
  
  return(provider_information_tibble)
}

#' Deduplicate providers by NPI number (CRITICAL for preventing duplicates)
#' @noRd
.deduplicate_providers_by_npi <- function(processed_provider_tibble, verbose) {
  
  original_row_count <- nrow(processed_provider_tibble)
  
  if (verbose) {
    logger::log_info("=== DEDUPLICATING PROVIDERS BY NPI ===")
    logger::log_info("Records before deduplication: {original_row_count}")
  }
  
  # Handle empty tibble case
  if (original_row_count == 0) {
    if (verbose) {
      logger::log_info("No records to deduplicate, returning empty tibble")
    }
    return(processed_provider_tibble)
  }
  
  # Check for missing NPIs
  missing_npi_count <- sum(is.na(processed_provider_tibble$npi))
  if (missing_npi_count > 0 && verbose) {
    logger::log_warn("Found {missing_npi_count} records with missing NPI numbers")
  }
  
  # Remove duplicates, keeping first occurrence for each NPI
  # Arrange by NPI first to ensure consistent ordering
  deduplicated_provider_tibble <- processed_provider_tibble |>
    dplyr::arrange(npi) |>
    dplyr::distinct(npi, .keep_all = TRUE)
  
  deduplicated_row_count <- nrow(deduplicated_provider_tibble)
  duplicate_count <- original_row_count - deduplicated_row_count
  
  if (verbose) {
    logger::log_info("Deduplication completed:")
    logger::log_info("  Unique providers after deduplication: {deduplicated_row_count}")
    logger::log_info("  Duplicate records removed: {duplicate_count}")
    
    if (duplicate_count > 0) {
      logger::log_info("Duplicates were likely caused by providers having multiple:")
      logger::log_info("  - Practice locations (addresses)")
      logger::log_info("  - Taxonomy codes/specialties")
      logger::log_info("  - Database entries in the NPI registry")
    }
  }
  
  return(deduplicated_provider_tibble)
}

#' Write provider tibble to CSV file
#' @noRd
.write_provider_tibble_to_csv <- function(deduplicated_provider_tibble, 
                                          output_csv_path, 
                                          verbose) {
  
  if (verbose) {
    logger::log_info("=== WRITING RESULTS TO CSV ===")
    logger::log_info("Output CSV file path: '{output_csv_path}'")
  }
  
  # Create directory if it doesn't exist
  output_directory <- dirname(output_csv_path)
  if (!dir.exists(output_directory) && output_directory != ".") {
    dir.create(output_directory, recursive = TRUE, showWarnings = FALSE)
    if (verbose) {
      logger::log_info("Created output directory: '{output_directory}'")
    }
  }
  
  # Write CSV file
  readr::write_csv(deduplicated_provider_tibble, output_csv_path, na = "")
  
  # Verify file was written and get details
  if (file.exists(output_csv_path)) {
    file_size_bytes <- file.size(output_csv_path)
    normalized_path <- normalizePath(output_csv_path, mustWork = FALSE)
    
    if (verbose) {
      logger::log_info("Successfully wrote CSV file:")
      logger::log_info("  Records written: {nrow(deduplicated_provider_tibble)}")
      logger::log_info("  Full file path: '{normalized_path}'")
      logger::log_info("  File size: {file_size_bytes} bytes")
    }
  } else {
    logger::log_error("Failed to write CSV file: '{output_csv_path}'")
  }
}

# RECOMMENDED: Use taxonomy description with high limits ----
obgyn_physicians_comprehensive <- npi_search_all(
  taxonomy_description = "Obstetrics & Gynecology",
  taxonomy = NULL,
  enumeration_type = "NPI-1",
  delay_seconds = 0.5,
  max_records = 150000L,  # Increase significantly
  output_csv = "data/02-search_taxonomy/nppes_obgyn_physicians_comprehensive.csv",
  filter_mode = "starts_with",  # This correctly excludes nurse practitioners
  debug_structure = FALSE,
  verbose = TRUE,
  country_code = "US"
)
 
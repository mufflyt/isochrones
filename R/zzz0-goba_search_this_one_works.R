# ABOG Sequential Physician Discovery via API ----
# 📌 PURPOSE
# This script automates sequential ID-based discovery of ABOG-certified physicians 
# using the https://api.abog.org/diplomate/<ID>/verify endpoint. It is optimized 
# for discovering OB/GYN residents and recent diplomates for health workforce 
# studies, particularly for geospatial access and availability modeling.
#
# It supports adaptive request delays, aggressive/conservative search modes,
# validation with known landmark IDs (e.g., Sydney Archer), and saves structured
# results to disk for further analysis.
#
# 📥 INPUTS (with provenance)
# 1. **ABOG API Endpoint**:
#    - `https://api.abog.org/diplomate/<ID>/verify`
#    - Used dynamically; no authentication required.
#
# 2. **start_physician_id**:
#    - Starting ID (e.g., 9039807, 9040369, 9043268) based on known resident clusters.
#    - Seeded from prior verified results or targeted ranges.
#
# 3. **Optional baseline file** (if extending or merging):
#    - Not directly loaded here but downstream scripts may use:
#      - `"physician_data/discovery_results/discovered_physicians_*.csv"` — prior batches
#      - `"physician_data/discovery_results/failed_physician_ids_*.csv"` — excluded ranges
#    - These are created by this script and referenced in future analyses.
#
# 📤 OUTPUTS
# Files are automatically written to `output_directory_path`, defaulting to:
#   - `"physician_data/discovery_results"` (relative to working directory)
#
# Output files include:
# 1. **Discovered Physicians**:
#    - Filename format: `discovered_physicians_<N>_starting_<ID>_<timestamp>.csv`
#    - Contains structured data: name, city, state, ID, location string, etc.
#
# 2. **Failed IDs**:
#    - Filename: `failed_physician_ids_<timestamp>.csv`
#    - Useful for future exclusion or estimation of density.
#
# 3. **In-memory return object**:
#    - `$discovered_physicians`: data.frame of valid results
#    - `$failed_physician_ids`: vector of missing IDs
#    - `$next_search_start_id`: useful for continuing search
#    - `$discovery_success_rate`: percentage successful
#    - `$output_files_created`: list of file paths
#
# 🔁 FUNCTIONAL STRUCTURE
# ▪ `search_physicians_by_sequential_ids()`: main exported function
# ▪ Internal helpers include:
#    - `create_output_directory()` — ensure target path exists
#    - `initialize_search_parameters()` — set counters and flags
#    - `execute_physician_discovery_loop()` — main iterative loop
#    - `make_physician_api_request()` — handles Tor-based GET
#    - `enrich_physician_data()` — format and add metadata
#    - `finalize_discovery_results()` — summarize, write files
#    - `log_*()` — custom progress and diagnostics messages
#
# 🧪 DIAGNOSTICS
# ▪ `investigate_abog_api()` — test other endpoints (e.g., /profile, /status)
# ▪ `test_field_consistency()` — compare field availability across IDs
#
# 🕵️ EXAMPLES
# - Aggressive mode discovery with 2,000 targets:
#   `search_physicians_by_sequential_ids(start_physician_id = 9043268, target_physician_count = 2000)`
#
# - Conservative mode with longer delays:
#   `search_physicians_by_sequential_ids(request_delay_seconds = 3.0, aggressive_search_mode = FALSE)`
#
# 📡 NETWORK/SECURITY
# ▪ All requests are routed via Tor proxy (`socks5://localhost:<port>`, default port = 9150).
# ▪ Adds jitter and delay to prevent detection or API throttling.
#
# 📚 INTENDED USE CASES
# ▪ Identify OB/GYN workforce growth trends by ID clustering
# ▪ Support access studies for Medicaid, Medicare, or underserved regions
# ▪ Feed data into microsimulation models for provider coverage
#

# Setup ----
source("R/01-setup.R")


# 📁 CONSTANT FILE PATHS -----
# Base output directory
OUTPUT_DIRECTORY <- "physician_data/discovery_results"

# Template for discovered physicians file
DISCOVERED_PHYSICIANS_FILENAME <- function(n_found, start_id, timestamp) {
  file.path(OUTPUT_DIRECTORY, paste0("discovered_physicians_", n_found, "_starting_", start_id, "_", timestamp, ".csv"))
}

# Template for failed physician IDs file
FAILED_IDS_FILENAME <- function(timestamp) {
  file.path(OUTPUT_DIRECTORY, paste0("failed_physician_ids_", timestamp, ".csv"))
}

# Example provenance inputs (optional for reproducibility tracking)
PRIOR_DISCOVERY_FILES <- list.files(OUTPUT_DIRECTORY, pattern = "^discovered_physicians_.*\\.csv$", full.names = TRUE)
PRIOR_FAILURE_FILES <- list.files(OUTPUT_DIRECTORY, pattern = "^failed_physician_ids_.*\\.csv$", full.names = TRUE)


# Search for New Physicians Using Sequential ID Discovery ----
#'
#' This function systematically searches for physician records using sequential
#' ID-based discovery methods, similar to geographic accessibility analyses for
#' healthcare workforce research. It employs robust error handling, comprehensive
#' logging, and modular design for reproducible healthcare workforce studies.
#'
#' @param start_physician_id Numeric. The starting physician ID for sequential
#'   search. Must be a positive integer greater than 0. Default is 9039807.
#' @param target_physician_count Numeric. The target number of new physicians
#'   to discover during the search session. Must be a positive integer.
#'   Default is 100.
#' @param tor_proxy_port Numeric. The port number for Tor proxy connection.
#'   Must be a valid port number. Default is 9150.
#' @param request_delay_seconds Numeric. The delay in seconds between API
#'   requests to prevent rate limiting. Must be positive. Default is 1.5.
#' @param aggressive_search_mode Logical. Whether to use aggressive search
#'   parameters with reduced delays after successful discoveries. Default is TRUE.
#' @param verbose_logging Logical. Whether to enable detailed console logging
#'   of search progress and discoveries. Default is TRUE.
#' @param output_directory_path Character. The directory path where output files
#'   will be saved. Directory will be created if it doesn't exist.
#'   Default is "physician_data/discovery_results".
#'
#' @return A list containing:
#'   \item{discovered_physicians}{Data frame of discovered physician records}
#'   \item{failed_physician_ids}{Vector of IDs that returned no valid data}
#'   \item{next_search_start_id}{Next ID to continue search from}
#'   \item{discovery_success_rate}{Percentage of successful discoveries}
#'   \item{total_requests_made}{Total number of API requests performed}
#'
#' @examples
#' # Basic physician discovery with default parameters
#' discovery_results <- search_physicians_by_sequential_ids(
#'   start_physician_id = 9039800,
#'   target_physician_count = 25,
#'   verbose_logging = TRUE
#' )
#'
#' # Conservative search with extended delays for rate limiting
#' conservative_results <- search_physicians_by_sequential_ids(
#'   start_physician_id = 9040000,
#'   target_physician_count = 50,
#'   request_delay_seconds = 3.0,
#'   aggressive_search_mode = FALSE,
#'   output_directory_path = "data/physician_workforce"
#' )
#'
#' # Aggressive discovery for high-density ID ranges with custom output
#' aggressive_results <- search_physicians_by_sequential_ids(
#'   start_physician_id = 9039807,
#'   target_physician_count = 100,
#'   tor_proxy_port = 9150,
#'   request_delay_seconds = 1.0,
#'   aggressive_search_mode = TRUE,
#'   verbose_logging = TRUE,
#'   output_directory_path = "workforce_analysis/discovery_2024"
#' )
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr bind_rows
#' @importFrom readr write_csv
#' @importFrom httr GET use_proxy content
#' @importFrom jsonlite fromJSON
#' @importFrom stats runif
#'
#' @export
search_physicians_by_sequential_ids <- function(start_physician_id = 9039807,
                                                target_physician_count = 100,
                                                tor_proxy_port = 9150,
                                                request_delay_seconds = 1.5,
                                                aggressive_search_mode = TRUE,
                                                verbose_logging = TRUE,
                                                output_directory_path = "physician_data/discovery_results") {
  
  # Input validation with detailed logging
  logger::log_info("=== PHYSICIAN DISCOVERY SEARCH INITIATED ===")
  logger::log_info("Validating input parameters...")
  
  assertthat::assert_that(
    is.numeric(start_physician_id) && start_physician_id > 0,
    msg = "start_physician_id must be a positive numeric value"
  )
  assertthat::assert_that(
    is.numeric(target_physician_count) && target_physician_count > 0,
    msg = "target_physician_count must be a positive numeric value"
  )
  assertthat::assert_that(
    is.numeric(tor_proxy_port) && tor_proxy_port > 0 && tor_proxy_port <= 65535,
    msg = "tor_proxy_port must be a valid port number (1-65535)"
  )
  assertthat::assert_that(
    is.numeric(request_delay_seconds) && request_delay_seconds > 0,
    msg = "request_delay_seconds must be a positive numeric value"
  )
  assertthat::assert_that(
    is.logical(aggressive_search_mode),
    msg = "aggressive_search_mode must be TRUE or FALSE"
  )
  assertthat::assert_that(
    is.logical(verbose_logging),
    msg = "verbose_logging must be TRUE or FALSE"
  )
  assertthat::assert_that(
    is.character(output_directory_path),
    msg = "output_directory_path must be a character string"
  )
  
  # Log validated inputs
  logger::log_info("Input validation successful")
  logger::log_info("Starting physician ID: {start_physician_id}")
  logger::log_info("Target physician count: {target_physician_count}")
  logger::log_info("Tor proxy port: {tor_proxy_port}")
  logger::log_info("Request delay: {request_delay_seconds} seconds")
  logger::log_info("Aggressive mode: {aggressive_search_mode}")
  logger::log_info("Verbose logging: {verbose_logging}")
  logger::log_info("Output directory: {output_directory_path}")
  
  # Create output directory
  output_directory_path <- create_output_directory(output_directory_path, verbose_logging)
  
  # Initialize search parameters
  search_parameters <- initialize_search_parameters(
    start_physician_id,
    target_physician_count,
    aggressive_search_mode,
    verbose_logging
  )
  
  # Execute main discovery loop
  discovery_results <- execute_physician_discovery_loop(
    search_parameters,
    tor_proxy_port,
    request_delay_seconds,
    aggressive_search_mode,
    verbose_logging,
    target_physician_count
  )
  
  # Save results and generate summary
  final_results <- finalize_discovery_results(
    discovery_results,
    output_directory_path,
    start_physician_id,
    verbose_logging
  )
  
  logger::log_info("=== PHYSICIAN DISCOVERY SEARCH COMPLETED ===")
  return(final_results)
}

#' Create Output Directory
#' @noRd
create_output_directory <- function(output_directory_path, verbose_logging) {
  if (!dir.exists(output_directory_path)) {
    dir.create(output_directory_path, recursive = TRUE, showWarnings = FALSE)
    logger::log_info("Created output directory: {output_directory_path}")
    if (verbose_logging) {
      cat("📁 Created output directory:", output_directory_path, "\n")
    }
  } else {
    logger::log_info("Using existing output directory: {output_directory_path}")
  }
  return(output_directory_path)
}

#' Initialize Search Parameters
#' @noRd
initialize_search_parameters <- function(start_physician_id, 
                                         target_physician_count,
                                         aggressive_search_mode,
                                         verbose_logging) {
  
  search_parameters <- list(
    current_physician_id = start_physician_id,
    discovered_physicians_data = data.frame(),
    failed_physician_ids = c(),
    consecutive_failure_count = 0,
    max_consecutive_failures = if (aggressive_search_mode) 100 else 50,
    total_request_count = 0,
    search_start_time = Sys.time(),
    sydney_archer_landmark_id = 9040369
  )
  
  logger::log_info("Initialized search parameters")
  logger::log_info("Maximum consecutive failures: {search_parameters$max_consecutive_failures}")
  
  if (verbose_logging) {
    cat("🎯 TARGETING HIGH-VALUE PHYSICIAN DISCOVERY RANGE\n")
    cat("📊 Search Intelligence Summary:\n")
    cat("   - Starting physician ID: {start_physician_id}\n", sep = "")
    cat("   - Target discoveries: {target_physician_count}\n", sep = "")
    cat("   - Search mode: {ifelse(aggressive_search_mode, 'AGGRESSIVE', 'CONSERVATIVE')}\n", sep = "")
    cat("   - Expected success rate: 60-80% (HIGH)\n")
    cat("\n🚀 Starting systematic physician discovery...\n")
  }
  
  return(search_parameters)
}

#' Execute Main Physician Discovery Loop
#' @noRd
execute_physician_discovery_loop <- function(search_parameters,
                                             tor_proxy_port,
                                             request_delay_seconds,
                                             aggressive_search_mode,
                                             verbose_logging,
                                             target_physician_count) {
  
  while (nrow(search_parameters$discovered_physicians_data) < target_physician_count && 
         search_parameters$consecutive_failure_count < search_parameters$max_consecutive_failures) {
    
    # Progress logging every 25 requests
    if (verbose_logging && search_parameters$total_request_count %% 25 == 0) {
      log_search_progress(search_parameters)
    }
    
    # Make API request
    api_response <- make_physician_api_request(
      search_parameters$current_physician_id, 
      tor_proxy_port
    )
    search_parameters$total_request_count <- search_parameters$total_request_count + 1
    
    # Process response
    search_parameters <- process_api_response(
      api_response,
      search_parameters,
      verbose_logging
    )
    
    # Validate landmark if reached
    search_parameters <- validate_landmark_physician(
      search_parameters,
      api_response,
      verbose_logging
    )
    
    # Apply adaptive delay
    adaptive_delay <- calculate_adaptive_delay(
      request_delay_seconds,
      aggressive_search_mode,
      nrow(search_parameters$discovered_physicians_data),
      api_response$success
    )
    
    Sys.sleep(adaptive_delay)
    search_parameters$current_physician_id <- search_parameters$current_physician_id + 1
  }
  
  return(search_parameters)
}

#' Make Single Physician API Request
#' @noRd
make_physician_api_request <- function(physician_id, tor_proxy_port) {
  api_url <- paste0("https://api.abog.org/diplomate/", physician_id, "/verify")
  
  logger::log_info("Making API request for physician ID: {physician_id}")
  
  tryCatch({
    api_response <- httr::GET(
      api_url, 
      httr::use_proxy(paste0("socks5://localhost:", tor_proxy_port))
    )
    
    if (api_response$status_code == 200) {
      response_content <- httr::content(api_response, as = "text", encoding = "UTF-8")
      
      if (nchar(response_content) == 0 || 
          response_content == "null" || 
          response_content == "{}") {
        logger::log_warn("Empty response for physician ID: {physician_id}")
        return(list(success = FALSE, physician_data = NULL))
      }
      
      parsed_data <- jsonlite::fromJSON(response_content)
      
      if (is.list(parsed_data) && length(parsed_data) > 0) {
        logger::log_info("Successful data retrieval for physician ID: {physician_id}")
        return(list(success = TRUE, physician_data = parsed_data))
      } else {
        logger::log_warn("Invalid data structure for physician ID: {physician_id}")
        return(list(success = FALSE, physician_data = NULL))
      }
      
    } else {
      logger::log_warn("HTTP error {api_response$status_code} for physician ID: {physician_id}")
      return(list(success = FALSE, physician_data = NULL))
    }
    
  }, error = function(e) {
    logger::log_error("API request error for physician ID {physician_id}: {e$message}")
    return(list(success = FALSE, physician_data = NULL))
  })
}

#' Process API Response
#' @noRd
process_api_response <- function(api_response, search_parameters, verbose_logging) {
  
  if (api_response$success && length(api_response$physician_data) > 0) {
    search_parameters$consecutive_failure_count <- 0
    
    # Enrich physician data
    enriched_physician_data <- enrich_physician_data(
      api_response$physician_data,
      search_parameters$current_physician_id
    )
    
    search_parameters$discovered_physicians_data <- dplyr::bind_rows(
      search_parameters$discovered_physicians_data,
      enriched_physician_data
    )
    
    if (verbose_logging) {
      log_successful_discovery(enriched_physician_data)
    }
    
    logger::log_info("Successfully processed physician discovery: {enriched_physician_data$physician_name}")
    
  } else {
    search_parameters$consecutive_failure_count <- search_parameters$consecutive_failure_count + 1
    search_parameters$failed_physician_ids <- c(
      search_parameters$failed_physician_ids,
      search_parameters$current_physician_id
    )
    
    if (verbose_logging && search_parameters$consecutive_failure_count %% 20 == 0) {
      cat("⚠️  Consecutive failures:", search_parameters$consecutive_failure_count, 
          "at ID:", search_parameters$current_physician_id, "\n")
    }
    
    logger::log_warn("Failed discovery for physician ID: {search_parameters$current_physician_id}")
  }
  
  return(search_parameters)
}

#' Enrich Physician Data
#' @noRd
enrich_physician_data <- function(physician_data, current_physician_id) {
  
  # Safely extract location information
  physician_city <- extract_safe_value(physician_data$city, "")
  physician_state <- extract_safe_value(physician_data$state, "")
  physician_name <- extract_safe_value(physician_data$name, "Unknown Name")
  
  # Construct location string
  physician_location <- construct_location_string(physician_city, physician_state)
  
  enriched_data <- data.frame(
    physician_id = current_physician_id,
    physician_name = physician_name,
    physician_city = physician_city,
    physician_state = physician_state,
    physician_location = physician_location,
    discovery_timestamp = Sys.time(),
    search_methodology = "sequential_id_discovery",
    likely_recent_graduate = TRUE,
    stringsAsFactors = FALSE
  )
  
  # Add any additional fields from original data
  additional_fields <- setdiff(names(physician_data), 
                               c("name", "city", "state"))
  
  for (field_name in additional_fields) {
    if (length(physician_data[[field_name]]) == 1) {
      enriched_data[[field_name]] <- physician_data[[field_name]]
    }
  }
  
  return(enriched_data)
}

#' Extract Safe Value
#' @noRd
extract_safe_value <- function(value, default_value = "") {
  if (is.null(value) || is.na(value) || length(value) == 0) {
    return(default_value)
  }
  return(as.character(value))
}

#' Construct Location String
#' @noRd
construct_location_string <- function(physician_city, physician_state) {
  if (nchar(physician_city) > 0 && nchar(physician_state) > 0) {
    return(paste0(physician_city, ", ", physician_state))
  } else if (nchar(physician_city) > 0) {
    return(physician_city)
  } else if (nchar(physician_state) > 0) {
    return(physician_state)
  } else {
    return("Location Not Available")
  }
}

#' Log Successful Discovery
#' @noRd
log_successful_discovery <- function(enriched_physician_data) {
  cat("🎉 NEW PHYSICIAN DISCOVERED! ID:", enriched_physician_data$physician_id,
      "| Name:", enriched_physician_data$physician_name,
      "| Location:", enriched_physician_data$physician_location, "\n")
}

#' Log Search Progress
#' @noRd
log_search_progress <- function(search_parameters) {
  elapsed_minutes <- as.numeric(
    difftime(Sys.time(), search_parameters$search_start_time, units = "mins")
  )
  success_rate <- if (search_parameters$total_request_count > 0) {
    nrow(search_parameters$discovered_physicians_data) / search_parameters$total_request_count * 100
  } else {
    0
  }
  
  cat("📈 Progress: ID", search_parameters$current_physician_id, 
      "| Found:", nrow(search_parameters$discovered_physicians_data),
      "| Requests:", search_parameters$total_request_count, 
      "| Success:", round(success_rate, 1), 
      "%| Time:", round(elapsed_minutes, 1), "min\n")
}

#' Validate Landmark Physician
#' @noRd
validate_landmark_physician <- function(search_parameters, api_response, verbose_logging) {
  if (search_parameters$current_physician_id == search_parameters$sydney_archer_landmark_id) {
    if (api_response$success && length(api_response$physician_data) > 0) {
      if (verbose_logging) {
        cat("✅ LANDMARK CONFIRMED: Sydney Archer found at expected ID", 
            search_parameters$sydney_archer_landmark_id, "\n")
      }
      logger::log_info("Landmark validation successful: Sydney Archer confirmed")
    } else {
      if (verbose_logging) {
        cat("⚠️  LANDMARK ISSUE: Sydney Archer not found at", 
            search_parameters$sydney_archer_landmark_id, "- data may be outdated\n")
      }
      logger::log_warn("Landmark validation failed: Sydney Archer not found at expected ID")
    }
  }
  return(search_parameters)
}

#' Calculate Adaptive Delay
#' @noRd
calculate_adaptive_delay <- function(base_delay_seconds, 
                                     aggressive_search_mode,
                                     successful_discoveries,
                                     request_successful) {
  
  if (aggressive_search_mode && successful_discoveries > 5) {
    adaptive_delay <- max(0.5, base_delay_seconds * 0.7)
  } else {
    adaptive_delay <- base_delay_seconds
  }
  
  # Add jitter to prevent predictable timing
  jitter_amount <- stats::runif(1, -0.2, 0.2)
  final_delay <- max(0.3, adaptive_delay + jitter_amount)
  
  return(final_delay)
}

#' Finalize Discovery Results
#' @noRd
finalize_discovery_results <- function(search_parameters,
                                       output_directory_path,
                                       start_physician_id,
                                       verbose_logging) {
  
  search_end_time <- Sys.time()
  total_duration_minutes <- as.numeric(
    difftime(search_end_time, search_parameters$search_start_time, units = "mins")
  )
  final_success_rate <- if (search_parameters$total_request_count > 0) {
    nrow(search_parameters$discovered_physicians_data) / search_parameters$total_request_count * 100
  } else {
    0
  }
  
  # Save discovery results
  saved_files <- save_discovery_results(
    search_parameters,
    output_directory_path,
    start_physician_id
  )
  
  # Log final summary
  log_final_summary(
    search_parameters,
    total_duration_minutes,
    final_success_rate,
    saved_files,
    verbose_logging
  )
  
  # Prepare return object
  final_results <- list(
    discovered_physicians = search_parameters$discovered_physicians_data,
    failed_physician_ids = search_parameters$failed_physician_ids,
    next_search_start_id = search_parameters$current_physician_id,
    discovery_success_rate = final_success_rate,
    total_requests_made = search_parameters$total_request_count,
    search_duration_minutes = total_duration_minutes,
    output_files_created = saved_files
  )
  
  logger::log_info("Discovery results finalized with {nrow(search_parameters$discovered_physicians_data)} physicians found")
  
  return(final_results)
}

#' Save Discovery Results
#' @noRd
save_discovery_results <- function(search_parameters, 
                                   output_directory_path,
                                   start_physician_id) {
  
  current_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  saved_files <- list()
  
  # Save discovered physicians
  if (nrow(search_parameters$discovered_physicians_data) > 0) {
    physicians_filename <- DISCOVERED_PHYSICIANS_FILENAME(
      n_found = nrow(search_parameters$discovered_physicians_data),
      start_id = start_physician_id,
      timestamp = current_timestamp
    )
    
    readr::write_csv(search_parameters$discovered_physicians_data, physicians_filename)
    saved_files$physicians_file <- physicians_filename
    
    logger::log_info("Saved discovered physicians to: {physicians_filename}")
  }
  
  # Save failed IDs
  if (length(search_parameters$failed_physician_ids) > 0) {
    failed_ids_filename <- FAILED_IDS_FILENAME(timestamp = current_timestamp)
    
    failed_ids_data <- data.frame(
      failed_physician_id = search_parameters$failed_physician_ids,
      search_methodology = "sequential_id_discovery",
      search_date = Sys.Date(),
      stringsAsFactors = FALSE
    )
    
    readr::write_csv(failed_ids_data, failed_ids_filename)
    saved_files$failed_ids_file <- failed_ids_filename
    
    logger::log_info("Saved failed IDs to: {failed_ids_filename}")
  }
  
  return(saved_files)
}

#' Log Final Summary
#' @noRd
log_final_summary <- function(search_parameters,
                              total_duration_minutes,
                              final_success_rate,
                              saved_files,
                              verbose_logging) {
  
  discovered_physician_count <- nrow(search_parameters$discovered_physicians_data)
  
  if (verbose_logging) {
    cat("\n=== PHYSICIAN DISCOVERY SEARCH COMPLETED ===\n")
    cat("⏱️  Total duration:", round(total_duration_minutes, 1), "minutes\n")
    cat("📊 Total requests:", search_parameters$total_request_count, "\n")
    cat("🎯 Physicians discovered:", discovered_physician_count, "\n")
    cat("📈 Success rate:", round(final_success_rate, 1), "%\n")
    cat("🔄 Consecutive failures at end:", search_parameters$consecutive_failure_count, "\n")
    
    if (discovered_physician_count > 0) {
      physician_id_range <- range(search_parameters$discovered_physicians_data$physician_id)
      cat("📍 Physician ID range:", physician_id_range[1], "to", physician_id_range[2], "\n")
      cat("➡️  Continue search from ID:", search_parameters$current_physician_id, "\n")
      
      # Calculate discovery density
      id_span <- max(search_parameters$discovered_physicians_data$physician_id) - 
        min(search_parameters$discovered_physicians_data$physician_id) + 1
      density_rate <- discovered_physician_count / id_span
      estimated_next_1000 <- density_rate * 1000
      cat("🔮 Estimated physicians in next 1000 IDs:", round(estimated_next_1000), "\n")
    }
    
    # Provide continuation recommendation
    if (discovered_physician_count > 0 && search_parameters$consecutive_failure_count < 50) {
      cat("\n🚀 RECOMMENDATION: High success rate detected. Continue searching forward.\n")
    } else {
      cat("\n🔄 RECOMMENDATION: Consider different strategy or check connection.\n")
    }
    
    # Log saved files
    if (length(saved_files) > 0) {
      cat("\n💾 Output files created:\n")
      for (file_type in names(saved_files)) {
        cat("   -", saved_files[[file_type]], "\n")
      }
    }
  }
  
  logger::log_info("Final summary logged - {discovered_physician_count} physicians discovered")
}

# run ----
search_physicians_by_sequential_ids(
  start_physician_id = 9000000,
  target_physician_count = 50000,  # This will now work correctly
  verbose_logging = TRUE
)

# DIAGNOSTICS ----
#' Investigate ABOG API for Additional Fields
#'
#' This function explores different API endpoints and request methods to find
#' the missing fields from your original GOBA data.
#'
#' @param physician_id Integer. Known physician ID to test with
#' @param tor_port Integer. Tor port (default: 9150)
#' @param verbose Logical. Enable verbose output (default: TRUE)
#'
#' @examples
#' # Test with a known physician ID
#' investigate_abog_api(physician_id = 9040482, verbose = TRUE)
#'
#' @export
investigate_abog_api <- function(physician_id = 9040482, 
                                 tor_port = 9150, 
                                 verbose = TRUE) {
  
  if (verbose) {
    cat("🔍 INVESTIGATING ABOG API FOR ADDITIONAL FIELDS\n")
    cat("Testing physician ID:", physician_id, "\n\n")
  }
  
  # Test different potential endpoints
  endpoints_to_test <- list(
    "verify" = "/verify",
    "details" = "/details", 
    "profile" = "/profile",
    "info" = "/info",
    "complete" = "/complete",
    "full" = "/full",
    "subspecialty" = "/subspecialty",
    "certification" = "/certification",
    "status" = "/status"
  )
  
  base_url <- "https://api.abog.org/diplomate/"
  results <- list()
  
  for (endpoint_name in names(endpoints_to_test)) {
    endpoint <- endpoints_to_test[[endpoint_name]]
    url <- paste0(base_url, physician_id, endpoint)
    
    if (verbose) cat("Testing endpoint:", endpoint_name, "->", url, "\n")
    
    tryCatch({
      response <- httr::GET(url, httr::use_proxy(paste0("socks5://localhost:", tor_port)))
      
      if (response$status_code == 200) {
        content_text <- httr::content(response, as = "text", encoding = "UTF-8")
        
        if (nchar(content_text) > 0 && content_text != "null" && content_text != "{}") {
          data <- jsonlite::fromJSON(content_text)
          
          if (is.list(data) && length(data) > 0) {
            results[[endpoint_name]] <- list(
              status = "SUCCESS",
              fields = names(data),
              data = data
            )
            
            if (verbose) {
              cat("  ✅ SUCCESS! Fields found:", paste(names(data), collapse = ", "), "\n")
            }
          } else {
            results[[endpoint_name]] <- list(status = "EMPTY", fields = character(0))
            if (verbose) cat("  ⚠️  Empty response\n")
          }
        } else {
          results[[endpoint_name]] <- list(status = "NULL", fields = character(0))
          if (verbose) cat("  ⚠️  Null/empty content\n")
        }
      } else {
        results[[endpoint_name]] <- list(status = paste("HTTP", response$status_code), fields = character(0))
        if (verbose) cat("  ❌ HTTP", response$status_code, "\n")
      }
      
    }, error = function(e) {
      results[[endpoint_name]] <- list(status = paste("ERROR:", e$message), fields = character(0))
      if (verbose) cat("  ❌ Error:", e$message, "\n")
    })
    
    Sys.sleep(1)  # Be respectful
  }
  
  # Test different base URLs
  if (verbose) cat("\n🌐 Testing alternative base URLs...\n")
  
  alternative_bases <- c(
    "https://www.abog.org/api/diplomate/",
    "https://abog.org/api/diplomate/", 
    "https://api.abog.org/physician/",
    "https://api.abog.org/member/",
    "https://api.abog.org/doctor/"
  )
  
  for (base in alternative_bases) {
    url <- paste0(base, physician_id, "/verify")
    if (verbose) cat("Testing base URL:", base, "\n")
    
    tryCatch({
      response <- httr::GET(url, httr::use_proxy(paste0("socks5://localhost:", tor_port)))
      
      if (response$status_code == 200) {
        if (verbose) cat("  ✅ Alternative base URL works!\n")
        # Could explore this further
      } else {
        if (verbose) cat("  ❌ HTTP", response$status_code, "\n")
      }
      
    }, error = function(e) {
      if (verbose) cat("  ❌ Error:", e$message, "\n")
    })
    
    Sys.sleep(1)
  }
  
  # Analyze results
  if (verbose) {
    cat("\n📊 ANALYSIS SUMMARY:\n")
    cat("Successful endpoints:\n")
    
    successful_endpoints <- names(results)[sapply(results, function(x) x$status == "SUCCESS")]
    
    if (length(successful_endpoints) > 0) {
      for (endpoint in successful_endpoints) {
        cat("  ✅", endpoint, "- Fields:", length(results[[endpoint]]$fields), "\n")
      }
      
      # Compare fields across successful endpoints
      all_unique_fields <- unique(unlist(sapply(results[successful_endpoints], function(x) x$fields)))
      
      cat("\nAll unique fields found across endpoints:\n")
      for (field in sort(all_unique_fields)) {
        endpoints_with_field <- names(results)[sapply(results, function(x) field %in% x$fields)]
        cat("  -", field, "- Available in:", paste(endpoints_with_field, collapse = ", "), "\n")
      }
      
      # Check for missing important fields
      missing_important <- c("sub1", "clinicallyActive", "sub1startDate", "sub1mocStatus", "npi")
      found_important <- intersect(missing_important, all_unique_fields)
      still_missing <- setdiff(missing_important, all_unique_fields)
      
      if (length(found_important) > 0) {
        cat("\n🎉 FOUND MISSING IMPORTANT FIELDS:\n")
        for (field in found_important) {
          endpoints_with_field <- names(results)[sapply(results, function(x) field %in% x$fields)]
          cat("  ✅", field, "- Found in:", paste(endpoints_with_field, collapse = ", "), "\n")
        }
      }
      
      if (length(still_missing) > 0) {
        cat("\n❌ STILL MISSING IMPORTANT FIELDS:\n")
        for (field in still_missing) {
          cat("  -", field, "\n")
        }
      }
      
    } else {
      cat("  No successful alternative endpoints found.\n")
      cat("  The /verify endpoint may be the only available option.\n")
    }
  }
  
  return(results)
}

#' Test Multiple Physician IDs for Field Consistency
#'
#' Tests several physician IDs to see if field availability is consistent
#' and to find physicians with subspecialty data.
#'
#' @param physician_ids Vector of physician IDs to test
#' @param tor_port Integer. Tor port (default: 9150)
#' @param verbose Logical. Enable verbose output (default: TRUE)
#'
#' @examples
#' # Test multiple known IDs
#' test_field_consistency(c(9040482, 9040500, 9040520), verbose = TRUE)
#'
#' @export
test_field_consistency <- function(physician_ids = c(9040482, 9040500, 9040520, 9039807, 9040369),
                                   tor_port = 9150,
                                   verbose = TRUE) {
  
  if (verbose) {
    cat("🧪 TESTING FIELD CONSISTENCY ACROSS MULTIPLE PHYSICIANS\n")
    cat("Testing", length(physician_ids), "physician IDs\n\n")
  }
  
  all_responses <- list()
  all_fields_found <- character(0)
  
  for (i in seq_along(physician_ids)) {
    id <- physician_ids[i]
    
    if (verbose) cat("Testing ID", id, "...")
    
    url <- paste0("https://api.abog.org/diplomate/", id, "/verify")
    
    tryCatch({
      response <- httr::GET(url, httr::use_proxy(paste0("socks5://localhost:", tor_port)))
      
      if (response$status_code == 200) {
        content_text <- httr::content(response, as = "text", encoding = "UTF-8")
        
        if (nchar(content_text) > 0 && content_text != "null" && content_text != "{}") {
          data <- jsonlite::fromJSON(content_text)
          
          if (is.list(data) && length(data) > 0) {
            all_responses[[as.character(id)]] <- data
            current_fields <- names(data)
            all_fields_found <- unique(c(all_fields_found, current_fields))
            
            if (verbose) cat(" ✅", length(current_fields), "fields\n")
            
            # Check for any subspecialty indicators
            subspecialty_indicators <- c("sub1", "subspecialty", "fellowship", "certification", "specialty")
            found_subspecialty <- intersect(subspecialty_indicators, current_fields)
            
            if (length(found_subspecialty) > 0 && verbose) {
              cat("    🎯 SUBSPECIALTY DATA:", paste(found_subspecialty, collapse = ", "), "\n")
            }
          } else {
            if (verbose) cat(" ⚠️  Empty\n")
          }
        } else {
          if (verbose) cat(" ⚠️  Null\n")
        }
      } else {
        if (verbose) cat(" ❌ HTTP", response$status_code, "\n")
      }
      
    }, error = function(e) {
      if (verbose) cat(" ❌ Error\n")
    })
    
    Sys.sleep(1)  # Be respectful
  }
  
  # Analysis
  if (verbose && length(all_responses) > 0) {
    cat("\n📊 FIELD CONSISTENCY ANALYSIS:\n")
    
    # Check which fields appear in all responses
    consistent_fields <- character(0)
    inconsistent_fields <- character(0)
    
    for (field in all_fields_found) {
      appearances <- sum(sapply(all_responses, function(x) field %in% names(x)))
      percentage <- round(appearances / length(all_responses) * 100, 1)
      
      if (appearances == length(all_responses)) {
        consistent_fields <- c(consistent_fields, field)
      } else {
        inconsistent_fields <- c(inconsistent_fields, field)
      }
      
      cat("  ", field, ":", appearances, "/", length(all_responses), "(", percentage, "%)\n")
    }
    
    cat("\n✅ Fields in ALL responses:", paste(consistent_fields, collapse = ", "), "\n")
    
    if (length(inconsistent_fields) > 0) {
      cat("⚠️  Fields in SOME responses:", paste(inconsistent_fields, collapse = ", "), "\n")
      cat("    (These may indicate subspecialty or status-dependent data)\n")
    }
  }
  
  return(all_responses)
}

cat("🔍 API Investigation tools loaded!\n")
cat("📋 Available functions:\n")
cat("   - investigate_abog_api() - Test different endpoints\n")
cat("   - test_field_consistency() - Check field consistency across physicians\n")
cat("🚀 Try: investigate_abog_api(9040482)\n")

# This will test multiple endpoints and show you what's available
investigate_abog_api(9040482)

# Keep collecting - we can enhance later
next_batch <- search_new_residents(
  start_id = 9040696,
  target_new_physicians = 10
)
                                                
#
# 0-this one works_adaptive_binary_search.R
#
# Purpose:
# This script implements a high-efficiency, resident-focused scraper for the 
# American Board of Obstetrics and Gynecology (ABOG) diplomate verification API. 
# The objective is to identify recent or upcoming OB/GYN graduates (e.g., class of 
# 2020–2026) by querying specific ID ranges and detecting valid certification data. 
# It includes multiple scraping strategies optimized for dense resident clusters and 
# dynamically filters out known invalid IDs to improve performance and reduce 
# redundant API requests.
#
# Inputs:
#
# 1. **ABOG Verification API**
#    - Endpoint: `https://api.abog.org/diplomate/<ID>/verify`
#    - Used dynamically to query each physician ID in a specified range.
#    - Response is parsed for valid physician data (JSON).
#
# 2. **Local CSV: MASTER_all_wrongs.csv**
#    - Path: `<output_directory>/MASTER_all_wrongs.csv`
#             (e.g., "~/Desktop/MASTER_all_wrongs.csv" or 
#             "physician_data/discovery_results/MASTER_all_wrongs.csv")
#    - Provenance: Created and updated by this script during previous runs.
#    - Contains a vector of previously discovered "wrong" (invalid) ABOG IDs.
#    - Used at startup to exclude IDs below a configurable threshold 
#      (`wrong_id_filter_threshold`).
#
# Parameters:
# - `strategy`: Strategy for ID generation: one of `"adaptive_binary"`, 
#   `"pattern_based"`, `"resident_pattern"`, `"chunk_sampling"`, or `"hybrid"`.
# - `start_id`, `end_id`: Integer range of ABOG IDs to scrape (e.g., 9040000–9055000).
# - `tor_port`: SOCKS5 port for routing traffic through Tor (default: 9150).
# - `max_requests`: Maximum number of total API requests in a run.
# - `delay_seconds`: Wait time between requests (jittered by ±0.3 sec).
# - `chunk_size`: For `"chunk_sampling"`, number of IDs per chunk (default: 1000).
# - `output_directory`: Path to write all CSV results and logs 
#   (e.g., "physician_data/discovery_results").
#
# Outputs:
#
# 1. **Physician Results CSV**
#    - Filename format: 
#      `physicians_<strategy>_<start>-<end>_filtered_<threshold>_<timestamp>.csv`
#    - Path: `<output_directory>/physicians_...csv`
#    - Content: Structured data for all valid physicians returned by the API, 
#      including scraped ID, name, graduation year, and timestamp.
#
# 2. **Wrong IDs CSV**
#    - Filename format: 
#      `wrong_ids_<strategy>_<start>-<end>_filtered_<threshold>_<timestamp>.csv`
#    - Path: `<output_directory>/wrong_ids_...csv`
#    - Content: All new invalid IDs discovered during this run with metadata 
#      (strategy used, threshold applied, scrape date).
#
# 3. **In-memory return object**
#    - `$physicians`: Data frame of valid physician records.
#    - `$wrong_ids`: Character vector of newly discovered invalid IDs.
#    - `$statistics`: Performance metrics: success rate, number of recent graduates,
#      duration (in minutes), request rate.
#    - `$files`: List of output CSV file paths.
#
# File Provenance Summary:
# - `MASTER_all_wrongs.csv`: Created and updated automatically by the script. 
#   Used to exclude previously known invalid IDs and optionally re-check IDs 
#   that were previously skipped but now exceed `wrong_id_filter_threshold`.
#
# - All CSV outputs (`physicians_*.csv`, `wrong_ids_*.csv`): Created in the folder 
#   defined by `output_directory`, typically one of:
#     • "~/Desktop/" (default)
#     • "physician_data/discovery_results/" (when set explicitly)
#
# Use Cases:
# - Building a verified list of OB/GYN residents and recent diplomates.
# - Evaluating workforce growth and certification clustering by year.
# - Mapping resident availability for mystery caller or access studies.
#
# Notes:
# - Script uses Tor for anonymous, distributed API access via `httr::use_proxy()`.
# - Includes adaptive and targeted ID generation strategies based on known resident 
#   ID ranges and recent graduate clustering patterns.
# - Success is logged and progress is printed at regular intervals (every 50 requests).
#
#

source("R/01-setup.R")

#
# 🔧 CONSTANTS: File Paths, Templates, and Directories -----
#

# Base directory for saving all output CSVs
BASE_OUTPUT_DIR <- "physician_data/discovery_results"

# Master wrong IDs file
MASTER_WRONG_IDS_FILE <- file.path(BASE_OUTPUT_DIR, "MASTER_all_wrongs.csv")

# Dynamic filename generator for valid physicians
make_physicians_output_file <- function(strategy, start_id, end_id, threshold, timestamp) {
  file.path(BASE_OUTPUT_DIR, paste0(
    "physicians_", strategy, "_", start_id, "-", end_id,
    "_filtered_", threshold, "_", timestamp, ".csv"
  ))
}

# Dynamic filename generator for wrong IDs
make_wrong_ids_output_file <- function(strategy, start_id, end_id, threshold, timestamp) {
  file.path(BASE_OUTPUT_DIR, paste0(
    "wrong_ids_", strategy, "_", start_id, "-", end_id,
    "_filtered_", threshold, "_", timestamp, ".csv"
  ))
}

# EXPLORES ABOG physician IDS ----
# scrape_abog_physicians
#' Efficient ABOG Physician Data Scraper with Corrected Chunk Sampling and Batch Saving
#'
#' This function implements multiple strategies to efficiently scrape physician
#' data from the ABOG API while respecting chunk_size and implementing batch saving.
#'
#' @param strategy Character. Strategy to use: "adaptive_binary", "pattern_based", 
#'   "resident_pattern", "chunk_sampling", or "hybrid"
#' @param start_id Integer. Starting ID for scraping (default: 1)
#' @param end_id Integer. Ending ID for scraping (default: 10000000)
#' @param tor_port Integer. Port number for Tor SOCKS proxy (default: 9150)
#' @param max_requests Integer. Maximum number of requests to make (default: 1000)
#' @param delay_seconds Numeric. Delay between requests in seconds (default: 2)
#' @param chunk_size Integer. Distance between IDs to sample (default: 100)
#' @param batch_save_size Integer. Save results every N successful discoveries (default: 25)
#' @param wrong_id_filter_threshold Integer. Filter wrong IDs below this value 
#'   (default: 9040372 - Erin Franks ID)
#' @param verbose Logical. Enable verbose logging (default: TRUE)
#' @param output_directory Character. Directory to save output files (default: "physician_data/discovery_results")
#'
#' @return A list containing physicians data frame, wrong IDs, and statistics
#'
#' @importFrom httr GET content use_proxy
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows filter mutate select arrange
#' @importFrom readr read_csv write_csv
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#'
#' @examples
#' # Proper chunk sampling with batch saving
#' gap_search_results <- scrape_abog_physicians_fixed(
#'   strategy = "chunk_sampling",
#'   start_id = 9042699,
#'   end_id = 9042999,
#'   max_requests = 1000,
#'   chunk_size = 100,
#'   batch_save_size = 10,
#'   wrong_id_filter_threshold = 9040372,
#'   delay_seconds = 1.5,
#'   verbose = TRUE
#' )
#'
#' @export
scrape_abog_physicians_fixed <- function(strategy = "adaptive_binary",
                                         start_id = 1,
                                         end_id = 10000000,
                                         tor_port = 9150,
                                         max_requests = 1000,
                                         delay_seconds = 2,
                                         chunk_size = 100,
                                         batch_save_size = 25,
                                         wrong_id_filter_threshold = 9040372,
                                         verbose = TRUE,
                                         output_directory = "physician_data/discovery_results") {
  
  # Input validation with updated parameters
  assertthat::assert_that(is.character(strategy))
  assertthat::assert_that(is.numeric(start_id) && start_id > 0)
  assertthat::assert_that(is.numeric(end_id) && end_id > start_id)
  assertthat::assert_that(is.numeric(tor_port) && tor_port > 0)
  assertthat::assert_that(is.numeric(max_requests) && max_requests > 0)
  assertthat::assert_that(is.numeric(chunk_size) && chunk_size > 0)
  assertthat::assert_that(is.numeric(batch_save_size) && batch_save_size > 0)
  assertthat::assert_that(is.numeric(wrong_id_filter_threshold))
  assertthat::assert_that(is.logical(verbose))
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE, showWarnings = FALSE)
  }
  
  if (verbose) {
    logger::log_info("Starting filtered ABOG physician scraper with batch saving")
    logger::log_info("Strategy: {strategy}, Range: {start_id}-{end_id}")
    logger::log_info("Max requests: {max_requests}, Delay: {delay_seconds}s")
    logger::log_info("Chunk size: {chunk_size}, Batch save size: {batch_save_size}")
    logger::log_info("Wrong ID filter threshold: {wrong_id_filter_threshold}")
  }
  
  # Load existing wrong IDs with filtering
  wrong_ids_existing <- load_filtered_wrong_ids(output_directory, wrong_id_filter_threshold, verbose)
  
  # Initialize tracking variables
  physicians_data_collected <- data.frame()
  wrong_ids_new_discovered <- c()
  request_count_total <- 0
  physicians_found_count <- 0
  last_batch_save_count <- 0
  scraping_start_time <- Sys.time()
  
  # Strategy selection with corrected chunk sampling
  candidate_ids_to_check <- switch(strategy,
                                   "adaptive_binary" = generate_adaptive_binary_ids_focused(start_id, end_id, max_requests, verbose),
                                   "pattern_based" = generate_resident_pattern_ids(start_id, end_id, max_requests, verbose),
                                   "resident_pattern" = generate_resident_pattern_ids(start_id, end_id, max_requests, verbose),
                                   "chunk_sampling" = generate_chunk_sampling_ids_corrected(start_id, end_id, chunk_size, max_requests, verbose),
                                   "hybrid" = generate_hybrid_resident_search(start_id, end_id, max_requests, verbose),
                                   stop("Unknown strategy: {strategy}")
  )
  
  if (verbose) {
    logger::log_info("Generated {length(candidate_ids_to_check)} candidate IDs using {strategy} strategy")
    logger::log_info("Filtered {length(wrong_ids_existing)} existing wrong IDs (threshold: {wrong_id_filter_threshold})")
    if (strategy == "chunk_sampling") {
      logger::log_info("Expected ID spacing: every {chunk_size} IDs")
    }
  }
  
  # Main scraping loop with batch saving
  for (current_id in candidate_ids_to_check) {
    if (request_count_total >= max_requests) {
      if (verbose) logger::log_info("Reached maximum request limit of {max_requests}")
      break
    }
    
    # Skip if ID is in filtered existing wrong IDs
    if (current_id %in% wrong_ids_existing) {
      if (verbose) logger::log_info("Skipping known wrong ID: {current_id}")
      next
    }
    
    # Make API request with enhanced error handling
    api_request_result <- make_abog_api_request_enhanced(current_id, tor_port, verbose)
    request_count_total <- request_count_total + 1
    
    if (api_request_result$success) {
      if (length(api_request_result$physician_data) > 0) {
        # Valid physician found - enhance data collection
        enhanced_physician_data <- enhance_physician_data(api_request_result$physician_data, current_id)
        physicians_data_collected <- dplyr::bind_rows(physicians_data_collected, enhanced_physician_data)
        physicians_found_count <- physicians_found_count + 1
        
        if (verbose) {
          physician_name <- enhanced_physician_data$name %||% "Unknown"
          display_info <- enhanced_physician_data$display_info %||% "Basic info"
          logger::log_info("Found physician {current_id}: {physician_name} ({display_info})")
        }
        
        # Check if batch save is needed
        if (physicians_found_count > last_batch_save_count && 
            (physicians_found_count - last_batch_save_count) >= batch_save_size) {
          
          batch_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
          batch_filename <- file.path(output_directory, 
                                      paste0("batch_", strategy, "_", start_id, "-", end_id, 
                                             "_found", physicians_found_count, "_", batch_timestamp, ".csv"))
          
          readr::write_csv(physicians_data_collected, batch_filename)
          last_batch_save_count <- physicians_found_count
          
          if (verbose) {
            logger::log_info("💾 BATCH SAVED: {physicians_found_count} physicians saved to {basename(batch_filename)}")
            cat("💾 BATCH SAVED:", physicians_found_count, "physicians to", basename(batch_filename), "\n")
          }
        }
        
      } else {
        # Empty response - invalid ID
        wrong_ids_new_discovered <- c(wrong_ids_new_discovered, current_id)
        if (verbose) logger::log_info("Empty response for ID: {current_id}")
      }
    } else {
      # Request failed - mark as wrong ID
      wrong_ids_new_discovered <- c(wrong_ids_new_discovered, current_id)
      if (verbose) logger::log_warn("Request failed for ID: {current_id}")
    }
    
    # Adaptive delay with jitter to avoid detection patterns
    jittered_delay <- delay_seconds + stats::runif(1, -0.3, 0.3)
    Sys.sleep(max(0.1, jittered_delay))
    
    # Enhanced progress reporting with success metrics
    if (verbose && request_count_total %% 50 == 0) {
      success_rate_percentage <- physicians_found_count / request_count_total * 100
      recent_graduates_count <- sum(physicians_data_collected$graduation_year >= 2020, na.rm = TRUE)
      logger::log_info("Progress: {request_count_total}/{max_requests} requests")
      logger::log_info("Found {physicians_found_count} physicians ({round(success_rate_percentage, 1)}% success)")
      logger::log_info("Recent graduates (2020+): {recent_graduates_count}")
    }
  }
  
  # Save final results with enhanced metadata
  saved_file_paths <- save_scraping_results_enhanced(physicians_data_collected, wrong_ids_new_discovered, 
                                                     start_id, end_id, strategy, wrong_id_filter_threshold,
                                                     output_directory, verbose)
  
  # Generate comprehensive summary statistics
  scraping_end_time <- Sys.time()
  total_duration_minutes <- as.numeric(difftime(scraping_end_time, scraping_start_time, units = "mins"))
  
  comprehensive_statistics <- generate_comprehensive_statistics(
    physicians_data_collected, wrong_ids_new_discovered, request_count_total,
    total_duration_minutes, strategy, start_id, end_id, wrong_id_filter_threshold
  )
  
  if (verbose) {
    logger::log_info("Scraping completed in {round(total_duration_minutes, 2)} minutes")
    logger::log_info("Found {physicians_found_count} physicians in {request_count_total} requests")
    logger::log_info("Success rate: {round(comprehensive_statistics$success_rate * 100, 1)}%")
    logger::log_info("Recent graduates: {comprehensive_statistics$recent_graduates_count}")
    logger::log_info("Batch saves performed: {floor(physicians_found_count / batch_save_size)}")
  }
  
  return(list(
    physicians = physicians_data_collected,
    wrong_ids = wrong_ids_new_discovered,
    statistics = comprehensive_statistics,
    files = saved_file_paths$file_paths
  ))
}

#' @noRd
generate_chunk_sampling_ids_corrected <- function(start_id, end_id, chunk_size, max_requests, verbose) {
  if (verbose) {
    logger::log_info("Using CORRECTED chunk sampling strategy with chunk size {chunk_size}")
    logger::log_info("This will sample IDs every {chunk_size} positions, not consecutive IDs")
  }
  
  # Generate IDs spaced by chunk_size
  sampled_ids <- seq(from = start_id, to = end_id, by = chunk_size)
  
  # Limit to max_requests
  if (length(sampled_ids) > max_requests) {
    sampled_ids <- sampled_ids[1:max_requests]
  }
  
  if (verbose) {
    logger::log_info("Generated {length(sampled_ids)} IDs with chunk spacing of {chunk_size}")
    logger::log_info("First few IDs: {paste(head(sampled_ids, 5), collapse=', ')}")
    logger::log_info("Last few IDs: {paste(tail(sampled_ids, 5), collapse=', ')}")
  }
  
  return(sampled_ids)
}

#' @noRd
load_filtered_wrong_ids <- function(output_directory, filter_threshold, verbose) {
  wrong_ids_master_file <- file.path(output_directory, "MASTER_all_wrongs.csv")
  
  if (file.exists(wrong_ids_master_file)) {
    tryCatch({
      master_wrongs_data <- readr::read_csv(wrong_ids_master_file, show_col_types = FALSE)
      all_wrong_ids <- master_wrongs_data$WrongIDs
      
      # Filter out wrong IDs below threshold (they may now be valid)
      filtered_wrong_ids <- all_wrong_ids[all_wrong_ids >= filter_threshold]
      
      if (verbose) {
        logger::log_info("Loaded {length(all_wrong_ids)} total wrong IDs")
        logger::log_info("Filtered to {length(filtered_wrong_ids)} wrong IDs above threshold {filter_threshold}")
        logger::log_info("Excluded {length(all_wrong_ids) - length(filtered_wrong_ids)} potentially outdated wrong IDs")
      }
      
      return(filtered_wrong_ids)
    }, error = function(e) {
      if (verbose) logger::log_warn("Could not load wrong IDs file: {e$message}")
      return(c())
    })
  } else {
    if (verbose) logger::log_info("No existing wrong IDs file found")
    return(c())
  }
}

#' @noRd
make_abog_api_request_enhanced <- function(physician_id, tor_port, verbose) {
  abog_base_url <- "https://api.abog.org/"
  api_endpoint <- "diplomate/"
  verification_action <- "/verify"
  full_request_url <- paste0(abog_base_url, api_endpoint, physician_id, verification_action)
  
  tryCatch({
    api_response <- httr::GET(full_request_url, httr::use_proxy(paste0("socks5://localhost:", tor_port)))
    
    if (api_response$status_code == 200) {
      response_content_text <- httr::content(api_response, as = "text", encoding = "UTF-8")
      parsed_physician_data <- jsonlite::fromJSON(response_content_text)
      
      return(list(success = TRUE, physician_data = parsed_physician_data))
    } else {
      if (verbose) logger::log_warn("HTTP {api_response$status_code} for ID {physician_id}")
      return(list(success = FALSE, physician_data = NULL))
    }
  }, error = function(error_details) {
    if (verbose) logger::log_error("Request error for ID {physician_id}: {error_details$message}")
    return(list(success = FALSE, physician_data = NULL))
  })
}

#' @noRd
enhance_physician_data <- function(raw_physician_data, physician_id) {
  enhanced_data <- raw_physician_data
  enhanced_data$physician_id <- physician_id
  enhanced_data$scraped_datetime <- Sys.time()
  
  # Extract graduation year if available
  if (!is.null(enhanced_data$residency_completion) || !is.null(enhanced_data$graduation_date)) {
    graduation_info <- enhanced_data$residency_completion %||% enhanced_data$graduation_date
    if (!is.null(graduation_info)) {
      # Try to extract year from various date formats
      year_match <- stringr::str_extract(as.character(graduation_info), "20\\d{2}")
      if (!is.na(year_match)) {
        enhanced_data$graduation_year <- as.numeric(year_match)
      }
    }
  }
  
  # Extract useful display information for logging
  physician_name <- enhanced_data$name %||% "Unknown"
  
  # Look for location info
  location_info <- ""
  if (!is.null(enhanced_data$city) && !is.null(enhanced_data$state)) {
    location_info <- paste0(enhanced_data$city, ", ", enhanced_data$state)
  } else if (!is.null(enhanced_data$state)) {
    location_info <- enhanced_data$state
  } else if (!is.null(enhanced_data$city)) {
    location_info <- enhanced_data$city
  }
  
  # Create display string with most useful info available
  enhanced_data$display_info <- if (location_info != "") {
    location_info
  } else {
    "Basic info"
  }
  
  # Identify if likely resident based on recent graduation
  current_year <- as.numeric(format(Sys.Date(), "%Y"))
  if (!is.null(enhanced_data$graduation_year)) {
    enhanced_data$is_recent_graduate <- enhanced_data$graduation_year >= (current_year - 5)
  }
  
  return(enhanced_data)
}

#' @noRd
save_scraping_results_enhanced <- function(physicians_data, wrong_ids_new, start_id, end_id, 
                                           strategy, filter_threshold, output_directory, verbose) {
  current_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  
  # Save physicians data with enhanced metadata
  physicians_filename <- file.path(output_directory, 
                                   paste0("physicians_FINAL_", strategy, "_", start_id, "-", end_id, 
                                          "_filtered_", filter_threshold, "_", current_timestamp, ".csv"))
  
  if (nrow(physicians_data) > 0) {
    readr::write_csv(physicians_data, physicians_filename)
    if (verbose) logger::log_info("Final physicians data saved to: {physicians_filename}")
  }
  
  # Save new wrong IDs with metadata
  wrong_ids_filename <- file.path(output_directory,
                                  paste0("wrong_ids_", strategy, "_", start_id, "-", end_id,
                                         "_filtered_", filter_threshold, "_", current_timestamp, ".csv"))
  
  if (length(wrong_ids_new) > 0) {
    wrong_ids_dataframe <- data.frame(
      WrongIDs = wrong_ids_new,
      discovered_date = Sys.Date(),
      strategy_used = strategy,
      filter_threshold = filter_threshold
    )
    readr::write_csv(wrong_ids_dataframe, wrong_ids_filename)
    if (verbose) logger::log_info("New wrong IDs saved to: {wrong_ids_filename}")
  }
  
  return(list(
    file_paths = list(
      physicians = if (nrow(physicians_data) > 0) physicians_filename else NULL,
      wrong_ids = if (length(wrong_ids_new) > 0) wrong_ids_filename else NULL
    )
  ))
}

#' @noRd
generate_comprehensive_statistics <- function(physicians_data, wrong_ids_new, request_count,
                                              duration_minutes, strategy, start_id, end_id, filter_threshold) {
  
  recent_graduates_count <- if (nrow(physicians_data) > 0) {
    sum(physicians_data$graduation_year >= 2020, na.rm = TRUE)
  } else {
    0
  }
  
  return(list(
    strategy = strategy,
    total_requests = request_count,
    physicians_found = nrow(physicians_data),
    recent_graduates_count = recent_graduates_count,
    new_wrong_ids = length(wrong_ids_new),
    success_rate = if (request_count > 0) nrow(physicians_data) / request_count else 0,
    duration_minutes = duration_minutes,
    requests_per_minute = request_count / max(duration_minutes, 0.1),
    id_range = c(start_id, end_id),
    wrong_id_filter_threshold = filter_threshold,
    analysis_timestamp = Sys.time()
  ))
}

#' @noRd
generate_adaptive_binary_ids_focused <- function(start_id, end_id, max_requests, verbose) {
  if (verbose) logger::log_info("Using adaptive binary search strategy (resident-focused)")
  
  collected_ids <- c()
  ranges_to_explore <- list(c(start_id, end_id))
  
  while (length(collected_ids) < max_requests && length(ranges_to_explore) > 0) {
    current_range <- ranges_to_explore[[1]]
    ranges_to_explore <- ranges_to_explore[-1]
    
    range_start <- current_range[1]
    range_end <- current_range[2]
    
    if (range_end - range_start < 20) {
      # Small range - sample all IDs
      new_ids <- seq(range_start, range_end)
    } else {
      # Large range - sample key points with resident focus
      mid_point <- floor((range_start + range_end) / 2)
      quarter_point <- floor((range_start + mid_point) / 2)
      three_quarter_point <- floor((mid_point + range_end) / 2)
      
      # Add extra sampling points for resident ranges
      eighth_point <- floor((range_start + quarter_point) / 2)
      five_eighth_point <- floor((quarter_point + mid_point) / 2)
      
      new_ids <- unique(c(range_start, eighth_point, quarter_point, five_eighth_point, 
                          mid_point, three_quarter_point, range_end))
      
      # Add sub-ranges for further exploration
      ranges_to_explore <- append(ranges_to_explore, list(
        c(range_start, quarter_point),
        c(quarter_point + 1, mid_point),
        c(mid_point + 1, three_quarter_point),
        c(three_quarter_point + 1, range_end)
      ))
    }
    
    collected_ids <- c(collected_ids, new_ids)
  }
  
  return(unique(collected_ids[1:min(length(collected_ids), max_requests)]))
}

#' @noRd
generate_resident_pattern_ids <- function(start_id, end_id, max_requests, verbose) {
  if (verbose) logger::log_info("Using resident-focused pattern strategy")
  
  # Known working IDs for testing
  known_working_ids <- c(3, 9033995, 9040372)
  
  # Based on known resident patterns - focus on 9.04M+ range
  resident_focused_patterns <- c(
    # Include known working IDs for testing
    known_working_ids,
    
    # Dense sampling around known resident clusters
    seq(9040000, 9043000, by = 50),    # Around Erin Franks and nearby
    seq(9041000, 9042999, by = 25),    # Gap fill range
    seq(9044000, 9048000, by = 100),   # 2025 graduate range
    
    # Systematic sampling in promising ranges
    seq(9040500, 9041500, by = 10),    # Very dense in promising area
    seq(9042000, 9043500, by = 15),    # Medium density
    
    # Exploration of higher ranges
    seq(9048000, 9055000, by = 200),   # Future resident exploration
    seq(9055000, 9060000, by = 500)    # Extended exploration
  )
  
  # Filter to specified range and sample if needed
  valid_pattern_ids <- resident_focused_patterns[resident_focused_patterns >= start_id & 
                                                   resident_focused_patterns <= end_id]
  
  if (length(valid_pattern_ids) > max_requests) {
    # Prioritize known working IDs first, then lower IDs (more likely to be residents)
    known_in_range <- known_working_ids[known_working_ids >= start_id & known_working_ids <= end_id]
    other_ids <- valid_pattern_ids[!valid_pattern_ids %in% known_working_ids]
    other_ids <- sort(other_ids)[1:(max_requests - length(known_in_range))]
    valid_pattern_ids <- c(known_in_range, other_ids)
  }
  
  return(sort(unique(valid_pattern_ids)))
}

#' @noRd
generate_hybrid_resident_search <- function(start_id, end_id, max_requests, verbose) {
  if (verbose) logger::log_info("Using hybrid resident search strategy")
  
  # Combine multiple approaches for resident finding
  requests_per_method <- floor(max_requests / 4)
  
  # Method 1: Dense sampling in gap range
  gap_range_ids <- seq(9041000, 9042999, by = 20)
  gap_range_ids <- gap_range_ids[gap_range_ids >= start_id & gap_range_ids <= end_id]
  gap_sample_ids <- head(gap_range_ids, requests_per_method)
  
  # Method 2: Systematic search for 2025 graduates
  graduate_2025_ids <- seq(9044000, 9050000, by = 75)
  graduate_2025_ids <- graduate_2025_ids[graduate_2025_ids >= start_id & graduate_2025_ids <= end_id]
  graduate_sample_ids <- head(graduate_2025_ids, requests_per_method)
  
  # Method 3: Random sampling in promising ranges
  promising_range_start <- max(start_id, 9040500)
  promising_range_end <- min(end_id, 9055000)
  if (promising_range_end > promising_range_start) {
    random_sample_ids <- sort(sample(promising_range_start:promising_range_end, 
                                     min(requests_per_method, promising_range_end - promising_range_start + 1)))
  } else {
    random_sample_ids <- c()
  }
  
  # Method 4: Targeted search around known patterns
  known_resident_ids <- c(9040372, 9040500, 9041000, 9041500, 9042000, 9042500)
  expansion_ids <- c()
  for (base_id in known_resident_ids) {
    if (base_id >= start_id && base_id <= end_id) {
      nearby_ids <- seq(base_id - 50, base_id + 50, by = 5)
      nearby_ids <- nearby_ids[nearby_ids >= start_id & nearby_ids <= end_id]
      expansion_ids <- c(expansion_ids, nearby_ids)
    }
  }
  expansion_sample_ids <- head(unique(expansion_ids), requests_per_method)
  
  # Combine all methods
  all_hybrid_ids <- unique(c(gap_sample_ids, graduate_sample_ids, random_sample_ids, expansion_sample_ids))
  
  # Ensure we don't exceed max_requests
  if (length(all_hybrid_ids) > max_requests) {
    all_hybrid_ids <- sort(all_hybrid_ids)[1:max_requests]
  }
  
  return(sort(all_hybrid_ids))
}
# run ----
gap_search_results <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9042699,
  end_id = 9042999,
  chunk_size = 100,        # Will check: 9042699, 9042799, 9042899
  batch_save_size = 10,    # Save every 10 physicians found
  max_requests = 1000,
  verbose = TRUE,
  output_directory = "physician_data/scrape_abog_physicians_fixed"
)

# 1. PRIORITY: Dense search in the proven resident cluster
dense_resident_search <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9040373,  # Right after Erin Franks
  end_id = 9041000,    # Focus on immediate area
  max_requests = 500,
  chunk_size = 25,     # Very dense sampling
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE
)

# 2. Search the gap with lower threshold to re-check old wrong IDs
gap_search_filtered <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling", 
  start_id = 9041000,
  end_id = 9042000,
  max_requests = 800,
  chunk_size = 50,
  wrong_id_filter_threshold = 9040000,  # Even lower threshold
  delay_seconds = 1.5,
  verbose = TRUE
)

# 1. CONTINUE in the productive range - extend further
extend_productive_range <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9041001,  # Continue where we left off
  end_id = 9042000,    # Extend the search
  max_requests = 800,
  chunk_size = 25,     # Keep dense sampling
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE
)

# 2. BACKFILL the gaps we found - search between clusters
backfill_gaps <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling", 
  start_id = 9040463,  # Fill gap after Anita Madison
  end_id = 9040848,    # Fill gap before Sonia Jackson
  max_requests = 600,
  chunk_size = 15,     # Very dense for gap filling
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE
)

# 1. SKIP AHEAD - Search much higher ranges for newer residents
future_residents_search <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9050000,  # Jump way ahead
  end_id = 9055000,    # Look for 2025+ residents  
  max_requests = 500,
  chunk_size = 100,
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE
)

# 2. SEARCH LOWER - Check if there are residents before the 9040000 range
lower_range_search <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling", 
  start_id = 9035000,  # Much lower range
  end_id = 9040000,    # Before our known cluster
  max_requests = 500,
  chunk_size = 100,
  wrong_id_filter_threshold = 9000000,  # Very low threshold
  delay_seconds = 1.5,
  verbose = TRUE
)

# 3. RANDOM SAMPLING - Test scattered IDs in unexplored ranges
random_exploration <- scrape_abog_physicians_fixed(
  strategy = "hybrid",
  start_id = 9060000,  # Even higher
  end_id = 9100000,    # Cast a wide net
  max_requests = 300,
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 2,
  verbose = TRUE
)  

# 1. EXTEND LOWER - Continue the success backwards
extend_lower_success <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9030000,  # Go even lower
  end_id = 9035000,    # Before our successful range
  max_requests = 500,
  chunk_size = 100,
  wrong_id_filter_threshold = 9000000,  # Same successful threshold
  delay_seconds = 1.5,
  verbose = TRUE,
  output_directory = "physician_data/discovery_results"
)

# Search for newest residents in high ranges
very_high_range <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling", 
  start_id = 9055000,  # Much higher than before
  end_id = 9070000,    # Cast wide net for 2025+ graduates
  max_requests = 800,
  chunk_size = 200,
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE,
  output_directory = "physician_data/discovery_results"
)

# 2. SEARCH EVEN HIGHER RANGES - Look for newest residents
# NOTHING FOUND HERE
very_high_range <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling", 
  start_id = 9055000,  # Much higher than before
  end_id = 9070000,    # Cast wide net for 2025+ graduates
  max_requests = 800,
  chunk_size = 200,
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE,
  output_directory = "physician_data/discovery_results"
)


# STOP the current search (Ctrl+C) and search the critical middle range
mid_range_search <- scrape_abog_physicians_fixed(
  strategy = "chunk_sampling",
  start_id = 9042000,  # Right after your successful range
  end_id = 9055000,    # Before the empty range
  max_requests = 800,
  chunk_size = 150,
  wrong_id_filter_threshold = 9040372,
  delay_seconds = 1.5,
  verbose = TRUE,
  output_directory = "physician_data/discovery_results"
)

# Complete ABOG Resident Map Discovered:
# ✅ 9030000-9032000: 160+ physicians (94.7% success)
# ✅ 9035000-9041000: 220+ physicians (98%+ success)
# ✅ 9042000-9045000: 83 physicians (30-84% success)
# ❌ 9045000-9055000: Sparse (10-15% success)
# ❌ 9055000+: Empty (0% success)  

# Scrape ABOG by Sequential IDs for when you find the gold mine! ----
# Complete Sequential Physician Search with All Helper Functions
#'
#' Systematically searches for physicians using sequential ID ranges with
#' automatic intermediate saves to prevent data loss during long searches.
#' All helper functions are integrated within the main function.
#'
#' @param start_physician_id Numeric. Starting physician ID for sequential search
#' @param target_physician_count Numeric. Target number of physicians to discover
#' @param tor_proxy_port Numeric. Tor proxy port (default: 9150)
#' @param request_delay_seconds Numeric. Delay between requests (default: 1.5)
#' @param batch_save_size Numeric. Save results every N discoveries (default: 25)
#' @param verbose_logging Logical. Enable detailed logging (default: TRUE)
#' @param output_directory Character. Output directory (default: "physician_data/discovery_results")
#' @param progress_interval Numeric. Log progress every N requests (default: 50)
#'
#' @return List with discovered physicians and search statistics
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr bind_rows
#' @importFrom readr write_csv
#' @importFrom httr GET use_proxy content
#' @importFrom jsonlite fromJSON
#' @importFrom stats runif
#'
#' @examples
#' # Search starting from ID 9000000 for 10 physicians
#' results <- search_physicians_by_sequential_ids_complete(
#'   start_physician_id = 9000000,
#'   target_physician_count = 10,
#'   batch_save_size = 50
#' )
#'
#' # Conservative search with longer delays
#' results <- search_physicians_by_sequential_ids_complete(
#'   start_physician_id = 9042700,
#'   target_physician_count = 100,
#'   request_delay_seconds = 2.5,
#'   batch_save_size = 25,
#'   verbose_logging = TRUE
#' )
#'
#' # Quick test with known productive range
#' results <- search_physicians_by_sequential_ids_complete(
#'   start_physician_id = 9042699,
#'   target_physician_count = 5,
#'   request_delay_seconds = 1.0,
#'   batch_save_size = 10,
#'   output_directory = "test_results"
#' )
#'
#' @export
search_physicians_by_sequential_ids_complete <- function(start_physician_id,
                                                         target_physician_count = 1000,
                                                         tor_proxy_port = 9150,
                                                         request_delay_seconds = 1.5,
                                                         batch_save_size = 25,
                                                         verbose_logging = TRUE,
                                                         output_directory = "physician_data/discovery_results",
                                                         progress_interval = 50) {
  
  # Input validation
  assertthat::assert_that(is.numeric(start_physician_id) && start_physician_id > 0)
  assertthat::assert_that(is.numeric(target_physician_count) && target_physician_count > 0)
  assertthat::assert_that(is.numeric(batch_save_size) && batch_save_size > 0)
  assertthat::assert_that(is.numeric(tor_proxy_port) && tor_proxy_port > 0)
  assertthat::assert_that(is.numeric(request_delay_seconds) && request_delay_seconds > 0)
  assertthat::assert_that(is.logical(verbose_logging))
  
  # Create output directory
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE, showWarnings = FALSE)
  }
  
  # Helper function: Extract safe value from API response
  extract_safe_value <- function(value, default_value = "") {
    if (is.null(value) || is.na(value) || length(value) == 0) {
      return(default_value)
    }
    return(as.character(value))
  }
  
  # Helper function: Construct location string
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
  
  # Helper function: Make ABOG API request
  make_abog_api_request_enhanced <- function(physician_id, tor_port, verbose) {
    abog_base_url <- "https://api.abog.org/"
    api_endpoint <- "diplomate/"
    verification_action <- "/verify"
    full_request_url <- paste0(abog_base_url, api_endpoint, physician_id, verification_action)
    
    tryCatch({
      api_response <- httr::GET(full_request_url, httr::use_proxy(paste0("socks5://localhost:", tor_port)))
      
      if (api_response$status_code == 200) {
        response_content_text <- httr::content(api_response, as = "text", encoding = "UTF-8")
        
        # Check for empty or null responses
        if (nchar(response_content_text) == 0 || 
            response_content_text == "null" || 
            response_content_text == "{}") {
          return(list(success = FALSE, physician_data = NULL))
        }
        
        parsed_physician_data <- jsonlite::fromJSON(response_content_text)
        
        # Validate parsed data
        if (is.list(parsed_physician_data) && length(parsed_physician_data) > 0) {
          return(list(success = TRUE, physician_data = parsed_physician_data))
        } else {
          return(list(success = FALSE, physician_data = NULL))
        }
        
      } else {
        if (verbose) logger::log_warn("HTTP {api_response$status_code} for ID {physician_id}")
        return(list(success = FALSE, physician_data = NULL))
      }
    }, error = function(error_details) {
      if (verbose) logger::log_error("Request error for ID {physician_id}: {error_details$message}")
      return(list(success = FALSE, physician_data = NULL))
    })
  }
  
  # Helper function: Enrich physician data
  enrich_sequential_physician_data <- function(physician_data, physician_id) {
    
    # Extract safe values
    physician_name <- extract_safe_value(physician_data$name, "Unknown Name")
    physician_city <- extract_safe_value(physician_data$city, "")
    physician_state <- extract_safe_value(physician_data$state, "")
    physician_location <- construct_location_string(physician_city, physician_state)
    
    # Create enriched data frame
    enriched_data <- data.frame(
      physician_id = physician_id,
      physician_name = physician_name,
      physician_city = physician_city,
      physician_state = physician_state,
      physician_location = physician_location,
      discovery_timestamp = Sys.time(),
      discovery_method = "sequential_id_search",
      stringsAsFactors = FALSE
    )
    
    # Add any additional fields from API response
    additional_fields <- setdiff(names(physician_data), c("name", "city", "state"))
    for (field_name in additional_fields) {
      if (length(physician_data[[field_name]]) == 1) {
        enriched_data[[paste0("api_", field_name)]] <- extract_safe_value(physician_data[[field_name]], "")
      }
    }
    
    return(enriched_data)
  }
  
  # Initialize search parameters
  search_start_time <- Sys.time()
  discovered_physicians_data <- data.frame()
  current_physician_id <- start_physician_id
  total_requests_made <- 0
  discovered_physicians_count <- 0
  consecutive_empty_responses <- 0
  last_batch_save_count <- 0
  
  if (verbose_logging) {
    # Format ID without scientific notation
    formatted_start_id <- format(start_physician_id, scientific = FALSE)
    
    logger::log_info("=== SEQUENTIAL PHYSICIAN ID SEARCH INITIATED ===")
    logger::log_info("Starting ID: {formatted_start_id}, Target: {target_physician_count} physicians")
    logger::log_info("Batch save every {batch_save_size} discoveries")
    
    cat("🔍 SEQUENTIAL PHYSICIAN DISCOVERY\n")
    cat("📊 Search Parameters:\n")
    cat("   - Starting ID:", formatted_start_id, "\n")
    cat("   - Target discoveries:", target_physician_count, "\n")
    cat("   - Batch save size:", batch_save_size, "\n")
    cat("   - Request delay:", request_delay_seconds, "seconds\n")
    cat("   - Tor proxy port:", tor_proxy_port, "\n")
    cat("\n🚀 Beginning systematic ID search...\n")
  }
  
  # Main search loop
  while (discovered_physicians_count < target_physician_count && consecutive_empty_responses < 1000) {
    
    # Progress logging
    if (verbose_logging && total_requests_made > 0 && total_requests_made %% progress_interval == 0) {
      elapsed_minutes <- as.numeric(difftime(Sys.time(), search_start_time, units = "mins"))
      success_rate <- (discovered_physicians_count / total_requests_made) * 100
      current_rate <- discovered_physicians_count / max(elapsed_minutes, 0.1)
      formatted_current_id <- format(current_physician_id, scientific = FALSE)
      
      cat("📈 Search Progress:", total_requests_made, "requests |", 
          discovered_physicians_count, "physicians found |",
          "Success:", round(success_rate, 1), "% |",
          "Rate:", round(current_rate, 1), "physicians/min |",
          "Current ID:", formatted_current_id, "\n")
    }
    
    # Log current request
    if (verbose_logging && total_requests_made %% 10 == 0) {
      formatted_current_id <- format(current_physician_id, scientific = FALSE)
      logger::log_info("Making API request for physician ID: {formatted_current_id}")
    }
    
    # Make API request
    api_response <- make_abog_api_request_enhanced(current_physician_id, tor_proxy_port, verbose_logging)
    total_requests_made <- total_requests_made + 1
    
    if (api_response$success && length(api_response$physician_data) > 0) {
      consecutive_empty_responses <- 0
      discovered_physicians_count <- discovered_physicians_count + 1
      
      formatted_current_id <- format(current_physician_id, scientific = FALSE)
      logger::log_info("Successful data retrieval for physician ID: {formatted_current_id}")
      
      # Process and enrich physician data
      enriched_physician_data <- enrich_sequential_physician_data(
        api_response$physician_data, 
        current_physician_id
      )
      
      # Add to discovered physicians
      discovered_physicians_data <- dplyr::bind_rows(discovered_physicians_data, enriched_physician_data)
      
      # Log discovery
      physician_name <- enriched_physician_data$physician_name
      physician_location <- enriched_physician_data$physician_location
      
      if (verbose_logging) {
        cat("🎉 NEW PHYSICIAN DISCOVERED! ID:", format(current_physician_id, scientific = FALSE), 
            "| Name:", physician_name, "| Location:", physician_location, "\n")
        logger::log_info("Successfully processed physician discovery: {physician_name}")
      }
      
      # Save intermediate batch if needed
      if (discovered_physicians_count > last_batch_save_count && 
          (discovered_physicians_count - last_batch_save_count) >= batch_save_size) {
        
        batch_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
        batch_filename <- file.path(output_directory, 
                                    paste0("sequential_discovery_batch_", 
                                           format(start_physician_id, scientific = FALSE), "_",
                                           discovered_physicians_count, "_", 
                                           batch_timestamp, ".csv"))
        
        readr::write_csv(discovered_physicians_data, batch_filename)
        last_batch_save_count <- discovered_physicians_count
        
        if (verbose_logging) {
          cat("💾 BATCH SAVED:", discovered_physicians_count, "physicians saved to", 
              basename(batch_filename), "\n")
          logger::log_info("Intermediate batch saved: {batch_filename}")
        }
      }
      
    } else {
      consecutive_empty_responses <- consecutive_empty_responses + 1
      if (verbose_logging && consecutive_empty_responses %% 100 == 0) {
        formatted_current_id <- format(current_physician_id, scientific = FALSE)
        logger::log_info("Empty response streak: {consecutive_empty_responses} for ID: {formatted_current_id}")
      }
    }
    
    # Move to next ID
    current_physician_id <- current_physician_id + 1
    
    # Apply delay with jitter
    jittered_delay <- request_delay_seconds + stats::runif(1, -0.2, 0.2)
    Sys.sleep(max(0.3, jittered_delay))
  }
  
  # Save final results
  search_end_time <- Sys.time()
  total_duration <- as.numeric(difftime(search_end_time, search_start_time, units = "mins"))
  
  # Final save
  if (nrow(discovered_physicians_data) > 0) {
    final_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
    final_filename <- file.path(output_directory,
                                paste0("sequential_discovery_final_",
                                       format(start_physician_id, scientific = FALSE), "_",
                                       discovered_physicians_count, "_",
                                       final_timestamp, ".csv"))
    
    readr::write_csv(discovered_physicians_data, final_filename)
    
    if (verbose_logging) {
      cat("\n💾 FINAL RESULTS SAVED:", nrow(discovered_physicians_data), 
          "physicians to", basename(final_filename), "\n")
    }
  }
  
  # Generate final summary
  final_success_rate <- if (total_requests_made > 0) {
    (discovered_physicians_count / total_requests_made) * 100
  } else {
    0
  }
  
  if (verbose_logging) {
    cat("\n=== SEQUENTIAL DISCOVERY COMPLETED ===\n")
    cat("⏱️  Total duration:", round(total_duration, 1), "minutes\n")
    cat("📊 Total requests:", total_requests_made, "\n")
    cat("🎯 Physicians discovered:", discovered_physicians_count, "\n")
    cat("📈 Success rate:", round(final_success_rate, 1), "%\n")
    cat("🔢 ID range searched:", format(start_physician_id, scientific = FALSE), 
        "to", format(current_physician_id - 1, scientific = FALSE), "\n")
    cat("📁 Results saved to:", output_directory, "\n")
    
    # Show some discovered physicians if any
    if (nrow(discovered_physicians_data) > 0) {
      cat("\n👥 Sample of discovered physicians:\n")
      sample_size <- min(5, nrow(discovered_physicians_data))
      for (i in 1:sample_size) {
        physician <- discovered_physicians_data[i, ]
        cat("   ", format(physician$physician_id, scientific = FALSE), 
            "-", physician$physician_name, 
            "(", physician$physician_location, ")\n")
      }
      if (nrow(discovered_physicians_data) > sample_size) {
        cat("   ... and", nrow(discovered_physicians_data) - sample_size, "more\n")
      }
    }
  }
  
  logger::log_info("=== SEQUENTIAL PHYSICIAN DISCOVERY COMPLETED ===")
  
  return(list(
    discovered_physicians = discovered_physicians_data,
    total_requests = total_requests_made,
    physicians_found = discovered_physicians_count,
    success_rate = final_success_rate,
    duration_minutes = total_duration,
    id_range = c(start_physician_id, current_physician_id - 1),
    final_output_file = if (exists("final_filename")) final_filename else NULL,
    consecutive_empty_responses = consecutive_empty_responses
  ))
}

  # TEST FIRST: Verify function works with known IDs
  # test_results <- test_known_physician_ids()
  # logger::log_info("Test found {nrow(test_results$physicians)} physicians")
  # 

# run ----
# Search starting from ID 9000000 for 10 physicians
results <- search_physicians_by_sequential_ids(
  start_physician_id = 9000000,
  target_physician_count = 10,
  batch_save_size = 50
)


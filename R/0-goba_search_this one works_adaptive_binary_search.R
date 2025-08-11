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

# Setup ----
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
search_physicians_by_sequential_ids <- function(start_physician_id,
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
  # Fixed helper function
  extract_safe_value <- function(value, default_value = "") {
    if (is.null(value) || length(value) == 0) {
      return(default_value)
    }
    # Handle vectors by taking the first element
    if (length(value) > 1) {
      value <- value[1]
    }
    if (is.na(value)) {
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
  start_physician_id = 9014000,
  target_physician_count = 1000,
  batch_save_size = 50
)

# Target the sweet spot for recent graduates
recent_grad_results <- search_physicians_by_sequential_ids(
  start_physician_id = 9042000,    # Start right at the recent grad zone
  target_physician_count = 9000,    # Good sample size
  batch_save_size = 50,
  request_delay_seconds = 0.5,
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)


#Analysis: Range 9050000+ is a Dead Zone
# The exploratory search confirms that 9050000-9050999 is completely empty - 0% success rate across 1,000 requests. This provides valuable intelligence about the ID allocation system.
# Jump to a higher range that might have newer registrations
explore_higher_range <- search_physicians_by_sequential_ids(
  start_physician_id = 9050000,    # Skip potential gap
  target_physician_count = 500,    # Smaller exploratory search
  batch_save_size = 25,
  request_delay_seconds = 1.0,
  verbose_logging = TRUE
)


# RECOMMENDED: Continue the systematic search
continuation_search <- search_physicians_by_sequential_ids(
  start_physician_id = 9048947,    # Exactly where you stopped
  target_physician_count = 1000,   # Conservative target
  batch_save_size = 50,
  request_delay_seconds = 0.8,     # Slightly more conservative
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)

# Search Dense Clusters Around Your Productive Range
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  "1437904760",   # Lizeth Acosta, MD
  "1568738854",   # Aaron Lazorwitz, MD
  "1194571661",   # Ana Gomez, MD
  "1699237040",   # Erin W. Franks, MD
  "1003311044",   # CATHERINE Callinan, MD
  "1609009943",   # Kristin Powell, MD
  "1114125051",   # Nathan Kow, MD
  "1043432123",   # Elena Tunitsky, MD
  "1215490446",   # PK
  "1487879987"    # Peter Jeppson
)
# List of physician names (optional, for better logging and output) ----
names_list <- c(
  "Tyler Muffly, MD",
  "John Curtin, MD",
  "Pedro Miranda, MD",
  "Lizeth Acosta, MD",
  "Aaron Lazorwitz, MD",
  "Ana Gomez, MD",
  "Erin W. Franks, MD", 
  "Catherine Callinan, MD",
  "Kristin Powell, MD",
  "Nathan Kow, MD", 
  "Elena Tunitsky, MD",
  "Parisa Khalighi, MD",
  "Peter Jeppson, MD"
)

# Search just before your main range (might be earlier graduates)
earlier_range <- search_physicians_by_sequential_ids(
  start_physician_id = 9040000,    # Search before your main range
  target_physician_count = 5000,
  batch_save_size = 100,
  request_delay_seconds = 0.8,
  verbose_logging = TRUE
)

# Search just after your productive range (before the dead zone)
continuation_range <- search_physicians_by_sequential_ids(
  start_physician_id = 9048947,    # Continue where you left off
  target_physician_count = 800,    # Go until you hit the dead zone
  batch_save_size = 50,
  request_delay_seconds = 0.8,
  verbose_logging = TRUE
)


# RECOMMENDED: Search the pre-2020 physician range
earlier_physicians_search <- search_physicians_by_sequential_ids(
  start_physician_id = 9020000,    # Much earlier range
  target_physician_count = 20000,   # Good sample size
  batch_save_size = 100,
  request_delay_seconds = 0.7,     # Faster since likely productive
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)

# RECOMMENDED: Search the pre-2020 physician range
earlier_physicians_search <- search_physicians_by_sequential_ids(
  start_physician_id = 9000763,    # Much earlier range
  target_physician_count = 80000,   # Good sample size
  batch_save_size = 100,
  request_delay_seconds = 0.7,     # Faster since likely productive
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)

# RECOMMENDED: Search the pre-2020 physician range
earlier_physicians_search <- search_physicians_by_sequential_ids(
  start_physician_id = 8920000,    # Much earlier range
  target_physician_count = 80000,   # Good sample size
  batch_save_size = 100,
  request_delay_seconds = 0.7,     # Faster since likely productive
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)


# RECOMMENDED: Search the pre-2020 physician range
earlier_physicians_search <- search_physicians_by_sequential_ids(
  start_physician_id = 9020973,    # Much earlier range
  target_physician_count = 80000,   # Good sample size
  batch_save_size = 100,
  request_delay_seconds = 5,     # Faster since likely productive
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)


# Search Dense Clusters Around Your Productive Range
abog_id_list <- c(
  "9014566",   # Tyler Muffly, MD
  "849120",   # John Curtin, MD
  "930075",   # Pedro Miranda, MD
  "9029730",   # Aaron Lazorwitz, MD
  "9040372",   # Erin W. Franks, MD
  "9038445",   # CATHERINE Callinan, MD
  "9026632",   # Kristin Powell, MD
  "9024942",   # Nathan Kow, MD
  "9020382",   # Elena Tunitsky, MD
  "9040368",   # PK
  "9021650"    # Peter Jeppson
)
# List of physician names (optional, for better logging and output) ----
names_list <- c(
  "Tyler Muffly, MD",
  "John Curtin, MD",
  "Pedro Miranda, MD",
  "Aaron Lazorwitz, MD",
  "Erin W. Franks, MD", 
  "Catherine Callinan, MD",
  "Kristin Powell, MD",
  "Nathan Kow, MD", 
  "Elena Tunitsky, MD",
  "Parisa Khalighi, MD",
  "Peter Jeppson, MD"
)

Lund_curtin <- search_physicians_by_sequential_ids(
  start_physician_id = 800000,
  target_physician_count = 1000,
  batch_save_size = 100,
  request_delay_seconds = 3,
  # Faster since likely productive
  verbose_logging = TRUE,
  output_directory = "physician_data/discovery_results"
)

# Funciton at 2011 ----
# COMPLETE Enhanced Helper Functions with All Dependencies
# Enhanced extract_safe_value function
extract_safe_value <- function(value, default_value = "") {
  if (is.null(value) || length(value) == 0) {
    return(default_value)
  }
  # Handle vectors by taking the first element
  if (length(value) > 1) {
    value <- value[1]
  }
  if (is.na(value) || value == "null" || value == "") {
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

# Enhanced Helper function: Enrich physician data with ALL API fields
enrich_sequential_physician_data_enhanced <- function(physician_data, physician_id) {
  
  # Enhanced extract_safe_value function
  extract_safe_value <- function(value, default_value = "") {
    if (is.null(value) || length(value) == 0) {
      return(default_value)
    }
    # Handle vectors by taking the first element
    if (length(value) > 1) {
      value <- value[1]
    }
    if (is.na(value) || value == "null" || value == "") {
      return(default_value)
    }
    return(as.character(value))
  }
  
  # Extract core physician information
  physician_name <- extract_safe_value(physician_data$name, "Unknown Name")
  physician_city <- extract_safe_value(physician_data$city, "")
  physician_state <- extract_safe_value(physician_data$state, "")
  physician_location <- construct_location_string(physician_city, physician_state)
  
  # Create base enriched data frame
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
  
  # =================================================================
  # COMPREHENSIVE API FIELD EXTRACTION
  # =================================================================
  
  # Core certification fields
  api_fields_to_extract <- c(
    "userid", "startDate", "certStatus", "mocStatus", "clinicallyActive",
    "certificationDate", "originalCertDate", "expirationDate",
    "participatingMOC", "mocCompliant", "status"
  )
  
  # Extract all standard API fields
  for (field_name in api_fields_to_extract) {
    if (!is.null(physician_data[[field_name]])) {
      enriched_data[[paste0("api_", field_name)]] <- extract_safe_value(physician_data[[field_name]], "")
    }
  }
  
  # =================================================================
  # SUBSPECIALTY EXTRACTION (CRITICAL FOR GEOGRAPHIC ANALYSIS)
  # =================================================================
  
  # Method 1: Check for "sub1" field (historical)
  if (!is.null(physician_data$sub1)) {
    enriched_data$api_subspecialty_1 <- extract_safe_value(physician_data$sub1, "")
  }
  
  # Method 2: Check for "subspecialty" field
  if (!is.null(physician_data$subspecialty)) {
    enriched_data$api_subspecialty <- extract_safe_value(physician_data$subspecialty, "")
  }
  
  # Method 3: Check for "subspecialties" array
  if (!is.null(physician_data$subspecialties)) {
    if (is.list(physician_data$subspecialties) || is.vector(physician_data$subspecialties)) {
      subspecialties_list <- as.character(physician_data$subspecialties)
      enriched_data$api_subspecialties_list <- paste(subspecialties_list, collapse = "; ")
      # Also extract first subspecialty for easy analysis
      enriched_data$api_primary_subspecialty <- extract_safe_value(subspecialties_list[1], "")
    }
  }
  
  # Method 4: Check for nested certification objects with subspecialties
  if (!is.null(physician_data$certifications)) {
    cert_data <- physician_data$certifications
    if (is.list(cert_data)) {
      subspecialty_certs <- c()
      for (i in seq_along(cert_data)) {
        cert <- cert_data[[i]]
        if (!is.null(cert$subspecialty)) {
          subspecialty_certs <- c(subspecialty_certs, as.character(cert$subspecialty))
        }
        if (!is.null(cert$specialty) && cert$specialty != "Obstetrics and Gynecology") {
          subspecialty_certs <- c(subspecialty_certs, as.character(cert$specialty))
        }
      }
      if (length(subspecialty_certs) > 0) {
        enriched_data$api_certification_subspecialties <- paste(unique(subspecialty_certs), collapse = "; ")
      }
    }
  }
  
  # =================================================================
  # EXTRACT ALL REMAINING FIELDS (COMPREHENSIVE CAPTURE)
  # =================================================================
  
  # Get all field names we haven't processed yet
  processed_fields <- c("name", "city", "state", api_fields_to_extract, 
                        "sub1", "subspecialty", "subspecialties", "certifications")
  
  remaining_fields <- setdiff(names(physician_data), processed_fields)
  
  # Extract all remaining fields
  for (field_name in remaining_fields) {
    field_value <- physician_data[[field_name]]
    
    # Handle different data types
    if (is.list(field_value) || is.vector(field_value)) {
      if (length(field_value) == 1) {
        enriched_data[[paste0("api_", field_name)]] <- extract_safe_value(field_value, "")
      } else if (length(field_value) > 1) {
        # Convert complex fields to JSON-like strings for analysis
        enriched_data[[paste0("api_", field_name)]] <- paste(as.character(field_value), collapse = "; ")
      }
    } else {
      enriched_data[[paste0("api_", field_name)]] <- extract_safe_value(field_value, "")
    }
  }
  
  # =================================================================
  # DERIVED FIELDS FOR ANALYSIS
  # =================================================================
  
  # Create a comprehensive subspecialty field for analysis
  subspecialty_sources <- c(
    enriched_data$api_subspecialty_1,
    enriched_data$api_subspecialty,
    enriched_data$api_primary_subspecialty,
    enriched_data$api_certification_subspecialties
  )
  
  # Take the first non-empty subspecialty
  final_subspecialty <- ""
  for (sub in subspecialty_sources) {
    if (!is.null(sub) && !is.na(sub) && sub != "" && sub != "NA") {
      final_subspecialty <- sub
      break
    }
  }
  
  enriched_data$subspecialty_final <- final_subspecialty
  
  # Flag if physician has subspecialty training (important for geographic analysis)
  enriched_data$has_subspecialty <- final_subspecialty != ""
  
  # Extract certification year for cohort analysis
  if (!is.null(enriched_data$api_originalCertDate) && enriched_data$api_originalCertDate != "") {
    cert_date <- enriched_data$api_originalCertDate
    if (grepl("\\d{4}", cert_date)) {
      cert_year <- regmatches(cert_date, regexpr("\\d{4}", cert_date))
      enriched_data$certification_year <- as.numeric(cert_year)
    }
  }
  
  return(enriched_data)
}

# Test the enhanced function on Elena Tunitsky-Bitton's ID
test_elena_id <- 9020382

# Make direct API call to test data extraction
abog_base_url <- "https://api.abog.org/"
api_endpoint <- "diplomate/"
verification_action <- "/verify"
full_request_url <- paste0(abog_base_url, api_endpoint, test_elena_id, verification_action)

# Get her data
api_response <- httr::GET(full_request_url, httr::use_proxy("socks5://localhost:9150"))
response_content <- httr::content(api_response, as = "text", encoding = "UTF-8")
elena_data <- jsonlite::fromJSON(response_content)

# Test the enhanced enrichment function
#source("path/to/your/enhanced_function.R")  # Load the enhanced function first
elena_enriched <- enrich_sequential_physician_data_enhanced(elena_data, test_elena_id)

# View the results
View(elena_enriched)
print(elena_enriched$subspecialty_final)
print(elena_enriched$has_subspecialty)

# Function at 2026 ----
# Load your existing physician dataset
existing_physician_data <- readr::read_csv("physician_data/discovery_results/physician_data.csv")

# Process in batches to avoid overwhelming the servers
batch_size <- 100
total_physicians <- nrow(existing_physician_data)
all_enhanced_data <- data.frame()

for (batch_start in seq(1, total_physicians, batch_size)) {
  batch_end <- min(batch_start + batch_size - 1, total_physicians)
  
  cat("Processing batch", batch_start, "to", batch_end, "\n")
  
  batch_data <- existing_physician_data[batch_start:batch_end, ]
  
  # Extract enhanced data for this batch
  enhanced_batch <- extract_enhanced_physician_data(
    physician_location_data = batch_data,
    use_proxy_for_subspecialty = TRUE,
    subspecialty_extraction_method = "comprehensive",
    request_delay_seconds = 3.0,  # Longer delay for batch processing
    output_directory_path = paste0("enhanced_batch_", batch_start),
    verbose_logging = TRUE
  )
  
  # Combine with previous batches
  all_enhanced_data <- dplyr::bind_rows(all_enhanced_data, enhanced_batch)
  
  # Save progress
  readr::write_csv(all_enhanced_data, 
                   paste0("enhanced_physicians_progress_", batch_end, ".csv"))
  
  cat("Completed batch", batch_start, "to", batch_end, 
      "- Total enhanced:", nrow(all_enhanced_data), "\n\n")
  
  # Rest between batches
  Sys.sleep(10)
}

# Final save
readr::write_csv(all_enhanced_data, 
                 paste0("enhanced_physicians_complete_", 
                        format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))

# Function on Aug 2, 2025 at 1631 ----
#' Reverse Engineer ABOG API Authentication and Headers
#'
#' This function systematically tests different authentication methods,
#' headers, and request formats to crack the ABOG subspecialty API.
#'
#' @param physician_id numeric. Physician ID to test with. Default: 9020382 (Elena)
#' @param verbose_debug logical. Enable extensive debugging output. Default: TRUE
#' @param use_proxy_reverse logical. Whether to use SOCKS5 proxy. Default: TRUE
#' @param proxy_url_reverse character. SOCKS5 proxy URL. Default: "socks5://localhost:9150"
#' @param request_timeout_seconds numeric. Request timeout in seconds. Default: 10
#'
#' @return list with successful API responses and authentication details
#'
#' @examples
#' # Test Elena's physician ID with full debugging
#' api_results <- reverse_engineer_abog_api(
#'   physician_id = 9020382,
#'   verbose_debug = TRUE,
#'   use_proxy_reverse = TRUE
#' )
#' 
#' # Test multiple physicians quickly
#' test_ids <- c(9020382, 3, 4, 5)
#' for (id in test_ids) {
#'   cat("Testing physician ID:", id, "\n")
#'   results <- reverse_engineer_abog_api(id, verbose_debug = FALSE)
#'   if (length(results) > 0) {
#'     cat("SUCCESS found for ID:", id, "\n")
#'     break
#'   }
#' }
#'
#' @importFrom httr GET POST PUT use_proxy add_headers set_cookies cookies content timeout
#' @importFrom logger log_info log_warn log_error
#'
#' @export
reverse_engineer_abog_api <- function(
    physician_id = 9020382, 
    verbose_debug = TRUE,
    use_proxy_reverse = TRUE,
    proxy_url_reverse = "socks5://localhost:9150",
    request_timeout_seconds = 10) {
  
  if (verbose_debug) {
    cat("🔍 REVERSE ENGINEERING ABOG API FOR PHYSICIAN:", physician_id, "\n")
    cat("Testing different authentication and header combinations...\n")
    cat("Proxy enabled:", use_proxy_reverse, "\n")
    cat("Timeout:", request_timeout_seconds, "seconds\n\n")
  }
  
  # Set up proxy configuration
  proxy_config <- if (use_proxy_reverse) httr::use_proxy(proxy_url_reverse) else NULL
  
  # Base URLs that returned 200 status with 400 errors (meaning endpoints exist)
  working_base_urls <- c(
    "https://www.abog.org/api/",
    "https://abog.org/api/"
  )
  
  subspecialty_endpoints <- c(
    "diplomate/{id}/subspecialty",
    "diplomate/{id}/details",
    "diplomate/{id}/certification-details",
    "diplomate/{id}/full-profile"
  )
  
  successful_responses <- list()
  
  # === METHOD 1: Test Different HTTP Methods ===
  if (verbose_debug) cat("=== TESTING HTTP METHODS ===\n")
  
  for (base_url in working_base_urls[1]) {  # Test first URL
    for (endpoint in subspecialty_endpoints[1]) {  # Test first endpoint
      test_url <- paste0(base_url, gsub("\\{id\\}", physician_id, endpoint))
      
      # Test GET with different headers
      response_get <- test_api_method("GET", test_url, proxy_config, request_timeout_seconds, verbose_debug)
      if (!is.null(response_get)) successful_responses[["GET"]] <- response_get
      
      # Test POST
      response_post <- test_api_method("POST", test_url, proxy_config, request_timeout_seconds, verbose_debug)
      if (!is.null(response_post)) successful_responses[["POST"]] <- response_post
      
      # Test PUT  
      response_put <- test_api_method("PUT", test_url, proxy_config, request_timeout_seconds, verbose_debug)
      if (!is.null(response_put)) successful_responses[["PUT"]] <- response_put
      
      break  # Only test first endpoint for methods
    }
    break  # Only test first URL for methods
  }
  
  # === METHOD 2: Test Authentication Headers ===
  if (verbose_debug) cat("\n=== TESTING AUTHENTICATION HEADERS ===\n")
  
  auth_headers_to_test <- list(
    # API Key patterns
    list("X-API-Key" = "abog-api-key", "Content-Type" = "application/json"),
    list("Authorization" = "Bearer abog-token", "Content-Type" = "application/json"),
    list("Authorization" = "API-Key abog-key", "Content-Type" = "application/json"),
    
    # ABOG-specific headers  
    list("X-ABOG-Auth" = "physician-lookup", "Content-Type" = "application/json"),
    list("X-ABOG-Token" = "verification-token", "Content-Type" = "application/json"),
    list("X-Requested-With" = "XMLHttpRequest", "Content-Type" = "application/json"),
    
    # Browser simulation headers
    list(
      "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
      "Accept" = "application/json, text/plain, */*",
      "Accept-Language" = "en-US,en;q=0.9",
      "Accept-Encoding" = "gzip, deflate, br",
      "Content-Type" = "application/json",
      "X-Requested-With" = "XMLHttpRequest",
      "Referer" = "https://www.abog.org/verify-physician",
      "Origin" = "https://www.abog.org"
    ),
    
    # Form submission headers
    list(
      "Content-Type" = "application/x-www-form-urlencoded",
      "X-Requested-With" = "XMLHttpRequest",
      "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ),
    
    # JSON API headers
    list(
      "Accept" = "application/json",
      "Content-Type" = "application/json",
      "Cache-Control" = "no-cache"
    )
  )
  
  test_url <- paste0(working_base_urls[1], "diplomate/", physician_id, "/subspecialty")
  
  for (i in seq_along(auth_headers_to_test)) {
    headers <- auth_headers_to_test[[i]]
    
    if (verbose_debug) {
      cat("Testing header set", i, ":", names(headers)[1], "=", headers[[1]], "\n")
    }
    
    response <- tryCatch({
      httr::GET(test_url, proxy_config, 
                httr::add_headers(.headers = headers), 
                httr::timeout(request_timeout_seconds))
    }, error = function(e) {
      if (verbose_debug) cat("  Error:", e$message, "\n")
      NULL
    })
    
    if (!is.null(response)) {
      content <- httr::content(response, as = "text", encoding = "UTF-8")
      
      # Check if response is different from 400 error
      if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
        if (verbose_debug) {
          cat("🎉 POTENTIALLY SUCCESSFUL RESPONSE WITH HEADERS:\n")
          print(headers)
          cat("Response preview:", substr(content, 1, 500), "\n\n")
        }
        successful_responses[[paste0("headers_", i)]] <- list(
          url = test_url,
          headers = headers,
          response = content,
          status_code = response$status_code
        )
      } else if (verbose_debug) {
        cat("  Still getting 400 error\n")
      }
    }
    
    Sys.sleep(0.5)
  }
  
  # === METHOD 3: Test Session/Cookie Based Authentication ===
  if (verbose_debug) cat("\n=== TESTING SESSION-BASED AUTHENTICATION ===\n")
  
  session_response <- test_session_based_auth(physician_id, proxy_config, request_timeout_seconds, verbose_debug)
  if (!is.null(session_response)) {
    successful_responses[["session_auth"]] <- session_response
  }
  
  # === METHOD 4: Test Form Data Submission ===
  if (verbose_debug) cat("\n=== TESTING FORM DATA SUBMISSION ===\n")
  
  form_response <- test_form_data_submission(physician_id, proxy_config, request_timeout_seconds, verbose_debug)
  if (!is.null(form_response)) {
    successful_responses[["form_data"]] <- form_response
  }
  
  # === METHOD 5: Test Query Parameters ===
  if (verbose_debug) cat("\n=== TESTING QUERY PARAMETERS ===\n")
  
  query_response <- test_query_parameters(physician_id, proxy_config, request_timeout_seconds, verbose_debug)
  if (!is.null(query_response)) {
    successful_responses[["query_params"]] <- query_response
  }
  
  # === METHOD 6: Test Different Endpoints with Best Headers ===
  if (verbose_debug) cat("\n=== TESTING ALL ENDPOINTS WITH BEST HEADERS ===\n")
  
  endpoint_response <- test_all_endpoints_with_best_headers(physician_id, proxy_config, request_timeout_seconds, verbose_debug)
  if (!is.null(endpoint_response)) {
    successful_responses[["endpoint_sweep"]] <- endpoint_response
  }
  
  if (verbose_debug) {
    cat("\n🏁 REVERSE ENGINEERING COMPLETE\n")
    cat("Successful responses found:", length(successful_responses), "\n")
    
    if (length(successful_responses) > 0) {
      cat("\n🎯 BREAKTHROUGH METHODS:\n")
      for (method_name in names(successful_responses)) {
        cat("-", method_name, "\n")
      }
    } else {
      cat("\n❌ No authentication breakthrough yet\n")
      cat("Try manual browser monitoring next\n")
    }
  }
  
  return(successful_responses)
}

#' @noRd
test_api_method <- function(method, test_url, proxy_config, request_timeout_seconds, verbose_debug) {
  
  if (verbose_debug) cat("  Testing", method, "method on", basename(test_url), "...\n")
  
  response <- tryCatch({
    if (method == "GET") {
      httr::GET(test_url, proxy_config, httr::timeout(request_timeout_seconds))
    } else if (method == "POST") {
      httr::POST(test_url, proxy_config, httr::timeout(request_timeout_seconds))
    } else if (method == "PUT") {
      httr::PUT(test_url, proxy_config, httr::timeout(request_timeout_seconds))
    }
  }, error = function(e) {
    if (verbose_debug) cat("    Error with", method, ":", e$message, "\n")
    NULL
  })
  
  if (!is.null(response)) {
    content <- httr::content(response, as = "text", encoding = "UTF-8")
    
    # Check for success indicators
    if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
      if (verbose_debug) {
        cat("🎉 SUCCESS with", method, "method!\n")
        cat("    Response preview:", substr(content, 1, 200), "\n")
      }
      return(list(method = method, url = test_url, response = content, status_code = response$status_code))
    } else if (verbose_debug) {
      cat("    Still 400 error with", method, "\n")
    }
  }
  
  return(NULL)
}

#' @noRd
test_session_based_auth <- function(physician_id, proxy_config, request_timeout_seconds, verbose_debug) {
  
  if (verbose_debug) cat("  Testing session-based authentication...\n")
  
  # Step 1: Get the main ABOG verification page to establish session
  main_page_url <- "https://www.abog.org/verify-physician"
  
  main_response <- tryCatch({
    httr::GET(main_page_url, proxy_config, httr::timeout(request_timeout_seconds + 5))
  }, error = function(e) {
    if (verbose_debug) cat("    Error getting main page:", e$message, "\n")
    NULL
  })
  
  if (is.null(main_response)) return(NULL)
  
  # Extract cookies/session tokens
  cookies <- httr::cookies(main_response)
  
  if (nrow(cookies) > 0) {
    if (verbose_debug) {
      cat("    Found", nrow(cookies), "cookies from main page\n")
      cat("    Cookie names:", paste(cookies$name, collapse = ", "), "\n")
    }
    
    # Step 2: Use session cookies for API call
    test_url <- paste0("https://www.abog.org/api/diplomate/", physician_id, "/subspecialty")
    
    session_response <- tryCatch({
      httr::GET(test_url, proxy_config, 
                httr::set_cookies(.cookies = setNames(cookies$value, cookies$name)),
                httr::add_headers(
                  "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                  "Referer" = main_page_url,
                  "X-Requested-With" = "XMLHttpRequest"
                ),
                httr::timeout(request_timeout_seconds))
    }, error = function(e) {
      if (verbose_debug) cat("    Error with session request:", e$message, "\n")
      NULL
    })
    
    if (!is.null(session_response)) {
      content <- httr::content(session_response, as = "text", encoding = "UTF-8")
      if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
        if (verbose_debug) {
          cat("🎉 SUCCESS with session cookies!\n")
          cat("    Response preview:", substr(content, 1, 200), "\n")
        }
        return(list(
          method = "session_cookies", 
          cookies = cookies, 
          response = content,
          status_code = session_response$status_code
        ))
      } else if (verbose_debug) {
        cat("    Session cookies still gave 400 error\n")
      }
    }
  } else if (verbose_debug) {
    cat("    No cookies found on main page\n")
  }
  
  return(NULL)
}

#' @noRd
test_form_data_submission <- function(physician_id, proxy_config, request_timeout_seconds, verbose_debug) {
  
  if (verbose_debug) cat("  Testing form data submission...\n")
  
  test_url <- paste0("https://www.abog.org/api/diplomate/", physician_id, "/subspecialty")
  
  form_data_sets <- list(
    list(physicianId = physician_id),
    list(id = physician_id),
    list(physid = physician_id),
    list(userId = physician_id),
    list(diplomateId = physician_id),
    list(abogId = physician_id),
    list(physId = as.character(physician_id))
  )
  
  for (i in seq_along(form_data_sets)) {
    form_data <- form_data_sets[[i]]
    
    if (verbose_debug) {
      cat("    Testing form data:", names(form_data)[1], "=", form_data[[1]], "\n")
    }
    
    # Test both form encoding and JSON encoding
    for (encoding_type in c("form", "json")) {
      response <- tryCatch({
        if (encoding_type == "form") {
          httr::POST(test_url, proxy_config, 
                     body = form_data, 
                     encode = "form",
                     httr::add_headers("X-Requested-With" = "XMLHttpRequest"),
                     httr::timeout(request_timeout_seconds))
        } else {
          httr::POST(test_url, proxy_config, 
                     body = form_data, 
                     encode = "json",
                     httr::add_headers(
                       "Content-Type" = "application/json",
                       "X-Requested-With" = "XMLHttpRequest"
                     ),
                     httr::timeout(request_timeout_seconds))
        }
      }, error = function(e) {
        if (verbose_debug) cat("      Error with", encoding_type, ":", e$message, "\n")
        NULL
      })
      
      if (!is.null(response)) {
        content <- httr::content(response, as = "text", encoding = "UTF-8")
        if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
          if (verbose_debug) {
            cat("🎉 SUCCESS with form data (", encoding_type, ")!\n")
            cat("      Response preview:", substr(content, 1, 200), "\n")
          }
          return(list(
            method = paste0("form_data_", encoding_type), 
            form_data = form_data, 
            response = content,
            status_code = response$status_code
          ))
        }
      }
    }
    
    Sys.sleep(0.3)
  }
  
  if (verbose_debug) cat("    No success with form data\n")
  return(NULL)
}

#' @noRd
test_query_parameters <- function(physician_id, proxy_config, request_timeout_seconds, verbose_debug) {
  
  if (verbose_debug) cat("  Testing query parameters...\n")
  
  base_url <- paste0("https://www.abog.org/api/diplomate/", physician_id, "/subspecialty")
  
  query_param_sets <- list(
    list(format = "json"),
    list(details = "true"),
    list(include = "subspecialty"),
    list(expand = "certifications"),
    list(full = "true"),
    list(type = "subspecialty"),
    list(output = "json"),
    list(complete = "1"),
    list(detailed = "yes")
  )
  
  for (i in seq_along(query_param_sets)) {
    query_params <- query_param_sets[[i]]
    
    if (verbose_debug) {
      cat("    Testing query params:", names(query_params)[1], "=", query_params[[1]], "\n")
    }
    
    response <- tryCatch({
      httr::GET(base_url, proxy_config, 
                query = query_params, 
                httr::add_headers(
                  "Accept" = "application/json",
                  "X-Requested-With" = "XMLHttpRequest"
                ),
                httr::timeout(request_timeout_seconds))
    }, error = function(e) {
      if (verbose_debug) cat("      Error:", e$message, "\n")
      NULL
    })
    
    if (!is.null(response)) {
      content <- httr::content(response, as = "text", encoding = "UTF-8")
      if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
        if (verbose_debug) {
          cat("🎉 SUCCESS with query parameters!\n")
          cat("      Response preview:", substr(content, 1, 200), "\n")
        }
        return(list(
          method = "query_params", 
          params = query_params, 
          response = content,
          status_code = response$status_code
        ))
      }
    }
    
    Sys.sleep(0.3)
  }
  
  if (verbose_debug) cat("    No success with query parameters\n")
  return(NULL)
}

#' @noRd
test_all_endpoints_with_best_headers <- function(physician_id, proxy_config, request_timeout_seconds, verbose_debug) {
  
  if (verbose_debug) cat("  Testing all endpoints with best headers...\n")
  
  # Best headers combination based on modern web standards
  best_headers <- list(
    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept" = "application/json, text/plain, */*",
    "Accept-Language" = "en-US,en;q=0.9",
    "Accept-Encoding" = "gzip, deflate, br",
    "Content-Type" = "application/json",
    "X-Requested-With" = "XMLHttpRequest",
    "Referer" = "https://www.abog.org/verify-physician",
    "Origin" = "https://www.abog.org",
    "Cache-Control" = "no-cache",
    "Pragma" = "no-cache"
  )
  
  all_endpoints <- c(
    "diplomate/{id}/subspecialty",
    "diplomate/{id}/details", 
    "diplomate/{id}/certification-details",
    "diplomate/{id}/full-profile",
    "diplomate/{id}/certifications",
    "physician/{id}/subspecialty",
    "physician/{id}/details"
  )
  
  base_urls <- c("https://www.abog.org/api/", "https://abog.org/api/")
  
  for (base_url in base_urls) {
    for (endpoint in all_endpoints) {
      test_url <- paste0(base_url, gsub("\\{id\\}", physician_id, endpoint))
      
      if (verbose_debug) {
        cat("    Testing:", gsub(".*api/", "", test_url), "\n")
      }
      
      response <- tryCatch({
        httr::GET(test_url, proxy_config,
                  httr::add_headers(.headers = best_headers),
                  httr::timeout(request_timeout_seconds))
      }, error = function(e) {
        if (verbose_debug) cat("      Error:", e$message, "\n")
        NULL
      })
      
      if (!is.null(response)) {
        content <- httr::content(response, as = "text", encoding = "UTF-8")
        if (!grepl("400", content) && !grepl("<!DOCTYPE html>", content) && nchar(content) > 50) {
          if (verbose_debug) {
            cat("🎉 SUCCESS with endpoint:", endpoint, "\n")
            cat("      Response preview:", substr(content, 1, 200), "\n")
          }
          return(list(
            method = "endpoint_sweep",
            endpoint = endpoint,
            url = test_url,
            headers = best_headers,
            response = content,
            status_code = response$status_code
          ))
        }
      }
      
      Sys.sleep(0.2)
    }
  }
  
  if (verbose_debug) cat("    No success with endpoint sweep\n")
  return(NULL)
}

# Test function for immediate execution
test_abog_api_breakthrough <- function() {
  cat("🚀 Testing ABOG API reverse engineering...\n\n")
  
  api_results <- reverse_engineer_abog_api(
    physician_id = 9020382,
    verbose_debug = TRUE,
    use_proxy_reverse = TRUE,
    request_timeout_seconds = 10
  )
  
  cat("\n=== FINAL RESULTS ===\n")
  cat("Successful methods found:", length(api_results), "\n")
  
  if (length(api_results) > 0) {
    for (method_name in names(api_results)) {
      cat("\n🎯 BREAKTHROUGH:", method_name, "\n")
      result <- api_results[[method_name]]
      cat("URL:", result$url, "\n")
      cat("Status:", result$status_code, "\n")
      cat("Response:", substr(result$response, 1, 500), "\n")
      cat("---\n")
    }
  } else {
    cat("❌ No API breakthrough yet - need alternative approach\n")
  }
  
  return(api_results)
}

# run ----
# Run the comprehensive API reverse engineering
api_breakthrough_results <- test_abog_api_breakthrough()

# If successful, examine the results
if (length(api_breakthrough_results) > 0) {
  cat("\n🎉 WE FOUND A BREAKTHROUGH!\n")
  cat("Let's extract subspecialty data using the successful method...\n")
  
  # Use the first successful method to test subspecialty extraction
  successful_method <- api_breakthrough_results[[1]]
  cat("Using method:", successful_method$method, "\n")
  cat("Response contains subspecialty data:", 
      grepl("subspecialty|urogynecology|oncology", successful_method$response, ignore.case = TRUE), "\n")
} else {
  cat("\n❌ API approach unsuccessful\n")
  cat("Next: Try RSelenium browser automation\n")
}


# Terst userid ----
# Test ABOG API using userid field instead of physician_id
test_userid_approach <- function() {
  cat("🔍 Testing ABOG API with userid field...\n\n")
  
  # First, get Elena's basic data to extract her userid
  elena_id <- 9020382
  proxy_config <- httr::use_proxy("socks5://localhost:9150")
  
  # Get Elena's basic verification data
  basic_url <- paste0("https://api.abog.org/diplomate/", elena_id, "/verify")
  cat("Getting basic data from:", basic_url, "\n")
  
  basic_response <- tryCatch({
    httr::GET(basic_url, proxy_config, httr::timeout(15))
  }, error = function(e) {
    cat("Error getting basic data:", e$message, "\n")
    NULL
  })
  
  if (is.null(basic_response)) {
    cat("❌ Failed to get basic physician data\n")
    return(NULL)
  }
  
  # Parse the response to get userid
  basic_content <- httr::content(basic_response, as = "text", encoding = "UTF-8")
  cat("Basic response status:", basic_response$status_code, "\n")
  cat("Basic response content:", substr(basic_content, 1, 300), "\n\n")
  
  if (basic_response$status_code == 200) {
    basic_data <- jsonlite::fromJSON(basic_content)
    
    if ("userid" %in% names(basic_data)) {
      userid_value <- basic_data$userid
      cat("✅ Found userid:", userid_value, "\n\n")
      
      # Now test subspecialty endpoints using userid
      test_subspecialty_with_userid(userid_value, proxy_config)
      
    } else {
      cat("❌ No userid field found in basic response\n")
      cat("Available fields:", paste(names(basic_data), collapse = ", "), "\n")
    }
  } else {
    cat("❌ Basic API call failed with status:", basic_response$status_code, "\n")
  }
}

test_subspecialty_with_userid <- function(userid, proxy_config) {
  cat("🧪 Testing subspecialty endpoints with userid:", userid, "\n")
  
  # Test different endpoint patterns using userid
  userid_endpoints <- c(
    paste0("https://www.abog.org/api/diplomate/", userid, "/subspecialty"),
    paste0("https://www.abog.org/api/diplomate/", userid, "/details"),
    paste0("https://www.abog.org/api/diplomate/", userid, "/certification-details"),
    paste0("https://api.abog.org/diplomate/", userid, "/subspecialty"),
    paste0("https://api.abog.org/diplomate/", userid, "/details"),
    
    # Try with userid as query parameter
    paste0("https://www.abog.org/api/diplomate/subspecialty?userid=", userid),
    paste0("https://www.abog.org/api/diplomate/details?userid=", userid),
    paste0("https://api.abog.org/diplomate/subspecialty?userid=", userid),
    
    # Try different parameter names
    paste0("https://www.abog.org/api/diplomate/subspecialty?id=", userid),
    paste0("https://www.abog.org/api/diplomate/details?physid=", userid)
  )
  
  # Test headers for userid requests
  userid_headers <- c(
    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept" = "application/json, text/plain, */*",
    "X-Requested-With" = "XMLHttpRequest",
    "Referer" = "https://www.abog.org/verify-physician",
    "Content-Type" = "application/json"
  )
  
  for (i in seq_along(userid_endpoints)) {
    endpoint_url <- userid_endpoints[i]
    cat("Testing endpoint", i, ":", endpoint_url, "\n")
    
    response <- tryCatch({
      httr::GET(endpoint_url, proxy_config,
                httr::add_headers(.headers = userid_headers),
                httr::timeout(15))
    }, error = function(e) {
      cat("  Error:", e$message, "\n")
      NULL
    })
    
    if (!is.null(response)) {
      content <- httr::content(response, as = "text", encoding = "UTF-8")
      cat("  Status:", response$status_code, "\n")
      
      # Check for success
      if (response$status_code == 200 && 
          !grepl("400|404", content) && 
          !grepl("<!DOCTYPE html>", content) &&
          nchar(content) > 50) {
        
        cat("🎉 SUCCESS WITH USERID APPROACH!\n")
        cat("URL:", endpoint_url, "\n")
        cat("Response:", substr(content, 1, 500), "\n")
        
        # Check if response contains subspecialty data
        if (grepl("subspecialty|urogynecology|oncology|maternal|reproductive", content, ignore.case = TRUE)) {
          cat("🎯 SUBSPECIALTY DATA FOUND!\n")
          
          # Try to parse as JSON
          tryCatch({
            json_data <- jsonlite::fromJSON(content)
            if (is.list(json_data)) {
              cat("Parsed JSON fields:", paste(names(json_data), collapse = ", "), "\n")
            }
          }, error = function(e) {
            cat("Not JSON format, but contains subspecialty keywords\n")
          })
        }
        
        return(list(url = endpoint_url, response = content, userid = userid))
      } else {
        cat("  Still error response\n")
      }
    }
    
    Sys.sleep(0.5)
  }
  
  cat("❌ No success with userid approach\n")
  return(NULL)
}

# Run the userid test
userid_result <- test_userid_approach()

if (!is.null(userid_result)) {
  cat("\n🎉 USERID APPROACH SUCCESSFUL!\n")
  cat("We found the key to ABOG subspecialty data!\n")
} else {
  cat("\n❌ Userid approach also unsuccessful\n")
  cat("The subspecialty data may require browser interaction\n")
}

# Closer ----
# Debug the 200 responses to see what ABOG is actually returning
debug_abog_responses <- function() {
  cat("🔬 DEBUGGING ABOG 200 RESPONSES...\n\n")
  
  userid <- 9020382
  proxy_config <- httr::use_proxy("socks5://localhost:9150")
  
  # Test the endpoints that returned 200 and examine their content
  working_endpoints <- c(
    "https://www.abog.org/api/diplomate/9020382/subspecialty",
    "https://www.abog.org/api/diplomate/9020382/details", 
    "https://www.abog.org/api/diplomate/9020382/certification-details",
    "https://www.abog.org/api/diplomate/subspecialty?userid=9020382"
  )
  
  headers <- c(
    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept" = "application/json, text/plain, */*",
    "X-Requested-With" = "XMLHttpRequest",
    "Referer" = "https://www.abog.org/verify-physician"
  )
  
  for (i in seq_along(working_endpoints)) {
    endpoint_url <- working_endpoints[i]
    cat("=== DEBUGGING ENDPOINT", i, "===\n")
    cat("URL:", endpoint_url, "\n")
    
    response <- tryCatch({
      httr::GET(endpoint_url, proxy_config,
                httr::add_headers(.headers = headers),
                httr::timeout(15))
    }, error = function(e) {
      cat("Error:", e$message, "\n")
      NULL
    })
    
    if (!is.null(response)) {
      cat("Status Code:", response$status_code, "\n")
      
      # Get raw content
      content <- httr::content(response, as = "text", encoding = "UTF-8")
      cat("Content Length:", nchar(content), "characters\n")
      cat("Content Preview:", substr(content, 1, 1000), "\n")
      
      # Check response headers
      cat("Response Headers:\n")
      response_headers <- response$headers
      for (header_name in names(response_headers)) {
        cat("  ", header_name, ":", response_headers[[header_name]], "\n")
      }
      
      # Try to parse as JSON
      tryCatch({
        json_data <- jsonlite::fromJSON(content)
        cat("JSON Parsing: SUCCESS\n")
        if (is.list(json_data)) {
          cat("JSON Fields:", paste(names(json_data), collapse = ", "), "\n")
          cat("JSON Content:", str(json_data), "\n")
        }
      }, error = function(e) {
        cat("JSON Parsing: FAILED -", e$message, "\n")
      })
      
      cat("\n" , rep("-", 50), "\n\n")
    }
    
    Sys.sleep(1)
  }
}

# Run the debugging
debug_abog_responses()


# Try POST requests with userid in body
test_post_userid_approach <- function() {
  cat("🧪 TESTING POST REQUESTS WITH USERID...\n\n")
  
  userid <- 9020382
  proxy_config <- httr::use_proxy("socks5://localhost:9150")
  
  # Test POST to subspecialty endpoint
  post_url <- "https://www.abog.org/api/diplomate/subspecialty"
  
  # Try different POST body formats
  post_bodies <- list(
    # JSON format
    list(userid = userid),
    list(id = userid),
    list(physicianId = userid),
    list(diplomateId = userid),
    
    # Form data format  
    list(userid = as.character(userid)),
    list(id = as.character(userid))
  )
  
  for (i in seq_along(post_bodies)) {
    body_data <- post_bodies[[i]]
    cat("Testing POST body", i, ":", names(body_data)[1], "=", body_data[[1]], "\n")
    
    # Try JSON encoding
    response_json <- tryCatch({
      httr::POST(post_url, proxy_config,
                 body = body_data,
                 encode = "json",
                 httr::add_headers(
                   "Content-Type" = "application/json",
                   "X-Requested-With" = "XMLHttpRequest",
                   "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                 ),
                 httr::timeout(15))
    }, error = function(e) NULL)
    
    if (!is.null(response_json)) {
      content_json <- httr::content(response_json, as = "text")
      cat("  JSON POST - Status:", response_json$status_code, "\n")
      
      if (response_json$status_code == 200 && 
          !grepl("400|404", content_json) && 
          nchar(content_json) > 50) {
        cat("🎉 SUCCESS WITH JSON POST!\n")
        cat("Response:", substr(content_json, 1, 500), "\n")
        return(list(method = "POST_JSON", body = body_data, response = content_json))
      }
    }
    
    # Try form encoding
    response_form <- tryCatch({
      httr::POST(post_url, proxy_config,
                 body = body_data,
                 encode = "form",
                 httr::add_headers(
                   "Content-Type" = "application/x-www-form-urlencoded",
                   "X-Requested-With" = "XMLHttpRequest"
                 ),
                 httr::timeout(15))
    }, error = function(e) NULL)
    
    if (!is.null(response_form)) {
      content_form <- httr::content(response_form, as = "text")
      cat("  FORM POST - Status:", response_form$status_code, "\n")
      
      if (response_form$status_code == 200 && 
          !grepl("400|404", content_form) && 
          nchar(content_form) > 50) {
        cat("🎉 SUCCESS WITH FORM POST!\n")
        cat("Response:", substr(content_form, 1, 500), "\n")
        return(list(method = "POST_FORM", body = body_data, response = content_form))
      }
    }
    
    Sys.sleep(0.5)
  }
  
  cat("❌ No success with POST approaches\n")
  return(NULL)
}

# Run both tests
post_result <- test_post_userid_approach()

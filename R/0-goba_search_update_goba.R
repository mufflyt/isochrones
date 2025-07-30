#
# Purpose:
# This script quantifies temporal changes in geographic access to gynecologic 
# oncologists in the United States from 2013 to 2023. It calculates the percentage 
# of the population within defined drive-time thresholds (e.g., 30, 60, 120, 180 
# minutes) and stratifies access by race/ethnicity, urban/rural classification, and 
# year. These data support longitudinal evaluation of access equity for policy 
# research, healthcare workforce planning, and geographic disparity analysis.
#
# Inputs:
#
# 1. access_by_group.csv
#    - Description: Contains summary statistics of access to gynecologic oncologists 
#      by racial/ethnic group and year. Variables include year, race/ethnicity, and 
#      percent of population within multiple drive-time thresholds.
#    - Provenance: Generated from isochrone-based access models linked with ACS 
#      population estimates stratified by race/ethnicity, from 2013–2023.
#
# 2. access_by_group_urban_rural.csv
#    - Description: Similar to access_by_group.csv, but stratified by urban and rural 
#      census tract classification using 2010 RUCA codes. Each record contains year, 
#      urban/rural label, and access percentages across different drive times.
#    - Provenance: Produced using national drive-time estimates merged with urban/
#      rural designation based on U.S. Census and RUCA classifications.
#
# 3. all_isochrone_demographics.csv
#    - Description: Census tract-level file with detailed demographic overlays 
#      (including total population, race/ethnicity, urban/rural status) for each year 
#      2013–2023, along with drive-time access bins (e.g., <30, <60 minutes).
#    - Provenance: Constructed by linking gynecologic oncologist isochrone output to 
#      ACS demographic data using tract-level spatial joins. Used as the foundation 
#      for aggregated summaries by year, group, and geography.
#
# Processing Overview:
# - Imports and cleans yearly access data.
# - Aggregates and visualizes trends in access over time.
# - Calculates magnitude and percentage change in access to providers across groups.
# - Prepares tables and visualizations for manuscript inclusion.
#
# Outputs:
# - Time-series plots and summary statistics of access trends.
# - Figures illustrating racial/ethnic disparities in geographic access.
# - Tables for publication summarizing urban-rural gaps in drive-time-based access.
# - Optional: CSV exports of cleaned or processed summary tables.
#

source("R/01-setup.R")

# Read in Data ----
# Read in subspecialist obgyns ----
goba_unrestricted_cleaned <- readr::read_csv(
  "data/B-nber_nppes_combine_columns/goba_unrestricted_cleaned.csv"
) 

# Read in ALL obgyns and do an anti-join of subspecialist obgyns ----
general_OBGYNs_who_may_have_gone_on_to_fellowship <- readr::read_csv(
  "data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv"
) %>%
  distinct(npi, .keep_all = TRUE) %>%
  dplyr::anti_join(`goba_unrestricted_cleaned`, by = join_by(`npi` == `npi`))

dim(general_OBGYNs_who_may_have_gone_on_to_fellowship)
glimpse(general_OBGYNs_who_may_have_gone_on_to_fellowship)
# View(general_OBGYNs_who_may_have_gone_on_to_fellowship)

# Fixed update_physician_records with proper scoping ----

#' Update Previously Discovered Physician Records (Fixed Version)
#'
#' This function systematically updates information for previously discovered
#' physicians to track career progressions, specialty changes, location moves,
#' and certification status updates. Fixed scoping issues for helper functions.
#'
#' @param input_physician_data Data frame or file path. Previously discovered
#'   physician data containing physician_id column.
#' @param tor_proxy_port Numeric. The port number for Tor proxy connection (default: 9150)
#' @param request_delay_seconds Numeric. Delay between requests (default: 2.0)
#' @param verbose_logging Logical. Enable detailed logging (default: TRUE)
#' @param output_directory_path Character. Output directory (default: "physician_data/updates")
#' @param track_changes_only Logical. Only save records with changes (default: FALSE)
#' @param batch_size Numeric. Batch size for saves (default: 100)
#' @param progress_logging_interval Numeric. Progress logging interval (default: 25)
#'
#' @return List with updated physicians and change analysis
#'
#' @examples
#' # Update from file
#' results <- update_physician_records_fixed(
#'   input_physician_data = "discovered_physicians.csv"
#' )
#'
#' # Update with change tracking only
#' results <- update_physician_records_fixed(
#'   input_physician_data = my_data,
#'   track_changes_only = TRUE
#' )
#'
#' @export
update_physician_records_fixed <- function(input_physician_data,
                                           tor_proxy_port = 9150,
                                           request_delay_seconds = 2.0,
                                           verbose_logging = TRUE,
                                           output_directory_path = "physician_data/updates",
                                           track_changes_only = FALSE,
                                           batch_size = 100,
                                           progress_logging_interval = 25) {
  
  # Input validation
  assertthat::assert_that(
    is.numeric(tor_proxy_port) && tor_proxy_port > 0 && tor_proxy_port <= 65535,
    msg = "tor_proxy_port must be a valid port number (1-65535)"
  )
  assertthat::assert_that(
    is.numeric(request_delay_seconds) && request_delay_seconds > 0,
    msg = "request_delay_seconds must be a positive numeric value"
  )
  assertthat::assert_that(
    is.logical(verbose_logging),
    msg = "verbose_logging must be TRUE or FALSE"
  )
  
  # Helper function: Extract safe value
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
  
  # Helper function: Make API request
  make_physician_api_request <- function(physician_id, tor_port) {
    api_url <- paste0("https://api.abog.org/diplomate/", physician_id, "/verify")
    
    tryCatch({
      api_response <- httr::GET(
        api_url, 
        httr::use_proxy(paste0("socks5://localhost:", tor_port))
      )
      
      if (api_response$status_code == 200) {
        response_content <- httr::content(api_response, as = "text", encoding = "UTF-8")
        
        if (nchar(response_content) == 0 || 
            response_content == "null" || 
            response_content == "{}") {
          return(list(success = FALSE, physician_data = NULL))
        }
        
        parsed_data <- jsonlite::fromJSON(response_content)
        
        if (is.list(parsed_data) && length(parsed_data) > 0) {
          return(list(success = TRUE, physician_data = parsed_data))
        } else {
          return(list(success = FALSE, physician_data = NULL))
        }
        
      } else {
        return(list(success = FALSE, physician_data = NULL))
      }
      
    }, error = function(e) {
      return(list(success = FALSE, physician_data = NULL))
    })
  }
  
  # Helper function: Detect changes (ignoring trivial whitespace)
  detect_physician_changes <- function(original_physician, updated_physician, verbose_logging) {
    changes_detected <- list()
    physician_id <- original_physician$physician_id
    
    # Helper to normalize strings (trim whitespace, handle NAs)
    normalize_string <- function(x) {
      if (is.na(x) || is.null(x)) return("")
      return(trimws(as.character(x)))
    }
    
    # Check name changes (ignore whitespace differences)
    original_name_clean <- normalize_string(original_physician$physician_name)
    updated_name_clean <- normalize_string(updated_physician$physician_name)
    
    if (nchar(original_name_clean) > 0 && nchar(updated_name_clean) > 0 &&
        original_name_clean != updated_name_clean) {
      changes_detected$name_change <- list(
        from = original_physician$physician_name,
        to = updated_physician$physician_name
      )
    }
    
    # Check location changes (ignore whitespace differences)
    original_location_clean <- normalize_string(original_physician$physician_location)
    updated_location_clean <- normalize_string(updated_physician$physician_location)
    
    if (nchar(original_location_clean) > 0 && nchar(updated_location_clean) > 0 &&
        original_location_clean != updated_location_clean) {
      changes_detected$location_change <- list(
        from = original_physician$physician_location,
        to = updated_physician$physician_location
      )
    }
    
    # Log significant changes
    if (length(changes_detected) > 0 && verbose_logging) {
      cat("🔄 CHANGES DETECTED for ID:", physician_id, "\n")
      
      if (!is.null(changes_detected$name_change)) {
        cat("   📝 Name: '", changes_detected$name_change$from, 
            "' → '", changes_detected$name_change$to, "'\n", sep = "")
      }
      
      if (!is.null(changes_detected$location_change)) {
        cat("   📍 Location: '", changes_detected$location_change$from, 
            "' → '", changes_detected$location_change$to, "'\n", sep = "")
      }
    }
    
    return(changes_detected)
  }
  
  # Load physician data
  if (is.character(input_physician_data)) {
    assertthat::assert_that(
      file.exists(input_physician_data),
      msg = paste("File not found:", input_physician_data)
    )
    physician_data <- readr::read_csv(input_physician_data, show_col_types = FALSE)
    if (verbose_logging) {
      cat("📂 Loaded physician data from file:", input_physician_data, "\n")
    }
  } else {
    physician_data <- input_physician_data
    if (verbose_logging) {
      cat("📊 Using provided physician data frame\n")
    }
  }
  
  # Validate required columns
  assertthat::assert_that(
    "physician_id" %in% names(physician_data),
    msg = "physician_data must contain 'physician_id' column"
  )
  
  if (verbose_logging) {
    cat("✅ Validation successful:", nrow(physician_data), "physician records to update\n")
  }
  
  # Create output directory
  if (!dir.exists(output_directory_path)) {
    dir.create(output_directory_path, recursive = TRUE, showWarnings = FALSE)
    if (verbose_logging) {
      cat("📁 Created update output directory:", output_directory_path, "\n")
    }
  }
  
  # Initialize tracking
  total_physicians <- nrow(physician_data)
  updated_physicians_data <- data.frame()
  changed_physicians_data <- data.frame()
  failed_physician_ids <- c()
  total_request_count <- 0
  successful_updates <- 0
  update_start_time <- Sys.time()
  last_batch_save_count <- 0
  
  if (verbose_logging) {
    cat("🔄 PHYSICIAN RECORD UPDATE ANALYSIS\n")
    cat("📊 Update Session Summary:\n")
    cat("   - Total physicians to update:", total_physicians, "\n")
    cat("   - Batch size for intermediate saves:", batch_size, "\n")
    cat("   - Expected duration: ~", round(total_physicians * 2.5 / 60, 1), "minutes\n")
    cat("\n🚀 Starting systematic physician record updates...\n")
  }
  
  # Main update loop
  for (i in 1:total_physicians) {
    
    # Progress logging
    if (verbose_logging && total_request_count %% progress_logging_interval == 0) {
      elapsed_minutes <- as.numeric(difftime(Sys.time(), update_start_time, units = "mins"))
      progress_percentage <- round((i / total_physicians) * 100, 1)
      success_rate <- if (total_request_count > 0) {
        (successful_updates / total_request_count) * 100
      } else {
        0
      }
      cat("📈 Update Progress:", i, "/", total_physicians,
          "(", progress_percentage, "%) | Success:", round(success_rate, 1), 
          "% | Time:", round(elapsed_minutes, 1), "min\n")
    }
    
    # Get current physician info
    current_physician <- physician_data[i, ]
    physician_id <- current_physician$physician_id
    
    # Make API request for updated data
    api_response <- make_physician_api_request(physician_id, tor_proxy_port)
    total_request_count <- total_request_count + 1
    
    if (api_response$success && length(api_response$physician_data) > 0) {
      successful_updates <- successful_updates + 1
      
      # Create updated physician record
      current_physician_city <- extract_safe_value(api_response$physician_data$city, "")
      current_physician_state <- extract_safe_value(api_response$physician_data$state, "")
      current_physician_name <- extract_safe_value(api_response$physician_data$name, "Unknown Name")
      current_physician_location <- construct_location_string(current_physician_city, current_physician_state)
      
      updated_physician_record <- data.frame(
        physician_id = physician_id,
        physician_name = current_physician_name,
        physician_city = current_physician_city,
        physician_state = current_physician_state,
        physician_location = current_physician_location,
        last_update_timestamp = Sys.time(),
        original_discovery_timestamp = current_physician$discovery_timestamp %||% NA,
        update_methodology = "longitudinal_tracking",
        original_physician_name = current_physician$physician_name %||% NA,
        original_physician_city = current_physician$physician_city %||% NA,
        original_physician_state = current_physician$physician_state %||% NA,
        original_physician_location = current_physician$physician_location %||% NA,
        stringsAsFactors = FALSE
      )
      
      # Detect changes
      changes_detected <- detect_physician_changes(current_physician, updated_physician_record, verbose_logging)
      
      # Add change info to record
      updated_physician_record$changes_detected <- length(changes_detected) > 0
      updated_physician_record$change_count <- length(changes_detected)
      updated_physician_record$change_types <- if (length(changes_detected) > 0) {
        paste(names(changes_detected), collapse = ", ")
      } else {
        "none"
      }
      
      updated_physicians_data <- dplyr::bind_rows(updated_physicians_data, updated_physician_record)
      
      # Track changed physicians separately
      if (length(changes_detected) > 0) {
        changed_physicians_data <- dplyr::bind_rows(changed_physicians_data, updated_physician_record)
      }
      
      if (verbose_logging && i %% 50 == 0) {
        cat("✅ Updated physician", physician_id, ":", current_physician_name, "\n")
      }
      
    } else {
      failed_physician_ids <- c(failed_physician_ids, physician_id)
      if (verbose_logging && i %% 100 == 0) {
        cat("❌ Failed to update physician ID:", physician_id, "\n")
      }
    }
    
    # Save intermediate batch if needed
    if (successful_updates - last_batch_save_count >= batch_size) {
      timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
      batch_filename <- file.path(output_directory_path, paste0("intermediate_updates_", timestamp, ".csv"))
      readr::write_csv(updated_physicians_data, batch_filename)
      last_batch_save_count <- successful_updates
      if (verbose_logging) {
        cat("💾 Saved intermediate batch:", nrow(updated_physicians_data), "records\n")
      }
    }
    
    # Apply delay
    Sys.sleep(request_delay_seconds + stats::runif(1, -0.2, 0.2))
  }
  
  # Finalize results
  update_end_time <- Sys.time()
  total_duration_minutes <- as.numeric(difftime(update_end_time, update_start_time, units = "mins"))
  final_success_rate <- if (total_request_count > 0) {
    (successful_updates / total_request_count) * 100
  } else {
    0
  }
  
  # Create change summary
  if (nrow(changed_physicians_data) > 0) {
    name_changes <- sum(grepl("name_change", changed_physicians_data$change_types))
    location_changes <- sum(grepl("location_change", changed_physicians_data$change_types))
    
    change_summary <- data.frame(
      change_type = c("Name Changes", "Location Changes", "Any Change"),
      count = c(name_changes, location_changes, nrow(changed_physicians_data)),
      percentage = round(c(name_changes, location_changes, nrow(changed_physicians_data)) / nrow(changed_physicians_data) * 100, 1),
      stringsAsFactors = FALSE
    )
  } else {
    change_summary <- data.frame(
      change_type = character(0),
      count = numeric(0),
      percentage = numeric(0)
    )
  }
  
  # Save final results
  timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  saved_files <- list()
  
  if (!track_changes_only && nrow(updated_physicians_data) > 0) {
    all_updates_filename <- file.path(output_directory_path, paste0("all_updated_physicians_", timestamp, ".csv"))
    readr::write_csv(updated_physicians_data, all_updates_filename)
    saved_files$all_updates_file <- all_updates_filename
  }
  
  if (nrow(changed_physicians_data) > 0) {
    changes_filename <- file.path(output_directory_path, paste0("physicians_with_changes_", timestamp, ".csv"))
    readr::write_csv(changed_physicians_data, changes_filename)
    saved_files$changes_file <- changes_filename
  }
  
  # Log final summary
  if (verbose_logging) {
    cat("\n=== PHYSICIAN UPDATE ANALYSIS COMPLETED ===\n")
    cat("⏱️  Total duration:", round(total_duration_minutes, 1), "minutes\n")
    cat("📊 Total update requests:", total_request_count, "\n")
    cat("✅ Successful updates:", successful_updates, "\n")
    cat("📈 Update success rate:", round(final_success_rate, 1), "%\n")
    cat("🔄 Physicians with changes:", nrow(changed_physicians_data), "\n")
    
    if (nrow(change_summary) > 0) {
      cat("\n📋 Change Summary:\n")
      for (i in 1:nrow(change_summary)) {
        cat("   -", change_summary$change_type[i], ":", change_summary$count[i], 
            "(", change_summary$percentage[i], "%)\n")
      }
    }
    
    # Provide recommendations
    change_rate <- (nrow(changed_physicians_data) / successful_updates) * 100
    if (change_rate > 10) {
      cat("\n📈 HIGH CHANGE RATE (", round(change_rate, 1), "%) - Consider more frequent updates\n")
    } else if (change_rate > 5) {
      cat("\n📊 MODERATE CHANGE RATE (", round(change_rate, 1), "%) - Standard update frequency appropriate\n")
    } else {
      cat("\n📉 LOW CHANGE RATE (", round(change_rate, 1), "%) - Less frequent updates may be sufficient\n")
    }
  }
  
  return(list(
    updated_physicians = updated_physicians_data,
    changed_physicians = changed_physicians_data,
    change_summary = change_summary,
    failed_updates = failed_physician_ids,
    update_success_rate = final_success_rate,
    total_changes_detected = nrow(changed_physicians_data),
    update_duration_minutes = total_duration_minutes,
    output_files_created = saved_files
  ))
}
# run ----
# Update all your discovered physicians
# Use the fixed version
update_results <- update_physician_records_fixed(
  input_physician_data = "physician_data/discovery_results/discovered_physicians_1000_starting_9041223_2025-07-27_16-40-13.csv",
  verbose_logging = TRUE,
  track_changes_only = FALSE
)

# Focus only on changes for efficiency
changes_only <- update_physician_records(
  input_physician_data = general_OBGYNs_who_may_have_gone_on_to_fellowship,
  track_changes_only = TRUE,  # Only save physicians with changes
  output_directory_path = "physician_data/longitudinal_changes"
)


# scrape_abog_physicians_filtered function at 2026 ----
  #' Efficient ABOG Physician Data Scraper with Updated Wrong ID Filtering
  #'
  #' This function implements multiple strategies to efficiently scrape physician
  #' data from the ABOG API while minimizing server load and respecting rate limits.
  #' Updated to filter outdated wrong IDs below a specified threshold.
  #'
  #' @param strategy Character. Strategy to use: "adaptive_binary", "pattern_based", 
  #'   "resident_pattern", "chunk_sampling", or "hybrid"
  #' @param start_id Integer. Starting ID for scraping (default: 1)
  #' @param end_id Integer. Ending ID for scraping (default: 10000000)
  #' @param tor_port Integer. Port number for Tor SOCKS proxy (default: 9150)
  #' @param max_requests Integer. Maximum number of requests to make (default: 1000)
  #' @param delay_seconds Numeric. Delay between requests in seconds (default: 2)
  #' @param chunk_size Integer. Size of chunks for sampling strategy (default: 1000)
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
  #' # Search for residents in the gap range with filtered wrong IDs
  #' gap_search_results <- scrape_abog_physicians_filtered(
  #'   strategy = "chunk_sampling",
  #'   start_id = 9041000,
  #'   end_id = 9042999,
  #'   max_requests = 1000,
  #'   chunk_size = 100,
  #'   wrong_id_filter_threshold = 9040372,
  #'   delay_seconds = 1.5,
  #'   verbose = TRUE
  #' )
  #' 
  #' # Comprehensive hybrid search for 2025 graduates
  #' graduate_search_results <- scrape_abog_physicians_filtered(
  #'   strategy = "hybrid",
  #'   start_id = 9044000,
  #'   end_id = 9050000,
  #'   max_requests = 1500,
  #'   wrong_id_filter_threshold = 9040372,
  #'   delay_seconds = 1.2,
  #'   verbose = TRUE
  #' )
  #' 
  #' # Dense sampling in promising range
  #' dense_search_results <- scrape_abog_physicians_filtered(
  #'   strategy = "chunk_sampling", 
  #'   start_id = 9041500,
  #'   end_id = 9042500,
  #'   max_requests = 800,
  #'   chunk_size = 50,
  #'   wrong_id_filter_threshold = 9040372,
  #'   delay_seconds = 1.5
  #' )
  #'
  #' @export
  scrape_abog_physicians_filtered <- function(strategy = "adaptive_binary",
                                              start_id = 1,
                                              end_id = 10000000,
                                              tor_port = 9150,
                                              max_requests = 1000,
                                              delay_seconds = 2,
                                              chunk_size = 1000,
                                              wrong_id_filter_threshold = 9040372,
                                              verbose = TRUE,
                                              output_directory = "physician_data/discovery_results") {
    
    # Input validation with updated parameters
    assertthat::assert_that(is.character(strategy))
    assertthat::assert_that(is.numeric(start_id) && start_id > 0)
    assertthat::assert_that(is.numeric(end_id) && end_id > start_id)
    assertthat::assert_that(is.numeric(tor_port) && tor_port > 0)
    assertthat::assert_that(is.numeric(max_requests) && max_requests > 0)
    assertthat::assert_that(is.numeric(wrong_id_filter_threshold))
    assertthat::assert_that(is.logical(verbose))
    
    if (verbose) {
      logger::log_info("Starting filtered ABOG physician scraper")
      logger::log_info("Strategy: {strategy}, Range: {start_id}-{end_id}")
      logger::log_info("Max requests: {max_requests}, Delay: {delay_seconds}s")
      logger::log_info("Wrong ID filter threshold: {wrong_id_filter_threshold}")
    }
    
    # Load existing wrong IDs with filtering
    wrong_ids_existing <- load_filtered_wrong_ids(output_directory, wrong_id_filter_threshold, verbose)
    
    # Initialize tracking variables
    physicians_data_collected <- data.frame()
    wrong_ids_new_discovered <- c()
    request_count_total <- 0
    scraping_start_time <- Sys.time()
    
    # Strategy selection with resident-focused patterns
    candidate_ids_to_check <- switch(strategy,
                                     "adaptive_binary" = generate_adaptive_binary_ids_focused(start_id, end_id, max_requests, verbose),
                                     "pattern_based" = generate_resident_pattern_ids(start_id, end_id, max_requests, verbose),
                                     "resident_pattern" = generate_resident_pattern_ids(start_id, end_id, max_requests, verbose),
                                     "chunk_sampling" = generate_chunk_sampling_ids_dense(start_id, end_id, chunk_size, max_requests, verbose),
                                     "hybrid" = generate_hybrid_resident_search(start_id, end_id, max_requests, verbose),
                                     stop("Unknown strategy: {strategy}")
    )
    
    if (verbose) {
      logger::log_info("Generated {length(candidate_ids_to_check)} candidate IDs using {strategy} strategy")
      logger::log_info("Filtered {length(wrong_ids_existing)} existing wrong IDs (threshold: {wrong_id_filter_threshold})")
    }
    
    # Main scraping loop with enhanced resident detection
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
          
          if (verbose) {
            physician_name <- enhanced_physician_data$name %||% "Unknown"
            display_info <- enhanced_physician_data$display_info %||% "Basic info"
            logger::log_info("Found physician {current_id}: {physician_name} ({display_info})")
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
        success_rate_percentage <- nrow(physicians_data_collected) / request_count_total * 100
        recent_graduates_count <- sum(physicians_data_collected$graduation_year >= 2020, na.rm = TRUE)
        logger::log_info("Progress: {request_count_total}/{max_requests} requests")
        logger::log_info("Found {nrow(physicians_data_collected)} physicians ({round(success_rate_percentage, 1)}% success)")
        logger::log_info("Recent graduates (2020+): {recent_graduates_count}")
      }
    }
    
    # Save results with enhanced metadata
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
      logger::log_info("Found {nrow(physicians_data_collected)} physicians in {request_count_total} requests")
      logger::log_info("Success rate: {round(comprehensive_statistics$success_rate * 100, 1)}%")
      logger::log_info("Recent graduates: {comprehensive_statistics$recent_graduates_count}")
    }
    
    return(list(
      physicians = physicians_data_collected,
      wrong_ids = wrong_ids_new_discovered,
      statistics = comprehensive_statistics,
      files = saved_file_paths$file_paths
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
  
  #' @noRd
  generate_chunk_sampling_ids_dense <- function(start_id, end_id, chunk_size, max_requests, verbose) {
    if (verbose) logger::log_info("Using dense chunk sampling strategy with chunk size {chunk_size}")
    
    total_range_size <- end_id - start_id + 1
    number_of_chunks <- ceiling(total_range_size / chunk_size)
    
    # Increase sampling density for resident search
    samples_per_chunk <- max(3, floor(max_requests / number_of_chunks))
    
    collected_ids <- c()
    
    for (chunk_index in 1:number_of_chunks) {
      chunk_start_id <- start_id + (chunk_index - 1) * chunk_size
      chunk_end_id <- min(start_id + chunk_index * chunk_size - 1, end_id)
      
      # Use systematic sampling within chunk for better coverage
      chunk_range_size <- chunk_end_id - chunk_start_id + 1
      if (chunk_range_size >= samples_per_chunk) {
        sampling_interval <- floor(chunk_range_size / samples_per_chunk)
        chunk_sampled_ids <- seq(chunk_start_id, chunk_end_id, by = sampling_interval)
        chunk_sampled_ids <- head(chunk_sampled_ids, samples_per_chunk)
      } else {
        chunk_sampled_ids <- chunk_start_id:chunk_end_id
      }
      
      collected_ids <- c(collected_ids, chunk_sampled_ids)
      
      if (length(collected_ids) >= max_requests) break
    }
    
    return(sort(collected_ids[1:min(length(collected_ids), max_requests)]))
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
    
    # Look for specialty/certification info
    specialty_info <- ""
    if (!is.null(enhanced_data$specialty)) {
      specialty_info <- enhanced_data$specialty
    } else if (!is.null(enhanced_data$certification)) {
      specialty_info <- enhanced_data$certification
    } else if (!is.null(enhanced_data$board_certification)) {
      specialty_info <- enhanced_data$board_certification
    }
    
    # Create display string with most useful info available
    display_parts <- c()
    if (location_info != "") display_parts <- c(display_parts, location_info)
    if (specialty_info != "") display_parts <- c(display_parts, specialty_info)
    if (!is.null(enhanced_data$graduation_year)) {
      display_parts <- c(display_parts, paste0("Grad:", enhanced_data$graduation_year))
    }
    
    enhanced_data$display_info <- if (length(display_parts) > 0) {
      paste(display_parts, collapse=" | ")
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
                                     paste0("physicians_", strategy, "_", start_id, "-", end_id, 
                                            "_filtered_", filter_threshold, "_", current_timestamp, ".csv"))
    
    if (nrow(physicians_data) > 0) {
      readr::write_csv(physicians_data, physicians_filename)
      if (verbose) logger::log_info("Enhanced physicians data saved to: {physicians_filename}")
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
  
  test_known_physician_ids <- function() {
    # Known working physician IDs from your previous examples
    known_working_ids <- c(3, 9033995, 9040372)  # Including Erin Franks
    
    logger::log_info("Testing with known working physician IDs: {paste(known_working_ids, collapse=', ')}")
    
    test_results <- scrape_abog_physicians_filtered(
      strategy = "pattern_based",
      start_id = min(known_working_ids),
      end_id = max(known_working_ids),
      max_requests = 20,
      wrong_id_filter_threshold = 1,  # Low threshold to test all IDs
      delay_seconds = 2,
      verbose = TRUE
    )
    
    return(test_results)
  }
  
# run ----
adaptive_binary <- scrape_abog_physicians_filtered(
    strategy = "adaptive_binary",
    start_id = 1,
    end_id = 10000000,
    tor_port = 9150,
    max_requests = 1000,
    delay_seconds = 2,
    chunk_size = 1000,
    wrong_id_filter_threshold = 9040372,
    verbose = TRUE,
    output_directory = "physician_data/discovery_results"
  )
  
  adaptive_binary
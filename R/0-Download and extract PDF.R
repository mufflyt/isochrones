# Setup and Configuration ----
source("R/01-setup.R")

# install.packages(c("rvest", "httr2", "pdftools", "stringr", "assertthat"))
library(rvest)
library(httr2)
library(pdftools)
library(tidyverse)
library(stringr)
library(assertthat)
library(cli)


# Based on the search results, non-expiring certification from the American Board of Obstetrics and Gynecology (ABOG) ended in 1986. If you achieved specialty certification prior to 1986, you hold a certificate that is not time-limited and your certificate doesn't expire

# Extract ABOG Subspecialty Data via PDF Certification Letters ----
#' Extract ABOG Subspecialty Data via PDF Certification Letters
#'
#' This function automates the process of downloading ABOG certification letter PDFs
#' and extracting subspecialty information from them. Uses modern web scraping 
#' techniques with rvest and httr2 instead of deprecated Selenium/PhantomJS.
#'
#' @param physician_id_list numeric vector. ABOG physician IDs to process
#' @param use_proxy_requests logical. Whether to use proxy for requests. Default: FALSE
#' @param proxy_url_requests character. Proxy URL (Tor SOCKS5). Default: "socks5://127.0.0.1:9050"
#' @param output_directory_extractions character. Directory to save PDFs and results. 
#'   Default: "abog_pdf_extractions"
#' @param request_delay_seconds numeric. Delay between requests in seconds. Default: 3.0
#' @param verbose_extraction_logging logical. Enable detailed logging. Default: TRUE
#' @param cleanup_downloaded_pdfs logical. Whether to delete PDFs after extraction. Default: FALSE
#' @param chunk_size_records numeric. Number of records to process before saving 
#'   progress to combined file. Default: 50
#' @param save_individual_files logical. Whether to save individual CSV files 
#'   for each chunk. Default: TRUE
#'
#' @return data.frame with subspecialty information extracted from PDFs:
#'   \itemize{
#'     \item{physician_id: ABOG physician ID}
#'     \item{abog_id_number: ABOG physician ID number}
#'     \item{physician_name: Full name from PDF}
#'     \item{clinically_active_status: Whether physician is clinically active}
#'     \item{primary_certification: Primary board certification}
#'     \item{primary_cert_date: Original certification date}
#'     \item{primary_cert_status: Current certification status}
#'     \item{primary_continuing_cert: Participating in continuing certification}
#'     \item{subspecialty_name: Subspecialty certification name}
#'     \item{subspecialty_cert_date: Original subspecialty certification date}
#'     \item{subspecialty_cert_status: Current subspecialty certification status}
#'     \item{subspecialty_continuing_cert: Participating in continuing certification}
#'     \item{extraction_timestamp: When data was extracted}
#'     \item{pdf_filename: Saved PDF filename}
#'   }
#'
#' @examples
#' # Example 1: Single physician test (Elena)
#' elena_subspecialty_data <- extract_subspecialty_from_pdf_letters(
#'   physician_id_list = 9020382,
#'   output_directory_extractions = "elena_pdf_test",
#'   verbose_extraction_logging = TRUE,
#'   cleanup_downloaded_pdfs = FALSE
#' )
#' 
#' # Example 2: Batch processing with Tor proxy
#' test_physician_list <- c(9020382, 9014566, 849120, 930075)
#' batch_subspecialty_results <- extract_subspecialty_from_pdf_letters(
#'   physician_id_list = test_physician_list,
#'   use_proxy_requests = TRUE,
#'   output_directory_extractions = "batch_pdf_extractions", 
#'   request_delay_seconds = 5.0,
#'   chunk_size_records = 25,
#'   cleanup_downloaded_pdfs = TRUE,
#'   verbose_extraction_logging = TRUE
#' )
#' 
#' # Example 3: Large dataset with chunking and combined file output
#' large_physician_cohort <- c(9000000:9000100)
#' large_subspecialty_extraction <- extract_subspecialty_from_pdf_letters(
#'   physician_id_list = large_physician_cohort,
#'   use_proxy_requests = TRUE,
#'   proxy_url_requests = "socks5://127.0.0.1:9050",
#'   output_directory_extractions = "large_pdf_batch",
#'   request_delay_seconds = 10.0,
#'   cleanup_downloaded_pdfs = TRUE,
#'   verbose_extraction_logging = FALSE,
#'   chunk_size_records = 25,
#'   save_individual_files = TRUE
#' )
#'
#' @importFrom rvest read_html html_elements html_attr
#' @importFrom httr2 request req_perform resp_body_raw resp_status
#' @importFrom pdftools pdf_text
#' @importFrom stringr str_extract str_detect str_trim
#' @importFrom dplyr tibble bind_rows
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom readr write_csv
#' @importFrom cli cli_progress_bar cli_progress_update cli_progress_done
#'
#' @export
extract_subspecialty_from_pdf_letters <- function(
    physician_id_list,
    use_proxy_requests = FALSE,
    proxy_url_requests = "socks5://127.0.0.1:9050",
    output_directory_extractions = "abog_pdf_extractions",
    request_delay_seconds = 3.0,
    verbose_extraction_logging = TRUE,
    cleanup_downloaded_pdfs = FALSE,
    chunk_size_records = 50,
    save_individual_files = TRUE) {
  
  # Input validation
  assertthat::assert_that(is.numeric(physician_id_list) || is.character(physician_id_list))
  assertthat::assert_that(length(physician_id_list) > 0)
  assertthat::assert_that(is.logical(use_proxy_requests))
  assertthat::assert_that(is.character(output_directory_extractions))
  assertthat::assert_that(is.numeric(request_delay_seconds) && request_delay_seconds >= 0)
  assertthat::assert_that(is.logical(verbose_extraction_logging))
  assertthat::assert_that(is.logical(cleanup_downloaded_pdfs))
  assertthat::assert_that(is.numeric(chunk_size_records) && chunk_size_records > 0)
  assertthat::assert_that(is.logical(save_individual_files))
  
  # Record extraction start time
  extraction_start_time <- Sys.time()
  
  if (verbose_extraction_logging) {
    logger::log_info("🔍 Starting ABOG PDF subspecialty extraction")
    logger::log_info("⏰ Start time: {format(extraction_start_time, '%Y-%m-%d %H:%M:%S')}")
    logger::log_info("Processing {length(physician_id_list)} physicians")
    logger::log_info("🆔 ID range: {min(physician_id_list)} to {max(physician_id_list)}")
    logger::log_info("Output directory: {output_directory_extractions}")
    logger::log_info("Request delay: {request_delay_seconds} seconds")
    logger::log_info("Chunk size: {chunk_size_records} records")
    logger::log_info("Save individual files: {save_individual_files}")
  }
  
  # Create timestamped output directory
  timestamp_subfolder <- format(extraction_start_time, "%Y%m%d_%H%M%S")
  timestamped_output_directory <- file.path(output_directory_extractions, timestamp_subfolder)
  
  if (!dir.exists(timestamped_output_directory)) {
    dir.create(timestamped_output_directory, recursive = TRUE)
    if (verbose_extraction_logging) {
      logger::log_info("📁 Created timestamped output directory: {timestamped_output_directory}")
    }
  }
  
  # Update output directory to use timestamped version
  output_directory_extractions <- timestamped_output_directory
  
  # Initialize HTTP session with Tor proxy support
  session_config <- list(
    use_proxy = use_proxy_requests,
    proxy_url = proxy_url_requests,
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  )
  
  if (verbose_extraction_logging) {
    logger::log_info("✅ HTTP session initialized successfully")
    if (use_proxy_requests && !is.null(proxy_url_requests)) {
      logger::log_info("🔐 Using Tor proxy: {proxy_url_requests}")
      logger::log_info("ℹ️ Ensure Tor is running on the specified port")
    }
  }
  
  # Test Tor connectivity if proxy is enabled
  if (use_proxy_requests && verbose_extraction_logging) {
    tor_test_result <- test_tor_connectivity(proxy_url_requests, verbose_extraction_logging)
    if (!tor_test_result) {
      logger::log_warn("⚠️ Tor connectivity test failed - extraction may not use proxy")
    }
  }
  
  # Split physician list into chunks
  physician_chunks <- split(physician_id_list, ceiling(seq_along(physician_id_list) / chunk_size_records))
  total_chunks <- length(physician_chunks)
  failed_chunks <- c()
  
  if (verbose_extraction_logging) {
    logger::log_info("📦 Created {total_chunks} chunks of max {chunk_size_records} physicians each")
  }
  
  # Initialize results tracking
  combined_extraction_results <- dplyr::tibble(
    physician_id = numeric(0),
    abog_id_number = character(0),
    physician_name = character(0),
    clinically_active_status = character(0),
    primary_certification = character(0),
    primary_cert_date = character(0),
    primary_cert_status = character(0),
    primary_continuing_cert = character(0),
    subspecialty_name = character(0),
    subspecialty_cert_date = character(0),
    subspecialty_cert_status = character(0),
    subspecialty_continuing_cert = character(0),
    extraction_timestamp = as.POSIXct(character(0)),
    pdf_filename = character(0)
  )
  
  successful_extraction_count <- 0
  failed_extraction_count <- 0
  
  # Initialize progress bar
  if (verbose_extraction_logging) {
    progress_bar <- cli::cli_progress_bar(
      "Processing physicians",
      total = length(physician_id_list),
      format = "{cli::pb_spin} {cli::pb_current}/{cli::pb_total} | {cli::pb_percent} | ETA: {cli::pb_eta} | {cli::pb_rate}"
    )
  }
  
  # Process each chunk with global error handling
  for (chunk_index in seq_along(physician_chunks)) {
    current_chunk <- physician_chunks[[chunk_index]]
    
    if (verbose_extraction_logging) {
      logger::log_info("🔄 Processing chunk {chunk_index}/{total_chunks} ({length(current_chunk)} physicians)")
    }
    
    # Wrap chunk processing in tryCatch
    tryCatch({
      chunk_results <- dplyr::tibble(
        physician_id = numeric(0),
        abog_id_number = character(0),
        physician_name = character(0),
        clinically_active_status = character(0),
        primary_certification = character(0),
        primary_cert_date = character(0),
        primary_cert_status = character(0),
        primary_continuing_cert = character(0),
        subspecialty_name = character(0),
        subspecialty_cert_date = character(0),
        subspecialty_cert_status = character(0),
        subspecialty_continuing_cert = character(0),
        extraction_timestamp = as.POSIXct(character(0)),
        pdf_filename = character(0)
      )
      
      # Process each physician in the chunk
      for (physician_index_in_chunk in seq_along(current_chunk)) {
        current_physician_identifier <- current_chunk[physician_index_in_chunk]
        overall_physician_index <- (chunk_index - 1) * chunk_size_records + physician_index_in_chunk
        
        if (verbose_extraction_logging) {
          logger::log_info("🔍 Processing physician {overall_physician_index}/{length(physician_id_list)}: ID {current_physician_identifier}")
        }
        
        # Download and extract subspecialty data from PDF
        physician_subspecialty_data <- download_and_extract_physician_pdf_data(
          current_physician_identifier, session_config, output_directory_extractions, 
          cleanup_downloaded_pdfs, verbose_extraction_logging)
        
        # Create record (successful or failed)
        if (!is.null(physician_subspecialty_data)) {
          current_record <- physician_subspecialty_data
          successful_extraction_count <- successful_extraction_count + 1
          
          if (verbose_extraction_logging) {
            physician_name <- if (!is.na(physician_subspecialty_data$physician_name)) {
              physician_subspecialty_data$physician_name
            } else {
              "Unknown"
            }
            subspecialty_information <- if (!is.na(physician_subspecialty_data$subspecialty_name) && 
                                            physician_subspecialty_data$subspecialty_name != "") {
              physician_subspecialty_data$subspecialty_name
            } else {
              "No subspecialty"
            }
            logger::log_info("✅ Success: {physician_name} | {subspecialty_information}")
          }
        } else {
          # Create failed extraction record
          current_record <- dplyr::tibble(
            physician_id = current_physician_identifier,
            abog_id_number = NA_character_,
            physician_name = NA_character_,
            clinically_active_status = NA_character_,
            primary_certification = NA_character_,
            primary_cert_date = NA_character_,
            primary_cert_status = NA_character_,
            primary_continuing_cert = NA_character_,
            subspecialty_name = NA_character_,
            subspecialty_cert_date = NA_character_,
            subspecialty_cert_status = NA_character_,
            subspecialty_continuing_cert = NA_character_,
            extraction_timestamp = Sys.time(),
            pdf_filename = NA_character_
          )
          
          failed_extraction_count <- failed_extraction_count + 1
          
          if (verbose_extraction_logging) {
            logger::log_warn("❌ Failed extraction for ID {current_physician_identifier}")
          }
        }
        
        # Add record to chunk results
        chunk_results <- dplyr::bind_rows(chunk_results, current_record)
        
        # Update progress bar
        if (verbose_extraction_logging) {
          cli::cli_progress_update(id = progress_bar)
        }
        
        # Rate limiting (except for last physician in chunk)
        if (physician_index_in_chunk < length(current_chunk)) {
          Sys.sleep(request_delay_seconds)
        }
      }
      
      # Add chunk results to combined results
      combined_extraction_results <- dplyr::bind_rows(combined_extraction_results, chunk_results)
      
      # Save chunk file if requested
      if (save_individual_files) {
        save_chunk_results_file(chunk_results, chunk_index, 
                                output_directory_extractions, verbose_extraction_logging,
                                physician_id_list = current_chunk)
      }
      
      # Save updated combined file after each chunk
      save_combined_results_file(combined_extraction_results, 
                                 output_directory_extractions, verbose_extraction_logging,
                                 physician_id_list = physician_id_list)
      
      if (verbose_extraction_logging) {
        chunk_success_rate <- round((sum(!is.na(chunk_results$physician_name)) / nrow(chunk_results)) * 100, 1)
        logger::log_info("📦 Chunk {chunk_index} completed: {nrow(chunk_results)} records ({chunk_success_rate}% success)")
      }
      
    }, error = function(e) {
      failed_chunks <<- c(failed_chunks, chunk_index)
      if (verbose_extraction_logging) {
        logger::log_error("⚠️ Error in chunk {chunk_index}: {e$message}")
        logger::log_warn("🔄 Continuing with next chunk...")
      }
    })
  }
  
  # Complete progress bar
  if (verbose_extraction_logging) {
    cli::cli_progress_done(id = progress_bar)
  }
  
  # Final save of combined results
  final_combined_filepath <- save_combined_results_file(combined_extraction_results, 
                                                        output_directory_extractions, verbose_extraction_logging,
                                                        physician_id_list = physician_id_list)
  
  # Create extraction summary file
  summary_filepath <- create_extraction_summary_file(
    combined_extraction_results, output_directory_extractions, 
    physician_id_list, extraction_start_time,
    successful_extraction_count, failed_extraction_count, 
    failed_chunks, verbose_extraction_logging)
  
  # Generate final summary
  if (verbose_extraction_logging) {
    extraction_end_time <- Sys.time()
    extraction_duration <- difftime(extraction_end_time, extraction_start_time, units = "mins")
    
    subspecialty_training_count <- sum(!is.na(combined_extraction_results$subspecialty_name) & 
                                         combined_extraction_results$subspecialty_name != "", na.rm = TRUE)
    final_success_rate <- round((successful_extraction_count / length(physician_id_list)) * 100, 1)
    successful_chunks <- total_chunks - length(failed_chunks)
    
    summary_lines <- c(
      paste("⏰ Start time:", format(extraction_start_time, "%Y-%m-%d %H:%M:%S")),
      paste("⏰ End time:", format(extraction_end_time, "%Y-%m-%d %H:%M:%S")),
      paste("⏱️ Duration:", round(as.numeric(extraction_duration), 1), "minutes"),
      paste("🆔 Physician ID range:", min(physician_id_list), "to", max(physician_id_list)),
      paste("🧑‍⚕️ Total physicians:", length(physician_id_list)),
      paste("✅ Successful extractions:", successful_extraction_count),
      paste("❌ Failed extractions:", failed_extraction_count),
      paste("📊 Success rate:", paste0(final_success_rate, "%")),
      paste("🏥 Physicians with subspecialty:", subspecialty_training_count),
      paste("📦 Total chunks:", total_chunks),
      paste("✅ Successful chunks:", successful_chunks),
      paste("❌ Failed chunks:", length(failed_chunks)),
      paste("📂 Output folder:", output_directory_extractions),
      paste("📄 Combined file:", basename(final_combined_filepath)),
      paste("📋 Summary file:", basename(summary_filepath))
    )
    
    if (length(failed_chunks) > 0) {
      summary_lines <- c(summary_lines, paste("⚠️ Failed chunk numbers:", paste(failed_chunks, collapse = ", ")))
    }
    
    # Log summary
    logger::log_info("🏁 ABOG PDF subspecialty extraction completed")
    logger::log_info(paste(rep("=", 60), collapse = ""))
    for (line in summary_lines) {
      logger::log_info(line)
    }
    logger::log_info(paste(rep("=", 60), collapse = ""))
    
    if (save_individual_files) {
      logger::log_info("📁 Individual chunk files saved in: {output_directory_extractions}")
    }
    
    if (use_proxy_requests) {
      logger::log_info("🔐 All requests routed through Tor proxy")
    }
  }
  
  return(combined_extraction_results)
}

# Helper Functions

#' @noRd
test_tor_connectivity <- function(proxy_url, verbose_logging) {
  if (verbose_logging) {
    logger::log_info("🧪 Testing Tor connectivity...")
  }
  
  tryCatch({
    # Test with a simple HTTP request through the proxy
    test_request <- httr2::request("http://httpbin.org/ip")
    
    # Configure proxy
    if (stringr::str_detect(proxy_url, "socks5://")) {
      test_request <- httr2::req_proxy(test_request, url = proxy_url)
    }
    
    # Set short timeout for test
    test_request <- httr2::req_timeout(test_request, 10)
    
    # Perform test request
    test_response <- httr2::req_perform(test_request)
    
    if (httr2::resp_status(test_response) == 200) {
      response_body <- httr2::resp_body_string(test_response)
      
      if (verbose_logging) {
        logger::log_info("✅ Tor connectivity test successful")
        logger::log_info("🌐 IP response: {substr(response_body, 1, 100)}...")
      }
      return(TRUE)
    } else {
      if (verbose_logging) {
        logger::log_warn("⚠️ Tor test got HTTP {httr2::resp_status(test_response)}")
      }
      return(FALSE)
    }
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_warn("❌ Tor connectivity test failed: {e$message}")
      
      # Provide helpful troubleshooting info
      if (stringr::str_detect(e$message, "connection|refused|timeout")) {
        logger::log_info("💡 Troubleshooting tips:")
        logger::log_info("   • Check if Tor is running: ps aux | grep tor")
        logger::log_info("   • Standard Tor port: 9050, Tor Browser: 9150")
        logger::log_info("   • Start Tor: brew install tor && tor")
      }
    }
    return(FALSE)
  })
}

#' @noRd
save_combined_results_file <- function(combined_data, output_directory, verbose_logging, 
                                       physician_id_list = NULL) {
  if (nrow(combined_data) > 0) {
    # Create timestamp
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    
    # Determine ID range from data or input list
    if (!is.null(physician_id_list) && length(physician_id_list) > 0) {
      # Use the original input list for accurate range
      min_id <- min(physician_id_list, na.rm = TRUE)
      max_id <- max(physician_id_list, na.rm = TRUE)
    } else {
      # Fall back to data if input list not available
      min_id <- min(combined_data$physician_id, na.rm = TRUE)
      max_id <- max(combined_data$physician_id, na.rm = TRUE)
    }
    
    # Format ID range
    if (!is.na(min_id) && !is.na(max_id)) {
      if (min_id == max_id) {
        id_range <- paste0("ID", min_id)
      } else {
        id_range <- paste0("ID", min_id, "_to_", max_id)
      }
    } else {
      id_range <- "IDunknown"
    }
    
    # Create comprehensive filename
    combined_filename <- paste0("combined_subspecialty_extractions_", 
                                timestamp, "_", 
                                id_range, "_",
                                nrow(combined_data), "records.csv")
    
    combined_filepath <- file.path(output_directory, combined_filename)
    
    readr::write_csv(combined_data, combined_filepath)
    
    if (verbose_logging) {
      logger::log_info("💾 Saved combined results: {combined_filename}")
      logger::log_info("   📊 {nrow(combined_data)} total records")
      logger::log_info("   🆔 Physician ID range: {min_id} to {max_id}")
    }
    
    return(combined_filepath)
  }
  return(NULL)
}

#' @noRd
save_chunk_results_file <- function(chunk_data, chunk_number, output_directory, 
                                    verbose_logging, physician_id_list = NULL) {
  if (nrow(chunk_data) > 0) {
    # Create timestamp
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    
    # Determine ID range for this chunk
    chunk_min_id <- min(chunk_data$physician_id, na.rm = TRUE)
    chunk_max_id <- max(chunk_data$physician_id, na.rm = TRUE)
    
    # Format chunk ID range
    if (!is.na(chunk_min_id) && !is.na(chunk_max_id)) {
      if (chunk_min_id == chunk_max_id) {
        chunk_id_range <- paste0("ID", chunk_min_id)
      } else {
        chunk_id_range <- paste0("ID", chunk_min_id, "_to_", chunk_max_id)
      }
    } else {
      chunk_id_range <- "IDunknown"
    }
    
    # Create detailed chunk filename
    chunk_filename <- paste0("chunk_", sprintf("%03d", chunk_number), 
                             "_subspecialty_extractions_", 
                             timestamp, "_",
                             chunk_id_range, "_",
                             nrow(chunk_data), "records.csv")
    
    chunk_filepath <- file.path(output_directory, chunk_filename)
    
    readr::write_csv(chunk_data, chunk_filepath)
    
    if (verbose_logging) {
      logger::log_info("📄 Saved chunk {chunk_number}: {chunk_filename}")
      logger::log_info("   📊 {nrow(chunk_data)} records in chunk")
      logger::log_info("   🆔 Chunk ID range: {chunk_min_id} to {chunk_max_id}")
    }
    
    return(chunk_filepath)
  }
  return(NULL)
}

#' @noRd
create_extraction_summary_file <- function(combined_data, output_directory, 
                                           physician_id_list, extraction_start_time,
                                           successful_count, failed_count, 
                                           failed_chunks, verbose_logging) {
  
  # Create summary timestamp
  summary_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  # Calculate extraction duration
  extraction_duration <- difftime(Sys.time(), extraction_start_time, units = "mins")
  
  # Determine ID range
  if (!is.null(physician_id_list) && length(physician_id_list) > 0) {
    min_id <- min(physician_id_list, na.rm = TRUE)
    max_id <- max(physician_id_list, na.rm = TRUE)
    id_range <- paste0("ID", min_id, "_to_", max_id)
  } else {
    id_range <- "IDunknown"
  }
  
  # Create summary data
  summary_data <- dplyr::tibble(
    extraction_timestamp = summary_timestamp,
    extraction_start_time = format(extraction_start_time, "%Y-%m-%d %H:%M:%S"),
    extraction_end_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    extraction_duration_minutes = round(as.numeric(extraction_duration), 2),
    total_physicians_requested = length(physician_id_list),
    physician_id_range = id_range,
    min_physician_id = if (!is.null(physician_id_list)) min(physician_id_list, na.rm = TRUE) else NA,
    max_physician_id = if (!is.null(physician_id_list)) max(physician_id_list, na.rm = TRUE) else NA,
    total_records_extracted = nrow(combined_data),
    successful_extractions = successful_count,
    failed_extractions = failed_count,
    success_rate_percent = round((successful_count / length(physician_id_list)) * 100, 2),
    physicians_with_subspecialty = sum(!is.na(combined_data$subspecialty_name) & 
                                         combined_data$subspecialty_name != "", na.rm = TRUE),
    subspecialty_rate_percent = round((sum(!is.na(combined_data$subspecialty_name) & 
                                             combined_data$subspecialty_name != "", na.rm = TRUE) / 
                                         nrow(combined_data)) * 100, 2),
    failed_chunk_numbers = if (length(failed_chunks) > 0) paste(failed_chunks, collapse = ", ") else "None",
    output_directory = output_directory
  )
  
  # Create summary filename
  summary_filename <- paste0("extraction_summary_", 
                             summary_timestamp, "_",
                             id_range, ".csv")
  
  summary_filepath <- file.path(output_directory, summary_filename)
  
  # Save summary
  readr::write_csv(summary_data, summary_filepath)
  
  if (verbose_logging) {
    logger::log_info("📋 Saved extraction summary: {summary_filename}")
    logger::log_info("   ⏱️ Duration: {round(as.numeric(extraction_duration), 1)} minutes")
    logger::log_info("   📊 Success rate: {summary_data$success_rate_percent}%")
    logger::log_info("   🏥 Subspecialty rate: {summary_data$subspecialty_rate_percent}%")
  }
  
  return(summary_filepath)
}

#' @noRd
download_and_extract_physician_pdf_data <- function(physician_id, session_config, 
                                                    output_directory, cleanup_pdfs, verbose_logging) {
  tryCatch({
    pdf_api_url <- paste0("https://api.abog.org/report/CertStatusLetter/", physician_id)
    
    if (verbose_logging) {
      logger::log_info("  📡 Accessing ABOG API for ID {physician_id}")
      logger::log_info("  🔗 PDF URL: {pdf_api_url}")
    }
    
    pdf_file_path <- download_certification_pdf(
      pdf_api_url, physician_id, output_directory, session_config, verbose_logging)
    
    if (!is.null(pdf_file_path) && file.exists(pdf_file_path)) {
      if (verify_pdf_file(pdf_file_path, verbose_logging)) {
        if (verbose_logging) {
          logger::log_info("    ✅ PDF verification successful")
        }
        
        extracted_physician_data <- extract_subspecialty_data_from_pdf(
          pdf_file_path, physician_id, verbose_logging)
        
        if (cleanup_pdfs && file.exists(pdf_file_path)) {
          file.remove(pdf_file_path)
          if (verbose_logging) {
            logger::log_info("    🗑️ Cleaned up PDF file: {basename(pdf_file_path)}")
          }
        }
        
        return(extracted_physician_data)
      } else {
        if (verbose_logging) {
          logger::log_warn("    ⚠️ PDF verification failed - checking for error response")
        }
        
        error_content <- readLines(pdf_file_path, warn = FALSE)
        error_text <- paste(error_content, collapse = "\n")
        
        if (verbose_logging) {
          logger::log_info("    📄 Response preview: {substr(error_text, 1, 100)}...")
        }
        
        if (stringr::str_detect(error_text, "(?i)(error|not found|unauthorized|access denied|invalid)")) {
          if (verbose_logging) {
            logger::log_warn("    ❌ API returned error response for physician {physician_id}")
          }
        }
        
        if (cleanup_pdfs && file.exists(pdf_file_path)) {
          file.remove(pdf_file_path)
        }
        
        return(NULL)
      }
    } else {
      if (verbose_logging) {
        logger::log_warn("  ❌ Failed to download PDF from API for physician {physician_id}")
      }
      return(NULL)
    }
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("  ❌ Processing error for physician {physician_id}: {e$message}")
    }
    return(NULL)
  })
}

#' @noRd
download_certification_pdf <- function(pdf_url, physician_id, output_directory, 
                                       session_config, verbose_logging) {
  tryCatch({
    pdf_request <- httr2::request(pdf_url)
    pdf_request <- httr2::req_headers(pdf_request, 
                                      "User-Agent" = session_config$user_agent,
                                      "Accept" = "application/pdf,application/octet-stream,*/*",
                                      "Accept-Language" = "en-US,en;q=0.5",
                                      "Accept-Encoding" = "gzip, deflate",
                                      "Connection" = "keep-alive",
                                      "Referer" = paste0("https://www.abog.org/verify-physician?physid=", physician_id)
    )
    
    # Add proxy configuration if enabled
    if (session_config$use_proxy && !is.null(session_config$proxy_url)) {
      if (verbose_logging) {
        logger::log_info("    🔐 Configuring Tor proxy: {session_config$proxy_url}")
      }
      
      # Configure proxy for httr2 with multiple approaches
      tryCatch({
        if (stringr::str_detect(session_config$proxy_url, "socks5://")) {
          # Extract host and port for SOCKS5
          proxy_host <- stringr::str_extract(session_config$proxy_url, "(?<=://).+?(?=:)")
          proxy_port <- as.numeric(stringr::str_extract(session_config$proxy_url, "(?<=:)[0-9]+$"))
          
          if (verbose_logging) {
            logger::log_info("    📡 SOCKS5 proxy: {proxy_host}:{proxy_port}")
          }
          
          # Method 1: Try httr2's req_proxy with full URL
          pdf_request <- httr2::req_proxy(pdf_request, url = session_config$proxy_url)
          
          if (verbose_logging) {
            logger::log_info("    ✅ SOCKS5 proxy configured successfully")
          }
          
        } else if (stringr::str_detect(session_config$proxy_url, "http://")) {
          # HTTP proxy configuration
          pdf_request <- httr2::req_proxy(pdf_request, url = session_config$proxy_url)
          
          if (verbose_logging) {
            logger::log_info("    ✅ HTTP proxy configured successfully")
          }
        }
        
      }, error = function(proxy_error) {
        if (verbose_logging) {
          logger::log_warn("    ⚠️ Primary proxy method failed: {proxy_error$message}")
          logger::log_info("    🔄 Trying alternative proxy configuration...")
        }
        
        # Fallback method: Try with httr2 options
        tryCatch({
          if (stringr::str_detect(session_config$proxy_url, "socks5://")) {
            proxy_host <- stringr::str_extract(session_config$proxy_url, "(?<=://).+?(?=:)")
            proxy_port <- as.numeric(stringr::str_extract(session_config$proxy_url, "(?<=:)[0-9]+$"))
            
            # Alternative: Use req_options with curl proxy settings
            pdf_request <- httr2::req_options(pdf_request,
                                              proxy = paste0(proxy_host, ":", proxy_port),
                                              proxytype = 7L  # CURLPROXY_SOCKS5 = 7
            )
            
            if (verbose_logging) {
              logger::log_info("    ✅ Alternative SOCKS5 proxy configured")
            }
          }
          
        }, error = function(fallback_error) {
          if (verbose_logging) {
            logger::log_error("    ❌ All proxy methods failed:")
            logger::log_error("    Primary error: {proxy_error$message}")
            logger::log_error("    Fallback error: {fallback_error$message}")
            logger::log_warn("    🚨 Proceeding WITHOUT proxy - check Tor setup!")
          }
        })
      })
    }
    
    pdf_request <- httr2::req_options(pdf_request, followlocation = TRUE)
    pdf_response <- httr2::req_perform(pdf_request)
    
    if (httr2::resp_status(pdf_response) == 200) {
      content_type <- httr2::resp_header(pdf_response, "content-type")
      
      if (verbose_logging) {
        logger::log_info("    📄 Response content type: {content_type}")
      }
      
      response_content <- httr2::resp_body_raw(pdf_response)
      file_extension <- if (stringr::str_detect(content_type, "pdf")) ".pdf" else ".html"
      download_filename <- paste0("physician_", physician_id, "_cert_", 
                                  format(Sys.time(), "%Y%m%d_%H%M%S"), file_extension)
      download_filepath <- file.path(output_directory, download_filename)
      
      writeBin(response_content, download_filepath)
      
      if (verbose_logging) {
        size_kb <- round(length(response_content) / 1024, 1)
        logger::log_info("    💾 Downloaded: {download_filename} ({size_kb} KB)")
      }
      
      return(download_filepath)
    } else {
      if (verbose_logging) {
        logger::log_warn("    ❌ Download failed: HTTP {httr2::resp_status(pdf_response)}")
      }
      return(NULL)
    }
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("    ❌ Download error: {e$message}")
    }
    return(NULL)
  })
}

#' @noRd
verify_pdf_file <- function(file_path, verbose_logging) {
  tryCatch({
    file_info <- file.info(file_path)
    if (file_info$size < 50) {
      if (verbose_logging) {
        logger::log_warn("  File too small to be a valid PDF ({file_info$size} bytes)")
      }
      return(FALSE)
    }
    
    file_connection <- file(file_path, "rb")
    file_header <- readBin(file_connection, "raw", n = 10)
    close(file_connection)
    
    pdf_signature <- charToRaw("%PDF")
    is_pdf <- length(file_header) >= 4 && identical(file_header[1:4], pdf_signature)
    
    if (verbose_logging) {
      if (is_pdf) {
        logger::log_info("  File verified as valid PDF")
      } else {
        header_text <- tryCatch({
          rawToChar(file_header[1:min(20, length(file_header))])
        }, error = function(e) {
          paste(as.character(file_header[1:min(10, length(file_header))]), collapse = " ")
        })
        logger::log_info("  File header indicates HTML/other format: {substr(header_text, 1, 50)}")
      }
    }
    
    return(is_pdf)
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("  Error verifying file format: {e$message}")
    }
    return(FALSE)
  })
}

#' @noRd
extract_subspecialty_data_from_pdf <- function(pdf_file_path, physician_id, verbose_logging) {
  tryCatch({
    pdf_text_content <- pdftools::pdf_text(pdf_file_path)
    pdf_combined_text <- paste(pdf_text_content, collapse = " ")
    
    if (verbose_logging) {
      char_count <- nchar(pdf_combined_text)
      logger::log_info("    📖 Extracted text from PDF ({char_count} characters)")
    }
    
    # Extract all information using helper functions
    physician_name_extracted <- extract_physician_name_from_text(pdf_combined_text)
    abog_id_extracted <- extract_abog_id_from_text(pdf_combined_text)
    clinically_active_extracted <- extract_clinically_active_status_from_text(pdf_combined_text)
    primary_certification_name <- extract_primary_certification_from_text(pdf_combined_text)
    primary_certification_date <- extract_primary_cert_date_from_text(pdf_combined_text)
    primary_certification_status <- extract_primary_cert_status_from_text(pdf_combined_text)
    primary_continuing_certification <- extract_primary_continuing_cert_from_text(pdf_combined_text)
    subspecialty_certification_name <- extract_subspecialty_name_from_pdf_text(pdf_combined_text)
    subspecialty_certification_date <- extract_subspecialty_cert_date_from_pdf_text(pdf_combined_text)
    subspecialty_certification_status <- extract_subspecialty_cert_status_from_pdf_text(pdf_combined_text)
    subspecialty_continuing_certification <- extract_subspecialty_continuing_cert_from_pdf_text(pdf_combined_text)
    
    if (verbose_logging) {
      name_info <- if (!is.na(physician_name_extracted)) physician_name_extracted else "Unknown"
      subspecialty_info <- if (!is.na(subspecialty_certification_name)) subspecialty_certification_name else "None"
      logger::log_info("    👤 Physician: {name_info}")
      logger::log_info("    🏥 Subspecialty: {subspecialty_info}")
    }
    
    # Create result data frame
    physician_extracted_data <- dplyr::tibble(
      physician_id = physician_id,
      abog_id_number = stringr::str_trim(abog_id_extracted),
      physician_name = stringr::str_trim(physician_name_extracted),
      clinically_active_status = stringr::str_trim(clinically_active_extracted),
      primary_certification = stringr::str_trim(primary_certification_name),
      primary_cert_date = stringr::str_trim(primary_certification_date),
      primary_cert_status = stringr::str_trim(primary_certification_status),
      primary_continuing_cert = stringr::str_trim(primary_continuing_certification),
      subspecialty_name = stringr::str_trim(subspecialty_certification_name),
      subspecialty_cert_date = stringr::str_trim(subspecialty_certification_date),
      subspecialty_cert_status = stringr::str_trim(subspecialty_certification_status),
      subspecialty_continuing_cert = stringr::str_trim(subspecialty_continuing_certification),
      extraction_timestamp = Sys.time(),
      pdf_filename = basename(pdf_file_path)
    )
    
    return(physician_extracted_data)
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("    ❌ PDF extraction error: {e$message}")
    }
    return(NULL)
  })
}

# Text Extraction Helper Functions

#' @noRd
extract_abog_id_from_text <- function(pdf_text) {
  abog_id_pattern <- stringr::str_extract(pdf_text,
                                          "(?i)abog id number:\\s*([0-9]+)")
  
  if (!is.na(abog_id_pattern)) {
    id_extracted <- stringr::str_replace(abog_id_pattern, "(?i)abog id number:\\s*", "")
    return(stringr::str_trim(id_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_clinically_active_status_from_text <- function(pdf_text) {
  active_status_pattern <- stringr::str_extract(pdf_text,
                                                "(?i)clinically active:\\s*(yes|no)")
  
  if (!is.na(active_status_pattern)) {
    status_extracted <- stringr::str_replace(active_status_pattern, "(?i)clinically active:\\s*", "")
    return(stringr::str_trim(status_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_physician_name_from_text <- function(pdf_text) {
  name_pattern <- stringr::str_extract(pdf_text, 
                                       "(?i)(?:RE:\\s*)?certification status of ([^,\\n\\r]+(?:,\\s*MD)?)")
  
  if (!is.na(name_pattern)) {
    name_extracted <- stringr::str_replace(name_pattern, "(?i)(?:RE:\\s*)?certification status of\\s*", "")
    name_extracted <- stringr::str_replace(name_extracted, ",\\s*MD$", "")
    return(stringr::str_trim(name_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_primary_certification_from_text <- function(pdf_text) {
  return(stringr::str_extract(pdf_text, 
                              "(?i)(obstetrics and gynecology|family medicine|internal medicine)"))
}

#' @noRd
extract_primary_cert_date_from_text <- function(pdf_text) {
  date_pattern <- stringr::str_extract(pdf_text,
                                       "(?i)original certification date:\\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
  
  if (!is.na(date_pattern)) {
    date_extracted <- stringr::str_replace(date_pattern, "(?i)original certification date:\\s*", "")
    return(stringr::str_trim(date_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_primary_cert_status_from_text <- function(pdf_text) {
  status_pattern <- stringr::str_extract(pdf_text,
                                         "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
  
  if (!is.na(status_pattern)) {
    status_extracted <- stringr::str_replace(status_pattern, "(?i)certification status:\\s*", "")
    return(stringr::str_trim(status_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_primary_continuing_cert_from_text <- function(pdf_text) {
  primary_continuing_pattern <- stringr::str_extract(pdf_text,
                                                     "(?i)(?:obstetrics and gynecology).*?participating in continuing certification:\\s*(yes|no)")
  
  if (!is.na(primary_continuing_pattern)) {
    cert_extracted <- stringr::str_extract(primary_continuing_pattern,
                                           "(?i)participating in continuing certification:\\s*(yes|no)")
    cert_cleaned <- stringr::str_replace(cert_extracted, "(?i)participating in continuing certification:\\s*", "")
    return(stringr::str_trim(cert_cleaned))
  }
  
  general_continuing_pattern <- stringr::str_extract(pdf_text,
                                                     "(?i)participating in continuing certification:\\s*(yes|no)")
  
  if (!is.na(general_continuing_pattern)) {
    cert_extracted <- stringr::str_replace(general_continuing_pattern, "(?i)participating in continuing certification:\\s*", "")
    return(stringr::str_trim(cert_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_subspecialty_name_from_pdf_text <- function(pdf_text) {
  abog_subspecialty_list <- c(
    "Urogynecology and Reconstructive Pelvic Surgery",
    "Female Pelvic Medicine and Reconstructive Surgery",
    "Gynecologic Oncology",
    "Maternal-Fetal Medicine", 
    "Reproductive Endocrinology and Infertility",
    "Complex Family Planning",
    "Critical Care Medicine",
    "Hospice and Palliative Medicine"
  )
  
  for (subspecialty_name in abog_subspecialty_list) {
    if (stringr::str_detect(pdf_text, stringr::fixed(subspecialty_name, ignore_case = TRUE))) {
      return(subspecialty_name)
    }
  }
  return(NA_character_)
}

#' @noRd
extract_subspecialty_cert_date_from_pdf_text <- function(pdf_text) {
  subspecialty_date_pattern <- stringr::str_extract(pdf_text,
                                                    "(?i)original subspecialty certification date:\\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
  
  if (!is.na(subspecialty_date_pattern)) {
    date_extracted <- stringr::str_replace(subspecialty_date_pattern, "(?i)original subspecialty certification date:\\s*", "")
    return(stringr::str_trim(date_extracted))
  }
  return(NA_character_)
}

#' @noRd
extract_subspecialty_cert_status_from_pdf_text <- function(pdf_text) {
  # Look for the second occurrence of "Certification Status" (after subspecialty section)
  # Split text into lines to better handle the structure
  text_lines <- strsplit(pdf_text, "\\n")[[1]]
  
  # Find the subspecialty section and look for certification status after it
  subspecialty_found <- FALSE
  for (i in seq_along(text_lines)) {
    line <- text_lines[i]
    
    # Check if we've found a subspecialty section
    if (stringr::str_detect(line, "(?i)(urogynecology|gynecologic oncology|maternal-fetal|reproductive endocrinology|complex family planning|critical care|hospice)")) {
      subspecialty_found <- TRUE
      next
    }
    
    # If we're in subspecialty section, look for certification status
    if (subspecialty_found && stringr::str_detect(line, "(?i)certification status:")) {
      status_match <- stringr::str_extract(line, "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
      if (!is.na(status_match)) {
        status_cleaned <- stringr::str_replace(status_match, "(?i)certification status:\\s*", "")
        return(stringr::str_trim(status_cleaned))
      }
    }
  }
  
  # Alternative approach: look for all certification status patterns and take the second one
  all_cert_status <- stringr::str_extract_all(pdf_text, 
                                              "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")[[1]]
  
  if (length(all_cert_status) >= 2) {
    # Take the second occurrence (subspecialty)
    status_cleaned <- stringr::str_replace(all_cert_status[2], "(?i)certification status:\\s*", "")
    return(stringr::str_trim(status_cleaned))
  }
  
  return(NA_character_)
}

#' @noRd
extract_subspecialty_continuing_cert_from_pdf_text <- function(pdf_text) {
  # Look for the second occurrence of "Participating in Continuing Certification"
  # Split text into lines to better handle the structure
  text_lines <- strsplit(pdf_text, "\\n")[[1]]
  
  # Find the subspecialty section and look for continuing cert after it
  subspecialty_found <- FALSE
  for (i in seq_along(text_lines)) {
    line <- text_lines[i]
    
    # Check if we've found a subspecialty section
    if (stringr::str_detect(line, "(?i)(urogynecology|gynecologic oncology|maternal-fetal|reproductive endocrinology|complex family planning|critical care|hospice)")) {
      subspecialty_found <- TRUE
      next
    }
    
    # If we're in subspecialty section, look for continuing certification
    if (subspecialty_found && stringr::str_detect(line, "(?i)participating in continuing certification:")) {
      cert_match <- stringr::str_extract(line, "(?i)participating in continuing certification:\\s*(yes|no)")
      if (!is.na(cert_match)) {
        cert_cleaned <- stringr::str_replace(cert_match, "(?i)participating in continuing certification:\\s*", "")
        return(stringr::str_trim(cert_cleaned))
      }
    }
  }
  
  # Alternative approach: look for all continuing cert patterns and take the second one
  all_continuing_cert <- stringr::str_extract_all(pdf_text, 
                                                  "(?i)participating in continuing certification:\\s*(yes|no)")[[1]]
  
  if (length(all_continuing_cert) >= 2) {
    # Take the second occurrence (subspecialty)
    cert_cleaned <- stringr::str_replace(all_continuing_cert[2], "(?i)participating in continuing certification:\\s*", "")
    return(stringr::str_trim(cert_cleaned))
  }
  
  return(NA_character_)
}

# run ----
# Run the full extraction with optimal settings
large_scale_results <- extract_subspecialty_from_pdf_letters(
  physician_id_list = next_ids,                    
  use_proxy_requests = TRUE,
  proxy_url_requests = "socks5://127.0.0.1:9150",
  output_directory_extractions = "physician_data/abog_large_scale_2025",
  request_delay_seconds = 1.0,
  chunk_size_records = 100,
  cleanup_downloaded_pdfs = FALSE,
  save_individual_files = TRUE,
  verbose_extraction_logging = TRUE
)


#' # Where to search?  -----
#' #' Smart ABOG Physician ID Search Tracking with Non-Existent ID Filtering
#' #' 
#' #' This improved system tracks which IDs exist vs don't exist, and excludes
#' #' non-existent IDs from future searches automatically.
#' 
#' #' Analyze Search Results with Smart ID Classification
#' #'
#' #' @param results_directory character. Directory containing extraction results
#' #' @param verbose logical. Enable detailed logging
#' #' @param save_exclusion_list logical. Save non-existent IDs to file
#' #'
#' #' @return list containing smart search analysis and recommendations
#' #'
#' #' @examples
#' #' # Smart analysis that tracks non-existent IDs
#' #' smart_analysis <- analyze_search_results_smart(
#' #'   results_directory = "physician_data/abog_large_scale_2025",
#' #'   verbose = TRUE,
#' #'   save_exclusion_list = TRUE
#' #' )
#' #'
#' #' @importFrom readr read_csv write_csv
#' #' @importFrom dplyr bind_rows arrange distinct summarise group_by filter
#' #' @importFrom logger log_info log_warn
#' #' @export
#' analyze_search_results_smart <- function(results_directory, 
#'                                          verbose = TRUE,
#'                                          save_exclusion_list = TRUE) {
#'   
#'   if (verbose) {
#'     logger::log_info("🧠 Smart analysis of ABOG search results...")
#'   }
#'   
#'   # Find all extraction result files
#'   all_csv_files <- list.files(
#'     results_directory, 
#'     pattern = "(combined_subspecialty_extractions|chunk_.*_subspecialty_extractions).*\\.csv$", 
#'     recursive = TRUE, 
#'     full.names = TRUE
#'   )
#'   
#'   if (length(all_csv_files) == 0) {
#'     if (verbose) {
#'       logger::log_warn("⚠️ No extraction result files found in {results_directory}")
#'     }
#'     return(NULL)
#'   }
#'   
#'   if (verbose) {
#'     logger::log_info("📁 Found {length(all_csv_files)} result files")
#'   }
#'   
#'   # Read and combine all results
#'   all_search_results <- list()
#'   
#'   for (csv_file in all_csv_files) {
#'     tryCatch({
#'       file_data <- readr::read_csv(csv_file, show_col_types = FALSE)
#'       
#'       # Extract search info from filename
#'       file_info <- extract_search_info_from_filename(basename(csv_file))
#'       
#'       # Add file metadata to data
#'       file_data$source_file <- basename(csv_file)
#'       file_data$extraction_date <- file_info$extraction_date
#'       file_data$extraction_time <- file_info$extraction_time
#'       
#'       all_search_results[[length(all_search_results) + 1]] <- file_data
#'       
#'       if (verbose) {
#'         logger::log_info("✅ Loaded {nrow(file_data)} records from {basename(csv_file)}")
#'       }
#'       
#'     }, error = function(e) {
#'       if (verbose) {
#'         logger::log_warn("❌ Failed to read {basename(csv_file)}: {e$message}")
#'       }
#'     })
#'   }
#'   
#'   if (length(all_search_results) == 0) {
#'     if (verbose) {
#'       logger::log_warn("⚠️ No valid data found in result files")
#'     }
#'     return(NULL)
#'   }
#'   
#'   # Combine all results
#'   combined_results <- dplyr::bind_rows(all_search_results)
#'   
#'   # Smart classification of physician IDs
#'   id_classification <- classify_physician_ids_smart(combined_results, verbose)
#'   
#'   # Create smart search analysis
#'   smart_analysis <- create_smart_search_analysis(combined_results, id_classification, verbose)
#'   
#'   # Save exclusion lists if requested
#'   if (save_exclusion_list) {
#'     save_exclusion_lists(id_classification, results_directory, verbose)
#'   }
#'   
#'   # Generate smart recommendations
#'   smart_recommendations <- generate_smart_recommendations(smart_analysis, id_classification, verbose)
#'   
#'   return(list(
#'     smart_analysis = smart_analysis,
#'     id_classification = id_classification,
#'     recommendations = smart_recommendations,
#'     combined_data = combined_results
#'   ))
#' }
#' 
#' #' @noRd
#' classify_physician_ids_smart <- function(combined_data, verbose) {
#'   
#'   if (verbose) {
#'     logger::log_info("🔍 Classifying physician IDs by existence and success...")
#'   }
#'   
#'   # Group by physician_id and analyze each
#'   id_summary <- combined_data %>%
#'     dplyr::group_by(physician_id) %>%
#'     dplyr::summarise(
#'       search_count = dplyr::n(),
#'       has_name = any(!is.na(physician_name)),
#'       has_valid_data = any(!is.na(physician_name) & physician_name != ""),
#'       has_subspecialty = any(!is.na(subspecialty_name) & subspecialty_name != ""),
#'       latest_search_date = max(extraction_timestamp, na.rm = TRUE),
#'       .groups = "drop"
#'     )
#'   
#'   # Classify IDs based on search results
#'   id_classification <- list(
#'     # IDs that exist and have valid physician data
#'     valid_existing = id_summary$physician_id[id_summary$has_valid_data],
#'     
#'     # IDs that were searched but returned no valid physician data (likely don't exist)
#'     non_existent = id_summary$physician_id[!id_summary$has_valid_data & id_summary$search_count >= 1],
#'     
#'     # IDs that exist but have minimal data (exist but incomplete records)
#'     minimal_data = id_summary$physician_id[id_summary$has_name & !id_summary$has_valid_data],
#'     
#'     # IDs with subspecialty training
#'     with_subspecialty = id_summary$physician_id[id_summary$has_subspecialty],
#'     
#'     # All searched IDs
#'     all_searched = sort(unique(combined_data$physician_id))
#'   )
#'   
#'   # Additional analysis
#'   id_classification$summary <- list(
#'     total_searched = length(id_classification$all_searched),
#'     valid_physicians = length(id_classification$valid_existing),
#'     non_existent_count = length(id_classification$non_existent),
#'     subspecialty_count = length(id_classification$with_subspecialty),
#'     existence_rate = round((length(id_classification$valid_existing) / 
#'                               length(id_classification$all_searched)) * 100, 2),
#'     subspecialty_rate = round((length(id_classification$with_subspecialty) / 
#'                                  length(id_classification$valid_existing)) * 100, 2)
#'   )
#'   
#'   if (verbose) {
#'     logger::log_info("📊 ID Classification Results:")
#'     logger::log_info("   ✅ Valid physicians: {formatC(id_classification$summary$valid_physicians, big.mark = ',', format = 'd')}")
#'     logger::log_info("   ❌ Non-existent IDs: {formatC(id_classification$summary$non_existent_count, big.mark = ',', format = 'd')}")
#'     logger::log_info("   🏥 With subspecialty: {formatC(id_classification$summary$subspecialty_count, big.mark = ',', format = 'd')}")
#'     logger::log_info("   📈 Existence rate: {id_classification$summary$existence_rate}%")
#'     logger::log_info("   📈 Subspecialty rate: {id_classification$summary$subspecialty_rate}%")
#'   }
#'   
#'   return(id_classification)
#' }
#' 
#' #' @noRd
#' create_smart_search_analysis <- function(combined_data, id_classification, verbose) {
#'   
#'   # Get ranges for different ID types
#'   valid_ids <- sort(id_classification$valid_existing)
#'   non_existent_ids <- sort(id_classification$non_existent)
#'   all_searched_ids <- sort(id_classification$all_searched)
#'   
#'   # Calculate search coverage excluding known non-existent IDs
#'   if (length(all_searched_ids) > 0) {
#'     min_searched <- min(all_searched_ids)
#'     max_searched <- max(all_searched_ids)
#'     
#'     # IDs in the searched range that we haven't tried yet
#'     full_searched_range <- min_searched:max_searched
#'     unsearched_in_range <- setdiff(full_searched_range, all_searched_ids)
#'     
#'     # IDs in range that we know don't exist
#'     non_existent_in_range <- intersect(full_searched_range, non_existent_ids)
#'     
#'     # IDs that are actually missing (haven't searched AND not known non-existent)
#'     truly_missing_ids <- setdiff(unsearched_in_range, non_existent_ids)
#'     
#'   } else {
#'     min_searched <- NA
#'     max_searched <- NA
#'     unsearched_in_range <- c()
#'     non_existent_in_range <- c()
#'     truly_missing_ids <- c()
#'   }
#'   
#'   # Analyze ID density patterns
#'   density_analysis <- analyze_id_density_patterns(valid_ids, verbose)
#'   
#'   smart_analysis <- list(
#'     # Basic coverage
#'     total_searches = nrow(combined_data),
#'     unique_ids_searched = length(all_searched_ids),
#'     min_id_searched = min_searched,
#'     max_id_searched = max_searched,
#'     
#'     # Smart classification
#'     valid_physician_count = length(valid_ids),
#'     non_existent_count = length(non_existent_ids),
#'     existence_rate = id_classification$summary$existence_rate,
#'     
#'     # Gap analysis (excluding known non-existent)
#'     unsearched_in_range = unsearched_in_range,
#'     non_existent_in_range = non_existent_in_range,
#'     truly_missing_ids = truly_missing_ids,
#'     truly_missing_count = length(truly_missing_ids),
#'     
#'     # Subspecialty analysis
#'     subspecialty_count = length(id_classification$with_subspecialty),
#'     subspecialty_rate = id_classification$summary$subspecialty_rate,
#'     
#'     # Density patterns
#'     density_analysis = density_analysis,
#'     
#'     # ID lists for reference
#'     valid_ids = valid_ids,
#'     non_existent_ids = non_existent_ids
#'   )
#'   
#'   if (verbose) {
#'     logger::log_info("🧠 Smart Search Analysis Complete:")
#'     logger::log_info("   📊 Total searches: {formatC(smart_analysis$total_searches, big.mark = ',', format = 'd')}")
#'     logger::log_info("   ✅ Valid physicians found: {formatC(smart_analysis$valid_physician_count, big.mark = ',', format = 'd')}")
#'     logger::log_info("   ❌ Non-existent IDs identified: {formatC(smart_analysis$non_existent_count, big.mark = ',', format = 'd')}")
#'     logger::log_info("   🔍 Truly missing IDs in range: {formatC(smart_analysis$truly_missing_count, big.mark = ',', format = 'd')}")
#'     logger::log_info("   📈 Physician existence rate: {smart_analysis$existence_rate}%")
#'   }
#'   
#'   return(smart_analysis)
#' }
#' 
#' #' @noRd
#' analyze_id_density_patterns <- function(valid_ids, verbose) {
#'   
#'   if (length(valid_ids) < 10) {
#'     return(list(
#'       pattern = "insufficient_data",
#'       density_score = NA,
#'       recommendation = "Need more data to analyze patterns"
#'     ))
#'   }
#'   
#'   # Calculate gaps between consecutive valid IDs
#'   id_gaps <- diff(sort(valid_ids))
#'   
#'   # Analyze gap patterns
#'   gap_analysis <- list(
#'     mean_gap = mean(id_gaps),
#'     median_gap = median(id_gaps),
#'     max_gap = max(id_gaps),
#'     min_gap = min(id_gaps),
#'     gap_std = sd(id_gaps)
#'   )
#'   
#'   # Determine density pattern
#'   if (gap_analysis$mean_gap < 10) {
#'     pattern <- "dense"
#'     recommendation <- "IDs are densely packed - search systematically"
#'   } else if (gap_analysis$mean_gap < 100) {
#'     pattern <- "moderate"
#'     recommendation <- "Moderate density - focus on ranges with known valid IDs"
#'   } else {
#'     pattern <- "sparse"
#'     recommendation <- "Sparse ID distribution - consider larger jumps between searches"
#'   }
#'   
#'   # Calculate density score (0-100, where 100 = very dense)
#'   density_score <- min(100, max(0, 100 - (gap_analysis$mean_gap - 1)))
#'   
#'   return(list(
#'     pattern = pattern,
#'     density_score = density_score,
#'     gap_analysis = gap_analysis,
#'     recommendation = recommendation
#'   ))
#' }
#' 
#' #' @noRd
#' save_exclusion_lists <- function(id_classification, results_directory, verbose) {
#'   
#'   timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#'   
#'   # Save non-existent IDs list
#'   if (length(id_classification$non_existent) > 0) {
#'     non_existent_df <- data.frame(
#'       physician_id = sort(id_classification$non_existent),
#'       status = "non_existent",
#'       identified_date = Sys.Date(),
#'       note = "ID searched but returned no valid physician data"
#'     )
#'     
#'     non_existent_file <- file.path(results_directory, 
#'                                    paste0("non_existent_physician_ids_", timestamp, ".csv"))
#'     readr::write_csv(non_existent_df, non_existent_file)
#'     
#'     if (verbose) {
#'       logger::log_info("❌ Saved non-existent IDs: {basename(non_existent_file)}")
#'       logger::log_info("   📊 {formatC(nrow(non_existent_df), big.mark = ',', format = 'd')} non-existent IDs excluded from future searches")
#'     }
#'   }
#'   
#'   # Save valid IDs list
#'   if (length(id_classification$valid_existing) > 0) {
#'     valid_df <- data.frame(
#'       physician_id = sort(id_classification$valid_existing),
#'       status = "valid",
#'       has_subspecialty = id_classification$valid_existing %in% id_classification$with_subspecialty,
#'       identified_date = Sys.Date()
#'     )
#'     
#'     valid_file <- file.path(results_directory, 
#'                             paste0("valid_physician_ids_", timestamp, ".csv"))
#'     readr::write_csv(valid_df, valid_file)
#'     
#'     if (verbose) {
#'       logger::log_info("✅ Saved valid IDs: {basename(valid_file)}")
#'     }
#'   }
#' }
#' 
#' #' @noRd
#' generate_smart_recommendations <- function(smart_analysis, id_classification, verbose) {
#'   
#'   if (verbose) {
#'     logger::log_info("💡 Generating smart search recommendations...")
#'   }
#'   
#'   recommendations <- list()
#'   
#'   # 1. Priority: Fill truly missing gaps (excluding known non-existent)
#'   if (smart_analysis$truly_missing_count > 0) {
#'     recommendations$fill_smart_gaps <- list(
#'       description = "Fill gaps excluding known non-existent IDs",
#'       priority = "HIGH",
#'       missing_ids = smart_analysis$truly_missing_ids,
#'       missing_count = smart_analysis$truly_missing_count,
#'       estimated_success_rate = paste0(smart_analysis$existence_rate, "%"),
#'       recommended_batch_size = min(1000, smart_analysis$truly_missing_count)
#'     )
#'   }
#'   
#'   # 2. Extend range based on density patterns
#'   current_max <- smart_analysis$max_id_searched
#'   density_pattern <- smart_analysis$density_analysis$pattern
#'   
#'   if (density_pattern == "dense") {
#'     extend_size <- 5000
#'     extend_note <- "Dense pattern detected - search systematically"
#'   } else if (density_pattern == "moderate") {
#'     extend_size <- 2000
#'     extend_note <- "Moderate density - focus search efforts"
#'   } else {
#'     extend_size <- 1000
#'     extend_note <- "Sparse pattern - consider targeted approach"
#'   }
#'   
#'   recommendations$smart_extend_range <- list(
#'     description = "Extend search range with density-aware strategy",
#'     priority = if (smart_analysis$truly_missing_count > 500) "MEDIUM" else "HIGH",
#'     current_max = current_max,
#'     recommended_next_range = (current_max + 1):(current_max + extend_size),
#'     density_pattern = density_pattern,
#'     extend_note = extend_note
#'   )
#'   
#'   # 3. Subspecialty-focused recommendations
#'   if (smart_analysis$subspecialty_rate < 15) {
#'     recommendations$subspecialty_strategy <- list(
#'       description = "Low subspecialty rate - target higher ID ranges",
#'       priority = "MEDIUM",
#'       current_rate = smart_analysis$subspecialty_rate,
#'       recommendation = "Focus on ID ranges > 5000000 (more recent physicians)"
#'     )
#'   }
#'   
#'   # 4. Efficiency warning
#'   if (smart_analysis$existence_rate < 30) {
#'     recommendations$efficiency_warning <- list(
#'       description = "Low existence rate - consider strategy adjustment",
#'       priority = "HIGH",
#'       current_rate = smart_analysis$existence_rate,
#'       recommendation = "Current ID range has many non-existent IDs. Consider skipping to higher ranges."
#'     )
#'   }
#'   
#'   if (verbose) {
#'     logger::log_info("🎯 Smart recommendations generated:")
#'     for (rec_name in names(recommendations)) {
#'       rec <- recommendations[[rec_name]]
#'       logger::log_info("   • {rec$description} (Priority: {rec$priority})")
#'     }
#'   }
#'   
#'   return(recommendations)
#' }
#' 
#' #' Generate Smart Next Search List (Excludes Non-Existent IDs)
#' #'
#' #' @param smart_analysis output from analyze_search_results_smart()
#' #' @param strategy character. "smart_gaps", "smart_extend", or "smart_mixed"
#' #' @param batch_size numeric. Number of IDs to include
#' #' @param exclude_non_existent logical. Exclude known non-existent IDs
#' #' @param verbose logical. Enable detailed logging
#' #'
#' #' @return numeric vector of physician IDs to search next (excludes non-existent)
#' #'
#' #' @examples
#' #' # Smart analysis first
#' #' smart_analysis <- analyze_search_results_smart("physician_data/abog_large_scale_2025")
#' #' 
#' #' # Get next search list excluding non-existent IDs
#' #' next_ids <- generate_smart_search_list(
#' #'   smart_analysis = smart_analysis,
#' #'   strategy = "smart_gaps",
#' #'   batch_size = 1000,
#' #'   exclude_non_existent = TRUE,
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' @export
#' generate_smart_search_list <- function(smart_analysis, 
#'                                        strategy = "smart_mixed",
#'                                        batch_size = 7000,
#'                                        exclude_non_existent = TRUE,
#'                                        verbose = TRUE) {
#'   
#'   assertthat::assert_that(is.list(smart_analysis))
#'   assertthat::assert_that(strategy %in% c("smart_gaps", "smart_extend", "smart_mixed"))
#'   assertthat::assert_that(is.numeric(batch_size) && batch_size > 0)
#'   assertthat::assert_that(is.logical(exclude_non_existent))
#'   
#'   if (verbose) {
#'     logger::log_info("🧠 Generating smart search list...")
#'     logger::log_info("   Strategy: {strategy}")
#'     logger::log_info("   Batch size: {formatC(batch_size, big.mark = ',', format = 'd')}")
#'     logger::log_info("   Exclude non-existent: {exclude_non_existent}")
#'   }
#'   
#'   next_search_ids <- c()
#'   
#'   if (strategy == "smart_gaps" || strategy == "smart_mixed") {
#'     # Add truly missing IDs (already excludes non-existent)
#'     truly_missing <- smart_analysis$smart_analysis$truly_missing_ids
#'     
#'     if (length(truly_missing) > 0) {
#'       gaps_to_add <- min(batch_size, length(truly_missing))
#'       next_search_ids <- c(next_search_ids, truly_missing[1:gaps_to_add])
#'       
#'       if (verbose) {
#'         logger::log_info("   📍 Added {formatC(gaps_to_add, big.mark = ',', format = 'd')} truly missing IDs (non-existent excluded)")
#'       }
#'     }
#'   }
#'   
#'   if (strategy == "smart_extend" || strategy == "smart_mixed") {
#'     # Add new IDs beyond current maximum
#'     current_max <- smart_analysis$smart_analysis$max_id_searched
#'     remaining_space <- batch_size - length(next_search_ids)
#'     
#'     if (remaining_space > 0) {
#'       new_ids <- (current_max + 1):(current_max + remaining_space)
#'       next_search_ids <- c(next_search_ids, new_ids)
#'       
#'       if (verbose) {
#'         logger::log_info("   🚀 Added {formatC(remaining_space, big.mark = ',', format = 'd')} new IDs beyond {formatC(current_max, big.mark = ',', format = 'd')}")
#'       }
#'     }
#'   }
#'   
#'   # Remove non-existent IDs if requested (shouldn't be needed for truly_missing, but safety check)
#'   if (exclude_non_existent && length(smart_analysis$smart_analysis$non_existent_ids) > 0) {
#'     original_count <- length(next_search_ids)
#'     next_search_ids <- setdiff(next_search_ids, smart_analysis$smart_analysis$non_existent_ids)
#'     removed_count <- original_count - length(next_search_ids)
#'     
#'     if (removed_count > 0 && verbose) {
#'       logger::log_info("   🚫 Removed {formatC(removed_count, big.mark = ',', format = 'd')} known non-existent IDs")
#'     }
#'   }
#'   
#'   # Remove duplicates and sort
#'   next_search_ids <- sort(unique(next_search_ids))
#'   
#'   if (verbose) {
#'     logger::log_info("✅ Smart search list generated:")
#'     logger::log_info("   🔢 Total IDs: {formatC(length(next_search_ids), big.mark = ',', format = 'd')}")
#'     
#'     if (length(next_search_ids) > 0) {
#'       logger::log_info("   🆔 Range: {formatC(min(next_search_ids), big.mark = ',', format = 'd')} to {formatC(max(next_search_ids), big.mark = ',', format = 'd')}")
#'       
#'       # Show some examples
#'       if (length(next_search_ids) <= 10) {
#'         logger::log_info("   📋 IDs: {paste(next_search_ids, collapse = ', ')}")
#'       } else {
#'         first_few <- paste(head(next_search_ids, 5), collapse = ", ")
#'         last_few <- paste(tail(next_search_ids, 5), collapse = ", ")
#'         logger::log_info("   📋 IDs: {first_few} ... {last_few}")
#'       }
#'       
#'       # Estimate success rate
#'       estimated_success <- round(smart_analysis$smart_analysis$existence_rate, 1)
#'       estimated_valid <- round(length(next_search_ids) * estimated_success / 100)
#'       logger::log_info("   📈 Estimated valid physicians: ~{formatC(estimated_valid, big.mark = ',', format = 'd')} ({estimated_success}% rate)")
#'     }
#'   }
#'   
#'   return(next_search_ids)
#' }
#' 
#' #' @noRd  
#' extract_search_info_from_filename <- function(filename) {
#'   # Extract timestamp from filename
#'   timestamp_match <- stringr::str_extract(filename, "\\d{8}_\\d{6}")
#'   
#'   if (!is.na(timestamp_match)) {
#'     extraction_date <- substr(timestamp_match, 1, 8)
#'     extraction_time <- substr(timestamp_match, 10, 15)
#'     
#'     # Convert to readable format
#'     formatted_date <- paste0(
#'       substr(extraction_date, 1, 4), "-",
#'       substr(extraction_date, 5, 6), "-", 
#'       substr(extraction_date, 7, 8)
#'     )
#'     
#'     formatted_time <- paste0(
#'       substr(extraction_time, 1, 2), ":",
#'       substr(extraction_time, 3, 4), ":",
#'       substr(extraction_time, 5, 6)
#'     )
#'     
#'     return(list(
#'       extraction_date = formatted_date,
#'       extraction_time = formatted_time,
#'       timestamp = timestamp_match
#'     ))
#'   }
#'   
#'   return(list(
#'     extraction_date = NA_character_,
#'     extraction_time = NA_character_,
#'     timestamp = NA_character_
#'   ))
#' }
#' 
#' #run ----
#' # Copy the new smart tracking code from the artifact above
#' 
#' # Run smart analysis
#' smart_analysis <- analyze_search_results_smart(
#'   results_directory = "physician_data/abog_large_scale_2025",
#'   verbose = TRUE,
#'   save_exclusion_list = TRUE  # Saves non-existent IDs to CSV
#' )
#' 
#' # Generate next search list (automatically excludes non-existent IDs)
#' next_ids_smart <- generate_smart_search_list(
#'   smart_analysis = smart_analysis,
#'   strategy = "smart_gaps",      # Fill gaps, excluding non-existent
#'   batch_size = 5000,
#'   exclude_non_existent = TRUE,  # Double safety check
#'   verbose = TRUE
#' )
#' 
#' # Run extraction with smart ID list
#' smart_results <- extract_subspecialty_from_pdf_letters(
#'   physician_id_list = next_ids_smart,
#'   use_proxy_requests = TRUE,
#'   proxy_url_requests = "socks5://127.0.0.1:9150",
#'   output_directory_extractions = "physician_data/abog_large_scale_2025",
#'   request_delay_seconds = 2.0,
#'   chunk_size_records = 100,
#'   cleanup_downloaded_pdfs = FALSE,
#'   save_individual_files = TRUE,
#'   verbose_extraction_logging = TRUE
#' )

# Make where to search search recursively? ----
#' Enhanced Smart ABOG Physician ID Search with Recursive Directory Search
#' 
#' This improved system searches ALL subdirectories and tracks which IDs exist 
#' vs don't exist, excluding non-existent IDs from future searches automatically.

#' Analyze Search Results with Smart ID Classification (Recursive Directory Search)
#'
#' @param search_results_directory character. Directory containing extraction 
#'   results (searches all subdirectories recursively)
#' @param extraction_file_pattern character. Pattern to match result files. 
#'   Default matches combined and chunk extraction files
#' @param enable_verbose_logging logical. Enable detailed logging via console
#' @param save_exclusion_lists logical. Save non-existent IDs to file for 
#'   future exclusion
#'
#' @return list containing smart search analysis and recommendations
#'
#' @examples
#' # Smart analysis that searches ALL subdirectories recursively
#' search_analysis_results <- analyze_search_results_smart(
#'   search_results_directory = "physician_data/abog_large_scale_2025",
#'   extraction_file_pattern = "(combined_subspecialty_extractions|chunk_.*_subspecialty_extractions|physician_.*_cert_).*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE
#' )
#' 
#' # Analyze with custom file pattern and verbose output
#' custom_analysis_results <- analyze_search_results_smart(
#'   search_results_directory = "data/physician_extractions", 
#'   extraction_file_pattern = ".*_extractions.*\\.csv$",
#'   enable_verbose_logging = FALSE,
#'   save_exclusion_lists = FALSE
#' )
#' 
#' # Quick analysis with minimal logging
#' quick_analysis_results <- analyze_search_results_smart(
#'   search_results_directory = "results",
#'   extraction_file_pattern = ".*\\.csv$", 
#'   enable_verbose_logging = FALSE,
#'   save_exclusion_lists = TRUE
#' )
#'
#' @importFrom readr read_csv write_csv
#' @importFrom dplyr bind_rows arrange distinct summarise group_by filter n n_distinct first
#' @importFrom logger log_info log_warn
#' @importFrom assertthat assert_that
#' @importFrom stringr str_extract
#' @export
analyze_search_results_smart <- function(search_results_directory, 
                                         extraction_file_pattern = "(combined_subspecialty_extractions|chunk_.*_subspecialty_extractions|physician_.*_cert_).*\\.csv$",
                                         enable_verbose_logging = TRUE,
                                         save_exclusion_lists = TRUE) {
  
  # Input validation with assertthat
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Smart analysis of ABOG search results (recursive search)...")
    logger::log_info("📁 Searching in: {search_results_directory}")
    logger::log_info("🔍 File pattern: {extraction_file_pattern}")
  }
  
  # Find all extraction result files RECURSIVELY in all subdirectories
  all_csv_files <- list.files(
    search_results_directory, 
    pattern = extraction_file_pattern,
    recursive = TRUE,        # KEY: Search all subdirectories
    full.names = TRUE,
    include.dirs = FALSE
  )
  
  if (length(all_csv_files) == 0) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️ No extraction result files found in {search_results_directory}")
      logger::log_info("🔍 Searched pattern: {extraction_file_pattern}")
      logger::log_info("📂 Available subdirectories:")
      subdirs <- list.dirs(search_results_directory, recursive = FALSE)
      for (subdir in subdirs) {
        subdir_files <- list.files(subdir, pattern = "\\.csv$")
        logger::log_info("   📁 {basename(subdir)}: {length(subdir_files)} CSV files")
      }
    }
    return(NULL)
  }
  
  if (enable_verbose_logging) {
    logger::log_info("📁 Found {length(all_csv_files)} result files across subdirectories")
    
    # Show which subdirectories contain files
    subdirs_with_files <- unique(dirname(all_csv_files))
    for (subdir in subdirs_with_files) {
      files_in_subdir <- sum(dirname(all_csv_files) == subdir)
      logger::log_info("   📂 {basename(subdir)}: {files_in_subdir} files")
    }
  }
  
  # Read and combine all results
  all_search_results <- list()
  successful_reads <- 0
  failed_reads <- 0
  
  for (csv_file in all_csv_files) {
    tryCatch({
      file_data <- readr::read_csv(csv_file, show_col_types = FALSE)
      
      # Standardize common column types to prevent binding errors
      file_data <- standardize_column_types(file_data, enable_verbose_logging)
      
      # Extract search info from filename
      file_info <- extract_search_info_from_filename(basename(csv_file))
      
      # Add file metadata to data
      file_data$source_file <- basename(csv_file)
      file_data$source_subdir <- basename(dirname(csv_file))
      file_data$extraction_date <- file_info$extraction_date
      file_data$extraction_time <- file_info$extraction_time
      file_data$full_file_path <- csv_file
      
      all_search_results[[length(all_search_results) + 1]] <- file_data
      successful_reads <- successful_reads + 1
      
      if (enable_verbose_logging) {
        logger::log_info("✅ {basename(dirname(csv_file))}/{basename(csv_file)}: {nrow(file_data)} records")
      }
      
    }, error = function(e) {
      failed_reads <- failed_reads + 1
      if (enable_verbose_logging) {
        logger::log_warn("❌ Failed to read {basename(csv_file)}: {e$message}")
      }
    })
  }
  
  if (length(all_search_results) == 0) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️ No valid data found in result files")
    }
    return(NULL)
  }
  
  if (enable_verbose_logging) {
    logger::log_info("📊 File reading summary:")
    logger::log_info("   ✅ Successfully read: {successful_reads} files")
    logger::log_info("   ❌ Failed to read: {failed_reads} files")
  }
  
  # Combine all results
  combined_results <- dplyr::bind_rows(all_search_results)
  
  if (enable_verbose_logging) {
    logger::log_info("📋 Combined dataset: {nrow(combined_results)} total records")
    logger::log_info("🔢 Unique physician IDs: {length(unique(combined_results$physician_id))}")
  }
  
  # Smart classification of physician IDs
  id_classification <- classify_physician_ids_smart(combined_results, enable_verbose_logging)
  
  # Create smart search analysis
  smart_analysis <- create_smart_search_analysis(combined_results, id_classification, enable_verbose_logging)
  
  # Save exclusion lists if requested
  if (save_exclusion_lists) {
    save_exclusion_lists_helper(id_classification, search_results_directory, enable_verbose_logging)
  }
  
  # Generate smart recommendations
  smart_recommendations <- generate_smart_recommendations(smart_analysis, id_classification, enable_verbose_logging)
  
  return(list(
    smart_analysis = smart_analysis,
    id_classification = id_classification,
    recommendations = smart_recommendations,
    combined_data = combined_results,
    file_summary = list(
      total_files_found = length(all_csv_files),
      files_successfully_read = successful_reads,
      files_failed_to_read = failed_reads,
      subdirectories_searched = unique(basename(dirname(all_csv_files)))
    )
  ))
}

#' @noRd
standardize_column_types <- function(file_data, enable_verbose_logging = FALSE) {
  
  # Convert common problematic columns to character to prevent binding issues
  problematic_columns <- c("extraction_timestamp", "physician_id", "subspecialty_name", 
                           "physician_name", "certification_date", "status")
  
  for (col_name in problematic_columns) {
    if (col_name %in% names(file_data)) {
      # Convert to character to standardize across files
      file_data[[col_name]] <- as.character(file_data[[col_name]])
    }
  }
  
  # Ensure physician_id is numeric if it exists and contains only numbers
  if ("physician_id" %in% names(file_data)) {
    # Try to convert to numeric, keeping as character if it fails
    tryCatch({
      file_data$physician_id <- as.numeric(file_data$physician_id)
    }, warning = function(w) {
      if (enable_verbose_logging) {
        logger::log_warn("Could not convert physician_id to numeric, keeping as character")
      }
    })
  }
  
  return(file_data)
}

#' @noRd
classify_physician_ids_smart <- function(combined_data, enable_verbose_logging) {
  
  if (enable_verbose_logging) {
    logger::log_info("🔍 Classifying physician IDs by existence and success...")
  }
  
  # Group by physician_id and analyze each
  id_summary <- combined_data %>%
    dplyr::group_by(physician_id) %>%
    dplyr::summarise(
      search_count = dplyr::n(),
      has_name = any(!is.na(physician_name) & physician_name != "" & physician_name != "NA"),
      has_valid_data = any(!is.na(physician_name) & physician_name != "" & physician_name != "NA"),
      has_subspecialty = any(!is.na(subspecialty_name) & subspecialty_name != "" & subspecialty_name != "NA"),
      latest_search_date = if("extraction_timestamp" %in% names(combined_data)) {
        tryCatch(max(extraction_timestamp, na.rm = TRUE), error = function(e) NA_character_)
      } else {
        NA_character_
      },
      first_found_in_subdir = dplyr::first(source_subdir),
      search_subdirs = paste(unique(source_subdir), collapse = ", "),
      .groups = "drop"
    )
  
  # Classify IDs based on search results
  id_classification <- list(
    # IDs that exist and have valid physician data
    valid_existing = id_summary$physician_id[id_summary$has_valid_data],
    
    # IDs that were searched but returned no valid physician data (likely don't exist)
    non_existent = id_summary$physician_id[!id_summary$has_valid_data & id_summary$search_count >= 1],
    
    # IDs that exist but have minimal data (exist but incomplete records)
    minimal_data = id_summary$physician_id[id_summary$has_name & !id_summary$has_valid_data],
    
    # IDs with subspecialty training
    with_subspecialty = id_summary$physician_id[id_summary$has_subspecialty],
    
    # All searched IDs
    all_searched = sort(unique(combined_data$physician_id))
  )
  
  # Additional analysis
  id_classification$summary <- list(
    total_searched = length(id_classification$all_searched),
    valid_physicians = length(id_classification$valid_existing),
    non_existent_count = length(id_classification$non_existent),
    subspecialty_count = length(id_classification$with_subspecialty),
    existence_rate = round((length(id_classification$valid_existing) / 
                              length(id_classification$all_searched)) * 100, 2),
    subspecialty_rate = round((length(id_classification$with_subspecialty) / 
                                 length(id_classification$valid_existing)) * 100, 2)
  )
  
  # Store detailed summary for reference
  id_classification$detailed_summary <- id_summary
  
  if (enable_verbose_logging) {
    logger::log_info("📊 ID Classification Results:")
    logger::log_info("   ✅ Valid physicians: {formatC(id_classification$summary$valid_physicians, big.mark = ',', format = 'd')}")
    logger::log_info("   ❌ Non-existent IDs: {formatC(id_classification$summary$non_existent_count, big.mark = ',', format = 'd')}")
    logger::log_info("   🏥 With subspecialty: {formatC(id_classification$summary$subspecialty_count, big.mark = ',', format = 'd')}")
    logger::log_info("   📈 Existence rate: {id_classification$summary$existence_rate}%")
    logger::log_info("   📈 Subspecialty rate: {id_classification$summary$subspecialty_rate}%")
  }
  
  return(id_classification)
}

#' @noRd
create_smart_search_analysis <- function(combined_data, id_classification, enable_verbose_logging) {
  
  # Get ranges for different ID types
  valid_ids <- sort(id_classification$valid_existing)
  non_existent_ids <- sort(id_classification$non_existent)
  all_searched_ids <- sort(id_classification$all_searched)
  
  # Calculate search coverage excluding known non-existent IDs
  if (length(all_searched_ids) > 0) {
    min_searched <- min(all_searched_ids)
    max_searched <- max(all_searched_ids)
    
    # IDs in the searched range that we haven't tried yet
    full_searched_range <- min_searched:max_searched
    unsearched_in_range <- setdiff(full_searched_range, all_searched_ids)
    
    # IDs in range that we know don't exist
    non_existent_in_range <- intersect(full_searched_range, non_existent_ids)
    
    # IDs that are actually missing (haven't searched AND not known non-existent)
    truly_missing_ids <- setdiff(unsearched_in_range, non_existent_ids)
    
  } else {
    min_searched <- NA
    max_searched <- NA
    unsearched_in_range <- c()
    non_existent_in_range <- c()
    truly_missing_ids <- c()
  }
  
  # Analyze ID density patterns
  density_analysis <- analyze_id_density_patterns(valid_ids, enable_verbose_logging)
  
  # Analyze subdirectory distribution
  subdir_analysis <- analyze_subdirectory_distribution(combined_data, enable_verbose_logging)
  
  smart_analysis <- list(
    # Basic coverage
    total_searches = nrow(combined_data),
    unique_ids_searched = length(all_searched_ids),
    min_id_searched = min_searched,
    max_id_searched = max_searched,
    
    # Smart classification
    valid_physician_count = length(valid_ids),
    non_existent_count = length(non_existent_ids),
    existence_rate = id_classification$summary$existence_rate,
    
    # Gap analysis (excluding known non-existent)
    unsearched_in_range = unsearched_in_range,
    non_existent_in_range = non_existent_in_range,
    truly_missing_ids = truly_missing_ids,
    truly_missing_count = length(truly_missing_ids),
    
    # Subspecialty analysis
    subspecialty_count = length(id_classification$with_subspecialty),
    subspecialty_rate = id_classification$summary$subspecialty_rate,
    
    # Density patterns
    density_analysis = density_analysis,
    
    # Subdirectory distribution
    subdir_analysis = subdir_analysis,
    
    # ID lists for reference
    valid_ids = valid_ids,
    non_existent_ids = non_existent_ids
  )
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Smart Search Analysis Complete:")
    logger::log_info("   📊 Total searches: {formatC(smart_analysis$total_searches, big.mark = ',', format = 'd')}")
    logger::log_info("   ✅ Valid physicians found: {formatC(smart_analysis$valid_physician_count, big.mark = ',', format = 'd')}")
    logger::log_info("   ❌ Non-existent IDs identified: {formatC(smart_analysis$non_existent_count, big.mark = ',', format = 'd')}")
    logger::log_info("   🔍 Truly missing IDs in range: {formatC(smart_analysis$truly_missing_count, big.mark = ',', format = 'd')}")
    logger::log_info("   📈 Physician existence rate: {smart_analysis$existence_rate}%")
  }
  
  return(smart_analysis)
}

#' @noRd
analyze_subdirectory_distribution <- function(combined_data, enable_verbose_logging) {
  
  if (!"source_subdir" %in% names(combined_data)) {
    return(list(
      subdirs_found = 0,
      subdir_summary = data.frame()
    ))
  }
  
  subdir_summary <- combined_data %>%
    dplyr::group_by(source_subdir) %>%
    dplyr::summarise(
      total_records = dplyr::n(),
      unique_ids = dplyr::n_distinct(physician_id),
      valid_physicians = sum(!is.na(physician_name) & physician_name != "" & physician_name != "NA"),
      with_subspecialty = sum(!is.na(subspecialty_name) & subspecialty_name != "" & subspecialty_name != "NA"),
      existence_rate = round((valid_physicians / unique_ids) * 100, 1),
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(total_records))
  
  if (enable_verbose_logging) {
    logger::log_info("📂 Subdirectory Analysis:")
    for (i in 1:min(5, nrow(subdir_summary))) {
      row <- subdir_summary[i, ]
      logger::log_info("   📁 {row$source_subdir}: {row$total_records} records, {row$unique_ids} IDs, {row$existence_rate}% valid")
    }
    if (nrow(subdir_summary) > 5) {
      logger::log_info("   📂 ... and {nrow(subdir_summary) - 5} more subdirectories")
    }
  }
  
  return(list(
    subdirs_found = nrow(subdir_summary),
    subdir_summary = subdir_summary,
    total_subdirs = length(unique(combined_data$source_subdir))
  ))
}

#' @noRd
analyze_id_density_patterns <- function(valid_ids, enable_verbose_logging) {
  
  if (length(valid_ids) < 10) {
    return(list(
      pattern = "insufficient_data",
      density_score = NA,
      recommendation = "Need more data to analyze patterns"
    ))
  }
  
  # Calculate gaps between consecutive valid IDs
  id_gaps <- diff(sort(valid_ids))
  
  # Analyze gap patterns
  gap_analysis <- list(
    mean_gap = mean(id_gaps),
    median_gap = median(id_gaps),
    max_gap = max(id_gaps),
    min_gap = min(id_gaps),
    gap_std = sd(id_gaps)
  )
  
  # Determine density pattern
  if (gap_analysis$mean_gap < 10) {
    pattern <- "dense"
    recommendation <- "IDs are densely packed - search systematically"
  } else if (gap_analysis$mean_gap < 100) {
    pattern <- "moderate"
    recommendation <- "Moderate density - focus on ranges with known valid IDs"
  } else {
    pattern <- "sparse"
    recommendation <- "Sparse ID distribution - consider larger jumps between searches"
  }
  
  # Calculate density score (0-100, where 100 = very dense)
  density_score <- min(100, max(0, 100 - (gap_analysis$mean_gap - 1)))
  
  return(list(
    pattern = pattern,
    density_score = density_score,
    gap_analysis = gap_analysis,
    recommendation = recommendation
  ))
}

#' @noRd
save_exclusion_lists_helper <- function(id_classification, search_results_directory, enable_verbose_logging) {
  
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs list
  if (length(id_classification$non_existent) > 0) {
    non_existent_df <- data.frame(
      physician_id = sort(id_classification$non_existent),
      status = "non_existent",
      identified_date = Sys.Date(),
      note = "ID searched but returned no valid physician data"
    )
    
    non_existent_file <- file.path(search_results_directory, 
                                   paste0("non_existent_physician_ids_", timestamp, ".csv"))
    readr::write_csv(non_existent_df, non_existent_file)
    
    if (enable_verbose_logging) {
      logger::log_info("❌ Saved non-existent IDs: {basename(non_existent_file)}")
      logger::log_info("   📊 {formatC(nrow(non_existent_df), big.mark = ',', format = 'd')} non-existent IDs excluded from future searches")
    }
  }
  
  # Save valid IDs list
  if (length(id_classification$valid_existing) > 0) {
    valid_df <- data.frame(
      physician_id = sort(id_classification$valid_existing),
      status = "valid",
      has_subspecialty = id_classification$valid_existing %in% id_classification$with_subspecialty,
      identified_date = Sys.Date()
    )
    
    valid_file <- file.path(search_results_directory, 
                            paste0("valid_physician_ids_", timestamp, ".csv"))
    readr::write_csv(valid_df, valid_file)
    
    if (enable_verbose_logging) {
      logger::log_info("✅ Saved valid IDs: {basename(valid_file)}")
    }
  }
}

#' @noRd
generate_smart_recommendations <- function(smart_analysis, id_classification, enable_verbose_logging) {
  
  if (enable_verbose_logging) {
    logger::log_info("💡 Generating smart search recommendations...")
  }
  
  recommendations <- list()
  
  # 1. Priority: Fill truly missing gaps (excluding known non-existent)
  if (smart_analysis$truly_missing_count > 0) {
    recommendations$fill_smart_gaps <- list(
      description = "Fill gaps excluding known non-existent IDs",
      priority = "HIGH",
      missing_ids = smart_analysis$truly_missing_ids,
      missing_count = smart_analysis$truly_missing_count,
      estimated_success_rate = paste0(smart_analysis$existence_rate, "%"),
      recommended_batch_size = min(1000, smart_analysis$truly_missing_count)
    )
  }
  
  # 2. Extend range based on density patterns
  current_max <- smart_analysis$max_id_searched
  density_pattern <- smart_analysis$density_analysis$pattern
  
  if (density_pattern == "dense") {
    extend_size <- 5000
    extend_note <- "Dense pattern detected - search systematically"
  } else if (density_pattern == "moderate") {
    extend_size <- 2000
    extend_note <- "Moderate density - focus search efforts"
  } else {
    extend_size <- 1000
    extend_note <- "Sparse pattern - consider targeted approach"
  }
  
  recommendations$smart_extend_range <- list(
    description = "Extend search range with density-aware strategy",
    priority = if (smart_analysis$truly_missing_count > 500) "MEDIUM" else "HIGH",
    current_max = current_max,
    recommended_next_range = (current_max + 1):(current_max + extend_size),
    density_pattern = density_pattern,
    extend_note = extend_note
  )
  
  # 3. Subspecialty-focused recommendations
  if (smart_analysis$subspecialty_rate < 15) {
    recommendations$subspecialty_strategy <- list(
      description = "Low subspecialty rate - target higher ID ranges",
      priority = "MEDIUM",
      current_rate = smart_analysis$subspecialty_rate,
      recommendation = "Focus on ID ranges > 5000000 (more recent physicians)"
    )
  }
  
  # 4. Efficiency warning
  if (smart_analysis$existence_rate < 30) {
    recommendations$efficiency_warning <- list(
      description = "Low existence rate - consider strategy adjustment",
      priority = "HIGH",
      current_rate = smart_analysis$existence_rate,
      recommendation = "Current ID range has many non-existent IDs. Consider skipping to higher ranges."
    )
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🎯 Smart recommendations generated:")
    for (rec_name in names(recommendations)) {
      rec <- recommendations[[rec_name]]
      logger::log_info("   • {rec$description} (Priority: {rec$priority})")
    }
  }
  
  return(recommendations)
}

#' Generate Smart Next Search List (Excludes Non-Existent IDs, Enhanced Recursive Search)
#'
#' @param smart_analysis_results output from analyze_search_results_smart()
#' @param search_strategy character. "smart_gaps", "smart_extend", or "smart_mixed"
#' @param batch_size numeric. Number of IDs to include
#' @param exclude_non_existent logical. Exclude known non-existent IDs
#' @param enable_verbose_logging logical. Enable detailed logging
#'
#' @return numeric vector of physician IDs to search next (excludes non-existent)
#'
#' @examples
#' # Smart analysis first (searches all subdirectories)
#' search_analysis_results <- analyze_search_results_smart(
#'   search_results_directory = "physician_data/abog_large_scale_2025"
#' )
#' 
#' # Get next search list excluding non-existent IDs
#' next_ids <- generate_smart_search_list(
#'   smart_analysis_results = search_analysis_results,
#'   search_strategy = "smart_gaps",
#'   batch_size = 1000,
#'   exclude_non_existent = TRUE,
#'   enable_verbose_logging = TRUE
#' )
#' 
#' # Generate mixed strategy search list
#' mixed_search_ids <- generate_smart_search_list(
#'   smart_analysis_results = search_analysis_results,
#'   search_strategy = "smart_mixed",
#'   batch_size = 5000,
#'   exclude_non_existent = TRUE,
#'   enable_verbose_logging = FALSE
#' )
#' 
#' # Generate extension-only search list
#' extension_ids <- generate_smart_search_list(
#'   smart_analysis_results = search_analysis_results,
#'   search_strategy = "smart_extend",
#'   batch_size = 10000,
#'   exclude_non_existent = FALSE,
#'   enable_verbose_logging = TRUE
#' )
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info
#' @export
generate_smart_search_list <- function(smart_analysis_results, 
                                       search_strategy = "smart_mixed",
                                       batch_size = 7000,
                                       exclude_non_existent = TRUE,
                                       enable_verbose_logging = TRUE) {
  
  assertthat::assert_that(is.list(smart_analysis_results))
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed"))
  assertthat::assert_that(is.numeric(batch_size) && batch_size > 0)
  assertthat::assert_that(is.logical(exclude_non_existent))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Generating smart search list...")
    logger::log_info("   Strategy: {search_strategy}")
    logger::log_info("   Batch size: {formatC(batch_size, big.mark = ',', format = 'd')}")
    logger::log_info("   Exclude non-existent: {exclude_non_existent}")
    logger::log_info("   Search covered {smart_analysis_results$file_summary$total_files_found} files in {length(smart_analysis_results$file_summary$subdirectories_searched)} subdirectories")
  }
  
  next_search_ids <- c()
  
  if (search_strategy == "smart_gaps" || search_strategy == "smart_mixed") {
    # Add truly missing IDs (already excludes non-existent)
    truly_missing <- smart_analysis_results$smart_analysis$truly_missing_ids
    
    if (length(truly_missing) > 0) {
      gaps_to_add <- min(batch_size, length(truly_missing))
      next_search_ids <- c(next_search_ids, truly_missing[1:gaps_to_add])
      
      if (enable_verbose_logging) {
        logger::log_info("   📍 Added {formatC(gaps_to_add, big.mark = ',', format = 'd')} truly missing IDs (non-existent excluded)")
      }
    }
  }
  
  if (search_strategy == "smart_extend" || search_strategy == "smart_mixed") {
    # Add new IDs beyond current maximum
    current_max <- smart_analysis_results$smart_analysis$max_id_searched
    remaining_space <- batch_size - length(next_search_ids)
    
    if (remaining_space > 0) {
      new_ids <- (current_max + 1):(current_max + remaining_space)
      next_search_ids <- c(next_search_ids, new_ids)
      
      if (enable_verbose_logging) {
        logger::log_info("   🚀 Added {formatC(remaining_space, big.mark = ',', format = 'd')} new IDs beyond {formatC(current_max, big.mark = ',', format = 'd')}")
      }
    }
  }
  
  # Remove non-existent IDs if requested (shouldn't be needed for truly_missing, but safety check)
  if (exclude_non_existent && length(smart_analysis_results$smart_analysis$non_existent_ids) > 0) {
    original_count <- length(next_search_ids)
    next_search_ids <- setdiff(next_search_ids, smart_analysis_results$smart_analysis$non_existent_ids)
    removed_count <- original_count - length(next_search_ids)
    
    if (removed_count > 0 && enable_verbose_logging) {
      logger::log_info("   🚫 Removed {formatC(removed_count, big.mark = ',', format = 'd')} known non-existent IDs")
    }
  }
  
  # Remove duplicates and sort
  next_search_ids <- sort(unique(next_search_ids))
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Smart search list generated:")
    logger::log_info("   🔢 Total IDs: {formatC(length(next_search_ids), big.mark = ',', format = 'd')}")
    
    if (length(next_search_ids) > 0) {
      logger::log_info("   🆔 Range: {formatC(min(next_search_ids), big.mark = ',', format = 'd')} to {formatC(max(next_search_ids), big.mark = ',', format = 'd')}")
      
      # Show some examples
      if (length(next_search_ids) <= 10) {
        logger::log_info("   📋 IDs: {paste(next_search_ids, collapse = ', ')}")
      } else {
        first_few <- paste(head(next_search_ids, 5), collapse = ", ")
        last_few <- paste(tail(next_search_ids, 5), collapse = ", ")
        logger::log_info("   📋 IDs: {first_few} ... {last_few}")
      }
      
      # Estimate success rate
      estimated_success <- round(smart_analysis_results$smart_analysis$existence_rate, 1)
      estimated_valid <- round(length(next_search_ids) * estimated_success / 100)
      logger::log_info("   📈 Estimated valid physicians: ~{formatC(estimated_valid, big.mark = ',', format = 'd')} ({estimated_success}% rate)")
    }
  }
  
  return(next_search_ids)
}

#' @noRd  
extract_search_info_from_filename <- function(filename) {
  # Extract timestamp from filename using stringr
  timestamp_match <- stringr::str_extract(filename, "\\d{8}_\\d{6}")
  
  if (!is.na(timestamp_match)) {
    extraction_date <- substr(timestamp_match, 1, 8)
    extraction_time <- substr(timestamp_match, 10, 15)
    
    # Convert to readable format
    formatted_date <- paste0(
      substr(extraction_date, 1, 4), "-",
      substr(extraction_date, 5, 6), "-", 
      substr(extraction_date, 7, 8)
    )
    
    formatted_time <- paste0(
      substr(extraction_time, 1, 2), ":",
      substr(extraction_time, 3, 4), ":",
      substr(extraction_time, 5, 6)
    )
    
    return(list(
      extraction_date = formatted_date,
      extraction_time = formatted_time,
      timestamp = timestamp_match
    ))
  }
  
  return(list(
    extraction_date = NA_character_,
    extraction_time = NA_character_,
    timestamp = NA_character_
  ))
}

# run ----
# Run the analysis
analysis_results <- analyze_search_results_smart(
  search_results_directory = "physician_data/abog_large_scale_2025",
  extraction_file_pattern = ".*\\.csv$",  # This will catch all CSV files
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE
)

# Generate next search list
next_ids <- generate_smart_search_list(
  smart_analysis_results = analysis_results,
  search_strategy = "smart_mixed",
  batch_size = 5000,
  exclude_non_existent = TRUE,
  enable_verbose_logging = TRUE
)

# WORKING but superceded
#' # Yes with Tor Browser Feature -----
#' #' Extract ABOG Subspecialty Data via PDF Certification Letters
#' #'
#' #' This function automates the process of downloading ABOG certification letter PDFs
#' #' and extracting subspecialty information from them. Uses modern web scraping 
#' #' techniques with rvest and httr2 instead of deprecated Selenium/PhantomJS.
#' #'
#' #' @param physician_id_list numeric vector. ABOG physician IDs to process
#' #' @param use_proxy_requests logical. Whether to use proxy for requests. Default: FALSE
#' #' @param proxy_url_requests character. Proxy URL (Tor SOCKS5). Default: "socks5://127.0.0.1:9050"
#' #' @param output_directory_extractions character. Directory to save PDFs and results. 
#' #'   Default: "abog_pdf_extractions"
#' #' @param request_delay_seconds numeric. Delay between requests in seconds. Default: 3.0
#' #' @param verbose_extraction_logging logical. Enable detailed logging. Default: TRUE
#' #' @param cleanup_downloaded_pdfs logical. Whether to delete PDFs after extraction. Default: FALSE
#' #' @param chunk_size_records numeric. Number of records to process before saving 
#' #'   progress to combined file. Default: 50
#' #' @param save_individual_files logical. Whether to save individual CSV files 
#' #'   for each chunk. Default: TRUE
#' #'
#' #' @return data.frame with subspecialty information extracted from PDFs:
#' #'   \itemize{
#' #'     \item{physician_id: ABOG physician ID}
#' #'     \item{abog_id_number: ABOG physician ID number}
#' #'     \item{physician_name: Full name from PDF}
#' #'     \item{clinically_active_status: Whether physician is clinically active}
#' #'     \item{primary_certification: Primary board certification}
#' #'     \item{primary_cert_date: Original certification date}
#' #'     \item{primary_cert_status: Current certification status}
#' #'     \item{primary_continuing_cert: Participating in continuing certification}
#' #'     \item{subspecialty_name: Subspecialty certification name}
#' #'     \item{subspecialty_cert_date: Original subspecialty certification date}
#' #'     \item{subspecialty_cert_status: Current subspecialty certification status}
#' #'     \item{subspecialty_continuing_cert: Participating in continuing certification}
#' #'     \item{extraction_timestamp: When data was extracted}
#' #'     \item{pdf_filename: Saved PDF filename}
#' #'   }
#' #'
#' #' @examples
#' #' # Example 1: Single physician test (Elena)
#' #' elena_subspecialty_data <- extract_subspecialty_from_pdf_letters(
#' #'   physician_id_list = 9020382,
#' #'   output_directory_extractions = "elena_pdf_test",
#' #'   verbose_extraction_logging = TRUE,
#' #'   cleanup_downloaded_pdfs = FALSE
#' #' )
#' #' 
#' #' # Example 2: Batch processing with Tor proxy
#' #' test_physician_list <- c(9020382, 9014566, 849120, 930075)
#' #' batch_subspecialty_results <- extract_subspecialty_from_pdf_letters(
#' #'   physician_id_list = test_physician_list,
#' #'   use_proxy_requests = TRUE,
#' #'   output_directory_extractions = "batch_pdf_extractions", 
#' #'   request_delay_seconds = 5.0,
#' #'   chunk_size_records = 25,
#' #'   cleanup_downloaded_pdfs = TRUE,
#' #'   verbose_extraction_logging = TRUE
#' #' )
#' #' 
#' #' # Example 3: Large dataset with chunking and combined file output
#' #' large_physician_cohort <- c(9000000:9000100)
#' #' large_subspecialty_extraction <- extract_subspecialty_from_pdf_letters(
#' #'   physician_id_list = large_physician_cohort,
#' #'   use_proxy_requests = TRUE,
#' #'   proxy_url_requests = "socks5://127.0.0.1:9050",
#' #'   output_directory_extractions = "large_pdf_batch",
#' #'   request_delay_seconds = 10.0,
#' #'   cleanup_downloaded_pdfs = TRUE,
#' #'   verbose_extraction_logging = FALSE,
#' #'   chunk_size_records = 25,
#' #'   save_individual_files = TRUE
#' #' )
#' #'
#' #' @importFrom rvest read_html html_elements html_attr
#' #' @importFrom httr2 request req_perform resp_body_raw resp_status
#' #' @importFrom pdftools pdf_text
#' #' @importFrom stringr str_extract str_detect str_trim
#' #' @importFrom dplyr tibble bind_rows
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom assertthat assert_that
#' #' @importFrom readr write_csv
#' #' @importFrom cli cli_progress_bar cli_progress_update cli_progress_done
#' #'
#' #' @export
#' extract_subspecialty_from_pdf_letters <- function(
    #'     physician_id_list,
#'     use_proxy_requests = FALSE,
#'     proxy_url_requests = "socks5://127.0.0.1:9050",
#'     output_directory_extractions = "abog_pdf_extractions",
#'     request_delay_seconds = 3.0,
#'     verbose_extraction_logging = TRUE,
#'     cleanup_downloaded_pdfs = FALSE,
#'     chunk_size_records = 50,
#'     save_individual_files = TRUE) {
#'   
#'   # Input validation
#'   assertthat::assert_that(is.numeric(physician_id_list) || is.character(physician_id_list))
#'   assertthat::assert_that(length(physician_id_list) > 0)
#'   assertthat::assert_that(is.logical(use_proxy_requests))
#'   assertthat::assert_that(is.character(output_directory_extractions))
#'   assertthat::assert_that(is.numeric(request_delay_seconds) && request_delay_seconds >= 0)
#'   assertthat::assert_that(is.logical(verbose_extraction_logging))
#'   assertthat::assert_that(is.logical(cleanup_downloaded_pdfs))
#'   assertthat::assert_that(is.numeric(chunk_size_records) && chunk_size_records > 0)
#'   assertthat::assert_that(is.logical(save_individual_files))
#'   
#'   if (verbose_extraction_logging) {
#'     logger::log_info("🔍 Starting ABOG PDF subspecialty extraction")
#'     logger::log_info("Processing {length(physician_id_list)} physicians")
#'     logger::log_info("Output directory: {output_directory_extractions}")
#'     logger::log_info("Request delay: {request_delay_seconds} seconds")
#'     logger::log_info("Chunk size: {chunk_size_records} records")
#'     logger::log_info("Save individual files: {save_individual_files}")
#'   }
#'   
#'   # Create output directory
#'   if (!dir.exists(output_directory_extractions)) {
#'     dir.create(output_directory_extractions, recursive = TRUE)
#'     if (verbose_extraction_logging) {
#'       logger::log_info("📁 Created output directory: {output_directory_extractions}")
#'     }
#'   }
#'   
#'   # Initialize HTTP session with Tor proxy support
#'   session_config <- list(
#'     use_proxy = use_proxy_requests,
#'     proxy_url = proxy_url_requests,
#'     user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
#'   )
#'   
#'   if (verbose_extraction_logging) {
#'     logger::log_info("✅ HTTP session initialized successfully")
#'     if (use_proxy_requests && !is.null(proxy_url_requests)) {
#'       logger::log_info("🔐 Using Tor proxy: {proxy_url_requests}")
#'       logger::log_info("ℹ️ Ensure Tor is running on the specified port")
#'     }
#'   }
#'   
#'   # Test Tor connectivity if proxy is enabled
#'   if (use_proxy_requests && verbose_extraction_logging) {
#'     tor_test_result <- test_tor_connectivity(proxy_url_requests, verbose_extraction_logging)
#'     if (!tor_test_result) {
#'       logger::log_warn("⚠️ Tor connectivity test failed - extraction may not use proxy")
#'     }
#'   }
#'   
#'   # Create timestamp for this extraction run
#'   extraction_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
#'   
#'   # Split physician list into chunks
#'   physician_chunks <- split(physician_id_list, ceiling(seq_along(physician_id_list) / chunk_size_records))
#'   total_chunks <- length(physician_chunks)
#'   failed_chunks <- c()
#'   
#'   if (verbose_extraction_logging) {
#'     logger::log_info("📦 Created {total_chunks} chunks of max {chunk_size_records} physicians each")
#'   }
#'   
#'   # Initialize results tracking
#'   combined_extraction_results <- dplyr::tibble(
#'     physician_id = numeric(0),
#'     abog_id_number = character(0),
#'     physician_name = character(0),
#'     clinically_active_status = character(0),
#'     primary_certification = character(0),
#'     primary_cert_date = character(0),
#'     primary_cert_status = character(0),
#'     primary_continuing_cert = character(0),
#'     subspecialty_name = character(0),
#'     subspecialty_cert_date = character(0),
#'     subspecialty_cert_status = character(0),
#'     subspecialty_continuing_cert = character(0),
#'     extraction_timestamp = as.POSIXct(character(0)),
#'     pdf_filename = character(0)
#'   )
#'   
#'   successful_extraction_count <- 0
#'   failed_extraction_count <- 0
#'   
#'   # Initialize progress bar
#'   if (verbose_extraction_logging) {
#'     progress_bar <- cli::cli_progress_bar(
#'       "Processing physicians",
#'       total = length(physician_id_list),
#'       format = "{cli::pb_spin} {cli::pb_current}/{cli::pb_total} | {cli::pb_percent} | ETA: {cli::pb_eta} | {cli::pb_rate}"
#'     )
#'   }
#'   
#'   # Process each chunk with global error handling
#'   for (chunk_index in seq_along(physician_chunks)) {
#'     current_chunk <- physician_chunks[[chunk_index]]
#'     
#'     if (verbose_extraction_logging) {
#'       logger::log_info("🔄 Processing chunk {chunk_index}/{total_chunks} ({length(current_chunk)} physicians)")
#'     }
#'     
#'     # Wrap chunk processing in tryCatch
#'     tryCatch({
#'       chunk_results <- dplyr::tibble(
#'         physician_id = numeric(0),
#'         abog_id_number = character(0),
#'         physician_name = character(0),
#'         clinically_active_status = character(0),
#'         primary_certification = character(0),
#'         primary_cert_date = character(0),
#'         primary_cert_status = character(0),
#'         primary_continuing_cert = character(0),
#'         subspecialty_name = character(0),
#'         subspecialty_cert_date = character(0),
#'         subspecialty_cert_status = character(0),
#'         subspecialty_continuing_cert = character(0),
#'         extraction_timestamp = as.POSIXct(character(0)),
#'         pdf_filename = character(0)
#'       )
#'       
#'       # Process each physician in the chunk
#'       for (physician_index_in_chunk in seq_along(current_chunk)) {
#'         current_physician_identifier <- current_chunk[physician_index_in_chunk]
#'         overall_physician_index <- (chunk_index - 1) * chunk_size_records + physician_index_in_chunk
#'         
#'         if (verbose_extraction_logging) {
#'           logger::log_info("🔍 Processing physician {overall_physician_index}/{length(physician_id_list)}: ID {current_physician_identifier}")
#'         }
#'         
#'         # Download and extract subspecialty data from PDF
#'         physician_subspecialty_data <- download_and_extract_physician_pdf_data(
#'           current_physician_identifier, session_config, output_directory_extractions, 
#'           cleanup_downloaded_pdfs, verbose_extraction_logging)
#'         
#'         # Create record (successful or failed)
#'         if (!is.null(physician_subspecialty_data)) {
#'           current_record <- physician_subspecialty_data
#'           successful_extraction_count <- successful_extraction_count + 1
#'           
#'           if (verbose_extraction_logging) {
#'             physician_name <- if (!is.na(physician_subspecialty_data$physician_name)) {
#'               physician_subspecialty_data$physician_name
#'             } else {
#'               "Unknown"
#'             }
#'             subspecialty_information <- if (!is.na(physician_subspecialty_data$subspecialty_name) && 
#'                                             physician_subspecialty_data$subspecialty_name != "") {
#'               physician_subspecialty_data$subspecialty_name
#'             } else {
#'               "No subspecialty"
#'             }
#'             logger::log_info("✅ Success: {physician_name} | {subspecialty_information}")
#'           }
#'         } else {
#'           # Create failed extraction record
#'           current_record <- dplyr::tibble(
#'             physician_id = current_physician_identifier,
#'             abog_id_number = NA_character_,
#'             physician_name = NA_character_,
#'             clinically_active_status = NA_character_,
#'             primary_certification = NA_character_,
#'             primary_cert_date = NA_character_,
#'             primary_cert_status = NA_character_,
#'             primary_continuing_cert = NA_character_,
#'             subspecialty_name = NA_character_,
#'             subspecialty_cert_date = NA_character_,
#'             subspecialty_cert_status = NA_character_,
#'             subspecialty_continuing_cert = NA_character_,
#'             extraction_timestamp = Sys.time(),
#'             pdf_filename = NA_character_
#'           )
#'           
#'           failed_extraction_count <- failed_extraction_count + 1
#'           
#'           if (verbose_extraction_logging) {
#'             logger::log_warn("❌ Failed extraction for ID {current_physician_identifier}")
#'           }
#'         }
#'         
#'         # Add record to chunk results
#'         chunk_results <- dplyr::bind_rows(chunk_results, current_record)
#'         
#'         # Update progress bar
#'         if (verbose_extraction_logging) {
#'           cli::cli_progress_update(id = progress_bar)
#'         }
#'         
#'         # Rate limiting (except for last physician in chunk)
#'         if (physician_index_in_chunk < length(current_chunk)) {
#'           Sys.sleep(request_delay_seconds)
#'         }
#'       }
#'       
#'       # Add chunk results to combined results
#'       combined_extraction_results <- dplyr::bind_rows(combined_extraction_results, chunk_results)
#'       
#'       # Save chunk file if requested
#'       if (save_individual_files) {
#'         save_chunk_results_file(chunk_results, chunk_index, 
#'                                 output_directory_extractions, verbose_extraction_logging)
#'       }
#'       
#'       # Save updated combined file after each chunk
#'       save_combined_results_file(combined_extraction_results, 
#'                                  output_directory_extractions, verbose_extraction_logging)
#'       
#'       if (verbose_extraction_logging) {
#'         chunk_success_rate <- round((sum(!is.na(chunk_results$physician_name)) / nrow(chunk_results)) * 100, 1)
#'         logger::log_info("📦 Chunk {chunk_index} completed: {nrow(chunk_results)} records ({chunk_success_rate}% success)")
#'       }
#'       
#'     }, error = function(e) {
#'       failed_chunks <<- c(failed_chunks, chunk_index)
#'       if (verbose_extraction_logging) {
#'         logger::log_error("⚠️ Error in chunk {chunk_index}: {e$message}")
#'         logger::log_warn("🔄 Continuing with next chunk...")
#'       }
#'     })
#'   }
#'   
#'   # Complete progress bar
#'   if (verbose_extraction_logging) {
#'     cli::cli_progress_done(id = progress_bar)
#'   }
#'   
#'   # Final save of combined results (ensure it's up to date)
#'   final_combined_filepath <- save_combined_results_file(combined_extraction_results, 
#'                                                         output_directory_extractions, verbose_extraction_logging)
#'   
#'   # Generate summary
#'   if (verbose_extraction_logging) {
#'     subspecialty_training_count <- sum(!is.na(combined_extraction_results$subspecialty_name) & 
#'                                          combined_extraction_results$subspecialty_name != "", na.rm = TRUE)
#'     final_success_rate <- round((successful_extraction_count / length(physician_id_list)) * 100, 1)
#'     successful_chunks <- total_chunks - length(failed_chunks)
#'     
#'     summary_lines <- c(
#'       paste("📅 Timestamp:", extraction_timestamp),
#'       paste("🧑‍⚕️ Total physicians:", length(physician_id_list)),
#'       paste("✅ Successful extractions:", successful_extraction_count),
#'       paste("❌ Failed extractions:", failed_extraction_count),
#'       paste("📊 Success rate:", paste0(final_success_rate, "%")),
#'       paste("🏥 Physicians with subspecialty:", subspecialty_training_count),
#'       paste("📦 Total chunks:", total_chunks),
#'       paste("✅ Successful chunks:", successful_chunks),
#'       paste("❌ Failed chunks:", length(failed_chunks)),
#'       paste("📂 Output folder:", output_directory_extractions),
#'       paste("📄 Combined file:", basename(final_combined_filepath))
#'     )
#'     
#'     if (length(failed_chunks) > 0) {
#'       summary_lines <- c(summary_lines, paste("⚠️ Failed chunk numbers:", paste(failed_chunks, collapse = ", ")))
#'     }
#'     
#'     # Log summary
#'     logger::log_info("🏁 ABOG PDF subspecialty extraction completed")
#'     logger::log_info(paste(rep("=", 60), collapse = ""))  # Separator line
#'     for (line in summary_lines) {
#'       logger::log_info(line)
#'     }
#'     logger::log_info(paste(rep("=", 60), collapse = ""))  # Separator line
#'     
#'     if (save_individual_files) {
#'       logger::log_info("📁 Individual chunk files saved in: {output_directory_extractions}")
#'     }
#'     
#'     if (use_proxy_requests) {
#'       logger::log_info("🔐 All requests routed through Tor proxy")
#'     }
#'   }
#'   
#'   return(combined_extraction_results)
#' }
#' 
#' # Helper Functions
#' 
#' #' @noRd
#' test_tor_connectivity <- function(proxy_url, verbose_logging) {
#'   if (verbose_logging) {
#'     logger::log_info("🧪 Testing Tor connectivity...")
#'   }
#'   
#'   tryCatch({
#'     # Test with a simple HTTP request through the proxy
#'     test_request <- httr2::request("http://httpbin.org/ip")
#'     
#'     # Configure proxy
#'     if (stringr::str_detect(proxy_url, "socks5://")) {
#'       test_request <- httr2::req_proxy(test_request, url = proxy_url)
#'     }
#'     
#'     # Set short timeout for test
#'     test_request <- httr2::req_timeout(test_request, 10)
#'     
#'     # Perform test request
#'     test_response <- httr2::req_perform(test_request)
#'     
#'     if (httr2::resp_status(test_response) == 200) {
#'       response_body <- httr2::resp_body_string(test_response)
#'       
#'       if (verbose_logging) {
#'         logger::log_info("✅ Tor connectivity test successful")
#'         logger::log_info("🌐 IP response: {substr(response_body, 1, 100)}...")
#'       }
#'       return(TRUE)
#'     } else {
#'       if (verbose_logging) {
#'         logger::log_warn("⚠️ Tor test got HTTP {httr2::resp_status(test_response)}")
#'       }
#'       return(FALSE)
#'     }
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_warn("❌ Tor connectivity test failed: {e$message}")
#'       
#'       # Provide helpful troubleshooting info
#'       if (stringr::str_detect(e$message, "connection|refused|timeout")) {
#'         logger::log_info("💡 Troubleshooting tips:")
#'         logger::log_info("   • Check if Tor is running: ps aux | grep tor")
#'         logger::log_info("   • Standard Tor port: 9050, Tor Browser: 9150")
#'         logger::log_info("   • Start Tor: brew install tor && tor")
#'       }
#'     }
#'     return(FALSE)
#'   })
#' }
#' 
#' #' @noRd
#' save_combined_results_file <- function(combined_data, output_directory, verbose_logging) {
#'   if (nrow(combined_data) > 0) {
#'     combined_filename <- "combined_subspecialty_extractions.csv"
#'     combined_filepath <- file.path(output_directory, combined_filename)
#'     
#'     readr::write_csv(combined_data, combined_filepath)
#'     
#'     if (verbose_logging) {
#'       logger::log_info("💾 Saved combined results: {combined_filename} ({nrow(combined_data)} records)")
#'     }
#'     
#'     return(combined_filepath)
#'   }
#'   return(NULL)
#' }
#' 
#' #' @noRd
#' save_chunk_results_file <- function(chunk_data, chunk_number, output_directory, verbose_logging) {
#'   if (nrow(chunk_data) > 0) {
#'     chunk_filename <- paste0("chunk_", sprintf("%03d", chunk_number), "_subspecialty_extractions_", 
#'                              format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
#'     chunk_filepath <- file.path(output_directory, chunk_filename)
#'     
#'     readr::write_csv(chunk_data, chunk_filepath)
#'     
#'     if (verbose_logging) {
#'       logger::log_info("📄 Saved chunk {chunk_number}: {chunk_filename} ({nrow(chunk_data)} records)")
#'     }
#'     
#'     return(chunk_filepath)
#'   }
#'   return(NULL)
#' }
#' 
#' #' @noRd
#' download_and_extract_physician_pdf_data <- function(physician_id, session_config, 
#'                                                     output_directory, cleanup_pdfs, verbose_logging) {
#'   tryCatch({
#'     pdf_api_url <- paste0("https://api.abog.org/report/CertStatusLetter/", physician_id)
#'     
#'     if (verbose_logging) {
#'       logger::log_info("  📡 Accessing ABOG API for ID {physician_id}")
#'       logger::log_info("  🔗 PDF URL: {pdf_api_url}")
#'     }
#'     
#'     pdf_file_path <- download_certification_pdf(
#'       pdf_api_url, physician_id, output_directory, session_config, verbose_logging)
#'     
#'     if (!is.null(pdf_file_path) && file.exists(pdf_file_path)) {
#'       if (verify_pdf_file(pdf_file_path, verbose_logging)) {
#'         if (verbose_logging) {
#'           logger::log_info("    ✅ PDF verification successful")
#'         }
#'         
#'         extracted_physician_data <- extract_subspecialty_data_from_pdf(
#'           pdf_file_path, physician_id, verbose_logging)
#'         
#'         if (cleanup_pdfs && file.exists(pdf_file_path)) {
#'           file.remove(pdf_file_path)
#'           if (verbose_logging) {
#'             logger::log_info("    🗑️ Cleaned up PDF file: {basename(pdf_file_path)}")
#'           }
#'         }
#'         
#'         return(extracted_physician_data)
#'       } else {
#'         if (verbose_logging) {
#'           logger::log_warn("    ⚠️ PDF verification failed - checking for error response")
#'         }
#'         
#'         error_content <- readLines(pdf_file_path, warn = FALSE)
#'         error_text <- paste(error_content, collapse = "\n")
#'         
#'         if (verbose_logging) {
#'           logger::log_info("    📄 Response preview: {substr(error_text, 1, 100)}...")
#'         }
#'         
#'         if (stringr::str_detect(error_text, "(?i)(error|not found|unauthorized|access denied|invalid)")) {
#'           if (verbose_logging) {
#'             logger::log_warn("    ❌ API returned error response for physician {physician_id}")
#'           }
#'         }
#'         
#'         if (cleanup_pdfs && file.exists(pdf_file_path)) {
#'           file.remove(pdf_file_path)
#'         }
#'         
#'         return(NULL)
#'       }
#'     } else {
#'       if (verbose_logging) {
#'         logger::log_warn("  ❌ Failed to download PDF from API for physician {physician_id}")
#'       }
#'       return(NULL)
#'     }
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_error("  ❌ Processing error for physician {physician_id}: {e$message}")
#'     }
#'     return(NULL)
#'   })
#' }
#' 
#' #' @noRd
#' download_certification_pdf <- function(pdf_url, physician_id, output_directory, 
#'                                        session_config, verbose_logging) {
#'   tryCatch({
#'     pdf_request <- httr2::request(pdf_url)
#'     pdf_request <- httr2::req_headers(pdf_request, 
#'                                       "User-Agent" = session_config$user_agent,
#'                                       "Accept" = "application/pdf,application/octet-stream,*/*",
#'                                       "Accept-Language" = "en-US,en;q=0.5",
#'                                       "Accept-Encoding" = "gzip, deflate",
#'                                       "Connection" = "keep-alive",
#'                                       "Referer" = paste0("https://www.abog.org/verify-physician?physid=", physician_id)
#'     )
#'     
#'     # Add proxy configuration if enabled
#'     if (session_config$use_proxy && !is.null(session_config$proxy_url)) {
#'       if (verbose_logging) {
#'         logger::log_info("    🔐 Configuring Tor proxy: {session_config$proxy_url}")
#'       }
#'       
#'       # Configure proxy for httr2 with multiple approaches
#'       tryCatch({
#'         if (stringr::str_detect(session_config$proxy_url, "socks5://")) {
#'           # Extract host and port for SOCKS5
#'           proxy_host <- stringr::str_extract(session_config$proxy_url, "(?<=://).+?(?=:)")
#'           proxy_port <- as.numeric(stringr::str_extract(session_config$proxy_url, "(?<=:)[0-9]+$"))
#'           
#'           if (verbose_logging) {
#'             logger::log_info("    📡 SOCKS5 proxy: {proxy_host}:{proxy_port}")
#'           }
#'           
#'           # Method 1: Try httr2's req_proxy with full URL
#'           pdf_request <- httr2::req_proxy(pdf_request, url = session_config$proxy_url)
#'           
#'           if (verbose_logging) {
#'             logger::log_info("    ✅ SOCKS5 proxy configured successfully")
#'           }
#'           
#'         } else if (stringr::str_detect(session_config$proxy_url, "http://")) {
#'           # HTTP proxy configuration
#'           pdf_request <- httr2::req_proxy(pdf_request, url = session_config$proxy_url)
#'           
#'           if (verbose_logging) {
#'             logger::log_info("    ✅ HTTP proxy configured successfully")
#'           }
#'         }
#'         
#'       }, error = function(proxy_error) {
#'         if (verbose_logging) {
#'           logger::log_warn("    ⚠️ Primary proxy method failed: {proxy_error$message}")
#'           logger::log_info("    🔄 Trying alternative proxy configuration...")
#'         }
#'         
#'         # Fallback method: Try with httr2 options
#'         tryCatch({
#'           if (stringr::str_detect(session_config$proxy_url, "socks5://")) {
#'             proxy_host <- stringr::str_extract(session_config$proxy_url, "(?<=://).+?(?=:)")
#'             proxy_port <- as.numeric(stringr::str_extract(session_config$proxy_url, "(?<=:)[0-9]+$"))
#'             
#'             # Alternative: Use req_options with curl proxy settings
#'             pdf_request <- httr2::req_options(pdf_request,
#'                                               proxy = paste0(proxy_host, ":", proxy_port),
#'                                               proxytype = 7L  # CURLPROXY_SOCKS5 = 7
#'             )
#'             
#'             if (verbose_logging) {
#'               logger::log_info("    ✅ Alternative SOCKS5 proxy configured")
#'             }
#'           }
#'           
#'         }, error = function(fallback_error) {
#'           if (verbose_logging) {
#'             logger::log_error("    ❌ All proxy methods failed:")
#'             logger::log_error("    Primary error: {proxy_error$message}")
#'             logger::log_error("    Fallback error: {fallback_error$message}")
#'             logger::log_warn("    🚨 Proceeding WITHOUT proxy - check Tor setup!")
#'           }
#'         })
#'       })
#'     }
#'     
#'     pdf_request <- httr2::req_options(pdf_request, followlocation = TRUE)
#'     pdf_response <- httr2::req_perform(pdf_request)
#'     
#'     if (httr2::resp_status(pdf_response) == 200) {
#'       content_type <- httr2::resp_header(pdf_response, "content-type")
#'       
#'       if (verbose_logging) {
#'         logger::log_info("    📄 Response content type: {content_type}")
#'       }
#'       
#'       response_content <- httr2::resp_body_raw(pdf_response)
#'       file_extension <- if (stringr::str_detect(content_type, "pdf")) ".pdf" else ".html"
#'       download_filename <- paste0("physician_", physician_id, "_cert_", 
#'                                   format(Sys.time(), "%Y%m%d_%H%M%S"), file_extension)
#'       download_filepath <- file.path(output_directory, download_filename)
#'       
#'       writeBin(response_content, download_filepath)
#'       
#'       if (verbose_logging) {
#'         size_kb <- round(length(response_content) / 1024, 1)
#'         logger::log_info("    💾 Downloaded: {download_filename} ({size_kb} KB)")
#'       }
#'       
#'       return(download_filepath)
#'     } else {
#'       if (verbose_logging) {
#'         logger::log_warn("    ❌ Download failed: HTTP {httr2::resp_status(pdf_response)}")
#'       }
#'       return(NULL)
#'     }
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_error("    ❌ Download error: {e$message}")
#'     }
#'     return(NULL)
#'   })
#' }
#' 
#' #' @noRd
#' verify_pdf_file <- function(file_path, verbose_logging) {
#'   tryCatch({
#'     file_info <- file.info(file_path)
#'     if (file_info$size < 50) {
#'       if (verbose_logging) {
#'         logger::log_warn("  File too small to be a valid PDF ({file_info$size} bytes)")
#'       }
#'       return(FALSE)
#'     }
#'     
#'     file_connection <- file(file_path, "rb")
#'     file_header <- readBin(file_connection, "raw", n = 10)
#'     close(file_connection)
#'     
#'     pdf_signature <- charToRaw("%PDF")
#'     is_pdf <- length(file_header) >= 4 && identical(file_header[1:4], pdf_signature)
#'     
#'     if (verbose_logging) {
#'       if (is_pdf) {
#'         logger::log_info("  File verified as valid PDF")
#'       } else {
#'         header_text <- tryCatch({
#'           rawToChar(file_header[1:min(20, length(file_header))])
#'         }, error = function(e) {
#'           paste(as.character(file_header[1:min(10, length(file_header))]), collapse = " ")
#'         })
#'         logger::log_info("  File header indicates HTML/other format: {substr(header_text, 1, 50)}")
#'       }
#'     }
#'     
#'     return(is_pdf)
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_error("  Error verifying file format: {e$message}")
#'     }
#'     return(FALSE)
#'   })
#' }
#' 
#' #' @noRd
#' extract_subspecialty_data_from_pdf <- function(pdf_file_path, physician_id, verbose_logging) {
#'   tryCatch({
#'     pdf_text_content <- pdftools::pdf_text(pdf_file_path)
#'     pdf_combined_text <- paste(pdf_text_content, collapse = " ")
#'     
#'     if (verbose_logging) {
#'       char_count <- nchar(pdf_combined_text)
#'       logger::log_info("    📖 Extracted text from PDF ({char_count} characters)")
#'     }
#'     
#'     # Extract all information using helper functions
#'     physician_name_extracted <- extract_physician_name_from_text(pdf_combined_text)
#'     abog_id_extracted <- extract_abog_id_from_text(pdf_combined_text)
#'     clinically_active_extracted <- extract_clinically_active_status_from_text(pdf_combined_text)
#'     primary_certification_name <- extract_primary_certification_from_text(pdf_combined_text)
#'     primary_certification_date <- extract_primary_cert_date_from_text(pdf_combined_text)
#'     primary_certification_status <- extract_primary_cert_status_from_text(pdf_combined_text)
#'     primary_continuing_certification <- extract_primary_continuing_cert_from_text(pdf_combined_text)
#'     subspecialty_certification_name <- extract_subspecialty_name_from_pdf_text(pdf_combined_text)
#'     subspecialty_certification_date <- extract_subspecialty_cert_date_from_pdf_text(pdf_combined_text)
#'     subspecialty_certification_status <- extract_subspecialty_cert_status_from_pdf_text(pdf_combined_text)
#'     subspecialty_continuing_certification <- extract_subspecialty_continuing_cert_from_pdf_text(pdf_combined_text)
#'     
#'     if (verbose_logging) {
#'       name_info <- if (!is.na(physician_name_extracted)) physician_name_extracted else "Unknown"
#'       subspecialty_info <- if (!is.na(subspecialty_certification_name)) subspecialty_certification_name else "None"
#'       logger::log_info("    👤 Physician: {name_info}")
#'       logger::log_info("    🏥 Subspecialty: {subspecialty_info}")
#'     }
#'     
#'     # Create result data frame
#'     physician_extracted_data <- dplyr::tibble(
#'       physician_id = physician_id,
#'       abog_id_number = stringr::str_trim(abog_id_extracted),
#'       physician_name = stringr::str_trim(physician_name_extracted),
#'       clinically_active_status = stringr::str_trim(clinically_active_extracted),
#'       primary_certification = stringr::str_trim(primary_certification_name),
#'       primary_cert_date = stringr::str_trim(primary_certification_date),
#'       primary_cert_status = stringr::str_trim(primary_certification_status),
#'       primary_continuing_cert = stringr::str_trim(primary_continuing_certification),
#'       subspecialty_name = stringr::str_trim(subspecialty_certification_name),
#'       subspecialty_cert_date = stringr::str_trim(subspecialty_certification_date),
#'       subspecialty_cert_status = stringr::str_trim(subspecialty_certification_status),
#'       subspecialty_continuing_cert = stringr::str_trim(subspecialty_continuing_certification),
#'       extraction_timestamp = Sys.time(),
#'       pdf_filename = basename(pdf_file_path)
#'     )
#'     
#'     return(physician_extracted_data)
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_error("    ❌ PDF extraction error: {e$message}")
#'     }
#'     return(NULL)
#'   })
#' }
#' 
#' # Text Extraction Helper Functions
#' 
#' #' @noRd
#' extract_abog_id_from_text <- function(pdf_text) {
#'   abog_id_pattern <- stringr::str_extract(pdf_text,
#'                                           "(?i)abog id number:\\s*([0-9]+)")
#'   
#'   if (!is.na(abog_id_pattern)) {
#'     id_extracted <- stringr::str_replace(abog_id_pattern, "(?i)abog id number:\\s*", "")
#'     return(stringr::str_trim(id_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_clinically_active_status_from_text <- function(pdf_text) {
#'   active_status_pattern <- stringr::str_extract(pdf_text,
#'                                                 "(?i)clinically active:\\s*(yes|no)")
#'   
#'   if (!is.na(active_status_pattern)) {
#'     status_extracted <- stringr::str_replace(active_status_pattern, "(?i)clinically active:\\s*", "")
#'     return(stringr::str_trim(status_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_physician_name_from_text <- function(pdf_text) {
#'   name_pattern <- stringr::str_extract(pdf_text, 
#'                                        "(?i)(?:RE:\\s*)?certification status of ([^,\\n\\r]+(?:,\\s*MD)?)")
#'   
#'   if (!is.na(name_pattern)) {
#'     name_extracted <- stringr::str_replace(name_pattern, "(?i)(?:RE:\\s*)?certification status of\\s*", "")
#'     name_extracted <- stringr::str_replace(name_extracted, ",\\s*MD$", "")
#'     return(stringr::str_trim(name_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_primary_certification_from_text <- function(pdf_text) {
#'   return(stringr::str_extract(pdf_text, 
#'                               "(?i)(obstetrics and gynecology|family medicine|internal medicine)"))
#' }
#' 
#' #' @noRd
#' extract_primary_cert_date_from_text <- function(pdf_text) {
#'   date_pattern <- stringr::str_extract(pdf_text,
#'                                        "(?i)original certification date:\\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
#'   
#'   if (!is.na(date_pattern)) {
#'     date_extracted <- stringr::str_replace(date_pattern, "(?i)original certification date:\\s*", "")
#'     return(stringr::str_trim(date_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_primary_cert_status_from_text <- function(pdf_text) {
#'   status_pattern <- stringr::str_extract(pdf_text,
#'                                          "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
#'   
#'   if (!is.na(status_pattern)) {
#'     status_extracted <- stringr::str_replace(status_pattern, "(?i)certification status:\\s*", "")
#'     return(stringr::str_trim(status_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_primary_continuing_cert_from_text <- function(pdf_text) {
#'   primary_continuing_pattern <- stringr::str_extract(pdf_text,
#'                                                      "(?i)(?:obstetrics and gynecology).*?participating in continuing certification:\\s*(yes|no)")
#'   
#'   if (!is.na(primary_continuing_pattern)) {
#'     cert_extracted <- stringr::str_extract(primary_continuing_pattern,
#'                                            "(?i)participating in continuing certification:\\s*(yes|no)")
#'     cert_cleaned <- stringr::str_replace(cert_extracted, "(?i)participating in continuing certification:\\s*", "")
#'     return(stringr::str_trim(cert_cleaned))
#'   }
#'   
#'   general_continuing_pattern <- stringr::str_extract(pdf_text,
#'                                                      "(?i)participating in continuing certification:\\s*(yes|no)")
#'   
#'   if (!is.na(general_continuing_pattern)) {
#'     cert_extracted <- stringr::str_replace(general_continuing_pattern, "(?i)participating in continuing certification:\\s*", "")
#'     return(stringr::str_trim(cert_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_subspecialty_name_from_pdf_text <- function(pdf_text) {
#'   abog_subspecialty_list <- c(
#'     "Urogynecology and Reconstructive Pelvic Surgery",
#'     "Female Pelvic Medicine and Reconstructive Surgery",
#'     "Gynecologic Oncology",
#'     "Maternal-Fetal Medicine", 
#'     "Reproductive Endocrinology and Infertility",
#'     "Complex Family Planning",
#'     "Critical Care Medicine",
#'     "Hospice and Palliative Medicine"
#'   )
#'   
#'   for (subspecialty_name in abog_subspecialty_list) {
#'     if (stringr::str_detect(pdf_text, stringr::fixed(subspecialty_name, ignore_case = TRUE))) {
#'       return(subspecialty_name)
#'     }
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_subspecialty_cert_date_from_pdf_text <- function(pdf_text) {
#'   subspecialty_date_pattern <- stringr::str_extract(pdf_text,
#'                                                     "(?i)original subspecialty certification date:\\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
#'   
#'   if (!is.na(subspecialty_date_pattern)) {
#'     date_extracted <- stringr::str_replace(subspecialty_date_pattern, "(?i)original subspecialty certification date:\\s*", "")
#'     return(stringr::str_trim(date_extracted))
#'   }
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_subspecialty_cert_status_from_pdf_text <- function(pdf_text) {
#'   # Look for the second occurrence of "Certification Status" (after subspecialty section)
#'   # Split text into lines to better handle the structure
#'   text_lines <- strsplit(pdf_text, "\\n")[[1]]
#'   
#'   # Find the subspecialty section and look for certification status after it
#'   subspecialty_found <- FALSE
#'   for (i in seq_along(text_lines)) {
#'     line <- text_lines[i]
#'     
#'     # Check if we've found a subspecialty section
#'     if (stringr::str_detect(line, "(?i)(urogynecology|gynecologic oncology|maternal-fetal|reproductive endocrinology|complex family planning|critical care|hospice)")) {
#'       subspecialty_found <- TRUE
#'       next
#'     }
#'     
#'     # If we're in subspecialty section, look for certification status
#'     if (subspecialty_found && stringr::str_detect(line, "(?i)certification status:")) {
#'       status_match <- stringr::str_extract(line, "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
#'       if (!is.na(status_match)) {
#'         status_cleaned <- stringr::str_replace(status_match, "(?i)certification status:\\s*", "")
#'         return(stringr::str_trim(status_cleaned))
#'       }
#'     }
#'   }
#'   
#'   # Alternative approach: look for all certification status patterns and take the second one
#'   all_cert_status <- stringr::str_extract_all(pdf_text, 
#'                                               "(?i)certification status:\\s*(valid through [0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")[[1]]
#'   
#'   if (length(all_cert_status) >= 2) {
#'     # Take the second occurrence (subspecialty)
#'     status_cleaned <- stringr::str_replace(all_cert_status[2], "(?i)certification status:\\s*", "")
#'     return(stringr::str_trim(status_cleaned))
#'   }
#'   
#'   return(NA_character_)
#' }
#' 
#' #' @noRd
#' extract_subspecialty_continuing_cert_from_pdf_text <- function(pdf_text) {
#'   # Look for the second occurrence of "Participating in Continuing Certification"
#'   # Split text into lines to better handle the structure
#'   text_lines <- strsplit(pdf_text, "\\n")[[1]]
#'   
#'   # Find the subspecialty section and look for continuing cert after it
#'   subspecialty_found <- FALSE
#'   for (i in seq_along(text_lines)) {
#'     line <- text_lines[i]
#'     
#'     # Check if we've found a subspecialty section
#'     if (stringr::str_detect(line, "(?i)(urogynecology|gynecologic oncology|maternal-fetal|reproductive endocrinology|complex family planning|critical care|hospice)")) {
#'       subspecialty_found <- TRUE
#'       next
#'     }
#'     
#'     # If we're in subspecialty section, look for continuing certification
#'     if (subspecialty_found && stringr::str_detect(line, "(?i)participating in continuing certification:")) {
#'       cert_match <- stringr::str_extract(line, "(?i)participating in continuing certification:\\s*(yes|no)")
#'       if (!is.na(cert_match)) {
#'         cert_cleaned <- stringr::str_replace(cert_match, "(?i)participating in continuing certification:\\s*", "")
#'         return(stringr::str_trim(cert_cleaned))
#'       }
#'     }
#'   }
#'   
#'   # Alternative approach: look for all continuing cert patterns and take the second one
#'   all_continuing_cert <- stringr::str_extract_all(pdf_text, 
#'                                                   "(?i)participating in continuing certification:\\s*(yes|no)")[[1]]
#'   
#'   if (length(all_continuing_cert) >= 2) {
#'     # Take the second occurrence (subspecialty)
#'     cert_cleaned <- stringr::str_replace(all_continuing_cert[2], "(?i)participating in continuing certification:\\s*", "")
#'     return(stringr::str_trim(cert_cleaned))
#'   }
#'   
#'   return(NA_character_)
#' }
#' 
#' # run ----
#' # Load your physician data
#' physician_data <- readr::read_rds("physician_data/discovery_results/physician_data_from_exploratory_after_2020.rds")
#' 
#' # Extract the userid column for the function
#' physician_id_list <- physician_data$userid
#' 
#' # Check the setup
#' length(physician_id_list)  # Should show 7,083
#' head(physician_id_list)    # Should show first few IDs like 9046330, 9046328, etc.
#' 
#' # Alternative approach
#' # Sequential vector of IDs from 9014566 to 9019566
#' physician_id_list <- 9025566:9035566
#' 
#' # Run the full extraction with optimal settings for 7,083 physicians
#' large_scale_results <- extract_subspecialty_from_pdf_letters(
#'   physician_id_list = physician_id_list,                    
#'   use_proxy_requests = TRUE,                                # Use Tor for anonymity
#'   proxy_url_requests = "socks5://127.0.0.1:9150",         # Your working Tor BROWSER setup
#'   output_directory_extractions = "physician_data/abog_large_scale_2025",  # Organized output folder
#'   request_delay_seconds = 2.0,                             # Be nice to their servers
#'   chunk_size_records = 100,                                # Save progress every 100 records
#'   cleanup_downloaded_pdfs = FALSE,                          # Save disk space
#'   save_individual_files = TRUE,                            # Keep chunk files for backup
#'   verbose_extraction_logging = TRUE                        # Monitor progress
#' )

# 
# 
# NOT WORKING 
# # Test with just the subspecialty extraction files
# subspecialty_files <- list.files(
#   "physician_data/discovery_results", 
#   pattern = "subspecialty_extractions", 
#   full.names = TRUE
# )
# 
# length(subspecialty_files)  # How many subspecialty files?
# 
# # Read one subspecialty file to test
# test_file <- readr::read_csv(subspecialty_files[1])
# table(test_file$subspecialty_name, useNA = "always")
# 
# # Count subspecialists in each file type
# merged_data_consolidated %>%
#   mutate(
#     file_type = case_when(
#       str_detect(source_file, "subspecialty_extractions") ~ "subspecialty",
#       str_detect(source_file, "sequential_discovery") ~ "discovery", 
#       TRUE ~ "other"
#     )
#   ) %>%
#   group_by(file_type) %>%
#   summarise(
#     total_rows = n(),
#     with_subspecialty = sum(!is.na(subspecialty_name) & subspecialty_name != ""),
#     subspecialty_rate = round(with_subspecialty / total_rows * 100, 2)
#   )
# 
# # Process subspecialty files separately
# subspecialty_data <- map_dfr(subspecialty_files, readr::read_csv) %>%
#   filter(!is.na(subspecialty_name) & subspecialty_name != "")
# 
# # Process discovery files separately  
# discovery_files <- list.files(
#   "physician_data/discovery_results",
#   pattern = "sequential_discovery", 
#   full.names = TRUE
# )
# 
# discovery_data <- map_dfr(discovery_files, readr::read_csv)
# 
# # Check subspecialty counts
# table(subspecialty_data$subspecialty_name, useNA = "always")
# 
# 
# 
# # Try merging just 2 files manually to see what happens
# file1 <- readr::read_csv(subspecialty_files[1])
# file2 <- readr::read_csv(discovery_files[1])
# 
# # Check subspecialty counts before merge
# cat("File 1 subspecialties:", sum(!is.na(file1$subspecialty_name)), "\n")
# cat("File 2 subspecialities:", sum(!is.na(file2$subspecialty_name)), "\n")
# 
# # Merge and check after
# manual_merge <- bind_rows(file1, file2)
# cat("After merge subspecialties:", sum(!is.na(manual_merge$subspecialty_name)), "\n")
# 

#' # Merge All CSV Files in a Directory -----
#' #' Merge All CSV Files in a Directory
#' #'
#' #' This function reads all CSV files from a specified directory and combines them
#' #' into a single data frame. Handles column name standardization and provides
#' #' detailed logging of the merge process.
#' #'
#' #' @param directory_path character. Path to directory containing CSV files.
#' #'   Default: "physician_data/discovery_results"
#' #' @param output_file_path character. Path to save merged CSV file. If NULL,
#' #'   no file is saved. Default: NULL
#' #' @param verbose_logging logical. Enable detailed logging. Default: TRUE
#' #' @param standardize_column_names logical. Whether to standardize column names
#' #'   across files. Default: TRUE
#' #' @param remove_duplicates logical. Whether to remove duplicate rows based on
#' #'   all columns. Default: FALSE
#' #' @param add_source_file_column logical. Whether to add a column indicating
#' #'   source file for each row. Default: TRUE
#' #'
#' #' @return data.frame containing all merged CSV data
#' #'
#' #' @examples
#' #' # Example 1: Basic merge with logging
#' #' merged_physician_data <- merge_all_csv_files_in_directory(
#' #'   directory_path = "physician_data/discovery_results",
#' #'   verbose_logging = TRUE
#' #' )
#' #'
#' #' # Example 2: Merge and save to file with source tracking
#' #' comprehensive_dataset <- merge_all_csv_files_in_directory(
#' #'   directory_path = "physician_data/discovery_results",
#' #'   output_file_path = "physician_data/merged_discovery_results.csv",
#' #'   add_source_file_column = TRUE,
#' #'   verbose_logging = TRUE
#' #' )
#' #'
#' #' # Example 3: Clean merge with duplicate removal
#' #' clean_merged_data <- merge_all_csv_files_in_directory(
#' #'   directory_path = "physician_data/discovery_results",
#' #'   standardize_column_names = TRUE,
#' #'   remove_duplicates = TRUE,
#' #'   add_source_file_column = FALSE,
#' #'   verbose_logging = FALSE
#' #' )
#' #'
#' #' @importFrom readr read_csv write_csv
#' #' @importFrom dplyr bind_rows mutate
#' #' @importFrom stringr str_detect str_replace_all str_to_lower
#' #' @importFrom logger log_info log_warn log_error
#' #' @importFrom assertthat assert_that
#' #'
#' #' @export
#' merge_all_csv_files_in_directory <- function(directory_path = "physician_data/discovery_results",
#'                                              output_file_path = NULL,
#'                                              verbose_logging = TRUE,
#'                                              standardize_column_names = TRUE,
#'                                              remove_duplicates = FALSE,
#'                                              add_source_file_column = TRUE) {
#'   
#'   # Input validation
#'   assertthat::assert_that(is.character(directory_path))
#'   assertthat::assert_that(dir.exists(directory_path), 
#'                           msg = paste("Directory does not exist:", directory_path))
#'   assertthat::assert_that(is.logical(verbose_logging))
#'   assertthat::assert_that(is.logical(standardize_column_names))
#'   assertthat::assert_that(is.logical(remove_duplicates))
#'   assertthat::assert_that(is.logical(add_source_file_column))
#'   
#'   if (verbose_logging) {
#'     logger::log_info("🔍 Starting CSV file merge process")
#'     logger::log_info("📁 Directory: {directory_path}")
#'   }
#'   
#'   # Find all CSV files in directory
#'   csv_file_paths <- list.files(
#'     path = directory_path,
#'     pattern = "\\.csv$",
#'     full.names = TRUE,
#'     recursive = FALSE
#'   )
#'   
#'   if (length(csv_file_paths) == 0) {
#'     if (verbose_logging) {
#'       logger::log_warn("⚠️ No CSV files found in directory: {directory_path}")
#'     }
#'     return(data.frame())
#'   }
#'   
#'   if (verbose_logging) {
#'     logger::log_info("📄 Found {length(csv_file_paths)} CSV files:")
#'     for (i in seq_along(csv_file_paths)) {
#'       file_name <- basename(csv_file_paths[i])
#'       logger::log_info("  {i}. {file_name}")
#'     }
#'   }
#'   
#'   # Initialize list to store data frames
#'   csv_data_list <- list()
#'   successful_files <- character(0)
#'   failed_files <- character(0)
#'   total_rows_processed <- 0
#'   
#'   # Read each CSV file
#'   for (i in seq_along(csv_file_paths)) {
#'     current_file_path <- csv_file_paths[i]
#'     current_file_name <- basename(current_file_path)
#'     
#'     if (verbose_logging) {
#'       logger::log_info("📖 Reading file {i}/{length(csv_file_paths)}: {current_file_name}")
#'     }
#'     
#'     tryCatch({
#'       # Read CSV file with consistent column types
#'       current_data <- readr::read_csv(
#'         current_file_path, 
#'         show_col_types = FALSE,
#'         col_types = readr::cols(.default = readr::col_character())
#'       )
#'       
#'       # Add source file column if requested
#'       if (add_source_file_column) {
#'         current_data <- current_data %>%
#'           dplyr::mutate(source_file = current_file_name, .before = 1)
#'       }
#'       
#'       # Standardize column names if requested
#'       if (standardize_column_names) {
#'         current_data <- standardize_csv_column_names(current_data, verbose_logging)
#'       }
#'       
#'       # Store data frame
#'       csv_data_list[[i]] <- current_data
#'       successful_files <- c(successful_files, current_file_name)
#'       
#'       current_row_count <- nrow(current_data)
#'       total_rows_processed <- total_rows_processed + current_row_count
#'       
#'       if (verbose_logging) {
#'         logger::log_info("  ✅ Success: {current_row_count} rows, {ncol(current_data)} columns")
#'         if (standardize_column_names) {
#'           logger::log_info("  📝 Columns: {paste(names(current_data)[1:min(5, ncol(current_data))], collapse = ', ')}...")
#'         }
#'       }
#'       
#'     }, error = function(e) {
#'       failed_files <<- c(failed_files, current_file_name)
#'       if (verbose_logging) {
#'         logger::log_error("  ❌ Failed to read {current_file_name}: {e$message}")
#'       }
#'     })
#'   }
#'   
#'   # Check if any files were successfully read
#'   if (length(csv_data_list) == 0) {
#'     if (verbose_logging) {
#'       logger::log_error("❌ No CSV files could be read successfully")
#'     }
#'     return(data.frame())
#'   }
#'   
#'   # Remove NULL entries (failed reads)
#'   csv_data_list <- csv_data_list[!sapply(csv_data_list, is.null)]
#'   
#'   if (verbose_logging) {
#'     logger::log_info("🔗 Combining {length(csv_data_list)} data frames...")
#'   }
#'   
#'   # Combine all data frames with type conversion
#'   tryCatch({
#'     # Convert all data frames to have consistent column types before binding
#'     standardized_data_list <- lapply(csv_data_list, function(df) {
#'       # Convert userid columns to character to avoid type conflicts
#'       userid_columns <- names(df)[stringr::str_detect(names(df), "userid")]
#'       for (col in userid_columns) {
#'         df[[col]] <- as.character(df[[col]])
#'       }
#'       return(df)
#'     })
#'     
#'     merged_dataset <- dplyr::bind_rows(standardized_data_list)
#'     
#'     if (verbose_logging) {
#'       logger::log_info("✅ Successfully combined data frames")
#'       logger::log_info("📊 Merged dataset: {nrow(merged_dataset)} rows, {ncol(merged_dataset)} columns")
#'     }
#'     
#'     # Remove duplicates if requested
#'     if (remove_duplicates) {
#'       original_row_count <- nrow(merged_dataset)
#'       
#'       # Remove duplicates based on all columns except source_file
#'       if (add_source_file_column) {
#'         merged_dataset <- merged_dataset %>%
#'           dplyr::distinct(dplyr::across(-source_file), .keep_all = TRUE)
#'       } else {
#'         merged_dataset <- merged_dataset %>%
#'           dplyr::distinct()
#'       }
#'       
#'       duplicates_removed <- original_row_count - nrow(merged_dataset)
#'       
#'       if (verbose_logging) {
#'         logger::log_info("🧹 Removed {duplicates_removed} duplicate rows")
#'         logger::log_info("📊 Final dataset: {nrow(merged_dataset)} rows")
#'       }
#'     }
#'     
#'     # Save to file if requested
#'     if (!is.null(output_file_path)) {
#'       if (verbose_logging) {
#'         logger::log_info("💾 Saving merged dataset to: {output_file_path}")
#'       }
#'       
#'       # Create output directory if it doesn't exist
#'       output_directory <- dirname(output_file_path)
#'       if (!dir.exists(output_directory)) {
#'         dir.create(output_directory, recursive = TRUE)
#'         if (verbose_logging) {
#'           logger::log_info("📁 Created output directory: {output_directory}")
#'         }
#'       }
#'       
#'       readr::write_csv(merged_dataset, output_file_path)
#'       
#'       if (verbose_logging) {
#'         file_size_mb <- round(file.size(output_file_path) / 1024^2, 2)
#'         logger::log_info("✅ File saved successfully ({file_size_mb} MB)")
#'       }
#'     }
#'     
#'     # Print summary
#'     if (verbose_logging) {
#'       logger::log_info("🏁 CSV merge completed successfully")
#'       logger::log_info("=" %>% rep(60) %>% paste(collapse = ""))
#'       logger::log_info("📈 MERGE SUMMARY:")
#'       logger::log_info("  • Total files found: {length(csv_file_paths)}")
#'       logger::log_info("  • Successfully read: {length(successful_files)}")
#'       logger::log_info("  • Failed to read: {length(failed_files)}")
#'       logger::log_info("  • Total rows processed: {total_rows_processed}")
#'       logger::log_info("  • Final dataset rows: {nrow(merged_dataset)}")
#'       logger::log_info("  • Final dataset columns: {ncol(merged_dataset)}")
#'       
#'       if (length(failed_files) > 0) {
#'         logger::log_info("  • Failed files: {paste(failed_files, collapse = ', ')}")
#'       }
#'       
#'       if (!is.null(output_file_path)) {
#'         logger::log_info("  • Output file: {output_file_path}")
#'       }
#'       
#'       logger::log_info("=" %>% rep(60) %>% paste(collapse = ""))
#'     }
#'     
#'     return(merged_dataset)
#'     
#'   }, error = function(e) {
#'     if (verbose_logging) {
#'       logger::log_error("❌ Error combining data frames: {e$message}")
#'     }
#'     return(data.frame())
#'   })
#' }
#' 
#' #' @noRd
#' standardize_csv_column_names <- function(data_frame, verbose_logging) {
#'   original_names <- names(data_frame)
#'   
#'   # Common column name mappings
#'   name_mappings <- list(
#'     # Physician identification
#'     "physician_name" = "name",
#'     "doctor_name" = "name",
#'     "physician_id" = "userid",
#'     "doctor_id" = "userid",
#'     "abog_id" = "abog_id_number",
#'     "id" = "userid",
#'     
#'     # Subspecialty information
#'     "subspecialty" = "subspecialty_name",
#'     "specialty" = "subspecialty_name",
#'     "sub_specialty" = "subspecialty_name",
#'     
#'     # Certification information
#'     "cert_date" = "primary_cert_date",
#'     "certification_date" = "primary_cert_date",
#'     "cert_status" = "primary_cert_status",
#'     "certification_status" = "primary_cert_status",
#'     
#'     # Location information
#'     "state_name" = "state",
#'     "city_name" = "city"
#'   )
#'   
#'   # Apply name mappings
#'   new_names <- original_names
#'   for (old_name in names(name_mappings)) {
#'     if (old_name %in% original_names) {
#'       new_name <- name_mappings[[old_name]]
#'       new_names[which(original_names == old_name)] <- new_name
#'       
#'       if (verbose_logging) {
#'         logger::log_info("    🔄 Column renamed: '{old_name}' → '{new_name}'")
#'       }
#'     }
#'   }
#'   
#'   # Clean column names (remove special characters, convert to lowercase)
#'   cleaned_names <- new_names %>%
#'     stringr::str_replace_all("[^a-zA-Z0-9_]", "_") %>%
#'     stringr::str_replace_all("_{2,}", "_") %>%
#'     stringr::str_replace_all("^_|_$", "") %>%
#'     stringr::str_to_lower()
#'   
#'   # Apply cleaned names
#'   names(data_frame) <- cleaned_names
#'   
#'   return(data_frame)
#' }
#' 
#' #run ----
#' merged_data <- merge_all_csv_files_in_directory(
#'   directory_path = "physician_data/discovery_results",
#'   output_file_path = "physician_data/merged_discovery_results.csv",
#'   verbose_logging = TRUE,
#'   standardize_column_names = TRUE,
#'   add_source_file_column = TRUE
#' )
#' View(merged_data)
#' glimpse(merged_data)
#' str(merged_data)
#' 
#' # Coalesce all userid columns into the main userid column ----
#' merged_data_consolidated <- merged_data %>%
#'   # Get all userid column names
#'   {
#'     userid_cols <- names(.)[str_detect(names(.), "^userid")]
#'     print(paste("Found userid columns:", paste(userid_cols, collapse = ", ")))
#'     
#'     # Coalesce all userid columns into the first one (userid)
#'     mutate(., userid = coalesce(!!!syms(userid_cols))) %>%
#'       
#'       # Remove all the other userid... columns
#'       select(-any_of(userid_cols[-1]))  # Keep the first one (userid), remove the rest
#'   }
#' 
#' # Check the results
#' glimpse(merged_data_consolidated)
#' 
#' # Verify no data was lost in the userid column
#' cat("Original userid NAs:", sum(is.na(merged_data$userid)), "\n")
#' cat("Consolidated userid NAs:", sum(is.na(merged_data_consolidated$userid)), "\n")
#' cat("Total rows maintained:", nrow(merged_data_consolidated), "\n")
#' 
#' # ----
#' #View(merged_data_consolidated)
#' 
#' # Fill in variables and chose to remove duplicates with sub1 ----
#' merged_data_deduplicated <- merged_data %>%
#'   dplyr::group_by(userid) %>%
#'   dplyr::arrange(userid) %>%
#'   
#'   # Fill down first
#'   tidyr::fill(
#'     name, subspecialty_name, primary_certification, 
#'     physician_city, physician_state, physician_location, city, state, id, clinicallyactive, primary_certification, clinically_active_status, clinicallyactive, sub1, 
#'     .direction = "down"
#'   ) %>%
#'   
#'   # Then fill up to catch any remaining NAs
#'   tidyr::fill(
#'     name, subspecialty_name, primary_certification, 
#'     physician_city, physician_state, physician_location, city, state, id, clinicallyactive, primary_certification, clinically_active_status, clinicallyactive, sub1, 
#'     .direction = "up"
#'   ) %>%
#'   
#'   # Then select the best row
#'   dplyr::slice_max(
#'     order_by = !is.na(sub1) | !is.na(subspecialty_name),
#'     n = 1,
#'     with_ties = FALSE
#'   ) %>%
#'   dplyr::ungroup()
#' 
#' View(merged_data_deduplicated)
#' 
#' # Or use table() to include NAs
#' table(merged_data_consolidated$subspecialty_name, useNA = "always")
#' 
#' # Calculate NA percentage for each column
#' na_summary <- merged_data_deduplicated %>%
#'   dplyr::summarise(across(everything(), ~ round(mean(is.na(.)) * 100, 1))) %>%
#'   tidyr::pivot_longer(everything(), names_to = "column", values_to = "na_percentage") %>%
#'   dplyr::arrange(desc(na_percentage))
#' 
#' # View the summary
#' print(na_summary)
#' 
#' # Show columns with >99% NAs ----
#' high_na_columns <- na_summary %>%
#'   dplyr::filter(na_percentage > 99) %>%
#'   dplyr::pull(column)
#' 
#' print(paste("Columns with >99% NAs:", paste(high_na_columns, collapse = ", ")))
#' 
#' cleaned_data <- merged_data_deduplicated %>%
#'   dplyr::select(-all_of(high_na_columns)) %>%
#'   dplyr::select(-abog_id_number) %>%
#'   dplyr::arrange(userid) %>%
#'   readr::write_csv("data/0-Download/output/merged_data_deduplicated.csv")
#' # View(cleaned_data)
#' 
#' miss_var_summary(cleaned_data)
#' 

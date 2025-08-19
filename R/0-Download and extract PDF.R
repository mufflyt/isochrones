# Setup and Configuration ----
source("R/01-setup.R")

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
  request_delay_seconds = 2.0,
  chunk_size_records = 100,
  cleanup_downloaded_pdfs = FALSE,
  save_individual_files = TRUE,
  verbose_extraction_logging = TRUE
)

# For ugoryn only.  
next_ids <- read_csv("data/0-Download/output/fpmrs_ids_only_abog_provider_dataframe.csv") %>%
  pull(abog_id) %>%  # Extract the column as a vector
  as.numeric()

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

# Make function where we sort duplicate abog_id by subspecialty AND date of data pulled----
#' Extract ABOG Provider Data from Files
#' 
#' Reads your extraction files and returns a clean dataframe with all ABOG providers
extract_abog_provider_data <- function(search_directory, file_pattern, verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting ABOG provider data extraction")
    logger::log_info("Search directories: {paste(search_directory, collapse = ', ')}")
    logger::log_info("File pattern: {file_pattern}")
  }
  
  # Get all extraction files from all directories
  extraction_files <- c()
  
  for (directory in search_directory) {
    if (dir.exists(directory)) {
      files_in_dir <- list.files(
        path = directory,
        pattern = file_pattern,
        full.names = TRUE,
        recursive = TRUE
      )
      extraction_files <- c(extraction_files, files_in_dir)
      if (verbose) {
        logger::log_info("Found {length(files_in_dir)} files in {directory}")
      }
    } else {
      if (verbose) {
        logger::log_warn("Directory does not exist: {directory}")
      }
    }
  }
  
  if (verbose) {
    logger::log_info("Total files found: {length(extraction_files)}")
  }
  
  # Read and combine all files
  all_provider_data <- data.frame()
  
  for(i in seq_along(extraction_files)) {
    if(i %% 20 == 0 && verbose) {
      logger::log_info("Processing file {i} of {length(extraction_files)}")
    }
    
    file_data <- readr::read_csv(extraction_files[i], show_col_types = FALSE)
    
    # Standardize column names
    file_data <- standardize_provider_column_names(file_data, verbose = verbose)
    
    # Add source file and file creation time for tracking
    file_data$source_file <- basename(extraction_files[i])
    file_data$file_creation_time <- as.POSIXct(file.info(extraction_files[i])$ctime)
    
    # Combine data
    if(nrow(all_provider_data) == 0) {
      all_provider_data <- file_data
    } else {
      all_provider_data <- dplyr::bind_rows(all_provider_data, file_data)
    }
  }
  
  if (verbose) {
    logger::log_info("Data extraction completed")
    logger::log_info("Total records: {nrow(all_provider_data)}")
    logger::log_info("Unique ABOG IDs: {length(unique(all_provider_data$abog_id))}")
    logger::log_info("Final columns: {paste(colnames(all_provider_data), collapse = ', ')}")
  }
  
  return(all_provider_data)
}

#' @noRd
standardize_provider_column_names <- function(input_dataframe, verbose = FALSE) {
  
  original_column_names <- colnames(input_dataframe)
  
  # Standardize physician_name variants
  name_column_patterns <- c("name\\.x", "name\\.y", "physician_name", "doctor_name", 
                            "provider_name", "^name$", "full_name", "practitioner_name")
  
  for (pattern in name_column_patterns) {
    matching_columns <- stringr::str_which(tolower(original_column_names), tolower(pattern))
    if (length(matching_columns) > 0) {
      # Use the first match and rename it to physician_name
      target_column <- original_column_names[matching_columns[1]]
      input_dataframe <- input_dataframe %>%
        dplyr::rename(physician_name = !!target_column)
      if (verbose) {
        logger::log_info("Standardized '{target_column}' to 'physician_name'")
      }
      break
    }
  }
  
  # Standardize physician_id/abog_id variants
  id_column_patterns <- c("physician_id", "abog_id", "provider_id", "doctor_id", "id")
  
  if (!"abog_id" %in% colnames(input_dataframe)) {
    for (pattern in id_column_patterns) {
      if (pattern %in% colnames(input_dataframe)) {
        input_dataframe <- input_dataframe %>%
          dplyr::rename(abog_id = !!pattern)
        if (verbose) {
          logger::log_info("Standardized '{pattern}' to 'abog_id'")
        }
        break
      }
    }
  }
  
  # Standardize subspecialty name variants
  subspecialty_column_patterns <- c("subspecialty_name", "sub1", "specialty", "subspecialty")
  
  if (!"subspecialty_name" %in% colnames(input_dataframe)) {
    for (pattern in subspecialty_column_patterns) {
      if (pattern %in% colnames(input_dataframe)) {
        input_dataframe <- input_dataframe %>%
          dplyr::rename(subspecialty_name = !!pattern)
        if (verbose) {
          logger::log_info("Standardized '{pattern}' to 'subspecialty_name'")
        }
        break
      }
    }
  }
  
  # Convert ALL columns to character
  input_dataframe <- input_dataframe %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  
  if (verbose) {
    logger::log_info("Converted all columns to character type")
  }
  
  return(input_dataframe)
}

# run dual priority system ----
# Add this at the top of your script before running the extraction
Sys.setenv("VROOM_CONNECTION_SIZE" = 2097152)  # 2MB buffer (double the default)

abog_provider_dataframe <- extract_abog_provider_data(
  search_directory = c(
    "~/Dropbox (Personal)/isochrones/physician_data",
    "~/Dropbox (Personal)/workforce/Master_Scraper" # can do ~/Dropbox (Personal)/workforce
  ),
  file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$|Physician.*\\.csv$)"
) %>%
  # Filter out deceased providers from certStatus and sub1certStatus columns
  dplyr::filter(
    !stringr::str_detect(tolower(coalesce(certStatus, "")), "deceased"),!stringr::str_detect(tolower(coalesce(sub1certStatus, "")), "deceased")
  ) %>%
  dplyr::mutate(
    subspecialty_name = dplyr::recode(
      subspecialty_name,
      "CFP" = "Complex Family Planning",
      "FPM" = "Female Pelvic Medicine & Reconstructive Surgery",
      "Urogynecology and Reconstructive Pelvic Surgery" = "Female Pelvic Medicine & Reconstructive Surgery",
      "URP" = "Female Pelvic Medicine & Reconstructive Surgery",
      "REI" = "Reproductive Endocrinology and Infertility",
      "PAG" = "Pediatric & Adolescent Gynecology",
      "MFM" = "Maternal-Fetal Medicine",
      "ONC" = "Gynecologic Oncology",
      "HPM" = "Hospice and Palliative Medicine"
    )
  ) %>%
  # Only process existing ID columns
  {
    existing_id_columns <- intersect(c("abog_id", "abog_id_number", "ID", "userid"), colnames(.))
    if (length(existing_id_columns) > 0) {
      dplyr::mutate(., dplyr::across(dplyr::all_of(existing_id_columns), readr::parse_number))
    } else {
      .
    }
  } %>%
  # Coalesce ID columns if they exist
  {
    existing_id_columns <- intersect(c("abog_id", "abog_id_number", "ID", "userid"), colnames(.))
    if (length(existing_id_columns) > 1) {
      dplyr::mutate(., abog_id = dplyr::coalesce(!!!dplyr::syms(existing_id_columns)))
    } else {
      .
    }
  } %>%
  # Remove extra ID columns if they exist
  {
    columns_to_remove <- intersect(c("abog_id_number", "ID", "userid"), colnames(.))
    if (length(columns_to_remove) > 0) {
      dplyr::select(., -dplyr::all_of(columns_to_remove))
    } else {
      .
    }
  } %>%
  dplyr::filter(!is.na(abog_id) & !is.na(physician_name)) %>%
  # DUAL PRIORITY: 1st subspecialty data, 2nd most recent file creation time
  dplyr::arrange(
    abog_id,
    dplyr::desc(
      !is.na(subspecialty_name) &
        subspecialty_name != "NA" & subspecialty_name != ""
    ),
    dplyr::desc(file_creation_time)
  ) %>%
  dplyr::distinct(abog_id, .keep_all = TRUE) %>%
  dplyr::select(-file_creation_time) %>%  # Remove helper column after deduplication
  dplyr::arrange(abog_id)

readr::write_csv(
  abog_provider_dataframe,
  "data/0-Download/output/abog_provider_dataframe_8_17_2025_1948_only_workforce_directory.csv"
)

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

#new ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both). Default is "smart_mixed".
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: Large-scale analysis with extension strategy
#' comprehensive_study <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/national_provider_study_2025",
#'   extraction_file_pattern = "chunk_.*_extractions_.*\\.csv$",
#'   enable_verbose_logging = FALSE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 10000,
#'   search_strategy = "smart_extend"
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct pull case_when
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed") {
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed"))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Process all CSV files and generate analysis
  file_processing_results <- process_healthcare_data_files(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate geographic patterns if location data available
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = search_results_directory,
      verbose_logging = enable_verbose_logging
    )
  }
  
  # Compile final results
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy
    )
  )
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files <- function(directory_path, file_pattern, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = FALSE
  )
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path}"))
  }
  
  if (verbose_logging) {
    logger::log_info("   📁 Found {length(csv_files_list)} matching files")
  }
  
  # Safe file reading function with enhanced error handling
  safe_read_csv <- function(file_path) {
    tryCatch({
      # Try reading with readr first
      file_data <- readr::read_csv(file_path, show_col_types = FALSE, 
                                   col_types = readr::cols(.default = readr::col_character()))
      return(file_data)
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  readr failed for {basename(file_path)}, trying utils::read.csv")
      }
      # Fallback to base R read.csv
      tryCatch({
        file_data <- utils::read.csv(file_path, stringsAsFactors = FALSE)
        return(file_data)
      }, error = function(e2) {
        if (verbose_logging) {
          logger::log_error("   ❌ Both read methods failed for {basename(file_path)}: {e2$message}")
        }
        return(NULL)
      })
    })
  }
  
  # Process each file
  all_provider_data <- purrr::map_dfr(csv_files_list, function(file_path) {
    
    if (verbose_logging) {
      logger::log_info("   📄 Processing: {basename(file_path)}")
    }
    
    current_file_data <- safe_read_csv(file_path)
    
    if (is.null(current_file_data)) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read: {basename(file_path)}")
      }
      return(NULL)
    }
    
    # Check if file is empty
    if (nrow(current_file_data) == 0) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Empty file: {basename(file_path)}")
      }
      return(NULL)
    }
    
    # Standardize column names and ensure consistent data types
    processed_file_data <- standardize_provider_data_columns(current_file_data, file_path, verbose_logging)
    
    return(processed_file_data)
  })
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    total_provider_records = nrow(all_provider_data)
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(all_provider_data), big.mark = ',', format = 'd')}")
  }
  
  return(list(
    combined_provider_data = all_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns <- function(file_data, file_path, verbose_logging) {
  
  # Convert ALL columns to character to avoid any type mismatch issues
  file_data <- file_data %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  
  # Handle common column name variations for provider_id
  if ("physician_id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    file_data <- file_data %>%
      dplyr::mutate(provider_id = physician_id)
  }
  
  if ("id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    file_data <- file_data %>%
      dplyr::mutate(provider_id = id)
  }
  
  if ("physician_ids" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    file_data <- file_data %>%
      dplyr::mutate(provider_id = physician_ids)
  }
  
  if ("npi" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    file_data <- file_data %>%
      dplyr::mutate(provider_id = npi)
  }
  
  # Add file source for tracking (already character)
  file_data <- file_data %>%
    dplyr::mutate(
      source_file = basename(file_path),
      processing_timestamp = as.character(lubridate::now())
    )
  
  return(file_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Extract all provider IDs and analyze patterns
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id)) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    stop("No valid provider IDs found in data")
  }
  
  min_provider_id <- min(all_provider_ids, na.rm = TRUE)
  max_provider_id <- max(all_provider_ids, na.rm = TRUE)
  expected_total_ids <- max_provider_id - min_provider_id + 1
  
  # Identify missing IDs in the range
  complete_id_sequence <- seq(min_provider_id, max_provider_id)
  missing_provider_ids <- setdiff(complete_id_sequence, all_provider_ids)
  
  # Identify non-existent IDs from failed searches
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique()
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_provider_ids <- setdiff(missing_provider_ids, non_existent_ids)
  
  # Calculate existence rate
  existence_rate <- round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  
  smart_analysis_summary <- list(
    min_id_searched = min_provider_id,
    max_id_searched = max_provider_id,
    total_ids_found = length(all_provider_ids),
    expected_total_in_range = expected_total_ids,
    missing_provider_ids = missing_provider_ids,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_provider_ids,
    existence_rate = existence_rate,
    coverage_completeness = round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Smart gap analysis completed")
    logger::log_info("   🆔 ID range: {formatC(min_provider_id, big.mark = ',', format = 'd')} to {formatC(max_provider_id, big.mark = ',', format = 'd')}")
    logger::log_info("   ✅ Found: {formatC(length(all_provider_ids), big.mark = ',', format = 'd')} providers")
    logger::log_info("   ❓ Missing: {formatC(length(missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   ❌ Non-existent: {formatC(length(non_existent_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   🎯 Truly missing: {formatC(length(truly_missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   📊 Existence rate: {existence_rate}%")
  }
  
  return(smart_analysis_summary)
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(geographic_analysis_available = FALSE))
  }
  
  geographic_summary <- provider_data %>%
    dplyr::group_by(across(all_of(available_geo_columns[1]))) %>%
    dplyr::summarise(
      provider_count = dplyr::n(),
      unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(provider_count))
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Geographic units analyzed: {nrow(geographic_summary)}")
  }
  
  return(list(
    geographic_analysis_available = TRUE,
    geographic_distribution = geographic_summary,
    geographic_columns_used = available_geo_columns
  ))
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
  }
  
  next_search_provider_ids <- c()
  
  if (search_strategy == "smart_gaps" || search_strategy == "smart_mixed") {
    # Prioritize truly missing IDs
    truly_missing_count <- min(batch_size, length(analysis_results$truly_missing_provider_ids))
    if (truly_missing_count > 0) {
      next_search_provider_ids <- c(next_search_provider_ids, 
                                    analysis_results$truly_missing_provider_ids[1:truly_missing_count])
    }
  }
  
  if (search_strategy == "smart_extend" || search_strategy == "smart_mixed") {
    # Add new IDs beyond current maximum
    remaining_batch_space <- batch_size - length(next_search_provider_ids)
    if (remaining_batch_space > 0) {
      new_id_start <- analysis_results$max_id_searched + 1
      new_id_end <- new_id_start + remaining_batch_space - 1
      new_provider_ids <- seq(new_id_start, new_id_end)
      next_search_provider_ids <- c(next_search_provider_ids, new_provider_ids)
    }
  }
  
  # Remove any known non-existent IDs and deduplicate
  next_search_provider_ids <- setdiff(next_search_provider_ids, analysis_results$non_existent_provider_ids)
  next_search_provider_ids <- sort(unique(next_search_provider_ids))
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🔍 Recommended IDs: {formatC(length(next_search_provider_ids), big.mark = ',', format = 'd')}")
    if (length(next_search_provider_ids) > 0) {
      logger::log_info("   📊 ID range: {formatC(min(next_search_provider_ids), big.mark = ',', format = 'd')} to {formatC(max(next_search_provider_ids), big.mark = ',', format = 'd')}")
    }
  }
  
  return(next_search_provider_ids)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_file_path <- fs::path(output_directory, 
                                       glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv"))
    
    data.frame(provider_id = non_existent_provider_ids) %>%
      readr::write_csv(non_existent_file_path)
    
    if (verbose_logging) {
      logger::log_info("   📄 Non-existent IDs saved: {non_existent_file_path}")
    }
  }
  
  # Save truly missing IDs
  if (length(truly_missing_provider_ids) > 0) {
    missing_file_path <- fs::path(output_directory, 
                                  glue::glue("truly_missing_provider_ids_{timestamp_suffix}.csv"))
    
    data.frame(provider_id = truly_missing_provider_ids) %>%
      readr::write_csv(missing_file_path)
    
    if (verbose_logging) {
      logger::log_info("   📄 Missing IDs saved: {missing_file_path}")
    }
  }
}

# run ----
# ============================================================================
# MAIN ANALYSIS EXECUTION
# ============================================================================

# Run the comprehensive healthcare access analysis
healthcare_access_analysis_results <- analyze_healthcare_access_patterns(
  search_results_directory = "physician_data",
  extraction_file_pattern = ".*\\.csv$",  # This will catch all CSV files
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE,
  batch_size_for_search = 5000,
  search_strategy = "smart_mixed"
)

# ============================================================================
# GENERATE NEXT SEARCH RECOMMENDATIONS  
# ============================================================================

# Generate intelligent search list for next data collection phase
next_ids <- generate_intelligent_search_list(
  analysis_results = healthcare_access_analysis_results$smart_analysis,
  search_strategy = "smart_mixed",
  batch_size = 5000,
  verbose_logging = TRUE
)

# Duckplyr using and top to bottom ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param search_subdirectories Logical. Whether to search recursively through
#'   subdirectories for CSV files. When TRUE, searches all subdirectories within
#'   search_results_directory. Default is FALSE for top-level only.
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: Memory-efficient analysis with chunking
#' low_memory_study <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   chunk_processing = TRUE,
#'   max_chunk_size = 50000
#' )
#'
#' @importFrom duckplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything
#' @importFrom dplyr pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE) {
  
  # Load required packages (if not already loaded)
  if (!require("duckplyr", quietly = TRUE)) {
    if (!require("dplyr", quietly = TRUE)) {
      stop("Neither duckplyr nor dplyr is available. Please install one of them.")
    }
    if (enable_verbose_logging) {
      logger::log_info("   ℹ️  Using dplyr (duckplyr not available)")
    }
  } else {
    if (enable_verbose_logging) {
      logger::log_info("   🦆 Using duckplyr for enhanced performance")
    }
  }
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files and generate analysis with memory management
  file_processing_results <- process_healthcare_data_files(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files <- function(directory_path, file_pattern, verbose_logging, 
                                          keep_only_summary = FALSE, chunk_processing = FALSE, 
                                          max_chunk_size = 100000, search_subdirectories = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories  # Now uses the parameter
  )
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path}"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only)")
    }
  }
  
  # Enhanced safe file reading function with chunking
  safe_read_csv_chunked <- function(file_path) {
    tryCatch({
      # Check file size first
      file_info <- file.info(file_path)
      file_size_mb <- file_info$size / (1024^2)
      
      if (verbose_logging && file_size_mb > 100) {
        logger::log_info("   ⚠️  Large file detected ({round(file_size_mb, 1)} MB): {basename(file_path)}")
      }
      
      # If chunk processing enabled and file is large, read in chunks
      if (chunk_processing && file_size_mb > 50) {
        if (verbose_logging) {
          logger::log_info("   🔄 Processing {basename(file_path)} in chunks to save memory")
        }
        
        # Read file in chunks and keep only essential data
        all_chunks <- list()
        chunk_num <- 1
        
        # Use data.table::fread for faster chunked reading if available
        if (requireNamespace("data.table", quietly = TRUE)) {
          repeat {
            chunk_data <- data.table::fread(
              file_path, 
              skip = (chunk_num - 1) * max_chunk_size,
              nrows = max_chunk_size,
              showProgress = FALSE
            )
            
            if (nrow(chunk_data) == 0) break
            
            # Convert to tibble and process
            chunk_data <- dplyr::as_tibble(chunk_data)
            chunk_data <- standardize_provider_data_columns(chunk_data, file_path, FALSE)
            
            # Keep all columns when not in summary mode, or only essential when in summary mode
            if (keep_only_summary && "provider_id" %in% colnames(chunk_data)) {
              essential_columns <- c("provider_id", "source_file")
              geographic_columns <- c("city", "state", "county", "zip_code")
              available_geo_columns <- intersect(names(chunk_data), geographic_columns)
              keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
              keep_columns <- intersect(keep_columns, colnames(chunk_data))
              chunk_data <- chunk_data %>% dplyr::select(all_of(keep_columns))
            }
            # If keep_only_summary is FALSE, we keep all columns as-is
            
            all_chunks[[chunk_num]] <- chunk_data
            chunk_num <- chunk_num + 1
            
            # Force garbage collection every 10 chunks
            if (chunk_num %% 10 == 0) {
              gc()
            }
          }
        } else {
          # Fallback to readr with chunking
          chunk_data <- readr::read_csv(file_path, 
                                        col_types = readr::cols(.default = readr::col_character()),
                                        n_max = max_chunk_size,
                                        show_col_types = FALSE)
          all_chunks[[1]] <- standardize_provider_data_columns(chunk_data, file_path, FALSE)
        }
        
        # Combine chunks efficiently
        if (requireNamespace("data.table", quietly = TRUE)) {
          combined_data <- data.table::rbindlist(all_chunks, fill = TRUE) %>% dplyr::as_tibble()
        } else {
          combined_data <- dplyr::bind_rows(all_chunks)
        }
        
        # Clear chunks from memory
        rm(all_chunks)
        gc()
        
        return(combined_data)
        
      } else {
        # Standard reading for smaller files
        file_data <- readr::read_csv(file_path, show_col_types = FALSE, 
                                     col_types = readr::cols(.default = readr::col_character()))
        return(file_data)
      }
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  readr failed for {basename(file_path)}, trying utils::read.csv")
      }
      # Fallback to base R read.csv
      tryCatch({
        file_data <- utils::read.csv(file_path, stringsAsFactors = FALSE)
        return(file_data)
      }, error = function(e2) {
        if (verbose_logging) {
          logger::log_error("   ❌ Both read methods failed for {basename(file_path)}: {e2$message}")
        }
        return(NULL)
      })
    })
  }
  
  # Process files one at a time to minimize memory usage
  all_provider_data <- NULL
  
  for (i in seq_along(csv_files_list)) {
    file_path <- csv_files_list[i]
    
    if (verbose_logging) {
      logger::log_info("   📄 Processing: {basename(file_path)} ({i}/{length(csv_files_list)})")
    }
    
    current_file_data <- safe_read_csv_chunked(file_path)
    
    if (is.null(current_file_data)) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read: {basename(file_path)}")
      }
      next
    }
    
    # Check if file is empty
    if (nrow(current_file_data) == 0) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Empty file: {basename(file_path)}")
      }
      next
    }
    
    # Standardize column names and ensure consistent data types
    if (!chunk_processing) {  # Only standardize if not already done in chunking
      processed_file_data <- standardize_provider_data_columns(current_file_data, file_path, verbose_logging)
    } else {
      processed_file_data <- current_file_data
    }
    
    # Apply column filtering only if keep_only_summary is TRUE
    if (keep_only_summary && "provider_id" %in% colnames(processed_file_data)) {
      essential_columns <- c("provider_id", "source_file")
      geographic_columns <- c("city", "state", "county", "zip_code")
      available_geo_columns <- intersect(names(processed_file_data), geographic_columns)
      
      keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
      keep_columns <- intersect(keep_columns, colnames(processed_file_data))
      
      processed_file_data <- processed_file_data %>%
        dplyr::select(all_of(keep_columns))
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
      }
    } else {
      if (verbose_logging && "provider_id" %in% colnames(processed_file_data)) {
        logger::log_info("   📋 Keeping all {ncol(processed_file_data)} columns as requested")
      }
    }
    
    # Combine with existing data efficiently
    if (is.null(all_provider_data)) {
      all_provider_data <- processed_file_data
    } else {
      if (requireNamespace("data.table", quietly = TRUE)) {
        all_provider_data <- data.table::rbindlist(
          list(all_provider_data, processed_file_data), 
          fill = TRUE
        ) %>% dplyr::as_tibble()
      } else {
        all_provider_data <- dplyr::bind_rows(all_provider_data, processed_file_data)
      }
    }
    
    # Clear current file data from memory
    rm(current_file_data, processed_file_data)
    
    # Force garbage collection after each file
    gc()
    
    if (verbose_logging) {
      current_memory <- utils::object.size(all_provider_data) / (1024^2)
      logger::log_info("   💾 Current data size: {round(current_memory, 1)} MB")
    }
  }
  
  # Final garbage collection
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    total_provider_records = nrow(all_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(all_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(all_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
  }
  
  return(list(
    combined_provider_data = all_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns <- function(file_data, file_path, verbose_logging) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  if (requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(duckplyr::across(duckplyr::everything(), as.character))
  } else {
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id
  if ("physician_id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = physician_id)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = physician_id)
    }
  }
  
  if ("id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = id)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = id)
    }
  }
  
  if ("physician_ids" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = physician_ids)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = physician_ids)
    }
  }
  
  if ("npi" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = npi)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = npi)
    }
  }
  
  # Add file source for tracking (already character)
  if (requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(
        source_file = basename(file_path),
        processing_timestamp = as.character(lubridate::now())
      )
  } else {
    file_data <- file_data %>%
      dplyr::mutate(
        source_file = basename(file_path),
        processing_timestamp = as.character(lubridate::now())
      )
  }
  
  return(file_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Use dplyr for all operations since duckplyr has limited function support
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id)) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    stop("No valid provider IDs found in data")
  }
  
  min_provider_id <- min(all_provider_ids, na.rm = TRUE)
  max_provider_id <- max(all_provider_ids, na.rm = TRUE)
  expected_total_ids <- max_provider_id - min_provider_id + 1
  
  # Identify missing IDs in the range
  complete_id_sequence <- seq(min_provider_id, max_provider_id)
  missing_provider_ids <- setdiff(complete_id_sequence, all_provider_ids)
  
  # Identify non-existent IDs from failed searches - use dplyr for all operations
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique()
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_provider_ids <- setdiff(missing_provider_ids, non_existent_ids)
  
  # Calculate existence rate
  existence_rate <- round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  
  smart_analysis_summary <- list(
    min_id_searched = min_provider_id,
    max_id_searched = max_provider_id,
    total_ids_found = length(all_provider_ids),
    expected_total_in_range = expected_total_ids,
    missing_provider_ids = missing_provider_ids,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_provider_ids,
    existence_rate = existence_rate,
    coverage_completeness = round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Smart gap analysis completed")
    logger::log_info("   🆔 ID range: {formatC(min_provider_id, big.mark = ',', format = 'd')} to {formatC(max_provider_id, big.mark = ',', format = 'd')}")
    logger::log_info("   ✅ Found: {formatC(length(all_provider_ids), big.mark = ',', format = 'd')} providers")
    logger::log_info("   ❓ Missing: {formatC(length(missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   ❌ Non-existent: {formatC(length(non_existent_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   🎯 Truly missing: {formatC(length(truly_missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   📊 Existence rate: {existence_rate}%")
  }
  
  return(smart_analysis_summary)
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(geographic_analysis_available = FALSE))
  }
  
  # Use dplyr for all operations since duckplyr has limited support
  geographic_summary <- provider_data %>%
    dplyr::group_by(across(all_of(available_geo_columns[1]))) %>%
    dplyr::summarise(
      provider_count = dplyr::n(),
      unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(provider_count))
  
  # If space-saving mode, keep only top geographic areas
  if (keep_only_summary && nrow(geographic_summary) > 100) {
    geographic_summary <- geographic_summary %>%
      dplyr::slice_head(n = 100)
    
    if (verbose_logging) {
      logger::log_info("   🧹 Space-saving: kept only top 100 geographic areas")
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Geographic units analyzed: {nrow(geographic_summary)}")
  }
  
  return(list(
    geographic_analysis_available = TRUE,
    geographic_distribution = geographic_summary,
    geographic_columns_used = available_geo_columns
  ))
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
  }
  
  next_search_provider_ids <- c()
  
  # Standard low-to-high strategies
  if (search_strategy == "smart_gaps" || search_strategy == "smart_mixed") {
    # Prioritize truly missing IDs (ascending order)
    truly_missing_count <- min(batch_size, length(analysis_results$truly_missing_provider_ids))
    if (truly_missing_count > 0) {
      next_search_provider_ids <- c(next_search_provider_ids, 
                                    analysis_results$truly_missing_provider_ids[1:truly_missing_count])
    }
  }
  
  # High-to-low gap filling strategies
  if (search_strategy == "high_to_low_gaps" || search_strategy == "high_to_low_mixed") {
    # Prioritize truly missing IDs from highest to lowest
    truly_missing_sorted_desc <- sort(analysis_results$truly_missing_provider_ids, decreasing = TRUE)
    truly_missing_count <- min(batch_size, length(truly_missing_sorted_desc))
    if (truly_missing_count > 0) {
      next_search_provider_ids <- c(next_search_provider_ids, 
                                    truly_missing_sorted_desc[1:truly_missing_count])
    }
  }
  
  if (search_strategy == "smart_extend" || search_strategy == "smart_mixed") {
    # Add new IDs beyond current maximum (ascending)
    remaining_batch_space <- batch_size - length(next_search_provider_ids)
    if (remaining_batch_space > 0) {
      new_id_start <- analysis_results$max_id_searched + 1
      new_id_end <- new_id_start + remaining_batch_space - 1
      new_provider_ids <- seq(new_id_start, new_id_end)
      next_search_provider_ids <- c(next_search_provider_ids, new_provider_ids)
    }
  }
  
  if (search_strategy == "high_to_low_extend" || search_strategy == "high_to_low_mixed") {
    # Add new IDs beyond current maximum (going even higher)
    remaining_batch_space <- batch_size - length(next_search_provider_ids)
    if (remaining_batch_space > 0) {
      new_id_start <- analysis_results$max_id_searched + 1
      new_id_end <- new_id_start + remaining_batch_space - 1
      new_provider_ids <- seq(new_id_start, new_id_end)
      # Sort in descending order for high-to-low approach
      new_provider_ids <- sort(new_provider_ids, decreasing = TRUE)
      next_search_provider_ids <- c(next_search_provider_ids, new_provider_ids)
    }
  }
  
  # Remove any known non-existent IDs and deduplicate
  next_search_provider_ids <- setdiff(next_search_provider_ids, analysis_results$non_existent_provider_ids)
  
  # Sort based on strategy
  if (grepl("high_to_low", search_strategy)) {
    next_search_provider_ids <- sort(unique(next_search_provider_ids), decreasing = TRUE)
    if (verbose_logging) {
      logger::log_info("   📈 Using high-to-low search strategy (newest physicians first)")
    }
  } else {
    next_search_provider_ids <- sort(unique(next_search_provider_ids))
    if (verbose_logging) {
      logger::log_info("   📉 Using low-to-high search strategy (oldest physicians first)")
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🔍 Recommended IDs: {formatC(length(next_search_provider_ids), big.mark = ',', format = 'd')}")
    if (length(next_search_provider_ids) > 0) {
      logger::log_info("   📊 ID range: {formatC(min(next_search_provider_ids), big.mark = ',', format = 'd')} to {formatC(max(next_search_provider_ids), big.mark = ',', format = 'd')}")
      
      # Show strategy-specific sample
      if (grepl("high_to_low", search_strategy)) {
        if (length(next_search_provider_ids) <= 10) {
          logger::log_info("   🆔 Starting from highest: {paste(next_search_provider_ids, collapse = ', ')}")
        } else {
          first_few <- paste(head(next_search_provider_ids, 5), collapse = ", ")
          last_few <- paste(tail(next_search_provider_ids, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from highest: {first_few} ... ending with: {last_few}")
        }
      } else {
        if (length(next_search_provider_ids) <= 10) {
          logger::log_info("   🆔 Starting from lowest: {paste(next_search_provider_ids, collapse = ', ')}")
        } else {
          first_few <- paste(head(next_search_provider_ids, 5), collapse = ", ")
          last_few <- paste(tail(next_search_provider_ids, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from lowest: {first_few} ... ending with: {last_few}")
        }
      }
    }
  }
  
  return(next_search_provider_ids)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    if (compress_files) {
      non_existent_file_path <- fs::path(output_directory, 
                                         glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv.gz"))
      
      data.frame(provider_id = non_existent_provider_ids) %>%
        readr::write_csv(gzfile(non_existent_file_path))
    } else {
      non_existent_file_path <- fs::path(output_directory, 
                                         glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv"))
      
      data.frame(provider_id = non_existent_provider_ids) %>%
        readr::write_csv(non_existent_file_path)
    }
    
    if (verbose_logging) {
      logger::log_info("   📄 Non-existent IDs saved: {non_existent_file_path}")
    }
  }
  
  # Save truly missing IDs (sample only if too large to save space)
  if (length(truly_missing_provider_ids) > 0) {
    # If there are more than 1 million missing IDs, save only a sample to save space
    if (length(truly_missing_provider_ids) > 1000000) {
      sample_size <- 100000
      sampled_missing_ids <- sample(truly_missing_provider_ids, sample_size)
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: sampling {formatC(sample_size, big.mark = ',', format = 'd')} missing IDs from {formatC(length(truly_missing_provider_ids), big.mark = ',', format = 'd')} total")
      }
      
      ids_to_save <- sampled_missing_ids
      file_suffix <- "_sampled"
    } else {
      ids_to_save <- truly_missing_provider_ids
      file_suffix <- ""
    }
    
    if (compress_files) {
      missing_file_path <- fs::path(output_directory, 
                                    glue::glue("truly_missing_provider_ids{file_suffix}_{timestamp_suffix}.csv.gz"))
      
      data.frame(provider_id = ids_to_save) %>%
        readr::write_csv(gzfile(missing_file_path))
    } else {
      missing_file_path <- fs::path(output_directory, 
                                    glue::glue("truly_missing_provider_ids{file_suffix}_{timestamp_suffix}.csv"))
      
      data.frame(provider_id = ids_to_save) %>%
        readr::write_csv(missing_file_path)
    }
    
    if (verbose_logging) {
      logger::log_info("   📄 Missing IDs saved: {missing_file_path}")
    }
  }
}

# run ----
# MEMORY-EFFICIENT analysis - should use ~1-2GB instead of 8GB
# This should work perfectly now - complete function with chunking
healthcare_access_analysis_full_columns <- analyze_healthcare_access_patterns(
  search_results_directory = "physician_data",
  extraction_file_pattern = ".*\\.csv$",
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE,
  batch_size_for_search = 5000,
  search_strategy = "high_to_low_mixed",
  external_drive_path = "/Volumes/MufflyNew",
  compress_output = TRUE,
  keep_only_summary = FALSE,                   # Keep ALL columns
  chunk_processing = TRUE,                     # Process in chunks to save RAM
  max_chunk_size = 50000,                      # 50K rows at a time
  search_subdirectories = TRUE
)


# Aug 14 ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: Memory-efficient analysis with chunking
#' low_memory_study <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   chunk_processing = TRUE,
#'   max_chunk_size = 50000
#' )
#'
#' @importFrom duckplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything
#' @importFrom dplyr pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$")) {
  
  # Load required packages (if not already loaded)
  if (!require("duckplyr", quietly = TRUE)) {
    if (!require("dplyr", quietly = TRUE)) {
      stop("Neither duckplyr nor dplyr is available. Please install one of them.")
    }
    if (enable_verbose_logging) {
      logger::log_info("   ℹ️  Using dplyr (duckplyr not available)")
    }
  } else {
    if (enable_verbose_logging) {
      logger::log_info("   🦆 Using duckplyr for enhanced performance")
    }
  }
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files and generate analysis with memory management
  file_processing_results <- process_healthcare_data_files(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files <- function(directory_path, file_pattern, verbose_logging, 
                                          keep_only_summary = FALSE, chunk_processing = FALSE, 
                                          max_chunk_size = 100000, search_subdirectories = FALSE,
                                          exclude_patterns = NULL) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories  # Now uses the parameter
  )
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
    }
  }
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Enhanced safe file reading function with chunking
  safe_read_csv_chunked <- function(file_path) {
    tryCatch({
      # Check file size first
      file_info <- file.info(file_path)
      file_size_mb <- file_info$size / (1024^2)
      
      if (verbose_logging && file_size_mb > 100) {
        logger::log_info("   ⚠️  Large file detected ({round(file_size_mb, 1)} MB): {basename(file_path)}")
      }
      
      # If chunk processing enabled and file is large, read in chunks
      if (chunk_processing && file_size_mb > 50) {
        if (verbose_logging) {
          logger::log_info("   🔄 Processing {basename(file_path)} in chunks to save memory")
        }
        
        # Read file in chunks and keep only essential data
        all_chunks <- list()
        chunk_num <- 1
        
        # Use data.table::fread for faster chunked reading if available
        if (requireNamespace("data.table", quietly = TRUE)) {
          repeat {
            chunk_data <- data.table::fread(
              file_path, 
              skip = (chunk_num - 1) * max_chunk_size,
              nrows = max_chunk_size,
              showProgress = FALSE
            )
            
            if (nrow(chunk_data) == 0) break
            
            # Convert to tibble and process
            chunk_data <- dplyr::as_tibble(chunk_data)
            chunk_data <- standardize_provider_data_columns(chunk_data, file_path, FALSE)
            
            # Keep all columns when not in summary mode, or only essential when in summary mode
            if (keep_only_summary && "provider_id" %in% colnames(chunk_data)) {
              essential_columns <- c("provider_id", "source_file")
              geographic_columns <- c("city", "state", "county", "zip_code")
              available_geo_columns <- intersect(names(chunk_data), geographic_columns)
              keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
              keep_columns <- intersect(keep_columns, colnames(chunk_data))
              chunk_data <- chunk_data %>% dplyr::select(all_of(keep_columns))
            }
            # If keep_only_summary is FALSE, we keep all columns as-is
            
            all_chunks[[chunk_num]] <- chunk_data
            chunk_num <- chunk_num + 1
            
            # Force garbage collection every 10 chunks
            if (chunk_num %% 10 == 0) {
              gc()
            }
          }
        } else {
          # Fallback to readr with chunking
          chunk_data <- readr::read_csv(file_path, 
                                        col_types = readr::cols(.default = readr::col_character()),
                                        n_max = max_chunk_size,
                                        show_col_types = FALSE)
          all_chunks[[1]] <- standardize_provider_data_columns(chunk_data, file_path, FALSE)
        }
        
        # Combine chunks efficiently
        if (requireNamespace("data.table", quietly = TRUE)) {
          combined_data <- data.table::rbindlist(all_chunks, fill = TRUE) %>% dplyr::as_tibble()
        } else {
          combined_data <- dplyr::bind_rows(all_chunks)
        }
        
        # Clear chunks from memory
        rm(all_chunks)
        gc()
        
        return(combined_data)
        
      } else {
        # Standard reading for smaller files
        file_data <- readr::read_csv(file_path, show_col_types = FALSE, 
                                     col_types = readr::cols(.default = readr::col_character()))
        return(file_data)
      }
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  readr failed for {basename(file_path)}, trying utils::read.csv")
      }
      # Fallback to base R read.csv
      tryCatch({
        file_data <- utils::read.csv(file_path, stringsAsFactors = FALSE)
        return(file_data)
      }, error = function(e2) {
        if (verbose_logging) {
          logger::log_error("   ❌ Both read methods failed for {basename(file_path)}: {e2$message}")
        }
        return(NULL)
      })
    })
  }
  
  # Process files one at a time to minimize memory usage
  all_provider_data <- NULL
  
  for (i in seq_along(csv_files_list)) {
    file_path <- csv_files_list[i]
    
    if (verbose_logging) {
      logger::log_info("   📄 Processing: {basename(file_path)} ({i}/{length(csv_files_list)})")
    }
    
    current_file_data <- safe_read_csv_chunked(file_path)
    
    if (is.null(current_file_data)) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read: {basename(file_path)}")
      }
      next
    }
    
    # Check if file is empty
    if (nrow(current_file_data) == 0) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Empty file: {basename(file_path)}")
      }
      next
    }
    
    # Standardize column names and ensure consistent data types
    if (!chunk_processing) {  # Only standardize if not already done in chunking
      processed_file_data <- standardize_provider_data_columns(current_file_data, file_path, verbose_logging)
    } else {
      processed_file_data <- current_file_data
    }
    
    # Apply column filtering only if keep_only_summary is TRUE
    if (keep_only_summary && "provider_id" %in% colnames(processed_file_data)) {
      essential_columns <- c("provider_id", "source_file")
      geographic_columns <- c("city", "state", "county", "zip_code")
      available_geo_columns <- intersect(names(processed_file_data), geographic_columns)
      
      keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
      keep_columns <- intersect(keep_columns, colnames(processed_file_data))
      
      processed_file_data <- processed_file_data %>%
        dplyr::select(all_of(keep_columns))
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
      }
    } else {
      if (verbose_logging && "provider_id" %in% colnames(processed_file_data)) {
        logger::log_info("   📋 Keeping all {ncol(processed_file_data)} columns as requested")
      }
    }
    
    # Combine with existing data efficiently
    if (is.null(all_provider_data)) {
      all_provider_data <- processed_file_data
    } else {
      if (requireNamespace("data.table", quietly = TRUE)) {
        all_provider_data <- data.table::rbindlist(
          list(all_provider_data, processed_file_data), 
          fill = TRUE
        ) %>% dplyr::as_tibble()
      } else {
        all_provider_data <- dplyr::bind_rows(all_provider_data, processed_file_data)
      }
    }
    
    # Clear current file data from memory
    rm(current_file_data, processed_file_data)
    
    # Force garbage collection after each file
    gc()
    
    if (verbose_logging) {
      current_memory <- utils::object.size(all_provider_data) / (1024^2)
      logger::log_info("   💾 Current data size: {round(current_memory, 1)} MB")
    }
  }
  
  # Final garbage collection
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    total_provider_records = nrow(all_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(all_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(all_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
  }
  
  return(list(
    combined_provider_data = all_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns <- function(file_data, file_path, verbose_logging) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  if (requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(duckplyr::across(duckplyr::everything(), as.character))
  } else {
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id
  if ("physician_id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = physician_id)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = physician_id)
    }
  }
  
  if ("id" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = id)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = id)
    }
  }
  
  if ("physician_ids" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = physician_ids)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = physician_ids)
    }
  }
  
  if ("npi" %in% colnames(file_data) && !"provider_id" %in% colnames(file_data)) {
    if (requireNamespace("duckplyr", quietly = TRUE)) {
      file_data <- file_data %>% duckplyr::mutate(provider_id = npi)
    } else {
      file_data <- file_data %>% dplyr::mutate(provider_id = npi)
    }
  }
  
  # Add file source for tracking (already character)
  if (requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(
        source_file = basename(file_path),
        processing_timestamp = as.character(lubridate::now())
      )
  } else {
    file_data <- file_data %>%
      dplyr::mutate(
        source_file = basename(file_path),
        processing_timestamp = as.character(lubridate::now())
      )
  }
  
  return(file_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Use dplyr for all operations since duckplyr has limited function support
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id)) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    stop("No valid provider IDs found in data")
  }
  
  min_provider_id <- min(all_provider_ids, na.rm = TRUE)
  max_provider_id <- max(all_provider_ids, na.rm = TRUE)
  expected_total_ids <- max_provider_id - min_provider_id + 1
  
  # Identify missing IDs in the range
  complete_id_sequence <- seq(min_provider_id, max_provider_id)
  missing_provider_ids <- setdiff(complete_id_sequence, all_provider_ids)
  
  # Identify non-existent IDs from failed searches - use dplyr for all operations
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique()
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_provider_ids <- setdiff(missing_provider_ids, non_existent_ids)
  
  # Calculate existence rate
  existence_rate <- round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  
  smart_analysis_summary <- list(
    min_id_searched = min_provider_id,
    max_id_searched = max_provider_id,
    total_ids_found = length(all_provider_ids),
    expected_total_in_range = expected_total_ids,
    missing_provider_ids = missing_provider_ids,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_provider_ids,
    existence_rate = existence_rate,
    coverage_completeness = round((length(all_provider_ids) / expected_total_ids) * 100, 2)
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Smart gap analysis completed")
    logger::log_info("   🆔 ID range: {formatC(min_provider_id, big.mark = ',', format = 'd')} to {formatC(max_provider_id, big.mark = ',', format = 'd')}")
    logger::log_info("   ✅ Found: {formatC(length(all_provider_ids), big.mark = ',', format = 'd')} providers")
    logger::log_info("   ❓ Missing: {formatC(length(missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   ❌ Non-existent: {formatC(length(non_existent_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   🎯 Truly missing: {formatC(length(truly_missing_provider_ids), big.mark = ',', format = 'd')} IDs")
    logger::log_info("   📊 Existence rate: {existence_rate}%")
  }
  
  return(smart_analysis_summary)
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(geographic_analysis_available = FALSE))
  }
  
  # Use dplyr for all operations since duckplyr has limited support
  geographic_summary <- provider_data %>%
    dplyr::group_by(across(all_of(available_geo_columns[1]))) %>%
    dplyr::summarise(
      provider_count = dplyr::n(),
      unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(provider_count))
  
  # If space-saving mode, keep only top geographic areas
  if (keep_only_summary && nrow(geographic_summary) > 100) {
    geographic_summary <- geographic_summary %>%
      dplyr::slice_head(n = 100)
    
    if (verbose_logging) {
      logger::log_info("   🧹 Space-saving: kept only top 100 geographic areas")
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Geographic units analyzed: {nrow(geographic_summary)}")
  }
  
  return(list(
    geographic_analysis_available = TRUE,
    geographic_distribution = geographic_summary,
    geographic_columns_used = available_geo_columns
  ))
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
  }
  
  next_search_provider_ids <- c()
  
  # Standard low-to-high strategies
  if (search_strategy == "smart_gaps" || search_strategy == "smart_mixed") {
    # Prioritize truly missing IDs (ascending order)
    truly_missing_count <- min(batch_size, length(analysis_results$truly_missing_provider_ids))
    if (truly_missing_count > 0) {
      next_search_provider_ids <- c(next_search_provider_ids, 
                                    analysis_results$truly_missing_provider_ids[1:truly_missing_count])
    }
  }
  
  # High-to-low gap filling strategies
  if (search_strategy == "high_to_low_gaps" || search_strategy == "high_to_low_mixed") {
    # Prioritize truly missing IDs from highest to lowest
    truly_missing_sorted_desc <- sort(analysis_results$truly_missing_provider_ids, decreasing = TRUE)
    truly_missing_count <- min(batch_size, length(truly_missing_sorted_desc))
    if (truly_missing_count > 0) {
      next_search_provider_ids <- c(next_search_provider_ids, 
                                    truly_missing_sorted_desc[1:truly_missing_count])
    }
  }
  
  if (search_strategy == "smart_extend" || search_strategy == "smart_mixed") {
    # Add new IDs beyond current maximum (ascending)
    remaining_batch_space <- batch_size - length(next_search_provider_ids)
    if (remaining_batch_space > 0) {
      new_id_start <- analysis_results$max_id_searched + 1
      new_id_end <- new_id_start + remaining_batch_space - 1
      new_provider_ids <- seq(new_id_start, new_id_end)
      next_search_provider_ids <- c(next_search_provider_ids, new_provider_ids)
    }
  }
  
  if (search_strategy == "high_to_low_extend" || search_strategy == "high_to_low_mixed") {
    # Add new IDs beyond current maximum (going even higher)
    remaining_batch_space <- batch_size - length(next_search_provider_ids)
    if (remaining_batch_space > 0) {
      new_id_start <- analysis_results$max_id_searched + 1
      new_id_end <- new_id_start + remaining_batch_space - 1
      new_provider_ids <- seq(new_id_start, new_id_end)
      # Sort in descending order for high-to-low approach
      new_provider_ids <- sort(new_provider_ids, decreasing = TRUE)
      next_search_provider_ids <- c(next_search_provider_ids, new_provider_ids)
    }
  }
  
  # Remove any known non-existent IDs and deduplicate
  next_search_provider_ids <- setdiff(next_search_provider_ids, analysis_results$non_existent_provider_ids)
  
  # Sort based on strategy
  if (grepl("high_to_low", search_strategy)) {
    next_search_provider_ids <- sort(unique(next_search_provider_ids), decreasing = TRUE)
    if (verbose_logging) {
      logger::log_info("   📈 Using high-to-low search strategy (newest physicians first)")
    }
  } else {
    next_search_provider_ids <- sort(unique(next_search_provider_ids))
    if (verbose_logging) {
      logger::log_info("   📉 Using low-to-high search strategy (oldest physicians first)")
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🔍 Recommended IDs: {formatC(length(next_search_provider_ids), big.mark = ',', format = 'd')}")
    if (length(next_search_provider_ids) > 0) {
      logger::log_info("   📊 ID range: {formatC(min(next_search_provider_ids), big.mark = ',', format = 'd')} to {formatC(max(next_search_provider_ids), big.mark = ',', format = 'd')}")
      
      # Show strategy-specific sample
      if (grepl("high_to_low", search_strategy)) {
        if (length(next_search_provider_ids) <= 10) {
          logger::log_info("   🆔 Starting from highest: {paste(next_search_provider_ids, collapse = ', ')}")
        } else {
          first_few <- paste(head(next_search_provider_ids, 5), collapse = ", ")
          last_few <- paste(tail(next_search_provider_ids, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from highest: {first_few} ... ending with: {last_few}")
        }
      } else {
        if (length(next_search_provider_ids) <= 10) {
          logger::log_info("   🆔 Starting from lowest: {paste(next_search_provider_ids, collapse = ', ')}")
        } else {
          first_few <- paste(head(next_search_provider_ids, 5), collapse = ", ")
          last_few <- paste(tail(next_search_provider_ids, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from lowest: {first_few} ... ending with: {last_few}")
        }
      }
    }
  }
  
  return(next_search_provider_ids)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    if (compress_files) {
      non_existent_file_path <- fs::path(output_directory, 
                                         glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv.gz"))
      
      data.frame(provider_id = non_existent_provider_ids) %>%
        readr::write_csv(gzfile(non_existent_file_path))
    } else {
      non_existent_file_path <- fs::path(output_directory, 
                                         glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv"))
      
      data.frame(provider_id = non_existent_provider_ids) %>%
        readr::write_csv(non_existent_file_path)
    }
    
    if (verbose_logging) {
      logger::log_info("   📄 Non-existent IDs saved: {non_existent_file_path}")
    }
  }
  
  # Save truly missing IDs (sample only if too large to save space)
  if (length(truly_missing_provider_ids) > 0) {
    # If there are more than 1 million missing IDs, save only a sample to save space
    if (length(truly_missing_provider_ids) > 1000000) {
      sample_size <- 100000
      sampled_missing_ids <- sample(truly_missing_provider_ids, sample_size)
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: sampling {formatC(sample_size, big.mark = ',', format = 'd')} missing IDs from {formatC(length(truly_missing_provider_ids), big.mark = ',', format = 'd')} total")
      }
      
      ids_to_save <- sampled_missing_ids
      file_suffix <- "_sampled"
    } else {
      ids_to_save <- truly_missing_provider_ids
      file_suffix <- ""
    }
    
    if (compress_files) {
      missing_file_path <- fs::path(output_directory, 
                                    glue::glue("truly_missing_provider_ids{file_suffix}_{timestamp_suffix}.csv.gz"))
      
      data.frame(provider_id = ids_to_save) %>%
        readr::write_csv(gzfile(missing_file_path))
    } else {
      missing_file_path <- fs::path(output_directory, 
                                    glue::glue("truly_missing_provider_ids{file_suffix}_{timestamp_suffix}.csv"))
      
      data.frame(provider_id = ids_to_save) %>%
        readr::write_csv(missing_file_path)
    }
    
    if (verbose_logging) {
      logger::log_info("   📄 Missing IDs saved: {missing_file_path}")
    }
  }
}

# Run  -----
# EMERGENCY: Restart R and increase memory limits
options(future.globals.maxSize = 32 * 1024^3)  # 32GB limit

# Use MUCH more aggressive filtering
healthcare_access_analysis_filtered <- analyze_healthcare_access_patterns(
  search_results_directory = "physician_data",
  extraction_file_pattern = "combined_subspecialty_extractions_.*\\.csv$",
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE,
  batch_size_for_search = 5000,
  search_strategy = "high_to_low_mixed",
  external_drive_path = "/Volumes/MufflyNew",
  compress_output = TRUE,
  keep_only_summary = FALSE,           # 🔥 CRITICAL: Use summary mode
  chunk_processing = TRUE,
  max_chunk_size = 25000,             # 🔥 Smaller chunks
  search_subdirectories = TRUE,
  exclude_patterns = c(
    "sequential_discovery_.*\\.csv$",  # Exclude sequential discovery
    "wrong_ids_.*\\.csv$",            # Exclude wrong IDs
    "temp_.*\\.csv$",                 # Exclude temp files
    "backup_.*\\.csv$",                # Exclude backups
    "truly_missing_.*\\.csv$",
    "non_existent_provider_.*\\.csv$",
    "extraction_summary_.*\\.csv$",
    "valid_physician_ids_.*\\.csv$"
  )
)


# August 15 -----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#' @param max_memory_gb Numeric. Maximum memory limit in GB to set for R session.
#'   Default is 16GB. Increase for very large datasets.
#' @param parallel_cores Numeric. Number of CPU cores to use for parallel processing.
#'   Default is NULL (auto-detect). Set to 1 to disable parallel processing.
#' @param use_data_table Logical. Whether to use data.table for faster file reading
#'   and data manipulation. Highly recommended for large datasets. Default is TRUE.
#' @param file_size_threshold_mb Numeric. Files larger than this threshold (in MB)
#'   will be processed with optimized methods. Default is 50MB.
#' @param progress_bar Logical. Whether to show progress bar during file processing.
#'   Default is TRUE.
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: High-performance analysis for 1000+ files
#' large_dataset_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   parallel_cores = 8,          # Use 8 CPU cores for parallel processing
#'   use_data_table = TRUE,       # Use data.table for speed
#'   file_size_threshold_mb = 25, # Optimize files larger than 25MB
#'   progress_bar = TRUE,         # Show progress for 1000 files
#'   max_memory_gb = 32
#' )
#'
#' # Example 3: Focused analysis on subspecialty with restart protection
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps",
#'   max_memory_gb = 24,
#'   compress_output = TRUE
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' @importFrom parallel detectCores makeCluster stopCluster parLapply clusterEvalQ clusterExport
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$"),
                                               max_memory_gb = 16,
                                               parallel_cores = NULL,
                                               use_data_table = TRUE,
                                               file_size_threshold_mb = 50,
                                               progress_bar = TRUE) {
  
  # Set memory limits first thing
  tryCatch({
    memory_bytes <- max_memory_gb * 1024^3
    if (.Platform$OS.type == "windows") {
      memory.limit(size = max_memory_gb * 1024)  # Windows uses MB
    } else {
      # Unix-like systems
      system(paste0("ulimit -v ", memory_bytes))
    }
    
    # Set R memory options
    options(future.globals.maxSize = memory_bytes)
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Memory limit set to {max_memory_gb} GB")
    }
  }, error = function(e) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  Could not set memory limit: {e$message}")
    }
  })
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  assertthat::assert_that(is.numeric(max_memory_gb) && max_memory_gb > 0)
  assertthat::assert_that(is.null(parallel_cores) || (is.numeric(parallel_cores) && parallel_cores >= 1))
  assertthat::assert_that(is.logical(use_data_table))
  assertthat::assert_that(is.numeric(file_size_threshold_mb) && file_size_threshold_mb > 0)
  assertthat::assert_that(is.logical(progress_bar))
  
  # Check and load required packages for performance optimization
  if (use_data_table && !requireNamespace("data.table", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  data.table not available, falling back to base R methods")
    }
    use_data_table <- FALSE
  }
  
  # Set up parallel processing
  if (is.null(parallel_cores)) {
    parallel_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for system
  }
  
  if (parallel_cores > 1 && !requireNamespace("parallel", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  parallel package not available, using single-core processing")
    }
    parallel_cores <- 1
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
    logger::log_info("   💾 Memory limit: {max_memory_gb} GB")
    logger::log_info("   🚀 Performance optimizations:")
    logger::log_info("      💻 CPU cores: {parallel_cores}")
    logger::log_info("      ⚡ data.table: {ifelse(use_data_table, 'enabled', 'disabled')}")
    logger::log_info("      📋 Progress bar: {ifelse(progress_bar, 'enabled', 'disabled')}")
    logger::log_info("      📏 File size threshold: {file_size_threshold_mb} MB")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Check if R session needs restart for memory increase
  current_memory_limit <- tryCatch({
    if (.Platform$OS.type == "windows") {
      memory.limit()
    } else {
      # For Unix systems, check ulimit
      as.numeric(system("ulimit -v", intern = TRUE)) / 1024^2  # Convert to GB
    }
  }, error = function(e) NULL)
  
  if (!is.null(current_memory_limit) && current_memory_limit < (max_memory_gb * 1024)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  Current memory limit ({round(current_memory_limit/1024, 1)} GB) is less than requested ({max_memory_gb} GB)")
      logger::log_warn("   💡 Consider restarting R session to apply new memory limits")
      logger::log_info("   🔄 Continuing with current limits...")
    }
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files and generate analysis with performance optimizations
  file_processing_results <- process_healthcare_data_files_optimized(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns,
    parallel_cores = parallel_cores,
    use_data_table = use_data_table,
    file_size_threshold_mb = file_size_threshold_mb,
    progress_bar = progress_bar
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary,
      memory_limit_gb = max_memory_gb,
      parallel_cores_used = parallel_cores,
      data_table_used = use_data_table,
      performance_optimized = TRUE
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Performance: Used {parallel_cores} cores with {ifelse(use_data_table, 'data.table', 'base R')} methods")
    }
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files_optimized <- function(directory_path, file_pattern, verbose_logging, 
                                                    keep_only_summary = FALSE, chunk_processing = FALSE, 
                                                    max_chunk_size = 100000, search_subdirectories = FALSE,
                                                    exclude_patterns = NULL, parallel_cores = 1,
                                                    use_data_table = TRUE, file_size_threshold_mb = 50,
                                                    progress_bar = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files with optimizations...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories
  )
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
    }
  }
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Analyze file sizes for optimization strategy
  file_info_data <- file.info(csv_files_list)
  file_sizes_mb <- file_info_data$size / (1024^2)
  large_files_count <- sum(file_sizes_mb > file_size_threshold_mb, na.rm = TRUE)
  
  if (verbose_logging) {
    logger::log_info("   📊 File size analysis:")
    logger::log_info("      📏 Average file size: {round(mean(file_sizes_mb, na.rm = TRUE), 1)} MB")
    logger::log_info("      📈 Largest file: {round(max(file_sizes_mb, na.rm = TRUE), 1)} MB") 
    logger::log_info("      🔥 Files > {file_size_threshold_mb}MB: {large_files_count}")
    if (parallel_cores > 1) {
      logger::log_info("      ⚡ Using {parallel_cores} cores for parallel processing")
    }
  }
  
  # Set up progress bar
  if (progress_bar && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing files [:bar] :percent :current/:total ETA: :eta",
      total = length(csv_files_list),
      clear = FALSE,
      width = 80
    )
    show_progress <- TRUE
  } else {
    show_progress <- FALSE
    if (progress_bar && verbose_logging) {
      logger::log_warn("⚠️  progress package not available, disabling progress bar")
    }
  }
  
  # Optimized file reading function
  optimized_read_csv <- function(file_path, file_index = NULL) {
    if (show_progress) {
      pb$tick()
    }
    
    tryCatch({
      # Check file size for optimization strategy
      file_size_mb <- file.info(file_path)$size / (1024^2)
      
      if (use_data_table && file_size_mb > file_size_threshold_mb) {
        # Use data.table::fread for large files (much faster)
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          nThread = 1,  # Control threading at file level
          stringsAsFactors = FALSE
        )
        # Convert to tibble if needed for consistency
        file_data <- dplyr::as_tibble(file_data)
      } else if (use_data_table) {
        # Use data.table for smaller files too for consistency  
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          stringsAsFactors = FALSE
        )
        file_data <- dplyr::as_tibble(file_data)
      } else {
        # Fallback to readr with optimized settings
        file_data <- readr::read_csv(
          file_path, 
          show_col_types = FALSE,
          lazy = FALSE,  # Read immediately for better memory management
          col_types = readr::cols(.default = readr::col_character()),
          locale = readr::locale(encoding = "UTF-8")
        )
      }
      
      # Standardize and filter early to save memory
      file_data <- standardize_provider_data_columns_optimized(file_data, file_path, FALSE)
      
      if (keep_only_summary && "provider_id" %in% colnames(file_data)) {
        file_data <- apply_summary_filtering(file_data, FALSE)  # Don't log for each file
      }
      
      return(file_data)
      
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read {basename(file_path)}: {e$message}")
      }
      return(NULL)
    })
  }
  
  # Process files - use parallel processing if beneficial
  if (parallel_cores > 1 && length(csv_files_list) > 10) {
    if (verbose_logging) {
      logger::log_info("   ⚡ Using parallel processing with {parallel_cores} cores")
    }
    
    # Set up cluster
    cl <- parallel::makeCluster(parallel_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary objects to cluster
    parallel::clusterEvalQ(cl, {
      library(dplyr)
      library(readr)
      library(stringr)
      library(glue)
      library(lubridate)
      if (requireNamespace("data.table", quietly = TRUE)) {
        library(data.table)
      }
    })
    
    parallel::clusterExport(cl, c(
      "use_data_table", "file_size_threshold_mb", "keep_only_summary", 
      "verbose_logging", "standardize_provider_data_columns_optimized",
      "apply_summary_filtering"
    ), envir = environment())
    
    # Process files in parallel
    file_data_list <- parallel::parLapply(cl, csv_files_list, optimized_read_csv)
    
  } else {
    # Sequential processing for smaller file sets or single core
    file_data_list <- lapply(csv_files_list, optimized_read_csv)
  }
  
  # Remove NULL entries (failed reads)
  file_data_list <- file_data_list[!sapply(file_data_list, is.null)]
  
  if (length(file_data_list) == 0) {
    stop("No files could be successfully read")
  }
  
  if (verbose_logging) {
    successful_files <- length(file_data_list)
    failed_files <- length(csv_files_list) - successful_files
    logger::log_info("   ✅ Successfully read {successful_files} files")
    if (failed_files > 0) {
      logger::log_warn("   ❌ Failed to read {failed_files} files")
    }
  }
  
  # Combine all data efficiently
  if (verbose_logging) {
    logger::log_info("   🔄 Combining data from all files...")
  }
  
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    # Use data.table::rbindlist for fastest binding
    combined_provider_data <- data.table::rbindlist(file_data_list, fill = TRUE, use.names = TRUE)
    combined_provider_data <- dplyr::as_tibble(combined_provider_data)
  } else {
    # Use dplyr::bind_rows as fallback
    combined_provider_data <- dplyr::bind_rows(file_data_list)
  }
  
  # Clean up memory
  rm(file_data_list)
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    successful_files = length(file_data_list),
    total_provider_records = nrow(combined_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing,
    parallel_cores_used = parallel_cores,
    data_table_used = use_data_table,
    average_file_size_mb = round(mean(file_sizes_mb, na.rm = TRUE), 1),
    large_files_count = large_files_count
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(combined_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(combined_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Parallel processing completed successfully")
    }
  }
  
  return(list(
    combined_provider_data = combined_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
read_file_in_chunks <- function(file_path, max_chunk_size, keep_only_summary, verbose_logging) {
  if (verbose_logging) {
    logger::log_info("   🔄 Processing {basename(file_path)} in chunks to save memory")
  }
  
  all_chunks <- list()
  chunk_num <- 1
  
  tryCatch({
    # Use data.table::fread for faster chunked reading if available
    if (requireNamespace("data.table", quietly = TRUE)) {
      repeat {
        chunk_data <- data.table::fread(
          file_path, 
          skip = (chunk_num - 1) * max_chunk_size,
          nrows = max_chunk_size,
          showProgress = FALSE
        )
        
        if (nrow(chunk_data) == 0) break
        
        # Convert to tibble and process
        chunk_data <- dplyr::as_tibble(chunk_data)
        chunk_data <- standardize_provider_data_columns_optimized(chunk_data, file_path, FALSE)
        
        # Apply filtering if in summary mode
        if (keep_only_summary) {
          chunk_data <- apply_summary_filtering(chunk_data, FALSE)
        }
        
        all_chunks[[chunk_num]] <- chunk_data
        chunk_num <- chunk_num + 1
        
        # Force garbage collection every 10 chunks
        if (chunk_num %% 10 == 0) {
          gc()
        }
      }
    } else {
      # Fallback to readr with chunking
      chunk_data <- readr::read_csv(file_path, 
                                    col_types = readr::cols(.default = readr::col_character()),
                                    n_max = max_chunk_size,
                                    show_col_types = FALSE)
      chunk_data <- standardize_provider_data_columns_optimized(chunk_data, file_path, FALSE)
      if (keep_only_summary) {
        chunk_data <- apply_summary_filtering(chunk_data, FALSE)
      }
      all_chunks[[1]] <- chunk_data
    }
    
    # Combine chunks efficiently
    if (requireNamespace("data.table", quietly = TRUE)) {
      combined_data <- data.table::rbindlist(all_chunks, fill = TRUE) %>% dplyr::as_tibble()
    } else {
      combined_data <- dplyr::bind_rows(all_chunks)
    }
    
    # Clear chunks from memory
    rm(all_chunks)
    gc()
    
    return(combined_data)
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("   ❌ Chunked reading failed for {basename(file_path)}: {e$message}")
    }
    return(NULL)
  })
}

#' @noRd
apply_summary_filtering <- function(provider_data, verbose_logging) {
  if (!"provider_id" %in% colnames(provider_data)) {
    return(provider_data)
  }
  
  essential_columns <- c("provider_id", "source_file")
  geographic_columns <- c("city", "state", "county", "zip_code")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
  keep_columns <- intersect(keep_columns, colnames(provider_data))
  
  filtered_data <- provider_data %>%
    dplyr::select(all_of(keep_columns))
  
  if (verbose_logging) {
    logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
  }
  
  return(filtered_data)
}

#' @noRd
standardize_provider_data_columns_optimized <- function(file_data, file_path, verbose_logging) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  # Use data.table if available for faster conversion
  if (requireNamespace("data.table", quietly = TRUE) && data.table::is.data.table(file_data)) {
    # data.table approach - faster for large data
    char_cols <- names(file_data)
    file_data[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    file_data <- dplyr::as_tibble(file_data)
  } else {
    # dplyr approach for smaller data
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id (vectorized approach)
  col_names <- colnames(file_data)
  
  if (!"provider_id" %in% col_names) {
    # Check for alternative ID columns in order of preference
    id_alternatives <- c("physician_id", "npi", "physician_ids", "id")
    available_alternatives <- intersect(id_alternatives, col_names)
    
    if (length(available_alternatives) > 0) {
      # Use the first available alternative
      file_data$provider_id <- file_data[[available_alternatives[1]]]
    }
  }
  
  # Add file source for tracking (already character) - optimized
  file_data$source_file <- basename(file_path)
  file_data$processing_timestamp <- as.character(lubridate::now())
  
  return(file_data)
}

#' @noRd  
batch_combine_data_efficiently <- function(data_list, use_data_table = TRUE, verbose_logging = FALSE) {
  if (length(data_list) == 0) {
    return(NULL)
  }
  
  if (length(data_list) == 1) {
    return(data_list[[1]])
  }
  
  if (verbose_logging) {
    logger::log_info("   🔄 Efficiently combining {length(data_list)} data objects...")
  }
  
  # Use data.table for fastest binding if available
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    tryCatch({
      combined_data <- data.table::rbindlist(data_list, fill = TRUE, use.names = TRUE)
      return(dplyr::as_tibble(combined_data))
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  data.table binding failed, using dplyr: {e$message}")
      }
      return(dplyr::bind_rows(data_list))
    })
  } else {
    return(dplyr::bind_rows(data_list))
  }
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Use dplyr for all operations
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id) & provider_id != "" & provider_id != "NA") %>%
    dplyr::pull(provider_id) %>%
    unique() %>%
    as.numeric() %>%
    .[!is.na(.)] %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No valid numeric provider IDs found in data")
    }
    return(list(
      provider_id_range = c(NA, NA),
      total_unique_ids = 0,
      gaps_identified = 0,
      non_existent_provider_ids = numeric(0),
      truly_missing_provider_ids = numeric(0),
      existence_rate = 0,
      gap_analysis_summary = "No valid provider IDs found"
    ))
  }
  
  # Calculate basic statistics
  min_id <- min(all_provider_ids)
  max_id <- max(all_provider_ids)
  total_unique_ids <- length(all_provider_ids)
  full_range_size <- max_id - min_id + 1
  
  # Find gaps in the sequence
  complete_sequence <- seq(min_id, max_id)
  missing_ids <- setdiff(complete_sequence, all_provider_ids)
  gaps_identified <- length(missing_ids)
  
  # Calculate existence rate
  existence_rate <- (total_unique_ids / full_range_size) * 100
  
  if (verbose_logging) {
    logger::log_info("   📊 Provider ID analysis:")
    logger::log_info("      🆔 Range: {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}")
    logger::log_info("      ✅ Found IDs: {formatC(total_unique_ids, big.mark = ',')}")
    logger::log_info("      ❌ Missing IDs: {formatC(gaps_identified, big.mark = ',')}")
    logger::log_info("      📈 Existence rate: {round(existence_rate, 1)}%")
  }
  
  # Distinguish between different types of missing IDs
  non_existent_ids <- missing_ids[sample(length(missing_ids), min(1000, length(missing_ids)))]
  truly_missing_ids <- missing_ids
  
  gap_analysis_summary <- glue::glue(
    "Found {formatC(total_unique_ids, big.mark = ',')} unique provider IDs in range {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}. ",
    "Identified {formatC(gaps_identified, big.mark = ',')} gaps ({round(100 - existence_rate, 1)}% missing). ",
    "Existence rate: {round(existence_rate, 1)}%."
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Gap analysis completed")
  }
  
  return(list(
    provider_id_range = c(min_id, max_id),
    total_unique_ids = total_unique_ids,
    gaps_identified = gaps_identified,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_ids,
    existence_rate = existence_rate,
    gap_analysis_summary = gap_analysis_summary,
    full_range_size = full_range_size
  ))
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution...")
  }
  
  # Check for geographic columns
  geo_columns <- c("state", "city", "county", "zip_code", "address")
  available_geo_columns <- intersect(names(provider_data), geo_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No geographic columns found for distribution analysis")
    }
    return(list(
      geographic_summary = "No geographic data available",
      state_distribution = NULL,
      city_distribution = NULL
    ))
  }
  
  geographic_analysis <- list()
  
  # State-level analysis if available
  if ("state" %in% available_geo_columns) {
    state_dist <- provider_data %>%
      dplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
      dplyr::group_by(state) %>%
      dplyr::summarise(
        provider_count = dplyr::n(),
        unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(provider_count))
    
    geographic_analysis$state_distribution <- state_dist
    
    if (verbose_logging) {
      logger::log_info("   🏛️  State analysis: {nrow(state_dist)} states with providers")
    }
  }
  
  # City-level analysis if available  
  if ("city" %in% available_geo_columns && !keep_only_summary) {
    city_dist <- provider_data %>%
      dplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
      dplyr::group_by(city, state) %>%
      dplyr::summarise(
        provider_count = dplyr::n(),
        unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(provider_count)) %>%
      dplyr::slice_head(n = 100)  # Top 100 cities only
    
    geographic_analysis$city_distribution <- city_dist
    
    if (verbose_logging) {
      logger::log_info("   🏙️  City analysis: {nrow(city_dist)} cities analyzed (top 100)")
    }
  }
  
  # Generate summary
  total_with_geo <- provider_data %>%
    dplyr::filter(dplyr::if_any(dplyr::all_of(available_geo_columns), ~ !is.na(.) & . != "" & . != "NA")) %>%
    nrow()
  
  geo_coverage_rate <- (total_with_geo / nrow(provider_data)) * 100
  
  geographic_summary <- glue::glue(
    "Geographic data available for {formatC(total_with_geo, big.mark = ',')} of {formatC(nrow(provider_data), big.mark = ',')} records ({round(geo_coverage_rate, 1)}%). ",
    "Available columns: {paste(available_geo_columns, collapse = ', ')}."
  )
  
  geographic_analysis$geographic_summary <- geographic_summary
  geographic_analysis$coverage_rate <- geo_coverage_rate
  geographic_analysis$available_columns <- available_geo_columns
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Coverage: {round(geo_coverage_rate, 1)}% of records have geographic data")
  }
  
  return(geographic_analysis)
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
    logger::log_info("   📊 Strategy: {search_strategy}")
    logger::log_info("   📦 Batch size: {formatC(batch_size, big.mark = ',')}")
  }
  
  # Extract key information from analysis
  min_id <- analysis_results$provider_id_range[1]
  max_id <- analysis_results$provider_id_range[2]
  missing_ids <- analysis_results$truly_missing_provider_ids
  
  if (is.na(min_id) || is.na(max_id) || length(missing_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  Insufficient data for search recommendations")
    }
    return(numeric(0))
  }
  
  search_candidates <- numeric(0)
  
  # Generate search candidates based on strategy
  if (stringr::str_detect(search_strategy, "gaps")) {
    # Add missing IDs from gaps
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Sort missing IDs from highest to lowest
      gap_candidates <- sort(missing_ids, decreasing = TRUE)
    } else {
      # Smart gaps - mix of random and systematic
      gap_candidates <- sample(missing_ids, min(length(missing_ids), batch_size * 2))
    }
    search_candidates <- c(search_candidates, gap_candidates)
  }
  
  if (stringr::str_detect(search_strategy, "extend")) {
    # Add IDs beyond current range
    extension_size <- min(batch_size, 10000)  # Don't extend too far
    
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Extend upward from max
      extend_candidates <- seq(max_id + 1, max_id + extension_size)
    } else {
      # Smart extend - both directions
      extend_up <- seq(max_id + 1, max_id + ceiling(extension_size/2))
      extend_down <- seq(max(1, min_id - floor(extension_size/2)), min_id - 1)
      extend_candidates <- c(extend_up, extend_down)
    }
    search_candidates <- c(search_candidates, extend_candidates)
  }
  
  # Remove duplicates and limit to batch size
  search_candidates <- unique(search_candidates)
  search_candidates <- search_candidates[search_candidates > 0]  # Remove negative IDs
  
  # Limit to batch size
  if (length(search_candidates) > batch_size) {
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      search_candidates <- sort(search_candidates, decreasing = TRUE)[1:batch_size]
    } else {
      search_candidates <- sample(search_candidates, batch_size)
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🆔 Recommended IDs: {formatC(length(search_candidates), big.mark = ',')}")
    if (length(search_candidates) > 0) {
      logger::log_info("   📈 Range: {formatC(min(search_candidates), big.mark = ',')} to {formatC(max(search_candidates), big.mark = ',')}")
    }
  }
  
  return(search_candidates)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_df <- data.frame(
      provider_id = non_existent_provider_ids,
      exclusion_reason = "non_existent",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(non_existent_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath}.gz ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath} ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  # Save truly missing IDs  
  if (length(truly_missing_provider_ids) > 0) {
    missing_df <- data.frame(
      provider_id = truly_missing_provider_ids,
      exclusion_reason = "missing_from_search",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("missing_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(missing_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath}.gz ({formatC(length(truly_missing_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath} ({formatC(length(truly_missing_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Exclusion lists saved successfully")
  }
}

# run ----
# Install required packages if needed
# install.packages(c("data.table", "parallel", "progress"))

# Optimized for 1,000 files
results <- analyze_healthcare_access_patterns(
  search_results_directory = "physician_data",
  extraction_file_pattern = ".*\\.csv$",
  
  # Performance settings
  parallel_cores = 8,              # Use 8 cores (adjust for your CPU)
  use_data_table = TRUE,           # Enable fast data.table methods
  file_size_threshold_mb = 25,     # Optimize files > 25MB
  progress_bar = TRUE,             # Show progress through 1000 files
  
  # Memory management  
  max_memory_gb = 32,              # Set memory limit
  keep_only_summary = FALSE,       # Keep all data as discussed
  
  # Logging and output
  enable_verbose_logging = TRUE,
  external_drive_path = "/Volumes/MufflyNew",  # Optional
  exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                       "wrong_ids_.*\\.csv$"#,
                       #"intermediate_updates"
                       )
)

# August 15, 2011 pm ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#' @param max_memory_gb Numeric. Maximum memory limit in GB to set for R session.
#'   Default is 16GB. Increase for very large datasets.
#' @param parallel_cores Numeric. Number of CPU cores to use for parallel processing.
#'   Default is NULL (auto-detect). Set to 1 to disable parallel processing.
#' @param use_data_table Logical. Whether to use data.table for faster file reading
#'   and data manipulation. Highly recommended for large datasets. Default is TRUE.
#' @param file_size_threshold_mb Numeric. Files larger than this threshold (in MB)
#'   will be processed with optimized methods. Default is 50MB.
#' @param progress_bar Logical. Whether to show progress bar during file processing.
#'   Default is TRUE.
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: High-performance analysis for 1000+ files
#' large_dataset_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   parallel_cores = 8,          # Use 8 CPU cores for parallel processing
#'   use_data_table = TRUE,       # Use data.table for speed
#'   file_size_threshold_mb = 25, # Optimize files larger than 25MB
#'   progress_bar = TRUE,         # Show progress for 1000 files
#'   max_memory_gb = 32
#' )
#'
#' # Example 3: Focused analysis on subspecialty with restart protection
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps",
#'   max_memory_gb = 24,
#'   compress_output = TRUE
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' @importFrom parallel detectCores makeCluster stopCluster parLapply clusterEvalQ clusterExport
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$"),
                                               max_memory_gb = 16,
                                               parallel_cores = NULL,
                                               use_data_table = TRUE,
                                               file_size_threshold_mb = 50,
                                               progress_bar = TRUE) {
  
  # Set memory limits first thing
  tryCatch({
    memory_bytes <- max_memory_gb * 1024^3
    if (.Platform$OS.type == "windows") {
      memory.limit(size = max_memory_gb * 1024)  # Windows uses MB
    } else {
      # Unix-like systems
      system(paste0("ulimit -v ", memory_bytes))
    }
    
    # Set R memory options
    options(future.globals.maxSize = memory_bytes)
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Memory limit set to {max_memory_gb} GB")
    }
  }, error = function(e) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  Could not set memory limit: {e$message}")
    }
  })
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  assertthat::assert_that(is.numeric(max_memory_gb) && max_memory_gb > 0)
  assertthat::assert_that(is.null(parallel_cores) || (is.numeric(parallel_cores) && parallel_cores >= 1))
  assertthat::assert_that(is.logical(use_data_table))
  assertthat::assert_that(is.numeric(file_size_threshold_mb) && file_size_threshold_mb > 0)
  assertthat::assert_that(is.logical(progress_bar))
  
  # Check and load required packages for performance optimization
  if (use_data_table && !requireNamespace("data.table", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  data.table not available, falling back to base R methods")
    }
    use_data_table <- FALSE
  }
  
  # Set up parallel processing
  if (is.null(parallel_cores)) {
    parallel_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for system
  }
  
  if (parallel_cores > 1 && !requireNamespace("parallel", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  parallel package not available, using single-core processing")
    }
    parallel_cores <- 1
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
    logger::log_info("   💾 Memory limit: {max_memory_gb} GB")
    logger::log_info("   🚀 Performance optimizations:")
    logger::log_info("      💻 CPU cores: {parallel_cores}")
    logger::log_info("      ⚡ data.table: {ifelse(use_data_table, 'enabled', 'disabled')}")
    logger::log_info("      📋 Progress bar: {ifelse(progress_bar, 'enabled', 'disabled')}")
    logger::log_info("      📏 File size threshold: {file_size_threshold_mb} MB")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Check if R session needs restart for memory increase
  current_memory_limit <- tryCatch({
    if (.Platform$OS.type == "windows") {
      memory.limit()
    } else {
      # For Unix systems, check ulimit
      as.numeric(system("ulimit -v", intern = TRUE)) / 1024^2  # Convert to GB
    }
  }, error = function(e) NULL)
  
  if (!is.null(current_memory_limit) && current_memory_limit < (max_memory_gb * 1024)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  Current memory limit ({round(current_memory_limit/1024, 1)} GB) is less than requested ({max_memory_gb} GB)")
      logger::log_warn("   💡 Consider restarting R session to apply new memory limits")
      logger::log_info("   🔄 Continuing with current limits...")
    }
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files and generate analysis with performance optimizations
  file_processing_results <- process_healthcare_data_files_optimized(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns,
    parallel_cores = parallel_cores,
    use_data_table = use_data_table,
    file_size_threshold_mb = file_size_threshold_mb,
    progress_bar = progress_bar
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary,
      memory_limit_gb = max_memory_gb,
      parallel_cores_used = parallel_cores,
      data_table_used = use_data_table,
      performance_optimized = TRUE
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Performance: Used {parallel_cores} cores with {ifelse(use_data_table, 'data.table', 'base R')} methods")
    }
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files_optimized <- function(directory_path, file_pattern, verbose_logging, 
                                                    keep_only_summary = FALSE, chunk_processing = FALSE, 
                                                    max_chunk_size = 100000, search_subdirectories = FALSE,
                                                    exclude_patterns = NULL, parallel_cores = 1,
                                                    use_data_table = TRUE, file_size_threshold_mb = 50,
                                                    progress_bar = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files with optimizations...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories
  )
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
    }
  }
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Analyze file sizes for optimization strategy
  file_info_data <- file.info(csv_files_list)
  file_sizes_mb <- file_info_data$size / (1024^2)
  large_files_count <- sum(file_sizes_mb > file_size_threshold_mb, na.rm = TRUE)
  
  if (verbose_logging) {
    logger::log_info("   📊 File size analysis:")
    logger::log_info("      📏 Average file size: {round(mean(file_sizes_mb, na.rm = TRUE), 1)} MB")
    logger::log_info("      📈 Largest file: {round(max(file_sizes_mb, na.rm = TRUE), 1)} MB") 
    logger::log_info("      🔥 Files > {file_size_threshold_mb}MB: {large_files_count}")
    if (parallel_cores > 1) {
      logger::log_info("      ⚡ Using {parallel_cores} cores for parallel processing")
    }
  }
  
  # Set up progress bar
  if (progress_bar && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing files [:bar] :percent :current/:total ETA: :eta",
      total = length(csv_files_list),
      clear = FALSE,
      width = 80
    )
    show_progress <- TRUE
  } else {
    show_progress <- FALSE
    if (progress_bar && verbose_logging) {
      logger::log_warn("⚠️  progress package not available, disabling progress bar")
    }
  }
  
  # Optimized file reading function
  optimized_read_csv <- function(file_path, file_index = NULL) {
    if (show_progress) {
      pb$tick()
    }
    
    tryCatch({
      # Check file size for optimization strategy
      file_size_mb <- file.info(file_path)$size / (1024^2)
      
      if (use_data_table && file_size_mb > file_size_threshold_mb) {
        # Use data.table::fread for large files (much faster)
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          nThread = 1,  # Control threading at file level
          stringsAsFactors = FALSE
        )
        # Convert to tibble if needed for consistency
        file_data <- dplyr::as_tibble(file_data)
      } else if (use_data_table) {
        # Use data.table for smaller files too for consistency  
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          stringsAsFactors = FALSE
        )
        file_data <- dplyr::as_tibble(file_data)
      } else {
        # Fallback to readr with optimized settings
        file_data <- readr::read_csv(
          file_path, 
          show_col_types = FALSE,
          lazy = FALSE,  # Read immediately for better memory management
          col_types = readr::cols(.default = readr::col_character()),
          locale = readr::locale(encoding = "UTF-8")
        )
      }
      
      # Standardize and filter early to save memory
      file_data <- standardize_provider_data_columns_optimized(file_data, file_path, FALSE)
      
      if (keep_only_summary && "provider_id" %in% colnames(file_data)) {
        file_data <- apply_summary_filtering(file_data, FALSE)  # Don't log for each file
      }
      
      return(file_data)
      
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read {basename(file_path)}: {e$message}")
      }
      return(NULL)
    })
  }
  
  # Process files - use parallel processing if beneficial
  if (parallel_cores > 1 && length(csv_files_list) > 10) {
    if (verbose_logging) {
      logger::log_info("   ⚡ Using parallel processing with {parallel_cores} cores")
    }
    
    # Set up cluster
    cl <- parallel::makeCluster(parallel_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary objects to cluster
    parallel::clusterEvalQ(cl, {
      library(dplyr)
      library(readr)
      library(stringr)
      library(glue)
      library(lubridate)
      if (requireNamespace("data.table", quietly = TRUE)) {
        library(data.table)
      }
    })
    
    parallel::clusterExport(cl, c(
      "use_data_table", "file_size_threshold_mb", "keep_only_summary", 
      "verbose_logging", "standardize_provider_data_columns_optimized",
      "apply_summary_filtering"
    ), envir = environment())
    
    # Process files in parallel
    file_data_list <- parallel::parLapply(cl, csv_files_list, optimized_read_csv)
    
  } else {
    # Sequential processing for smaller file sets or single core
    file_data_list <- lapply(csv_files_list, optimized_read_csv)
  }
  
  # Remove NULL entries (failed reads)
  file_data_list <- file_data_list[!sapply(file_data_list, is.null)]
  
  if (length(file_data_list) == 0) {
    stop("No files could be successfully read")
  }
  
  if (verbose_logging) {
    successful_files <- length(file_data_list)
    failed_files <- length(csv_files_list) - successful_files
    logger::log_info("   ✅ Successfully read {successful_files} files")
    if (failed_files > 0) {
      logger::log_warn("   ❌ Failed to read {failed_files} files")
    }
  }
  
  # Combine all data efficiently
  if (verbose_logging) {
    logger::log_info("   🔄 Combining data from all files...")
  }
  
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    # Use data.table::rbindlist for fastest binding
    combined_provider_data <- data.table::rbindlist(file_data_list, fill = TRUE, use.names = TRUE)
    combined_provider_data <- dplyr::as_tibble(combined_provider_data)
  } else {
    # Use dplyr::bind_rows as fallback
    combined_provider_data <- dplyr::bind_rows(file_data_list)
  }
  
  # Clean up memory
  rm(file_data_list)
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    successful_files = length(file_data_list),
    total_provider_records = nrow(combined_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing,
    parallel_cores_used = parallel_cores,
    data_table_used = use_data_table,
    average_file_size_mb = round(mean(file_sizes_mb, na.rm = TRUE), 1),
    large_files_count = large_files_count
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(combined_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(combined_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Parallel processing completed successfully")
    }
  }
  
  return(list(
    combined_provider_data = combined_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
read_file_in_chunks <- function(file_path, max_chunk_size, keep_only_summary, verbose_logging) {
  if (verbose_logging) {
    logger::log_info("   🔄 Processing {basename(file_path)} in chunks to save memory")
  }
  
  all_chunks <- list()
  chunk_num <- 1
  
  tryCatch({
    # Use data.table::fread for faster chunked reading if available
    if (requireNamespace("data.table", quietly = TRUE)) {
      repeat {
        chunk_data <- data.table::fread(
          file_path, 
          skip = (chunk_num - 1) * max_chunk_size,
          nrows = max_chunk_size,
          showProgress = FALSE
        )
        
        if (nrow(chunk_data) == 0) break
        
        # Convert to tibble and process
        chunk_data <- dplyr::as_tibble(chunk_data)
        chunk_data <- standardize_provider_data_columns_optimized(chunk_data, file_path, FALSE)
        
        # Apply filtering if in summary mode
        if (keep_only_summary) {
          chunk_data <- apply_summary_filtering(chunk_data, FALSE)
        }
        
        all_chunks[[chunk_num]] <- chunk_data
        chunk_num <- chunk_num + 1
        
        # Force garbage collection every 10 chunks
        if (chunk_num %% 10 == 0) {
          gc()
        }
      }
    } else {
      # Fallback to readr with chunking
      chunk_data <- readr::read_csv(file_path, 
                                    col_types = readr::cols(.default = readr::col_character()),
                                    n_max = max_chunk_size,
                                    show_col_types = FALSE)
      chunk_data <- standardize_provider_data_columns_optimized(chunk_data, file_path, FALSE)
      if (keep_only_summary) {
        chunk_data <- apply_summary_filtering(chunk_data, FALSE)
      }
      all_chunks[[1]] <- chunk_data
    }
    
    # Combine chunks efficiently
    if (requireNamespace("data.table", quietly = TRUE)) {
      combined_data <- data.table::rbindlist(all_chunks, fill = TRUE) %>% dplyr::as_tibble()
    } else {
      combined_data <- dplyr::bind_rows(all_chunks)
    }
    
    # Clear chunks from memory
    rm(all_chunks)
    gc()
    
    return(combined_data)
    
  }, error = function(e) {
    if (verbose_logging) {
      logger::log_error("   ❌ Chunked reading failed for {basename(file_path)}: {e$message}")
    }
    return(NULL)
  })
}

#' @noRd
apply_summary_filtering <- function(provider_data, verbose_logging) {
  if (!"provider_id" %in% colnames(provider_data)) {
    return(provider_data)
  }
  
  essential_columns <- c("provider_id", "source_file")
  geographic_columns <- c("city", "state", "county", "zip_code")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
  keep_columns <- intersect(keep_columns, colnames(provider_data))
  
  filtered_data <- provider_data %>%
    dplyr::select(all_of(keep_columns))
  
  if (verbose_logging) {
    logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
  }
  
  return(filtered_data)
}

#' @noRd
standardize_provider_data_columns_optimized <- function(file_data, file_path, verbose_logging) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  # Use data.table if available for faster conversion
  if (requireNamespace("data.table", quietly = TRUE) && data.table::is.data.table(file_data)) {
    # data.table approach - faster for large data
    char_cols <- names(file_data)
    file_data[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    file_data <- dplyr::as_tibble(file_data)
  } else {
    # dplyr approach for smaller data
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id (vectorized approach)
  col_names <- colnames(file_data)
  
  if (!"provider_id" %in% col_names) {
    # Check for alternative ID columns in order of preference
    id_alternatives <- c("physician_id", "npi", "physician_ids", "id")
    available_alternatives <- intersect(id_alternatives, col_names)
    
    if (length(available_alternatives) > 0) {
      # Use the first available alternative
      file_data$provider_id <- file_data[[available_alternatives[1]]]
    }
  }
  
  # Add file source for tracking (already character) - optimized
  file_data$source_file <- basename(file_path)
  file_data$processing_timestamp <- as.character(lubridate::now())
  
  return(file_data)
}

#' @noRd  
batch_combine_data_efficiently <- function(data_list, use_data_table = TRUE, verbose_logging = FALSE) {
  if (length(data_list) == 0) {
    return(NULL)
  }
  
  if (length(data_list) == 1) {
    return(data_list[[1]])
  }
  
  if (verbose_logging) {
    logger::log_info("   🔄 Efficiently combining {length(data_list)} data objects...")
  }
  
  # Use data.table for fastest binding if available
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    tryCatch({
      combined_data <- data.table::rbindlist(data_list, fill = TRUE, use.names = TRUE)
      return(dplyr::as_tibble(combined_data))
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  data.table binding failed, using dplyr: {e$message}")
      }
      return(dplyr::bind_rows(data_list))
    })
  } else {
    return(dplyr::bind_rows(data_list))
  }
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Use dplyr for all operations
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id) & provider_id != "" & provider_id != "NA") %>%
    dplyr::pull(provider_id) %>%
    unique() %>%
    as.numeric() %>%
    .[!is.na(.)] %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No valid numeric provider IDs found in data")
    }
    return(list(
      provider_id_range = c(NA, NA),
      total_unique_ids = 0,
      gaps_identified = 0,
      non_existent_provider_ids = numeric(0),
      truly_missing_provider_ids = numeric(0),
      existence_rate = 0,
      gap_analysis_summary = "No valid provider IDs found"
    ))
  }
  
  # Calculate basic statistics
  min_id <- min(all_provider_ids)
  max_id <- max(all_provider_ids)
  total_unique_ids <- length(all_provider_ids)
  full_range_size <- max_id - min_id + 1
  
  # Find gaps in the sequence
  complete_sequence <- seq(min_id, max_id)
  missing_ids <- setdiff(complete_sequence, all_provider_ids)
  gaps_identified <- length(missing_ids)
  
  # Calculate existence rate
  existence_rate <- (total_unique_ids / full_range_size) * 100
  
  if (verbose_logging) {
    logger::log_info("   📊 Provider ID analysis:")
    logger::log_info("      🆔 Range: {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}")
    logger::log_info("      ✅ Found IDs: {formatC(total_unique_ids, big.mark = ',')}")
    logger::log_info("      ❌ Missing IDs: {formatC(gaps_identified, big.mark = ',')}")
    logger::log_info("      📈 Existence rate: {round(existence_rate, 1)}%")
  }
  
  # Distinguish between different types of missing IDs
  non_existent_ids <- missing_ids[sample(length(missing_ids), min(1000, length(missing_ids)))]
  truly_missing_ids <- missing_ids
  
  gap_analysis_summary <- glue::glue(
    "Found {formatC(total_unique_ids, big.mark = ',')} unique provider IDs in range {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}. ",
    "Identified {formatC(gaps_identified, big.mark = ',')} gaps ({round(100 - existence_rate, 1)}% missing). ",
    "Existence rate: {round(existence_rate, 1)}%."
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Gap analysis completed")
  }
  
  return(list(
    provider_id_range = c(min_id, max_id),
    total_unique_ids = total_unique_ids,
    gaps_identified = gaps_identified,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_ids,
    existence_rate = existence_rate,
    gap_analysis_summary = gap_analysis_summary,
    full_range_size = full_range_size
  ))
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution...")
  }
  
  # Check for geographic columns
  geo_columns <- c("state", "city", "county", "zip_code", "address")
  available_geo_columns <- intersect(names(provider_data), geo_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No geographic columns found for distribution analysis")
    }
    return(list(
      geographic_summary = "No geographic data available",
      state_distribution = NULL,
      city_distribution = NULL
    ))
  }
  
  geographic_analysis <- list()
  
  # State-level analysis if available
  if ("state" %in% available_geo_columns) {
    state_dist <- provider_data %>%
      dplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
      dplyr::group_by(state) %>%
      dplyr::summarise(
        provider_count = dplyr::n(),
        unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(provider_count))
    
    geographic_analysis$state_distribution <- state_dist
    
    if (verbose_logging) {
      logger::log_info("   🏛️  State analysis: {nrow(state_dist)} states with providers")
    }
  }
  
  # City-level analysis if available  
  if ("city" %in% available_geo_columns && !keep_only_summary) {
    city_dist <- provider_data %>%
      dplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
      dplyr::group_by(city, state) %>%
      dplyr::summarise(
        provider_count = dplyr::n(),
        unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(provider_count)) %>%
      dplyr::slice_head(n = 100)  # Top 100 cities only
    
    geographic_analysis$city_distribution <- city_dist
    
    if (verbose_logging) {
      logger::log_info("   🏙️  City analysis: {nrow(city_dist)} cities analyzed (top 100)")
    }
  }
  
  # Generate summary
  total_with_geo <- provider_data %>%
    dplyr::filter(dplyr::if_any(dplyr::all_of(available_geo_columns), ~ !is.na(.) & . != "" & . != "NA")) %>%
    nrow()
  
  geo_coverage_rate <- (total_with_geo / nrow(provider_data)) * 100
  
  geographic_summary <- glue::glue(
    "Geographic data available for {formatC(total_with_geo, big.mark = ',')} of {formatC(nrow(provider_data), big.mark = ',')} records ({round(geo_coverage_rate, 1)}%). ",
    "Available columns: {paste(available_geo_columns, collapse = ', ')}."
  )
  
  geographic_analysis$geographic_summary <- geographic_summary
  geographic_analysis$coverage_rate <- geo_coverage_rate
  geographic_analysis$available_columns <- available_geo_columns
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Coverage: {round(geo_coverage_rate, 1)}% of records have geographic data")
  }
  
  return(geographic_analysis)
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
    logger::log_info("   📊 Strategy: {search_strategy}")
    logger::log_info("   📦 Batch size: {formatC(batch_size, big.mark = ',')}")
  }
  
  # Extract key information from analysis
  min_id <- analysis_results$provider_id_range[1]
  max_id <- analysis_results$provider_id_range[2]
  missing_ids <- analysis_results$truly_missing_provider_ids
  
  if (is.na(min_id) || is.na(max_id) || length(missing_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  Insufficient data for search recommendations")
    }
    return(numeric(0))
  }
  
  search_candidates <- numeric(0)
  
  # Generate search candidates based on strategy
  if (stringr::str_detect(search_strategy, "gaps")) {
    # Add missing IDs from gaps
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Sort missing IDs from highest to lowest
      gap_candidates <- sort(missing_ids, decreasing = TRUE)
    } else {
      # Smart gaps - mix of random and systematic
      gap_candidates <- sample(missing_ids, min(length(missing_ids), batch_size * 2))
    }
    search_candidates <- c(search_candidates, gap_candidates)
  }
  
  if (stringr::str_detect(search_strategy, "extend")) {
    # Add IDs beyond current range
    extension_size <- min(batch_size, 10000)  # Don't extend too far
    
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Extend upward from max
      extend_candidates <- seq(max_id + 1, max_id + extension_size)
    } else {
      # Smart extend - both directions
      extend_up <- seq(max_id + 1, max_id + ceiling(extension_size/2))
      extend_down <- seq(max(1, min_id - floor(extension_size/2)), min_id - 1)
      extend_candidates <- c(extend_up, extend_down)
    }
    search_candidates <- c(search_candidates, extend_candidates)
  }
  
  # Remove duplicates and limit to batch size
  search_candidates <- unique(search_candidates)
  search_candidates <- search_candidates[search_candidates > 0]  # Remove negative IDs
  
  # Limit to batch size
  if (length(search_candidates) > batch_size) {
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      search_candidates <- sort(search_candidates, decreasing = TRUE)[1:batch_size]
    } else {
      search_candidates <- sample(search_candidates, batch_size)
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🆔 Recommended IDs: {formatC(length(search_candidates), big.mark = ',')}")
    if (length(search_candidates) > 0) {
      logger::log_info("   📈 Range: {formatC(min(search_candidates), big.mark = ',')} to {formatC(max(search_candidates), big.mark = ',')}")
    }
  }
  
  return(search_candidates)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_df <- data.frame(
      provider_id = non_existent_provider_ids,
      exclusion_reason = "non_existent",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(non_existent_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath}.gz ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath} ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  # Save truly missing IDs  
  if (length(truly_missing_provider_ids) > 0) {
    missing_df <- data.frame(
      provider_id = truly_missing_provider_ids,
      exclusion_reason = "missing_from_search",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("missing_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(missing_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath}.gz ({formatC(length(truly_missing_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath} ({formatC(length(truly_missing_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Exclusion lists saved successfully")
  }
}

# run ----
# Optimized for 1,000 files - Complete configuration
results <- analyze_healthcare_access_patterns(
  # Required arguments
  search_results_directory = "physician_data",
  
  # File processing settings
  extraction_file_pattern = ".*\\.csv$",
  search_subdirectories = TRUE,           # Set TRUE if files are in subdirectories
  exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                       "wrong_ids_.*\\.csv$"),
  
  # Performance optimization settings
  parallel_cores = 8,                      # Use 8 cores (adjust for your CPU)
  use_data_table = TRUE,                   # Enable fast data.table methods
  file_size_threshold_mb = 25,             # Optimize files > 25MB
  progress_bar = TRUE,                     # Show progress through 1000 files
  
  # Memory management settings
  max_memory_gb = 32,                      # Set memory limit
  keep_only_summary = FALSE,               # Keep all data for comprehensive analysis
  chunk_processing = FALSE,                # Set TRUE if individual files are >1GB
  max_chunk_size = 100000,                 # Rows per chunk if chunk_processing = TRUE
  
  # Analysis and search settings
  batch_size_for_search = 7000,            # IDs to include in next search batch
  search_strategy = "smart_mixed",         # Strategy for gap filling
  
  # Output and storage settings
  external_drive_path = "/Volumes/MufflyNew",  # External drive for large files
  save_exclusion_lists = TRUE,             # Save lists of missing/non-existent IDs
  compress_output = TRUE,                  # Compress output files to save space
  
  # Logging and monitoring
  enable_verbose_logging = TRUE            # Detailed console logging
)


# Aug 14 ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#' @param parallel_cores Numeric. Number of CPU cores to use for parallel processing.
#'   Default is NULL (auto-detect). Set to 1 to disable parallel processing.
#' @param use_data_table Logical. Whether to use data.table for faster file reading
#'   and data manipulation. Highly recommended for large datasets. Default is TRUE.
#' @param file_size_threshold_mb Numeric. Files larger than this threshold (in MB)
#'   will be processed with optimized methods. Default is 50MB.
#' @param progress_bar Logical. Whether to show progress bar during file processing.
#'   Default is TRUE.
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: High-performance analysis for 1000+ files
#' large_dataset_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   parallel_cores = 8,
#'   use_data_table = TRUE,
#'   file_size_threshold_mb = 25,
#'   progress_bar = TRUE,
#'   chunk_processing = TRUE,
#'   max_chunk_size = 50000
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' @importFrom parallel detectCores makeCluster stopCluster parLapply clusterEvalQ clusterExport
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$"),
                                               parallel_cores = NULL,
                                               use_data_table = TRUE,
                                               file_size_threshold_mb = 50,
                                               progress_bar = TRUE) {
  
  # Check and load required packages for performance optimization
  if (use_data_table && !requireNamespace("data.table", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  data.table not available, falling back to base R methods")
    }
    use_data_table <- FALSE
  }
  
  # Set up parallel processing
  if (is.null(parallel_cores)) {
    parallel_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for system
  }
  
  if (parallel_cores > 1 && !requireNamespace("parallel", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  parallel package not available, using single-core processing")
    }
    parallel_cores <- 1
  }
  
  # Load preferred data manipulation package
  if (!require("duckplyr", quietly = TRUE)) {
    if (!require("dplyr", quietly = TRUE)) {
      stop("Neither duckplyr nor dplyr is available. Please install one of them.")
    }
    if (enable_verbose_logging) {
      logger::log_info("   ℹ️  Using dplyr (duckplyr not available)")
    }
    use_duckplyr <- FALSE
  } else {
    if (enable_verbose_logging) {
      logger::log_info("   🦆 Using duckplyr for enhanced performance")
    }
    use_duckplyr <- TRUE
  }
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  assertthat::assert_that(is.null(parallel_cores) || (is.numeric(parallel_cores) && parallel_cores >= 1))
  assertthat::assert_that(is.logical(use_data_table))
  assertthat::assert_that(is.numeric(file_size_threshold_mb) && file_size_threshold_mb > 0)
  assertthat::assert_that(is.logical(progress_bar))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
    logger::log_info("   🚀 Performance optimizations:")
    logger::log_info("      💻 CPU cores: {parallel_cores}")
    logger::log_info("      ⚡ data.table: {ifelse(use_data_table, 'enabled', 'disabled')}")
    logger::log_info("      📋 Progress bar: {ifelse(progress_bar, 'enabled', 'disabled')}")
    logger::log_info("      📏 File size threshold: {file_size_threshold_mb} MB")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files with optimizations
  file_processing_results <- process_healthcare_data_files_optimized(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns,
    parallel_cores = parallel_cores,
    use_data_table = use_data_table,
    file_size_threshold_mb = file_size_threshold_mb,
    progress_bar = progress_bar,
    use_duckplyr = use_duckplyr
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    use_duckplyr = use_duckplyr
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    use_duckplyr = use_duckplyr
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary,
      parallel_cores_used = parallel_cores,
      data_table_used = use_data_table,
      performance_optimized = TRUE
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Performance: Used {parallel_cores} cores with {ifelse(use_data_table, 'data.table', 'base R')} methods")
    }
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files_optimized <- function(directory_path, file_pattern, verbose_logging, 
                                                    keep_only_summary = FALSE, chunk_processing = FALSE, 
                                                    max_chunk_size = 100000, search_subdirectories = FALSE,
                                                    exclude_patterns = NULL, parallel_cores = 1,
                                                    use_data_table = TRUE, file_size_threshold_mb = 50,
                                                    progress_bar = TRUE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files with optimizations...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories
  )
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
    }
  }
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Analyze file sizes for optimization strategy
  file_info_data <- file.info(csv_files_list)
  file_sizes_mb <- file_info_data$size / (1024^2)
  large_files_count <- sum(file_sizes_mb > file_size_threshold_mb, na.rm = TRUE)
  
  if (verbose_logging) {
    logger::log_info("   📊 File size analysis:")
    logger::log_info("      📏 Average file size: {round(mean(file_sizes_mb, na.rm = TRUE), 1)} MB")
    logger::log_info("      📈 Largest file: {round(max(file_sizes_mb, na.rm = TRUE), 1)} MB") 
    logger::log_info("      🔥 Files > {file_size_threshold_mb}MB: {large_files_count}")
    if (parallel_cores > 1) {
      logger::log_info("      ⚡ Using {parallel_cores} cores for parallel processing")
    }
  }
  
  # Set up progress bar
  if (progress_bar && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing files [:bar] :percent :current/:total ETA: :eta",
      total = length(csv_files_list),
      clear = FALSE,
      width = 80
    )
    show_progress <- TRUE
  } else {
    show_progress <- FALSE
    if (progress_bar && verbose_logging) {
      logger::log_warn("⚠️  progress package not available, disabling progress bar")
    }
  }
  
  # Optimized file reading function
  optimized_read_csv <- function(file_path, file_index = NULL) {
    if (show_progress) {
      pb$tick()
    }
    
    tryCatch({
      # Check file size for optimization strategy
      file_size_mb <- file.info(file_path)$size / (1024^2)
      
      if (use_data_table && file_size_mb > file_size_threshold_mb) {
        # Use data.table::fread for large files (much faster)
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          nThread = 1,  # Control threading at file level
          stringsAsFactors = FALSE
        )
        # Convert to tibble if needed for consistency
        file_data <- dplyr::as_tibble(file_data)
      } else if (use_data_table) {
        # Use data.table for smaller files too for consistency  
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          stringsAsFactors = FALSE
        )
        file_data <- dplyr::as_tibble(file_data)
      } else {
        # Fallback to readr with optimized settings
        file_data <- readr::read_csv(
          file_path, 
          show_col_types = FALSE,
          lazy = FALSE,  # Read immediately for better memory management
          col_types = readr::cols(.default = readr::col_character()),
          locale = readr::locale(encoding = "UTF-8")
        )
      }
      
      # Standardize and filter early to save memory
      file_data <- standardize_provider_data_columns_optimized(file_data, file_path, FALSE, use_duckplyr)
      
      if (keep_only_summary && "provider_id" %in% colnames(file_data)) {
        file_data <- apply_summary_filtering(file_data, FALSE, use_duckplyr)  # Don't log for each file
      }
      
      return(file_data)
      
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read {basename(file_path)}: {e$message}")
      }
      return(NULL)
    })
  }
  
  # Process files - use parallel processing if beneficial
  if (parallel_cores > 1 && length(csv_files_list) > 10) {
    if (verbose_logging) {
      logger::log_info("   ⚡ Using parallel processing with {parallel_cores} cores")
    }
    
    # Set up cluster
    cl <- parallel::makeCluster(parallel_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary objects to cluster
    parallel::clusterEvalQ(cl, {
      library(dplyr)
      library(readr)
      library(stringr)
      library(glue)
      library(lubridate)
      if (requireNamespace("data.table", quietly = TRUE)) {
        library(data.table)
      }
      if (requireNamespace("duckplyr", quietly = TRUE)) {
        library(duckplyr)
      }
    })
    
    parallel::clusterExport(cl, c(
      "use_data_table", "file_size_threshold_mb", "keep_only_summary", 
      "verbose_logging", "standardize_provider_data_columns_optimized",
      "apply_summary_filtering", "use_duckplyr"
    ), envir = environment())
    
    # Process files in parallel
    file_data_list <- parallel::parLapply(cl, csv_files_list, optimized_read_csv)
    
  } else {
    # Sequential processing for smaller file sets or single core
    file_data_list <- lapply(csv_files_list, optimized_read_csv)
  }
  
  # Remove NULL entries (failed reads)
  file_data_list <- file_data_list[!sapply(file_data_list, is.null)]
  
  if (length(file_data_list) == 0) {
    stop("No files could be successfully read")
  }
  
  if (verbose_logging) {
    successful_files <- length(file_data_list)
    failed_files <- length(csv_files_list) - successful_files
    logger::log_info("   ✅ Successfully read {successful_files} files")
    if (failed_files > 0) {
      logger::log_warn("   ❌ Failed to read {failed_files} files")
    }
  }
  
  # Combine all data efficiently
  if (verbose_logging) {
    logger::log_info("   🔄 Combining data from all files...")
  }
  
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    # Use data.table::rbindlist for fastest binding
    combined_provider_data <- data.table::rbindlist(file_data_list, fill = TRUE, use.names = TRUE)
    combined_provider_data <- dplyr::as_tibble(combined_provider_data)
  } else {
    # Use dplyr::bind_rows as fallback
    combined_provider_data <- dplyr::bind_rows(file_data_list)
  }
  
  # Clean up memory
  rm(file_data_list)
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    successful_files = length(file_data_list),
    total_provider_records = nrow(combined_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing,
    parallel_cores_used = parallel_cores,
    data_table_used = use_data_table,
    average_file_size_mb = round(mean(file_sizes_mb, na.rm = TRUE), 1),
    large_files_count = large_files_count
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(combined_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(combined_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Parallel processing completed successfully")
    }
  }
  
  return(list(
    combined_provider_data = combined_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns_optimized <- function(file_data, file_path, verbose_logging, use_duckplyr = FALSE) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(duckplyr::across(duckplyr::everything(), as.character))
  } else if (requireNamespace("data.table", quietly = TRUE) && data.table::is.data.table(file_data)) {
    # data.table approach - faster for large data
    char_cols <- names(file_data)
    file_data[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    file_data <- dplyr::as_tibble(file_data)
  } else {
    # dplyr approach for smaller data
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id (vectorized approach)
  col_names <- colnames(file_data)
  
  if (!"provider_id" %in% col_names) {
    # Check for alternative ID columns in order of preference - expanded list for subspecialty files
    id_alternatives <- c(
      "physician_id", "npi", "physician_ids", "id", "provider_npi", "doctor_id",
      "subspecialty_id", "specialist_id", "physician_number", "provider_number",
      "doc_id", "dr_id", "medical_id", "practitioner_id"
    )
    available_alternatives <- intersect(id_alternatives, col_names)
    
    if (length(available_alternatives) > 0) {
      # Use the first available alternative
      file_data$provider_id <- file_data[[available_alternatives[1]]]
      if (verbose_logging) {
        logger::log_info("   🔄 Using '{available_alternatives[1]}' as provider_id for {basename(file_path)}")
      }
    } else {
      # Log what columns are available to help debug
      if (verbose_logging) {
        logger::log_warn("   ⚠️  No standard ID column found in {basename(file_path)}")
        logger::log_info("   🔍 Available columns: {paste(head(col_names, 10), collapse = ', ')}")
      }
    }
  }
  
  # Add file source for tracking (already character) - optimized
  file_data$source_file <- basename(file_path)
  file_data$processing_timestamp <- as.character(lubridate::now())
  
  return(file_data)
}

#' @noRd
apply_summary_filtering <- function(provider_data, verbose_logging, use_duckplyr = FALSE) {
  if (!"provider_id" %in% colnames(provider_data)) {
    return(provider_data)
  }
  
  essential_columns <- c("provider_id", "source_file")
  geographic_columns <- c("city", "state", "county", "zip_code")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
  keep_columns <- intersect(keep_columns, colnames(provider_data))
  
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    filtered_data <- provider_data %>%
      duckplyr::select(all_of(keep_columns))
  } else {
    filtered_data <- provider_data %>%
      dplyr::select(all_of(keep_columns))
  }
  
  if (verbose_logging) {
    logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
  }
  
  return(filtered_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Check if provider_id column exists
  if (!"provider_id" %in% colnames(combined_provider_data)) {
    if (verbose_logging) {
      logger::log_warn("⚠️  'provider_id' column not found in data")
      logger::log_info("   🔍 Available columns: {paste(colnames(combined_provider_data), collapse = ', ')}")
      
      # Try to find alternative ID columns
      potential_id_columns <- c("physician_id", "npi", "id", "physician_ids", "provider_npi", "doctor_id")
      available_id_columns <- intersect(potential_id_columns, colnames(combined_provider_data))
      
      if (length(available_id_columns) > 0) {
        logger::log_info("   💡 Found potential ID columns: {paste(available_id_columns, collapse = ', ')}")
        logger::log_info("   🔄 Attempting to use '{available_id_columns[1]}' as provider_id")
        
        # Use the first available alternative
        combined_provider_data$provider_id <- combined_provider_data[[available_id_columns[1]]]
      } else {
        logger::log_error("❌ No valid ID columns found for gap analysis")
        return(list(
          provider_id_range = c(NA, NA),
          total_unique_ids = 0,
          gaps_identified = 0,
          non_existent_provider_ids = numeric(0),
          truly_missing_provider_ids = numeric(0),
          existence_rate = 0,
          gap_analysis_summary = "No valid provider ID column found in data",
          error = "missing_provider_id_column"
        ))
      }
    }
  }
  
  # Use dplyr for all operations (duckplyr has limited support for complex operations)
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id) & provider_id != "" & provider_id != "NA") %>%
    dplyr::pull(provider_id) %>%
    unique() %>%
    as.numeric() %>%
    .[!is.na(.)] %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No valid numeric provider IDs found in data")
      logger::log_info("   🔍 Sample provider_id values: {paste(head(unique(combined_provider_data$provider_id), 10), collapse = ', ')}")
    }
    return(list(
      provider_id_range = c(NA, NA),
      total_unique_ids = 0,
      gaps_identified = 0,
      non_existent_provider_ids = numeric(0),
      truly_missing_provider_ids = numeric(0),
      existence_rate = 0,
      gap_analysis_summary = "No valid numeric provider IDs found - check data format",
      error = "no_numeric_ids"
    ))
  }
  
  # Calculate basic statistics
  min_id <- min(all_provider_ids)
  max_id <- max(all_provider_ids)
  total_unique_ids <- length(all_provider_ids)
  full_range_size <- max_id - min_id + 1
  
  # Find gaps in the sequence
  complete_sequence <- seq(min_id, max_id)
  missing_ids <- setdiff(complete_sequence, all_provider_ids)
  gaps_identified <- length(missing_ids)
  
  # Calculate existence rate
  existence_rate <- (total_unique_ids / full_range_size) * 100
  
  if (verbose_logging) {
    logger::log_info("   📊 Provider ID analysis:")
    logger::log_info("      🆔 Range: {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}")
    logger::log_info("      ✅ Found IDs: {formatC(total_unique_ids, big.mark = ',')}")
    logger::log_info("      ❌ Missing IDs: {formatC(gaps_identified, big.mark = ',')}")
    logger::log_info("      📈 Existence rate: {round(existence_rate, 1)}%")
  }
  
  # Identify non-existent IDs from failed searches - use dplyr for all operations
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique() %>%
    .[!is.na(.)]
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_ids <- setdiff(missing_ids, non_existent_ids)
  
  # Sample large lists to prevent memory issues
  if (length(non_existent_ids) > 10000) {
    non_existent_ids <- sample(non_existent_ids, 10000)
  }
  if (length(truly_missing_ids) > 100000) {
    truly_missing_ids <- sample(truly_missing_ids, 100000)
  }
  
  gap_analysis_summary <- glue::glue(
    "Found {formatC(total_unique_ids, big.mark = ',')} unique provider IDs in range {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}. ",
    "Identified {formatC(gaps_identified, big.mark = ',')} gaps ({round(100 - existence_rate, 1)}% missing). ",
    "Existence rate: {round(existence_rate, 1)}%."
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Gap analysis completed")
  }
  
  return(list(
    min_id_searched = min_id,
    max_id_searched = max_id,
    provider_id_range = c(min_id, max_id),
    total_unique_ids = total_unique_ids,
    gaps_identified = gaps_identified,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_ids,
    existence_rate = existence_rate,
    gap_analysis_summary = gap_analysis_summary,
    full_range_size = full_range_size
  ))
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(
      geographic_analysis_available = FALSE,
      geographic_summary = "No geographic data available",
      state_distribution = NULL,
      city_distribution = NULL
    ))
  }
  
  geographic_analysis <- list()
  
  # State-level analysis if available
  if ("state" %in% available_geo_columns) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      state_dist <- provider_data %>%
        duckplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        duckplyr::group_by(state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count))
    } else {
      state_dist <- provider_data %>%
        dplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        dplyr::group_by(state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count))
    }
    
    geographic_analysis$state_distribution <- state_dist
    
    if (verbose_logging) {
      logger::log_info("   🏛️  State analysis: {nrow(state_dist)} states with providers")
    }
  }
  
  # City-level analysis if available  
  if ("city" %in% available_geo_columns && !keep_only_summary) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      city_dist <- provider_data %>%
        duckplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        duckplyr::group_by(city, state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count)) %>%
        duckplyr::slice_head(n = 100)  # Top 100 cities only
    } else {
      city_dist <- provider_data %>%
        dplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        dplyr::group_by(city, state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count)) %>%
        dplyr::slice_head(n = 100)  # Top 100 cities only
    }
    
    geographic_analysis$city_distribution <- city_dist
    
    if (verbose_logging) {
      logger::log_info("   🏙️  City analysis: {nrow(city_dist)} cities analyzed (top 100)")
    }
  }
  
  # Generate summary
  total_with_geo <- provider_data %>%
    dplyr::filter(dplyr::if_any(dplyr::all_of(available_geo_columns), ~ !is.na(.) & . != "" & . != "NA")) %>%
    nrow()
  
  geo_coverage_rate <- (total_with_geo / nrow(provider_data)) * 100
  
  geographic_summary <- glue::glue(
    "Geographic data available for {formatC(total_with_geo, big.mark = ',')} of {formatC(nrow(provider_data), big.mark = ',')} records ({round(geo_coverage_rate, 1)}%). ",
    "Available columns: {paste(available_geo_columns, collapse = ', ')}."
  )
  
  geographic_analysis$geographic_analysis_available <- TRUE
  geographic_analysis$geographic_summary <- geographic_summary
  geographic_analysis$coverage_rate <- geo_coverage_rate
  geographic_analysis$available_columns <- available_geo_columns
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Coverage: {round(geo_coverage_rate, 1)}% of records have geographic data")
  }
  
  return(geographic_analysis)
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
    logger::log_info("   📊 Strategy: {search_strategy}")
    logger::log_info("   📦 Batch size: {formatC(batch_size, big.mark = ',')}")
  }
  
  # Extract key information from analysis
  min_id <- analysis_results$provider_id_range[1]
  max_id <- analysis_results$provider_id_range[2]
  missing_ids <- analysis_results$truly_missing_provider_ids
  
  if (is.na(min_id) || is.na(max_id) || length(missing_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  Insufficient data for search recommendations")
    }
    return(numeric(0))
  }
  
  search_candidates <- numeric(0)
  
  # Generate search candidates based on strategy
  if (stringr::str_detect(search_strategy, "gaps")) {
    # Add missing IDs from gaps
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Sort missing IDs from highest to lowest
      gap_candidates <- sort(missing_ids, decreasing = TRUE)
    } else {
      # Smart gaps - mix of random and systematic
      gap_candidates <- sample(missing_ids, min(length(missing_ids), batch_size * 2))
    }
    search_candidates <- c(search_candidates, gap_candidates)
  }
  
  if (stringr::str_detect(search_strategy, "extend")) {
    # Add IDs beyond current range
    extension_size <- min(batch_size, 10000)  # Don't extend too far
    
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Extend upward from max
      extend_candidates <- seq(max_id + 1, max_id + extension_size)
    } else {
      # Smart extend - both directions
      extend_up <- seq(max_id + 1, max_id + ceiling(extension_size/2))
      extend_down <- seq(max(1, min_id - floor(extension_size/2)), min_id - 1)
      extend_candidates <- c(extend_up, extend_down)
    }
    search_candidates <- c(search_candidates, extend_candidates)
  }
  
  # Remove duplicates and limit to batch size
  search_candidates <- unique(search_candidates)
  search_candidates <- search_candidates[search_candidates > 0]  # Remove negative IDs
  
  # Limit to batch size
  if (length(search_candidates) > batch_size) {
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      search_candidates <- sort(search_candidates, decreasing = TRUE)[1:batch_size]
    } else {
      search_candidates <- sample(search_candidates, batch_size)
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🆔 Recommended IDs: {formatC(length(search_candidates), big.mark = ',')}")
    if (length(search_candidates) > 0) {
      logger::log_info("   📈 Range: {formatC(min(search_candidates), big.mark = ',')} to {formatC(max(search_candidates), big.mark = ',')}")
      
      # Show strategy-specific sample
      if (stringr::str_detect(search_strategy, "high_to_low")) {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from highest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from highest: {first_few} ... ending with: {last_few}")
        }
      } else {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from lowest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from lowest: {first_few} ... ending with: {last_few}")
        }
      }
    }
  }
  
  return(search_candidates)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_df <- data.frame(
      provider_id = non_existent_provider_ids,
      exclusion_reason = "non_existent",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(non_existent_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath}.gz ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath} ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  # Save truly missing IDs  
  if (length(truly_missing_provider_ids) > 0) {
    # If there are more than 1 million missing IDs, save only a sample to save space
    if (length(truly_missing_provider_ids) > 1000000) {
      sample_size <- 100000
      sampled_missing_ids <- sample(truly_missing_provider_ids, sample_size)
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: sampling {formatC(sample_size, big.mark = ',')} missing IDs from {formatC(length(truly_missing_provider_ids), big.mark = ',')} total")
      }
      
      ids_to_save <- sampled_missing_ids
      file_suffix <- "_sampled"
    } else {
      ids_to_save <- truly_missing_provider_ids
      file_suffix <- ""
    }
    
    missing_df <- data.frame(
      provider_id = ids_to_save,
      exclusion_reason = "missing_from_search",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("missing_provider_ids{file_suffix}_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(missing_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath}.gz ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath} ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Exclusion lists saved successfully")
  }
}


# God help us we are going to be late for soccer try outs ----
# Aug 14 ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#' @param parallel_cores Numeric. Number of CPU cores to use for parallel processing.
#'   Default is NULL (auto-detect). Set to 1 to disable parallel processing.
#' @param use_data_table Logical. Whether to use data.table for faster file reading
#'   and data manipulation. Highly recommended for large datasets. Default is TRUE.
#' @param file_size_threshold_mb Numeric. Files larger than this threshold (in MB)
#'   will be processed with optimized methods. Default is 50MB.
#' @param progress_bar Logical. Whether to show progress bar during file processing.
#'   Default is TRUE.
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: High-performance analysis for 1000+ files
#' large_dataset_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   parallel_cores = 8,
#'   use_data_table = TRUE,
#'   file_size_threshold_mb = 25,
#'   progress_bar = TRUE,
#'   chunk_processing = TRUE,
#'   max_chunk_size = 50000
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' @importFrom parallel detectCores makeCluster stopCluster parLapply clusterEvalQ clusterExport
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$"),
                                               parallel_cores = NULL,
                                               use_data_table = TRUE,
                                               file_size_threshold_mb = 50,
                                               progress_bar = TRUE) {
  
  # Check and load required packages for performance optimization
  if (use_data_table && !requireNamespace("data.table", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  data.table not available, falling back to base R methods")
    }
    use_data_table <- FALSE
  }
  
  # Set up parallel processing
  if (is.null(parallel_cores)) {
    parallel_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for system
  }
  
  if (parallel_cores > 1 && !requireNamespace("parallel", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  parallel package not available, using single-core processing")
    }
    parallel_cores <- 1
  }
  
  # Load preferred data manipulation package
  if (!require("duckplyr", quietly = TRUE)) {
    if (!require("dplyr", quietly = TRUE)) {
      stop("Neither duckplyr nor dplyr is available. Please install one of them.")
    }
    if (enable_verbose_logging) {
      logger::log_info("   ℹ️  Using dplyr (duckplyr not available)")
    }
    use_duckplyr <- FALSE
  } else {
    if (enable_verbose_logging) {
      logger::log_info("   🦆 Using duckplyr for enhanced performance")
    }
    use_duckplyr <- TRUE
  }
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  assertthat::assert_that(is.null(parallel_cores) || (is.numeric(parallel_cores) && parallel_cores >= 1))
  assertthat::assert_that(is.logical(use_data_table))
  assertthat::assert_that(is.numeric(file_size_threshold_mb) && file_size_threshold_mb > 0)
  assertthat::assert_that(is.logical(progress_bar))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
    logger::log_info("   🚀 Performance optimizations:")
    logger::log_info("      💻 CPU cores: {parallel_cores}")
    logger::log_info("      ⚡ data.table: {ifelse(use_data_table, 'enabled', 'disabled')}")
    logger::log_info("      📋 Progress bar: {ifelse(progress_bar, 'enabled', 'disabled')}")
    logger::log_info("      📏 File size threshold: {file_size_threshold_mb} MB")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      stop(glue::glue("External drive path does not exist: {external_drive_path}"))
    }
    output_directory <- external_drive_path
    if (enable_verbose_logging) {
      logger::log_info("💾 Using external drive for output: {external_drive_path}")
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files with optimizations
  file_processing_results <- process_healthcare_data_files_optimized(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns,
    parallel_cores = parallel_cores,
    use_data_table = use_data_table,
    file_size_threshold_mb = file_size_threshold_mb,
    progress_bar = progress_bar,
    use_duckplyr = use_duckplyr
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    use_duckplyr = use_duckplyr
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    use_duckplyr = use_duckplyr
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary,
      parallel_cores_used = parallel_cores,
      data_table_used = use_data_table,
      performance_optimized = TRUE
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Performance: Used {parallel_cores} cores with {ifelse(use_data_table, 'data.table', 'base R')} methods")
    }
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files_optimized <- function(directory_path, file_pattern, verbose_logging, 
                                                    keep_only_summary = FALSE, chunk_processing = FALSE, 
                                                    max_chunk_size = 100000, search_subdirectories = FALSE,
                                                    exclude_patterns = NULL, parallel_cores = 1,
                                                    use_data_table = TRUE, file_size_threshold_mb = 50,
                                                    progress_bar = TRUE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files with optimizations...")
  }
  
  # Get list of matching files
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories
  )
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
    }
  }
  
  if (length(csv_files_list) == 0) {
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Analyze file sizes for optimization strategy
  file_info_data <- file.info(csv_files_list)
  file_sizes_mb <- file_info_data$size / (1024^2)
  large_files_count <- sum(file_sizes_mb > file_size_threshold_mb, na.rm = TRUE)
  
  if (verbose_logging) {
    logger::log_info("   📊 File size analysis:")
    logger::log_info("      📏 Average file size: {round(mean(file_sizes_mb, na.rm = TRUE), 1)} MB")
    logger::log_info("      📈 Largest file: {round(max(file_sizes_mb, na.rm = TRUE), 1)} MB") 
    logger::log_info("      🔥 Files > {file_size_threshold_mb}MB: {large_files_count}")
    if (parallel_cores > 1) {
      logger::log_info("      ⚡ Using {parallel_cores} cores for parallel processing")
    }
  }
  
  # Set up progress bar
  if (progress_bar && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing files [:bar] :percent :current/:total ETA: :eta",
      total = length(csv_files_list),
      clear = FALSE,
      width = 80
    )
    show_progress <- TRUE
  } else {
    show_progress <- FALSE
    if (progress_bar && verbose_logging) {
      logger::log_warn("⚠️  progress package not available, disabling progress bar")
    }
  }
  
  # Optimized file reading function
  optimized_read_csv <- function(file_path, file_index = NULL) {
    if (show_progress) {
      pb$tick()
    }
    
    tryCatch({
      # Check file size for optimization strategy
      file_size_mb <- file.info(file_path)$size / (1024^2)
      
      if (use_data_table && file_size_mb > file_size_threshold_mb) {
        # Use data.table::fread for large files (much faster)
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          nThread = 1,  # Control threading at file level
          stringsAsFactors = FALSE
        )
        # Convert to tibble if needed for consistency
        file_data <- dplyr::as_tibble(file_data)
      } else if (use_data_table) {
        # Use data.table for smaller files too for consistency  
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          stringsAsFactors = FALSE
        )
        file_data <- dplyr::as_tibble(file_data)
      } else {
        # Fallback to readr with optimized settings
        file_data <- readr::read_csv(
          file_path, 
          show_col_types = FALSE,
          lazy = FALSE,  # Read immediately for better memory management
          col_types = readr::cols(.default = readr::col_character()),
          locale = readr::locale(encoding = "UTF-8")
        )
      }
      
      # Standardize and filter early to save memory
      file_data <- standardize_provider_data_columns_optimized(file_data, file_path, FALSE, use_duckplyr)
      
      if (keep_only_summary && "provider_id" %in% colnames(file_data)) {
        file_data <- apply_summary_filtering(file_data, FALSE, use_duckplyr)  # Don't log for each file
      }
      
      return(file_data)
      
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read {basename(file_path)}: {e$message}")
      }
      return(NULL)
    })
  }
  
  # Process files - use parallel processing if beneficial
  if (parallel_cores > 1 && length(csv_files_list) > 10) {
    if (verbose_logging) {
      logger::log_info("   ⚡ Using parallel processing with {parallel_cores} cores")
    }
    
    # Set up cluster
    cl <- parallel::makeCluster(parallel_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary objects to cluster
    parallel::clusterEvalQ(cl, {
      library(dplyr)
      library(readr)
      library(stringr)
      library(glue)
      library(lubridate)
      if (requireNamespace("data.table", quietly = TRUE)) {
        library(data.table)
      }
      if (requireNamespace("duckplyr", quietly = TRUE)) {
        library(duckplyr)
      }
    })
    
    parallel::clusterExport(cl, c(
      "use_data_table", "file_size_threshold_mb", "keep_only_summary", 
      "verbose_logging", "standardize_provider_data_columns_optimized",
      "apply_summary_filtering", "use_duckplyr"
    ), envir = environment())
    
    # Process files in parallel
    file_data_list <- parallel::parLapply(cl, csv_files_list, optimized_read_csv)
    
  } else {
    # Sequential processing for smaller file sets or single core
    file_data_list <- lapply(csv_files_list, optimized_read_csv)
  }
  
  # Remove NULL entries (failed reads)
  file_data_list <- file_data_list[!sapply(file_data_list, is.null)]
  
  if (length(file_data_list) == 0) {
    stop("No files could be successfully read")
  }
  
  if (verbose_logging) {
    successful_files <- length(file_data_list)
    failed_files <- length(csv_files_list) - successful_files
    logger::log_info("   ✅ Successfully read {successful_files} files")
    if (failed_files > 0) {
      logger::log_warn("   ❌ Failed to read {failed_files} files")
    }
  }
  
  # Combine all data efficiently
  if (verbose_logging) {
    logger::log_info("   🔄 Combining data from all files...")
  }
  
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    # Use data.table::rbindlist for fastest binding
    combined_provider_data <- data.table::rbindlist(file_data_list, fill = TRUE, use.names = TRUE)
    combined_provider_data <- dplyr::as_tibble(combined_provider_data)
  } else {
    # Use dplyr::bind_rows as fallback
    combined_provider_data <- dplyr::bind_rows(file_data_list)
  }
  
  # Clean up memory
  rm(file_data_list)
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    successful_files = length(file_data_list),
    total_provider_records = nrow(combined_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing,
    parallel_cores_used = parallel_cores,
    data_table_used = use_data_table,
    average_file_size_mb = round(mean(file_sizes_mb, na.rm = TRUE), 1),
    large_files_count = large_files_count
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(combined_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(combined_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Parallel processing completed successfully")
    }
  }
  
  return(list(
    combined_provider_data = combined_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns_optimized <- function(file_data, file_path, verbose_logging, use_duckplyr = FALSE) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(duckplyr::across(duckplyr::everything(), as.character))
  } else if (requireNamespace("data.table", quietly = TRUE) && data.table::is.data.table(file_data)) {
    # data.table approach - faster for large data
    char_cols <- names(file_data)
    file_data[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    file_data <- dplyr::as_tibble(file_data)
  } else {
    # dplyr approach for smaller data
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id (vectorized approach)
  col_names <- colnames(file_data)
  
  if (!"provider_id" %in% col_names) {
    # Check for alternative ID columns in order of preference - expanded list for subspecialty files
    id_alternatives <- c(
      "physician_id", "npi", "physician_ids", "id", "provider_npi", "doctor_id",
      "subspecialty_id", "specialist_id", "physician_number", "provider_number",
      "doc_id", "dr_id", "medical_id", "practitioner_id"
    )
    available_alternatives <- intersect(id_alternatives, col_names)
    
    if (length(available_alternatives) > 0) {
      # Use the first available alternative
      file_data$provider_id <- file_data[[available_alternatives[1]]]
      if (verbose_logging) {
        logger::log_info("   🔄 Using '{available_alternatives[1]}' as provider_id for {basename(file_path)}")
      }
    } else {
      # Log what columns are available to help debug
      if (verbose_logging) {
        logger::log_warn("   ⚠️  No standard ID column found in {basename(file_path)}")
        logger::log_info("   🔍 Available columns: {paste(head(col_names, 10), collapse = ', ')}")
      }
    }
  }
  
  # Add file source for tracking (already character) - optimized
  file_data$source_file <- basename(file_path)
  file_data$processing_timestamp <- as.character(lubridate::now())
  
  return(file_data)
}

#' @noRd
apply_summary_filtering <- function(provider_data, verbose_logging, use_duckplyr = FALSE) {
  if (!"provider_id" %in% colnames(provider_data)) {
    return(provider_data)
  }
  
  essential_columns <- c("provider_id", "source_file")
  geographic_columns <- c("city", "state", "county", "zip_code")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
  keep_columns <- intersect(keep_columns, colnames(provider_data))
  
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    filtered_data <- provider_data %>%
      duckplyr::select(all_of(keep_columns))
  } else {
    filtered_data <- provider_data %>%
      dplyr::select(all_of(keep_columns))
  }
  
  if (verbose_logging) {
    logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
  }
  
  return(filtered_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Check if provider_id column exists
  if (!"provider_id" %in% colnames(combined_provider_data)) {
    if (verbose_logging) {
      logger::log_warn("⚠️  'provider_id' column not found in data")
      logger::log_info("   🔍 Available columns: {paste(colnames(combined_provider_data), collapse = ', ')}")
      
      # Try to find alternative ID columns
      potential_id_columns <- c("physician_id", "npi", "id", "physician_ids", "provider_npi", "doctor_id")
      available_id_columns <- intersect(potential_id_columns, colnames(combined_provider_data))
      
      if (length(available_id_columns) > 0) {
        logger::log_info("   💡 Found potential ID columns: {paste(available_id_columns, collapse = ', ')}")
        logger::log_info("   🔄 Attempting to use '{available_id_columns[1]}' as provider_id")
        
        # Use the first available alternative
        combined_provider_data$provider_id <- combined_provider_data[[available_id_columns[1]]]
      } else {
        logger::log_error("❌ No valid ID columns found for gap analysis")
        return(list(
          provider_id_range = c(NA, NA),
          total_unique_ids = 0,
          gaps_identified = 0,
          non_existent_provider_ids = numeric(0),
          truly_missing_provider_ids = numeric(0),
          existence_rate = 0,
          gap_analysis_summary = "No valid provider ID column found in data",
          error = "missing_provider_id_column"
        ))
      }
    }
  }
  
  # Use dplyr for all operations (duckplyr has limited support for complex operations)
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id) & provider_id != "" & provider_id != "NA") %>%
    dplyr::pull(provider_id) %>%
    unique() %>%
    as.numeric() %>%
    .[!is.na(.)] %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No valid numeric provider IDs found in data")
      logger::log_info("   🔍 Sample provider_id values: {paste(head(unique(combined_provider_data$provider_id), 10), collapse = ', ')}")
    }
    return(list(
      provider_id_range = c(NA, NA),
      total_unique_ids = 0,
      gaps_identified = 0,
      non_existent_provider_ids = numeric(0),
      truly_missing_provider_ids = numeric(0),
      existence_rate = 0,
      gap_analysis_summary = "No valid numeric provider IDs found - check data format",
      error = "no_numeric_ids"
    ))
  }
  
  # Calculate basic statistics
  min_id <- min(all_provider_ids)
  max_id <- max(all_provider_ids)
  total_unique_ids <- length(all_provider_ids)
  full_range_size <- max_id - min_id + 1
  
  # Find gaps in the sequence
  complete_sequence <- seq(min_id, max_id)
  missing_ids <- setdiff(complete_sequence, all_provider_ids)
  gaps_identified <- length(missing_ids)
  
  # Calculate existence rate
  existence_rate <- (total_unique_ids / full_range_size) * 100
  
  if (verbose_logging) {
    logger::log_info("   📊 Provider ID analysis:")
    logger::log_info("      🆔 Range: {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}")
    logger::log_info("      ✅ Found IDs: {formatC(total_unique_ids, big.mark = ',')}")
    logger::log_info("      ❌ Missing IDs: {formatC(gaps_identified, big.mark = ',')}")
    logger::log_info("      📈 Existence rate: {round(existence_rate, 1)}%")
  }
  
  # Identify non-existent IDs from failed searches - use dplyr for all operations
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique() %>%
    .[!is.na(.)]
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_ids <- setdiff(missing_ids, non_existent_ids)
  
  # Sample large lists to prevent memory issues
  if (length(non_existent_ids) > 10000) {
    non_existent_ids <- sample(non_existent_ids, 10000)
  }
  if (length(truly_missing_ids) > 100000) {
    truly_missing_ids <- sample(truly_missing_ids, 100000)
  }
  
  gap_analysis_summary <- glue::glue(
    "Found {formatC(total_unique_ids, big.mark = ',')} unique provider IDs in range {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}. ",
    "Identified {formatC(gaps_identified, big.mark = ',')} gaps ({round(100 - existence_rate, 1)}% missing). ",
    "Existence rate: {round(existence_rate, 1)}%."
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Gap analysis completed")
  }
  
  return(list(
    min_id_searched = min_id,
    max_id_searched = max_id,
    provider_id_range = c(min_id, max_id),
    total_unique_ids = total_unique_ids,
    gaps_identified = gaps_identified,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_ids,
    existence_rate = existence_rate,
    gap_analysis_summary = gap_analysis_summary,
    full_range_size = full_range_size
  ))
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(
      geographic_analysis_available = FALSE,
      geographic_summary = "No geographic data available",
      state_distribution = NULL,
      city_distribution = NULL
    ))
  }
  
  geographic_analysis <- list()
  
  # State-level analysis if available
  if ("state" %in% available_geo_columns) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      state_dist <- provider_data %>%
        duckplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        duckplyr::group_by(state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count))
    } else {
      state_dist <- provider_data %>%
        dplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        dplyr::group_by(state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count))
    }
    
    geographic_analysis$state_distribution <- state_dist
    
    if (verbose_logging) {
      logger::log_info("   🏛️  State analysis: {nrow(state_dist)} states with providers")
    }
  }
  
  # City-level analysis if available  
  if ("city" %in% available_geo_columns && !keep_only_summary) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      city_dist <- provider_data %>%
        duckplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        duckplyr::group_by(city, state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count)) %>%
        duckplyr::slice_head(n = 100)  # Top 100 cities only
    } else {
      city_dist <- provider_data %>%
        dplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        dplyr::group_by(city, state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count)) %>%
        dplyr::slice_head(n = 100)  # Top 100 cities only
    }
    
    geographic_analysis$city_distribution <- city_dist
    
    if (verbose_logging) {
      logger::log_info("   🏙️  City analysis: {nrow(city_dist)} cities analyzed (top 100)")
    }
  }
  
  # Generate summary
  total_with_geo <- provider_data %>%
    dplyr::filter(dplyr::if_any(dplyr::all_of(available_geo_columns), ~ !is.na(.) & . != "" & . != "NA")) %>%
    nrow()
  
  geo_coverage_rate <- (total_with_geo / nrow(provider_data)) * 100
  
  geographic_summary <- glue::glue(
    "Geographic data available for {formatC(total_with_geo, big.mark = ',')} of {formatC(nrow(provider_data), big.mark = ',')} records ({round(geo_coverage_rate, 1)}%). ",
    "Available columns: {paste(available_geo_columns, collapse = ', ')}."
  )
  
  geographic_analysis$geographic_analysis_available <- TRUE
  geographic_analysis$geographic_summary <- geographic_summary
  geographic_analysis$coverage_rate <- geo_coverage_rate
  geographic_analysis$available_columns <- available_geo_columns
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Coverage: {round(geo_coverage_rate, 1)}% of records have geographic data")
  }
  
  return(geographic_analysis)
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
    logger::log_info("   📊 Strategy: {search_strategy}")
    logger::log_info("   📦 Batch size: {formatC(batch_size, big.mark = ',')}")
  }
  
  # Extract key information from analysis
  min_id <- analysis_results$provider_id_range[1]
  max_id <- analysis_results$provider_id_range[2]
  missing_ids <- analysis_results$truly_missing_provider_ids
  
  if (is.na(min_id) || is.na(max_id) || length(missing_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  Insufficient data for search recommendations")
    }
    return(numeric(0))
  }
  
  search_candidates <- numeric(0)
  
  # Generate search candidates based on strategy
  if (stringr::str_detect(search_strategy, "gaps")) {
    # Add missing IDs from gaps
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Sort missing IDs from highest to lowest
      gap_candidates <- sort(missing_ids, decreasing = TRUE)
    } else {
      # Smart gaps - mix of random and systematic
      gap_candidates <- sample(missing_ids, min(length(missing_ids), batch_size * 2))
    }
    search_candidates <- c(search_candidates, gap_candidates)
  }
  
  if (stringr::str_detect(search_strategy, "extend")) {
    # Add IDs beyond current range
    extension_size <- min(batch_size, 10000)  # Don't extend too far
    
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Extend upward from max
      extend_candidates <- seq(max_id + 1, max_id + extension_size)
    } else {
      # Smart extend - both directions
      extend_up <- seq(max_id + 1, max_id + ceiling(extension_size/2))
      extend_down <- seq(max(1, min_id - floor(extension_size/2)), min_id - 1)
      extend_candidates <- c(extend_up, extend_down)
    }
    search_candidates <- c(search_candidates, extend_candidates)
  }
  
  # Remove duplicates and limit to batch size
  search_candidates <- unique(search_candidates)
  search_candidates <- search_candidates[search_candidates > 0]  # Remove negative IDs
  
  # Limit to batch size
  if (length(search_candidates) > batch_size) {
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      search_candidates <- sort(search_candidates, decreasing = TRUE)[1:batch_size]
    } else {
      search_candidates <- sample(search_candidates, batch_size)
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🆔 Recommended IDs: {formatC(length(search_candidates), big.mark = ',')}")
    if (length(search_candidates) > 0) {
      logger::log_info("   📈 Range: {formatC(min(search_candidates), big.mark = ',')} to {formatC(max(search_candidates), big.mark = ',')}")
      
      # Show strategy-specific sample
      if (stringr::str_detect(search_strategy, "high_to_low")) {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from highest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from highest: {first_few} ... ending with: {last_few}")
        }
      } else {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from lowest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from lowest: {first_few} ... ending with: {last_few}")
        }
      }
    }
  }
  
  return(search_candidates)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_df <- data.frame(
      provider_id = non_existent_provider_ids,
      exclusion_reason = "non_existent",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(non_existent_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath}.gz ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath} ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  # Save truly missing IDs  
  if (length(truly_missing_provider_ids) > 0) {
    # If there are more than 1 million missing IDs, save only a sample to save space
    if (length(truly_missing_provider_ids) > 1000000) {
      sample_size <- 100000
      sampled_missing_ids <- sample(truly_missing_provider_ids, sample_size)
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: sampling {formatC(sample_size, big.mark = ',')} missing IDs from {formatC(length(truly_missing_provider_ids), big.mark = ',')} total")
      }
      
      ids_to_save <- sampled_missing_ids
      file_suffix <- "_sampled"
    } else {
      ids_to_save <- truly_missing_provider_ids
      file_suffix <- ""
    }
    
    missing_df <- data.frame(
      provider_id = ids_to_save,
      exclusion_reason = "missing_from_search",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("missing_provider_ids{file_suffix}_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(missing_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath}.gz ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath} ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Exclusion lists saved successfully")
  }
}

# run ----
# This should now work perfectly with your files
healthcare_access_analysis_filtered <- analyze_healthcare_access_patterns(
  search_results_directory = "~/Dropbox (Personal)/isochrones/physician_data",
  extraction_file_pattern = "combined_subspecialty_extractions_.*\\.csv$",
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE,
  batch_size_for_search = 5000,
  search_strategy = "high_to_low_mixed",
  external_drive_path = "/Volumes/MufflyNew",
  compress_output = TRUE,
  keep_only_summary = FALSE,
  chunk_processing = TRUE,
  max_chunk_size = 25000,
  search_subdirectories = TRUE,
  exclude_patterns = c(
    "sequential_discovery_.*\\.csv$",
    "wrong_ids_.*\\.csv$",
    "temp_.*\\.csv$",
    "backup_.*\\.csv$",
    "truly_missing_.*\\.csv$",
    "non_existent_provider_.*\\.csv$",
    "extraction_summary_.*\\.csv$",
    "valid_physician_ids_.*\\.csv$"
  ),
  # Performance settings
  parallel_cores = 8,
  use_data_table = TRUE,
  file_size_threshold_mb = 25,
  progress_bar = TRUE
)


# August 16 Pay soccer club and wait for 1 hour but we are not sure if son will be on the team.  Thanks Cherry Creek.  ----
# Aug 14 ----
#' Analyze Geographic Healthcare Access Patterns
#'
#' A comprehensive function that analyzes geographic disparities in healthcare
#' provider accessibility, following methodology similar to gynecologic oncology
#' workforce distribution studies. Processes search results, identifies gaps,
#' and generates smart search recommendations.
#'
#' @param search_results_directory Character. Path to directory containing CSV 
#'   files with healthcare provider search results. Must be a valid directory 
#'   path with read permissions.
#' @param extraction_file_pattern Character. Regular expression pattern to match
#'   CSV files for analysis. Default is ".*\\.csv$" to match all CSV files.
#'   Use specific patterns like "physician_.*\\.csv$" for targeted analysis.
#' @param enable_verbose_logging Logical. Whether to enable detailed console 
#'   logging of all processing steps, data transformations, and results.
#'   Default is TRUE for comprehensive tracking.
#' @param save_exclusion_lists Logical. Whether to save lists of non-existent
#'   and missing provider IDs to CSV files for further analysis. Default is TRUE.
#' @param external_drive_path Character. Optional path to external drive for saving
#'   large files and results. If NULL, saves to local directory. Recommended for
#'   large datasets to save local disk space. Default is NULL.
#' @param compress_output Logical. Whether to compress output files using gzip
#'   to save disk space. Default is TRUE for space efficiency.
#' @param keep_only_summary Logical. Whether to keep only summary results and
#'   discard large intermediate datasets to save memory and disk space. 
#'   When FALSE, keeps all columns from original data. Default is FALSE.
#' @param chunk_processing Logical. Whether to process large files in chunks
#'   to reduce memory usage. Recommended for files over 1GB or when experiencing
#'   memory issues. Default is FALSE.
#' @param exclude_patterns Character vector. Regular expression patterns for files
#'   to exclude from analysis. Files matching these patterns will be ignored.
#'   Default excludes sequential discovery files: c("sequential_discovery_.*\\.csv$").
#' @param batch_size_for_search Numeric. Maximum number of provider IDs to 
#'   include in next search batch. Must be positive integer. Default is 7000.
#' @param search_strategy Character. Strategy for generating next search list.
#'   Options: "smart_gaps" (fill missing IDs), "smart_extend" (new IDs beyond max),
#'   "smart_mixed" (combination of both), "high_to_low_gaps" (fill gaps from highest first),
#'   "high_to_low_extend" (extend beyond max going higher), "high_to_low_mixed" (both high-to-low).
#'   Default is "smart_mixed".
#' @param parallel_cores Numeric. Number of CPU cores to use for parallel processing.
#'   Default is NULL (auto-detect). Set to 1 to disable parallel processing.
#' @param use_data_table Logical. Whether to use data.table for faster file reading
#'   and data manipulation. Highly recommended for large datasets. Default is TRUE.
#' @param file_size_threshold_mb Numeric. Files larger than this threshold (in MB)
#'   will be processed with optimized methods. Default is 50MB.
#' @param progress_bar Logical. Whether to show progress bar during file processing.
#'   Default is TRUE.
#'
#' @return Named list containing:
#'   \itemize{
#'     \item file_summary: Summary of files processed and directory structure
#'     \item smart_analysis: Analysis of gaps, existence rates, and search coverage
#'     \item geographic_patterns: Geographic distribution analysis (if applicable)
#'     \item next_search_recommendations: Recommended IDs for future searches
#'   }
#'
#' @examples
#' # Example 1: Basic analysis with default settings
#' healthcare_access_results <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/physician_searches_2025",
#'   extraction_file_pattern = ".*physician.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "smart_mixed"
#' )
#'
#' # Example 2: Focused analysis on specific subspecialty with gap-filling
#' oncology_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/oncology_provider_data",
#'   extraction_file_pattern = ".*oncology.*extractions.*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = FALSE,
#'   batch_size_for_search = 3000,
#'   search_strategy = "smart_gaps"
#' )
#'
#' # Example 3: High-performance analysis for 1000+ files
#' large_dataset_analysis <- analyze_healthcare_access_patterns(
#'   search_results_directory = "data/large_provider_study_2025",
#'   extraction_file_pattern = ".*\\.csv$",
#'   enable_verbose_logging = TRUE,
#'   save_exclusion_lists = TRUE,
#'   batch_size_for_search = 5000,
#'   search_strategy = "high_to_low_mixed",
#'   external_drive_path = "/Volumes/ExternalDrive",
#'   parallel_cores = 8,
#'   use_data_table = TRUE,
#'   file_size_threshold_mb = 25,
#'   progress_bar = TRUE,
#'   chunk_processing = TRUE,
#'   max_chunk_size = 50000
#' )
#'
#' @importFrom dplyr filter mutate select bind_rows arrange group_by 
#'   summarise n distinct across everything pull
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_extract str_detect str_remove_all
#' @importFrom purrr map_dfr map_chr possibly
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that
#' @importFrom lubridate now
#' @importFrom fs dir_exists file_exists path
#' @importFrom glue glue
#' @importFrom parallel detectCores makeCluster stopCluster parLapply clusterEvalQ clusterExport
#' 
#' @export
analyze_healthcare_access_patterns <- function(search_results_directory,
                                               extraction_file_pattern = ".*\\.csv$",
                                               enable_verbose_logging = TRUE,
                                               save_exclusion_lists = TRUE,
                                               batch_size_for_search = 7000,
                                               search_strategy = "smart_mixed",
                                               external_drive_path = NULL,
                                               compress_output = TRUE,
                                               keep_only_summary = FALSE,
                                               chunk_processing = FALSE,
                                               max_chunk_size = 100000,
                                               search_subdirectories = FALSE,
                                               exclude_patterns = c("sequential_discovery_.*\\.csv$", 
                                                                    "wrong_ids_.*\\.csv$"),
                                               parallel_cores = NULL,
                                               use_data_table = TRUE,
                                               file_size_threshold_mb = 50,
                                               progress_bar = TRUE) {
  
  # Check and load required packages for performance optimization
  if (use_data_table && !requireNamespace("data.table", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  data.table not available, falling back to base R methods")
    }
    use_data_table <- FALSE
  }
  
  # Set up parallel processing
  if (is.null(parallel_cores)) {
    parallel_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for system
  }
  
  if (parallel_cores > 1 && !requireNamespace("parallel", quietly = TRUE)) {
    if (enable_verbose_logging) {
      logger::log_warn("⚠️  parallel package not available, using single-core processing")
    }
    parallel_cores <- 1
  }
  
  # Load preferred data manipulation package
  use_duckplyr <- FALSE  # Force disable duckplyr for now due to compatibility issues
  
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    # Test if duckplyr mutate function works
    tryCatch({
      test_df <- data.frame(x = 1)
      test_result <- test_df %>% duckplyr::mutate(y = 2)
      if (enable_verbose_logging) {
        logger::log_info("   🦆 Using duckplyr for enhanced performance")
      }
      use_duckplyr <- TRUE
    }, error = function(e) {
      if (enable_verbose_logging) {
        logger::log_warn("   ⚠️  duckplyr compatibility issue, using dplyr instead")
      }
      use_duckplyr <- FALSE
    })
  }
  
  if (!use_duckplyr) {
    if (!requireNamespace("dplyr", quietly = TRUE)) {
      stop("dplyr is required but not available. Please install dplyr.")
    }
    if (enable_verbose_logging) {
      logger::log_info("   📊 Using dplyr for data manipulation")
    }
  }
  
  # Input validation with comprehensive assertions
  assertthat::assert_that(is.character(search_results_directory))
  assertthat::assert_that(is.character(extraction_file_pattern))
  assertthat::assert_that(is.logical(enable_verbose_logging))
  assertthat::assert_that(is.logical(save_exclusion_lists))
  assertthat::assert_that(is.numeric(batch_size_for_search) && batch_size_for_search > 0)
  assertthat::assert_that(search_strategy %in% c("smart_gaps", "smart_extend", "smart_mixed", 
                                                 "high_to_low_gaps", "high_to_low_extend", "high_to_low_mixed"))
  assertthat::assert_that(is.null(external_drive_path) || is.character(external_drive_path))
  assertthat::assert_that(is.logical(compress_output))
  assertthat::assert_that(is.logical(keep_only_summary))
  assertthat::assert_that(is.logical(chunk_processing))
  assertthat::assert_that(is.numeric(max_chunk_size) && max_chunk_size > 0)
  assertthat::assert_that(is.logical(search_subdirectories))
  assertthat::assert_that(is.character(exclude_patterns) || is.null(exclude_patterns))
  assertthat::assert_that(is.null(parallel_cores) || (is.numeric(parallel_cores) && parallel_cores >= 1))
  assertthat::assert_that(is.logical(use_data_table))
  assertthat::assert_that(is.numeric(file_size_threshold_mb) && file_size_threshold_mb > 0)
  assertthat::assert_that(is.logical(progress_bar))
  
  if (enable_verbose_logging) {
    logger::log_info("🏥 Starting healthcare access pattern analysis...")
    logger::log_info("   📁 Directory: {search_results_directory}")
    logger::log_info("   🔍 File pattern: {extraction_file_pattern}")
    logger::log_info("   📊 Batch size: {formatC(batch_size_for_search, big.mark = ',', format = 'd')}")
    logger::log_info("   🎯 Strategy: {search_strategy}")
    logger::log_info("   🚀 Performance optimizations:")
    logger::log_info("      💻 CPU cores: {parallel_cores}")
    logger::log_info("      ⚡ data.table: {ifelse(use_data_table, 'enabled', 'disabled')}")
    logger::log_info("      📋 Progress bar: {ifelse(progress_bar, 'enabled', 'disabled')}")
    logger::log_info("      📏 File size threshold: {file_size_threshold_mb} MB")
    logger::log_info("   🔍 Validating directory and external drive...")
  }
  
  # Validate directory existence
  if (!fs::dir_exists(search_results_directory)) {
    stop(glue::glue("Directory does not exist: {search_results_directory}"))
  }
  
  if (enable_verbose_logging) {
    logger::log_info("   ✅ Search directory validated: {search_results_directory}")
  }
  
  # Set up external drive path if provided
  if (!is.null(external_drive_path)) {
    if (!fs::dir_exists(external_drive_path)) {
      if (enable_verbose_logging) {
        logger::log_warn("⚠️  External drive path does not exist: {external_drive_path}")
        logger::log_info("   🔄 Using local directory instead")
      }
      output_directory <- search_results_directory
    } else {
      output_directory <- external_drive_path
      if (enable_verbose_logging) {
        logger::log_info("   ✅ External drive validated: {external_drive_path}")
      }
    }
  } else {
    output_directory <- search_results_directory
  }
  
  if (enable_verbose_logging) {
    logger::log_info("🧠 Memory management settings:")
    logger::log_info("   📊 Keep only summary: {keep_only_summary}")
    logger::log_info("   🔄 Chunk processing: {chunk_processing}")
    if (chunk_processing) {
      logger::log_info("   📦 Chunk size: {formatC(max_chunk_size, big.mark = ',', format = 'd')} rows")
    }
  }
  
  # Process all CSV files with optimizations
  file_processing_results <- process_healthcare_data_files_optimized(
    directory_path = search_results_directory,
    file_pattern = extraction_file_pattern,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    chunk_processing = chunk_processing,
    max_chunk_size = max_chunk_size,
    search_subdirectories = search_subdirectories,
    exclude_patterns = exclude_patterns,
    parallel_cores = parallel_cores,
    use_data_table = use_data_table,
    file_size_threshold_mb = file_size_threshold_mb,
    progress_bar = progress_bar,
    use_duckplyr = use_duckplyr
  )
  
  smart_analysis_results <- perform_smart_gap_analysis(
    combined_provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    use_duckplyr = use_duckplyr
  )
  
  # Generate geographic patterns if location data available (but keep it lightweight)
  geographic_analysis <- analyze_geographic_distribution(
    provider_data = file_processing_results$combined_provider_data,
    verbose_logging = enable_verbose_logging,
    keep_only_summary = keep_only_summary,
    use_duckplyr = use_duckplyr
  )
  
  # Clear large data from memory if space-saving mode
  if (keep_only_summary) {
    file_processing_results$combined_provider_data <- NULL
    gc() # Force garbage collection
    if (enable_verbose_logging) {
      logger::log_info("🧹 Cleared large datasets from memory to save space")
    }
  }
  
  # Generate next search recommendations
  next_search_ids <- generate_intelligent_search_list(
    analysis_results = smart_analysis_results,
    search_strategy = search_strategy,
    batch_size = batch_size_for_search,
    verbose_logging = enable_verbose_logging
  )
  
  # Save exclusion lists if requested (to external drive if specified)
  if (save_exclusion_lists) {
    save_provider_exclusion_lists(
      non_existent_provider_ids = smart_analysis_results$non_existent_provider_ids,
      truly_missing_provider_ids = smart_analysis_results$truly_missing_provider_ids,
      output_directory = output_directory,
      verbose_logging = enable_verbose_logging,
      compress_files = compress_output
    )
  }
  
  # Compile final results (keep lightweight if space-saving)
  final_analysis_results <- list(
    file_summary = file_processing_results$file_summary,
    smart_analysis = smart_analysis_results,
    geographic_patterns = geographic_analysis,
    next_search_recommendations = next_search_ids,
    analysis_timestamp = lubridate::now(),
    analysis_parameters = list(
      directory = search_results_directory,
      file_pattern = extraction_file_pattern,
      batch_size = batch_size_for_search,
      strategy = search_strategy,
      external_drive_used = !is.null(external_drive_path),
      space_saving_mode = keep_only_summary,
      parallel_cores_used = parallel_cores,
      data_table_used = use_data_table,
      performance_optimized = TRUE
    )
  )
  
  # Save final results to external drive if specified
  if (!is.null(external_drive_path)) {
    timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
    results_filename <- glue::glue("healthcare_analysis_results_{timestamp_suffix}")
    
    if (compress_output) {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds.gz"))
      saveRDS(final_analysis_results, file = results_file_path, compress = "gzip")
    } else {
      results_file_path <- fs::path(output_directory, glue::glue("{results_filename}.rds"))
      saveRDS(final_analysis_results, file = results_file_path)
    }
    
    if (enable_verbose_logging) {
      logger::log_info("💾 Results saved to external drive: {results_file_path}")
    }
  }
  
  if (enable_verbose_logging) {
    logger::log_info("✅ Healthcare access analysis completed successfully")
    logger::log_info("   📋 Files processed: {length(file_processing_results$file_summary$files_found)}")
    if (!is.null(file_processing_results$combined_provider_data)) {
      logger::log_info("   🆔 Total provider IDs analyzed: {formatC(length(unique(file_processing_results$combined_provider_data$provider_id)), big.mark = ',', format = 'd')}")
    }
    logger::log_info("   🎯 Next search recommendations: {formatC(length(next_search_ids), big.mark = ',', format = 'd')} IDs")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Performance: Used {parallel_cores} cores with {ifelse(use_data_table, 'data.table', 'base R')} methods")
    }
  }
  
  return(final_analysis_results)
}

#' @noRd
process_healthcare_data_files_optimized <- function(directory_path, file_pattern, verbose_logging, 
                                                    keep_only_summary = FALSE, chunk_processing = FALSE, 
                                                    max_chunk_size = 100000, search_subdirectories = FALSE,
                                                    exclude_patterns = NULL, parallel_cores = 1,
                                                    use_data_table = TRUE, file_size_threshold_mb = 50,
                                                    progress_bar = TRUE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("📂 Processing healthcare data files with optimizations...")
    logger::log_info("   🔍 Searching for files with pattern: {file_pattern}")
    logger::log_info("   📁 Recursive search: {search_subdirectories}")
  }
  
  # Get list of matching files
  if (verbose_logging) {
    logger::log_info("   🔍 Starting file discovery...")
  }
  
  csv_files_list <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = search_subdirectories
  )
  
  if (verbose_logging) {
    logger::log_info("   📋 Initial file discovery found: {length(csv_files_list)} files")
    if (length(csv_files_list) > 0 && length(csv_files_list) <= 10) {
      logger::log_info("   📄 Files found: {paste(basename(csv_files_list), collapse = ', ')}")
    } else if (length(csv_files_list) > 10) {
      logger::log_info("   📄 First 5 files: {paste(basename(csv_files_list[1:5]), collapse = ', ')}")
    }
  }
  
  # Filter out excluded patterns
  if (!is.null(exclude_patterns) && length(exclude_patterns) > 0) {
    original_count <- length(csv_files_list)
    
    if (verbose_logging) {
      logger::log_info("   🚫 Applying exclusion patterns...")
    }
    
    for (exclude_pattern in exclude_patterns) {
      csv_files_list <- csv_files_list[!stringr::str_detect(basename(csv_files_list), exclude_pattern)]
    }
    
    excluded_count <- original_count - length(csv_files_list)
    
    if (verbose_logging && excluded_count > 0) {
      logger::log_info("   🚫 Excluded {excluded_count} files matching exclusion patterns")
      logger::log_info("   📋 Remaining files after exclusions: {length(csv_files_list)}")
    }
  }
  
  if (length(csv_files_list) == 0) {
    if (verbose_logging) {
      logger::log_error("❌ No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions")
      logger::log_info("   💡 Check your file pattern and exclusion patterns")
      logger::log_info("   🔍 Recursive search was: {search_subdirectories}")
    }
    stop(glue::glue("No files matching pattern '{file_pattern}' found in {directory_path} after applying exclusions"))
  }
  
  if (verbose_logging) {
    if (search_subdirectories) {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (including subdirectories, after exclusions)")
    } else {
      logger::log_info("   📁 Found {length(csv_files_list)} matching files (top-level only, after exclusions)")
    }
  }
  
  # Analyze file sizes for optimization strategy
  file_info_data <- file.info(csv_files_list)
  file_sizes_mb <- file_info_data$size / (1024^2)
  large_files_count <- sum(file_sizes_mb > file_size_threshold_mb, na.rm = TRUE)
  
  if (verbose_logging) {
    logger::log_info("   📊 File size analysis:")
    logger::log_info("      📏 Average file size: {round(mean(file_sizes_mb, na.rm = TRUE), 1)} MB")
    logger::log_info("      📈 Largest file: {round(max(file_sizes_mb, na.rm = TRUE), 1)} MB") 
    logger::log_info("      🔥 Files > {file_size_threshold_mb}MB: {large_files_count}")
    if (parallel_cores > 1) {
      logger::log_info("      ⚡ Using {parallel_cores} cores for parallel processing")
    }
  }
  
  # Set up progress bar
  if (progress_bar && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing files [:bar] :percent :current/:total ETA: :eta",
      total = length(csv_files_list),
      clear = FALSE,
      width = 80
    )
    show_progress <- TRUE
  } else {
    show_progress <- FALSE
    if (progress_bar && verbose_logging) {
      logger::log_warn("⚠️  progress package not available, disabling progress bar")
    }
  }
  
  # Optimized file reading function
  optimized_read_csv <- function(file_path, file_index = NULL) {
    if (show_progress) {
      pb$tick()
    }
    
    tryCatch({
      # Check file size for optimization strategy
      file_size_mb <- file.info(file_path)$size / (1024^2)
      
      if (use_data_table && file_size_mb > file_size_threshold_mb) {
        # Use data.table::fread for large files (much faster)
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          nThread = 1,  # Control threading at file level
          stringsAsFactors = FALSE
        )
        # Convert to tibble if needed for consistency
        file_data <- dplyr::as_tibble(file_data)
      } else if (use_data_table) {
        # Use data.table for smaller files too for consistency  
        file_data <- data.table::fread(
          file_path,
          showProgress = FALSE,
          verbose = FALSE,
          stringsAsFactors = FALSE
        )
        file_data <- dplyr::as_tibble(file_data)
      } else {
        # Fallback to readr with optimized settings
        file_data <- readr::read_csv(
          file_path, 
          show_col_types = FALSE,
          lazy = FALSE,  # Read immediately for better memory management
          col_types = readr::cols(.default = readr::col_character()),
          locale = readr::locale(encoding = "UTF-8")
        )
      }
      
      # Standardize and filter early to save memory
      file_data <- standardize_provider_data_columns_optimized(file_data, file_path, FALSE, use_duckplyr)
      
      if (keep_only_summary && "provider_id" %in% colnames(file_data)) {
        file_data <- apply_summary_filtering(file_data, FALSE, use_duckplyr)  # Don't log for each file
      }
      
      return(file_data)
      
    }, error = function(e) {
      if (verbose_logging) {
        logger::log_warn("   ⚠️  Failed to read {basename(file_path)}: {e$message}")
      }
      return(NULL)
    })
  }
  
  # Process files - use parallel processing if beneficial
  if (parallel_cores > 1 && length(csv_files_list) > 10) {
    if (verbose_logging) {
      logger::log_info("   ⚡ Using parallel processing with {parallel_cores} cores")
    }
    
    # Set up cluster
    cl <- parallel::makeCluster(parallel_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary objects to cluster
    parallel::clusterEvalQ(cl, {
      library(dplyr)
      library(readr)
      library(stringr)
      library(glue)
      library(lubridate)
      if (requireNamespace("data.table", quietly = TRUE)) {
        library(data.table)
      }
      if (requireNamespace("duckplyr", quietly = TRUE)) {
        library(duckplyr)
      }
    })
    
    parallel::clusterExport(cl, c(
      "use_data_table", "file_size_threshold_mb", "keep_only_summary", 
      "verbose_logging", "standardize_provider_data_columns_optimized",
      "apply_summary_filtering", "use_duckplyr"
    ), envir = environment())
    
    # Process files in parallel
    file_data_list <- parallel::parLapply(cl, csv_files_list, optimized_read_csv)
    
  } else {
    # Sequential processing for smaller file sets or single core
    file_data_list <- lapply(csv_files_list, optimized_read_csv)
  }
  
  # Remove NULL entries (failed reads)
  file_data_list <- file_data_list[!sapply(file_data_list, is.null)]
  
  if (length(file_data_list) == 0) {
    stop("No files could be successfully read")
  }
  
  # Store the count for file_summary before cleaning up
  successful_files_count <- length(file_data_list)
  
  if (verbose_logging) {
    failed_files <- length(csv_files_list) - successful_files_count
    logger::log_info("   ✅ Successfully read {successful_files_count} files")
    if (failed_files > 0) {
      logger::log_warn("   ❌ Failed to read {failed_files} files")
    }
  }
  
  # Combine all data efficiently
  if (verbose_logging) {
    logger::log_info("   🔄 Combining data from all files...")
  }
  
  if (use_data_table && requireNamespace("data.table", quietly = TRUE)) {
    # Use data.table::rbindlist for fastest binding
    combined_provider_data <- data.table::rbindlist(file_data_list, fill = TRUE, use.names = TRUE)
    combined_provider_data <- dplyr::as_tibble(combined_provider_data)
  } else {
    # Use dplyr::bind_rows as fallback
    combined_provider_data <- dplyr::bind_rows(file_data_list)
  }
  
  # Clean up memory
  rm(file_data_list)
  gc()
  
  file_summary <- list(
    directory_analyzed = directory_path,
    files_found = basename(csv_files_list),
    total_files_processed = length(csv_files_list),
    successful_files = successful_files_count,
    total_provider_records = nrow(combined_provider_data),
    space_saving_mode = keep_only_summary,
    chunk_processing_used = chunk_processing,
    parallel_cores_used = parallel_cores,
    data_table_used = use_data_table,
    average_file_size_mb = round(mean(file_sizes_mb, na.rm = TRUE), 1),
    large_files_count = large_files_count
  )
  
  if (verbose_logging) {
    logger::log_info("✅ File processing completed")
    logger::log_info("   📊 Total records: {formatC(nrow(combined_provider_data), big.mark = ',', format = 'd')}")
    final_memory <- utils::object.size(combined_provider_data) / (1024^2)
    logger::log_info("   💾 Final data size: {round(final_memory, 1)} MB")
    if (parallel_cores > 1) {
      logger::log_info("   ⚡ Parallel processing completed successfully")
    }
  }
  
  return(list(
    combined_provider_data = combined_provider_data,
    file_summary = file_summary
  ))
}

#' @noRd
standardize_provider_data_columns_optimized <- function(file_data, file_path, verbose_logging, use_duckplyr = FALSE) {
  
  # Handle duplicate column names by making them unique first
  if (any(duplicated(names(file_data)))) {
    if (verbose_logging) {
      logger::log_warn("   ⚠️  Duplicate column names detected in {basename(file_path)}, making unique")
    }
    names(file_data) <- make.unique(names(file_data), sep = "_dup_")
  }
  
  # Convert ALL columns to character to avoid any type mismatch issues
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    file_data <- file_data %>%
      duckplyr::mutate(duckplyr::across(duckplyr::everything(), as.character))
  } else if (requireNamespace("data.table", quietly = TRUE) && data.table::is.data.table(file_data)) {
    # data.table approach - faster for large data
    char_cols <- names(file_data)
    file_data[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    file_data <- dplyr::as_tibble(file_data)
  } else {
    # dplyr approach for smaller data
    file_data <- file_data %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }
  
  # Handle common column name variations for provider_id (vectorized approach)
  col_names <- colnames(file_data)
  
  if (!"provider_id" %in% col_names) {
    # Check for alternative ID columns in order of preference - expanded list for subspecialty files
    id_alternatives <- c(
      "physician_id", "npi", "physician_ids", "id", "provider_npi", "doctor_id",
      "subspecialty_id", "specialist_id", "physician_number", "provider_number",
      "doc_id", "dr_id", "medical_id", "practitioner_id"
    )
    available_alternatives <- intersect(id_alternatives, col_names)
    
    if (length(available_alternatives) > 0) {
      # Use the first available alternative
      file_data$provider_id <- file_data[[available_alternatives[1]]]
      if (verbose_logging) {
        logger::log_info("   🔄 Using '{available_alternatives[1]}' as provider_id for {basename(file_path)}")
      }
    } else {
      # Log what columns are available to help debug
      if (verbose_logging) {
        logger::log_warn("   ⚠️  No standard ID column found in {basename(file_path)}")
        logger::log_info("   🔍 Available columns: {paste(head(col_names, 10), collapse = ', ')}")
      }
    }
  }
  
  # Add file source for tracking (already character) - optimized
  file_data$source_file <- basename(file_path)
  file_data$processing_timestamp <- as.character(lubridate::now())
  
  return(file_data)
}

#' @noRd
apply_summary_filtering <- function(provider_data, verbose_logging, use_duckplyr = FALSE) {
  if (!"provider_id" %in% colnames(provider_data)) {
    return(provider_data)
  }
  
  essential_columns <- c("provider_id", "source_file")
  geographic_columns <- c("city", "state", "county", "zip_code")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  keep_columns <- c(essential_columns, available_geo_columns[1:min(2, length(available_geo_columns))])
  keep_columns <- intersect(keep_columns, colnames(provider_data))
  
  if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
    filtered_data <- provider_data %>%
      duckplyr::select(all_of(keep_columns))
  } else {
    filtered_data <- provider_data %>%
      dplyr::select(all_of(keep_columns))
  }
  
  if (verbose_logging) {
    logger::log_info("   🧹 Space-saving: kept only {length(keep_columns)} essential columns")
  }
  
  return(filtered_data)
}

#' @noRd
perform_smart_gap_analysis <- function(combined_provider_data, verbose_logging, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🧠 Performing smart gap analysis...")
  }
  
  # Check if provider_id column exists
  if (!"provider_id" %in% colnames(combined_provider_data)) {
    if (verbose_logging) {
      logger::log_warn("⚠️  'provider_id' column not found in data")
      logger::log_info("   🔍 Available columns: {paste(colnames(combined_provider_data), collapse = ', ')}")
      
      # Try to find alternative ID columns
      potential_id_columns <- c("physician_id", "npi", "id", "physician_ids", "provider_npi", "doctor_id")
      available_id_columns <- intersect(potential_id_columns, colnames(combined_provider_data))
      
      if (length(available_id_columns) > 0) {
        logger::log_info("   💡 Found potential ID columns: {paste(available_id_columns, collapse = ', ')}")
        logger::log_info("   🔄 Attempting to use '{available_id_columns[1]}' as provider_id")
        
        # Use the first available alternative
        combined_provider_data$provider_id <- combined_provider_data[[available_id_columns[1]]]
      } else {
        logger::log_error("❌ No valid ID columns found for gap analysis")
        return(list(
          provider_id_range = c(NA, NA),
          total_unique_ids = 0,
          gaps_identified = 0,
          non_existent_provider_ids = numeric(0),
          truly_missing_provider_ids = numeric(0),
          existence_rate = 0,
          gap_analysis_summary = "No valid provider ID column found in data",
          error = "missing_provider_id_column"
        ))
      }
    }
  }
  
  # Use dplyr for all operations (duckplyr has limited support for complex operations)
  all_provider_ids <- combined_provider_data %>%
    dplyr::filter(!is.na(provider_id) & provider_id != "" & provider_id != "NA") %>%
    dplyr::pull(provider_id) %>%
    unique() %>%
    as.numeric() %>%
    .[!is.na(.)] %>%
    sort()
  
  if (length(all_provider_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  No valid numeric provider IDs found in data")
      logger::log_info("   🔍 Sample provider_id values: {paste(head(unique(combined_provider_data$provider_id), 10), collapse = ', ')}")
    }
    return(list(
      provider_id_range = c(NA, NA),
      total_unique_ids = 0,
      gaps_identified = 0,
      non_existent_provider_ids = numeric(0),
      truly_missing_provider_ids = numeric(0),
      existence_rate = 0,
      gap_analysis_summary = "No valid numeric provider IDs found - check data format",
      error = "no_numeric_ids"
    ))
  }
  
  # Calculate basic statistics
  min_id <- min(all_provider_ids)
  max_id <- max(all_provider_ids)
  total_unique_ids <- length(all_provider_ids)
  full_range_size <- max_id - min_id + 1
  
  # Find gaps in the sequence
  complete_sequence <- seq(min_id, max_id)
  missing_ids <- setdiff(complete_sequence, all_provider_ids)
  gaps_identified <- length(missing_ids)
  
  # Calculate existence rate
  existence_rate <- (total_unique_ids / full_range_size) * 100
  
  if (verbose_logging) {
    logger::log_info("   📊 Provider ID analysis:")
    logger::log_info("      🆔 Range: {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}")
    logger::log_info("      ✅ Found IDs: {formatC(total_unique_ids, big.mark = ',')}")
    logger::log_info("      ❌ Missing IDs: {formatC(gaps_identified, big.mark = ',')}")
    logger::log_info("      📈 Existence rate: {round(existence_rate, 1)}%")
  }
  
  # Identify non-existent IDs from failed searches - use dplyr for all operations
  non_existent_ids <- combined_provider_data %>%
    dplyr::filter(stringr::str_detect(tolower(source_file), "non_existent|failed")) %>%
    dplyr::pull(provider_id) %>%
    as.numeric() %>%
    unique() %>%
    .[!is.na(.)]
  
  # Truly missing = missing but not known to be non-existent
  truly_missing_ids <- setdiff(missing_ids, non_existent_ids)
  
  # Sample large lists to prevent memory issues
  if (length(non_existent_ids) > 10000) {
    non_existent_ids <- sample(non_existent_ids, 10000)
  }
  if (length(truly_missing_ids) > 100000) {
    truly_missing_ids <- sample(truly_missing_ids, 100000)
  }
  
  gap_analysis_summary <- glue::glue(
    "Found {formatC(total_unique_ids, big.mark = ',')} unique provider IDs in range {formatC(min_id, big.mark = ',')} to {formatC(max_id, big.mark = ',')}. ",
    "Identified {formatC(gaps_identified, big.mark = ',')} gaps ({round(100 - existence_rate, 1)}% missing). ",
    "Existence rate: {round(existence_rate, 1)}%."
  )
  
  if (verbose_logging) {
    logger::log_info("✅ Gap analysis completed")
  }
  
  return(list(
    min_id_searched = min_id,
    max_id_searched = max_id,
    provider_id_range = c(min_id, max_id),
    total_unique_ids = total_unique_ids,
    gaps_identified = gaps_identified,
    non_existent_provider_ids = non_existent_ids,
    truly_missing_provider_ids = truly_missing_ids,
    existence_rate = existence_rate,
    gap_analysis_summary = gap_analysis_summary,
    full_range_size = full_range_size
  ))
}

#' @noRd
analyze_geographic_distribution <- function(provider_data, verbose_logging, keep_only_summary = FALSE, use_duckplyr = FALSE) {
  
  if (verbose_logging) {
    logger::log_info("🗺️  Analyzing geographic distribution patterns...")
  }
  
  # Check for geographic data columns
  geographic_columns <- c("state", "county", "zip_code", "city", "location")
  available_geo_columns <- intersect(names(provider_data), geographic_columns)
  
  if (length(available_geo_columns) == 0) {
    if (verbose_logging) {
      logger::log_info("   ℹ️  No geographic columns found for distribution analysis")
    }
    return(list(
      geographic_analysis_available = FALSE,
      geographic_summary = "No geographic data available",
      state_distribution = NULL,
      city_distribution = NULL
    ))
  }
  
  geographic_analysis <- list()
  
  # State-level analysis if available
  if ("state" %in% available_geo_columns) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      state_dist <- provider_data %>%
        duckplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        duckplyr::group_by(state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count))
    } else {
      state_dist <- provider_data %>%
        dplyr::filter(!is.na(state) & state != "" & state != "NA") %>%
        dplyr::group_by(state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count))
    }
    
    geographic_analysis$state_distribution <- state_dist
    
    if (verbose_logging) {
      logger::log_info("   🏛️  State analysis: {nrow(state_dist)} states with providers")
    }
  }
  
  # City-level analysis if available  
  if ("city" %in% available_geo_columns && !keep_only_summary) {
    if (use_duckplyr && requireNamespace("duckplyr", quietly = TRUE)) {
      city_dist <- provider_data %>%
        duckplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        duckplyr::group_by(city, state) %>%
        duckplyr::summarise(
          provider_count = duckplyr::n(),
          unique_providers = duckplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        duckplyr::arrange(duckplyr::desc(provider_count)) %>%
        duckplyr::slice_head(n = 100)  # Top 100 cities only
    } else {
      city_dist <- provider_data %>%
        dplyr::filter(!is.na(city) & city != "" & city != "NA") %>%
        dplyr::group_by(city, state) %>%
        dplyr::summarise(
          provider_count = dplyr::n(),
          unique_providers = dplyr::n_distinct(provider_id, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(dplyr::desc(provider_count)) %>%
        dplyr::slice_head(n = 100)  # Top 100 cities only
    }
    
    geographic_analysis$city_distribution <- city_dist
    
    if (verbose_logging) {
      logger::log_info("   🏙️  City analysis: {nrow(city_dist)} cities analyzed (top 100)")
    }
  }
  
  # Generate summary
  total_with_geo <- provider_data %>%
    dplyr::filter(dplyr::if_any(dplyr::all_of(available_geo_columns), ~ !is.na(.) & . != "" & . != "NA")) %>%
    nrow()
  
  geo_coverage_rate <- (total_with_geo / nrow(provider_data)) * 100
  
  geographic_summary <- glue::glue(
    "Geographic data available for {formatC(total_with_geo, big.mark = ',')} of {formatC(nrow(provider_data), big.mark = ',')} records ({round(geo_coverage_rate, 1)}%). ",
    "Available columns: {paste(available_geo_columns, collapse = ', ')}."
  )
  
  geographic_analysis$geographic_analysis_available <- TRUE
  geographic_analysis$geographic_summary <- geographic_summary
  geographic_analysis$coverage_rate <- geo_coverage_rate
  geographic_analysis$available_columns <- available_geo_columns
  
  if (verbose_logging) {
    logger::log_info("✅ Geographic analysis completed")
    logger::log_info("   📍 Coverage: {round(geo_coverage_rate, 1)}% of records have geographic data")
  }
  
  return(geographic_analysis)
}

#' @noRd
generate_intelligent_search_list <- function(analysis_results, search_strategy, batch_size, verbose_logging) {
  
  if (verbose_logging) {
    logger::log_info("🎯 Generating intelligent search recommendations...")
    logger::log_info("   📊 Strategy: {search_strategy}")
    logger::log_info("   📦 Batch size: {formatC(batch_size, big.mark = ',')}")
  }
  
  # Extract key information from analysis
  min_id <- analysis_results$provider_id_range[1]
  max_id <- analysis_results$provider_id_range[2]
  missing_ids <- analysis_results$truly_missing_provider_ids
  
  if (is.na(min_id) || is.na(max_id) || length(missing_ids) == 0) {
    if (verbose_logging) {
      logger::log_warn("⚠️  Insufficient data for search recommendations")
    }
    return(numeric(0))
  }
  
  search_candidates <- numeric(0)
  
  # Generate search candidates based on strategy
  if (stringr::str_detect(search_strategy, "gaps")) {
    # Add missing IDs from gaps
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Sort missing IDs from highest to lowest
      gap_candidates <- sort(missing_ids, decreasing = TRUE)
    } else {
      # Smart gaps - mix of random and systematic
      gap_candidates <- sample(missing_ids, min(length(missing_ids), batch_size * 2))
    }
    search_candidates <- c(search_candidates, gap_candidates)
  }
  
  if (stringr::str_detect(search_strategy, "extend")) {
    # Add IDs beyond current range
    extension_size <- min(batch_size, 10000)  # Don't extend too far
    
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      # Extend upward from max
      extend_candidates <- seq(max_id + 1, max_id + extension_size)
    } else {
      # Smart extend - both directions
      extend_up <- seq(max_id + 1, max_id + ceiling(extension_size/2))
      extend_down <- seq(max(1, min_id - floor(extension_size/2)), min_id - 1)
      extend_candidates <- c(extend_up, extend_down)
    }
    search_candidates <- c(search_candidates, extend_candidates)
  }
  
  # Remove duplicates and limit to batch size
  search_candidates <- unique(search_candidates)
  search_candidates <- search_candidates[search_candidates > 0]  # Remove negative IDs
  
  # Limit to batch size
  if (length(search_candidates) > batch_size) {
    if (stringr::str_detect(search_strategy, "high_to_low")) {
      search_candidates <- sort(search_candidates, decreasing = TRUE)[1:batch_size]
    } else {
      search_candidates <- sample(search_candidates, batch_size)
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Search recommendations generated")
    logger::log_info("   🆔 Recommended IDs: {formatC(length(search_candidates), big.mark = ',')}")
    if (length(search_candidates) > 0) {
      logger::log_info("   📈 Range: {formatC(min(search_candidates), big.mark = ',')} to {formatC(max(search_candidates), big.mark = ',')}")
      
      # Show strategy-specific sample
      if (stringr::str_detect(search_strategy, "high_to_low")) {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from highest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from highest: {first_few} ... ending with: {last_few}")
        }
      } else {
        if (length(search_candidates) <= 10) {
          logger::log_info("   🆔 Starting from lowest: {paste(search_candidates, collapse = ', ')}")
        } else {
          first_few <- paste(head(search_candidates, 5), collapse = ", ")
          last_few <- paste(tail(search_candidates, 5), collapse = ", ")
          logger::log_info("   🆔 Starting from lowest: {first_few} ... ending with: {last_few}")
        }
      }
    }
  }
  
  return(search_candidates)
}

#' @noRd
save_provider_exclusion_lists <- function(non_existent_provider_ids, truly_missing_provider_ids, 
                                          output_directory, verbose_logging, compress_files = TRUE) {
  
  if (verbose_logging) {
    logger::log_info("💾 Saving provider exclusion lists...")
  }
  
  timestamp_suffix <- format(lubridate::now(), "%Y%m%d_%H%M%S")
  
  # Save non-existent IDs
  if (length(non_existent_provider_ids) > 0) {
    non_existent_df <- data.frame(
      provider_id = non_existent_provider_ids,
      exclusion_reason = "non_existent",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("non_existent_provider_ids_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(non_existent_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath}.gz ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Non-existent IDs: {filepath} ({formatC(length(non_existent_provider_ids), big.mark = ',')} IDs)")
      }
    }
  }
  
  # Save truly missing IDs  
  if (length(truly_missing_provider_ids) > 0) {
    # If there are more than 1 million missing IDs, save only a sample to save space
    if (length(truly_missing_provider_ids) > 1000000) {
      sample_size <- 100000
      sampled_missing_ids <- sample(truly_missing_provider_ids, sample_size)
      
      if (verbose_logging) {
        logger::log_info("   🧹 Space-saving: sampling {formatC(sample_size, big.mark = ',')} missing IDs from {formatC(length(truly_missing_provider_ids), big.mark = ',')} total")
      }
      
      ids_to_save <- sampled_missing_ids
      file_suffix <- "_sampled"
    } else {
      ids_to_save <- truly_missing_provider_ids
      file_suffix <- ""
    }
    
    missing_df <- data.frame(
      provider_id = ids_to_save,
      exclusion_reason = "missing_from_search",
      timestamp = lubridate::now()
    )
    
    filename <- glue::glue("missing_provider_ids{file_suffix}_{timestamp_suffix}.csv")
    filepath <- fs::path(output_directory, filename)
    
    readr::write_csv(missing_df, filepath)
    
    if (compress_files) {
      R.utils::gzip(filepath, overwrite = TRUE)
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath}.gz ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    } else {
      if (verbose_logging) {
        logger::log_info("   📁 Missing IDs: {filepath} ({formatC(length(ids_to_save), big.mark = ',')} IDs)")
      }
    }
  }
  
  if (verbose_logging) {
    logger::log_info("✅ Exclusion lists saved successfully")
  }
}

# Run ----
# This should work perfectly now
healthcare_access_analysis_fixed <- analyze_healthcare_access_patterns(
  search_results_directory = "~/Dropbox (Personal)/isochrones/physician_data",
  extraction_file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$)",
  enable_verbose_logging = TRUE,
  save_exclusion_lists = TRUE,
  batch_size_for_search = 5000,
  search_strategy = "high_to_low_mixed",
  external_drive_path = "/Volumes/MufflyNew",
  compress_output = TRUE,
  keep_only_summary = FALSE,
  chunk_processing = FALSE,
  search_subdirectories = TRUE,
  exclude_patterns = c(
    "sequential_discovery_.*\\.csv$",
    "wrong_ids_.*\\.csv$",
    "temp_.*\\.csv$",
    "backup_.*\\.csv$",
    "truly_missing_.*\\.csv$",
    "non_existent_provider_.*\\.csv$",
    "extraction_summary_.*\\.csv$",
    "valid_physician_ids_.*\\.csv$"
  ),
  parallel_cores = 1,
  use_data_table = TRUE,
  file_size_threshold_mb = 25,
  progress_bar = TRUE
)

# Left textbook in the park after soccer practice but thank god it was there the next day ----
#' Extract ABOG Provider Data from Files

abog_provider_dataframe <- extract_abog_provider_data(
  search_directory = "~/Dropbox (Personal)/isochrones/physician_data",
  file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$)"
) %>%
  dplyr::mutate(subspecialty_name = dplyr::recode(subspecialty_name, 
                                                  "CFP" = "Complex Family Planning", 
                                                  "FPM" = "Female Pelvic Medicine & Reconstructive Surgery", 
                                                  "Urogynecology and Reconstructive Pelvic Surgery" = "Female Pelvic Medicine & Reconstructive Surgery", 
                                                  "REI" = "Reproductive Endocrinology and Infertility", 
                                                  "PAG" = "Pediatric & Adolescent Gynecology", 
                                                  "MFM" = "Maternal-Fetal Medicine", 
                                                  "ONC" = "Gynecologic Oncology", 
                                                  "HPM" = "Hospice and Palliative Medicine")) %>%
  # Only process existing ID columns
  {
    existing_id_columns <- intersect(c("abog_id", "abog_id_number", "ID", "userid"), colnames(.))
    if (length(existing_id_columns) > 0) {
      dplyr::mutate(., dplyr::across(dplyr::all_of(existing_id_columns), readr::parse_number))
    } else {
      .
    }
  } %>%
  # Coalesce ID columns if they exist
  {
    existing_id_columns <- intersect(c("abog_id", "abog_id_number", "ID", "userid"), colnames(.))
    if (length(existing_id_columns) > 1) {
      dplyr::mutate(., abog_id = dplyr::coalesce(!!!dplyr::syms(existing_id_columns)))
    } else {
      .
    }
  } %>%
  # Remove extra ID columns if they exist
  {
    columns_to_remove <- intersect(c("abog_id_number", "ID", "userid"), colnames(.))
    if (length(columns_to_remove) > 0) {
      dplyr::select(., -dplyr::all_of(columns_to_remove))
    } else {
      .
    }
  } %>%
  dplyr::filter(!is.na(abog_id) & !is.na(physician_name)) %>%
  # PRIORITIZE rows with subspecialty data before taking distinct
  dplyr::arrange(abog_id, 
                 dplyr::desc(!is.na(subspecialty_name) & subspecialty_name != "NA" & subspecialty_name != "")) %>%
  dplyr::distinct(abog_id, .keep_all = TRUE) %>%
  dplyr::arrange(abog_id)


#run extraction ----
# Extract your ABOG provider data
abog_provider_dataframe <- extract_abog_provider_data(
  search_directory = "~/Dropbox (Personal)/isochrones/physician_data",
  file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$)"
) %>%
  mutate(subspecialty_name = recode(subspecialty_name, "CFP" = "Complex Family Planning", "FPM" = "Female Pelvic Medicine & Reconstructive Surgery", "Urogynecology and Reconstructive Pelvic Surgery" = "Female Pelvic Medicine & Reconstructive Surgery", "REI" = "Reproductive Endocrinology and Infertility", "PAG" = "Pediatric & Adolescent Gynecology", "MFM" = "Maternal-Fetal Medicine", "ONC" = "Gynecologic Oncology", "HPM" = "Hospice and Palliative Medicine", type_convert = TRUE)) %>%
  mutate(across(c(abog_id, abog_id_number, ID, userid), parse_number)) %>%
  mutate(abog_id = coalesce(abog_id, abog_id_number, ID, userid)) %>%
  select(-abog_id_number, -ID, -userid) %>%
  filter(!is.na(abog_id) & !is.na(physician_name)) %>%
  distinct(abog_id, .keep_all = TRUE) %>%
  dplyr::arrange(abog_id)


     #                    Complex Family Planning Female Pelvic Medicine & Reconstructive Surgery 
     #                                        333                                             914 
     #                       Gynecologic Oncology                 Hospice and Palliative Medicine 
     #                                       1278                                              14 
     #                    Maternal-Fetal Medicine               Pediatric & Adolescent Gynecology 
     #                                       2430                                              34 
     # Reproductive Endocrinology and Infertility 
     #                                       1330 

# View the structure
dim(abog_provider_dataframe)
table(abog_provider_dataframe$subspecialty_name)
str(abog_provider_dataframe)
head(abog_provider_dataframe)
View(abog_provider_dataframe)

# Save to external drive
readr::write_csv(abog_provider_dataframe, "data/0-Download/output/complete_abog_provider_data.csv")





# This finally got it by using more directories with previously scraped data ----
#' Extract ABOG Provider Data from Files
#' 
#' Reads your extraction files and returns a clean dataframe with all ABOG providers
extract_abog_provider_data <- function(search_directory, file_pattern, verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting ABOG provider data extraction")
    logger::log_info("Search directories: {paste(search_directory, collapse = ', ')}")
    logger::log_info("File pattern: {file_pattern}")
  }
  
  # Get all extraction files from all directories
  extraction_files <- c()
  
  for (directory in search_directory) {
    if (dir.exists(directory)) {
      files_in_dir <- list.files(
        path = directory,
        pattern = file_pattern,
        full.names = TRUE,
        recursive = TRUE
      )
      extraction_files <- c(extraction_files, files_in_dir)
      if (verbose) {
        logger::log_info("Found {length(files_in_dir)} files in {directory}")
      }
    } else {
      if (verbose) {
        logger::log_warn("Directory does not exist: {directory}")
      }
    }
  }
  
  if (verbose) {
    logger::log_info("Total files found: {length(extraction_files)}")
  }
  
  # Read and combine all files
  all_provider_data <- data.frame()
  
  for(i in seq_along(extraction_files)) {
    if(i %% 20 == 0 && verbose) {
      logger::log_info("Processing file {i} of {length(extraction_files)}")
    }
    
    file_data <- readr::read_csv(extraction_files[i], show_col_types = FALSE)
    
    # Standardize column names
    file_data <- standardize_provider_column_names(file_data, verbose = verbose)
    
    # Add source file for tracking
    file_data$source_file <- basename(extraction_files[i])
    
    # Combine data
    if(nrow(all_provider_data) == 0) {
      all_provider_data <- file_data
    } else {
      all_provider_data <- dplyr::bind_rows(all_provider_data, file_data)
    }
  }
  
  if (verbose) {
    logger::log_info("Data extraction completed")
    logger::log_info("Total records: {nrow(all_provider_data)}")
    logger::log_info("Unique ABOG IDs: {length(unique(all_provider_data$abog_id))}")
    logger::log_info("Final columns: {paste(colnames(all_provider_data), collapse = ', ')}")
  }
  
  return(all_provider_data)
}

#' @noRd
standardize_provider_column_names <- function(input_dataframe, verbose = FALSE) {
  
  original_column_names <- colnames(input_dataframe)
  
  # Standardize physician_name variants
  name_column_patterns <- c("name\\.x", "name\\.y", "physician_name", "doctor_name", 
                            "provider_name", "^name$", "full_name", "practitioner_name")
  
  for (pattern in name_column_patterns) {
    matching_columns <- stringr::str_which(tolower(original_column_names), tolower(pattern))
    if (length(matching_columns) > 0) {
      # Use the first match and rename it to physician_name
      target_column <- original_column_names[matching_columns[1]]
      input_dataframe <- input_dataframe %>%
        dplyr::rename(physician_name = !!target_column)
      if (verbose) {
        logger::log_info("Standardized '{target_column}' to 'physician_name'")
      }
      break
    }
  }
  
  # Standardize physician_id/abog_id variants
  id_column_patterns <- c("physician_id", "abog_id", "provider_id", "doctor_id", "id")
  
  if (!"abog_id" %in% colnames(input_dataframe)) {
    for (pattern in id_column_patterns) {
      if (pattern %in% colnames(input_dataframe)) {
        input_dataframe <- input_dataframe %>%
          dplyr::rename(abog_id = !!pattern)
        if (verbose) {
          logger::log_info("Standardized '{pattern}' to 'abog_id'")
        }
        break
      }
    }
  }
  
  # Standardize subspecialty name variants
  subspecialty_column_patterns <- c("subspecialty_name", "sub1", "specialty", "subspecialty")
  
  if (!"subspecialty_name" %in% colnames(input_dataframe)) {
    for (pattern in subspecialty_column_patterns) {
      if (pattern %in% colnames(input_dataframe)) {
        input_dataframe <- input_dataframe %>%
          dplyr::rename(subspecialty_name = !!pattern)
        if (verbose) {
          logger::log_info("Standardized '{pattern}' to 'subspecialty_name'")
        }
        break
      }
    }
  }
  
  # Convert ALL columns to character
  input_dataframe <- input_dataframe %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  
  if (verbose) {
    logger::log_info("Converted all columns to character type")
  }
  
  return(input_dataframe)
}

# run ----
abog_provider_dataframe <- extract_abog_provider_data(
  search_directory = c(
    "~/Dropbox (Personal)/isochrones/physician_data",
    "~/Dropbox (Personal)/workforce/Master_Scraper"
  ),
  file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$|Physicians .*\\.csv$)"
) %>%
  # Filter out deceased providers - only check certStatus and sub1certStatus columns
  dplyr::filter(
    !stringr::str_detect(tolower(coalesce(certStatus, "")), "deceased"),!stringr::str_detect(tolower(coalesce(sub1certStatus, "")), "deceased")
  ) %>%
  dplyr::mutate(
    subspecialty_name = dplyr::recode(
      subspecialty_name,
      "CFP" = "Complex Family Planning",
      "FPM" = "Female Pelvic Medicine & Reconstructive Surgery",
      "Urogynecology and Reconstructive Pelvic Surgery" = "Female Pelvic Medicine & Reconstructive Surgery",
      "REI" = "Reproductive Endocrinology and Infertility",
      "PAG" = "Pediatric & Adolescent Gynecology",
      "MFM" = "Maternal-Fetal Medicine",
      "ONC" = "Gynecologic Oncology",
      "HPM" = "Hospice and Palliative Medicine",
      "URP" = "Female Pelvic Medicine & Reconstructive Surgery"
    )
  ) %>%
  dplyr::filter(!is.na(abog_id) & !is.na(physician_name)) %>%
  # PRIORITIZE rows with subspecialty data before taking distinct
  dplyr::arrange(abog_id,
                 dplyr::desc(
                   !is.na(subspecialty_name) &
                     subspecialty_name != "NA" & subspecialty_name != ""
                 )) %>%
  dplyr::distinct(abog_id, .keep_all = TRUE) %>%
  dplyr::arrange(abog_id)

write_csv(abog_provider_dataframe, "data/0-Download/output/abog_provider_dataframe.csv")

View(abog_provider_dataframe %>% 
  filter(subspecialty_name=="Female Pelvic Medicine & Reconstructive Surgery"))


# run with a wider net of directories ----
abog_provider_dataframe <- extract_abog_provider_data(
  search_directory = c(
    "~/Dropbox (Personal)/isochrones/physician_data",
    "~/Dropbox (Personal)/workforce" #changed from MasterScraper subdirectory to the entire directory.  This gets us a lot of old results and we only are interested in  
  ),
  file_pattern = "(combined_subspecialty_extract.*\\.csv$|.*updated_physicians.*\\.csv$|Physicians .*\\.csv$)"
) %>%
  # Filter out deceased providers - only check certStatus and sub1certStatus columns
  dplyr::filter(
    !stringr::str_detect(tolower(coalesce(certStatus, "")), "deceased"),!stringr::str_detect(tolower(coalesce(sub1certStatus, "")), "deceased")
  ) %>%
  dplyr::mutate(
    subspecialty_name = dplyr::recode(
      subspecialty_name,
      "CFP" = "Complex Family Planning",
      "FPM" = "Female Pelvic Medicine & Reconstructive Surgery",
      "Urogynecology and Reconstructive Pelvic Surgery" = "Female Pelvic Medicine & Reconstructive Surgery",
      "REI" = "Reproductive Endocrinology and Infertility",
      "PAG" = "Pediatric & Adolescent Gynecology",
      "MFM" = "Maternal-Fetal Medicine",
      "ONC" = "Gynecologic Oncology",
      "HPM" = "Hospice and Palliative Medicine",
      "URP" = "Female Pelvic Medicine & Reconstructive Surgery"
    )
  ) %>%
  dplyr::filter(!is.na(abog_id) & !is.na(physician_name)) %>%
  # PRIORITIZE rows with subspecialty data before taking distinct
  dplyr::arrange(abog_id,
                 dplyr::desc(
                   !is.na(subspecialty_name) &
                     subspecialty_name != "NA" & subspecialty_name != ""
                 )) %>%
  dplyr::distinct(abog_id, .keep_all = TRUE) %>%
  dplyr::arrange(abog_id)

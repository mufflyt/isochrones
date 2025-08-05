# install.packages(c("rvest", "httr2", "pdftools", "stringr", "assertthat"))
library(rvest)
library(httr2)
library(pdftools)
library(tidyverse)
library(stringr)
library(assertthat)
library(cli)

# Yes with Tor Browser Feature -----
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
  
  if (verbose_extraction_logging) {
    logger::log_info("🔍 Starting ABOG PDF subspecialty extraction")
    logger::log_info("Processing {length(physician_id_list)} physicians")
    logger::log_info("Output directory: {output_directory_extractions}")
    logger::log_info("Request delay: {request_delay_seconds} seconds")
    logger::log_info("Chunk size: {chunk_size_records} records")
    logger::log_info("Save individual files: {save_individual_files}")
  }
  
  # Create output directory
  if (!dir.exists(output_directory_extractions)) {
    dir.create(output_directory_extractions, recursive = TRUE)
    if (verbose_extraction_logging) {
      logger::log_info("📁 Created output directory: {output_directory_extractions}")
    }
  }
  
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
  
  # Create timestamp for this extraction run
  extraction_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
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
                                output_directory_extractions, verbose_extraction_logging)
      }
      
      # Save updated combined file after each chunk
      save_combined_results_file(combined_extraction_results, 
                                 output_directory_extractions, verbose_extraction_logging)
      
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
  
  # Final save of combined results (ensure it's up to date)
  final_combined_filepath <- save_combined_results_file(combined_extraction_results, 
                                                        output_directory_extractions, verbose_extraction_logging)
  
  # Generate summary
  if (verbose_extraction_logging) {
    subspecialty_training_count <- sum(!is.na(combined_extraction_results$subspecialty_name) & 
                                         combined_extraction_results$subspecialty_name != "", na.rm = TRUE)
    final_success_rate <- round((successful_extraction_count / length(physician_id_list)) * 100, 1)
    successful_chunks <- total_chunks - length(failed_chunks)
    
    summary_lines <- c(
      paste("📅 Timestamp:", extraction_timestamp),
      paste("🧑‍⚕️ Total physicians:", length(physician_id_list)),
      paste("✅ Successful extractions:", successful_extraction_count),
      paste("❌ Failed extractions:", failed_extraction_count),
      paste("📊 Success rate:", paste0(final_success_rate, "%")),
      paste("🏥 Physicians with subspecialty:", subspecialty_training_count),
      paste("📦 Total chunks:", total_chunks),
      paste("✅ Successful chunks:", successful_chunks),
      paste("❌ Failed chunks:", length(failed_chunks)),
      paste("📂 Output folder:", output_directory_extractions),
      paste("📄 Combined file:", basename(final_combined_filepath))
    )
    
    if (length(failed_chunks) > 0) {
      summary_lines <- c(summary_lines, paste("⚠️ Failed chunk numbers:", paste(failed_chunks, collapse = ", ")))
    }
    
    # Log summary
    logger::log_info("🏁 ABOG PDF subspecialty extraction completed")
    logger::log_info(paste(rep("=", 60), collapse = ""))  # Separator line
    for (line in summary_lines) {
      logger::log_info(line)
    }
    logger::log_info(paste(rep("=", 60), collapse = ""))  # Separator line
    
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
save_combined_results_file <- function(combined_data, output_directory, verbose_logging) {
  if (nrow(combined_data) > 0) {
    combined_filename <- "combined_subspecialty_extractions.csv"
    combined_filepath <- file.path(output_directory, combined_filename)
    
    readr::write_csv(combined_data, combined_filepath)
    
    if (verbose_logging) {
      logger::log_info("💾 Saved combined results: {combined_filename} ({nrow(combined_data)} records)")
    }
    
    return(combined_filepath)
  }
  return(NULL)
}

#' @noRd
save_chunk_results_file <- function(chunk_data, chunk_number, output_directory, verbose_logging) {
  if (nrow(chunk_data) > 0) {
    chunk_filename <- paste0("chunk_", sprintf("%03d", chunk_number), "_subspecialty_extractions_", 
                             format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    chunk_filepath <- file.path(output_directory, chunk_filename)
    
    readr::write_csv(chunk_data, chunk_filepath)
    
    if (verbose_logging) {
      logger::log_info("📄 Saved chunk {chunk_number}: {chunk_filename} ({nrow(chunk_data)} records)")
    }
    
    return(chunk_filepath)
  }
  return(NULL)
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
# Test with Tor using the enhanced configuration
elena_results_browser <- extract_subspecialty_from_pdf_letters(
  physician_id_list = 9020382,
  use_proxy_requests = TRUE,
  proxy_url_requests = "socks5://127.0.0.1:9150",  # Tor Browser port
  chunk_size_records = 10,
  verbose_extraction_logging = TRUE
)

# Load your physician data
physician_data <- readr::read_rds("physician_data/discovery_results/physician_data_from_exploratory_after_2020.rds")

# Extract the userid column for the function
physician_id_list <- physician_data$userid

# Check the setup
length(physician_id_list)  # Should show 7,083
head(physician_id_list)    # Should show first few IDs like 9046330, 9046328, etc.

# Run the full extraction with optimal settings for 7,083 physicians
large_scale_results <- extract_subspecialty_from_pdf_letters(
  physician_id_list = physician_id_list,                    # All 7,083 IDs
  use_proxy_requests = TRUE,                                # Use Tor for anonymity
  proxy_url_requests = "socks5://127.0.0.1:9150",         # Your working Tor BROWSER setup
  output_directory_extractions = "physician_data/abog_large_scale_2025",  # Organized output folder
  request_delay_seconds = 2.0,                             # Be nice to their servers
  chunk_size_records = 100,                                # Save progress every 100 records
  cleanup_downloaded_pdfs = FALSE,                          # Save disk space
  save_individual_files = TRUE,                            # Keep chunk files for backup
  verbose_extraction_logging = TRUE                        # Monitor progress
)


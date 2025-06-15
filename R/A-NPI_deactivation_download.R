# ------------------------------------------------------------------------------
# 1. Initial Setup
# ------------------------------------------------------------------------------
# Libraries loaded include logging (logger), data handling (duckdb, dplyr, readr),
# string manipulation (stringr), and validation (assertthat). The logger is set
# up to document each operation clearly, facilitating troubleshooting and auditing.

# ------------------------------------------------------------------------------
# 2. Data Download (wget)
# ------------------------------------------------------------------------------
# The script uses wget to download Medicare Part D CSV files from data.cms.gov.
# URLs are explicitly defined for each file year (RY13 to RY22). Files are saved
# to a predefined directory, and wget ensures retries and continuation of downloads
# in case of interruptions.


# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------
# Load libraries
library(logger)
library(duckdb)
library(dplyr)
library(readr)
library(stringr)
library(assertthat)
library(tidyverse)  # For data manipulation and visualization
library(downloader)  # For downloading large files efficiently

# Conflict preferences
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

#==============================================================================
# Stage 1: NPI Deactivation files
#==============================================================================
#' List available NPPES Deactivated NPI Reports
#'
#' This function retrieves information about the latest available
#' NPPES Deactivated NPI Report files.
#'
#' @param verbose Logical indicating whether to print additional information.
#'   Default is FALSE.
#'
#' @return A data frame containing information about available files
#'   including file names, dates, and URLs.
#'
#' @examples
#' # Get information about available NPPES Deactivated NPI Report files
#' available_files <- list_nppes_deactivated_reports(verbose = TRUE)
#' print(available_files)
#'
#' @importFrom logger log_info log_debug
#'
#' @export
list_nppes_deactivated_reports <- function(verbose = FALSE) {
  # Initialize logger if not already done
  initialize_logger(verbose)
  
  # Current information based on the latest search results
  latest_date <- get_latest_release_date()
  
  logger::log_info("Latest known NPPES Deactivated NPI Report: {latest_date}")
  
  # Create a data frame with the information
  files_info <- data.frame(
    file_name = paste0("NPPES_Deactivated_NPI_Report_", latest_date, ".zip"),
    release_date = format(as.Date(paste0("20", substr(latest_date, 5, 6), "-", 
                                         substr(latest_date, 1, 2), "-", 
                                         substr(latest_date, 3, 4))),
                          "%Y-%m-%d"),
    url = paste0("https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_", 
                 latest_date, ".zip"),
    stringsAsFactors = FALSE
  )
  
  logger::log_info("Found {nrow(files_info)} available file(s)")
  
  return(files_info)
}#' Download the NPPES Deactivated NPI Report
#' 
#' This function downloads the NPPES Deactivated NPI Report from CMS using wget
#' with robust error handling and detailed logging. It provides options for
#' downloading to a specified directory and extracting the zip file contents.
#'
#' @param destination_directory Character string specifying the directory where the 
#'   file should be downloaded. If the directory doesn't exist, it will be created.
#'   Default is "nppes_deactivated_downloads" in the current working directory.
#' @param extract_file Logical indicating whether to extract the contents of the 
#'   zip file after download. Default is TRUE.
#' @param retry_attempts Integer specifying the number of download retry attempts 
#'   if the download fails. Default is 3.
#' @param timeout_seconds Integer specifying the timeout in seconds for the download
#'   operation. Default is 600 (10 minutes).
#' @param verbose Logical indicating whether to print additional progress messages.
#'   Default is FALSE.
#' @param file_date Character string specifying the date in MMDDYY format (e.g., "041425"
#'   for April 14, 2025) to download a specific version of the report. Default is NULL,
#'   which will use the most recent known release.
#' @param file_url Character string specifying the complete URL to download. If provided,
#'   this overrides file_date parameter. Default is NULL, which constructs the URL based
#'   on file_date or the most recent known release.
#'
#' @return Character vector containing the paths to the downloaded and, if extracted,
#'   the paths to the extracted files.
#'
#' @examples
#' # Example 1: Basic usage with default parameters (most recent release)
#' downloaded_files <- download_nppes_deactivated(
#'   destination_directory = "nppes_data",
#'   extract_file = TRUE,
#'   retry_attempts = 3,
#'   timeout_seconds = 600,
#'   verbose = FALSE,
#'   file_date = NULL,
#'   file_url = NULL
#' )
#' # Returns paths to downloaded zip and extracted files in 'nppes_data' directory
#'
#' # Example 2: Download a specific version by date
#' zip_file_path <- download_nppes_deactivated(
#'   destination_directory = "raw_nppes_files",
#'   extract_file = FALSE,
#'   retry_attempts = 5,
#'   timeout_seconds = 1200,
#'   verbose = TRUE,
#'   file_date = "041425",
#'   file_url = NULL
#' )
#' # Returns path to downloaded zip file only in 'raw_nppes_files' directory
#'
#' # Example 3: Provide a complete custom URL
#' full_file_paths <- download_nppes_deactivated(
#'   destination_directory = "/data/cms/nppes",
#'   extract_file = TRUE,
#'   retry_attempts = 2,
#'   timeout_seconds = 300,
#'   verbose = TRUE,
#'   file_date = NULL,
#'   file_url = "https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_041425.zip"
#' )
#' # Returns paths to downloaded and extracted files with detailed console logging
#'
#' @importFrom logger log_info log_warn log_error log_debug
#' @importFrom assertthat assert_that
#' @importFrom stringr str_extract
#' @importFrom utils unzip
#'
#' @export
download_nppes_deactivated <- function(destination_directory = "nppes_deactivated_downloads",
                                       extract_file = TRUE,
                                       retry_attempts = 3,
                                       timeout_seconds = 600,
                                       verbose = FALSE,
                                       file_date = NULL,
                                       file_url = NULL) {
  # Initialize logger if not already done
  initialize_logger(verbose)
  
  # Validate inputs
  validate_inputs(destination_directory, extract_file, retry_attempts, timeout_seconds, 
                  file_date, file_url)
  
  # Create directory if it doesn't exist
  create_destination_directory(destination_directory)
  
  # Build the URL for the NPPES Deactivated Report
  if (is.null(file_url)) {
    file_url <- construct_nppes_url(file_date)
  } else {
    logger::log_info("Using provided custom URL: {file_url}")
  }
  
  # Extract filename from URL
  file_name <- get_filename_from_url(file_url)
  
  # Set file paths
  destination_file_path <- file.path(destination_directory, file_name)
  
  # Log the download operation
  logger::log_info("Starting download of NPPES Deactivated NPI Report: {file_url}")
  logger::log_info("Download destination: {destination_file_path}")
  
  # Download the file using wget
  download_status <- download_with_wget(
    file_url = file_url,
    destination_file = destination_file_path,
    retry_attempts = retry_attempts,
    timeout_seconds = timeout_seconds
  )
  
  # Check download status
  output_files <- check_download_result(
    download_status = download_status,
    destination_file = destination_file_path,
    extract_file = extract_file,
    destination_directory = destination_directory
  )
  
  # Return paths to downloaded/extracted files
  return(output_files)
}

#' @noRd
initialize_logger <- function(verbose) {
  # Set up logger with appropriate threshold
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
    logger::log_debug("Logger initialized with DEBUG level")
  } else {
    logger::log_threshold(logger::INFO)
  }
}

#' @noRd
validate_inputs <- function(destination_directory, extract_file, retry_attempts, timeout_seconds, 
                            file_date = NULL, file_url = NULL) {
  logger::log_debug("Validating function parameters")
  
  assertthat::assert_that(is.character(destination_directory) && length(destination_directory) == 1,
                          msg = "destination_directory must be a single character string")
  
  assertthat::assert_that(is.logical(extract_file) && length(extract_file) == 1,
                          msg = "extract_file must be a single logical value")
  
  assertthat::assert_that(is.numeric(retry_attempts) && length(retry_attempts) == 1 && retry_attempts > 0,
                          msg = "retry_attempts must be a positive integer")
  
  assertthat::assert_that(is.numeric(timeout_seconds) && length(timeout_seconds) == 1 && timeout_seconds > 0,
                          msg = "timeout_seconds must be a positive integer")
  
  # Validate file_date format if provided
  if (!is.null(file_date)) {
    assertthat::assert_that(is.character(file_date) && length(file_date) == 1,
                            msg = "file_date must be a single character string")
    
    assertthat::assert_that(grepl("^[0-9]{6}$", file_date),
                            msg = "file_date must be in MMDDYY format (e.g., '041425')")
    
    # Extract month, day, and year to validate ranges
    month <- as.numeric(substr(file_date, 1, 2))
    day <- as.numeric(substr(file_date, 3, 4))
    
    assertthat::assert_that(month >= 1 && month <= 12,
                            msg = "Month in file_date must be between 01 and 12")
    
    assertthat::assert_that(day >= 1 && day <= 31,
                            msg = "Day in file_date must be between 01 and 31")
  }
  
  # Validate file_url if provided
  if (!is.null(file_url)) {
    assertthat::assert_that(is.character(file_url) && length(file_url) == 1,
                            msg = "file_url must be a single character string")
    
    assertthat::assert_that(grepl("^https?://", file_url),
                            msg = "file_url must begin with 'http://' or 'https://'")
    
    assertthat::assert_that(grepl("\\.zip$", file_url, ignore.case = TRUE),
                            msg = "file_url must end with '.zip'")
  }
}

#' @noRd
create_destination_directory <- function(destination_directory) {
  if (!dir.exists(destination_directory)) {
    dir.create(destination_directory, recursive = TRUE)
    logger::log_info("Created download directory: {destination_directory}")
  } else {
    logger::log_debug("Using existing directory: {destination_directory}")
  }
}

#' @noRd
get_current_date_formatted <- function() {
  # Get current date and format it for CMS URL pattern
  current_date <- Sys.Date()
  formatted_date <- format(current_date, "%m%d%y")
  logger::log_debug("Current date formatted for URL: {formatted_date}")
  return(formatted_date)
}

#' @noRd
get_latest_release_date <- function() {
  # As of April 26, 2025, the latest known release is from April 14, 2025
  # This is based on the information found in the search results
  return("041425")
}

#' @noRd
construct_nppes_url <- function(date_string = NULL) {
  # Construct URL for NPPES Deactivated Report using fixed date format
  # Current format is "NPPES_Deactivated_NPI_Report_MMDDYY.zip"
  
  # Use provided date or get latest release date
  if (is.null(date_string)) {
    date_string <- get_latest_release_date()
    logger::log_debug("Using latest known release date: {date_string}")
  }
  
  # Construct the URL
  base_url <- "https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_"
  file_url <- paste0(base_url, date_string, ".zip")
  
  logger::log_info("Using CMS file URL: {file_url}")
  return(file_url)
}

#' @noRd
get_filename_from_url <- function(url) {
  # Extract filename from URL
  file_name <- basename(url)
  logger::log_debug("Extracted filename: {file_name}")
  return(file_name)
}

#' @noRd
download_with_wget <- function(file_url, destination_file, retry_attempts, timeout_seconds) {
  # Check if wget is available
  if (Sys.which("wget") == "") {
    logger::log_error("wget is not available on the system")
    stop("wget is not available on your system. Please install wget to proceed.")
  }
  
  # Download file with wget
  logger::log_info("Downloading with wget, attempt will timeout after {timeout_seconds} seconds")
  
  # Prepare wget command arguments
  download_dir_quoted <- shQuote(dirname(destination_file))
  dest_file_quoted <- shQuote(destination_file)
  
  cmd <- "wget"
  args <- c(
    paste0("--tries=", retry_attempts),
    "--continue",
    paste0("--timeout=", timeout_seconds),
    "--no-check-certificate", # Added to handle potential SSL certificate issues
    "--server-response",      # Show server response to help diagnose problems
    "--directory-prefix", download_dir_quoted,
    "--output-document", dest_file_quoted,
    file_url
  )
  
  # Log the command that will be executed
  logger::log_debug("Executing command: wget {paste(args, collapse=' ')}")
  
  # Execute wget command
  wget_result <- tryCatch({
    result <- system2(cmd, args, stdout = TRUE, stderr = TRUE)
    status <- attr(result, "status")
    
    # Log the wget exit status
    if (!is.null(status) && status != 0) {
      logger::log_warn("wget exited with status code {status}")
      
      # Log common wget error codes
      error_message <- case_when(
        status == 1 ~ "Generic error",
        status == 2 ~ "Parse error (invalid command line usage)",
        status == 3 ~ "File I/O error",
        status == 4 ~ "Network failure",
        status == 5 ~ "SSL verification failure",
        status == 6 ~ "Username/password authentication failure",
        status == 7 ~ "Protocol error",
        status == 8 ~ "Server issued an error response (check URL is correct)",
        TRUE ~ paste("Unknown error with status code", status)
      )
      logger::log_warn("wget error: {error_message}")
    }
    
    # Return both result and status
    list(
      result = result,
      status = status,
      success = is.null(status) || status == 0
    )
  }, error = function(e) {
    logger::log_error("Error executing wget: {e$message}")
    list(
      result = character(0),
      status = 1,
      success = FALSE
    )
  })
  
  # Log any output from wget to help with debugging
  if (length(wget_result$result) > 0) {
    logger::log_debug("wget output: {paste(head(wget_result$result, 10), collapse='\n')}")
    if (length(wget_result$result) > 10) {
      logger::log_debug("... and {length(wget_result$result) - 10} more lines")
    }
  }
  
  return(wget_result)
}

#' @noRd
check_download_result <- function(download_status, destination_file, extract_file, destination_directory) {
  output_files <- character(0)
  
  # Check if download was successful
  if (file.exists(destination_file)) {
    # Verify file size to ensure it's not empty or corrupted
    file_size <- file.info(destination_file)$size
    
    if (file_size > 0) {
      logger::log_info("✅ Successfully downloaded file: {destination_file} ({formatFileSize(file_size)})")
      output_files <- c(output_files, destination_file)
      
      # Extract the zip file if requested
      if (extract_file) {
        logger::log_info("Extracting contents of zip file: {destination_file}")
        
        # First check if it's a valid zip file
        is_valid_zip <- tryCatch({
          zip_contents <- utils::unzip(destination_file, list = TRUE)
          TRUE
        }, error = function(e) {
          logger::log_error("❌ Invalid or corrupted zip file: {e$message}")
          FALSE
        })
        
        if (is_valid_zip) {
          extraction_result <- tryCatch({
            extracted_files <- utils::unzip(destination_file, exdir = destination_directory)
            
            if (length(extracted_files) > 0) {
              logger::log_info("✅ Successfully extracted {length(extracted_files)} files")
              extracted_files
            } else {
              logger::log_warn("⚠️ Zip file was valid but contained no files")
              character(0)
            }
          }, error = function(e) {
            logger::log_error("❌ Failed to extract zip file: {e$message}")
            character(0)
          })
          
          output_files <- c(output_files, extraction_result)
        } else {
          logger::log_warn("Skipping extraction due to invalid zip file")
        }
      }
    } else {
      logger::log_error("❌ Downloaded file exists but has zero size: {destination_file}")
      if (download_status$status != 0) {
        logger::log_error("wget failed with status code: {download_status$status}")
      }
    }
  } else {
    logger::log_error("❌ Failed to download file: {destination_file}")
    if (!is.null(download_status$result)) {
      logger::log_error("wget output: {paste(head(download_status$result, 5), collapse='\n')}")
    }
  }
  
  logger::log_info("Operation completed with {length(output_files)} total files")
  return(output_files)
}

#' @noRd
formatFileSize <- function(size_bytes) {
  # Format file size in human-readable format
  if (size_bytes < 1024) {
    return(paste0(size_bytes, " B"))
  } else if (size_bytes < 1024^2) {
    return(paste0(round(size_bytes/1024, 1), " KB"))
  } else if (size_bytes < 1024^3) {
    return(paste0(round(size_bytes/1024^2, 1), " MB"))
  } else {
    return(paste0(round(size_bytes/1024^3, 1), " GB"))
  }
}

###########
download_nppes_deactivated(destination_directory = "data/nppes_deactivated_downloads",
                                       extract_file = TRUE,
                                       retry_attempts = 3,
                                       timeout_seconds = 600,
                                       verbose = TRUE)


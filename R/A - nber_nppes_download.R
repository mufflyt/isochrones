# A - nber_nppes_download.R

library(arrow)
library(tidyverse)
library(DBI)
library(duckdb)


# 1000 Function ----
#' Download NBER NPI Files Using wget (More Reliable for Large Files)
#'
#' Uses system wget command for robust downloading of large NBER NPI files.
#' Provides automatic resume capability and better handling of network issues.
#'
#' @param target_years Integer vector of years to download (2007-2023).
#'   Default: 2007:2023
#' @param file_types Character vector of file types to download. Options:
#'   "core", "npi", "othpid", "plic", "ptax". Default: c("npi")
#' @param output_directory Character string specifying destination directory.
#'   Default: "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped"
#' @param file_format Character string specifying format: "csv" or "parquet".
#'   Default: "parquet"
#' @param fallback_to_csv Logical indicating whether to try CSV format if
#'   parquet is not available. Default: TRUE
#' @param timeout_minutes Numeric timeout in minutes. Default: 60 (1 hour)
#' @param max_retries Integer number of retry attempts. Default: 3
#' @param resume_downloads Logical to resume partial downloads. Default: TRUE
#' @param verbose Logical indicating whether to show detailed progress.
#'   Default: TRUE
#' @param overwrite_existing Logical indicating whether to overwrite existing
#'   files. Default: FALSE
#'
#' @return Invisible list containing download summary statistics
#'
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom fs dir_create file_exists dir_exists file_size
#' @importFrom glue glue
#'
#' @examples
#' # Download with wget (much more reliable for large files)
#' wget_download <- download_nber_npi_wget(
#'   target_years = 2015:2023,
#'   file_types = "npi",
#'   output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   file_format = "parquet",
#'   fallback_to_csv = TRUE,
#'   timeout_minutes = 60,
#'   max_retries = 3,
#'   resume_downloads = TRUE,
#'   verbose = TRUE,
#'   overwrite_existing = FALSE
#' )
#'
#' # Download single problematic year with resume
#' wget_single <- download_nber_npi_wget(
#'   target_years = 2015,
#'   file_types = "npi",
#'   timeout_minutes = 120,
#'   max_retries = 5,
#'   resume_downloads = TRUE,
#'   verbose = TRUE,
#'   overwrite_existing = FALSE
#' )
#'
#' @export
download_nber_npi_wget <- function(target_years = 2007:2023,
                                   file_types = c("npi"),
                                   output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
                                   file_format = "parquet",
                                   fallback_to_csv = TRUE,
                                   timeout_minutes = 60,
                                   max_retries = 3,
                                   resume_downloads = TRUE,
                                   verbose = TRUE,
                                   overwrite_existing = FALSE) {
  
  # Check if wget is available
  wget_available <- check_wget_availability()
  if (!wget_available) {
    stop("wget is not available. Please install wget or use the regular download function.")
  }
  
  # Input validation
  validate_wget_inputs(target_years, file_types, output_directory, file_format,
                       fallback_to_csv, timeout_minutes, max_retries, 
                       resume_downloads, verbose, overwrite_existing)
  
  if (verbose) {
    logger::log_info("Starting wget-based NBER NPI data download")
    logger::log_info("Target years: {paste(target_years, collapse = ', ')}")
    logger::log_info("File types: {paste(file_types, collapse = ', ')}")
    logger::log_info("Output directory: {output_directory}")
    logger::log_info("File format: {file_format}")
    logger::log_info("Timeout: {timeout_minutes} minutes")
    logger::log_info("Max retries: {max_retries}")
    logger::log_info("Resume downloads: {resume_downloads}")
  }
  
  # Create output directory
  if (!fs::dir_exists(output_directory)) {
    fs::dir_create(output_directory, recurse = TRUE)
    if (verbose) {
      logger::log_info("Created output directory: {output_directory}")
    }
  }
  
  # Construct download URLs (using existing logic)
  download_manifest <- construct_nber_file_urls_wget(
    target_years = target_years,
    file_types = file_types,
    file_format = file_format,
    fallback_to_csv = fallback_to_csv,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("Generated {nrow(download_manifest)} download tasks")
  }
  
  # Execute downloads with wget
  download_results <- data.frame()
  
  for (i in seq_len(nrow(download_manifest))) {
    current_task <- download_manifest[i, ]
    
    download_result <- wget_single_file(
      source_url = current_task$source_url,
      destination_path = file.path(output_directory, current_task$filename),
      file_description = current_task$file_description,
      timeout_minutes = timeout_minutes,
      max_retries = max_retries,
      resume_downloads = resume_downloads,
      verbose = verbose,
      overwrite_existing = overwrite_existing
    )
    
    download_results <- rbind(download_results, download_result)
  }
  
  # Generate summary statistics
  download_summary <- generate_wget_summary(
    download_results = download_results,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("Wget download process completed")
    logger::log_info("Success rate: {download_summary$success_rate}%")
    logger::log_info("Total files processed: {download_summary$total_files}")
    logger::log_info("Output directory: {output_directory}")
  }
  
  return(invisible(download_summary))
}

#' @noRd
check_wget_availability <- function() {
  result <- suppressWarnings(system("which wget", intern = TRUE, ignore.stderr = TRUE))
  return(length(result) > 0 && result != "")
}

#' @noRd
validate_wget_inputs <- function(target_years, file_types, output_directory, 
                                 file_format, fallback_to_csv, timeout_minutes,
                                 max_retries, resume_downloads, verbose, overwrite_existing) {
  
  assertthat::assert_that(is.numeric(target_years))
  assertthat::assert_that(all(target_years >= 2007 & target_years <= 2023))
  
  valid_file_types <- c("core", "npi", "othpid", "plic", "ptax")
  assertthat::assert_that(all(file_types %in% valid_file_types))
  
  assertthat::assert_that(assertthat::is.string(output_directory))
  assertthat::assert_that(file_format %in% c("csv", "parquet"))
  assertthat::assert_that(assertthat::is.flag(fallback_to_csv))
  assertthat::assert_that(is.numeric(timeout_minutes) && timeout_minutes > 0)
  assertthat::assert_that(is.numeric(max_retries) && max_retries >= 1)
  assertthat::assert_that(assertthat::is.flag(resume_downloads))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(assertthat::is.flag(overwrite_existing))
  
  return(TRUE)
}

#' @noRd
construct_nber_file_urls_wget <- function(target_years, file_types, file_format,
                                          fallback_to_csv, verbose) {
  
  base_url <- "https://data.nber.org/npi/webdir/csv"
  
  # Create basic manifest
  download_manifest <- data.frame()
  
  for (year in target_years) {
    for (file_type in file_types) {
      
      # Determine filename based on year and format
      if (year >= 2023) {
        primary_filename <- paste0(file_type, "_April_", year, ".", file_format)
        fallback_filename <- paste0(file_type, "_April_", year, ".csv")
      } else {
        primary_filename <- paste0(file_type, year, "4.", file_format)
        fallback_filename <- paste0(file_type, year, "4.csv")
      }
      
      primary_url <- paste0(base_url, "/", year, "/", primary_filename)
      fallback_url <- paste0(base_url, "/", year, "/", fallback_filename)
      file_description <- paste(stringr::str_to_title(file_type), "data for April", year)
      
      # Check which format to use
      if (file_format == "parquet" && year < 2020) {
        if (fallback_to_csv) {
          # Use CSV fallback for older years
          new_row <- data.frame(
            year = year,
            file_type = file_type,
            filename = fallback_filename,
            source_url = fallback_url,
            file_description = paste(file_description, "(CSV fallback)"),
            format_used = "csv",
            stringsAsFactors = FALSE
          )
        } else {
          # Try parquet anyway (will likely fail)
          new_row <- data.frame(
            year = year,
            file_type = file_type,
            filename = primary_filename,
            source_url = primary_url,
            file_description = file_description,
            format_used = file_format,
            stringsAsFactors = FALSE
          )
        }
      } else {
        # Use primary format
        new_row <- data.frame(
          year = year,
          file_type = file_type,
          filename = primary_filename,
          source_url = primary_url,
          file_description = file_description,
          format_used = file_format,
          stringsAsFactors = FALSE
        )
      }
      
      download_manifest <- rbind(download_manifest, new_row)
    }
  }
  
  if (verbose) {
    logger::log_info("Constructed URLs for {nrow(download_manifest)} files")
  }
  
  return(download_manifest)
}

#' @noRd
wget_single_file <- function(source_url, destination_path, file_description,
                             timeout_minutes, max_retries, resume_downloads,
                             verbose, overwrite_existing) {
  
  # Check if file already exists and is complete
  if (fs::file_exists(destination_path) && !overwrite_existing) {
    if (verbose) {
      file_size_mb <- as.numeric(fs::file_size(destination_path)) / (1024^2)
      logger::log_info("Skipping existing file: {basename(destination_path)} ({round(file_size_mb, 2)} MB)")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "skipped",
      file_size_mb = as.numeric(fs::file_size(destination_path)) / (1024^2),
      download_time_seconds = 0,
      error_message = NA_character_,
      stringsAsFactors = FALSE
    ))
  }
  
  if (verbose) {
    logger::log_info("Starting wget download: {file_description}")
    logger::log_info("Source URL: {source_url}")
    logger::log_info("Destination: {destination_path}")
  }
  
  download_start_time <- Sys.time()
  
  # Build wget command
  wget_cmd <- build_wget_command(
    source_url = source_url,
    destination_path = destination_path,
    timeout_minutes = timeout_minutes,
    max_retries = max_retries,
    resume_downloads = resume_downloads,
    verbose = verbose,
    overwrite_existing = overwrite_existing
  )
  
  if (verbose) {
    logger::log_info("Executing: {wget_cmd}")
  }
  
  # Execute wget
  exit_code <- system(wget_cmd)
  
  download_end_time <- Sys.time()
  download_duration <- as.numeric(difftime(download_end_time, download_start_time, units = "secs"))
  
  # Check results
  if (exit_code == 0 && fs::file_exists(destination_path)) {
    file_size_mb <- as.numeric(fs::file_size(destination_path)) / (1024^2)
    
    if (verbose) {
      logger::log_info("✅ Successfully downloaded: {basename(destination_path)}")
      logger::log_info("File size: {round(file_size_mb, 2)} MB")
      logger::log_info("Download time: {round(download_duration/60, 1)} minutes")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "success",
      file_size_mb = file_size_mb,
      download_time_seconds = download_duration,
      error_message = NA_character_,
      stringsAsFactors = FALSE
    ))
    
  } else {
    error_msg <- paste("wget failed with exit code", exit_code)
    
    if (verbose) {
      logger::log_error("❌ Download failed: {basename(destination_path)} - {error_msg}")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "failed",
      file_size_mb = 0,
      download_time_seconds = download_duration,
      error_message = error_msg,
      stringsAsFactors = FALSE
    ))
  }
}

#' @noRd
build_wget_command <- function(source_url, destination_path, timeout_minutes,
                               max_retries, resume_downloads, verbose, overwrite_existing) {
  
  # Base wget command with essential options
  cmd_parts <- c(
    "wget",
    "--no-check-certificate",  # Handle SSL issues
    "--user-agent='Mozilla/5.0 (compatible; R-wget)'",  # Better user agent
    paste0("--timeout=", timeout_minutes * 60),  # Convert to seconds
    paste0("--tries=", max_retries + 1),  # +1 because tries includes initial attempt
    "--wait=5",  # Wait 5 seconds between retries
    "--waitretry=10"  # Wait 10 seconds on connection failures
  )
  
  # Add resume capability if requested
  if (resume_downloads) {
    cmd_parts <- c(cmd_parts, "--continue")
  }
  
  # Add progress indicator
  if (verbose) {
    cmd_parts <- c(cmd_parts, "--progress=bar:force")
  } else {
    cmd_parts <- c(cmd_parts, "--quiet")
  }
  
  # Add output file specification
  cmd_parts <- c(cmd_parts, paste0("--output-document='", destination_path, "'"))
  
  # Add URL (must be last)
  cmd_parts <- c(cmd_parts, paste0("'", source_url, "'"))
  
  # Join all parts
  return(paste(cmd_parts, collapse = " "))
}

#' @noRd
generate_wget_summary <- function(download_results, verbose) {
  
  summary_stats <- data.frame(
    total_files = nrow(download_results),
    successful_downloads = sum(download_results$status == "success"),
    failed_downloads = sum(download_results$status == "failed"),
    skipped_files = sum(download_results$status == "skipped"),
    total_size_mb = sum(download_results$file_size_mb, na.rm = TRUE),
    total_download_time_minutes = sum(download_results$download_time_seconds, na.rm = TRUE) / 60,
    stringsAsFactors = FALSE
  )
  
  summary_stats$success_rate <- round((summary_stats$successful_downloads / summary_stats$total_files) * 100, 1)
  summary_stats$average_speed_mbps <- round((summary_stats$total_size_mb * 8) / (summary_stats$total_download_time_minutes * 60), 2)
  
  if (verbose) {
    if (summary_stats$failed_downloads > 0) {
      failed_files <- download_results$filename[download_results$status == "failed"]
      logger::log_warn("Failed downloads: {paste(failed_files, collapse = ', ')}")
    }
    
    if (summary_stats$total_download_time_minutes > 0) {
      logger::log_info("Average download speed: {summary_stats$average_speed_mbps} Mbps")
      logger::log_info("Total download time: {round(summary_stats$total_download_time_minutes, 1)} minutes")
    }
  }
  
  return(summary_stats)
}

# Execute ----
# RUN THIS OVERNIGHT :)
# Download all your target years with robust wget
wget_download_all <- download_nber_npi_wget(
  target_years = 2013:2023,
  file_types = "npi",
  output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
  file_format = "parquet",
  fallback_to_csv = TRUE,
  timeout_minutes = 90,      # 1.5 hours per file
  max_retries = 3,
  resume_downloads = TRUE,   # Key advantage!
  verbose = TRUE,
  overwrite_existing = FALSE
)




# 1022 function ----
#' Download NBER NPI Files Using wget (More Reliable for Large Files)
#'
#' Uses system wget command for robust downloading of large NBER NPI files.
#' Provides automatic resume capability and better handling of network issues.
#'
#' @param target_years Integer vector of years to download (2007-2023).
#'   Default: 2007:2023
#' @param file_types Character vector of file types to download. Options:
#'   "core", "npi", "othpid", "plic", "ptax". Default: c("npi")
#' @param output_directory Character string specifying destination directory.
#'   Default: "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped"
#' @param file_format Character string specifying format: "csv" or "parquet".
#'   Default: "parquet"
#' @param fallback_to_csv Logical indicating whether to try CSV format if
#'   parquet is not available. Default: TRUE
#' @param timeout_minutes Numeric timeout in minutes. Default: 60 (1 hour)
#' @param max_retries Integer number of retry attempts. Default: 3
#' @param resume_downloads Logical to resume partial downloads. Default: TRUE
#' @param verbose Logical indicating whether to show detailed progress.
#'   Default: TRUE
#' @param overwrite_existing Logical indicating whether to overwrite existing
#'   files. Default: FALSE
#'
#' @return Invisible list containing download summary statistics
#'
#' @importFrom logger log_info log_warn log_error
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom fs dir_create file_exists dir_exists file_size
#' @importFrom glue glue
#'
#' @examples
#' # Download with wget (much more reliable for large files)
#' wget_download <- download_nber_npi_wget(
#'   target_years = 2015:2023,
#'   file_types = "npi",
#'   output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
#'   file_format = "parquet",
#'   fallback_to_csv = TRUE,
#'   timeout_minutes = 60,
#'   max_retries = 3,
#'   resume_downloads = TRUE,
#'   verbose = TRUE,
#'   overwrite_existing = FALSE
#' )
#'
#' # Download single problematic year with resume
#' wget_single <- download_nber_npi_wget(
#'   target_years = 2015,
#'   file_types = "npi",
#'   timeout_minutes = 120,
#'   max_retries = 5,
#'   resume_downloads = TRUE,
#'   verbose = TRUE,
#'   overwrite_existing = FALSE
#' )
#'
#' @export
download_nber_npi_wget <- function(target_years = 2007:2023,
                                   file_types = c("npi"),
                                   output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
                                   file_format = "parquet",
                                   fallback_to_csv = TRUE,
                                   preferred_month = 4,
                                   month_fallback_order = c(3, 5, 2, 6, 1, 7, 12, 8, 11, 9, 10),
                                   check_availability = TRUE,
                                   timeout_minutes = 60,
                                   max_retries = 3,
                                   resume_downloads = TRUE,
                                   verbose = TRUE,
                                   overwrite_existing = FALSE) {
  
  # Check if wget is available
  wget_available <- check_wget_availability()
  if (!wget_available) {
    stop("wget is not available. Please install wget or use the regular download function.")
  }
  
  # Input validation
  validate_wget_inputs(target_years, file_types, output_directory, file_format,
                       fallback_to_csv, preferred_month, month_fallback_order,
                       check_availability, timeout_minutes, max_retries, 
                       resume_downloads, verbose, overwrite_existing)
  
  if (verbose) {
    logger::log_info("Starting smart wget-based NBER NPI data download")
    logger::log_info("Target years: {paste(target_years, collapse = ', ')}")
    logger::log_info("File types: {paste(file_types, collapse = ', ')}")
    logger::log_info("Output directory: {output_directory}")
    logger::log_info("File format: {file_format}")
    logger::log_info("Preferred month: {preferred_month} (April = 4)")
    logger::log_info("Month fallback order: {paste(month_fallback_order[1:5], collapse = ', ')}...")
    logger::log_info("Check availability: {check_availability}")
    logger::log_info("Timeout: {timeout_minutes} minutes")
  }
  
  # Create output directory
  if (!fs::dir_exists(output_directory)) {
    fs::dir_create(output_directory, recurse = TRUE)
    if (verbose) {
      logger::log_info("Created output directory: {output_directory}")
    }
  }
  
  # Smart URL construction with availability checking
  download_manifest <- construct_smart_download_urls(
    target_years = target_years,
    file_types = file_types,
    file_format = file_format,
    fallback_to_csv = fallback_to_csv,
    preferred_month = preferred_month,
    month_fallback_order = month_fallback_order,
    check_availability = check_availability,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("Generated {nrow(download_manifest)} download tasks")
  }
  
  # Execute downloads with wget
  download_results <- data.frame()
  
  for (i in seq_len(nrow(download_manifest))) {
    current_task <- download_manifest[i, ]
    
    download_result <- wget_single_file(
      source_url = current_task$source_url,
      destination_path = file.path(output_directory, current_task$filename),
      file_description = current_task$file_description,
      timeout_minutes = timeout_minutes,
      max_retries = max_retries,
      resume_downloads = resume_downloads,
      verbose = verbose,
      overwrite_existing = overwrite_existing
    )
    
    download_results <- rbind(download_results, download_result)
  }
  
  # Generate summary statistics
  download_summary <- generate_wget_summary(
    download_results = download_results,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("Smart wget download process completed")
    logger::log_info("Success rate: {download_summary$success_rate}%")
    logger::log_info("Total files processed: {download_summary$total_files}")
    logger::log_info("Output directory: {output_directory}")
  }
  
  return(invisible(list(
    summary = download_summary,
    file_mapping = download_manifest,
    results = download_results
  )))
}

#' @noRd
check_wget_availability <- function() {
  result <- suppressWarnings(system("which wget", intern = TRUE, ignore.stderr = TRUE))
  return(length(result) > 0 && result != "")
}

#' @noRd
validate_wget_inputs <- function(target_years, file_types, output_directory, 
                                 file_format, fallback_to_csv, preferred_month,
                                 month_fallback_order, check_availability,
                                 timeout_minutes, max_retries, resume_downloads, 
                                 verbose, overwrite_existing) {
  
  assertthat::assert_that(is.numeric(target_years))
  assertthat::assert_that(all(target_years >= 2007 & target_years <= 2023))
  
  valid_file_types <- c("core", "npi", "othpid", "plic", "ptax")
  assertthat::assert_that(all(file_types %in% valid_file_types))
  
  assertthat::assert_that(assertthat::is.string(output_directory))
  assertthat::assert_that(file_format %in% c("csv", "parquet"))
  assertthat::assert_that(assertthat::is.flag(fallback_to_csv))
  
  assertthat::assert_that(is.numeric(preferred_month))
  assertthat::assert_that(preferred_month >= 1 && preferred_month <= 12)
  
  assertthat::assert_that(is.numeric(month_fallback_order))
  assertthat::assert_that(all(month_fallback_order >= 1 & month_fallback_order <= 12))
  
  assertthat::assert_that(assertthat::is.flag(check_availability))
  assertthat::assert_that(is.numeric(timeout_minutes) && timeout_minutes > 0)
  assertthat::assert_that(is.numeric(max_retries) && max_retries >= 1)
  assertthat::assert_that(assertthat::is.flag(resume_downloads))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(assertthat::is.flag(overwrite_existing))
  
  return(TRUE)
}

#' @noRd
construct_smart_download_urls <- function(target_years, file_types, file_format,
                                          fallback_to_csv, preferred_month,
                                          month_fallback_order, check_availability, verbose) {
  
  base_url <- "https://data.nber.org/npi/webdir/csv"
  download_manifest <- data.frame()
  
  for (year in target_years) {
    for (file_type in file_types) {
      
      if (verbose) {
        logger::log_info("Finding best file for {file_type} {year}...")
      }
      
      # Build list of months to try (preferred first, then fallbacks)
      months_to_try <- c(preferred_month, month_fallback_order)
      months_to_try <- unique(months_to_try)  # Remove duplicates
      
      best_file <- NULL
      
      # Try each month until we find one that works
      for (month in months_to_try) {
        
        # Handle different naming conventions
        if (year >= 2023) {
          # 2023+ uses explicit month names for April only
          if (month == 4) {
            primary_filename <- paste0(file_type, "_April_", year, ".", file_format)
            fallback_filename <- paste0(file_type, "_April_", year, ".csv")
          } else {
            # For non-April months in 2023+, use numeric format
            primary_filename <- paste0(file_type, year, month, ".", file_format)
            fallback_filename <- paste0(file_type, year, month, ".csv")
          }
        } else {
          # Pre-2023 uses numeric month format
          primary_filename <- paste0(file_type, year, month, ".", file_format)
          fallback_filename <- paste0(file_type, year, month, ".csv")
        }
        
        # Try primary format first
        primary_url <- paste0(base_url, "/", year, "/", primary_filename)
        
        if (check_availability) {
          primary_available <- check_url_exists(primary_url)
        } else {
          primary_available <- TRUE  # Assume available if not checking
        }
        
        if (primary_available) {
          best_file <- list(
            filename = primary_filename,
            url = primary_url,
            month_used = month,
            format_used = file_format,
            description = paste("Monthly", stringr::str_to_title(file_type), 
                                "data for", year, "month", month,
                                if (month == preferred_month) "(preferred)" else "(fallback)")
          )
          
          if (verbose && month != preferred_month) {
            logger::log_info("Using month {month} instead of {preferred_month} for {file_type} {year}")
          }
          break
          
        } else if (fallback_to_csv && file_format == "parquet") {
          # Try CSV fallback for this month
          fallback_url <- paste0(base_url, "/", year, "/", fallback_filename)
          
          if (check_availability) {
            fallback_available <- check_url_exists(fallback_url)
          } else {
            fallback_available <- TRUE
          }
          
          if (fallback_available) {
            best_file <- list(
              filename = fallback_filename,
              url = fallback_url,
              month_used = month,
              format_used = "csv",
              description = paste("Monthly", stringr::str_to_title(file_type),
                                  "data for", year, "month", month, "(CSV fallback)")
            )
            
            if (verbose) {
              if (month != preferred_month) {
                logger::log_info("Using CSV fallback for month {month} instead of {preferred_month} for {file_type} {year}")
              } else {
                logger::log_info("Using CSV fallback for {file_type} {year} month {month}")
              }
            }
            break
          }
        }
      }
      
      # If we found a file, add it to manifest
      if (!is.null(best_file)) {
        new_row <- data.frame(
          year = year,
          file_type = file_type,
          month_used = best_file$month_used,
          filename = best_file$filename,
          source_url = best_file$url,
          file_description = best_file$description,
          format_used = best_file$format_used,
          stringsAsFactors = FALSE
        )
        
        download_manifest <- rbind(download_manifest, new_row)
        
      } else {
        if (verbose) {
          logger::log_warn("No available files found for {file_type} {year} in any month")
        }
      }
    }
  }
  
  if (verbose && nrow(download_manifest) > 0) {
    logger::log_info("Smart URL construction completed")
    
    # Show summary of what was found
    month_summary <- download_manifest %>%
      group_by(month_used) %>%
      summarise(
        count = n(),
        years = paste(sort(year), collapse = ", "),
        .groups = "drop"
      )
    
    for (i in seq_len(nrow(month_summary))) {
      month <- month_summary$month_used[i]
      count <- month_summary$count[i]
      years <- month_summary$years[i]
      
      month_name <- month.name[month]
      if (month == preferred_month) {
        logger::log_info("Month {month} ({month_name} - preferred): {count} files for years {years}")
      } else {
        logger::log_info("Month {month} ({month_name} - fallback): {count} files for years {years}")
      }
    }
  }
  
  return(download_manifest)
}

#' @noRd
check_url_exists <- function(url) {
  # Simple HEAD request to check if URL exists
  result <- tryCatch({
    response <- system(paste0("curl -I --silent --head --fail '", url, "'"), intern = FALSE)
    return(response == 0)  # Exit code 0 means success
  }, error = function(e) {
    return(FALSE)
  })
  
  return(result)
}

#' @noRd
wget_single_file <- function(source_url, destination_path, file_description,
                             timeout_minutes, max_retries, resume_downloads,
                             verbose, overwrite_existing) {
  
  # Check if file already exists and is complete
  if (fs::file_exists(destination_path) && !overwrite_existing) {
    if (verbose) {
      file_size_mb <- as.numeric(fs::file_size(destination_path)) / (1024^2)
      logger::log_info("Skipping existing file: {basename(destination_path)} ({round(file_size_mb, 2)} MB)")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "skipped",
      file_size_mb = as.numeric(fs::file_size(destination_path)) / (1024^2),
      download_time_seconds = 0,
      error_message = NA_character_,
      stringsAsFactors = FALSE
    ))
  }
  
  if (verbose) {
    logger::log_info("Starting wget download: {file_description}")
    logger::log_info("Source URL: {source_url}")
    logger::log_info("Destination: {destination_path}")
  }
  
  download_start_time <- Sys.time()
  
  # Build wget command
  wget_cmd <- build_wget_command(
    source_url = source_url,
    destination_path = destination_path,
    timeout_minutes = timeout_minutes,
    max_retries = max_retries,
    resume_downloads = resume_downloads,
    verbose = verbose,
    overwrite_existing = overwrite_existing
  )
  
  if (verbose) {
    logger::log_info("Executing: {wget_cmd}")
  }
  
  # Execute wget
  exit_code <- system(wget_cmd)
  
  download_end_time <- Sys.time()
  download_duration <- as.numeric(difftime(download_end_time, download_start_time, units = "secs"))
  
  # Check results
  if (exit_code == 0 && fs::file_exists(destination_path)) {
    file_size_mb <- as.numeric(fs::file_size(destination_path)) / (1024^2)
    
    if (verbose) {
      logger::log_info("✅ Successfully downloaded: {basename(destination_path)}")
      logger::log_info("File size: {round(file_size_mb, 2)} MB")
      logger::log_info("Download time: {round(download_duration/60, 1)} minutes")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "success",
      file_size_mb = file_size_mb,
      download_time_seconds = download_duration,
      error_message = NA_character_,
      stringsAsFactors = FALSE
    ))
    
  } else {
    error_msg <- paste("wget failed with exit code", exit_code)
    
    if (verbose) {
      logger::log_error("❌ Download failed: {basename(destination_path)} - {error_msg}")
    }
    
    return(data.frame(
      filename = basename(destination_path),
      status = "failed",
      file_size_mb = 0,
      download_time_seconds = download_duration,
      error_message = error_msg,
      stringsAsFactors = FALSE
    ))
  }
}

#' @noRd
build_wget_command <- function(source_url, destination_path, timeout_minutes,
                               max_retries, resume_downloads, verbose, overwrite_existing) {
  
  # Base wget command with essential options
  cmd_parts <- c(
    "wget",
    "--no-check-certificate",  # Handle SSL issues
    "--user-agent='Mozilla/5.0 (compatible; R-wget)'",  # Better user agent
    paste0("--timeout=", timeout_minutes * 60),  # Convert to seconds
    paste0("--tries=", max_retries + 1),  # +1 because tries includes initial attempt
    "--wait=5",  # Wait 5 seconds between retries
    "--waitretry=10"  # Wait 10 seconds on connection failures
  )
  
  # Add resume capability if requested
  if (resume_downloads) {
    cmd_parts <- c(cmd_parts, "--continue")
  }
  
  # Add progress indicator
  if (verbose) {
    cmd_parts <- c(cmd_parts, "--progress=bar:force")
  } else {
    cmd_parts <- c(cmd_parts, "--quiet")
  }
  
  # Add output file specification
  cmd_parts <- c(cmd_parts, paste0("--output-document='", destination_path, "'"))
  
  # Add URL (must be last)
  cmd_parts <- c(cmd_parts, paste0("'", source_url, "'"))
  
  # Join all parts
  return(paste(cmd_parts, collapse = " "))
}

#' @noRd
generate_wget_summary <- function(download_results, verbose) {
  
  summary_stats <- data.frame(
    total_files = nrow(download_results),
    successful_downloads = sum(download_results$status == "success"),
    failed_downloads = sum(download_results$status == "failed"),
    skipped_files = sum(download_results$status == "skipped"),
    total_size_mb = sum(download_results$file_size_mb, na.rm = TRUE),
    total_download_time_minutes = sum(download_results$download_time_seconds, na.rm = TRUE) / 60,
    stringsAsFactors = FALSE
  )
  
  summary_stats$success_rate <- round((summary_stats$successful_downloads / summary_stats$total_files) * 100, 1)
  summary_stats$average_speed_mbps <- round((summary_stats$total_size_mb * 8) / (summary_stats$total_download_time_minutes * 60), 2)
  
  if (verbose) {
    if (summary_stats$failed_downloads > 0) {
      failed_files <- download_results$filename[download_results$status == "failed"]
      logger::log_warn("Failed downloads: {paste(failed_files, collapse = ', ')}")
    }
    
    if (summary_stats$total_download_time_minutes > 0) {
      logger::log_info("Average download speed: {summary_stats$average_speed_mbps} Mbps")
      logger::log_info("Total download time: {round(summary_stats$total_download_time_minutes, 1)} minutes")
    }
  }
  
  return(summary_stats)
}

# Execute ----
# RUN THIS OVERNIGHT :)
# Download all your target years with robust wget
wget_download_all <- download_nber_npi_wget(
  target_years = 2013:2023,
  file_types = "npi",
  output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
  file_format = "parquet",
  fallback_to_csv = TRUE,
  timeout_minutes = 90,      # 1.5 hours per file
  max_retries = 3,
  resume_downloads = TRUE,   # Key advantage!
  verbose = TRUE,
  overwrite_existing = FALSE
)

# Just get 2018
wget_download_all <- download_nber_npi_wget(
  target_years = 2018,
  file_types = "npi",
  output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
  file_format = "parquet",
  fallback_to_csv = TRUE,
  timeout_minutes = 90,      # 1.5 hours per file
  max_retries = 3,
  resume_downloads = TRUE,   # Key advantage!
  verbose = TRUE,
  overwrite_existing = TRUE
)

# Just get 2015, 2016, 2017
wget_download_all <- download_nber_npi_wget(
  target_years = 2015:2017,
  file_types = "npi",
  output_directory = "/Volumes/MufflyNew/nppes_historical_downloads/nber_shaped",
  file_format = "parquet",
  fallback_to_csv = TRUE,
  timeout_minutes = 90,      # 1.5 hours per file
  max_retries = 3,
  resume_downloads = TRUE,   # Key advantage!
  verbose = TRUE,
  overwrite_existing = TRUE # I think there was an incomplete year 2015, 2016, 2017.  
); wget_download_all


#!/usr/bin/env Rscript

# Simple Batch NPI Processing System
# Direct CSV processing without complex column detection
# Author: Tyler & Claude
# Created: 2025-08-24

library(dplyr)
library(parallel)
library(doParallel)
library(foreach)
source('R/03.1-real_database_integration.R')
source("R/03.3-fill_best_middle_names.R")

#' Simple Batch Process from CSV
#' @param csv_file Path to CSV file
#' @param physician_name_column Required column name for physician names
#' @param npi_column Column name for expected NPIs (optional)
#' @param num_cores Number of cores for parallel processing
#' @param chunk_size Number of physicians per processing chunk
#' @param verbose Show progress
#' @param export_file_path Full path and filename for exported results CSV ("auto" for default timestamped file, NULL for no export, or custom path)
simple_batch_process_csv <- function(csv_file, 
                                   physician_name_column,
                                   npi_column = "npi", 
                                   num_cores = 4,
                                   chunk_size = 10,
                                   verbose = TRUE,
                                   export_file_path = "auto") {
  
  # Handle automatic export file path generation
  if (!is.null(export_file_path) && export_file_path == "auto") {
    timestamp <- format(Sys.time(), "%Y_%m_%d_%H%M%S")
    base_name <- tools::file_path_sans_ext(basename(csv_file))
    export_file_path <- file.path("data/03-search_and_process_npi/intermediate", 
                                  paste0("simple_batch_", base_name, "_", timestamp, ".csv"))
  }
  
  if (verbose) {
    cat("🚀 SIMPLE BATCH PROCESSING\n")
    cat("==========================\n")
    cat(sprintf("File: %s\n", csv_file))
    cat(sprintf("Name column: %s\n", physician_name_column))
    cat(sprintf("NPI column: %s\n", npi_column))
    cat(sprintf("Cores: %d\n", num_cores))
    if (!is.null(export_file_path)) {
      cat(sprintf("Export path: %s\n", export_file_path))
    }
  }
  
  # Read CSV
  physician_data <- read.csv(csv_file, stringsAsFactors = FALSE, na.strings = c("", "NA"))
  
  if (verbose) {
    cat(sprintf("✅ Loaded %d physicians\n", nrow(physician_data)))
  }
  
  # Check required column exists
  if (!physician_name_column %in% names(physician_data)) {
    stop(sprintf("Column '%s' not found! Available: %s", 
                 physician_name_column, paste(names(physician_data), collapse = ", ")))
  }
  
  # Create standardized data
  standardized_data <- data.frame(
    Provider_Name = physician_data[[physician_name_column]],
    stringsAsFactors = FALSE
  )
  
  if (verbose) {
    cat(sprintf("📋 Column '%s' contains %d values\n", physician_name_column, length(standardized_data$Provider_Name)))
    cat(sprintf("📋 Non-empty values: %d\n", sum(!is.na(standardized_data$Provider_Name) & standardized_data$Provider_Name != "")))
    cat("📋 First few values:\n")
    print(head(standardized_data$Provider_Name, 5))
    cat("\n")
  }
  
  # Add NPI column if available
  if (!is.null(npi_column) && npi_column %in% names(physician_data)) {
    standardized_data$NPI_Number <- as.character(physician_data[[npi_column]])
    if (verbose) cat(sprintf("✅ Added expected NPIs from: %s\n", npi_column))
  }
  
  # Remove any rows with missing names
  complete_rows <- !is.na(standardized_data$Provider_Name) & standardized_data$Provider_Name != ""
  
  if (verbose) {
    cat(sprintf("📋 Complete rows: %d out of %d\n", sum(complete_rows), length(complete_rows)))
  }
  
  standardized_data <- standardized_data[complete_rows, , drop = FALSE]
  
  # Get the number of rows safely
  n_rows <- nrow(standardized_data)
  if (is.null(n_rows) || length(n_rows) == 0) {
    n_rows <- 0
  }
  
  if (verbose) {
    cat(sprintf("✅ Processing %d physicians with complete names\n", n_rows))
    cat("\n")
  }
  
  # Check if we have any data to process
  if (n_rows == 0) {
    warning("No physicians with complete names found. Check your physician_name_column and data.")
    return(list(
      results = data.frame(),
      summary = list(
        total_physicians = 0,
        total_results = 0,
        unique_physicians = 0,
        processing_time = 0,
        chunks_processed = 0,
        error = "No physicians with complete names found"
      )
    ))
  }
  
  # Set up parallel processing
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  on.exit({
    stopCluster(cl)
    registerDoSEQ()
  })
  
  # Create chunks
  n_physicians <- n_rows  # Use the safely calculated n_rows
  n_chunks <- ceiling(n_physicians / chunk_size)
  
  if (verbose) {
    cat(sprintf("📦 Processing %d chunks of %d physicians each\n", n_chunks, chunk_size))
    cat("🔍 Starting parallel processing with progress monitoring...\n")
    cat("⏱️  Estimated time: ~%.1f minutes\n", (n_physicians * 6) / 60)  # 6 sec per physician estimate
    cat("\n")
  }
  
  start_time <- Sys.time()
  
  # Create progress tracking
  progress_dir <- tempdir()
  progress_pattern <- file.path(progress_dir, "chunk_*.done")
  
  # Clear any existing progress files
  existing_files <- list.files(progress_dir, pattern = "chunk_.*\\.done", full.names = TRUE)
  if (length(existing_files) > 0) file.remove(existing_files)
  
  # Start parallel processing in the background and monitor progress
  if (verbose) {
    cat("🚀 PARALLEL PROCESSING STARTED - Monitoring progress...\n")
    cat("==========================================\n")
  }
  
  # Start the parallel job
  all_results <- foreach(chunk_idx = 1:n_chunks, 
                        .combine = rbind,
                        .packages = c("humaniformat", "stringdist", "dplyr", "DBI"),
                        .errorhandling = "pass") %dopar% {
    
    tryCatch({
      # Source functions needed in parallel worker
      source('R/03.1-real_database_integration.R')
      
      # Define %||% operator for parallel workers
      `%||%` <- function(x, y) if (is.null(x)) y else x
      
      # Get chunk data
      start_row <- (chunk_idx - 1) * chunk_size + 1
      end_row <- min(chunk_idx * chunk_size, n_physicians)
      chunk_data <- standardized_data[start_row:end_row, , drop = FALSE]
    
    # Log chunk start (will appear when chunk completes)
    chunk_start_time <- Sys.time()
    
    # Initialize chunk results with standard structure
    chunk_results <- data.frame(
      original_name = character(0),
      expected_npi = character(0),
      chunk_id = integer(0),
      physician_id = integer(0),
      first_name = character(0),
      middle_name = character(0),
      last_name = character(0),
      npi = character(0),
      database_source = character(0),
      search_variation = character(0),
      stringsAsFactors = FALSE
    )
    
    for (i in 1:nrow(chunk_data)) {
      physician <- chunk_data[i, , drop = FALSE]  # Keep as data frame
      full_name <- physician$Provider_Name[1]     # Extract first element
      expected_npi <- if("NPI_Number" %in% names(physician)) physician$NPI_Number[1] else NA
      
      # Parse name
      clean_name <- gsub(',\\s*(MD|DO|PhD).*$', '', full_name, ignore.case = TRUE)
      clean_name <- trimws(clean_name)
      
      tryCatch({
        parsed <- humaniformat::parse_names(clean_name)
        first_name <- parsed$first_name[1]
        middle_name <- ifelse(is.na(parsed$middle_name[1]) | parsed$middle_name[1] == "", "", parsed$middle_name[1])
        last_name <- parsed$last_name[1]
        
        cat(sprintf("   📝 Parsed: '%s' '%s' '%s'\n", first_name, middle_name, last_name))
        
        # Search databases
        cat(sprintf("   🔎 Searching databases for %s %s...\n", first_name, last_name))
        search_results <- search_real_databases(
          first_name = first_name,
          last_name = last_name,
          middle_name = middle_name,
          max_results_per_db = 3,
          search_variations = TRUE
        )
        
        # Process results
        if (!is.null(search_results) && nrow(search_results) > 0) {
          cat(sprintf("   ✅ FOUND %d matches across databases!\n", nrow(search_results)))
          
          # Check for expected NPI match
          if (!is.na(expected_npi)) {
            npi_match <- any(search_results$npi == expected_npi, na.rm = TRUE)
            if (npi_match) {
              cat(sprintf("   🎯 EXPECTED NPI MATCH: %s\n", expected_npi))
            } else {
              found_npis <- unique(search_results$npi[!is.na(search_results$npi)])
              cat(sprintf("   ⚠️  Expected NPI not found. Found: %s\n", paste(found_npis[1:min(3, length(found_npis))], collapse = ", ")))
            }
          }
          
          # Standardize search results to match chunk_results structure
          for (j in 1:nrow(search_results)) {
            result_row <- data.frame(
              original_name = full_name,
              expected_npi = expected_npi,
              chunk_id = chunk_idx,
              physician_id = start_row + i - 1,
              first_name = if("first_name" %in% names(search_results)) search_results$first_name[j] else first_name,
              middle_name = if("middle_name" %in% names(search_results)) search_results$middle_name[j] else middle_name,
              last_name = if("last_name" %in% names(search_results)) search_results$last_name[j] else last_name,
              npi = if("npi" %in% names(search_results)) search_results$npi[j] else "",
              database_source = if("database_source" %in% names(search_results)) search_results$database_source[j] else "",
              search_variation = if("search_variation" %in% names(search_results)) search_results$search_variation[j] else "",
              stringsAsFactors = FALSE
            )
            chunk_results <- rbind(chunk_results, result_row)
          }
        } else {
          cat(sprintf("   ❌ No matches found in any database\n"))
          # Add row for no matches (exact same structure)
          no_match <- data.frame(
            original_name = full_name,
            expected_npi = expected_npi,
            chunk_id = chunk_idx,
            physician_id = start_row + i - 1,
            first_name = first_name,
            middle_name = middle_name,
            last_name = last_name,
            npi = "",
            database_source = "NO_MATCHES",
            search_variation = "",
            stringsAsFactors = FALSE
          )
          chunk_results <- rbind(chunk_results, no_match)
        }
        
      }, error = function(e) {
        cat(sprintf("   💥 ERROR processing %s: %s\n", full_name, e$message))
        error_row <- data.frame(
          original_name = full_name,
          expected_npi = expected_npi,
          chunk_id = chunk_idx,
          physician_id = start_row + i - 1,
          first_name = "",
          middle_name = "",
          last_name = "",
          npi = "",
          database_source = paste("ERROR:", e$message),
          search_variation = "",
          stringsAsFactors = FALSE
        )
        chunk_results <- rbind(chunk_results, error_row)
      })
      
      cat(sprintf("   ⏱️  Physician %d/%d complete\n", i, nrow(chunk_data)))
    }
    
    # Add chunk completion info
    chunk_end_time <- Sys.time()
    chunk_time <- difftime(chunk_end_time, chunk_start_time, units = "secs")
    
    # Write progress file to signal completion
    progress_file <- file.path(progress_dir, paste0("chunk_", chunk_idx, ".done"))
    writeLines(sprintf("Chunk %d completed with %d results in %.1f seconds", 
                       chunk_idx, nrow(chunk_results), as.numeric(chunk_time)), 
               progress_file)
    
      return(chunk_results)
      
    }, error = function(e) {
      # Return error information for debugging
      error_result <- data.frame(
        original_name = paste("ERROR in chunk", chunk_idx),
        expected_npi = "",
        chunk_id = chunk_idx,
        physician_id = 0,
        first_name = "",
        middle_name = "", 
        last_name = "",
        npi = "",
        database_source = paste("CHUNK_ERROR:", e$message),
        search_variation = "",
        stringsAsFactors = FALSE
      )
      return(error_result)
    })
  }
  
  end_time <- Sys.time()
  total_time <- difftime(end_time, start_time, units = "secs")
  
  if (verbose) {
    cat(sprintf("\n⏱️  All chunks completed in %.1f seconds\n", as.numeric(total_time)))
  }
  
  # Generate summary
  summary_stats <- list(
    total_physicians = n_physicians,
    total_results = nrow(all_results),
    unique_physicians = length(unique(all_results$original_name)),
    processing_time = as.numeric(total_time),
    chunks_processed = n_chunks
  )
  
  # Calculate success metrics if NPI column was provided
  if ("expected_npi" %in% names(all_results) && any(!is.na(all_results$expected_npi))) {
    matched_npis <- sum(!is.na(all_results$npi) & 
                        !is.na(all_results$expected_npi) & 
                        all_results$npi == all_results$expected_npi)
    
    physicians_with_expected <- length(unique(all_results$expected_npi[!is.na(all_results$expected_npi)]))
    
    summary_stats$expected_npis_found <- matched_npis
    summary_stats$physicians_with_expected <- physicians_with_expected
    summary_stats$npi_match_rate <- (matched_npis / physicians_with_expected) * 100
  }
  
  if (verbose) {
    cat("\n🎯 SIMPLE BATCH PROCESSING COMPLETE!\n")
    cat("====================================\n")
    cat(sprintf("Physicians processed: %d\n", summary_stats$total_physicians))
    cat(sprintf("Total results found: %d\n", summary_stats$total_results))
    cat(sprintf("Processing time: %.1f seconds\n", summary_stats$processing_time))
    
    if (!is.null(summary_stats$npi_match_rate)) {
      cat(sprintf("NPI match rate: %.1f%% (%d/%d)\n", 
                  summary_stats$npi_match_rate,
                  summary_stats$expected_npis_found,
                  summary_stats$physicians_with_expected))
    }
  }
  
  # Export results if file path is provided
  if (!is.null(export_file_path)) {
    tryCatch({
      # Create directory if it doesn't exist
      export_dir <- dirname(export_file_path)
      if (!dir.exists(export_dir)) {
        dir.create(export_dir, recursive = TRUE)
        if (verbose) cat(sprintf("📁 Created directory: %s\n", export_dir))
      }
      
      # Write CSV file
      write.csv(all_results, export_file_path, row.names = FALSE)
      
      if (verbose) {
        cat(sprintf("💾 Results exported to: %s\n", export_file_path))
        cat(sprintf("📊 Exported %d rows\n", nrow(all_results)))
      }
      
    }, error = function(e) {
      warning(sprintf("Failed to export results to %s: %s", export_file_path, e$message))
    })
  }
  
  return(list(
    results = all_results,
    summary = summary_stats,
    export_file_path = export_file_path
  ))
}

#' Export simple batch results
#' @param batch_results Results from simple_batch_process_csv
#' @param output_dir Directory to save files
#' @param file_prefix Prefix for output files
export_simple_results <- function(batch_results, output_dir = ".", file_prefix = "simple_batch") {
  
  results_file <- file.path(output_dir, paste0(file_prefix, "_results.csv"))
  write.csv(batch_results$results, results_file, row.names = FALSE)
  
  cat(sprintf("📁 Results exported to: %s\n", results_file))
  
  return(results_file)
}

#' Generate detailed validation report comparing input vs output
#' @param input_file Path to original input CSV
#' @param input_name_column Column name used for physician names
#' @param output_results Processed results dataframe  
#' @param middle_name_summary Middle name processing summary
#' @param batch_summary Batch processing summary
#' @param verbose Show progress
#' @return List containing validation metrics and analysis
generate_validation_report <- function(input_file, input_name_column, output_results, 
                                     middle_name_summary, batch_summary, verbose = FALSE) {
  
  if (verbose) cat("📊 Generating validation report...\n")
  
  # Read original input data
  input_data <- read.csv(input_file, stringsAsFactors = FALSE, na.strings = c("", "NA"))
  
  # Basic counts
  input_count <- nrow(input_data)
  output_count <- nrow(output_results)
  unique_input_names <- length(unique(input_data[[input_name_column]]))
  unique_output_names <- length(unique(output_results$original_name))
  
  # NPI Analysis
  npis_found <- sum(!is.na(output_results$npi) & output_results$npi != "", na.rm = TRUE)
  unique_npis_found <- length(unique(output_results$npi[!is.na(output_results$npi) & output_results$npi != ""]))
  no_matches <- sum(output_results$database_source == "NO_MATCHES", na.rm = TRUE)
  
  # Database source analysis
  db_sources <- table(output_results$database_source, useNA = "ifany")
  
  # Name parsing success analysis
  parsed_successfully <- sum(!is.na(output_results$first_name) & output_results$first_name != "", na.rm = TRUE)
  parsing_errors <- sum(grepl("ERROR", output_results$database_source, ignore.case = TRUE), na.rm = TRUE)
  
  # Middle name analysis (if available)
  middle_name_stats <- NULL
  if ("middle_name" %in% names(output_results)) {
    middle_names_present <- sum(!is.na(output_results$middle_name) & 
                               output_results$middle_name != "", na.rm = TRUE)
    middle_name_stats <- list(
      total_with_middle_names = middle_names_present,
      percentage_with_middle_names = round((middle_names_present / output_count) * 100, 1)
    )
  }
  
  # Expected NPI validation (if available)
  expected_npi_stats <- NULL
  if ("expected_npi" %in% names(output_results)) {
    expected_npis_provided <- sum(!is.na(output_results$expected_npi) & 
                                 output_results$expected_npi != "", na.rm = TRUE)
    if (expected_npis_provided > 0) {
      expected_matches <- sum(!is.na(output_results$npi) & 
                             !is.na(output_results$expected_npi) & 
                             output_results$npi == output_results$expected_npi, na.rm = TRUE)
      expected_npi_stats <- list(
        total_expected_npis = expected_npis_provided,
        matching_expected_npis = expected_matches,
        expected_match_rate = round((expected_matches / expected_npis_provided) * 100, 1)
      )
    }
  }
  
  # Data quality issues
  quality_issues <- list()
  
  # Check for duplicate NPIs with different names
  if (unique_npis_found > 0) {
    npi_name_combos <- output_results %>%
      filter(!is.na(npi) & npi != "" & !is.na(original_name)) %>%
      select(npi, original_name) %>%
      distinct()
    
    duplicate_npis <- npi_name_combos %>%
      group_by(npi) %>%
      filter(n() > 1) %>%
      arrange(npi)
    
    if (nrow(duplicate_npis) > 0) {
      quality_issues$duplicate_npis <- duplicate_npis
    }
  }
  
  # Check for duplicate names with different NPIs
  name_npi_combos <- output_results %>%
    filter(!is.na(npi) & npi != "" & !is.na(original_name)) %>%
    select(original_name, npi) %>%
    distinct()
  
  duplicate_names <- name_npi_combos %>%
    group_by(original_name) %>%
    filter(n() > 1) %>%
    arrange(original_name)
  
  if (nrow(duplicate_names) > 0) {
    quality_issues$duplicate_names <- duplicate_names
  }
  
  # Success rates
  overall_success_rate <- round(((output_count - no_matches) / output_count) * 100, 1)
  npi_success_rate <- round((npis_found / output_count) * 100, 1)
  parsing_success_rate <- round((parsed_successfully / output_count) * 100, 1)
  
  # Compile report
  report <- list(
    input_analysis = list(
      total_input_records = input_count,
      unique_input_names = unique_input_names,
      input_file = input_file,
      name_column_used = input_name_column
    ),
    output_analysis = list(
      total_output_records = output_count,
      unique_output_names = unique_output_names,
      records_with_npis = npis_found,
      unique_npis_found = unique_npis_found,
      no_matches_found = no_matches
    ),
    success_rates = list(
      overall_success_rate = overall_success_rate,
      npi_success_rate = npi_success_rate,
      parsing_success_rate = parsing_success_rate
    ),
    database_sources = as.list(db_sources),
    middle_name_analysis = middle_name_stats,
    expected_npi_validation = expected_npi_stats,
    quality_issues = quality_issues,
    processing_summary = list(
      processing_time = batch_summary$processing_time,
      chunks_processed = batch_summary$chunks_processed
    ),
    generated_at = Sys.time()
  )
  
  if (verbose) cat("✅ Validation report generated\n")
  
  return(report)
}

#' Write validation report to text file
#' @param report Validation report from generate_validation_report
#' @param file_path Path to write the report
#' @param verbose Show progress
write_validation_report <- function(report, file_path, verbose = FALSE) {
  
  if (verbose) cat(sprintf("📄 Writing validation report to: %s\n", file_path))
  
  # Create directory if needed
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
  
  sink(file_path)
  
  cat("NPI BATCH PROCESSING VALIDATION REPORT\n")
  cat("=====================================\n")
  cat(sprintf("Generated: %s\n", format(report$generated_at, "%Y-%m-%d %H:%M:%S")))
  cat(sprintf("Input File: %s\n", report$input_analysis$input_file))
  cat(sprintf("Name Column: %s\n", report$input_analysis$name_column_used))
  cat("\n")
  
  cat("INPUT ANALYSIS\n")
  cat("--------------\n")
  cat(sprintf("Total input records: %s\n", format(report$input_analysis$total_input_records, big.mark = ",")))
  cat(sprintf("Unique input names: %s\n", format(report$input_analysis$unique_input_names, big.mark = ",")))
  cat("\n")
  
  cat("OUTPUT ANALYSIS\n") 
  cat("---------------\n")
  cat(sprintf("Total output records: %s\n", format(report$output_analysis$total_output_records, big.mark = ",")))
  cat(sprintf("Unique output names: %s\n", format(report$output_analysis$unique_output_names, big.mark = ",")))
  cat(sprintf("Records with NPIs: %s\n", format(report$output_analysis$records_with_npis, big.mark = ",")))
  cat(sprintf("Unique NPIs found: %s\n", format(report$output_analysis$unique_npis_found, big.mark = ",")))
  cat(sprintf("No matches found: %s\n", format(report$output_analysis$no_matches_found, big.mark = ",")))
  cat("\n")
  
  cat("SUCCESS RATES\n")
  cat("-------------\n")
  cat(sprintf("Overall success rate: %.1f%%\n", report$success_rates$overall_success_rate))
  cat(sprintf("NPI discovery rate: %.1f%%\n", report$success_rates$npi_success_rate))
  cat(sprintf("Name parsing success: %.1f%%\n", report$success_rates$parsing_success_rate))
  cat("\n")
  
  cat("DATABASE SOURCES\n")
  cat("----------------\n")
  for (source in names(report$database_sources)) {
    cat(sprintf("%s: %s records\n", source, format(report$database_sources[[source]], big.mark = ",")))
  }
  cat("\n")
  
  if (!is.null(report$middle_name_analysis)) {
    cat("MIDDLE NAME ANALYSIS\n")
    cat("--------------------\n")
    cat(sprintf("Records with middle names: %s (%.1f%%)\n", 
                format(report$middle_name_analysis$total_with_middle_names, big.mark = ","),
                report$middle_name_analysis$percentage_with_middle_names))
    cat("\n")
  }
  
  if (!is.null(report$expected_npi_validation)) {
    cat("EXPECTED NPI VALIDATION\n") 
    cat("-----------------------\n")
    cat(sprintf("Expected NPIs provided: %s\n", 
                format(report$expected_npi_validation$total_expected_npis, big.mark = ",")))
    cat(sprintf("Expected NPIs matched: %s\n", 
                format(report$expected_npi_validation$matching_expected_npis, big.mark = ",")))
    cat(sprintf("Expected NPI match rate: %.1f%%\n", 
                report$expected_npi_validation$expected_match_rate))
    cat("\n")
  }
  
  if (length(report$quality_issues) > 0) {
    cat("DATA QUALITY ISSUES\n")
    cat("-------------------\n")
    
    if (!is.null(report$quality_issues$duplicate_npis)) {
      cat(sprintf("NPIs with multiple names: %d\n", nrow(report$quality_issues$duplicate_npis)))
      cat("Sample duplicate NPIs:\n")
      print(head(report$quality_issues$duplicate_npis, 10))
      cat("\n")
    }
    
    if (!is.null(report$quality_issues$duplicate_names)) {
      cat(sprintf("Names with multiple NPIs: %d\n", nrow(report$quality_issues$duplicate_names)))  
      cat("Sample duplicate names:\n")
      print(head(report$quality_issues$duplicate_names, 10))
      cat("\n")
    }
  } else {
    cat("DATA QUALITY ISSUES\n")
    cat("-------------------\n")
    cat("No significant data quality issues detected.\n\n")
  }
  
  cat("PROCESSING SUMMARY\n")
  cat("------------------\n")
  cat(sprintf("Processing time: %.1f seconds\n", report$processing_summary$processing_time))
  cat(sprintf("Chunks processed: %d\n", report$processing_summary$chunks_processed))
  cat("\n")
  
  cat("END OF REPORT\n")
  
  sink()
  
  if (verbose) cat("✅ Validation report written successfully\n")
}

#' Print validation summary to console
#' @param report Validation report from generate_validation_report
print_validation_summary <- function(report) {
  cat("📊 VALIDATION SUMMARY\n")
  cat(sprintf("Input records: %s | Output records: %s\n", 
              format(report$input_analysis$total_input_records, big.mark = ","),
              format(report$output_analysis$total_output_records, big.mark = ",")))
  cat(sprintf("NPIs found: %s (%.1f%% success rate)\n", 
              format(report$output_analysis$records_with_npis, big.mark = ","),
              report$success_rates$npi_success_rate))
  cat(sprintf("Unique NPIs: %s | No matches: %s\n", 
              format(report$output_analysis$unique_npis_found, big.mark = ","),
              format(report$output_analysis$no_matches_found, big.mark = ",")))
  
  if (!is.null(report$expected_npi_validation)) {
    cat(sprintf("Expected NPI match rate: %.1f%%\n", 
                report$expected_npi_validation$expected_match_rate))
  }
  
  quality_issue_count <- length(report$quality_issues)
  if (quality_issue_count > 0) {
    cat(sprintf("⚠️  Data quality issues detected: %d types\n", quality_issue_count))
  } else {
    cat("✅ No significant data quality issues\n")
  }
}

#' Combined Batch Process with Middle Name Filling
#' @param input_csv_file Path to input CSV file to process
#' @param output_csv_file Path for output CSV file ("auto" for default timestamped file in intermediate dir, NULL for no export, or custom path)
#' @param physician_name_column Column name for physician names in input CSV (default: "original_input_name")
#' @param npi_column Column name for expected NPIs (optional)
#' @param num_cores Number of cores for parallel processing
#' @param chunk_size Number of physicians per processing chunk
#' @param verbose Show progress
#' @param fill_middle_names Whether to fill middle names with most informative values (default: TRUE)
#' @param preview_middle_changes Whether to show preview of middle name changes (default: TRUE)
#' @return List with results, summary, and export_file_path
simple_batch_process_with_middle_names <- function(input_csv_file, 
                                                 output_csv_file = "auto",
                                                 physician_name_column = "original_input_name",
                                                 npi_column = "npi",
                                                 num_cores = 4,
                                                 chunk_size = 10,
                                                 verbose = TRUE,
                                                 fill_middle_names = TRUE,
                                                 preview_middle_changes = TRUE) {
  
  if (verbose) {
    cat("🚀 COMBINED BATCH PROCESSING WITH MIDDLE NAME FILLING\n")
    cat("====================================================\n")
  }
  
  # Step 1: Run standard batch processing
  if (verbose) cat("📋 Step 1: Running NPI batch processing...\n")
  
  batch_results <- simple_batch_process_csv(
    csv_file = input_csv_file,
    physician_name_column = physician_name_column,
    npi_column = npi_column,
    num_cores = num_cores,
    chunk_size = chunk_size,
    verbose = verbose,
    export_file_path = NULL  # Don't export yet - we'll do it after middle name filling
  )
  
  # Step 2: Fill middle names if requested
  final_results <- batch_results$results
  middle_name_summary <- NULL
  
  if (fill_middle_names && nrow(final_results) > 0) {
    if (verbose) cat("\n📝 Step 2: Filling middle names with most informative values...\n")
    
    # Preview changes if requested
    if (preview_middle_changes) {
      middle_name_preview <- preview_middle_name_changes(
        data = final_results,
        npi_column = "npi",
        middle_name_column = "middle_name"
      )
      
      if (nrow(middle_name_preview) > 0) {
        if (verbose) {
          cat("🔍 Middle name changes preview:\n")
          print(middle_name_preview)
          cat("\n")
        }
        middle_name_summary <- list(
          npis_with_multiple_middle_names = nrow(middle_name_preview),
          middle_name_changes = middle_name_preview
        )
      } else {
        if (verbose) cat("✅ No middle name standardization needed\n")
        middle_name_summary <- list(
          npis_with_multiple_middle_names = 0,
          middle_name_changes = data.frame()
        )
      }
    }
    
    # Apply middle name filling
    final_results <- fill_best_middle_names(
      data = final_results,
      npi_column = "npi", 
      middle_name_column = "middle_name"
    )
    
    if (verbose) cat("✅ Middle names filled successfully\n")
  }
  
  # Step 3: Export final results if requested
  final_export_path <- NULL
  if (!is.null(output_csv_file)) {
    # Generate timestamp for all outputs
    timestamp <- format(Sys.time(), "%Y_%m_%d_%H%M%S")
    
    # Handle automatic file path generation with "filled" suffix
    if (!is.null(output_csv_file) && output_csv_file == "auto") {
      base_name <- tools::file_path_sans_ext(basename(input_csv_file))
      suffix <- if (fill_middle_names) "_filled" else ""
      final_export_path <- file.path("data/03-search_and_process_npi/intermediate", 
                                   paste0("simple_batch_", base_name, suffix, "_", timestamp, ".csv"))
    } else {
      # Add timestamp to custom file path
      file_dir <- dirname(output_csv_file)
      file_name <- tools::file_path_sans_ext(basename(output_csv_file))
      file_ext <- tools::file_ext(output_csv_file)
      if (file_ext == "") file_ext <- "csv"  # Default to csv if no extension
      
      final_export_path <- file.path(file_dir, paste0(file_name, "_", timestamp, ".", file_ext))
    }
    
    tryCatch({
      # Create directory if it doesn't exist
      export_dir <- dirname(final_export_path)
      if (!dir.exists(export_dir)) {
        dir.create(export_dir, recursive = TRUE)
        if (verbose) cat(sprintf("📁 Created directory: %s\n", export_dir))
      }
      
      # Write CSV file
      write.csv(final_results, final_export_path, row.names = FALSE)
      
      if (verbose) {
        cat(sprintf("💾 Final results exported to: %s\n", final_export_path))
        cat(sprintf("📊 Exported %d rows\n", nrow(final_results)))
      }
      
    }, error = function(e) {
      warning(sprintf("Failed to export final results to %s: %s", final_export_path, e$message))
    })
  }
  
  # Step 4: Generate detailed validation and summary report
  validation_report <- generate_validation_report(
    input_file = input_csv_file,
    input_name_column = physician_name_column,
    output_results = final_results,
    middle_name_summary = middle_name_summary,
    batch_summary = batch_results$summary,
    verbose = verbose
  )
  
  # Export validation report if we're exporting results
  if (!is.null(final_export_path)) {
    report_path <- sub("\\.csv$", "_validation_report.txt", final_export_path)
    write_validation_report(validation_report, report_path, verbose)
  }
  
  # Prepare final summary
  final_summary <- batch_results$summary
  final_summary$validation_report <- validation_report
  if (!is.null(middle_name_summary)) {
    final_summary$middle_name_processing <- middle_name_summary
  }
  
  if (verbose) {
    cat("\n🎯 COMBINED PROCESSING COMPLETE!\n")
    cat("=================================\n")
    print_validation_summary(validation_report)
    if (fill_middle_names && !is.null(middle_name_summary)) {
      cat(sprintf("NPIs with standardized middle names: %d\n", 
                  middle_name_summary$npis_with_multiple_middle_names))
    }
  }
  
  return(list(
    results = final_results,
    summary = final_summary,
    export_file_path = final_export_path
  ))
}

cat("✅ Simple Batch Processing System loaded!\n")
cat("🔧 Functions available:\n")
cat("   - simple_batch_process_csv() - Process CSV with minimal complexity\n")
cat("   - simple_batch_process_with_middle_names() - Combined processing with middle name filling & validation\n")
cat("   - export_simple_results() - Export results to CSV\n")
cat("   - generate_validation_report() - Generate detailed validation analysis\n")
cat("\n💡 Example usage:\n")
cat("   # Standard processing:\n")
cat("   results <- simple_batch_process_csv('input.csv', 'api_full_name')\n")
cat("   \n")
cat("   # Combined processing with middle name filling (all defaults):\n")
cat("   results <- simple_batch_process_with_middle_names('input.csv')\n")
cat("   \n")
cat("   # Combined processing with custom output path:\n")
cat("   results <- simple_batch_process_with_middle_names('input.csv', \n")
cat("                                                     'output/results.csv')\n")
cat("   \n")
cat("   # Combined processing with custom column name:\n")
cat("   results <- simple_batch_process_with_middle_names('input.csv', \n")
cat("                                                     physician_name_column = 'api_full_name')\n")
cat("   \n")
cat("   # Combined processing with no export:\n")
cat("   results <- simple_batch_process_with_middle_names('input.csv', \n")
cat("                                                     output_csv_file = NULL)\n\n")
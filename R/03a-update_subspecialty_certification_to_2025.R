###############################################################################
# update_goba_with_missing_subspecialists.R
#
# Purpose:
# This script updates the GOBA (Gynecologic Oncologist Board Analytics) dataset
# by identifying and integrating missing subspecialists who were recently certified
# or missed in the original scrape.
#
# The final goal is to support geospatial access studies (e.g., Desjardins et al., 2023),
# ensuring that all clinically active gynecologic oncologists, maternal-fetal medicine,
# reproductive endocrinology, and urogynecology subspecialists are captured.
#
# Input Files:
# - GOBA_Scrape_subspecialists_only.csv: The original scraped GOBA file from 2024.
#     > Source: 03-search_and_process_npi/
#     > Contents: Subspecialists verified via ABOG website scrape
#
# - combined_subspecialty_extractions.csv: Extracted data from PDF downloads of ABOG profiles.
#     > Source: 0-Download_and_extract_PDF.R output
#     > Purpose: Capture newly certified individuals not in GOBA, e.g., Dr. Marisa Moroney.
#
# Output File:
# - updated_GOBA_subspecialists.csv: A merged dataset that includes newly identified
#     subspecialists and ensures standardized structure for downstream accessibility analysis.
#
# Example usage:
# - Run this script after updating your PDF download extractions to ensure completeness.
#
# Requirements:
# - R packages: dplyr, readr, stringr, assertthat, logger
#
# Author: Tyler Muffly, MD
# Last Updated: 2025-08-04
###############################################################################

# Call main function with verbose logging and overwrite output file if exists
# updated_goba_basic <- update_goba_with_missing_subspecialists(
#   goba_file_path = "data/03-search_and_process_npi/GOBA_Scrape_subspecialists_only.csv",
#   combined_extractions_file_path = "data/03-search_and_process_npi/combined_subspecialty_extractions.csv",
#   output_file_path = "data/03-search_and_process_npi/updated_GOBA_subspecialists.csv",
#   # Optional: restrict to specific subspecialties only
#   # subspecialty_filter = c("Gynecologic Oncology", "Maternal-Fetal Medicine"),
#   verbose = TRUE
# )
# 
# ✅ Notes for Use in Your Project
# When to run: After you've scraped PDFs for newly certified physicians (e.g., new board-certified Gyn Oncs).
# 
# What it fixes: Ensures your spatial access maps and counts are current, accounting for new certs not in the original scrape.
# 
# Next steps: Feed the output (updated_GOBA_subspecialists.csv) into your geospatial pipeline (generate_isos.R, calculate_coverage.R, etc.).





#' Update GOBA Dataset with Missing Subspecialists
#'
#' This function updates the GOBA (Gynecologic Oncologist Board Analytics) 
#' dataset by identifying and adding subspecialists that are present in the 
#' combined subspecialty extractions but missing from the GOBA dataset. This 
#' function is designed to support geographic accessibility studies of 
#' gynecologic oncologists similar to Desjardins et al. (2023).
#'
#' @param goba_file_path Character string. Path to the GOBA CSV file containing
#'   existing subspecialist data. Must be a valid file path.
#' @param combined_extractions_file_path Character string. Path to the combined
#'   subspecialty extractions CSV file containing potential missing 
#'   subspecialists. Must be a valid file path.
#' @param output_file_path Character string. Path where the updated GOBA 
#'   dataset should be saved. If NULL, no file is written. Default is NULL.
#' @param subspecialty_filter Character vector. Subspecialties to include in 
#'   the update process. If NULL, all subspecialties are included. Default is 
#'   c("Gynecologic Oncology", "Maternal-Fetal Medicine", 
#'   "Reproductive Endocrinology", "Female Pelvic Medicine").
#' @param verbose Logical. If TRUE, detailed logging information is printed to
#'   console. If FALSE, only essential messages are shown. Default is TRUE.
#'
#' @return A data.frame containing the updated GOBA dataset with any missing
#'   subspecialists added from the combined extractions file.
#'
#' @examples
#' # Example 1: Basic usage with default subspecialty filter and verbose output
#' updated_goba_basic <- update_goba_with_missing_subspecialists(
#'   goba_file_path = "data/GOBA_Scrape_subspecialists_only.csv",
#'   combined_extractions_file_path = "data/combined_subspecialty_extractions.csv",
#'   output_file_path = "data/updated_GOBA_subspecialists.csv",
#'   subspecialty_filter = c("Gynecologic Oncology", "Maternal-Fetal Medicine"),
#'   verbose = TRUE
#' )
#'
#' # Example 2: Include all subspecialties with custom output path and logging
#' updated_goba_all <- update_goba_with_missing_subspecialists(
#'   goba_file_path = "data/GOBA_Scrape_subspecialists_only.csv",
#'   combined_extractions_file_path = "data/combined_subspecialty_extractions.csv",
#'   output_file_path = "output/comprehensive_subspecialists_database.csv",
#'   subspecialty_filter = NULL,
#'   verbose = TRUE
#' )
#'
#' # Example 3: Return dataset without saving file, minimal logging
#' updated_goba_memory <- update_goba_with_missing_subspecialists(
#'   goba_file_path = "data/GOBA_Scrape_subspecialists_only.csv",
#'   combined_extractions_file_path = "data/combined_subspecialty_extractions.csv",
#'   output_file_path = NULL,
#'   subspecialty_filter = c("Gynecologic Oncology"),
#'   verbose = FALSE
#' )
#'
#' @importFrom dplyr filter select mutate bind_rows anti_join distinct arrange
#' @importFrom readr read_csv write_csv
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom stringr str_trim str_to_title
#'
#' @export
update_goba_with_missing_subspecialists <- function(
    goba_file_path,
    combined_extractions_file_path,
    output_file_path = NULL,
    subspecialty_filter = c("Gynecologic Oncology", "Maternal-Fetal Medicine", 
                            "Reproductive Endocrinology", "Female Pelvic Medicine"),
    verbose = TRUE
) {
  
  # Input validation and logging setup
  setup_logging_and_validation(goba_file_path, combined_extractions_file_path, 
                               subspecialty_filter, verbose)
  
  # Load and process GOBA dataset
  goba_processed_data <- load_and_process_goba_data(goba_file_path, verbose)
  
  # Load and process combined extractions dataset
  combined_processed_data <- load_and_process_combined_data(
    combined_extractions_file_path, subspecialty_filter, verbose
  )
  
  # Identify missing subspecialists
  missing_subspecialists_data <- identify_missing_subspecialists(
    goba_processed_data, combined_processed_data, verbose
  )
  
  # Update GOBA dataset with missing subspecialists
  updated_goba_dataset <- merge_datasets_and_finalize(
    goba_processed_data, missing_subspecialists_data, verbose
  )
  
  # Save output file if path provided
  if (!is.null(output_file_path)) {
    save_updated_dataset(updated_goba_dataset, output_file_path, verbose)
  }
  
  # Final logging and return
  log_final_summary(updated_goba_dataset, missing_subspecialists_data, verbose)
  
  return(updated_goba_dataset)
}

#' Setup Logging and Input Validation
#' @noRd
setup_logging_and_validation <- function(goba_file_path, 
                                         combined_extractions_file_path,
                                         subspecialty_filter, verbose) {
  
  if (verbose) {
    logger::log_info("Starting GOBA subspecialist update process")
    logger::log_info("Input parameters:")
    logger::log_info("  GOBA file path: {goba_file_path}")
    logger::log_info("  Combined extractions file path: {combined_extractions_file_path}")
    logger::log_info("  Subspecialty filter: {paste(subspecialty_filter, collapse = ', ')}")
  }
  
  # Validate input parameters
  assertthat::assert_that(is.character(goba_file_path),
                          msg = "goba_file_path must be a character string")
  assertthat::assert_that(file.exists(goba_file_path),
                          msg = paste("GOBA file does not exist:", goba_file_path))
  
  assertthat::assert_that(is.character(combined_extractions_file_path),
                          msg = "combined_extractions_file_path must be a character string")
  assertthat::assert_that(file.exists(combined_extractions_file_path),
                          msg = paste("Combined extractions file does not exist:", 
                                      combined_extractions_file_path))
  
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be TRUE or FALSE")
  
  if (!is.null(subspecialty_filter)) {
    assertthat::assert_that(is.character(subspecialty_filter),
                            msg = "subspecialty_filter must be a character vector or NULL")
  }
  
  if (verbose) {
    logger::log_info("Input validation completed successfully")
  }
}

#' Load and Process GOBA Dataset
#' @noRd
load_and_process_goba_data <- function(goba_file_path, verbose) {
  
  if (verbose) {
    logger::log_info("Loading GOBA dataset from: {goba_file_path}")
  }
  
  goba_raw_data <- readr::read_csv(goba_file_path, show_col_types = FALSE)
  
  if (verbose) {
    logger::log_info("GOBA dataset loaded successfully")
    logger::log_info("  Original GOBA records: {nrow(goba_raw_data)}")
    logger::log_info("  GOBA columns: {ncol(goba_raw_data)}")
    logger::log_info("  GOBA column names: {paste(names(goba_raw_data), collapse = ', ')}")
  }
  
  assertthat::assert_that(nrow(goba_raw_data) > 0,
                          msg = "GOBA dataset is empty")
  
  # Process GOBA data - create standardized identifier
  goba_processed_data <- goba_raw_data %>%
    dplyr::mutate(
      standardized_physician_name = stringr::str_trim(
        paste(stringr::str_to_title(first_name), 
              stringr::str_to_title(last_name))
      ),
      data_source = "GOBA"
    )
  
  if (verbose) {
    logger::log_info("GOBA data processing completed")
    logger::log_info("  Processed GOBA records: {nrow(goba_processed_data)}")
  }
  
  return(goba_processed_data)
}

#' Load and Process Combined Extractions Dataset
#' @noRd
load_and_process_combined_data <- function(combined_extractions_file_path, 
                                           subspecialty_filter, verbose) {
  
  if (verbose) {
    logger::log_info("Loading combined extractions dataset from: {combined_extractions_file_path}")
  }
  
  combined_raw_data <- readr::read_csv(combined_extractions_file_path, 
                                       show_col_types = FALSE)
  
  if (verbose) {
    logger::log_info("Combined extractions dataset loaded successfully")
    logger::log_info("  Original combined extraction records: {nrow(combined_raw_data)}")
    logger::log_info("  Combined extractions columns: {ncol(combined_raw_data)}")
    logger::log_info("  Combined extractions column names: {paste(names(combined_raw_data), collapse = ', ')}")
  }
  
  assertthat::assert_that(nrow(combined_raw_data) > 0,
                          msg = "Combined extractions dataset is empty")
  
  # Apply subspecialty filter if provided
  if (!is.null(subspecialty_filter)) {
    combined_filtered_data <- combined_raw_data %>%
      dplyr::filter(subspecialty_name %in% subspecialty_filter)
    
    if (verbose) {
      logger::log_info("Applied subspecialty filter")
      logger::log_info("  Records after subspecialty filtering: {nrow(combined_filtered_data)}")
      logger::log_info("  Filtered subspecialties: {paste(unique(combined_filtered_data$subspecialty_name), collapse = ', ')}")
    }
  } else {
    combined_filtered_data <- combined_raw_data
    if (verbose) {
      logger::log_info("No subspecialty filter applied - including all records")
    }
  }
  
  # Process combined data - create standardized identifier and select relevant columns
  combined_processed_data <- combined_filtered_data %>%
    dplyr::filter(!is.na(physician_name)) %>%
    dplyr::mutate(
      standardized_physician_name = stringr::str_trim(physician_name),
      data_source = "Combined_Extractions"
    ) %>%
    dplyr::select(
      physician_id, abog_id_number, standardized_physician_name,
      clinically_active_status, primary_certification, subspecialty_name,
      subspecialty_cert_status, extraction_timestamp, data_source
    )
  
  if (verbose) {
    logger::log_info("Combined extractions data processing completed")
    logger::log_info("  Processed combined extraction records: {nrow(combined_processed_data)}")
    logger::log_info("  Unique subspecialties in processed data: {paste(unique(combined_processed_data$subspecialty_name), collapse = ', ')}")
  }
  
  return(combined_processed_data)
}

#' Identify Missing Subspecialists
#' @noRd
identify_missing_subspecialists <- function(goba_processed_data, 
                                            combined_processed_data, verbose) {
  
  if (verbose) {
    logger::log_info("Identifying missing subspecialists")
  }
  
  # Find subspecialists in combined extractions not in GOBA
  missing_subspecialists_data <- combined_processed_data %>%
    dplyr::anti_join(
      goba_processed_data,
      by = "standardized_physician_name"
    ) %>%
    dplyr::distinct(standardized_physician_name, .keep_all = TRUE)
  
  if (verbose) {
    logger::log_info("Missing subspecialists identification completed")
    logger::log_info("  Missing subspecialists found: {nrow(missing_subspecialists_data)}")
    
    if (nrow(missing_subspecialists_data) > 0) {
      missing_subspecialty_breakdown <- missing_subspecialists_data %>%
        dplyr::count(subspecialty_name, name = "missing_count")
      
      logger::log_info("  Missing subspecialists by specialty:")
      for (i in seq_len(nrow(missing_subspecialty_breakdown))) {
        specialty <- missing_subspecialty_breakdown$subspecialty_name[i]
        count <- missing_subspecialty_breakdown$missing_count[i]
        logger::log_info("    {specialty}: {count}")
      }
    }
  }
  
  return(missing_subspecialists_data)
}

#' Merge Datasets and Finalize
#' @noRd
merge_datasets_and_finalize <- function(goba_processed_data, 
                                        missing_subspecialists_data, verbose) {
  
  if (verbose) {
    logger::log_info("Merging datasets and finalizing updated GOBA dataset")
  }
  
  if (nrow(missing_subspecialists_data) > 0) {
    
    # Analyze GOBA data types for proper conversion
    goba_column_types <- sapply(goba_processed_data, class)
    if (verbose) {
      logger::log_info("Analyzing GOBA data types for proper conversion")
      logger::log_info("  Key data types detected:")
      logger::log_info("    startDate: {paste(goba_column_types[['startDate']], collapse = ', ')}")
      logger::log_info("    DateTime: {paste(goba_column_types[['DateTime']], collapse = ', ')}")
      logger::log_info("    NPI: {paste(goba_column_types[['NPI']], collapse = ', ')}")
    }
    
    # Create base structure from combined extractions
    missing_for_merge <- missing_subspecialists_data %>%
      dplyr::mutate(
        # Map fields from combined extractions to GOBA structure
        first_name = stringr::str_extract(standardized_physician_name, "^\\S+"),
        last_name = stringr::str_extract(standardized_physician_name, "\\S+$"),
        sub1 = subspecialty_name,
        clinicallyActive = clinically_active_status,
        sub1certStatus = subspecialty_cert_status,
        DateTime = extraction_timestamp,
        certStatus = primary_certification,
        Input.name = standardized_physician_name
      )
    
    # Add all remaining GOBA columns with proper data types
    goba_column_names <- names(goba_processed_data)
    for (col_name in goba_column_names) {
      if (!col_name %in% names(missing_for_merge)) {
        goba_col_type <- class(goba_processed_data[[col_name]])[1]
        missing_for_merge[[col_name]] <- switch(goba_col_type,
                                                "character" = as.character(NA),
                                                "numeric" = as.numeric(NA),
                                                "integer" = as.integer(NA),
                                                "logical" = as.logical(NA),
                                                "Date" = as.Date(NA),
                                                "POSIXct" = as.POSIXct(NA),
                                                "POSIXt" = as.POSIXct(NA),
                                                as.character(NA)  # Default fallback
        )
        
        if (verbose && col_name %in% c("startDate", "sub1startDate", "DateTime")) {
          logger::log_info("    Set {col_name} as {goba_col_type} type")
        }
      }
    }
    
    # Ensure all columns match GOBA structure
    goba_column_names <- names(goba_processed_data)
    missing_columns_in_new_data <- setdiff(goba_column_names, names(missing_for_merge))
    
    if (length(missing_columns_in_new_data) > 0) {
      if (verbose) {
        logger::log_info("Adding missing columns to new data: {paste(missing_columns_in_new_data, collapse = ', ')}")
      }
      
      # Add missing columns with appropriate NA values
      for (col_name in missing_columns_in_new_data) {
        goba_col_type <- class(goba_processed_data[[col_name]])[1]
        missing_for_merge[[col_name]] <- switch(goba_col_type,
                                                "character" = as.character(NA),
                                                "numeric" = as.numeric(NA),
                                                "integer" = as.integer(NA),
                                                "logical" = as.logical(NA),
                                                "Date" = as.Date(NA),
                                                "POSIXct" = as.POSIXct(NA),
                                                "POSIXt" = as.POSIXct(NA),
                                                as.character(NA)  # Default fallback
        )
      }
    }
    
    # Select only columns that exist in GOBA and ensure same order
    missing_for_merge <- missing_for_merge %>%
      dplyr::select(dplyr::all_of(goba_column_names))
    
    if (verbose) {
      logger::log_info("Data type conversion completed")
      logger::log_info("  Columns in missing data: {ncol(missing_for_merge)}")
      logger::log_info("  Columns in GOBA data: {ncol(goba_processed_data)}")
    }
    
    # Bind rows to create updated dataset
    updated_goba_dataset <- goba_processed_data %>%
      dplyr::bind_rows(missing_for_merge) %>%
      dplyr::distinct() %>%
      dplyr::arrange(last_name, first_name)
    
    if (verbose) {
      logger::log_info("Dataset merging completed successfully")
      logger::log_info("  Records added to GOBA: {nrow(missing_subspecialists_data)}")
    }
  } else {
    updated_goba_dataset <- goba_processed_data
    if (verbose) {
      logger::log_info("No missing subspecialists found - original GOBA dataset unchanged")
    }
  }
  
  if (verbose) {
    logger::log_info("Final dataset preparation completed")
    logger::log_info("  Total records in updated dataset: {nrow(updated_goba_dataset)}")
  }
  
  return(updated_goba_dataset)
}

#' Save Updated Dataset
#' @noRd
save_updated_dataset <- function(updated_goba_dataset, output_file_path, verbose) {
  
  if (verbose) {
    logger::log_info("Saving updated dataset to: {output_file_path}")
  }
  
  # Create directory if it doesn't exist
  output_directory <- dirname(output_file_path)
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
    if (verbose) {
      logger::log_info("Created output directory: {output_directory}")
    }
  }
  
  readr::write_csv(updated_goba_dataset, output_file_path)
  
  if (verbose) {
    logger::log_info("Updated dataset saved successfully")
    logger::log_info("  Output file path: {output_file_path}")
    logger::log_info("  File size: {file.size(output_file_path)} bytes")
  }
}

#' Log Final Summary
#' @noRd
log_final_summary <- function(updated_goba_dataset, missing_subspecialists_data, 
                              verbose) {
  
  if (verbose) {
    logger::log_info("=== FINAL SUMMARY ===")
    logger::log_info("GOBA subspecialist update process completed successfully")
    logger::log_info("  Total records in final dataset: {nrow(updated_goba_dataset)}")
    logger::log_info("  New subspecialists added: {nrow(missing_subspecialists_data)}")
    
    if (nrow(missing_subspecialists_data) > 0) {
      improvement_percentage <- round(
        (nrow(missing_subspecialists_data) / 
           (nrow(updated_goba_dataset) - nrow(missing_subspecialists_data))) * 100, 
        2
      )
      logger::log_info("  Dataset size improvement: {improvement_percentage}%")
    }
    
    logger::log_info("Process completed at: {Sys.time()}")
  }
}

# run ----
updated_goba_basic <- update_goba_with_missing_subspecialists(
  goba_file_path = "data/03-search_and_process_npi/GOBA_Scrape_subspecialists_only.csv",  # This is the original 2024 GOBA data.  
  combined_extractions_file_path = "data/03-search_and_process_npi/combined_subspecialty_extractions.csv", # This comes from 0-Download and extract PDF.R so that we can get people who were recently board-certified like Marisa Moroney
  output_file_path = "data/03-search_and_process_npi/updated_GOBA_subspecialists.csv",
  #subspecialty_filter = c("Gynecologic Oncology", "Maternal-Fetal Medicine"),
  verbose = TRUE
)

#read_csv("data/03-search_and_process_npi/updated_GOBA_subspecialists.csv")

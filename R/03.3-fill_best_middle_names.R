#!/usr/bin/env Rscript

# Fill Middle Names with Most Informative Values
# Takes grouped data by NPI and fills the middle name column with the longest/most complete value
# Author: Tyler & Claude  
# Created: 2025-08-24

library(dplyr)
library(purrr)

#' Fill middle names with most informative value for each NPI group
#' @param data Data frame containing NPI records
#' @param npi_column Name of the NPI column (default: "npi")  
#' @param middle_name_column Name of the middle name column (default: "provider_middle_name")
#' @return Data frame with middle names filled with most informative values
fill_best_middle_names <- function(data, npi_column = "npi", middle_name_column = "provider_middle_name") {
  
  # Input validation
  if (!npi_column %in% names(data)) {
    stop(paste("Column", npi_column, "not found in data"))
  }
  if (!middle_name_column %in% names(data)) {
    stop(paste("Column", middle_name_column, "not found in data"))
  }
  
  # Group by NPI and determine the best middle name for each group
  best_middle_names <- data %>%
    # Group by NPI
    group_by(!!sym(npi_column)) %>%
    # Get all unique middle names for this NPI (excluding NA and empty strings)
    summarise(
      all_middle_names = list(unique(.data[[middle_name_column]][
        !is.na(.data[[middle_name_column]]) & 
        trimws(.data[[middle_name_column]]) != ""
      ])),
      .groups = "drop"
    ) %>%
    # Select the best middle name using our ranking criteria
    mutate(
      best_middle_name = map_chr(all_middle_names, function(names_list) {
        if (length(names_list) == 0) {
          # No valid middle names found
          return(NA_character_)
        } else if (length(names_list) == 1) {
          # Only one valid middle name
          return(names_list[1])
        } else {
          # Multiple middle names - pick the most informative one
          # Rank by: 1) Length (longer is better), 2) Alphabetically (for consistency)
          ordered_candidates <- names_list[order(-nchar(names_list), names_list)]
          return(ordered_candidates[1])
        }
      })
    ) %>%
    select(!!sym(npi_column), best_middle_name)
  
  # Join back to original data and update middle name column
  result <- data %>%
    left_join(best_middle_names, by = npi_column) %>%
    mutate(
      !!sym(middle_name_column) := coalesce(best_middle_name, .data[[middle_name_column]])
    ) %>%
    select(-best_middle_name)
  
  return(result)
}

#' Preview which middle names will be changed
#' @param data Data frame containing NPI records  
#' @param npi_column Name of the NPI column (default: "npi")
#' @param middle_name_column Name of the middle name column (default: "provider_middle_name")
#' @return Data frame showing NPIs with multiple middle names and which will be selected
preview_middle_name_changes <- function(data, npi_column = "npi", middle_name_column = "provider_middle_name") {
  
  # Find NPIs with multiple different middle names
  multiple_middle_names <- data %>%
    group_by(!!sym(npi_column)) %>%
    summarise(
      unique_middle_names = list(unique(.data[[middle_name_column]][
        !is.na(.data[[middle_name_column]]) & 
        trimws(.data[[middle_name_column]]) != ""
      ])),
      count = n(),
      .groups = "drop"
    ) %>%
    # Only keep NPIs with multiple different middle names
    filter(lengths(unique_middle_names) > 1) %>%
    mutate(
      current_names = map_chr(unique_middle_names, ~ paste(.x, collapse = " | ")),
      selected_name = map_chr(unique_middle_names, function(names_list) {
        ordered_candidates <- names_list[order(-nchar(names_list), names_list)]
        ordered_candidates[1]
      })
    ) %>%
    select(!!sym(npi_column), current_names, selected_name, count)
  
  return(multiple_middle_names)
}

# Example usage and testing
if (FALSE) {
  # Create test data similar to your structure
  test_data <- data.frame(
    npi = c("1700146826", "1700146826", "1700146826", "1234567890", "1234567890"),
    provider_first_name = c("CAROL", "CAROL", "CAROL", "JOHN", "JOHN"),
    provider_last_name = c("BRETSCHNEIDER", "BRETSCHNEIDER", "BRETSCHNEIDER", "SMITH", "SMITH"),
    provider_middle_name = c("E", "EMI", NA, "J", "JAMES"),
    stringsAsFactors = FALSE
  )
  
  # Preview changes
  cat("=== Preview of Middle Name Changes ===\n")
  preview_changes <- preview_middle_name_changes(test_data)
  print(preview_changes)
  
  # Apply changes
  cat("\n=== Before Filling ===\n")
  print(test_data)
  
  filled_data <- fill_best_middle_names(test_data)
  
  cat("\n=== After Filling ===\n") 
  print(filled_data)
}

cat("✅ Middle name filling functions loaded successfully!\n")
cat("📋 Functions available:\n")
cat("   - fill_best_middle_names() - Fill middle names with most informative values\n")
cat("   - preview_middle_name_changes() - Preview which changes will be made\n\n")
cat("🎯 Usage example:\n")
cat("   preview_changes <- preview_middle_name_changes(your_data)\n")
cat("   filled_data <- fill_best_middle_names(your_data)\n")
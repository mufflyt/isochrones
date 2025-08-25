#!/usr/bin/env Rscript

# Robust Processing System with Edge Case Handling and Reproducibility
# Handles empty datasets, unexpected inputs, and ensures reproducible results
# Author: Tyler & Claude
# Created: 2025-08-24

# Set reproducibility seed
set.seed(1978)

#' Record Package Versions for Reproducibility
#' Captures all loaded package versions
#' 
#' @param save_to_file Whether to save versions to file (default: TRUE)
#' @param output_dir Directory for saving version info (default: "logs")
#' @return Data frame of package versions
record_package_versions <- function(save_to_file = TRUE, output_dir = "logs") {
  
  cat("📦 RECORDING PACKAGE VERSIONS FOR REPRODUCIBILITY\n")
  cat("=================================================\n")
  
  # Get session info
  session_info <- utils::sessionInfo()
  
  # Extract package information
  base_packages <- session_info$basePkgs
  other_packages <- session_info$otherPkgs
  loaded_packages <- session_info$loadedOnly
  
  # Create comprehensive package list
  package_versions <- data.frame()
  
  # Base packages
  for (pkg in base_packages) {
    package_versions <- rbind(package_versions, data.frame(
      package = pkg,
      version = as.character(utils::packageVersion(pkg)),
      type = "base",
      stringsAsFactors = FALSE
    ))
  }
  
  # Attached packages  
  if (!is.null(other_packages)) {
    for (pkg_name in names(other_packages)) {
      pkg_info <- other_packages[[pkg_name]]
      package_versions <- rbind(package_versions, data.frame(
        package = pkg_name,
        version = pkg_info$Version %||% "unknown",
        type = "attached",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Loaded only packages
  if (!is.null(loaded_packages)) {
    for (pkg_name in names(loaded_packages)) {
      pkg_info <- loaded_packages[[pkg_name]]
      package_versions <- rbind(package_versions, data.frame(
        package = pkg_name,
        version = pkg_info$Version %||% "unknown", 
        type = "loaded",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Add R version and system info
  system_info <- data.frame(
    package = c("R", "Platform", "OS"),
    version = c(
      as.character(getRversion()),
      session_info$platform,
      Sys.info()["sysname"]
    ),
    type = "system",
    stringsAsFactors = FALSE
  )
  
  package_versions <- rbind(system_info, package_versions)
  
  # Add timestamp
  package_versions$recorded_at <- Sys.time()
  package_versions$seed_used <- 1978
  
  # Print summary
  cat(sprintf("📊 Recorded %s packages (%s base, %s attached, %s loaded only)\n",
              format_count(nrow(package_versions) - 3),
              format_count(length(base_packages)),
              format_count(length(other_packages)),
              format_count(length(loaded_packages))))
  
  cat(sprintf("🌱 Random seed set to: %d\n", 1978))
  cat(sprintf("💻 R version: %s\n", getRversion()))
  cat(sprintf("🖥️  Platform: %s\n", session_info$platform))
  
  # Save to file if requested
  if (save_to_file) {
    tryCatch({
      ensure_output_directory(output_dir)
      version_file <- generate_timestamped_filename(
        base_name = "package_versions",
        extension = "csv",
        output_dir = output_dir
      )
      readr::write_csv(package_versions, version_file)
      cat(sprintf("💾 Package versions saved to: %s\n", version_file))
    }, error = function(e) {
      warning(sprintf("Could not save package versions: %s", e$message))
    })
  }
  
  cat("\n")
  return(package_versions)
}

#' Robust Data Validation with Edge Case Handling
#' Validates data frames and handles edge cases gracefully
#' 
#' @param data Data frame to validate
#' @param required_columns Vector of required column names
#' @param data_name Name for error reporting (default: "Dataset")
#' @return List with validation results and cleaned data
robust_data_validation <- function(data, required_columns = NULL, data_name = "Dataset") {
  
  cat(sprintf("🔍 ROBUST VALIDATION: %s\n", data_name))
  cat(paste(rep("=", nchar(data_name) + 20), collapse = ""), "\n")
  
  validation_result <- list(
    is_valid = FALSE,
    data = data.frame(),
    issues = character(),
    warnings = character(),
    metadata = list()
  )
  
  # Edge Case 1: NULL or missing data
  if (is.null(data)) {
    validation_result$issues <- c(validation_result$issues, "Data is NULL")
    cat("❌ CRITICAL: Data is NULL\n")
    return(validation_result)
  }
  
  # Edge Case 2: Not a data frame
  if (!is.data.frame(data)) {
    validation_result$issues <- c(validation_result$issues, "Data is not a data frame")
    cat(sprintf("❌ CRITICAL: Data is %s, not data.frame\n", class(data)[1]))
    
    # Attempt conversion
    tryCatch({
      data <- as.data.frame(data)
      validation_result$warnings <- c(validation_result$warnings, "Converted to data frame")
      cat("⚠️  Attempted conversion to data.frame\n")
    }, error = function(e) {
      validation_result$issues <- c(validation_result$issues, paste("Conversion failed:", e$message))
      cat(sprintf("❌ Conversion failed: %s\n", e$message))
      return(validation_result)
    })
  }
  
  # Edge Case 3: Empty data frame
  if (nrow(data) == 0) {
    validation_result$issues <- c(validation_result$issues, "Data frame has 0 rows")
    cat("❌ CRITICAL: Empty data frame (0 rows)\n")
    return(validation_result)
  }
  
  # Edge Case 4: No columns
  if (ncol(data) == 0) {
    validation_result$issues <- c(validation_result$issues, "Data frame has 0 columns")
    cat("❌ CRITICAL: Data frame has no columns\n")
    return(validation_result)
  }
  
  # Basic metadata
  validation_result$metadata$original_rows <- nrow(data)
  validation_result$metadata$original_cols <- ncol(data)
  validation_result$metadata$column_names <- names(data)
  
  cat(sprintf("📊 Input data: %s rows, %s columns\n", 
              format_count(nrow(data)), format_count(ncol(data))))
  
  # Edge Case 5: Check required columns
  if (!is.null(required_columns)) {
    missing_cols <- setdiff(required_columns, names(data))
    if (length(missing_cols) > 0) {
      validation_result$issues <- c(validation_result$issues, 
                                   paste("Missing required columns:", paste(missing_cols, collapse = ", ")))
      cat(sprintf("❌ Missing required columns: %s\n", paste(missing_cols, collapse = ", ")))
      return(validation_result)
    } else {
      cat(sprintf("✅ All %s required columns present\n", format_count(length(required_columns))))
    }
  }
  
  # Edge Case 6: All NA columns
  na_columns <- sapply(data, function(x) all(is.na(x)))
  if (any(na_columns)) {
    na_col_names <- names(data)[na_columns]
    validation_result$warnings <- c(validation_result$warnings, 
                                   paste("Columns with all NA values:", paste(na_col_names, collapse = ", ")))
    cat(sprintf("⚠️  Columns with all NA: %s\n", paste(na_col_names, collapse = ", ")))
  }
  
  # Edge Case 7: Duplicate column names
  if (any(duplicated(names(data)))) {
    dup_names <- names(data)[duplicated(names(data))]
    validation_result$issues <- c(validation_result$issues, 
                                 paste("Duplicate column names:", paste(dup_names, collapse = ", ")))
    cat(sprintf("❌ Duplicate column names: %s\n", paste(dup_names, collapse = ", ")))
    return(validation_result)
  }
  
  # Edge Case 8: Check for unusual data types
  unusual_types <- c()
  for (col_name in names(data)) {
    col_class <- class(data[[col_name]])[1]
    if (!col_class %in% c("character", "numeric", "integer", "logical", "Date", "POSIXct", "POSIXt", "factor")) {
      unusual_types <- c(unusual_types, paste0(col_name, " (", col_class, ")"))
    }
  }
  
  if (length(unusual_types) > 0) {
    validation_result$warnings <- c(validation_result$warnings, 
                                   paste("Unusual data types:", paste(unusual_types, collapse = ", ")))
    cat(sprintf("⚠️  Unusual data types: %s\n", paste(unusual_types, collapse = ", ")))
  }
  
  # Edge Case 9: Memory size check
  data_size <- utils::object.size(data)
  size_mb <- as.numeric(data_size) / (1024^2)
  validation_result$metadata$size_mb <- size_mb
  
  if (size_mb > 100) {  # Warning for datasets > 100MB
    validation_result$warnings <- c(validation_result$warnings, 
                                   paste("Large dataset size:", format_number(size_mb, 1), "MB"))
    cat(sprintf("⚠️  Large dataset: %s MB\n", format_number(size_mb, 1)))
  }
  
  # Clean data of problematic rows
  cleaned_data <- data
  
  # Remove completely empty rows
  empty_rows <- apply(data, 1, function(x) all(is.na(x) | x == ""))
  if (any(empty_rows)) {
    cleaned_data <- cleaned_data[!empty_rows, ]
    removed_count <- sum(empty_rows)
    validation_result$warnings <- c(validation_result$warnings, 
                                   paste("Removed", removed_count, "completely empty rows"))
    cat(sprintf("🧹 Removed %s completely empty rows\n", format_count(removed_count)))
  }
  
  # Final validation
  if (nrow(cleaned_data) == 0) {
    validation_result$issues <- c(validation_result$issues, "No valid data remains after cleaning")
    cat("❌ CRITICAL: No valid data remains after cleaning\n")
    return(validation_result)
  }
  
  validation_result$is_valid <- TRUE
  validation_result$data <- cleaned_data
  validation_result$metadata$cleaned_rows <- nrow(cleaned_data)
  validation_result$metadata$rows_removed <- nrow(data) - nrow(cleaned_data)
  
  cat(sprintf("✅ Validation complete: %s valid rows remaining\n", 
              format_count(nrow(cleaned_data))))
  
  if (validation_result$metadata$rows_removed > 0) {
    cat(sprintf("🧹 Removed %s problematic rows\n", 
                format_count(validation_result$metadata$rows_removed)))
  }
  
  cat("\n")
  return(validation_result)
}

#' Safe Numeric Conversion with Edge Case Handling
#' Converts values to numeric while handling edge cases
#' 
#' @param x Vector to convert
#' @param default_value Default for failed conversions (default: NA)
#' @return Numeric vector with handled edge cases
safe_numeric_conversion <- function(x, default_value = NA) {
  
  if (length(x) == 0) return(numeric(0))
  
  # Handle different input types
  if (is.numeric(x)) return(x)
  
  if (is.factor(x)) {
    x <- as.character(x)
  }
  
  if (!is.character(x)) {
    x <- as.character(x)
  }
  
  # Clean common problematic values
  x_clean <- x
  x_clean[is.na(x_clean)] <- ""
  x_clean <- stringr::str_trim(x_clean)
  x_clean[x_clean %in% c("", "NULL", "null", "N/A", "n/a", "NA", "#N/A", "missing", "MISSING")] <- ""
  
  # Remove currency symbols, commas, percentages
  x_clean <- stringr::str_remove_all(x_clean, "[$,%]")
  x_clean <- stringr::str_remove_all(x_clean, "^[+]")  # Leading plus
  
  # Convert to numeric
  result <- suppressWarnings(as.numeric(x_clean))
  
  # Replace failed conversions with default
  failed_conversions <- is.na(result) & x_clean != ""
  if (any(failed_conversions)) {
    result[failed_conversions] <- default_value
  }
  
  return(result)
}

#' Robust Statistics Calculation
#' Calculates statistics while handling edge cases
#' 
#' @param x Numeric vector
#' @param variable_name Variable name for reporting
#' @return Enhanced statistics with edge case handling
robust_enhanced_describe <- function(x, variable_name = "Variable") {
  
  cat(sprintf("📊 Computing robust statistics for: %s\n", variable_name))
  
  # Edge case handling
  if (length(x) == 0) {
    cat("❌ Empty vector - no statistics possible\n")
    return(list(
      mean = NA, sd = NA, median = NA, q25 = NA, q75 = NA,
      min = NA, max = NA, n = 0, n_missing = 0,
      edge_case = "empty_vector"
    ))
  }
  
  # Convert to numeric if needed
  if (!is.numeric(x)) {
    cat("⚠️  Converting to numeric\n")
    x <- safe_numeric_conversion(x)
  }
  
  # Check for infinite values
  inf_count <- sum(is.infinite(x))
  if (inf_count > 0) {
    cat(sprintf("⚠️  Found %s infinite values - removing\n", format_count(inf_count)))
    x <- x[!is.infinite(x)]
  }
  
  n_total <- length(x)
  n_missing <- sum(is.na(x))
  n_valid <- n_total - n_missing
  
  if (n_valid == 0) {
    cat("❌ All values are missing - no statistics possible\n")
    return(list(
      mean = NA, sd = NA, median = NA, q25 = NA, q75 = NA,
      min = NA, max = NA, n = 0, n_missing = n_missing,
      edge_case = "all_missing"
    ))
  }
  
  if (n_valid == 1) {
    single_value <- x[!is.na(x)][1]
    cat(sprintf("⚠️  Only one valid value: %s\n", format_number(single_value)))
    return(list(
      mean = single_value, sd = 0, median = single_value, 
      q25 = single_value, q75 = single_value,
      min = single_value, max = single_value, 
      n = 1, n_missing = n_missing,
      edge_case = "single_value"
    ))
  }
  
  # Calculate robust statistics
  tryCatch({
    mean_val <- mean(x, na.rm = TRUE)
    sd_val <- stats::sd(x, na.rm = TRUE)
    
    # Use robust quantile method
    quantiles <- stats::quantile(x, probs = c(0.25, 0.5, 0.75), na.rm = TRUE, type = 7)
    
    min_val <- min(x, na.rm = TRUE)
    max_val <- max(x, na.rm = TRUE)
    
    result <- list(
      mean = round(mean_val, 3),
      sd = round(sd_val, 3),
      median = round(quantiles[["50%"]], 3),
      q25 = round(quantiles[["25%"]], 3),
      q75 = round(quantiles[["75%"]], 3),
      min = round(min_val, 3),
      max = round(max_val, 3),
      n = n_valid,
      n_missing = n_missing,
      edge_case = "none"
    )
    
    # Check for potential issues
    if (sd_val == 0) {
      result$edge_case <- "zero_variance"
      cat("⚠️  Zero variance detected (all values identical)\n")
    } else if (abs(mean_val) > 1e6) {
      result$edge_case <- "very_large_values"
      cat("⚠️  Very large values detected\n")
    }
    
    return(result)
    
  }, error = function(e) {
    cat(sprintf("❌ Error calculating statistics: %s\n", e$message))
    return(list(
      mean = NA, sd = NA, median = NA, q25 = NA, q75 = NA,
      min = NA, max = NA, n = 0, n_missing = n_total,
      edge_case = "calculation_error",
      error_message = e$message
    ))
  })
}

#' Initialize Reproducible Session
#' Sets up reproducible environment with logging
#' 
#' @param project_name Project name for logging
#' @param output_dir Output directory for logs
initialize_reproducible_session <- function(project_name = "NPI_Matching", output_dir = "logs") {
  
  cat("🌱 INITIALIZING REPRODUCIBLE SESSION\n")
  cat("====================================\n")
  
  # Set seed for reproducibility
  set.seed(1978)
  cat("🌱 Random seed set to: 1978\n")
  
  # Record start time
  session_start <- Sys.time()
  cat(sprintf("⏰ Session started: %s\n", session_start))
  
  # Record package versions
  package_versions <- record_package_versions(save_to_file = TRUE, output_dir = output_dir)
  
  # Create session log
  ensure_output_directory(output_dir)
  session_log_file <- generate_timestamped_filename(
    base_name = paste0("session_log_", stringr::str_replace_all(stringr::str_to_lower(project_name), "\\s+", "_")),
    extension = "txt",
    output_dir = output_dir
  )
  
  # Write session info
  session_info_lines <- c(
    paste("🚀 REPRODUCIBLE SESSION LOG -", project_name),
    paste(rep("=", 50), collapse = ""),
    "",
    paste("Started:", session_start),
    paste("Random seed:", 1978),
    paste("R version:", getRversion()),
    paste("Platform:", Sys.info()["sysname"], Sys.info()["release"]),
    paste("User:", Sys.info()["user"]),
    paste("Working directory:", getwd()),
    "",
    "📦 Key packages:",
    paste("  dplyr:", utils::packageVersion("dplyr")),
    paste("  readr:", utils::packageVersion("readr")),
    paste("  stringr:", utils::packageVersion("stringr")),
    ""
  )
  
  tryCatch({
    writeLines(session_info_lines, session_log_file)
    cat(sprintf("📋 Session log created: %s\n", session_log_file))
  }, error = function(e) {
    warning(sprintf("Could not create session log: %s", e$message))
  })
  
  cat("✅ Reproducible session initialized successfully\n\n")
  
  return(list(
    session_start = session_start,
    session_log_file = session_log_file,
    package_versions = package_versions,
    seed = 1978
  ))
}

# Initialize reproducible session
session_info <- initialize_reproducible_session("Enhanced_NPI_Matching")

cat("✅ Robust Processing System loaded successfully!\n")
cat("🔧 Functions available:\n")
cat("   - robust_data_validation(data, required_cols, name)\n")
cat("   - safe_numeric_conversion(x, default)\n")
cat("   - robust_enhanced_describe(x, name)\n")
cat("   - record_package_versions(save, dir)\n")
cat("   - initialize_reproducible_session(project, dir)\n\n")

cat("🛡️ Edge cases handled:\n")
cat("   ✅ NULL/empty datasets\n")
cat("   ✅ Invalid data types\n") 
cat("   ✅ Missing required columns\n")
cat("   ✅ All-NA columns\n")
cat("   ✅ Duplicate column names\n")
cat("   ✅ Infinite/NaN values\n")
cat("   ✅ Memory size warnings\n")
cat("   ✅ Single-value datasets\n")
cat("   ✅ Zero variance data\n\n")

cat("🔬 Reproducibility features:\n")
cat("   ✅ Fixed random seed: 1978\n")
cat("   ✅ Package version recording\n")
cat("   ✅ Session logging\n")
cat("   ✅ Timestamped outputs\n")
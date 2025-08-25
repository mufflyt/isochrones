#!/usr/bin/env Rscript

# Comprehensive Output Formatting and File Management System
# Ensures consistent formatting, timestamps, and directory management
# Author: Tyler & Claude
# Created: 2025-08-24

#' Format Number with Thousands Comma
#' 
#' @param x Numeric value
#' @param digits Number of decimal places (default: 3)
#' @return Formatted string with comma separators
format_number <- function(x, digits = 3) {
  if (is.na(x) || !is.numeric(x)) return("NA")
  
  # Round first, then format
  rounded <- round(x, digits)
  
  # Use base R formatting with comma separator
  formatted <- formatC(rounded, format = "f", digits = digits, big.mark = ",", decimal.mark = ".")
  
  return(formatted)
}

#' Format Percentage to Tenths
#' 
#' @param x Numeric value (as proportion, e.g., 0.75 for 75%)
#' @param as_percentage If TRUE, multiply by 100 (default: TRUE)
#' @return Formatted percentage string
format_percentage <- function(x, as_percentage = TRUE) {
  if (is.na(x) || !is.numeric(x)) return("NA%")
  
  # Convert to percentage if needed
  pct_value <- if (as_percentage) x * 100 else x
  
  # Round to tenths (1 decimal place)
  rounded_pct <- round(pct_value, 1)
  
  # Format with comma if needed (for large percentages)
  formatted <- format_number(rounded_pct, digits = 1)
  
  return(paste0(formatted, "%"))
}

#' Format Count with Thousands Comma
#' 
#' @param x Integer count
#' @return Formatted count string  
format_count <- function(x) {
  if (is.na(x) || !is.numeric(x)) return("NA")
  
  # Ensure integer
  count <- round(x)
  
  # Format with commas
  formatted <- formatC(count, format = "d", big.mark = ",")
  
  return(formatted)
}

#' Enhanced Statistics Formatter
#' Formats comprehensive statistics with consistent formatting
#' 
#' @param stats_list Output from enhanced_describe
#' @param variable_name Name of the variable
#' @return Consistently formatted string
format_enhanced_stats_consistent <- function(stats_list, variable_name = "Variable") {
  
  if (stats_list$n == 0) {
    return(sprintf("%s: No valid data (all %s values missing)", 
                   variable_name, format_count(stats_list$n_missing)))
  }
  
  # Format all components with consistent formatting
  mean_str <- format_number(stats_list$mean)
  sd_str <- format_number(stats_list$sd)
  median_str <- format_number(stats_list$median)
  q25_str <- format_number(stats_list$q25)
  q75_str <- format_number(stats_list$q75)
  min_str <- format_number(stats_list$min)
  max_str <- format_number(stats_list$max)
  n_str <- format_count(stats_list$n)
  
  result <- sprintf(
    "%s: Mean=%s (SD=%s), Median=%s (Q1=%s, Q3=%s), Range=[%s, %s], N=%s",
    variable_name,
    mean_str, sd_str,
    median_str, q25_str, q75_str, 
    min_str, max_str,
    n_str
  )
  
  if (stats_list$n_missing > 0) {
    result <- paste0(result, sprintf(" (%s missing)", format_count(stats_list$n_missing)))
  }
  
  return(result)
}

#' Generate Timestamp String
#' Creates consistent timestamp for file naming
#' 
#' @param format Timestamp format (default: "YYYYMMDD_HHMMSS")
#' @return Timestamp string
generate_timestamp <- function(format = "compact") {
  
  current_time <- Sys.time()
  
  if (format == "compact") {
    # Format: 20250824_143052
    return(format(current_time, "%Y%m%d_%H%M%S"))
  } else if (format == "readable") {
    # Format: 2025-08-24_14-30-52
    return(format(current_time, "%Y-%m-%d_%H-%M-%S"))
  } else if (format == "iso") {
    # Format: 2025-08-24T14:30:52
    return(format(current_time, "%Y-%m-%dT%H:%M:%S"))
  } else {
    # Custom format
    return(format(current_time, format))
  }
}

#' Safe Directory Creation
#' Creates output directory if it doesn't exist
#' 
#' @param dir_path Directory path to create
#' @param recursive Create parent directories (default: TRUE)
#' @return TRUE if successful, FALSE otherwise
ensure_output_directory <- function(dir_path, recursive = TRUE) {
  
  if (is.null(dir_path) || dir_path == "") {
    warning("Empty directory path provided")
    return(FALSE)
  }
  
  # Check if directory already exists
  if (dir.exists(dir_path)) {
    return(TRUE)
  }
  
  # Attempt to create directory
  tryCatch({
    dir.create(dir_path, recursive = recursive, showWarnings = FALSE)
    
    if (dir.exists(dir_path)) {
      cat(sprintf("✅ Created output directory: %s\n", dir_path))
      return(TRUE)
    } else {
      warning(sprintf("Failed to create directory: %s", dir_path))
      return(FALSE)
    }
  }, error = function(e) {
    warning(sprintf("Error creating directory %s: %s", dir_path, e$message))
    return(FALSE)
  })
}

#' Generate Timestamped Filename
#' Creates filename with timestamp and ensures directory exists
#' 
#' @param base_name Base filename without extension
#' @param extension File extension (default: "csv")
#' @param output_dir Output directory (default: "output")
#' @param timestamp_format Timestamp format (default: "compact")
#' @return Full file path with timestamp
generate_timestamped_filename <- function(base_name, extension = "csv", 
                                        output_dir = "output", timestamp_format = "compact") {
  
  # Ensure output directory exists
  if (!ensure_output_directory(output_dir)) {
    # Fallback to current directory
    output_dir <- "."
    warning("Using current directory as fallback")
  }
  
  # Generate timestamp
  timestamp <- generate_timestamp(timestamp_format)
  
  # Create filename
  filename <- sprintf("%s_%s.%s", base_name, timestamp, extension)
  
  # Combine with directory path
  full_path <- file.path(output_dir, filename)
  
  return(full_path)
}

#' Save Data with Timestamped Filename
#' Saves data frame with automatic timestamping and directory creation
#' 
#' @param data Data frame to save
#' @param base_name Base filename
#' @param output_dir Output directory (default: "output")
#' @param format File format ("csv", "xlsx", "rds") (default: "csv")
#' @return Full path of saved file
save_with_timestamp <- function(data, base_name, output_dir = "output", format = "csv") {
  
  # Generate timestamped filename
  file_path <- generate_timestamped_filename(base_name, format, output_dir)
  
  # Save based on format
  tryCatch({
    if (format == "csv") {
      readr::write_csv(data, file_path)
    } else if (format == "xlsx") {
      if (!requireNamespace("writexl", quietly = TRUE)) {
        warning("writexl package not available, falling back to CSV")
        file_path <- stringr::str_replace(file_path, "\\.xlsx$", ".csv")
        readr::write_csv(data, file_path)
      } else {
        writexl::write_xlsx(data, file_path)
      }
    } else if (format == "rds") {
      saveRDS(data, file_path)
    } else {
      # Default to CSV
      readr::write_csv(data, file_path)
    }
    
    cat(sprintf("💾 Saved %s records to: %s\n", format_count(nrow(data)), file_path))
    return(file_path)
    
  }, error = function(e) {
    warning(sprintf("Error saving file %s: %s", file_path, e$message))
    return(NULL)
  })
}

#' Enhanced Confidence Score Reporting
#' Creates detailed confidence score report with consistent formatting
#' 
#' @param confidence_scores Vector of confidence scores
#' @param group_labels Optional group labels
#' @param method_name Method name for reporting
#' @param save_report Whether to save report to file (default: FALSE)
#' @param output_dir Output directory for report (default: "reports")
#' @return Detailed confidence statistics with consistent formatting
analyze_confidence_scores_enhanced <- function(confidence_scores, group_labels = NULL, 
                                             method_name = "Method", save_report = FALSE,
                                             output_dir = "reports") {
  
  report_lines <- c()
  
  # Header
  header <- sprintf("📊 CONFIDENCE SCORE ANALYSIS: %s", method_name)
  separator <- paste(rep("=", nchar(header)), collapse = "")
  
  report_lines <- c(report_lines, header, separator, "")
  
  # Overall statistics using enhanced describe
  overall_stats <- enhanced_describe(confidence_scores)
  formatted_stats <- format_enhanced_stats_consistent(overall_stats, "Overall Confidence")
  report_lines <- c(report_lines, formatted_stats, "")
  
  # Confidence tier analysis with consistent formatting
  if (overall_stats$n > 0) {
    high_conf <- sum(confidence_scores >= 75, na.rm = TRUE)
    med_conf <- sum(confidence_scores >= 60 & confidence_scores < 75, na.rm = TRUE)
    low_conf <- sum(confidence_scores < 60, na.rm = TRUE)
    
    high_pct <- format_percentage(high_conf / overall_stats$n)
    med_pct <- format_percentage(med_conf / overall_stats$n)
    low_pct <- format_percentage(low_conf / overall_stats$n)
    
    tier_analysis <- sprintf(
      "Confidence Tiers: High (≥75%%): %s (%s), Moderate (60-74%%): %s (%s), Low (<60%%): %s (%s)",
      format_count(high_conf), high_pct,
      format_count(med_conf), med_pct, 
      format_count(low_conf), low_pct
    )
    
    report_lines <- c(report_lines, tier_analysis, "")
  }
  
  # Group analysis if labels provided
  if (!is.null(group_labels)) {
    report_lines <- c(report_lines, "📊 BY GROUP:", "")
    unique_groups <- unique(group_labels)
    
    for (group in unique_groups) {
      group_scores <- confidence_scores[group_labels == group]
      if (length(group_scores) > 0) {
        group_stats <- enhanced_describe(group_scores)
        group_formatted <- format_enhanced_stats_consistent(group_stats, paste("  ", group))
        report_lines <- c(report_lines, group_formatted)
      }
    }
    report_lines <- c(report_lines, "")
  }
  
  # Print to console
  for (line in report_lines) {
    cat(line, "\n")
  }
  
  # Save report if requested
  if (save_report) {
    report_file <- generate_timestamped_filename(
      base_name = paste0("confidence_analysis_", stringr::str_replace_all(stringr::str_to_lower(method_name), "\\s+", "_")),
      extension = "txt",
      output_dir = output_dir
    )
    
    tryCatch({
      writeLines(report_lines, report_file)
      cat(sprintf("📋 Report saved to: %s\n", report_file))
    }, error = function(e) {
      warning(sprintf("Could not save report: %s", e$message))
    })
  }
  
  return(overall_stats)
}

#' Create Comprehensive Summary Report
#' Generates a summary with all formatting standards
#' 
#' @param results_list Named list of results from different methods
#' @param output_dir Output directory for reports (default: "reports")
#' @return Summary statistics
create_comprehensive_summary <- function(results_list, output_dir = "reports") {
  
  cat("📊 COMPREHENSIVE RESULTS SUMMARY\n")
  cat("================================\n\n")
  
  summary_data <- data.frame()
  
  for (method_name in names(results_list)) {
    method_data <- results_list[[method_name]]
    
    if (is.null(method_data) || nrow(method_data) == 0) {
      cat(sprintf("⚠️  %s: No results\n", method_name))
      next
    }
    
    # Calculate method statistics
    total_records <- nrow(method_data)
    if ("confidence_score" %in% names(method_data)) {
      conf_stats <- enhanced_describe(method_data$confidence_score)
      high_conf_count <- sum(method_data$confidence_score >= 75, na.rm = TRUE)
      high_conf_pct <- high_conf_count / total_records
      
      cat(sprintf("✅ %s: %s total records, %s high confidence (%s)\n",
                  method_name, 
                  format_count(total_records),
                  format_count(high_conf_count),
                  format_percentage(high_conf_pct)))
      
      # Add to summary data
      summary_row <- data.frame(
        method = method_name,
        total_records = total_records,
        high_confidence_count = high_conf_count,
        high_confidence_percentage = high_conf_pct * 100,
        mean_confidence = conf_stats$mean,
        sd_confidence = conf_stats$sd,
        median_confidence = conf_stats$median,
        stringsAsFactors = FALSE
      )
      
      summary_data <- rbind(summary_data, summary_row)
    } else {
      cat(sprintf("✅ %s: %s records (no confidence scores)\n",
                  method_name, format_count(total_records)))
    }
  }
  
  # Save comprehensive summary
  if (nrow(summary_data) > 0) {
    summary_file <- save_with_timestamp(
      data = summary_data,
      base_name = "comprehensive_summary",
      output_dir = output_dir,
      format = "csv"
    )
  }
  
  cat("\n")
  return(summary_data)
}

# Test the formatting functions
test_formatting <- function() {
  cat("🧪 TESTING OUTPUT FORMATTING\n")
  cat("============================\n\n")
  
  # Test number formatting
  test_numbers <- c(1234.5678, 0.001234, 1000000.999, 0.1)
  cat("Number formatting:\n")
  for (num in test_numbers) {
    cat(sprintf("  %g -> %s\n", num, format_number(num)))
  }
  
  # Test percentage formatting
  test_percentages <- c(0.12345, 0.007, 1.234, 0.999)
  cat("\nPercentage formatting:\n")
  for (pct in test_percentages) {
    cat(sprintf("  %g -> %s\n", pct, format_percentage(pct)))
  }
  
  # Test count formatting
  test_counts <- c(1234, 1000000, 1, 0)
  cat("\nCount formatting:\n")
  for (count in test_counts) {
    cat(sprintf("  %g -> %s\n", count, format_count(count)))
  }
  
  # Test timestamp generation
  cat("\nTimestamp formats:\n")
  cat(sprintf("  Compact: %s\n", generate_timestamp("compact")))
  cat(sprintf("  Readable: %s\n", generate_timestamp("readable")))
  cat(sprintf("  ISO: %s\n", generate_timestamp("iso")))
  
  cat("\n✅ Formatting tests complete\n")
}

cat("✅ Output Formatting System loaded successfully!\n")
cat("📋 Functions available:\n")
cat("   - format_number(x, digits) - thousandths comma formatting\n")
cat("   - format_percentage(x) - percentage to tenths\n") 
cat("   - format_count(x) - integer count formatting\n")
cat("   - generate_timestamp(format) - consistent timestamps\n")
cat("   - ensure_output_directory(path) - auto-create directories\n")
cat("   - save_with_timestamp(data, base_name, dir) - timestamped saves\n")
cat("   - analyze_confidence_scores_enhanced() - formatted analysis\n")
cat("   - create_comprehensive_summary() - complete summary reports\n\n")

cat("🎯 Formatting standards:\n")
cat("   ✅ Numbers: Comma thousands separator, 3 decimal places\n")
cat("   ✅ Percentages: Rounded to tenths (1 decimal place)\n")
cat("   ✅ File timestamps: YYYYMMDD_HHMMSS format\n")
cat("   ✅ Automatic directory creation\n")
cat("   ✅ Consistent statistical reporting\n\n")

cat("🧪 Run: test_formatting() to see examples\n")
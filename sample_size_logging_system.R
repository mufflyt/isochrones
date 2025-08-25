#!/usr/bin/env Rscript

#' Sample Size Logging System
#' 
#' Tracks and logs sample sizes at each stage of data processing, joins, and filtering
#' Provides detailed audit trail for data flow analysis
#' 
#' @title Sample Size Logging and Audit Trail System
#' @description Comprehensive logging system that tracks data transformations
#' @author Tyler & Claude
#' @date 2025-08-24

# Load required packages with explicit namespacing
source("output_formatting_system.R")

#' Initialize Sample Size Logger
#' 
#' Creates a new sample size tracking object to log data transformations
#' 
#' @title Initialize Sample Size Logger
#' @description Creates logger object with initial metadata
#' @param process_name Character. Name of the process being logged
#' @param initial_data Data frame. Initial dataset to start logging
#' @param log_file Character. Optional file path for persistent logging
#' @return List object containing logger state
#' @examples
#' \dontrun{
#' logger <- initialize_sample_logger("NPI_Matching", physician_data)
#' }
#' @export
initialize_sample_logger <- function(process_name, initial_data = NULL, log_file = NULL) {
  
  # Initialize logger structure
  logger <- list(
    process_name = process_name,
    start_time = Sys.time(),
    stages = list(),
    current_stage = 0,
    log_file = log_file,
    total_stages_logged = 0,
    session_seed = 1978
  )
  
  cat("📊 INITIALIZING SAMPLE SIZE LOGGER\n")
  cat(paste0("Process: ", process_name, "\n"))
  cat(paste0("Started: ", logger$start_time, "\n"))
  cat("=" %s% 40, "\n\n")
  
  # Log initial data if provided
  if (!is.null(initial_data)) {
    logger <- log_sample_size(
      logger = logger,
      data = initial_data, 
      stage_name = "Initial Data Load",
      operation = "LOAD",
      notes = "Starting dataset"
    )
  }
  
  return(logger)
}

#' Log Sample Size at Processing Stage
#' 
#' Records sample size and metadata at each data processing step
#' 
#' @title Log Sample Size at Processing Stage
#' @description Logs data dimensions and processing details at each transformation step
#' @param logger List. Logger object from initialize_sample_logger
#' @param data Data frame. Current dataset after processing step
#' @param stage_name Character. Descriptive name for this processing stage
#' @param operation Character. Type of operation (LOAD, FILTER, JOIN, TRANSFORM, etc.)
#' @param notes Character. Additional notes about the operation
#' @param key_columns Character vector. Names of key columns for joins/filtering
#' @param previous_data Data frame. Optional previous dataset for comparison
#' @return Updated logger object
#' @examples
#' \dontrun{
#' logger <- log_sample_size(logger, filtered_data, "Remove Missing NPIs", "FILTER")
#' }
#' @export
log_sample_size <- function(logger, data, stage_name, operation = "PROCESS", 
                           notes = "", key_columns = NULL, previous_data = NULL) {
  
  # Validate inputs
  if (!is.data.frame(data)) {
    warning("Data is not a data frame - converting")
    data <- tryCatch(as.data.frame(data), error = function(e) data.frame())
  }
  
  # Increment stage counter
  logger$current_stage <- logger$current_stage + 1
  stage_number <- logger$current_stage
  
  # Calculate current data metrics
  current_rows <- nrow(data)
  current_cols <- ncol(data)
  current_size_mb <- as.numeric(utils::object.size(data)) / (1024^2)
  
  # Calculate changes from previous stage
  rows_change <- NA
  rows_change_pct <- NA
  if (length(logger$stages) > 0) {
    previous_stage <- logger$stages[[length(logger$stages)]]
    rows_change <- current_rows - previous_stage$rows
    rows_change_pct <- if (previous_stage$rows > 0) (rows_change / previous_stage$rows) * 100 else NA
  }
  
  # Alternative: calculate changes from explicitly provided previous data
  if (!is.null(previous_data) && is.data.frame(previous_data)) {
    previous_rows <- nrow(previous_data)
    rows_change <- current_rows - previous_rows  
    rows_change_pct <- if (previous_rows > 0) (rows_change / previous_rows) * 100 else NA
  }
  
  # Check for key column issues in joins
  key_warnings <- character()
  if (!is.null(key_columns) && operation %in% c("JOIN", "MERGE")) {
    for (key_col in key_columns) {
      if (!key_col %in% names(data)) {
        key_warnings <- c(key_warnings, paste("Missing key column:", key_col))
      } else {
        # Check for duplicates in key columns
        key_duplicates <- sum(duplicated(data[[key_col]]))
        if (key_duplicates > 0) {
          key_warnings <- c(key_warnings, paste("Duplicate keys in", key_col, ":", format_count(key_duplicates)))
        }
        
        # Check for missing values in key columns
        key_missing <- sum(is.na(data[[key_col]]) | data[[key_col]] == "")
        if (key_missing > 0) {
          key_warnings <- c(key_warnings, paste("Missing values in", key_col, ":", format_count(key_missing)))
        }
      }
    }
  }
  
  # Create stage record
  stage_record <- list(
    stage_number = stage_number,
    stage_name = stage_name,
    operation = operation,
    timestamp = Sys.time(),
    rows = current_rows,
    columns = current_cols,
    size_mb = round(current_size_mb, 2),
    rows_change = rows_change,
    rows_change_pct = rows_change_pct,
    key_columns = key_columns,
    key_warnings = key_warnings,
    notes = notes,
    column_names = names(data)[1:min(10, length(names(data)))]  # First 10 column names
  )
  
  # Add to logger
  logger$stages[[stage_number]] <- stage_record
  logger$total_stages_logged <- stage_number
  
  # Format output messages
  change_text <- ""
  if (!is.na(rows_change)) {
    change_symbol <- if (rows_change >= 0) "+" else ""
    change_text <- sprintf(" (%s%s rows, %s)",
                          change_symbol,
                          format_count(rows_change), 
                          format_percentage(abs(rows_change_pct) / 100))
  }
  
  warning_text <- ""
  if (length(key_warnings) > 0) {
    warning_text <- paste0(" ⚠️ ", paste(key_warnings, collapse = "; "))
  }
  
  # Console logging
  cat(sprintf("📊 Stage %d: %s [%s]\n", 
              stage_number, stage_name, operation))
  cat(sprintf("   📈 Sample size: %s rows × %s cols (%.1f MB)%s\n",
              format_count(current_rows),
              format_count(current_cols),
              current_size_mb,
              change_text))
  
  if (length(key_columns) > 0) {
    cat(sprintf("   🔑 Key columns: %s%s\n", 
                paste(key_columns, collapse = ", "),
                warning_text))
  }
  
  if (notes != "") {
    cat(sprintf("   📝 Notes: %s\n", notes))
  }
  cat("\n")
  
  # File logging if specified
  if (!is.null(logger$log_file)) {
    write_log_entry(logger$log_file, stage_record, logger$process_name)
  }
  
  return(logger)
}

#' Log Join Operation with Detailed Metrics
#' 
#' Specialized logging for database joins with join-specific metrics
#' 
#' @title Log Join Operation with Detailed Metrics
#' @description Logs join operations with detailed metrics about key matching and duplicates
#' @param logger List. Logger object
#' @param left_data Data frame. Left dataset before join
#' @param right_data Data frame. Right dataset before join  
#' @param result_data Data frame. Result dataset after join
#' @param join_keys Character vector. Column names used for joining
#' @param join_type Character. Type of join (left, inner, full, anti, etc.)
#' @param left_name Character. Name of left dataset
#' @param right_name Character. Name of right dataset
#' @return Updated logger object
#' @examples
#' \dontrun{
#' logger <- log_join_operation(logger, physicians, npi_lookup, result, 
#'                             join_keys = "npi", join_type = "left_join")
#' }
#' @export
log_join_operation <- function(logger, left_data, right_data, result_data, 
                              join_keys, join_type = "join", 
                              left_name = "Left", right_name = "Right") {
  
  # Validate join keys exist
  missing_keys_left <- setdiff(join_keys, names(left_data))
  missing_keys_right <- setdiff(join_keys, names(right_data))
  
  if (length(missing_keys_left) > 0 || length(missing_keys_right) > 0) {
    warning(paste("Missing join keys:",
                  if (length(missing_keys_left) > 0) paste("Left:", paste(missing_keys_left, collapse = ", ")),
                  if (length(missing_keys_right) > 0) paste("Right:", paste(missing_keys_right, collapse = ", "))))
  }
  
  # Calculate join metrics
  left_rows <- nrow(left_data)
  right_rows <- nrow(right_data)
  result_rows <- nrow(result_data)
  
  # Analyze key overlap (for available keys)
  available_keys <- intersect(join_keys, intersect(names(left_data), names(right_data)))
  
  key_metrics <- list()
  if (length(available_keys) > 0) {
    for (key in available_keys) {
      left_unique_keys <- unique(left_data[[key]][!is.na(left_data[[key]]) & left_data[[key]] != ""])
      right_unique_keys <- unique(right_data[[key]][!is.na(right_data[[key]]) & right_data[[key]] != ""])
      
      overlapping_keys <- intersect(left_unique_keys, right_unique_keys)
      
      key_metrics[[key]] <- list(
        left_unique_count = length(left_unique_keys),
        right_unique_count = length(right_unique_keys), 
        overlapping_count = length(overlapping_keys),
        overlap_pct_left = if (length(left_unique_keys) > 0) length(overlapping_keys) / length(left_unique_keys) * 100 else 0,
        overlap_pct_right = if (length(right_unique_keys) > 0) length(overlapping_keys) / length(right_unique_keys) * 100 else 0
      )
    }
  }
  
  # Check for unintended row multiplication
  expected_max_rows <- max(left_rows, right_rows)  # Conservative estimate
  row_multiplication_factor <- if (left_rows > 0) result_rows / left_rows else NA
  
  multiplication_warning <- ""
  if (!is.na(row_multiplication_factor) && row_multiplication_factor > 1.1) {
    multiplication_warning <- sprintf("⚠️ Possible row multiplication: %.2fx increase", row_multiplication_factor)
  }
  
  # Create detailed join notes
  join_notes <- sprintf("%s %s: %s (%s) + %s (%s) = %s result rows",
                       join_type, 
                       paste(join_keys, collapse = ", "),
                       format_count(left_rows), left_name,
                       format_count(right_rows), right_name, 
                       format_count(result_rows))
  
  if (length(key_metrics) > 0) {
    key_summary <- paste(sapply(names(key_metrics), function(k) {
      m <- key_metrics[[k]]
      sprintf("%s: %s overlapping keys (%.1f%% of left, %.1f%% of right)",
              k, format_count(m$overlapping_count), m$overlap_pct_left, m$overlap_pct_right)
    }), collapse = "; ")
    join_notes <- paste(join_notes, key_summary, sep = ". ")
  }
  
  if (multiplication_warning != "") {
    join_notes <- paste(join_notes, multiplication_warning, sep = ". ")
  }
  
  # Log the join operation
  logger <- log_sample_size(
    logger = logger,
    data = result_data,
    stage_name = sprintf("%s Join: %s + %s", stringr::str_to_title(join_type), left_name, right_name),
    operation = "JOIN",
    notes = join_notes,
    key_columns = join_keys,
    previous_data = left_data
  )
  
  return(logger)
}

#' Log Filter Operation with Criteria Details
#' 
#' Specialized logging for filtering operations showing what was filtered and why
#' 
#' @title Log Filter Operation with Criteria Details  
#' @description Logs filtering operations with detailed criteria and impact analysis
#' @param logger List. Logger object
#' @param before_data Data frame. Dataset before filtering
#' @param after_data Data frame. Dataset after filtering
#' @param filter_description Character. Description of filter criteria
#' @param filter_columns Character vector. Columns involved in filtering
#' @return Updated logger object
#' @examples
#' \dontrun{
#' logger <- log_filter_operation(logger, all_data, clean_data, 
#'                               "Remove records with missing NPI", "npi")
#' }
#' @export
log_filter_operation <- function(logger, before_data, after_data, 
                                filter_description, filter_columns = NULL) {
  
  before_rows <- nrow(before_data)
  after_rows <- nrow(after_data)
  filtered_out <- before_rows - after_rows
  filter_rate <- if (before_rows > 0) (filtered_out / before_rows) * 100 else 0
  
  # Analyze filtering impact by column if specified
  column_analysis <- ""
  if (!is.null(filter_columns)) {
    column_stats <- sapply(filter_columns, function(col) {
      if (col %in% names(before_data)) {
        missing_before <- sum(is.na(before_data[[col]]) | before_data[[col]] == "")
        missing_rate <- if (before_rows > 0) (missing_before / before_rows) * 100 else 0
        sprintf("%s: %s missing (%.1f%%)", col, format_count(missing_before), missing_rate)
      } else {
        sprintf("%s: column not found", col)
      }
    })
    column_analysis <- paste(". Column analysis:", paste(column_stats, collapse = "; "))
  }
  
  filter_notes <- sprintf("%s. Filtered out %s records (%.1f%% reduction)%s",
                         filter_description,
                         format_count(filtered_out),
                         filter_rate,
                         column_analysis)
  
  logger <- log_sample_size(
    logger = logger,
    data = after_data,
    stage_name = paste("Filter:", filter_description),
    operation = "FILTER", 
    notes = filter_notes,
    key_columns = filter_columns,
    previous_data = before_data
  )
  
  return(logger)
}

#' Generate Comprehensive Sample Size Report
#' 
#' Creates detailed report of all logged sample size changes throughout the process
#' 
#' @title Generate Comprehensive Sample Size Report
#' @description Creates comprehensive audit trail report of all data transformations
#' @param logger List. Logger object with accumulated stages
#' @param save_report Logical. Whether to save report to file
#' @param output_dir Character. Directory for saving report
#' @return Summary statistics of the logging session
#' @examples
#' \dontrun{
#' report_summary <- generate_sample_size_report(logger, save_report = TRUE)
#' }
#' @export
generate_sample_size_report <- function(logger, save_report = TRUE, output_dir = "reports") {
  
  cat("📋 COMPREHENSIVE SAMPLE SIZE REPORT\n")
  cat("===================================\n\n")
  
  if (length(logger$stages) == 0) {
    cat("❌ No stages logged - cannot generate report\n")
    return(list(valid = FALSE))
  }
  
  # Overall process summary
  first_stage <- logger$stages[[1]]
  last_stage <- logger$stages[[length(logger$stages)]]
  
  total_time <- as.numeric(difftime(last_stage$timestamp, first_stage$timestamp, units = "mins"))
  total_row_change <- last_stage$rows - first_stage$rows
  total_change_pct <- if (first_stage$rows > 0) (total_row_change / first_stage$rows) * 100 else 0
  
  cat(sprintf("🎯 Process: %s\n", logger$process_name))
  cat(sprintf("⏱️  Duration: %.1f minutes (%d stages)\n", total_time, length(logger$stages)))
  cat(sprintf("📊 Data Flow: %s → %s rows (%+s, %s)\n",
              format_count(first_stage$rows),
              format_count(last_stage$rows),
              format_count(total_row_change),
              format_percentage(abs(total_change_pct) / 100)))
  cat("\n")
  
  # Stage-by-stage breakdown
  cat("📈 STAGE-BY-STAGE BREAKDOWN\n")
  cat("===========================\n")
  
  for (i in seq_along(logger$stages)) {
    stage <- logger$stages[[i]]
    
    change_text <- ""
    if (!is.na(stage$rows_change)) {
      change_symbol <- if (stage$rows_change >= 0) "+" else ""
      change_text <- sprintf(" (%s%s, %s)",
                            change_symbol,
                            format_count(stage$rows_change),
                            format_percentage(abs(stage$rows_change_pct) / 100))
    }
    
    cat(sprintf("%d. %s [%s]\n", i, stage$stage_name, stage$operation))
    cat(sprintf("   📊 %s rows × %s cols (%.1f MB)%s\n",
                format_count(stage$rows),
                format_count(stage$columns), 
                stage$size_mb,
                change_text))
    
    if (length(stage$key_columns) > 0) {
      cat(sprintf("   🔑 Keys: %s\n", paste(stage$key_columns, collapse = ", ")))
    }
    
    if (length(stage$key_warnings) > 0) {
      cat(sprintf("   ⚠️  Warnings: %s\n", paste(stage$key_warnings, collapse = "; ")))
    }
    
    if (stage$notes != "") {
      cat(sprintf("   📝 %s\n", stage$notes))
    }
    cat("\n")
  }
  
  # Identify potential issues
  cat("🔍 POTENTIAL ISSUES IDENTIFIED\n")
  cat("==============================\n")
  
  issues_found <- 0
  
  # Check for significant data loss
  for (i in seq_along(logger$stages)) {
    stage <- logger$stages[[i]]
    if (!is.na(stage$rows_change_pct) && stage$rows_change_pct < -50) {
      cat(sprintf("⚠️  Stage %d (%s): Large data loss (%.1f%% reduction)\n",
                  i, stage$stage_name, abs(stage$rows_change_pct)))
      issues_found <- issues_found + 1
    }
  }
  
  # Check for unexpected row multiplication  
  for (i in seq_along(logger$stages)) {
    stage <- logger$stages[[i]]
    if (stage$operation == "JOIN" && !is.na(stage$rows_change_pct) && stage$rows_change_pct > 10) {
      cat(sprintf("⚠️  Stage %d (%s): Possible row multiplication (+%.1f%%)\n",
                  i, stage$stage_name, stage$rows_change_pct))
      issues_found <- issues_found + 1
    }
  }
  
  # Check for key column warnings
  for (i in seq_along(logger$stages)) {
    stage <- logger$stages[[i]]
    if (length(stage$key_warnings) > 0) {
      cat(sprintf("⚠️  Stage %d (%s): Key issues - %s\n",
                  i, stage$stage_name, paste(stage$key_warnings, collapse = "; ")))
      issues_found <- issues_found + 1
    }
  }
  
  if (issues_found == 0) {
    cat("✅ No significant issues detected\n")
  }
  cat("\n")
  
  # Summary statistics
  operations_summary <- table(sapply(logger$stages, function(s) s$operation))
  
  cat("📊 OPERATION SUMMARY\n")
  cat("===================\n")
  for (op in names(operations_summary)) {
    cat(sprintf("   %s: %s operations\n", op, format_count(operations_summary[op])))
  }
  cat("\n")
  
  # Create summary object
  summary_stats <- list(
    process_name = logger$process_name,
    total_stages = length(logger$stages),
    duration_minutes = total_time,
    initial_rows = first_stage$rows,
    final_rows = last_stage$rows,
    total_row_change = total_row_change,
    total_change_percentage = total_change_pct,
    operations_performed = operations_summary,
    issues_identified = issues_found,
    largest_data_loss_stage = which.min(sapply(logger$stages, function(s) s$rows_change_pct %||% 0)),
    most_complex_stage = which.max(sapply(logger$stages, function(s) s$columns))
  )
  
  # Save report if requested
  if (save_report) {
    report_file <- save_sample_size_report_to_file(logger, summary_stats, output_dir)
    cat(sprintf("💾 Sample size report saved to: %s\n", report_file))
  }
  
  return(summary_stats)
}

#' Save Sample Size Report to File
#' 
#' Internal function to save detailed sample size report to file
#' 
#' @param logger List. Logger object
#' @param summary_stats List. Summary statistics
#' @param output_dir Character. Output directory
#' @return Path to saved file
save_sample_size_report_to_file <- function(logger, summary_stats, output_dir) {
  
  ensure_output_directory(output_dir)
  
  report_file <- generate_timestamped_filename(
    base_name = paste0("sample_size_report_", stringr::str_replace_all(stringr::str_to_lower(logger$process_name), "\\s+", "_")),
    extension = "txt",
    output_dir = output_dir
  )
  
  # Create comprehensive report content
  report_lines <- c(
    paste0("📊 SAMPLE SIZE AUDIT TRAIL: ", toupper(logger$process_name)),
    paste(rep("=", nchar(logger$process_name) + 32), collapse = ""),
    "",
    paste("Generated:", Sys.time()),
    paste("Process duration:", sprintf("%.1f minutes", summary_stats$duration_minutes)),
    paste("Total stages logged:", format_count(summary_stats$total_stages)),
    paste("Random seed:", logger$session_seed),
    "",
    "📈 OVERALL DATA FLOW",
    "===================",
    sprintf("Initial rows: %s", format_count(summary_stats$initial_rows)),
    sprintf("Final rows: %s", format_count(summary_stats$final_rows)),
    sprintf("Net change: %+s rows (%s)", format_count(summary_stats$total_row_change), format_percentage(abs(summary_stats$total_change_percentage) / 100)),
    "",
    "📋 DETAILED STAGE LOG",
    "====================",
    ""
  )
  
  # Add each stage
  for (i in seq_along(logger$stages)) {
    stage <- logger$stages[[i]]
    
    stage_lines <- c(
      sprintf("Stage %d: %s [%s]", i, stage$stage_name, stage$operation),
      sprintf("  Timestamp: %s", stage$timestamp),
      sprintf("  Sample size: %s rows × %s columns (%.1f MB)", 
              format_count(stage$rows), format_count(stage$columns), stage$size_mb)
    )
    
    if (!is.na(stage$rows_change)) {
      stage_lines <- c(stage_lines, sprintf("  Change: %+s rows (%s)", 
                                           format_count(stage$rows_change),
                                           format_percentage(abs(stage$rows_change_pct) / 100)))
    }
    
    if (length(stage$key_columns) > 0) {
      stage_lines <- c(stage_lines, sprintf("  Key columns: %s", paste(stage$key_columns, collapse = ", ")))
    }
    
    if (length(stage$key_warnings) > 0) {
      stage_lines <- c(stage_lines, sprintf("  Warnings: %s", paste(stage$key_warnings, collapse = "; ")))
    }
    
    if (stage$notes != "") {
      stage_lines <- c(stage_lines, sprintf("  Notes: %s", stage$notes))
    }
    
    stage_lines <- c(stage_lines, "")
    report_lines <- c(report_lines, stage_lines)
  }
  
  # Add summary section
  report_lines <- c(report_lines,
                   "📊 SUMMARY STATISTICS",
                   "====================",
                   sprintf("Total operations: %s", format_count(sum(summary_stats$operations_performed))),
                   "Operations by type:")
  
  for (op_type in names(summary_stats$operations_performed)) {
    report_lines <- c(report_lines, sprintf("  %s: %s", op_type, format_count(summary_stats$operations_performed[op_type])))
  }
  
  report_lines <- c(report_lines,
                   "",
                   sprintf("Issues identified: %s", format_count(summary_stats$issues_identified)),
                   sprintf("Data quality: %s", if (summary_stats$issues_identified == 0) "GOOD" else "NEEDS ATTENTION"))
  
  # Write report to file
  tryCatch({
    writeLines(report_lines, report_file)
    return(report_file)
  }, error = function(e) {
    warning(sprintf("Could not save sample size report: %s", e$message))
    return(NULL)
  })
}

#' Write Single Log Entry to File
#' 
#' Internal helper function to write individual log entries to persistent log file
#' 
#' @param log_file Character. Path to log file
#' @param stage_record List. Stage record to write
#' @param process_name Character. Process name
write_log_entry <- function(log_file, stage_record, process_name) {
  
  ensure_output_directory(dirname(log_file))
  
  # Create log entry
  log_entry <- sprintf("[%s] %s | Stage %d: %s [%s] | %s rows × %s cols | %s",
                      Sys.time(),
                      process_name,
                      stage_record$stage_number,
                      stage_record$stage_name,
                      stage_record$operation, 
                      format_count(stage_record$rows),
                      format_count(stage_record$columns),
                      stage_record$notes)
  
  # Append to log file
  tryCatch({
    cat(log_entry, "\n", file = log_file, append = TRUE)
  }, error = function(e) {
    warning(sprintf("Could not write to log file %s: %s", log_file, e$message))
  })
}

cat("✅ Sample Size Logging System loaded successfully!\n")
cat("📊 Functions available:\n")
cat("   - initialize_sample_logger(process_name, initial_data, log_file)\n")
cat("   - log_sample_size(logger, data, stage_name, operation, notes, keys)\n")
cat("   - log_join_operation(logger, left, right, result, keys, type)\n")
cat("   - log_filter_operation(logger, before, after, description, columns)\n")
cat("   - generate_sample_size_report(logger, save_report, output_dir)\n\n")

cat("🎯 Logging capabilities:\n")
cat("   ✅ Sample sizes at each processing stage\n")
cat("   ✅ Join operation metrics and key overlap analysis\n")
cat("   ✅ Filter operation impact and criteria details\n")
cat("   ✅ Row multiplication detection for joins\n")
cat("   ✅ Missing/duplicate key validation\n")
cat("   ✅ Comprehensive audit trail generation\n")
cat("   ✅ Persistent file logging with timestamps\n")
cat("   ✅ Issue identification and warnings\n")
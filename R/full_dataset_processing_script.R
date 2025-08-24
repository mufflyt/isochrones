# Full Dataset NPI Processing Script for RStudio
# Process 3,913 OB/GYN subspecialists with enhanced taxonomy matching
# Author: Tyler & Claude
# Created: 2025-08-23

# ============================================================================
# SETUP AND CONFIGURATION
# ============================================================================

# Clear environment and load required libraries
rm(list = ls())
gc()

# Load the enhanced NPI matching system
source("R/03-search_and_process_npi_testing.R")

# Configuration
INPUT_FILE <- "data/03-search_and_process_npi/complete_npi_for_subspecialists.csv"
OUTPUT_DIR <- "data/03-search_and_process_npi/full_dataset_results"
BATCH_SIZE <- 250  # Process in chunks to manage API rate limits
CONFIDENCE_THRESHOLD <- 80
USE_TAXONOMY_CODES <- TRUE  # Use precise ABOG taxonomy matching

# Create output directory
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(OUTPUT_DIR, "batches"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(OUTPUT_DIR, "logs"), recursive = TRUE, showWarnings = FALSE)

cat("🚀 FULL DATASET NPI PROCESSING SCRIPT\n")
cat("=====================================\n")
cat("Input file:", INPUT_FILE, "\n")
cat("Output directory:", OUTPUT_DIR, "\n")
cat("Batch size:", BATCH_SIZE, "names per batch\n")
cat("Confidence threshold:", CONFIDENCE_THRESHOLD, "%\n")
cat("Using taxonomy codes:", USE_TAXONOMY_CODES, "\n\n")

# ============================================================================
# PHASE 1: INITIAL VALIDATION AND SMALL TEST
# ============================================================================

cat("📋 PHASE 1: Initial Validation\n")
cat("==============================\n")

# Check input file exists
if (!file.exists(INPUT_FILE)) {
  stop("❌ Input file not found: ", INPUT_FILE)
}

# Load and inspect data
full_data <- read.csv(INPUT_FILE)
cat("✅ Data loaded:", nrow(full_data), "total records\n")
cat("✅ Column names:", paste(names(full_data), collapse = ", "), "\n")

# Validate required columns
required_cols <- c("first", "last")
missing_cols <- setdiff(required_cols, names(full_data))
if (length(missing_cols) > 0) {
  stop("❌ Missing required columns: ", paste(missing_cols, collapse = ", "))
}

# Remove rows with missing names
full_data_clean <- full_data[
  !is.na(full_data$first) & 
  !is.na(full_data$last) & 
  full_data$first != "" & 
  full_data$last != "", 
]

cat("✅ After cleaning:", nrow(full_data_clean), "records with valid names\n")

# ============================================================================
# PHASE 2: SMALL TEST RUN (20 names)
# ============================================================================

cat("\n🧪 PHASE 2: Small Test Run\n")
cat("===========================\n")

# Test with first 20 names
test_results <- enhanced_npi_matching(
  input_file = INPUT_FILE,
  output_dir = file.path(OUTPUT_DIR, "test_run"),
  confidence_threshold = CONFIDENCE_THRESHOLD,
  use_taxonomy_codes = USE_TAXONOMY_CODES,
  max_records = 20
)

cat("Test Results Summary:\n")
print(test_results)

# ============================================================================
# PHASE 3: BATCH PROCESSING OF FULL DATASET
# ============================================================================

cat("\n🔥 PHASE 3: Full Dataset Batch Processing\n")
cat("==========================================\n")

# Calculate batches
total_records <- nrow(full_data_clean)
total_batches <- ceiling(total_records / BATCH_SIZE)

cat("Total records to process:", total_records, "\n")
cat("Batch size:", BATCH_SIZE, "\n") 
cat("Total batches:", total_batches, "\n")
cat("Estimated time: ~", round(total_records * 0.1 / 60, 1), "minutes\n\n")

# Initialize tracking
batch_results <- list()
overall_stats <- data.frame(
  batch = integer(),
  start_idx = integer(), 
  end_idx = integer(),
  processed = integer(),
  high_conf_matches = integer(),
  low_conf_matches = integer(),
  no_matches = integer(),
  success_rate = numeric(),
  processing_time = numeric(),
  stringsAsFactors = FALSE
)

# Create log file
log_file <- file.path(OUTPUT_DIR, "logs", paste0("processing_log_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt"))
log_conn <- file(log_file, open = "a")

# Log function
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_message <- paste0("[", timestamp, "] ", message)
  cat(full_message, "\n")
  cat(full_message, "\n", file = log_conn)
  flush(log_conn)
}

log_message("Starting full dataset processing")

# Main processing loop
for (batch_num in 1:total_batches) {
  
  # Calculate batch indices
  start_idx <- (batch_num - 1) * BATCH_SIZE + 1
  end_idx <- min(batch_num * BATCH_SIZE, total_records)
  
  log_message(paste0("Starting batch ", batch_num, "/", total_batches, " (records ", start_idx, "-", end_idx, ")"))
  
  # Create batch input file
  batch_data <- full_data_clean[start_idx:end_idx, ]
  batch_input_file <- file.path(OUTPUT_DIR, "batches", paste0("batch_", batch_num, "_input.csv"))
  write.csv(batch_data, batch_input_file, row.names = FALSE)
  
  # Process batch
  batch_start_time <- Sys.time()
  
  tryCatch({
    batch_result <- enhanced_npi_matching(
      input_file = batch_input_file,
      output_dir = file.path(OUTPUT_DIR, "batches", paste0("batch_", batch_num)),
      confidence_threshold = CONFIDENCE_THRESHOLD,
      use_taxonomy_codes = USE_TAXONOMY_CODES,
      max_records = NULL  # Process all records in batch
    )
    
    batch_end_time <- Sys.time()
    processing_time <- as.numeric(batch_end_time - batch_start_time, units = "mins")
    
    # Store batch results
    batch_results[[batch_num]] <- batch_result
    
    # Update overall stats
    batch_stats <- data.frame(
      batch = batch_num,
      start_idx = start_idx,
      end_idx = end_idx, 
      processed = end_idx - start_idx + 1,
      high_conf_matches = batch_result$high_confidence_matches,
      low_conf_matches = batch_result$low_confidence_matches,
      no_matches = batch_result$unmatched_names,
      success_rate = batch_result$overall_match_rate,
      processing_time = processing_time,
      stringsAsFactors = FALSE
    )
    
    overall_stats <- rbind(overall_stats, batch_stats)
    
    log_message(paste0("Batch ", batch_num, " completed: ", 
                      batch_result$overall_match_rate, "% success rate, ",
                      round(processing_time, 1), " minutes"))
    
    # Progress update
    if (batch_num %% 5 == 0 || batch_num == total_batches) {
      overall_success_rate <- mean(overall_stats$success_rate, na.rm = TRUE)
      total_processed <- sum(overall_stats$processed)
      total_matches <- sum(overall_stats$high_conf_matches + overall_stats$low_conf_matches)
      
      cat("\n📊 PROGRESS UPDATE - Batch", batch_num, "of", total_batches, "\n")
      cat("Records processed:", total_processed, "/", total_records, 
          "(", round(total_processed/total_records*100, 1), "%)\n")
      cat("Overall success rate:", round(overall_success_rate, 1), "%\n")
      cat("Total matches found:", total_matches, "\n")
      cat("Estimated time remaining:", 
          round((total_batches - batch_num) * mean(overall_stats$processing_time, na.rm = TRUE), 1), 
          "minutes\n\n")
    }
    
  }, error = function(e) {
    log_message(paste0("ERROR in batch ", batch_num, ": ", e$message))
    cat("❌ Error in batch", batch_num, ":", e$message, "\n")
  })
  
  # Save progress regularly
  if (batch_num %% 10 == 0 || batch_num == total_batches) {
    saveRDS(batch_results, file.path(OUTPUT_DIR, "batch_results_progress.rds"))
    write.csv(overall_stats, file.path(OUTPUT_DIR, "processing_stats.csv"), row.names = FALSE)
    log_message("Progress saved")
  }
}

close(log_conn)

# ============================================================================
# PHASE 4: FINAL CONSOLIDATION AND ANALYSIS
# ============================================================================

cat("\n📊 PHASE 4: Final Analysis\n")
cat("===========================\n")

# Consolidate all results
log_message("Starting final consolidation")

# Calculate final statistics
total_processed <- sum(overall_stats$processed, na.rm = TRUE)
total_high_conf <- sum(overall_stats$high_conf_matches, na.rm = TRUE)
total_low_conf <- sum(overall_stats$low_conf_matches, na.rm = TRUE)
total_no_match <- sum(overall_stats$no_matches, na.rm = TRUE)
overall_success_rate <- (total_high_conf + total_low_conf) / total_processed * 100
high_conf_rate <- total_high_conf / total_processed * 100
total_time <- sum(overall_stats$processing_time, na.rm = TRUE)

# Create final summary
final_summary <- list(
  processing_date = Sys.time(),
  input_file = INPUT_FILE,
  total_records_input = nrow(full_data),
  total_records_processed = total_processed,
  confidence_threshold = CONFIDENCE_THRESHOLD,
  use_taxonomy_codes = USE_TAXONOMY_CODES,
  batch_size = BATCH_SIZE,
  total_batches = total_batches,
  high_confidence_matches = total_high_conf,
  low_confidence_matches = total_low_conf,
  no_matches = total_no_match,
  overall_success_rate = overall_success_rate,
  high_confidence_rate = high_conf_rate,
  total_processing_time_minutes = total_time,
  average_time_per_record = total_time * 60 / total_processed
)

# Save final results
saveRDS(final_summary, file.path(OUTPUT_DIR, "final_summary.rds"))
saveRDS(batch_results, file.path(OUTPUT_DIR, "all_batch_results.rds"))
write.csv(overall_stats, file.path(OUTPUT_DIR, "final_processing_stats.csv"), row.names = FALSE)

# Print final summary
cat("\n🎯 FINAL PROCESSING SUMMARY\n")
cat("============================\n")
cat("Total records processed:", total_processed, "\n")
cat("High confidence matches:", total_high_conf, "(", round(high_conf_rate, 1), "%)\n")
cat("Low confidence matches:", total_low_conf, "\n") 
cat("No matches:", total_no_match, "\n")
cat("Overall success rate:", round(overall_success_rate, 1), "%\n")
cat("Total processing time:", round(total_time, 1), "minutes (", round(total_time/60, 1), "hours)\n")
cat("Average time per record:", round(final_summary$average_time_per_record, 2), "seconds\n")

cat("\n📂 OUTPUT FILES:\n")
cat("- Final summary: final_summary.rds\n")
cat("- All batch results: all_batch_results.rds\n")
cat("- Processing stats: final_processing_stats.csv\n")
cat("- Individual batch results in: batches/\n")
cat("- Processing log in: logs/\n")

cat("\n🚀 Full dataset processing completed successfully!\n")
log_message("Full dataset processing completed successfully")

# Optional: Create quick visualization
if (requireNamespace("ggplot2", quietly = TRUE)) {
  library(ggplot2)
  
  # Success rate by batch
  p1 <- ggplot(overall_stats, aes(x = batch, y = success_rate)) +
    geom_line() + geom_point() +
    labs(title = "Success Rate by Batch", 
         x = "Batch Number", y = "Success Rate (%)") +
    theme_minimal()
  
  ggsave(file.path(OUTPUT_DIR, "success_rate_by_batch.png"), p1, width = 10, height = 6)
  cat("📈 Success rate plot saved: success_rate_by_batch.png\n")
}

cat("\n✅ All processing complete! Check the output directory for results.\n")
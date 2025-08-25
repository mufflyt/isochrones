#!/usr/bin/env Rscript

# Complete NPI Matching with Fixed Cross-Reference Function
# This script processes all 1,563 physician names with the debugged system

cat("🚀 Starting Enhanced NPI Matching - Fixed Version\n")
cat("==============================================\n\n")

# Load the enhanced system
source("R/03-search_and_process_npi_testing.R")

# Run the enhanced matching with your dataset
# Replace with your actual input file path
input_file <- "data/03-search_and_process_npi/full_dataset_results/physicians_to_match.csv"  

# Check if the file exists, if not use a likely alternative
if (!file.exists(input_file)) {
  # Look for CSV files in common locations
  possible_files <- c(
    "physicians_to_match.csv",
    "physician_names.csv", 
    "data/physician_names.csv",
    list.files(".", pattern = "*.csv", recursive = TRUE)[1]
  )
  
  for (file in possible_files) {
    if (file.exists(file)) {
      input_file <- file
      cat("📁 Using input file:", input_file, "\n")
      break
    }
  }
}

# If still no file found, create a sample
if (!file.exists(input_file)) {
  cat("📁 No input file found. Please specify your CSV file path.\n")
  cat("📝 Expected format: CSV with 'first' and 'last' columns\n")
  quit(save = "no")
}

# Run enhanced matching
cat("🔍 Processing physician names...\n")
results <- enhanced_npi_matching(
  input_file = input_file,
  output_dir = "data/03-search_and_process_npi/fixed_results",
  confidence_threshold = 75,
  use_taxonomy_codes = TRUE,  # Enable all databases
  max_records = NULL,         # Process all records
  batch_size = 50
)

# Display results
cat("\n✅ MATCHING COMPLETE!\n")
cat("====================\n")
cat("📊 FINAL RESULTS:\n")
cat("Total processed:", results$total_processed, "\n")
cat("High confidence matches:", results$high_confidence_matches, 
    paste0("(", results$high_confidence_rate, "%)"), "\n")
cat("Low confidence matches:", results$low_confidence_matches, "\n")
cat("Unmatched names:", results$unmatched_names, "\n")
cat("Overall match rate:", results$overall_match_rate, "%\n")
cat("\n📁 Output files saved to:", results$output_directory, "\n")

cat("\n🎉 Processing complete! Check the output directory for your results.\n")
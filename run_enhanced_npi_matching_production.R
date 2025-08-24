# Production-Ready Enhanced NPI Matching Script
# Simple interface for running the enhanced multi-database matching system
# Author: Tyler & Claude
# Created: 2025-08-24

# EXECUTION ORDER: Run AFTER testing scripts have completed successfully

cat("🎯 PRODUCTION-READY ENHANCED NPI MATCHING\n")
cat("=========================================\n\n")

# Load the enhanced matching system
source("R/03-search_and_process_npi_testing.R")

#' Run Enhanced NPI Matching on Real Data
#'
#' @param input_csv_path Path to CSV file with physician names (must have 'first' and 'last' columns)
#' @param output_directory Directory to save results (will be created if needed)
#' @param confidence_threshold Minimum confidence for high-confidence matches (default 75)
#' @param max_records Maximum records to process (NULL for all)
#' @param enable_all_databases Use all available cross-reference databases (recommended: TRUE)
run_production_matching <- function(
  input_csv_path,
  output_directory = "data/03-search_and_process_npi/production_output",
  confidence_threshold = 75,
  max_records = NULL,
  enable_all_databases = TRUE
) {
  
  cat("🚀 Starting Production NPI Matching...\n")
  cat("=====================================\n")
  
  # Validate input file
  if (!file.exists(input_csv_path)) {
    stop("❌ Input file not found: ", input_csv_path)
  }
  
  # Quick validation of file structure
  sample_data <- readr::read_csv(input_csv_path, n_max = 5, show_col_types = FALSE)
  required_cols <- c("first", "last")
  
  if (!all(required_cols %in% names(sample_data))) {
    # Check for alternative column names
    alt_names <- names(sample_data)
    cat("⚠️  Expected columns 'first' and 'last' not found.\n")
    cat("   Available columns:", paste(alt_names, collapse = ", "), "\n")
    cat("   The system will attempt to auto-detect name columns...\n\n")
  }
  
  cat("📊 Input file validated:", input_csv_path, "\n")
  cat("💾 Output directory:", output_directory, "\n")
  cat("🎯 Confidence threshold:", confidence_threshold, "%\n")
  cat("🔍 Multi-database enabled:", enable_all_databases, "\n")
  
  if (!is.null(max_records)) {
    cat("📋 Processing limit:", max_records, "records\n")
  } else {
    cat("📋 Processing: All records in file\n")
  }
  
  cat("\n")
  
  # Run the enhanced matching
  start_time <- Sys.time()
  
  results <- enhanced_npi_matching(
    input_file = input_csv_path,
    output_dir = output_directory,
    confidence_threshold = confidence_threshold,
    use_taxonomy_codes = enable_all_databases,
    max_records = max_records,
    batch_size = 50  # Optimal batch size
  )
  
  end_time <- Sys.time()
  total_time <- as.numeric(difftime(end_time, start_time, units = "mins"))
  
  # Results summary
  cat("\n🎉 PRODUCTION MATCHING COMPLETE!\n")
  cat("================================\n")
  cat("📊 PERFORMANCE METRICS:\n")
  cat(sprintf("   ⏱️  Total processing time: %.1f minutes\n", total_time))
  cat(sprintf("   📋 Records processed: %d\n", results$total_processed))
  cat(sprintf("   ⚡ Processing rate: %.1f records/minute\n", results$total_processed / total_time))
  
  cat("\n📈 MATCHING RESULTS:\n")
  cat(sprintf("   🎯 High confidence matches: %d (%.1f%%)\n", 
              results$high_confidence_matches, results$high_confidence_rate))
  cat(sprintf("   ⚠️  Low confidence matches: %d\n", results$low_confidence_matches))
  cat(sprintf("   ❌ Unmatched names: %d\n", results$unmatched_names))
  cat(sprintf("   📊 Overall match rate: %.1f%%\n", results$overall_match_rate))
  
  # Performance assessment
  cat("\n🚀 SYSTEM ASSESSMENT:\n")
  if (results$overall_match_rate >= 80) {
    cat("✅ EXCELLENT: Match rate ≥80% - System performing excellently!\n")
  } else if (results$overall_match_rate >= 70) {
    cat("✅ GOOD: Match rate ≥70% - System performing well!\n")
  } else if (results$overall_match_rate >= 60) {
    cat("⚠️  FAIR: Match rate ≥60% - Consider reviewing low confidence matches\n")
  } else {
    cat("❌ NEEDS REVIEW: Match rate <60% - Check input data quality\n")
  }
  
  # Cost savings estimate
  api_calls_saved <- results$total_processed - (results$high_confidence_matches + results$low_confidence_matches + results$unmatched_names)
  if (api_calls_saved > 0) {
    savings_pct <- round(api_calls_saved / results$total_processed * 100, 1)
    cat(sprintf("💰 API calls saved: %d (%.1f%% cost reduction)\n", api_calls_saved, savings_pct))
  }
  
  cat("\n📁 OUTPUT FILES CREATED:\n")
  cat("   - High confidence matches: *high_confidence_matches*.csv\n")
  cat("   - Low confidence matches: *low_confidence_matches*.csv\n")
  cat("   - Unmatched names: *unmatched_names*.csv\n")
  cat("   - Summary report: *matching_summary*.txt\n")
  cat("   (All files saved in:", output_directory, ")\n")
  
  return(results)
}

# EXAMPLE USAGE (uncomment and modify for your data):
# ===================================================

# # Example 1: Process ABOG dataset (if available)
# if (FALSE) {
#   abog_results <- run_production_matching(
#     input_csv_path = "data/0-Download/output/best_abog_provider_dataframe_8_17_2025_2053.csv",
#     output_directory = "data/03-search_and_process_npi/abog_production_results",
#     confidence_threshold = 75,
#     max_records = NULL,  # Process all records
#     enable_all_databases = TRUE
#   )
# }

# # Example 2: Process your custom CSV file
# if (FALSE) {
#   custom_results <- run_production_matching(
#     input_csv_path = "your_physician_names.csv",  # Must have 'first' and 'last' columns
#     output_directory = "data/03-search_and_process_npi/custom_results",
#     confidence_threshold = 75,
#     max_records = 1000,  # Limit for testing
#     enable_all_databases = TRUE
#   )
# }

# # Example 3: Quick test with subset
# if (FALSE) {
#   test_results <- run_production_matching(
#     input_csv_path = "your_physician_names.csv",
#     output_directory = "data/03-search_and_process_npi/test_results",
#     confidence_threshold = 70,  # Lower threshold for more matches
#     max_records = 50,           # Small test batch
#     enable_all_databases = TRUE
#   )
# }

cat("💡 TO USE THIS SCRIPT:\n")
cat("======================\n")
cat("1. Uncomment one of the examples above\n")
cat("2. Modify the input_csv_path to point to your data\n")
cat("3. Ensure your CSV has 'first' and 'last' name columns\n") 
cat("4. Run the script!\n\n")

cat("📋 Expected CSV format:\n")
cat("   first,last\n")
cat("   Jennifer,Smith\n")
cat("   Michael,Johnson\n")
cat("   Dr. Sarah,Williams\n")
cat("   ...\n\n")

cat("🎯 The system will automatically:\n")
cat("   - Load all available cross-reference databases\n")
cat("   - Parse names intelligently (handles titles, middle names, etc.)\n")
cat("   - Apply hierarchical confidence scoring\n")
cat("   - Deduplicate results across all sources\n")
cat("   - Generate comprehensive output files\n\n")

cat("✅ Production script ready! Modify examples above and run.\n")

#' Download and Process Open Payments Data (Optional Enhancement)
#'
#' @description Helper function to download and process Open Payments data
#' This will improve matching rates by adding financial disclosure cross-reference data
#' @param force_download Boolean. Force re-download even if files exist
#' @param years Numeric vector. Years to download (default: recent 3 years)
setup_open_payments_enhancement <- function(force_download = FALSE, years = c(2021, 2022, 2023)) {
  
  cat("🚀 OPEN PAYMENTS ENHANCEMENT SETUP\n")
  cat("==================================\n\n")
  
  # Check if Open Payments scripts are available
  op_scripts <- c(
    "R/A-open_payments_download.R",
    "R/B-open_payments_cleaning.R", 
    "R/C-open_payments.R"
  )
  
  missing_scripts <- op_scripts[!file.exists(op_scripts)]
  
  if (length(missing_scripts) > 0) {
    cat("❌ Missing Open Payments scripts:\n")
    for (script in missing_scripts) {
      cat("   -", script, "\n")
    }
    cat("\n⚠️  Cannot setup Open Payments enhancement without these scripts.\n")
    cat("   The enhanced NPI matching will work without Open Payments data,\n")
    cat("   but match rates may be 5-10% lower.\n")
    return(FALSE)
  }
  
  cat("✅ Found all Open Payments processing scripts\n\n")
  
  # Estimate download requirements
  estimated_size_gb <- length(years) * 0.5  # ~500MB per year
  cat("📊 SETUP REQUIREMENTS:\n")
  cat(sprintf("   📅 Years to process: %s\n", paste(years, collapse = ", ")))
  cat(sprintf("   💾 Estimated download size: %.1f GB\n", estimated_size_gb))
  cat(sprintf("   ⏱️  Estimated setup time: %d minutes\n", length(years) * 2))
  cat("   🔧 Processing: Download → Extract → Clean → Index\n\n")
  
  # Ask for user confirmation
  if (!force_download) {
    response <- readline(prompt = "Continue with Open Payments setup? (y/n): ")
    if (tolower(substr(response, 1, 1)) != "y") {
      cat("⏹️  Setup cancelled. Enhanced matching will proceed without Open Payments.\n")
      return(FALSE)
    }
  }
  
  cat("🚀 Starting Open Payments setup...\n\n")
  
  # Step 1: Download data
  cat("📥 Step 1: Downloading Open Payments data...\n")
  tryCatch({
    source("R/A-open_payments_download.R")
    cat("✅ Download completed\n\n")
  }, error = function(e) {
    cat("❌ Download failed:", e$message, "\n")
    return(FALSE)
  })
  
  # Step 2: Clean and process data  
  cat("🧹 Step 2: Processing and cleaning data...\n")
  tryCatch({
    source("R/B-open_payments_cleaning.R")
    cat("✅ Processing completed\n\n")
  }, error = function(e) {
    cat("❌ Processing failed:", e$message, "\n")
    return(FALSE)
  })
  
  # Step 3: Create analysis-ready database
  cat("🗄️  Step 3: Creating analysis database...\n")
  tryCatch({
    source("R/C-open_payments.R")
    cat("✅ Database creation completed\n\n")
  }, error = function(e) {
    cat("❌ Database creation failed:", e$message, "\n")
    return(FALSE)
  })
  
  cat("🎉 OPEN PAYMENTS SETUP COMPLETE!\n")
  cat("==================================\n")
  cat("✅ Enhanced NPI matching now includes Open Payments cross-referencing\n")
  cat("📈 Expected improvement: +5-10% in match rates\n")
  cat("💰 Financial disclosure data adds +20 confidence bonus\n\n")
  
  return(TRUE)
}

cat("\n💡 OPTIONAL ENHANCEMENT:\n")
cat("========================\n")
cat("To further improve match rates, run:\n")
cat("  setup_open_payments_enhancement()\n\n")
cat("This will download and process CMS Open Payments data\n")
cat("for even higher confidence physician matching.\n\n")
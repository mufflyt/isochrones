#!/usr/bin/env Rscript

#' Enhanced NPI Physician Matching System - Main Processing Script
#' 
#' @title Enhanced NPI Physician Matching with Multi-Database Cross-Referencing
#' @description Comprehensive system for matching physician names to NPI numbers using
#'   multiple data sources, enhanced confidence scoring, and robust validation
#' @author Tyler Muffly & Claude (Anthropic)
#' @date 2025-08-24
#' @version 2.0.0
#' 
#' @section Purpose:
#' This script provides a production-ready system for matching physician names 
#' (first, last, middle) to National Provider Identifier (NPI) numbers using 
#' multiple authoritative databases and advanced matching algorithms.
#' 
#' @section Key Features:
#' \itemize{
#'   \item Multi-database cross-referencing (Medicare Part D, Physician Compare, Open Payments)
#'   \item Intelligent name parsing with humaniformat (handles titles, middle names, hyphens)
#'   \item Gender-aware matching with demographic-specific bonuses
#'   \item Credential validation (MD vs DO) with confidence adjustments  
#'   \item Middle name/initial matching with fuzzy similarity scoring
#'   \item Comprehensive taxonomy code matching across all OB/GYN specialties
#'   \item Dual API search using provider::nppes + npi::npi_search
#'   \item Robust deduplication across multiple data sources
#'   \item Enhanced confidence scoring with statistical validation
#'   \item Comprehensive logging and audit trails
#' }
#' 
#' @section Data Sources and Expected Formats:
#' 
#' \strong{INPUT DATA ASSUMPTIONS:}
#' \itemize{
#'   \item Primary input: CSV file with columns 'first' and 'last' (required)
#'   \item Optional columns: 'middle', 'credentials', 'gender', 'specialty'
#'   \item Names can include titles (Dr.), suffixes (Jr., III), hyphens, apostrophes
#'   \item Missing values: NA, empty strings, "NULL", "N/A" are handled gracefully
#'   \item Character encoding: UTF-8 expected for international characters
#' }
#' 
#' \strong{EXTERNAL DATABASE ASSUMPTIONS:}
#' \itemize{
#'   \item Medicare Part D: Contains PRSCRBR_NPI only, requires external name lookup
#'   \item Physician Compare: Contains NPI, frst_nm, lst_nm, mid_nm, gndr, Cred
#'   \item Open Payments: Contains Covered_Recipient_NPI, Covered_Recipient_First_Name, 
#'     Covered_Recipient_Last_Name, Covered_Recipient_Middle_Name
#'   \item NBER NPPES: Contains comprehensive provider data with taxonomy codes
#'   \item All databases assumed to have NPI as 10-digit numeric strings
#' }
#' 
#' @section Matching Algorithm Assumptions:
#' 
#' \strong{NAME MATCHING:}
#' \itemize{
#'   \item First/Last names: Jaro-Winkler similarity with 85% threshold for matches
#'   \item Middle names: Exact match (1.0), initial vs full (0.8), fuzzy (variable)
#'   \item Nicknames: Common variations handled (Mike/Michael, Kate/Katherine)
#'   \item International names: Accents normalized, hyphens preserved
#'   \item Case insensitive matching throughout
#' }
#' 
#' \strong{GENDER INFERENCE:}
#' \itemize{
#'   \item Uses multiple methods: gender package, name dictionaries, pattern matching
#'   \item OB/GYN demographic assumptions: ~80% female, ~20% male practitioners
#'   \item Male OB/GYN matches get higher confidence bonus due to rarity
#'   \item Gender mismatches penalized heavily (-12 points)
#' }
#' 
#' \strong{CREDENTIAL MATCHING:}
#' \itemize{
#'   \item MD physicians: ~85-90% of provider population
#'   \item DO physicians: ~10-15% of provider population, higher confidence when matched
#'   \item MD/DO conflicts: Major penalty (-15 points) as likely different providers
#'   \item Other credentials: NP, PA supported with appropriate bonuses
#' }
#' 
#' @section Database-Specific Assumptions:
#' 
#' \strong{Medicare Part D:}
#' \itemize{
#'   \item File location: /Volumes/MufflyNew/Medicare_part_D_prescribers/unzipped_files/
#'   \item Contains only NPI numbers, no name data
#'   \item Used primarily for NPI validation and active prescriber confirmation
#'   \item External NPI lookup required for name enrichment
#' }
#' 
#' \strong{Physician Compare:}
#' \itemize{
#'   \item File location: /Volumes/MufflyNew/physician_compare/
#'   \item Rich name data including middle names, suffixes, credentials, gender
#'   \item No maiden names available
#'   \item Primary source for name-based matching
#' }
#' 
#' \strong{Open Payments:}
#' \itemize{
#'   \item File location: /Volumes/MufflyNew/open_payments_data/
#'   \item Most comprehensive data including full addresses
#'   \item Payment history provides additional validation
#'   \item No maiden names available
#' }
#' 
#' @section Performance Assumptions:
#' \itemize{
#'   \item Expected match rate: 85-90% for clean input data
#'   \item Processing speed: ~100-500 records per minute depending on search methods
#'   \item Memory usage: ~100-500MB for typical datasets (1,000-10,000 records)
#'   \item Network dependency: Requires internet for live NPI API calls
#' }
#' 
#' @section Output Assumptions:
#' \itemize{
#'   \item High confidence matches: ≥75% confidence threshold
#'   \item All outputs timestamped with YYYYMMDD_HHMMSS format
#'   \item CSV format with comma thousands separators, percentages to tenths
#'   \item Comprehensive audit trails and sample size logging
#'   \item Raw input data preserved immutably, processed data saved separately
#' }
#' 
#' @section Limitations and Known Issues:
#' \itemize{
#'   \item No maiden name columns available in any external database
#'   \item Medicare Part D requires external API calls for name enrichment
#'   \item Large datasets (>10,000 records) may require batch processing
#'   \item International characters may have varying support across databases
#'   \item API rate limits may affect processing speed for live lookups
#' }
#' 
#' @section Dependencies:
#' \itemize{
#'   \item R (>= 4.0.0)
#'   \item dplyr (>= 1.0.0) - Data manipulation
#'   \item readr (>= 2.0.0) - File I/O  
#'   \item stringr (>= 1.4.0) - String processing
#'   \item stringdist (>= 0.9.0) - Fuzzy matching
#'   \item humaniformat (>= 0.6.0) - Name parsing
#'   \item provider (>= 0.1.0) - NPI API access
#'   \item npi (>= 0.1.0) - Alternative NPI API
#'   \item assertthat (>= 0.2.0) - Input validation
#' }
#' 
#' @section Usage Examples:
#' 
#' \strong{Basic Usage:}
#' \code{
#' # Load the system
#' source("R/03-search_and_process_npi_testing.R")
#' 
#' # Process a CSV file
#' results <- enhanced_npi_matching(
#'   input_file = "physicians.csv",
#'   output_dir = "results", 
#'   confidence_threshold = 75
#' )
#' }
#' 
#' \strong{Advanced Usage with Logging:}
#' \code{
#' # Initialize logging
#' logger <- initialize_sample_logger("My_NPI_Matching")
#' 
#' # Load and process with detailed logging
#' input_data <- readr::read_csv("physicians.csv")
#' logger <- log_sample_size(logger, input_data, "Input Data Load", "LOAD")
#' 
#' # Run matching with full cross-referencing
#' crossref_sources <- load_all_crossref_data()
#' results <- enhanced_npi_matching(
#'   input_file = "physicians.csv",
#'   crossref_sources = crossref_sources,
#'   use_taxonomy_codes = TRUE,
#'   confidence_threshold = 75
#' )
#' 
#' # Generate comprehensive report
#' report <- generate_sample_size_report(logger, save_report = TRUE)
#' }
#' 
#' @section Configuration:
#' 
#' \strong{Confidence Thresholds:}
#' \itemize{
#'   \item High confidence: ≥75% (recommended for production)
#'   \item Moderate confidence: 60-74% (requires manual review)
#'   \item Low confidence: <60% (likely false matches)
#' }
#' 
#' \strong{Search Method Hierarchy (with bonuses):}
#' \enumerate{
#'   \item NBER Cross-reference: +25 points
#'   \item ABMS Certification: +22 points  
#'   \item Medicare Part D: +22 points
#'   \item Open Payments: +20 points
#'   \item Physician Compare: +20 points
#'   \item Taxonomy Codes: +15 points
#'   \item provider::nppes API: +15 points
#'   \item npi::npi_search API: +12 points
#'   \item Fallback methods: +10 points
#' }
#' 
#' @section Error Handling:
#' \itemize{
#'   \item Graceful degradation when external databases unavailable
#'   \item Comprehensive input validation with detailed error messages
#'   \item Automatic fallback to alternative search methods
#'   \item Missing value handling throughout pipeline
#'   \item Network timeout handling for API calls
#' }
#' 
#' @section Reproducibility:
#' \itemize{
#'   \item Fixed random seed: 1978
#'   \item Package version logging
#'   \item Session information capture
#'   \item Timestamped outputs with full audit trails
#' }
#' 
#' @section Quality Assurance:
#' \itemize{
#'   \item Comprehensive unit tests for all major functions
#'   \item Edge case handling (empty datasets, single records, etc.)
#'   \item Data type validation and standardization
#'   \item Duplicate detection and deduplication
#'   \item Statistical validation of confidence scores
#' }
#' 
#' @seealso 
#' \itemize{
#'   \item \code{\link{enhanced_npi_matching}} Main matching function
#'   \item \code{\link{search_provider_enhanced}} Individual search function
#'   \item \code{\link{load_all_crossref_data}} Cross-reference data loader
#'   \item \code{\link{calculate_enhanced_confidence}} Confidence scoring
#' }
#' 
#' @references
#' \itemize{
#'   \item National Plan and Provider Enumeration System (NPPES): https://nppes.cms.hhs.gov/
#'   \item CMS Open Payments Database: https://openpaymentsdata.cms.gov/
#'   \item Medicare Part D Prescriber Data: https://data.cms.gov/
#' }

# END OF DOCUMENTATION HEADER
# ============================

# The actual script implementation would continue below...
# This template shows the comprehensive documentation structure
# that should be at the top of each major script file.

cat("📚 Script Documentation Template loaded successfully!\n")
cat("📋 Documentation sections included:\n")
cat("   ✅ Title, description, author, version\n") 
cat("   ✅ Purpose and key features\n")
cat("   ✅ Data source assumptions and formats\n")
cat("   ✅ Algorithm assumptions and thresholds\n")
cat("   ✅ Database-specific assumptions\n")
cat("   ✅ Performance expectations\n")
cat("   ✅ Output specifications\n")
cat("   ✅ Known limitations\n")
cat("   ✅ Dependencies and versions\n")
cat("   ✅ Usage examples (basic and advanced)\n")
cat("   ✅ Configuration options\n")
cat("   ✅ Error handling approach\n")
cat("   ✅ Reproducibility measures\n")
cat("   ✅ Quality assurance notes\n")
cat("   ✅ References and links\n\n")

cat("💡 Apply this documentation template to all major scripts:\n")
cat("   - R/03-search_and_process_npi_testing.R\n")
cat("   - enhanced_matching_with_credentials.R\n")
cat("   - dual_api_npi_matching.R\n")
cat("   - And other main processing scripts\n")
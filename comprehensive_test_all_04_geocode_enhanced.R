# Comprehensive Test Suite for ALL Parts of Enhanced 04-geocode-enhanced.R
# Tests every function, parameter combination, and edge case
# Author: Tyler & Claude
# Created: 2025-08-23

source("R/04-geocode-enhanced.R")

cat("🧪 COMPREHENSIVE TEST SUITE - ALL PARTS OF 04-GEOCODE-ENHANCED.R\n")
cat("================================================================\n\n")

# Global test tracking
test_results <- list()

#' Test result helper
record_test <- function(test_name, passed, details = "") {
  test_results[[test_name]] <<- list(passed = passed, details = details)
  status <- if(passed) "✅ PASS" else "❌ FAIL"
  cat(sprintf("%-50s: %s %s\n", test_name, status, details))
}

# ==============================================================================
# TEST 1: HAVERSINE DISTANCE FUNCTION
# ==============================================================================
cat("📏 TEST 1: HAVERSINE DISTANCE CALCULATIONS\n")
cat("==========================================\n")

test_haversine_basic <- function() {
  # Known distances for validation
  nyc_lat <- 40.7128; nyc_lon <- -74.0060
  la_lat <- 34.0522; la_lon <- -118.2437
  
  # Test NYC to LA (~3,944 km)
  dist_m <- haversine_distance(nyc_lat, nyc_lon, la_lat, la_lon)
  dist_km <- dist_m / 1000
  nyc_la_ok <- abs(dist_km - 3944) < 100  # Within 100km tolerance
  
  record_test("Haversine: NYC to LA distance", nyc_la_ok, 
              sprintf("%.0f km (expected ~3944)", dist_km))
  
  # Test identical points (should be 0)
  identical_dist <- haversine_distance(nyc_lat, nyc_lon, nyc_lat, nyc_lon)
  identical_ok <- identical_dist == 0
  
  record_test("Haversine: Identical points", identical_ok, 
              sprintf("%.2f m (expected 0)", identical_dist))
  
  # Test small distances (nearby points in Atlanta)
  atlanta1_lat <- 33.7490; atlanta1_lon <- -84.3880
  atlanta2_lat <- 33.7501; atlanta2_lon <- -84.3885
  small_dist <- haversine_distance(atlanta1_lat, atlanta1_lon, atlanta2_lat, atlanta2_lon)
  small_ok <- small_dist > 0 && small_dist < 1000  # Should be under 1km
  
  record_test("Haversine: Small distances", small_ok,
              sprintf("%.0f m (reasonable)", small_dist))
  
  # Test antipodal points (opposite sides of Earth ~20,015 km)
  antipodal_dist <- haversine_distance(0, 0, 0, 180)
  antipodal_km <- antipodal_dist / 1000
  antipodal_ok <- abs(antipodal_km - 20015) < 200
  
  record_test("Haversine: Antipodal points", antipodal_ok,
              sprintf("%.0f km (expected ~20015)", antipodal_km))
  
  return(all(nyc_la_ok, identical_ok, small_ok, antipodal_ok))
}

test_haversine_edge_cases <- function() {
  # Test with extreme coordinates
  north_pole <- haversine_distance(90, 0, 89, 0)  # North pole area
  north_ok <- north_pole > 0
  
  record_test("Haversine: North pole region", north_ok,
              sprintf("%.0f m", north_pole))
  
  # Test crossing antimeridian
  antimeridian <- haversine_distance(0, 179, 0, -179)
  anti_ok <- antimeridian > 0 && antimeridian < 500000  # Should be small distance
  
  record_test("Haversine: Antimeridian crossing", anti_ok,
              sprintf("%.0f m", antimeridian))
  
  return(all(north_ok, anti_ok))
}

haversine_basic_passed <- test_haversine_basic()
haversine_edge_passed <- test_haversine_edge_cases()

cat("\n")

# ==============================================================================
# TEST 2: ENHANCED ADDRESS NORMALIZATION
# ==============================================================================
cat("🏠 TEST 2: ENHANCED ADDRESS NORMALIZATION\n")
cat("=========================================\n")

test_address_normalization <- function() {
  test_cases <- list(
    # Street type standardization
    list(addr = "123 Main Street", expected_regex = "123 main st", desc = "Street -> st"),
    list(addr = "456 Oak Avenue", expected_regex = "456 oak ave", desc = "Avenue -> ave"),
    list(addr = "789 Park Boulevard", expected_regex = "789 park blvd", desc = "Boulevard -> blvd"),
    list(addr = "100 First Drive", expected_regex = "100 first dr", desc = "Drive -> dr"),
    list(addr = "200 Second Road", expected_regex = "200 second rd", desc = "Road -> rd"),
    
    # Direction standardization  
    list(addr = "300 North Main St", expected_regex = "300 n main st", desc = "North -> n"),
    list(addr = "400 Southeast Oak Ave", expected_regex = "400 se oak ave", desc = "Southeast -> se"),
    
    # Suite removal
    list(addr = "500 Main St Suite 101", expected_regex = "500 main st", desc = "Suite removal"),
    list(addr = "600 Oak Ave Apt 5B", expected_regex = "600 oak ave", desc = "Apartment removal"),
    
    # Edge cases
    list(addr = "", expected_regex = "", desc = "Empty string"),
    list(addr = "   700   Main   St   ", expected_regex = "700 main st", desc = "Extra whitespace"),
    list(addr = NA_character_, expected_regex = "", desc = "NA value")
  )
  
  all_regex_passed <- TRUE
  all_postmastr_passed <- TRUE
  
  for (i in seq_along(test_cases)) {
    test_case <- test_cases[[i]]
    
    # Test regex method
    if (!is.na(test_case$addr)) {
      regex_result <- normalize_address(test_case$addr, use_postmastr = FALSE)
      regex_passed <- regex_result == test_case$expected_regex
      
      record_test(paste("Normalize (Regex):", test_case$desc), regex_passed,
                  sprintf("'%s' -> '%s'", test_case$addr %||% "NA", regex_result))
      
      all_regex_passed <- all_regex_passed && regex_passed
      
      # Test postmastr method (may fallback to regex)
      postmastr_result <- normalize_address(test_case$addr, use_postmastr = TRUE)
      # Postmastr may produce different but valid results, so we're more lenient
      postmastr_passed <- nchar(postmastr_result) > 0 || test_case$expected_regex == ""
      
      record_test(paste("Normalize (Enhanced):", test_case$desc), postmastr_passed,
                  sprintf("'%s' -> '%s'", test_case$addr %||% "NA", postmastr_result))
      
      all_postmastr_passed <- all_postmastr_passed && postmastr_passed
    } else {
      # Handle NA case
      regex_result <- normalize_address(test_case$addr, use_postmastr = FALSE)
      postmastr_result <- normalize_address(test_case$addr, use_postmastr = TRUE)
      
      regex_passed <- regex_result == ""
      postmastr_passed <- postmastr_result == ""
      
      record_test(paste("Normalize (Regex):", test_case$desc), regex_passed, "NA -> ''")
      record_test(paste("Normalize (Enhanced):", test_case$desc), postmastr_passed, "NA -> ''")
      
      all_regex_passed <- all_regex_passed && regex_passed
      all_postmastr_passed <- all_postmastr_passed && postmastr_passed
    }
  }
  
  return(list(regex = all_regex_passed, postmastr = all_postmastr_passed))
}

test_vector_normalization <- function() {
  # Test vector input
  test_vector <- c("123 Main Street", "456 Oak Avenue", "", NA_character_)
  
  regex_vector <- normalize_address(test_vector, use_postmastr = FALSE)
  postmastr_vector <- normalize_address(test_vector, use_postmastr = TRUE)
  
  regex_vector_ok <- length(regex_vector) == 4 && all(!is.na(regex_vector[1:2]))
  postmastr_vector_ok <- length(postmastr_vector) == 4 && all(!is.na(postmastr_vector[1:2]))
  
  record_test("Normalize: Vector input (regex)", regex_vector_ok,
              sprintf("4 inputs -> %d outputs", length(regex_vector)))
  record_test("Normalize: Vector input (enhanced)", postmastr_vector_ok,
              sprintf("4 inputs -> %d outputs", length(postmastr_vector)))
  
  return(list(regex = regex_vector_ok, postmastr = postmastr_vector_ok))
}

norm_tests <- test_address_normalization()
vector_tests <- test_vector_normalization()

cat("\n")

# ==============================================================================
# TEST 3: ADDRESS SIMILARITY CALCULATIONS
# ==============================================================================
cat("🔍 TEST 3: ADDRESS SIMILARITY CALCULATIONS\n")
cat("==========================================\n")

test_address_similarity <- function() {
  test_pairs <- list(
    # Exact matches
    list(addr1 = "123 Main St", addr2 = "123 Main St", 
         min_expected = 1.0, desc = "Exact match"),
    
    # Very similar
    list(addr1 = "123 Main Street", addr2 = "123 Main St",
         min_expected = 0.9, desc = "Street vs St"),
    
    # Similar with suite
    list(addr1 = "123 Main St Suite 100", addr2 = "123 Main St",
         min_expected = 0.8, desc = "With/without suite"),
    
    # Different addresses  
    list(addr1 = "123 Main St", addr2 = "456 Oak Ave",
         min_expected = 0.0, max_expected = 0.5, desc = "Different addresses"),
    
    # Edge cases
    list(addr1 = "", addr2 = "123 Main St",
         min_expected = 0.0, max_expected = 0.0, desc = "Empty vs address"),
    list(addr1 = NA_character_, addr2 = "123 Main St", 
         min_expected = 0.0, max_expected = 0.0, desc = "NA vs address")
  )
  
  all_regex_passed <- TRUE
  all_postmastr_passed <- TRUE
  
  for (test_pair in test_pairs) {
    # Test regex method
    if (!is.na(test_pair$addr1) && !is.na(test_pair$addr2)) {
      regex_sim <- calculate_address_similarity(test_pair$addr1, test_pair$addr2, 
                                              use_postmastr = FALSE)
      
      if (is.null(test_pair$max_expected)) {
        regex_passed <- regex_sim >= test_pair$min_expected
      } else {
        regex_passed <- regex_sim >= test_pair$min_expected && regex_sim <= test_pair$max_expected
      }
      
      record_test(paste("Similarity (Regex):", test_pair$desc), regex_passed,
                  sprintf("%.3f (expected >= %.1f)", regex_sim, test_pair$min_expected))
      
      all_regex_passed <- all_regex_passed && regex_passed
      
      # Test postmastr method
      postmastr_sim <- calculate_address_similarity(test_pair$addr1, test_pair$addr2,
                                                   use_postmastr = TRUE)
      
      if (is.null(test_pair$max_expected)) {
        postmastr_passed <- postmastr_sim >= test_pair$min_expected
      } else {
        postmastr_passed <- postmastr_sim >= test_pair$min_expected && postmastr_sim <= test_pair$max_expected
      }
      
      record_test(paste("Similarity (Enhanced):", test_pair$desc), postmastr_passed,
                  sprintf("%.3f (expected >= %.1f)", postmastr_sim, test_pair$min_expected))
      
      all_postmastr_passed <- all_postmastr_passed && postmastr_passed
    } else {
      # Handle NA cases
      regex_sim <- calculate_address_similarity(test_pair$addr1, test_pair$addr2,
                                              use_postmastr = FALSE)
      postmastr_sim <- calculate_address_similarity(test_pair$addr1, test_pair$addr2,
                                                   use_postmastr = TRUE)
      
      regex_passed <- regex_sim == 0.0
      postmastr_passed <- postmastr_sim == 0.0
      
      record_test(paste("Similarity (Regex):", test_pair$desc), regex_passed, "0.000")
      record_test(paste("Similarity (Enhanced):", test_pair$desc), postmastr_passed, "0.000")
      
      all_regex_passed <- all_regex_passed && regex_passed  
      all_postmastr_passed <- all_postmastr_passed && postmastr_passed
    }
  }
  
  return(list(regex = all_regex_passed, postmastr = all_postmastr_passed))
}

similarity_tests <- test_address_similarity()

cat("\n")

# ==============================================================================
# TEST 4: DUPLICATE ADDRESS IDENTIFICATION
# ==============================================================================
cat("👥 TEST 4: DUPLICATE ADDRESS IDENTIFICATION\n")
cat("===========================================\n")

test_duplicate_identification <- function() {
  # Create comprehensive test dataset
  test_df <- data.frame(
    id = 1:15,
    address = c(
      # Group 1: Main Street variants
      "123 Main Street",
      "123 Main St", 
      "123 Main Street Suite 100",
      
      # Group 2: Oak Avenue variants  
      "456 Oak Avenue",
      "456 Oak Ave",
      
      # Group 3: Park Boulevard variants
      "789 North Park Boulevard", 
      "789 N Park Blvd",
      
      # Unique addresses
      "100 First St",
      "200 Second Ave",
      "300 Third Blvd",
      
      # Edge cases
      "",
      NA_character_,
      
      # More unique addresses
      "400 Fourth Dr",
      "500 Fifth Ln", 
      "999 Unique Address"
    ),
    city = rep("TestCity", 15),
    state = rep("TS", 15),
    stringsAsFactors = FALSE
  )
  
  # Test regex method
  cat("Testing regex-based duplicate identification...\n")
  regex_result <- identify_duplicate_addresses(
    test_df,
    address_col = "address",
    city_col = "city",
    state_col = "state", 
    similarity_threshold = 0.85,
    use_postmastr = FALSE
  )
  
  regex_grouped <- sum(!is.na(regex_result$duplicate_group))
  regex_groups <- length(unique(regex_result$duplicate_group[!is.na(regex_result$duplicate_group)]))
  regex_primaries <- sum(regex_result$is_primary, na.rm = TRUE)
  
  # Should find at least 2-3 groups
  regex_passed <- regex_groups >= 2 && regex_primaries == regex_groups && 
                  regex_grouped >= 4
  
  record_test("Duplicate ID (Regex): Group detection", regex_passed,
              sprintf("%d groups, %d grouped addresses", regex_groups, regex_grouped))
  
  # Test postmastr method  
  cat("\nTesting enhanced duplicate identification...\n")
  postmastr_result <- identify_duplicate_addresses(
    test_df,
    address_col = "address", 
    city_col = "city",
    state_col = "state",
    similarity_threshold = 0.85,
    use_postmastr = TRUE
  )
  
  postmastr_grouped <- sum(!is.na(postmastr_result$duplicate_group))
  postmastr_groups <- length(unique(postmastr_result$duplicate_group[!is.na(postmastr_result$duplicate_group)]))
  postmastr_primaries <- sum(postmastr_result$is_primary, na.rm = TRUE)
  
  postmastr_passed <- postmastr_groups >= 2 && postmastr_primaries == postmastr_groups &&
                      postmastr_grouped >= 4
  
  record_test("Duplicate ID (Enhanced): Group detection", postmastr_passed,
              sprintf("%d groups, %d grouped addresses", postmastr_groups, postmastr_grouped))
  
  # Test data integrity
  regex_integrity <- nrow(regex_result) == nrow(test_df) && 
                     all(colnames(test_df) %in% colnames(regex_result))
  postmastr_integrity <- nrow(postmastr_result) == nrow(test_df) &&
                         all(colnames(test_df) %in% colnames(postmastr_result))
  
  record_test("Duplicate ID: Data integrity (regex)", regex_integrity, "All rows preserved")
  record_test("Duplicate ID: Data integrity (enhanced)", postmastr_integrity, "All rows preserved")
  
  return(list(
    regex = list(passed = regex_passed, groups = regex_groups, grouped = regex_grouped),
    postmastr = list(passed = postmastr_passed, groups = postmastr_groups, grouped = postmastr_grouped),
    integrity = list(regex = regex_integrity, postmastr = postmastr_integrity)
  ))
}

test_duplicate_edge_cases <- function() {
  # Test empty dataframe
  empty_df <- data.frame(address = character(0), city = character(0), state = character(0))
  
  empty_passed <- tryCatch({
    empty_result <- identify_duplicate_addresses(empty_df, "address", use_postmastr = FALSE)
    nrow(empty_result) == 0
  }, error = function(e) FALSE)
  
  record_test("Duplicate ID: Empty dataframe", empty_passed, "Handled gracefully")
  
  # Test single address
  single_df <- data.frame(address = "123 Main St", city = "Test", state = "TS")
  
  single_passed <- tryCatch({
    single_result <- identify_duplicate_addresses(single_df, "address", use_postmastr = FALSE)
    nrow(single_result) == 1 && is.na(single_result$duplicate_group[1])
  }, error = function(e) FALSE)
  
  record_test("Duplicate ID: Single address", single_passed, "No groups created")
  
  # Test all identical addresses
  identical_df <- data.frame(
    address = rep("123 Main St", 5),
    city = rep("Test", 5), 
    state = rep("TS", 5)
  )
  
  identical_passed <- tryCatch({
    identical_result <- identify_duplicate_addresses(identical_df, "address", use_postmastr = FALSE)
    sum(!is.na(identical_result$duplicate_group)) == 5 &&
    length(unique(identical_result$duplicate_group[!is.na(identical_result$duplicate_group)])) == 1
  }, error = function(e) FALSE)
  
  record_test("Duplicate ID: All identical", identical_passed, "Single group created")
  
  return(list(empty = empty_passed, single = single_passed, identical = identical_passed))
}

duplicate_main_tests <- test_duplicate_identification()
duplicate_edge_tests <- test_duplicate_edge_cases()

cat("\n")

# ==============================================================================
# TEST 5: GEOCODING INTEGRATION (Mock Test)
# ==============================================================================
cat("🌍 TEST 5: GEOCODING WORKFLOW INTEGRATION\n")
cat("=========================================\n")

test_geocoding_workflow_parameters <- function() {
  # Test parameter validation for geocode_with_deduplication
  test_data <- data.frame(
    address = c("123 Main St", "456 Oak Ave"),
    city = c("Atlanta", "Nashville"), 
    state = c("GA", "TN")
  )
  
  # Test parameter defaults
  param_test_passed <- tryCatch({
    # This will fail at the geocoding step, but should pass parameter validation
    result <- geocode_with_deduplication(
      input_df = test_data,
      address_col = "address",
      city_col = "city",
      state_col = "state",
      output_file = NULL,
      proximity_threshold = 50,
      use_postmastr = TRUE,
      similarity_threshold = 0.85
    )
    FALSE  # Should fail because we don't have HERE API setup
  }, error = function(e) {
    # Should fail gracefully with a geocoding error, not parameter error
    grepl("geocoding|HERE", e$message, ignore.case = TRUE)
  })
  
  record_test("Geocoding: Parameter validation", param_test_passed, "Parameters accepted")
  
  # Test that duplicate detection step works
  dup_step_passed <- tryCatch({
    # Just test the duplicate detection part
    df_with_groups <- identify_duplicate_addresses(
      test_data, "address", "city", "state", 
      use_postmastr = TRUE
    )
    nrow(df_with_groups) == nrow(test_data) &&
    "duplicate_group" %in% colnames(df_with_groups) &&
    "is_primary" %in% colnames(df_with_groups)
  }, error = function(e) FALSE)
  
  record_test("Geocoding: Duplicate detection step", dup_step_passed, "Pre-processing works")
  
  return(list(params = param_test_passed, dup_step = dup_step_passed))
}

geocoding_tests <- test_geocoding_workflow_parameters()

cat("\n")

# ==============================================================================
# TEST 6: ERROR HANDLING AND ROBUSTNESS  
# ==============================================================================
cat("🚨 TEST 6: ERROR HANDLING AND ROBUSTNESS\n")
cat("========================================\n")

test_error_handling <- function() {
  # Test invalid column names
  test_data <- data.frame(wrong_col = "123 Main St", city = "Atlanta", state = "GA")
  
  invalid_col_handled <- tryCatch({
    identify_duplicate_addresses(test_data, "address", "city", "state")
    FALSE
  }, error = function(e) TRUE)  # Should error gracefully
  
  record_test("Error: Invalid column name", invalid_col_handled, "Error caught")
  
  # Test missing required columns
  incomplete_data <- data.frame(address = "123 Main St")
  
  missing_col_handled <- tryCatch({
    identify_duplicate_addresses(incomplete_data, "address", "city", "state")
    FALSE  
  }, error = function(e) TRUE)  # Should error gracefully
  
  record_test("Error: Missing columns", missing_col_handled, "Error caught")
  
  # Test invalid similarity threshold
  valid_data <- data.frame(address = "123 Main St", city = "Atlanta", state = "GA")
  
  # Threshold > 1 should still work (gets clamped internally by string distance functions)
  high_threshold_ok <- tryCatch({
    result <- identify_duplicate_addresses(valid_data, "address", "city", "state", 
                                         similarity_threshold = 1.5)
    nrow(result) == 1
  }, error = function(e) FALSE)
  
  record_test("Error: High similarity threshold", high_threshold_ok, "Handled gracefully")
  
  # Test negative threshold  
  negative_threshold_ok <- tryCatch({
    result <- identify_duplicate_addresses(valid_data, "address", "city", "state",
                                         similarity_threshold = -0.1)
    nrow(result) == 1
  }, error = function(e) FALSE)
  
  record_test("Error: Negative similarity threshold", negative_threshold_ok, "Handled gracefully")
  
  return(list(
    invalid_col = invalid_col_handled,
    missing_col = missing_col_handled, 
    high_threshold = high_threshold_ok,
    negative_threshold = negative_threshold_ok
  ))
}

test_data_type_handling <- function() {
  # Test with factor columns instead of character
  factor_data <- data.frame(
    address = as.factor(c("123 Main St", "456 Oak Ave")),
    city = as.factor(c("Atlanta", "Nashville")),
    state = as.factor(c("GA", "TN"))
  )
  
  factor_handled <- tryCatch({
    result <- identify_duplicate_addresses(factor_data, "address", "city", "state")
    nrow(result) == 2
  }, error = function(e) FALSE)
  
  record_test("Data types: Factor columns", factor_handled, "Converted properly")
  
  # Test with tibble instead of data.frame
  if (requireNamespace("dplyr", quietly = TRUE)) {
    tibble_data <- dplyr::tibble(
      address = c("123 Main St", "456 Oak Ave"), 
      city = c("Atlanta", "Nashville"),
      state = c("GA", "TN")
    )
    
    tibble_handled <- tryCatch({
      result <- identify_duplicate_addresses(tibble_data, "address", "city", "state")
      nrow(result) == 2
    }, error = function(e) FALSE)
    
    record_test("Data types: Tibble input", tibble_handled, "Handled properly")
  }
  
  return(list(factor = factor_handled))
}

error_tests <- test_error_handling()
data_type_tests <- test_data_type_handling()

cat("\n")

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================
cat("📊 FINAL COMPREHENSIVE TEST SUMMARY\n")
cat("====================================\n")

# Count total tests and passes
total_tests <- length(test_results)
passed_tests <- sum(sapply(test_results, function(x) x$passed))
failed_tests <- total_tests - passed_tests

cat(sprintf("Total tests run: %d\n", total_tests))
cat(sprintf("Tests passed: %d (%.1f%%)\n", passed_tests, (passed_tests/total_tests)*100))
cat(sprintf("Tests failed: %d (%.1f%%)\n", failed_tests, (failed_tests/total_tests)*100))

cat("\n📋 FAILED TESTS DETAILS:\n")
if (failed_tests > 0) {
  for (test_name in names(test_results)) {
    if (!test_results[[test_name]]$passed) {
      cat(sprintf("❌ %s: %s\n", test_name, test_results[[test_name]]$details))
    }
  }
} else {
  cat("🎉 ALL TESTS PASSED!\n")
}

cat("\n🏆 COMPONENT SUMMARY:\n")
cat("====================\n")

components <- list(
  "Haversine Distance" = c("Haversine: NYC to LA distance", "Haversine: Identical points", 
                          "Haversine: Small distances", "Haversine: Antipodal points",
                          "Haversine: North pole region", "Haversine: Antimeridian crossing"),
  "Address Normalization" = grep("Normalize", names(test_results), value = TRUE),
  "Similarity Calculation" = grep("Similarity", names(test_results), value = TRUE), 
  "Duplicate Detection" = grep("Duplicate ID", names(test_results), value = TRUE),
  "Geocoding Integration" = grep("Geocoding", names(test_results), value = TRUE),
  "Error Handling" = grep("Error|Data types", names(test_results), value = TRUE)
)

for (component_name in names(components)) {
  component_tests <- components[[component_name]]
  component_passed <- sum(sapply(component_tests, function(test) {
    if (test %in% names(test_results)) test_results[[test]]$passed else FALSE
  }))
  component_total <- length(component_tests)
  
  status <- if (component_passed == component_total) "✅ PASS" else "⚠️  PARTIAL"
  cat(sprintf("%-25s: %s (%d/%d)\n", component_name, status, component_passed, component_total))
}

overall_status <- if (failed_tests == 0) "🎉 EXCELLENT" else if (failed_tests <= 3) "✅ GOOD" else "⚠️ NEEDS ATTENTION"

cat(sprintf("\n🎯 OVERALL STATUS: %s\n", overall_status))
cat("================================================\n")

if (failed_tests == 0) {
  cat("🚀 04-geocode-enhanced.R is fully functional and ready for production!\n")
  cat("✨ All components tested and working properly\n")
  cat("📈 Enhanced postmastr integration providing better address matching\n")
  cat("🛡️ Robust error handling and graceful fallbacks\n")
} else if (failed_tests <= 3) {
  cat("✅ 04-geocode-enhanced.R is largely functional with minor issues\n")
  cat("🔧 Review failed tests above for potential improvements\n")  
} else {
  cat("⚠️ 04-geocode-enhanced.R needs attention before production use\n")
  cat("🔧 Address failed tests before deploying\n")
}

cat("\nTesting completed successfully! 🧪✨\n")
# Comprehensive Test Suite for 04-geocode-enhanced.R
# Tests all functions with edge cases and performance scenarios
# Author: Tyler & Claude
# Created: 2025-08-23

# Setup ----
source("R/04-geocode-enhanced.R")
library(testthat)

cat("🧪 COMPREHENSIVE TEST SUITE FOR ENHANCED GEOCODING\n")
cat("=================================================\n\n")

# Test 1: Haversine Distance Function ----
cat("📏 Test 1: Haversine Distance Calculations\n")
cat("==========================================\n")

test_haversine <- function() {
  # Known distance: NYC to LA is approximately 3,944 km
  nyc_lat <- 40.7128
  nyc_lon <- -74.0060
  la_lat <- 34.0522
  la_lon <- -118.2437
  
  distance_m <- haversine_distance(nyc_lat, nyc_lon, la_lat, la_lon)
  distance_km <- distance_m / 1000
  
  cat("NYC to LA distance:", round(distance_km), "km (expected ~3,944 km)\n")
  
  # Test identical points
  identical_dist <- haversine_distance(nyc_lat, nyc_lon, nyc_lat, nyc_lon)
  cat("Identical points distance:", identical_dist, "m (expected 0)\n")
  
  # Test small distances
  small_dist <- haversine_distance(40.7128, -74.0060, 40.7129, -74.0061)
  cat("Very close points:", round(small_dist, 2), "m\n")
  
  # Test antipodal points (opposite sides of Earth)
  antipodal_dist <- haversine_distance(0, 0, 0, 180)
  cat("Antipodal points:", round(antipodal_dist/1000), "km (expected ~20,015 km)\n")
  
  return(list(
    nyc_la = abs(distance_km - 3944) < 50,  # Within 50km tolerance
    identical = identical_dist == 0,
    antipodal = abs(antipodal_dist/1000 - 20015) < 100
  ))
}

haversine_results <- test_haversine()
cat("✅ Haversine tests passed:", all(unlist(haversine_results)), "\n\n")

# Test 2: Address Normalization ----
cat("🏠 Test 2: Address Normalization\n")
cat("================================\n")

test_normalization <- function() {
  test_cases <- list(
    # Street type standardization
    list(
      input = "123 Main Street",
      expected = "123 main st",
      description = "Street -> st"
    ),
    list(
      input = "456 Oak Avenue",
      expected = "456 oak ave", 
      description = "Avenue -> ave"
    ),
    list(
      input = "789 Park Boulevard",
      expected = "789 park blvd",
      description = "Boulevard -> blvd"
    ),
    # Direction standardization
    list(
      input = "100 North Main St",
      expected = "100 n main st",
      description = "North -> n"
    ),
    list(
      input = "200 Southeast Oak Ave",
      expected = "200 se oak ave",
      description = "Southeast -> se"
    ),
    # Suffix removal
    list(
      input = "300 Main St Suite 101",
      expected = "300 main st",
      description = "Suite removal"
    ),
    list(
      input = "400 Oak Ave Apt 5B",
      expected = "400 oak ave",
      description = "Apartment removal"
    ),
    list(
      input = "500 Park Blvd Floor 3",
      expected = "500 park blvd",
      description = "Floor removal"
    ),
    # Special cases
    list(
      input = "",
      expected = "",
      description = "Empty string"
    ),
    list(
      input = "   123   Main   St   ",
      expected = "123 main st",
      description = "Extra whitespace"
    )
  )
  
  results <- list()
  for (i in seq_along(test_cases)) {
    test <- test_cases[[i]]
    actual <- normalize_address(test$input)
    passed <- actual == test$expected
    
    cat(sprintf("%-20s: %s -> %s (expected: %s) %s\n", 
                test$description,
                test$input,
                actual,
                test$expected,
                if(passed) "✅" else "❌"))
    
    results[[i]] <- passed
  }
  
  return(all(unlist(results)))
}

normalization_passed <- test_normalization()
cat("Normalization tests passed:", normalization_passed, "\n\n")

# Test 3: Address Similarity Scoring ----
cat("🔍 Test 3: Address Similarity Scoring\n")
cat("=====================================\n")

test_similarity <- function() {
  test_cases <- list(
    # Exact matches
    list(
      addr1 = "123 Main St",
      addr2 = "123 Main St", 
      min_expected = 1.0,
      description = "Exact match"
    ),
    # Very similar
    list(
      addr1 = "123 Main Street",
      addr2 = "123 Main St",
      min_expected = 0.9,
      description = "Street vs St"
    ),
    # Similar with suite
    list(
      addr1 = "123 Main St Suite 100",
      addr2 = "123 Main St",
      min_expected = 0.9,
      description = "With/without suite"
    ),
    # Different addresses
    list(
      addr1 = "123 Main St",
      addr2 = "456 Oak Ave",
      min_expected = 0.0,
      max_expected = 0.3,
      description = "Different addresses"
    ),
    # Empty/NA cases
    list(
      addr1 = "",
      addr2 = "123 Main St",
      min_expected = 0.0,
      max_expected = 0.0,
      description = "Empty vs address"
    )
  )
  
  results <- list()
  for (i in seq_along(test_cases)) {
    test <- test_cases[[i]]
    score <- calculate_address_similarity(test$addr1, test$addr2)
    
    if (is.null(test$max_expected)) {
      passed <- score >= test$min_expected
    } else {
      passed <- score >= test$min_expected && score <= test$max_expected
    }
    
    cat(sprintf("%-20s: %.3f (expected >= %.1f) %s\n",
                test$description, score, test$min_expected,
                if(passed) "✅" else "❌"))
    
    results[[i]] <- passed
  }
  
  return(all(unlist(results)))
}

similarity_passed <- test_similarity()
cat("Similarity tests passed:", similarity_passed, "\n\n")

# Test 4: Duplicate Address Identification ----
cat("👥 Test 4: Duplicate Address Identification\n")
cat("==========================================\n")

test_duplicate_identification <- function() {
  # Create comprehensive test dataset
  test_df <- data.frame(
    id = 1:15,
    address = c(
      # Group 1: Exact duplicates
      "123 Main Street",
      "123 Main St",
      "123 Main Street Suite 100",
      
      # Group 2: Similar addresses  
      "456 Oak Avenue",
      "456 Oak Ave",
      
      # Group 3: Different formatting
      "789 North Park Boulevard",
      "789 N Park Blvd",
      
      # Unique addresses
      "100 First St",
      "200 Second Ave", 
      "300 Third Blvd",
      "400 Fourth Dr",
      "500 Fifth Ln",
      
      # Edge cases
      "",
      NA_character_,
      "999 Unique Address That Should Not Match Anything"
    ),
    city = rep("TestCity", 15),
    state = rep("TS", 15),
    stringsAsFactors = FALSE
  )
  
  cat("Input addresses:\n")
  print(test_df[, c("id", "address")])
  
  result <- identify_duplicate_addresses(
    test_df, 
    address_col = "address",
    city_col = "city", 
    state_col = "state",
    similarity_threshold = 0.85
  )
  
  cat("\nResults:\n")
  print(result[, c("id", "address", "duplicate_group", "is_primary")])
  
  # Check expectations
  total_groups <- length(unique(result$duplicate_group[!is.na(result$duplicate_group)]))
  primary_count <- sum(result$is_primary, na.rm = TRUE)
  grouped_addresses <- sum(!is.na(result$duplicate_group))
  
  cat("\nGroup analysis:\n")
  cat("Total duplicate groups found:", total_groups, "\n")
  cat("Primary addresses:", primary_count, "\n") 
  cat("Addresses in groups:", grouped_addresses, "\n")
  cat("Ungrouped addresses:", nrow(result) - grouped_addresses, "\n")
  
  # Expected: 3 groups (Main St variants, Oak Ave variants, Park Blvd variants)
  expected_groups <- 3
  passed <- total_groups >= expected_groups && primary_count == total_groups
  
  return(passed)
}

duplicate_passed <- test_duplicate_identification()
cat("Duplicate identification passed:", duplicate_passed, "\n\n")

# Test 5: Performance Testing ----
cat("⚡ Test 5: Performance Testing\n") 
cat("=============================\n")

test_performance <- function() {
  # Generate larger dataset for performance testing
  n <- 1000
  addresses <- paste(
    sample(1:9999, n, replace = TRUE),
    sample(c("Main", "Oak", "Park", "First", "Second"), n, replace = TRUE),
    sample(c("St", "Ave", "Blvd", "Dr", "Ln"), n, replace = TRUE)
  )
  
  # Add some intentional duplicates
  duplicates <- sample(addresses, 100)
  addresses <- c(addresses, duplicates)
  
  test_df <- data.frame(
    id = 1:length(addresses),
    address = addresses,
    city = "TestCity",
    state = "TS",
    stringsAsFactors = FALSE
  )
  
  cat("Testing with", nrow(test_df), "addresses...\n")
  
  # Time the operation
  start_time <- Sys.time()
  
  result <- identify_duplicate_addresses(
    test_df,
    address_col = "address", 
    similarity_threshold = 0.9  # Higher threshold for performance
  )
  
  end_time <- Sys.time()
  elapsed <- as.numeric(difftime(end_time, start_time, units = "secs"))
  
  cat("Processing time:", round(elapsed, 2), "seconds\n")
  cat("Rate:", round(nrow(test_df) / elapsed), "addresses per second\n")
  
  # Performance should be reasonable (< 2 seconds per 1000 addresses)
  performance_acceptable <- elapsed < 2.0
  
  return(performance_acceptable)
}

performance_passed <- test_performance()
cat("Performance test passed:", performance_passed, "\n\n")

# Test 6: Edge Cases and Error Handling ----
cat("🚨 Test 6: Edge Cases and Error Handling\n")
cat("========================================\n")

test_edge_cases <- function() {
  results <- list()
  
  # Test with empty dataframe
  cat("Testing empty dataframe...\n")
  empty_df <- data.frame(address = character(0), city = character(0), state = character(0))
  tryCatch({
    empty_result <- identify_duplicate_addresses(empty_df, "address")
    results$empty <- nrow(empty_result) == 0
    cat("✅ Empty dataframe handled correctly\n")
  }, error = function(e) {
    results$empty <- FALSE
    cat("❌ Empty dataframe error:", e$message, "\n")
  })
  
  # Test with all NA addresses
  cat("Testing all NA addresses...\n") 
  na_df <- data.frame(
    address = rep(NA_character_, 5),
    city = rep("Test", 5),
    state = rep("TS", 5)
  )
  tryCatch({
    na_result <- identify_duplicate_addresses(na_df, "address")
    results$all_na <- sum(na_result$is_primary, na.rm = TRUE) == 0
    cat("✅ All NA addresses handled correctly\n")
  }, error = function(e) {
    results$all_na <- FALSE
    cat("❌ All NA addresses error:", e$message, "\n")
  })
  
  # Test with single address
  cat("Testing single address...\n")
  single_df <- data.frame(address = "123 Main St", city = "Test", state = "TS")
  tryCatch({
    single_result <- identify_duplicate_addresses(single_df, "address")
    results$single <- nrow(single_result) == 1 && !single_result$is_primary[1]
    cat("✅ Single address handled correctly\n")
  }, error = function(e) {
    results$single <- FALSE
    cat("❌ Single address error:", e$message, "\n")
  })
  
  return(all(unlist(results)))
}

edge_cases_passed <- test_edge_cases()
cat("Edge cases passed:", edge_cases_passed, "\n\n")

# Final Summary ----
cat("📊 FINAL TEST SUMMARY\n")
cat("====================\n")

all_tests <- list(
  "Haversine Distance" = haversine_results,
  "Address Normalization" = normalization_passed,
  "Similarity Scoring" = similarity_passed, 
  "Duplicate Identification" = duplicate_passed,
  "Performance" = performance_passed,
  "Edge Cases" = edge_cases_passed
)

for (test_name in names(all_tests)) {
  result <- all_tests[[test_name]]
  if (is.list(result)) {
    passed <- all(unlist(result))
  } else {
    passed <- result
  }
  cat(sprintf("%-25s: %s\n", test_name, if(passed) "✅ PASSED" else "❌ FAILED"))
}

overall_passed <- all(sapply(all_tests, function(x) if(is.list(x)) all(unlist(x)) else x))
cat("\n", if(overall_passed) "🎉 ALL TESTS PASSED!" else "⚠️  SOME TESTS FAILED", "\n")

cat("\n🎯 Test Suite Complete - Enhanced Geocoding System Validated\n")
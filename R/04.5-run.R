
# run ----
# Load the dataset first
source("load_and_test_abog_data.R")

# Now abog_data should be available
head(abog_data)

# Test duplicate detection (fast - no API calls)
suppressWarnings({
  abog_deduplicated <- identify_duplicate_addresses(
    abog_data,
    address_col = "practice_address_combined",
    city_col = "practice_city",
    state_col = "practice_state",
    use_postmastr = TRUE,
    similarity_threshold = 0.75
  )
})

# Check the duplicate detection results
summary(abog_deduplicated)

# Test Haversine distance
dist_meters <- haversine_distance(40.7128, -74.0060, 40.7589, -73.9851)
cat("Distance between NYC points:", round(dist_meters), "meters\n")

write.csv(abog_deduplicated, "data/04-geocode/intermediate/abog_deduplicated_results.csv")
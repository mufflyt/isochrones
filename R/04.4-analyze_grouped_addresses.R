# Analyze Grouped Addresses from ABOG NPI Duplicate Detection
# Show detailed analysis of which addresses were grouped together
# Author: Tyler & Claude

# Make sure we have the deduplicated data
if (!exists("abog_deduplicated")) {
  source("load_and_test_abog_data.R")
  abog_deduplicated <- identify_duplicate_addresses(
    abog_data,
    address_col = "practice_address_combined",
    city_col = "practice_city",
    state_col = "practice_state",
    use_postmastr = TRUE,
    similarity_threshold = 0.75
  )
}

cat("🔍 ANALYZING GROUPED ADDRESSES FROM ABOG NPI DATA\n")
cat("================================================\n\n")

# Get all grouped addresses
grouped_addresses <- abog_deduplicated[!is.na(abog_deduplicated$duplicate_group), ]
cat("📊 Found", nrow(grouped_addresses), "addresses in", 
    length(unique(grouped_addresses$duplicate_group)), "duplicate groups\n\n")

# Create detailed analysis
duplicate_groups <- unique(grouped_addresses$duplicate_group)

# Sort by group size (largest first)
group_sizes <- table(grouped_addresses$duplicate_group)
group_sizes_sorted <- sort(group_sizes, decreasing = TRUE)

cat("📈 GROUP SIZE DISTRIBUTION:\n")
cat("==========================\n")
size_dist <- table(group_sizes)
for (size in names(size_dist)) {
  cat(sprintf("Groups with %s addresses: %d groups\n", size, size_dist[size]))
}
cat("\n")

# Show top 20 largest groups in detail
cat("🏥 TOP 20 LARGEST DUPLICATE GROUPS (Detailed Analysis):\n")
cat("=======================================================\n\n")

top_groups <- names(head(group_sizes_sorted, 20))

for (i in seq_along(top_groups)) {
  group_id <- as.numeric(top_groups[i])
  group_data <- grouped_addresses[grouped_addresses$duplicate_group == group_id, ]
  group_size <- nrow(group_data)
  
  cat(sprintf("GROUP %d (%d addresses):\n", group_id, group_size))
  cat(paste(rep("=", 40), collapse = ""), "\n")
  
  # Show all addresses in this group
  for (j in 1:nrow(group_data)) {
    primary_marker <- if (group_data$is_primary[j]) " ★ PRIMARY" else ""
    cat(sprintf("  %d. %s%s\n", 
                group_data$query_id[j],
                group_data$practice_address_combined[j],
                primary_marker))
    cat(sprintf("     Provider: %s %s (NPI: %s)\n",
                group_data$provider_first_name[j],
                group_data$provider_last_name[j], 
                group_data$npi[j]))
  }
  cat("\n")
  
  # Analyze what makes these similar
  addresses <- group_data$practice_address_combined
  if (length(addresses) >= 2) {
    cat("  🔍 SIMILARITY ANALYSIS:\n")
    
    # Check for common patterns
    common_features <- c()
    
    # Street number patterns
    street_nums <- stringr::str_extract(addresses, "^\\d+")
    if (length(unique(street_nums[!is.na(street_nums)])) == 1) {
      common_features <- c(common_features, paste("Same street number:", unique(street_nums)[1]))
    }
    
    # Street name patterns
    street_parts <- stringr::str_extract(addresses, "\\d+\\s+([^,]+)")
    street_parts <- stringr::str_remove(street_parts, "^\\d+\\s+")
    if (length(unique(street_parts[!is.na(street_parts)])) <= 2) {
      common_features <- c(common_features, "Similar street names")
    }
    
    # City/State patterns
    cities <- unique(group_data$practice_city)
    states <- unique(group_data$practice_state)
    if (length(cities) == 1 && length(states) == 1) {
      common_features <- c(common_features, paste("Same location:", cities[1], states[1]))
    }
    
    # Suite/unit differences
    has_suite <- stringr::str_detect(addresses, "(?i)(suite|ste|unit|apt|#)")
    if (sum(has_suite) > 0 && sum(has_suite) < length(addresses)) {
      common_features <- c(common_features, "Suite/unit variations")
    }
    
    # Zip code differences
    zip_codes <- stringr::str_extract(addresses, "\\d{5,}")
    unique_zips <- unique(zip_codes[!is.na(zip_codes)])
    if (length(unique_zips) > 1) {
      common_features <- c(common_features, paste("Multiple zip codes:", paste(unique_zips, collapse = ", ")))
    }
    
    for (feature in common_features) {
      cat(sprintf("     - %s\n", feature))
    }
  }
  cat("\n")
}

# Save detailed analysis to file
output_file <- "data/04-geocode/output/abog_duplicate_groups_analysis.csv"
dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

# Create a detailed export
grouped_export <- grouped_addresses %>%
  arrange(duplicate_group, desc(is_primary)) %>%
  select(
    duplicate_group, is_primary, query_id,
    provider_first_name, provider_last_name, npi,
    practice_address_combined, practice_city, practice_state,
    original_input_name
  )

write.csv(grouped_export, output_file, row.names = FALSE)
cat("💾 Detailed analysis saved to:", output_file, "\n")

# Summary statistics
cat("\n📊 SUMMARY STATISTICS:\n")
cat("=====================\n")
cat("Total facilities analyzed:", nrow(abog_data), "\n")
cat("Facilities in duplicate groups:", nrow(grouped_addresses), "\n")
cat("Number of duplicate groups:", length(unique(grouped_addresses$duplicate_group)), "\n")
cat("Primary addresses to geocode:", sum(grouped_addresses$is_primary), "\n")
cat("Duplicate addresses (save API calls):", sum(!grouped_addresses$is_primary), "\n")
cat("Potential geocoding cost reduction:", 
    round(sum(!grouped_addresses$is_primary) / nrow(abog_data) * 100, 1), "%\n")

# Group size statistics
cat("\nGroup size statistics:\n")
cat("- Largest group size:", max(group_sizes), "addresses\n")
cat("- Average group size:", round(mean(group_sizes), 1), "addresses\n")
cat("- Most common group size:", names(sort(table(group_sizes), decreasing = TRUE))[1], "addresses\n")

cat("\n🎯 Analysis complete! Review the groups above to validate duplicate detection accuracy.\n")
cat("💡 Large groups may indicate medical centers with multiple providers at same location.\n")
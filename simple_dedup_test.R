# Simple Address Deduplication Test
# Demonstrates the enhanced geocoding deduplication functionality
source("R/04-geocode-enhanced.R")

# Create test dataset with similar addresses
test_data <- data.frame(
  id = 1:6,
  address = c(
    "550 Peachtree St NE, Atlanta, GA 30308",
    "550 Peachtree Street Northeast, Atlanta, GA 30308",  # Similar to #1
    "818 Ellicott St, Buffalo, NY 14203",
    "818 Ellicott Street, Buffalo, NY 14203-1021",        # Similar to #3
    "111 Broadway, New York, NY 10006",
    "5323 Harry Hines Blvd, Dallas, TX 75390"
  ),
  stringsAsFactors = FALSE
)

cat("🧪 Simple Address Deduplication Test\n")
cat("===================================\n\n")

cat("Test addresses:\n")
print(test_data)

cat("\n🔍 Running deduplication...\n")
result <- identify_duplicate_addresses(test_data, address_col = "address")

cat("\nResults with duplicate groups:\n")
print(result)

# Count unique groups
unique_groups <- unique(result$duplicate_group)
original_count <- nrow(test_data)
unique_count <- length(unique_groups)
saved_calls <- original_count - unique_count

cat(sprintf("\n📊 SUMMARY:\n"))
cat(sprintf("Original addresses: %d\n", original_count))
cat(sprintf("Unique groups: %d\n", unique_count)) 
cat(sprintf("API calls saved: %d\n", saved_calls))
cat(sprintf("Cost reduction: %.1f%%\n", (saved_calls/original_count)*100))

cat("\n✅ Deduplication test completed!\n")
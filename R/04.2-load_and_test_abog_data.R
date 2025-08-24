# Load and Test ABOG NPI Data with Enhanced Geocoding
# Author: Tyler & Claude

# Make sure enhanced geocoding is loaded
source("R/04-geocode-enhanced.R")

cat("📊 Loading ABOG NPI dataset...\n")

# Load the dataset
data_path <- "/Users/tmuffly/isochrones/data/02.33-nber_nppes_data/output/abog_npi_matched_8_18_2025.csv"
abog_data <- read.csv(data_path, stringsAsFactors = FALSE)

cat("✅ Dataset loaded successfully!\n")
cat("📋 Dataset info:\n")
cat("   - Total records:", nrow(abog_data), "\n")
cat("   - Columns:", ncol(abog_data), "\n")
cat("   - Address column: 'practice_address_combined'\n")

# Show first few addresses
cat("\n🏥 Sample medical facility addresses:\n")
for (i in 1:min(5, nrow(abog_data))) {
  cat(sprintf("%d. %s\n", i, abog_data$practice_address_combined[i]))
}

cat("\n🎯 Ready for enhanced geocoding!\n")
cat("Next steps:\n")
cat("1. Test duplicate detection: identify_duplicate_addresses()\n") 
cat("2. Full geocoding with deduplication: geocode_with_deduplication()\n")
cat("3. Use optimal settings: similarity_threshold = 0.75\n")

# Make abog_data available in global environment
assign("abog_data", abog_data, envir = .GlobalEnv)
cat("\n📦 'abog_data' is now loaded in your environment!\n")
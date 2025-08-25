# Optimize address list to minimize geocoding calls
library(dplyr)
library(readr)
library(stringr)
library(logger)

# Load existing geocoded data to avoid re-geocoding
log_info("Loading existing geocoded results...")
full_results <- readr::read_csv("data/04-geocode/output/abog_simple_geocoded.csv", show_col_types = FALSE)

# Load unique addresses from previous step
unique_addresses <- readr::read_csv("data/04-geocode/output/physician_addresses_to_geocode.csv", show_col_types = FALSE)

log_info("Original unique addresses: {nrow(unique_addresses)}")
log_info("Existing geocoded addresses: {nrow(full_results)}")

# Function to standardize addresses for proper matching
standardize_address <- function(addr) {
  addr %>%
    toupper() %>%                                    # Convert to uppercase
    str_replace_all(",", "") %>%                     # Remove commas  
    str_replace_all("\\s+", " ") %>%                 # Normalize whitespace
    str_trim()                                       # Trim leading/trailing spaces
}

# Method 1: Use only pm_address (standardized) to reduce duplicates further
minimal_addresses <- unique_addresses %>%
  dplyr::select(pm_address) %>%
  dplyr::distinct() %>%
  dplyr::mutate(address_id = row_number())

log_info("Using pm_address only: {nrow(minimal_addresses)} addresses")

# Method 2: Properly standardize and match addresses
# Standardize addresses from both datasets
full_results_standardized <- full_results %>%
  dplyr::mutate(standardized_address = standardize_address(practice_address_combined)) %>%
  dplyr::select(standardized_address, final_lat, final_lng) %>%
  dplyr::distinct()

minimal_addresses_standardized <- minimal_addresses %>%
  dplyr::mutate(standardized_address = standardize_address(pm_address))

log_info("Checking for matches after proper standardization...")

# Check matches after standardization
already_geocoded <- inner_join(
  minimal_addresses_standardized, 
  full_results_standardized, 
  by = "standardized_address"
)

log_info("Found {nrow(already_geocoded)} addresses that are already geocoded!")

# Get addresses that still need geocoding
addresses_to_geocode_cleaned <- minimal_addresses_standardized %>%
  dplyr::anti_join(full_results_standardized, by = "standardized_address") %>%
  dplyr::select(pm_address, address_id) %>%
  dplyr::rename(address_to_geocode = pm_address)

log_info("After additional cleaning: {nrow(addresses_to_geocode_cleaned)} final addresses to geocode")


# Save the optimized results
readr::write_csv(addresses_to_geocode_cleaned, "data/04-geocode/output/addresses_to_geocode_optimized.csv")
readr::write_csv(already_geocoded, "data/04-geocode/output/addresses_already_geocoded.csv")

# Show reduction
log_info("Reduction summary:")
log_info("  Original: {nrow(unique_addresses)} addresses") 
log_info("  Using pm_address only: {nrow(minimal_addresses)} addresses")
log_info("  Already geocoded matches: {nrow(already_geocoded)} addresses")
log_info("  Still need geocoding: {nrow(addresses_to_geocode_cleaned)} addresses")
log_info("  Total reduction: {round((1 - nrow(addresses_to_geocode_cleaned)/nrow(unique_addresses)) * 100, 1)}%")

log_info("Saved:")
log_info("  Addresses still needed: data/04-geocode/output/addresses_to_geocode_optimized.csv")
log_info("  Already geocoded matches: data/04-geocode/output/addresses_already_geocoded.csv")
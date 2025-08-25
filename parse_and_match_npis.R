#!/usr/bin/env Rscript

# Parse Physician Names and Match NPIs
# Extracts first/last names from physician_name column and searches for NPIs
# Author: Tyler & Claude
# Created: 2025-08-24

library(dplyr)
library(readr)

# Load enhanced matching if available
if (file.exists("enhanced_npi_matching_fixed.R")) {
  source("enhanced_npi_matching_fixed.R")
} else {
  cat("Enhanced matching not available, using basic approach\n")
}

cat("🧠 Parsing Physician Names and Matching NPIs\n")
cat("=============================================\n\n")

# Read the dataset
input_file <- "data/0-Download/output/abog_provider_dataframe_8_17_2025_1531.csv"
data <- readr::read_csv(input_file, show_col_types = FALSE)

cat("📊 Dataset loaded:", nrow(data), "physicians\n\n")

# Parse physician names using a simple approach
cat("🔍 Parsing physician names...\n")

# Function to parse physician names
parse_physician_name <- function(name_string) {
  if (is.na(name_string) || name_string == "") {
    return(list(first = "", last = "", middle = ""))
  }
  
  # Remove common titles and suffixes
  clean_name <- gsub("\\b(Dr\\.?|MD\\.?|PhD\\.?|Prof\\.?)\\b", "", name_string, ignore.case = TRUE)
  clean_name <- trimws(clean_name)
  
  # Split by comma (Last, First format) or spaces
  if (grepl(",", clean_name)) {
    parts <- strsplit(clean_name, ",")[[1]]
    if (length(parts) >= 2) {
      last_name <- trimws(parts[1])
      first_part <- trimws(parts[2])
      first_words <- strsplit(first_part, "\\s+")[[1]]
      first_name <- first_words[1]
      middle_name <- if(length(first_words) > 1) paste(first_words[-1], collapse = " ") else ""
    } else {
      # Fallback for single part
      first_name <- clean_name
      last_name <- ""
      middle_name <- ""
    }
  } else {
    # Space-separated format
    words <- strsplit(clean_name, "\\s+")[[1]]
    words <- words[words != ""]
    
    if (length(words) == 0) {
      first_name <- ""
      last_name <- ""
      middle_name <- ""
    } else if (length(words) == 1) {
      first_name <- words[1]
      last_name <- ""
      middle_name <- ""
    } else if (length(words) == 2) {
      first_name <- words[1]
      last_name <- words[2]
      middle_name <- ""
    } else {
      # 3+ words: First Middle(s) Last
      first_name <- words[1]
      last_name <- words[length(words)]
      middle_name <- paste(words[2:(length(words)-1)], collapse = " ")
    }
  }
  
  return(list(
    first = first_name, 
    last = last_name, 
    middle = middle_name
  ))
}

# Parse all names
parsed_names <- lapply(data$physician_name, parse_physician_name)

# Create a dataset with parsed names
data_with_names <- data %>%
  mutate(
    parsed_first = sapply(parsed_names, function(x) x$first),
    parsed_last = sapply(parsed_names, function(x) x$last),
    parsed_middle = sapply(parsed_names, function(x) x$middle)
  ) %>%
  filter(
    !is.na(parsed_first) & parsed_first != "",
    !is.na(parsed_last) & parsed_last != ""
  )

cat("✅ Successfully parsed", nrow(data_with_names), "physician names\n")
cat("📊 Names with first and last:", nrow(data_with_names), 
    paste0("(", round(nrow(data_with_names)/nrow(data)*100, 1), "%)"), "\n\n")

# Show sample of parsed names
cat("📋 Sample of parsed names:\n")
sample_names <- data_with_names %>%
  select(physician_name, parsed_first, parsed_last, parsed_middle) %>%
  head(10)
print(sample_names)

cat("\n")

# Create output files
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# 1. Save all parsed names
output_file <- paste0("parsed_physician_names_", timestamp, ".csv")
write_csv(data_with_names %>%
  select(abog_id, physician_name, parsed_first, parsed_last, parsed_middle,
         city, state, clinically_active_status, primary_certification),
  output_file)

# 2. Create a test sample for NPI matching (first 50)
test_sample <- data_with_names %>%
  select(abog_id, physician_name, parsed_first, parsed_last, city, state) %>%
  head(50) %>%
  rename(first = parsed_first, last = parsed_last)

test_file <- paste0("npi_test_sample_", timestamp, ".csv")  
write_csv(test_sample, test_file)

cat("📁 Files created:\n")
cat("1. All parsed names:", output_file, "\n")
cat("2. Test sample for NPI matching:", test_file, "\n\n")

# Test NPI matching on small sample if enhanced matching is available
if (exists("enhanced_npi_matching_fixed")) {
  cat("🚀 Testing NPI matching on sample...\n")
  
  tryCatch({
    results <- enhanced_npi_matching_fixed(
      input_file = test_file,
      max_records = 10,
      confidence_threshold = 75
    )
    
    cat("✅ NPI matching test completed!\n")
    cat("Match rate:", results$overall_match_rate, "%\n")
    
  }, error = function(e) {
    cat("❌ NPI matching test failed:", e$message, "\n")
  })
}

cat("\n🎯 Summary:\n")
cat("============\n")
cat("Total physicians:", nrow(data), "\n")
cat("Successfully parsed:", nrow(data_with_names), "\n")
cat("Parse success rate:", round(nrow(data_with_names)/nrow(data)*100, 1), "%\n")
cat("\n💡 Next steps:\n")
cat("1. Review parsed names in:", output_file, "\n")
cat("2. Use", test_file, "for NPI matching testing\n")
cat("3. Run enhanced NPI matching on full dataset when ready\n")

# Return results
list(
  total_physicians = nrow(data),
  successfully_parsed = nrow(data_with_names),
  parse_success_rate = round(nrow(data_with_names)/nrow(data)*100, 1),
  files_created = c(output_file, test_file)
)
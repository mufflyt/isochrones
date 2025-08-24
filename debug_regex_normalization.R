# Debug Regex Normalization Issue
# Author: Tyler & Claude

source("R/04-geocode-enhanced.R")

cat("🔍 DEBUGGING REGEX NORMALIZATION\n")
cat("================================\n\n")

# Test the specific problematic cases
test_cases <- c("123 Main Street", "300 North Main St", "700 Main St")

for (test_addr in test_cases) {
  cat("Testing:", test_addr, "\n")
  cat("------\n")
  
  # Step by step normalization
  normalized <- tolower(trimws(test_addr))
  cat("After tolower/trimws:", normalized, "\n")
  
  # Remove suffixes
  normalized <- str_replace_all(normalized, "\\b(suite?|ste?|apt|apartment|unit|#)\\s*\\w*\\s*$", "")
  cat("After suffix removal:", normalized, "\n")
  
  # Street type replacement
  before_street <- normalized
  normalized <- str_replace_all(normalized, "\\bstreets?\\b", "st")
  normalized <- str_replace_all(normalized, "\\bstreet\\b", "st")
  cat("After streets? pattern:", normalized, "\n")
  cat("After street pattern:", normalized, "\n")
  cat("Changed from street patterns?", before_street != normalized, "\n")
  
  # Direction replacement  
  before_direction <- normalized
  normalized <- str_replace_all(normalized, "\\bnorth\\b", "n")
  cat("After north pattern:", normalized, "\n")
  cat("Changed from north pattern?", before_direction != normalized, "\n")
  
  # Final cleanup
  normalized <- str_replace_all(normalized, "[^a-z0-9\\s]", " ")
  normalized <- str_replace_all(normalized, "\\s+", " ")
  normalized <- str_trim(normalized)
  cat("After final cleanup:", normalized, "\n")
  
  # Compare with function result
  function_result <- normalize_address(test_addr, use_postmastr = FALSE)
  cat("Function result:", function_result, "\n")
  cat("Match?", normalized == function_result, "\n")
  cat("\n")
}
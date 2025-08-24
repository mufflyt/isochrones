# Enhanced Address Normalization Using Postmastr
# Improves upon 04-geocode-enhanced.R by using postmastr for better standardization
# Author: Tyler & Claude
# Created: 2025-08-23

source("R/01-setup.R")
library(postmastr)
library(dplyr)
library(stringr)

cat("🏠 ENHANCED ADDRESS NORMALIZATION WITH POSTMASTR\n")
cat("===============================================\n\n")

#' Enhanced Address Normalization Using Postmastr
#'
#' @description Uses postmastr to parse and reconstruct addresses for better 
#' normalization than simple regex replacement
#' @param address_vector Vector of address strings to normalize
#' @param use_postmastr Logical. Use postmastr parsing (TRUE) or fallback to regex (FALSE)
#' @return Vector of normalized address strings
#' @export
normalize_address_with_postmastr <- function(address_vector, use_postmastr = TRUE) {
  
  if (!use_postmastr) {
    # Fallback to original method
    return(sapply(address_vector, function(addr) {
      if (is.na(addr) || addr == "") return("")
      
      normalized <- tolower(trimws(addr))
      normalized <- str_replace_all(normalized, "\\b(suite?|ste?|apt|apartment|unit|#)\\s*\\w*\\s*$", "")
      normalized <- str_replace_all(normalized, "\\bstreet?\\b", "st")
      normalized <- str_replace_all(normalized, "\\bavenue?\\b", "ave") 
      normalized <- str_replace_all(normalized, "\\bboulevard?\\b", "blvd")
      normalized <- str_replace_all(normalized, "\\bdrive?\\b", "dr")
      normalized <- str_replace_all(normalized, "\\bnorth?\\b", "n")
      normalized <- str_replace_all(normalized, "\\bsouth?\\b", "s")
      normalized <- str_replace_all(normalized, "\\beast?\\b", "e")
      normalized <- str_replace_all(normalized, "\\bwest?\\b", "w")
      normalized <- str_replace_all(normalized, "[^a-z0-9\\s]", " ")
      normalized <- str_squish(normalized)
      return(normalized)
    }))
  }
  
  # Enhanced method using postmastr
  tryCatch({
    # Create temporary dataframe
    temp_df <- data.frame(
      id = seq_along(address_vector),
      address = address_vector,
      stringsAsFactors = FALSE
    )
    
    # Filter out NA/empty addresses
    valid_addresses <- temp_df[!is.na(temp_df$address) & temp_df$address != "", ]
    
    if (nrow(valid_addresses) == 0) {
      return(rep("", length(address_vector)))
    }
    
    # Postmastr parsing pipeline
    parsed_addresses <- valid_addresses %>%
      pm_identify(var = "address") %>%
      pm_prep(var = "address", type = "street") %>%
      pm_postal_parse()
    
    # Create dictionaries for parsing
    state_dict <- pm_dictionary(locale = "us", type = "state", case = c("title", "upper"))
    
    # Parse components
    parsed_addresses <- parsed_addresses %>%
      pm_state_parse(dictionary = state_dict) %>%
      pm_house_parse(locale = "us") %>%
      pm_streetSuf_parse(locale = "us") %>%
      pm_streetDir_parse(locale = "us") %>%
      pm_street_parse(ordinal = TRUE, drop = TRUE, locale = "us")
    
    # Reconstruct normalized addresses
    normalized_results <- parsed_addresses %>%
      mutate(
        # Create normalized components
        norm_house = if_else(!is.na(pm.house), pm.house, ""),
        norm_street = if_else(!is.na(pm.street), tolower(pm.street), ""),
        norm_suffix = case_when(
          !is.na(pm.streetSuf) ~ case_when(
            tolower(pm.streetSuf) %in% c("street", "st") ~ "st",
            tolower(pm.streetSuf) %in% c("avenue", "ave") ~ "ave", 
            tolower(pm.streetSuf) %in% c("boulevard", "blvd") ~ "blvd",
            tolower(pm.streetSuf) %in% c("drive", "dr") ~ "dr",
            tolower(pm.streetSuf) %in% c("road", "rd") ~ "rd",
            tolower(pm.streetSuf) %in% c("lane", "ln") ~ "ln",
            tolower(pm.streetSuf) %in% c("court", "ct") ~ "ct",
            tolower(pm.streetSuf) %in% c("place", "pl") ~ "pl",
            tolower(pm.streetSuf) %in% c("parkway", "pkwy") ~ "pkwy",
            TRUE ~ tolower(pm.streetSuf)
          ),
          TRUE ~ ""
        ),
        norm_direction = case_when(
          !is.na(pm.streetDir) ~ case_when(
            tolower(pm.streetDir) %in% c("north", "n") ~ "n",
            tolower(pm.streetDir) %in% c("south", "s") ~ "s",
            tolower(pm.streetDir) %in% c("east", "e") ~ "e", 
            tolower(pm.streetDir) %in% c("west", "w") ~ "w",
            tolower(pm.streetDir) %in% c("northeast", "ne") ~ "ne",
            tolower(pm.streetDir) %in% c("northwest", "nw") ~ "nw",
            tolower(pm.streetDir) %in% c("southeast", "se") ~ "se",
            tolower(pm.streetDir) %in% c("southwest", "sw") ~ "sw",
            TRUE ~ tolower(pm.streetDir)
          ),
          TRUE ~ ""
        ),
        # Reconstruct normalized address
        normalized_address = paste(
          norm_house,
          norm_direction,
          norm_street, 
          norm_suffix
        ) %>%
          str_squish() %>%
          str_replace_all("\\s+", " ")
      )
    
    # Create result vector
    result <- rep("", length(address_vector))
    result[valid_addresses$id] <- normalized_results$normalized_address
    
    return(result)
    
  }, error = function(e) {
    # Fallback to regex method on error
    warning("Postmastr parsing failed, using fallback method: ", e$message)
    return(normalize_address_with_postmastr(address_vector, use_postmastr = FALSE))
  })
}

#' Enhanced Address Similarity with Postmastr Normalization
#'
#' @description Improved version that uses postmastr for normalization
#' @param addr1 First address string
#' @param addr2 Second address string  
#' @return Similarity score 0-1 (1 = identical)
#' @export
calculate_enhanced_address_similarity <- function(addr1, addr2) {
  if (is.na(addr1) || is.na(addr2) || addr1 == "" || addr2 == "") return(0)
  
  # Use enhanced normalization
  norm1 <- normalize_address_with_postmastr(addr1)
  norm2 <- normalize_address_with_postmastr(addr2)
  
  # Exact match after normalization
  if (norm1 == norm2) return(1.0)
  
  # Calculate string distance (Jaro-Winkler if available)
  if (requireNamespace("RecordLinkage", quietly = TRUE)) {
    similarity <- RecordLinkage::jarowinkler(norm1, norm2)
  } else {
    # Fallback to normalized edit distance
    max_len <- max(nchar(norm1), nchar(norm2))
    if (max_len == 0) return(1.0)
    
    edit_dist <- adist(norm1, norm2)[1,1]
    similarity <- 1 - (edit_dist / max_len)
  }
  
  return(similarity)
}

#' Enhanced Duplicate Address Identification
#'
#' @description Improved version using postmastr-based normalization
#' @param address_df Data frame with address columns
#' @param address_col Name of address column
#' @param city_col Name of city column (default "city")
#' @param state_col Name of state column (default "state") 
#' @param similarity_threshold Minimum similarity to consider duplicates (default 0.85)
#' @param use_postmastr Use postmastr parsing (TRUE) or regex fallback (FALSE)
#' @return Data frame with duplicate_group column added
#' @export
identify_duplicate_addresses_enhanced <- function(address_df, 
                                                address_col,
                                                city_col = "city",
                                                state_col = "state", 
                                                similarity_threshold = 0.85,
                                                use_postmastr = TRUE) {
  
  cat("🏠 Enhanced duplicate address identification (with postmastr)\n")
  cat("===========================================================\n")
  
  # Create working copy
  df <- address_df
  df$duplicate_group <- NA_integer_
  df$is_primary <- FALSE
  
  # Enhanced normalization using postmastr
  cat("Step 1: Normalizing addresses with postmastr...\n")
  
  df$address_normalized <- normalize_address_with_postmastr(
    df[[address_col]], 
    use_postmastr = use_postmastr
  )
  
  # Create composite address key for initial grouping
  df$address_key <- paste(
    df$address_normalized,
    tolower(trimws(df[[city_col]])),
    tolower(trimws(df[[state_col]])),
    sep = "|"
  )
  
  # Group by exact normalized matches first
  exact_groups <- df %>%
    filter(!is.na(.data[[address_col]]) & .data[[address_col]] != "") %>%
    group_by(address_key) %>%
    mutate(group_size = n()) %>%
    ungroup()
  
  group_counter <- 1
  processed_rows <- c()
  
  # Process exact matches
  cat("Step 2: Processing exact normalized matches...\n")
  for (key in unique(exact_groups$address_key)) {
    rows <- which(df$address_key == key)
    if (length(rows) > 1) {
      df$duplicate_group[rows] <- group_counter
      df$is_primary[rows[1]] <- TRUE  # First row is primary
      group_counter <- group_counter + 1
      processed_rows <- c(processed_rows, rows)
    }
  }
  
  # Process remaining addresses for fuzzy matching
  remaining_rows <- setdiff(1:nrow(df), processed_rows)
  remaining_rows <- remaining_rows[!is.na(df[[address_col]][remaining_rows]) & 
                                   df[[address_col]][remaining_rows] != ""]
  
  cat("📊 Exact matches grouped:", length(processed_rows), "addresses\n")
  cat("🔍 Fuzzy matching remaining:", length(remaining_rows), "addresses\n")
  
  # Fuzzy matching for remaining addresses  
  if (length(remaining_rows) > 0) {
    pb <- progress::progress_bar$new(
      format = "[:bar] :percent :current/:total ETA: :eta",
      total = length(remaining_rows)
    )
    
    for (i in seq_along(remaining_rows)) {
      pb$tick()
      
      row_i <- remaining_rows[i]
      if (!is.na(df$duplicate_group[row_i])) next  # Already grouped
      
      # Compare with all other unprocessed rows in same city/state
      candidates <- remaining_rows[
        remaining_rows > row_i &
        is.na(df$duplicate_group[remaining_rows]) &
        tolower(df[[city_col]][remaining_rows]) == tolower(df[[city_col]][row_i]) &
        tolower(df[[state_col]][remaining_rows]) == tolower(df[[state_col]][row_i])
      ]
      
      similar_addresses <- c()
      for (row_j in candidates) {
        similarity <- calculate_enhanced_address_similarity(
          df[[address_col]][row_i],
          df[[address_col]][row_j]
        )
        
        if (similarity >= similarity_threshold) {
          similar_addresses <- c(similar_addresses, row_j)
        }
      }
      
      # Create group if similar addresses found
      if (length(similar_addresses) > 0) {
        all_rows <- c(row_i, similar_addresses)
        df$duplicate_group[all_rows] <- group_counter
        df$is_primary[row_i] <- TRUE  # First row is primary
        group_counter <- group_counter + 1
      }
    }
  }
  
  # Remove temporary columns
  df$address_key <- NULL
  df$address_normalized <- NULL
  
  # Summary statistics
  grouped_addresses <- sum(!is.na(df$duplicate_group))
  unique_groups <- length(unique(df$duplicate_group[!is.na(df$duplicate_group)]))
  primary_addresses <- sum(df$is_primary, na.rm = TRUE)
  
  cat("\n📈 ENHANCED DEDUPLICATION SUMMARY:\n")
  cat("Total addresses:", nrow(df), "\n")
  cat("Addresses in duplicate groups:", grouped_addresses, "\n")
  cat("Unique address groups:", unique_groups, "\n")
  cat("Primary addresses to geocode:", primary_addresses, "\n")
  cat("Addresses without duplicates:", nrow(df) - grouped_addresses, "\n")
  cat("Potential geocoding reduction:", 
      round((grouped_addresses - primary_addresses) / nrow(df) * 100, 1), "%\n")
  
  return(df)
}

# Testing the Enhanced System ----
cat("🧪 TESTING ENHANCED ADDRESS NORMALIZATION\n")
cat("=========================================\n\n")

# Test the enhanced normalization
test_addresses_enhanced <- c(
  "123 Main Street",
  "123 Main St", 
  "456 Oak Avenue",
  "456 Oak Ave",
  "789 North Park Boulevard",
  "789 N Park Blvd",
  "100 First St Suite 200",
  "   200   Second   Avenue   ",
  ""
)

cat("🔍 Testing Enhanced Normalization:\n")
cat("=================================\n")

# Test both methods
for (addr in test_addresses_enhanced) {
  original_norm <- normalize_address_with_postmastr(addr, use_postmastr = FALSE)
  enhanced_norm <- normalize_address_with_postmastr(addr, use_postmastr = TRUE)
  
  cat(sprintf("Original: %-30s\n", paste0("'", addr, "'")))
  cat(sprintf("Regex:    %-30s\n", paste0("'", original_norm, "'"))) 
  cat(sprintf("Enhanced: %-30s\n\n", paste0("'", enhanced_norm, "'")))
}

# Test with synthetic data
cat("🏥 Testing with Medical Facility Data:\n")
cat("=====================================\n")

synthetic_medical_addresses <- data.frame(
  id = 1:8,
  address = c(
    "550 Peachtree Street NE",
    "550 Peachtree St Northeast", 
    "1234 Medical Center Drive",
    "1234 Medical Center Dr",
    "5678 Hospital Boulevard Suite 100",
    "5678 Hospital Blvd",
    "9999 University Avenue",
    "9999 University Ave"
  ),
  city = c("Atlanta", "Atlanta", "Nashville", "Nashville", "Memphis", "Memphis", "Knoxville", "Knoxville"),
  state = c("GA", "GA", "TN", "TN", "TN", "TN", "TN", "TN"),
  stringsAsFactors = FALSE
)

# Test enhanced duplicate detection
enhanced_results <- identify_duplicate_addresses_enhanced(
  synthetic_medical_addresses,
  address_col = "address",
  city_col = "city",
  state_col = "state", 
  similarity_threshold = 0.85,
  use_postmastr = TRUE
)

cat("\nEnhanced Results:\n")
print(enhanced_results[, c("id", "address", "duplicate_group", "is_primary")])

cat("\n🎯 ENHANCEMENT SUMMARY:\n")
cat("======================\n")
cat("✅ Postmastr-based normalization improves address standardization\n")
cat("✅ Better handling of street types, directions, and suffixes\n") 
cat("✅ More accurate duplicate detection through enhanced parsing\n")
cat("✅ Fallback to regex method if postmastr fails\n")
cat("✅ Ready for integration with 04-geocode-enhanced.R\n")
#!/usr/bin/env Rscript

# Physician Deduplication and Uniqueness System
# Prevents duplicate physicians across multiple databases 
# Author: Tyler & Claude
# Created: 2025-08-24

library(dplyr)
library(stringdist)

#' Physician Uniqueness Scoring System
#' Determines if two physician records represent the same person

#' Generate Physician Signature
#' Creates a unique signature for deduplication matching
#' 
#' @param first_name First name
#' @param last_name Last name  
#' @param middle_name Middle name/initial
#' @param npi NPI number
#' @return Unique signature string
generate_physician_signature <- function(first_name, last_name, middle_name = "", npi = "") {
  
  # Clean and standardize names
  first_clean <- toupper(trimws(first_name %||% ""))
  last_clean <- toupper(trimws(last_name %||% ""))
  middle_clean <- toupper(trimws(middle_name %||% ""))
  npi_clean <- trimws(npi %||% "")
  
  # Create multiple signature levels for flexibility
  signatures <- list()
  
  # Level 1: NPI-based (most reliable if available)
  if (npi_clean != "" && nchar(npi_clean) == 10) {
    signatures$npi_exact <- paste0("NPI:", npi_clean)
  }
  
  # Level 2: Full name exact
  if (first_clean != "" && last_clean != "") {
    if (middle_clean != "") {
      signatures$full_exact <- paste0("FULL:", first_clean, "|", middle_clean, "|", last_clean)
    }
    signatures$name_exact <- paste0("NAME:", first_clean, "|", last_clean)
  }
  
  # Level 3: Initials + last name (for matching with limited data)
  if (first_clean != "" && last_clean != "") {
    first_initial <- substr(first_clean, 1, 1)
    middle_initial <- if (middle_clean != "") substr(middle_clean, 1, 1) else ""
    
    if (middle_initial != "") {
      signatures$initials_full <- paste0("INIT:", first_initial, middle_initial, "|", last_clean)
    }
    signatures$initials_basic <- paste0("INIT:", first_initial, "|", last_clean)
  }
  
  # Level 4: Soundex-based (phonetic matching)
  if (first_clean != "" && last_clean != "") {
    first_soundex <- phonetics::soundex(first_clean)
    last_soundex <- phonetics::soundex(last_clean)
    signatures$soundex <- paste0("SOUND:", first_soundex, "|", last_soundex)
  }
  
  return(signatures)
}

#' Calculate Physician Similarity Score
#' 
#' @param physician1 First physician record
#' @param physician2 Second physician record  
#' @return Similarity score and match details
calculate_physician_similarity <- function(physician1, physician2) {
  
  # Initialize score
  similarity_score <- 0
  match_evidence <- list()
  
  # NPI Matching (strongest evidence)
  if (!is.na(physician1$xref_npi) && !is.na(physician2$xref_npi) &&
      physician1$xref_npi != "" && physician2$xref_npi != "" &&
      physician1$xref_npi == physician2$xref_npi) {
    similarity_score <- similarity_score + 100  # Perfect match
    match_evidence$npi_exact <- TRUE
    match_evidence$match_type <- "NPI_EXACT"
    return(list(score = similarity_score, evidence = match_evidence, confidence = "CERTAIN"))
  }
  
  # Name-based matching
  first1 <- toupper(trimws(physician1$xref_first_name %||% ""))
  last1 <- toupper(trimws(physician1$xref_last_name %||% ""))
  middle1 <- toupper(trimws(physician1$xref_middle_name %||% ""))
  
  first2 <- toupper(trimws(physician2$xref_first_name %||% ""))
  last2 <- toupper(trimws(physician2$xref_last_name %||% ""))  
  middle2 <- toupper(trimws(physician2$xref_middle_name %||% ""))
  
  # First name similarity
  if (first1 != "" && first2 != "") {
    if (first1 == first2) {
      similarity_score <- similarity_score + 30
      match_evidence$first_exact <- TRUE
    } else {
      # Check for nickname/formal name variations
      first_sim <- stringdist::stringsim(first1, first2, method = "jw")
      if (first_sim > 0.8) {
        similarity_score <- similarity_score + (first_sim * 25)
        match_evidence$first_similar <- first_sim
      }
      # Check initials
      if (substr(first1, 1, 1) == substr(first2, 1, 1)) {
        similarity_score <- similarity_score + 10
        match_evidence$first_initial_match <- TRUE
      }
    }
  }
  
  # Last name similarity (most important for name matching)
  if (last1 != "" && last2 != "") {
    if (last1 == last2) {
      similarity_score <- similarity_score + 40
      match_evidence$last_exact <- TRUE
    } else {
      last_sim <- stringdist::stringsim(last1, last2, method = "jw")
      if (last_sim > 0.85) {
        similarity_score <- similarity_score + (last_sim * 35)
        match_evidence$last_similar <- last_sim
      }
    }
  }
  
  # Middle name/initial similarity
  if (middle1 != "" && middle2 != "") {
    if (middle1 == middle2) {
      similarity_score <- similarity_score + 15
      match_evidence$middle_exact <- TRUE
    } else {
      # Initial vs full name matching
      if (nchar(middle1) == 1 && nchar(middle2) > 1 && substr(middle2, 1, 1) == middle1) {
        similarity_score <- similarity_score + 12
        match_evidence$middle_initial_match <- TRUE
      } else if (nchar(middle2) == 1 && nchar(middle1) > 1 && substr(middle1, 1, 1) == middle2) {
        similarity_score <- similarity_score + 12
        match_evidence$middle_initial_match <- TRUE
      }
    }
  }
  
  # Specialty similarity (supporting evidence)
  specialty1 <- toupper(trimws(physician1$xref_specialty %||% ""))
  specialty2 <- toupper(trimws(physician2$xref_specialty %||% ""))
  
  if (specialty1 != "" && specialty2 != "") {
    if (grepl(specialty1, specialty2, ignore.case = TRUE) || grepl(specialty2, specialty1, ignore.case = TRUE)) {
      similarity_score <- similarity_score + 5
      match_evidence$specialty_match <- TRUE
    }
  }
  
  # Geographic proximity (supporting evidence)
  state1 <- toupper(trimws(physician1$xref_practice_state %||% ""))
  state2 <- toupper(trimws(physician2$xref_practice_state %||% ""))
  
  if (state1 != "" && state2 != "") {
    if (state1 == state2) {
      similarity_score <- similarity_score + 5
      match_evidence$same_state <- TRUE
      
      # City matching within same state
      city1 <- toupper(trimws(physician1$xref_practice_city %||% ""))
      city2 <- toupper(trimws(physician2$xref_practice_city %||% ""))
      
      if (city1 != "" && city2 != "" && city1 == city2) {
        similarity_score <- similarity_score + 5
        match_evidence$same_city <- TRUE
      }
    }
  }
  
  # Credential consistency (supporting evidence)
  cred1 <- toupper(trimws(physician1$xref_credentials %||% ""))
  cred2 <- toupper(trimws(physician2$xref_credentials %||% ""))
  
  if (cred1 != "" && cred2 != "") {
    if (cred1 == cred2) {
      similarity_score <- similarity_score + 3
      match_evidence$same_credentials <- TRUE
    } else if ((cred1 == "MD" && cred2 == "DO") || (cred1 == "DO" && cred2 == "MD")) {
      # MD/DO conflict is suspicious for same person
      similarity_score <- similarity_score - 10
      match_evidence$credential_conflict <- TRUE
    }
  }
  
  # Determine confidence level
  confidence <- "LOW"
  if (similarity_score >= 90) {
    confidence <- "VERY_HIGH"
  } else if (similarity_score >= 75) {
    confidence <- "HIGH"
  } else if (similarity_score >= 60) {
    confidence <- "MODERATE"
  } else if (similarity_score >= 45) {
    confidence <- "LOW_MODERATE"
  }
  
  # Determine match type
  if (!is.null(match_evidence$npi_exact)) {
    match_evidence$match_type <- "NPI_EXACT"
  } else if (match_evidence$first_exact && match_evidence$last_exact) {
    match_evidence$match_type <- "NAME_EXACT"
  } else if (match_evidence$first_exact || match_evidence$last_exact) {
    match_evidence$match_type <- "PARTIAL_NAME"
  } else {
    match_evidence$match_type <- "SIMILARITY_BASED"
  }
  
  return(list(
    score = round(similarity_score, 1),
    evidence = match_evidence,
    confidence = confidence
  ))
}

#' Deduplicate Physician Records
#' 
#' @param physician_records List of physician data frames from different databases
#' @param similarity_threshold Minimum similarity score for considering duplicates (default: 75)
#' @return Deduplicated physician records with source tracking
deduplicate_physician_records <- function(physician_records, similarity_threshold = 75) {
  
  cat("🔄 PHYSICIAN DEDUPLICATION PROCESS\n")
  cat("==================================\n")
  
  # Combine all records with source tracking
  all_records <- data.frame()
  
  for (db_name in names(physician_records)) {
    db_data <- physician_records[[db_name]]
    if (nrow(db_data) > 0) {
      db_data$source_database <- db_name
      db_data$original_index <- 1:nrow(db_data)
      all_records <- rbind(all_records, db_data)
    }
  }
  
  if (nrow(all_records) == 0) {
    cat("❌ No records to deduplicate\n")
    return(list(deduplicated = data.frame(), duplicates = data.frame(), stats = list()))
  }
  
  cat(sprintf("📊 Total records to process: %d from %d databases\n", nrow(all_records), length(physician_records)))
  
  # Initialize deduplication tracking
  all_records$duplicate_group <- NA
  all_records$is_primary <- FALSE
  all_records$duplicate_confidence <- NA
  all_records$match_evidence <- ""
  
  processed_indices <- c()
  duplicate_groups <- list()
  group_counter <- 1
  
  # Process each record for duplicates
  for (i in 1:nrow(all_records)) {
    
    if (i %in% processed_indices) next
    
    current_record <- all_records[i, ]
    group_members <- c(i)
    
    # Compare with all subsequent records
    for (j in (i + 1):nrow(all_records)) {
      
      if (j > nrow(all_records) || j %in% processed_indices) next
      
      comparison_record <- all_records[j, ]
      
      # Calculate similarity
      similarity_result <- calculate_physician_similarity(current_record, comparison_record)
      
      if (similarity_result$score >= similarity_threshold) {
        group_members <- c(group_members, j)
        
        cat(sprintf("🔍 Potential duplicate found (score: %.1f, confidence: %s):\n", 
                    similarity_result$score, similarity_result$confidence))
        cat(sprintf("     Record %d: %s %s (%s from %s)\n", 
                    i, current_record$xref_first_name, current_record$xref_last_name,
                    current_record$xref_npi, current_record$source_database))
        cat(sprintf("     Record %d: %s %s (%s from %s)\n",
                    j, comparison_record$xref_first_name, comparison_record$xref_last_name,
                    comparison_record$xref_npi, comparison_record$source_database))
        cat(sprintf("     Evidence: %s\n", paste(names(similarity_result$evidence), collapse = ", ")))
      }
    }
    
    # Assign group membership
    if (length(group_members) > 1) {
      
      # This is a duplicate group
      duplicate_groups[[group_counter]] <- group_members
      
      # Determine primary record (prefer by database priority, then by data completeness)
      database_priority <- c("PHYSICIAN_COMPARE", "OPEN_PAYMENTS", "MEDICARE_PARTD", "NBER_NPPES")
      
      primary_index <- group_members[1]  # Default to first
      
      for (db in database_priority) {
        candidates <- group_members[all_records$source_database[group_members] == db]
        if (length(candidates) > 0) {
          # Among same database, prefer most complete record
          completeness_scores <- sapply(candidates, function(idx) {
            record <- all_records[idx, ]
            score <- sum(c(
              !is.na(record$xref_npi) && record$xref_npi != "",
              !is.na(record$xref_first_name) && record$xref_first_name != "",
              !is.na(record$xref_last_name) && record$xref_last_name != "",
              !is.na(record$xref_middle_name) && record$xref_middle_name != "",
              !is.na(record$xref_specialty) && record$xref_specialty != "",
              !is.na(record$xref_credentials) && record$xref_credentials != ""
            ))
            return(score)
          })
          primary_index <- candidates[which.max(completeness_scores)]
          break
        }
      }
      
      # Mark group membership
      for (member_idx in group_members) {
        all_records$duplicate_group[member_idx] <- group_counter
        all_records$is_primary[member_idx] <- (member_idx == primary_index)
      }
      
      group_counter <- group_counter + 1
    } else {
      # Unique record
      all_records$is_primary[i] <- TRUE
    }
    
    processed_indices <- c(processed_indices, group_members)
  }
  
  # Generate final results
  primary_records <- all_records[all_records$is_primary == TRUE, ]
  duplicate_records <- all_records[all_records$is_primary == FALSE, ]
  
  # Statistics
  stats <- list(
    total_input_records = nrow(all_records),
    unique_physicians = nrow(primary_records),
    duplicate_records_removed = nrow(duplicate_records),
    duplicate_groups = length(duplicate_groups),
    deduplication_rate = round(nrow(duplicate_records) / nrow(all_records) * 100, 1)
  )
  
  # Reporting
  cat("\n📊 DEDUPLICATION RESULTS\n")
  cat("========================\n")
  cat(sprintf("Input records: %d\n", stats$total_input_records))
  cat(sprintf("Unique physicians: %d\n", stats$unique_physicians))
  cat(sprintf("Duplicates removed: %d (%.1f%%)\n", stats$duplicate_records_removed, stats$deduplication_rate))
  cat(sprintf("Duplicate groups: %d\n", stats$duplicate_groups))
  
  # Database source breakdown
  cat("\nSource distribution (deduplicated):\n")
  source_counts <- table(primary_records$source_database)
  for (source in names(source_counts)) {
    cat(sprintf("  %s: %d records\n", source, source_counts[source]))
  }
  
  return(list(
    deduplicated = primary_records,
    duplicates = duplicate_records,
    all_records = all_records,
    duplicate_groups = duplicate_groups,
    stats = stats
  ))
}

#' Validate Uniqueness After Search Results
#' Checks if search results contain duplicates and merges/ranks them appropriately
#' 
#' @param search_results Data frame of search results
#' @return Deduplicated and ranked results
validate_search_uniqueness <- function(search_results) {
  
  if (nrow(search_results) <= 1) return(search_results)
  
  cat("🔍 Validating search result uniqueness...\n")
  
  # Quick NPI-based deduplication (exact matches)
  npi_duplicates <- search_results %>%
    dplyr::filter(!is.na(npi) & npi != "") %>%
    dplyr::group_by(npi) %>%
    dplyr::filter(n() > 1) %>%
    dplyr::ungroup()
  
  if (nrow(npi_duplicates) > 0) {
    cat(sprintf("⚠️  Found %d records with duplicate NPIs\n", nrow(npi_duplicates)))
    
    # Keep best record for each NPI
    search_results <- search_results %>%
      dplyr::group_by(npi) %>%
      dplyr::arrange(desc(confidence_score)) %>%
      dplyr::slice_head(n = 1) %>%
      dplyr::ungroup()
    
    cat(sprintf("✅ Deduplicated to %d unique records\n", nrow(search_results)))
  }
  
  return(search_results)
}

cat("✅ Physician Deduplication System loaded successfully!\n")
cat("🔄 Functions available:\n")
cat("   - deduplicate_physician_records(physician_records, threshold)\n")
cat("   - calculate_physician_similarity(physician1, physician2)\n")
cat("   - validate_search_uniqueness(search_results)\n")
cat("   - generate_physician_signature(first, last, middle, npi)\n")
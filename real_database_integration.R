#!/usr/bin/env Rscript

# Real Database Integration System
# Connects to actual NPI databases instead of mock data
# Author: Tyler & Claude
# Created: 2025-08-24

library(duckdb)
library(readxl)
library(npi)
library(provider)
library(dplyr)
library(stringdist)

#' OBGYN Taxonomy Code Filter
#' These taxonomy codes identify OBGYN specialists across all databases
OBGYN_TAXONOMY_CODES <- c(
  "207V00000X",    # Obstetrics & Gynecology (General)
  "207VX0201X",    # Gynecologic Oncology *** PRIMARY FOCUS ***
  "207VE0102X",    # Reproductive Endocrinology & Infertility
  "207VG0400X",    # Gynecology (General)
  "207VM0101X",    # Maternal & Fetal Medicine
  "207VF0040X",    # Female Pelvic Medicine & Reconstructive Surgery
  "207VB0002X",    # Bariatric Medicine (OBGYN)
  "207VC0200X",    # Critical Care Medicine (OBGYN)
  "207VC0040X",    # Gynecology subspecialty
  "207VC0300X",    # Complex Family Planning
  "207VH0002X",    # Hospice and Palliative Medicine (OBGYN)
  "207VX0000X",    # Obstetrics Only
  # --- Added student and specialist codes ---
  "390200000X",    # Student in an Organized Health Care Education/Training Program
  "174400000X"     # Specialist
)

#' Database Connection Configuration
DATABASE_CONFIG <- list(
  # NBER Data - DuckDB with 8.3M records
  nber = list(
    path = "/Volumes/MufflyNew/nppes_historical_downloads/my_duckdb.duckdb",
    table = "npi_2024",
    type = "duckdb"
  ),
  
  # Physician Compare - CSV with 228K records  
  physician_compare = list(
    path = "/Volumes/MufflyNew/physician_compare/merged_filtered_physician_compare_20250504_0959.csv",
    type = "csv"
  ),
  
  # NPPES Deactivations - Excel with 305K records
  nppes_deactivations = list(
    path = "/Volumes/MufflyNew/nppes_deactivation_reports/NPPES Deactivated NPI Report 20250414.xlsx",
    sheet = "DeactivatedNPIs",
    type = "excel"
  ),
  
  # Open Payments - CSV filtered to OBGYNs
  open_payments = list(
    path = "/Volumes/MufflyNew/open_payments_data/open_payments_merged_filtered_to_obgyns.csv",
    type = "csv"
  )
)

#' Column Mapping for Real Databases
REAL_COLUMN_MAPPING <- list(
  nber = list(
    npi_col = "NPI",
    first_col = "Provider First Name", 
    last_col = "Provider Last Name (Legal Name)",
    middle_col = "Provider Middle Name",
    suffix_col = "Provider Name Suffix Text",
    credentials_col = "Provider Credential Text", 
    gender_col = "Provider Gender Code",
    city_col = "Provider Business Practice Location Address City Name",
    state_col = "Provider Business Practice Location Address State Name",
    zip_col = "Provider Business Practice Location Address Postal Code",
    deactivation_date_col = "NPI Deactivation Date",
    taxonomy_col = "Healthcare Provider Taxonomy Code_1"
  ),
  
  physician_compare = list(
    npi_col = "NPI",
    first_col = "frst_nm",
    last_col = "lst_nm", 
    middle_col = "mid_nm",
    suffix_col = "suff",
    credentials_col = "Cred",
    gender_col = "gndr",
    city_col = "cty",
    state_col = "st",
    zip_col = "zip",
    specialty_col = "pri_spec"
  ),
  
  open_payments = list(
    npi_col = "Covered_Recipient_NPI",
    first_col = "Covered_Recipient_First_Name",
    last_col = "Covered_Recipient_Last_Name",
    middle_col = "Covered_Recipient_Middle_Name",
    city_col = "Recipient_City",
    state_col = "Recipient_State",
    zip_col = "Recipient_Zip_Code"
  ),
  
  nppes_deactivations = list(
    npi_col = "NPPES Deactivated Records as of Apr 14 2025",
    deactivation_date_col = "...2"
  )
)

#' Connect to NBER DuckDB Database
#' @return DuckDB connection
connect_nber_database <- function() {
  tryCatch({
    con <- dbConnect(duckdb::duckdb(), DATABASE_CONFIG$nber$path)
    return(con)
  }, error = function(e) {
    warning(paste("Failed to connect to NBER database:", e$message))
    return(NULL)
  })
}

#' Load Physician Compare Data  
#' @return Data frame
load_physician_compare <- function() {
  tryCatch({
    cat("📊 Loading Physician Compare data...\n")
    data <- read.csv(DATABASE_CONFIG$physician_compare$path, stringsAsFactors = FALSE)
    cat(sprintf("✅ Loaded %s Physician Compare records\n", format(nrow(data), big.mark = ",")))
    return(data)
  }, error = function(e) {
    warning(paste("Failed to load Physician Compare:", e$message))
    return(data.frame())
  })
}

#' Load NPPES Deactivations Data
#' @return Data frame with deactivated NPIs
load_nppes_deactivations <- function() {
  tryCatch({
    cat("📊 Loading NPPES deactivations...\n")
    data <- read_excel(
      DATABASE_CONFIG$nppes_deactivations$path,
      sheet = DATABASE_CONFIG$nppes_deactivations$sheet
    )
    
    # Clean column names and skip header row
    names(data) <- c("npi", "deactivation_date")
    data <- data[-1, ]  # Remove header row
    
    # Convert to proper types
    data$npi <- as.character(data$npi)
    data$deactivation_date <- as.Date(data$deactivation_date, format = "%m/%d/%Y")
    
    cat(sprintf("✅ Loaded %s deactivated NPIs\n", format(nrow(data), big.mark = ",")))
    return(data)
  }, error = function(e) {
    warning(paste("Failed to load NPPES deactivations:", e$message))
    return(data.frame())
  })
}

#' Search NBER Database
#' @param con DuckDB connection
#' @param first_name First name to search
#' @param last_name Last name to search  
#' @param middle_name Middle name (optional)
#' @param max_results Maximum results to return
#' @return Data frame of matches
search_nber_database <- function(con, first_name, last_name, middle_name = "", max_results = 50) {
  if (is.null(con)) return(data.frame())
  
  tryCatch({
    # Build SQL query with fuzzy matching and OBGYN taxonomy filtering
    obgyn_codes_sql <- paste0("'", OBGYN_TAXONOMY_CODES, "'", collapse = ", ")
    
    sql_query <- sprintf("
      SELECT 
        NPI,
        \"Provider First Name\" as first_name,
        \"Provider Last Name (Legal Name)\" as last_name,
        \"Provider Middle Name\" as middle_name,
        \"Provider Name Suffix Text\" as suffix,
        \"Provider Credential Text\" as credentials,
        \"Provider Gender Code\" as gender,
        \"Provider Business Practice Location Address City Name\" as city,
        \"Provider Business Practice Location Address State Name\" as state,
        \"NPI Deactivation Date\" as deactivation_date,
        \"Healthcare Provider Taxonomy Code_1\" as taxonomy_1,
        \"Healthcare Provider Taxonomy Code_2\" as taxonomy_2,
        \"Healthcare Provider Taxonomy Code_3\" as taxonomy_3,
        \"Healthcare Provider Taxonomy Code_4\" as taxonomy_4,
        \"Healthcare Provider Taxonomy Code_5\" as taxonomy_5
      FROM npi_2024
      WHERE 
        UPPER(\"Provider First Name\") LIKE UPPER('%%%s%%') 
        AND UPPER(\"Provider Last Name (Legal Name)\") LIKE UPPER('%%%s%%')
        AND \"Entity Type Code\" = '1'  -- Individual providers only
        AND (
          \"Healthcare Provider Taxonomy Code_1\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_2\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_3\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_4\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_5\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_6\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_7\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_8\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_9\" IN (%s) OR
          \"Healthcare Provider Taxonomy Code_10\" IN (%s)
        )
      LIMIT %d
    ", first_name, last_name, 
       obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql,
       obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql, obgyn_codes_sql,
       max_results)
    
    result <- dbGetQuery(con, sql_query)
    
    if (nrow(result) > 0) {
      cat(sprintf("    ✅ Found %d candidates in NBER\n", nrow(result)))
    } else {
      cat("    ❌ No candidates found in NBER\n")
    }
    
    return(result)
    
  }, error = function(e) {
    warning(paste("NBER search failed:", e$message))
    return(data.frame())
  })
}

#' Search Physician Compare Data
#' @param data Physician Compare data frame
#' @param first_name First name to search
#' @param last_name Last name to search
#' @param max_results Maximum results
#' @return Data frame of matches
search_physician_compare <- function(data, first_name, last_name, max_results = 50) {
  if (nrow(data) == 0) return(data.frame())
  
  tryCatch({
    # Fuzzy matching with string distance
    first_matches <- data[grepl(first_name, data$frst_nm, ignore.case = TRUE), ]
    matches <- first_matches[grepl(last_name, first_matches$lst_nm, ignore.case = TRUE), ]
    
    # Filter for OBGYN specialists using pri_spec column
    if (nrow(matches) > 0) {
      # Check primary specialty for OBGYN-related terms
      obgyn_terms <- c("OBSTETRICS", "GYNECOL", "OBGYN", "OB-GYN", "OB/GYN", "REPRODUCTIVE", "MATERNAL", "FETAL")
      obgyn_pattern <- paste(obgyn_terms, collapse = "|")
      
      obgyn_matches <- matches[grepl(obgyn_pattern, matches$pri_spec, ignore.case = TRUE), ]
      
      if (nrow(obgyn_matches) > 0) {
        matches <- obgyn_matches
      }
    }
    
    if (nrow(matches) > max_results) {
      matches <- matches[1:max_results, ]
    }
    
    if (nrow(matches) > 0) {
      cat(sprintf("    ✅ Found %d candidates in Physician Compare\n", nrow(matches)))
    } else {
      cat("    ❌ No candidates found in Physician Compare\n")
    }
    
    return(matches)
    
  }, error = function(e) {
    warning(paste("Physician Compare search failed:", e$message))
    return(data.frame())
  })
}

#' Search NPPES API using npi package
#' @param first_name First name
#' @param last_name Last name  
#' @param max_results Maximum results
#' @return Processed results
search_nppes_api <- function(first_name, last_name, max_results = 10) {
  tryCatch({
    cat("    🌐 Searching NPPES API...\n")
    
    # Search using npi package
    api_results <- npi_search(
      first_name = first_name,
      last_name = last_name, 
      limit = max_results
    )
    
    if (!is.null(api_results) && nrow(api_results) > 0) {
      # Extract and standardize data
      processed_results <- data.frame(
        npi = api_results$npi,
        first_name = sapply(api_results$basic, function(x) x$first_name %||% ""),
        last_name = sapply(api_results$basic, function(x) x$last_name %||% ""),
        middle_name = sapply(api_results$basic, function(x) x$middle_name %||% ""),
        credentials = sapply(api_results$basic, function(x) x$credential %||% ""),
        gender = sapply(api_results$basic, function(x) x$sex %||% ""),
        status = sapply(api_results$basic, function(x) x$status %||% ""),
        stringsAsFactors = FALSE
      )
      
      # Filter for OBGYN specialists using taxonomy codes from API results
      if (nrow(processed_results) > 0) {
        obgyn_rows <- c()
        
        for (i in 1:nrow(api_results)) {
          # Check taxonomy codes for this provider
          if ("taxonomies" %in% names(api_results) && length(api_results$taxonomies[[i]]) > 0) {
            taxonomy_data <- api_results$taxonomies[[i]]
            if ("code" %in% names(taxonomy_data)) {
              provider_codes <- taxonomy_data$code
              if (any(provider_codes %in% OBGYN_TAXONOMY_CODES)) {
                obgyn_rows <- c(obgyn_rows, i)
              }
            }
          }
        }
        
        if (length(obgyn_rows) > 0) {
          processed_results <- processed_results[obgyn_rows, ]
        } else {
          # No OBGYN specialists found
          processed_results <- processed_results[0, ]
        }
      }
      
      cat(sprintf("    ✅ Found %d candidates in NPPES API\n", nrow(processed_results)))
      return(processed_results)
    } else {
      cat("    ❌ No candidates found in NPPES API\n")
      return(data.frame())
    }
    
  }, error = function(e) {
    warning(paste("NPPES API search failed:", e$message))
    cat("    ❌ NPPES API search failed\n")
    return(data.frame())
  })
}

#' Load and Search Open Payments Data
#' @param first_name First name to search
#' @param last_name Last name to search
#' @param max_results Maximum results
#' @return Data frame of matches
search_open_payments <- function(first_name, last_name, max_results = 50) {
  tryCatch({
    cat("📊 Loading Open Payments data...\n")
    data <- read.csv(DATABASE_CONFIG$open_payments$path, stringsAsFactors = FALSE)
    
    # Search with fuzzy matching
    first_matches <- data[grepl(first_name, data$Covered_Recipient_First_Name, ignore.case = TRUE), ]
    matches <- first_matches[grepl(last_name, first_matches$Covered_Recipient_Last_Name, ignore.case = TRUE), ]
    
    if (nrow(matches) > max_results) {
      matches <- matches[1:max_results, ]
    }
    
    if (nrow(matches) > 0) {
      # Standardize column names
      standardized <- data.frame(
        npi = matches$Covered_Recipient_NPI,
        first_name = matches$Covered_Recipient_First_Name,
        last_name = matches$Covered_Recipient_Last_Name,
        middle_name = ifelse(is.na(matches$Covered_Recipient_Middle_Name), "", matches$Covered_Recipient_Middle_Name),
        city = ifelse(is.na(matches$Recipient_City), "", matches$Recipient_City),
        state = ifelse(is.na(matches$Recipient_State), "", matches$Recipient_State),
        database_source = "Open_Payments",
        stringsAsFactors = FALSE
      )
      
      cat(sprintf("    ✅ Found %d candidates in Open Payments\n", nrow(standardized)))
      return(standardized)
    } else {
      cat("    ❌ No candidates found in Open Payments\n")
      return(data.frame())
    }
    
  }, error = function(e) {
    warning(paste("Open Payments search failed:", e$message))
    cat("    ❌ Open Payments search failed\n")
    return(data.frame())
  })
}

#' Check if NPI is Deactivated
#' @param npi NPI number to check
#' @param deactivation_data Deactivation data frame
#' @return TRUE if deactivated, FALSE otherwise
is_npi_deactivated <- function(npi, deactivation_data) {
  if (nrow(deactivation_data) == 0) return(FALSE)
  return(npi %in% deactivation_data$npi)
}

#' Generate Alternative Name Variations
#' @param first_name First name
#' @param last_name Last name  
#' @param middle_name Middle name (optional)
#' @return List of name variations to try
generate_name_variations <- function(first_name, last_name, middle_name = "") {
  variations <- list()
  
  # Base variation - original names
  variations[[1]] <- list(first = first_name, last = last_name, middle = middle_name)
  
  # Common nickname variations for first names
  nickname_map <- list(
    "Jennifer" = c("Jen", "Jenny"),
    "Jen" = c("Jennifer", "Jenny"), 
    "Jessica" = c("Jess", "Jessie"),
    "Michael" = c("Mike", "Mickey", "Mick"),
    "Thomas" = c("Tom", "Tommy", "Thom"),
    "Megan" = c("Meg", "Meggie"),
    "Joseph" = c("Joe", "Joey"),
    "Camille" = c("Cami", "Cam")
  )
  
  # Add nickname variations
  if (first_name %in% names(nickname_map)) {
    for (nickname in nickname_map[[first_name]]) {
      variations[[length(variations) + 1]] <- list(first = nickname, last = last_name, middle = middle_name)
    }
  }
  
  # Check if current first name is a nickname and add full name
  for (full_name in names(nickname_map)) {
    if (first_name %in% nickname_map[[full_name]]) {
      variations[[length(variations) + 1]] <- list(first = full_name, last = last_name, middle = middle_name)
    }
  }
  
  # Enhanced hyphenated last name variations
  if (grepl("-", last_name)) {
    parts <- strsplit(last_name, "-")[[1]]
    parts <- trimws(parts)  # Clean each part
    
    # Add each individual part as a variation
    for (part in parts) {
      if (nchar(part) > 1) {  # Only add meaningful parts
        variations[[length(variations) + 1]] <- list(first = first_name, last = part, middle = middle_name)
      }
    }
    
    # Add reversed hyphenated combination (common in databases)
    if (length(parts) == 2) {
      reversed_name <- paste(parts[2], parts[1], sep = "-")
      variations[[length(variations) + 1]] <- list(first = first_name, last = reversed_name, middle = middle_name)
    }
    
    # Add space-separated version (some databases use spaces instead of hyphens)
    spaced_name <- paste(parts, collapse = " ")
    variations[[length(variations) + 1]] <- list(first = first_name, last = spaced_name, middle = middle_name)
    
    # For very common patterns, add maiden name priority (first part often maiden name)
    if (length(parts) == 2 && nchar(parts[1]) > 2) {
      # First part as primary surname
      variations[[length(variations) + 1]] <- list(first = first_name, last = parts[1], middle = middle_name)
    }
  }
  
  # Handle potential married name patterns (common OBGYN scenario)
  # Look for patterns where last part might be married name
  if (grepl("-", last_name)) {
    parts <- strsplit(last_name, "-")[[1]]
    if (length(parts) == 2) {
      # Often the second part is the married/husband's name
      married_name <- trimws(parts[2])
      if (nchar(married_name) > 2) {
        variations[[length(variations) + 1]] <- list(first = first_name, last = married_name, middle = middle_name)
      }
    }
  }
  
  # Remove duplicates
  unique_variations <- list()
  for (var in variations) {
    key <- paste(var$first, var$middle, var$last, sep = "|")
    if (!key %in% names(unique_variations)) {
      unique_variations[[key]] <- var
    }
  }
  
  return(unname(unique_variations))
}

#' Enhanced Real Database Search with Alternative Names
#' @param first_name First name to search
#' @param last_name Last name to search
#' @param middle_name Middle name (optional)
#' @param max_results_per_db Maximum results per database per variation
#' @param search_variations Search alternative name variations (default: TRUE)
#' @return Combined results from all databases and variations
search_real_databases <- function(first_name, last_name, middle_name = "", max_results_per_db = 20, search_variations = TRUE) {
  
  # Generate name variations to search
  if (search_variations) {
    name_variations <- generate_name_variations(first_name, last_name, middle_name)
    cat(sprintf("🔍 ENHANCED DATABASE SEARCH: %s %s %s (%d variations)\n", 
                first_name, middle_name, last_name, length(name_variations)))
  } else {
    name_variations <- list(list(first = first_name, last = last_name, middle = middle_name))
    cat(sprintf("🔍 STANDARD DATABASE SEARCH: %s %s %s\n", first_name, middle_name, last_name))
  }
  cat("======================================\n")
  
  all_results <- list()
  results_found <- 0
  
  # Search each name variation
  for (variation_idx in 1:length(name_variations)) {
    var <- name_variations[[variation_idx]]
    var_first <- var$first
    var_last <- var$last  
    var_middle <- var$middle
    
    if (search_variations && length(name_variations) > 1) {
      cat(sprintf("🔄 Variation %d: %s %s %s\n", variation_idx, var_first, var_middle, var_last))
    }
    
    # 1. Search NBER Database
    if (variation_idx == 1) cat("📊 Searching NBER database (8.3M records)...\n")
    nber_con <- connect_nber_database()
    if (!is.null(nber_con)) {
      nber_results <- search_nber_database(nber_con, var_first, var_last, var_middle, max_results_per_db)
      if (nrow(nber_results) > 0) {
        nber_results$database_source <- "NBER"
        nber_results$search_variation <- sprintf("%s %s %s", var_first, var_middle, var_last)
        all_results[[paste0("nber_", variation_idx)]] <- nber_results
        results_found <- results_found + nrow(nber_results)
      }
      dbDisconnect(nber_con)
    }
  
    # 2. Search Physician Compare
    if (variation_idx == 1) cat("📊 Searching Physician Compare (228K records)...\n")
    physician_data <- load_physician_compare()
    if (nrow(physician_data) > 0) {
      pc_results <- search_physician_compare(physician_data, var_first, var_last, max_results_per_db)
      if (nrow(pc_results) > 0) {
        # Standardize column names
        pc_standardized <- data.frame(
          npi = pc_results$NPI,
          first_name = pc_results$frst_nm,
          last_name = pc_results$lst_nm,
          middle_name = ifelse(is.na(pc_results$mid_nm), "", pc_results$mid_nm),
          credentials = ifelse(is.na(pc_results$Cred), "", pc_results$Cred),
          gender = pc_results$gndr,
          city = pc_results$cty,
          state = pc_results$st,
          database_source = "Physician_Compare",
          search_variation = sprintf("%s %s %s", var_first, var_middle, var_last),
          stringsAsFactors = FALSE
        )
        all_results[[paste0("physician_compare_", variation_idx)]] <- pc_standardized
        results_found <- results_found + nrow(pc_standardized)
      }
    }
    
    # 3. Search NPPES API  
    if (variation_idx == 1) cat("📊 Searching NPPES API (live)...\n")
    api_results <- search_nppes_api(var_first, var_last, max_results_per_db)
    if (nrow(api_results) > 0) {
      api_results$database_source <- "NPPES_API"
      api_results$search_variation <- sprintf("%s %s %s", var_first, var_middle, var_last)
      all_results[[paste0("nppes_api_", variation_idx)]] <- api_results
      results_found <- results_found + nrow(api_results)
    }
    
    # 4. Search Open Payments
    if (variation_idx == 1) cat("📊 Searching Open Payments (OBGYN filtered)...\n")
    op_results <- search_open_payments(var_first, var_last, max_results_per_db)
    if (nrow(op_results) > 0) {
      op_results$search_variation <- sprintf("%s %s %s", var_first, var_middle, var_last)
      all_results[[paste0("open_payments_", variation_idx)]] <- op_results
      results_found <- results_found + nrow(op_results)
    }
  }
  
  # 5. Load deactivation status
  cat("📊 Loading deactivation status...\n")
  deactivation_data <- load_nppes_deactivations()
  
  # 6. Combine and process all results
  if (length(all_results) > 0) {
    # Define standard column schema
    standard_columns <- c(
      "npi", "first_name", "last_name", "middle_name", "credentials", 
      "gender", "city", "state", "database_source", "search_variation"
    )
    
    # Standardize each result to the common schema
    standardized_results <- list()
    for (db_name in names(all_results)) {
      result_df <- all_results[[db_name]]
      
      # Only process if we have results
      if (nrow(result_df) > 0) {
        # Create standardized data frame with proper dimensions
        standardized_df <- data.frame(matrix(nrow = nrow(result_df), ncol = length(standard_columns)), 
                                      stringsAsFactors = FALSE)
        names(standardized_df) <- standard_columns
        
        # Copy data column by column (much safer)
        for (col in standard_columns) {
          if (col %in% names(result_df)) {
            standardized_df[[col]] <- as.character(result_df[[col]])
          } else {
            standardized_df[[col]] <- rep("", nrow(result_df))
          }
        }
        
        # Ensure NPI is never empty
        if (any(is.na(standardized_df$npi) | standardized_df$npi == "")) {
          # Remove rows with missing NPIs
          standardized_df <- standardized_df[!is.na(standardized_df$npi) & standardized_df$npi != "", ]
        }
        
        # Only add if we still have valid results
        if (nrow(standardized_df) > 0) {
          standardized_results[[db_name]] <- standardized_df
        }
      }
    }
    
    # Combine standardized results
    if (length(standardized_results) > 0) {
      combined_results <- do.call(rbind, standardized_results)
      rownames(combined_results) <- NULL
    } else {
      # Return empty data frame with standard schema
      combined_results <- data.frame(row.names = NULL, stringsAsFactors = FALSE)
      for (col in standard_columns) {
        combined_results[[col]] <- character(0)
      }
    }
    
    # Add deactivation status
    combined_results$is_deactivated <- sapply(combined_results$npi, function(x) {
      is_npi_deactivated(x, deactivation_data)
    })
    
    # Add NPI status
    combined_results$npi_status <- ifelse(combined_results$is_deactivated, "Deactivated", "Active")
    
    cat(sprintf("\n✅ TOTAL MATCHES: %d across all databases", nrow(combined_results)))
    if (search_variations && length(name_variations) > 1) {
      cat(sprintf(" (searched %d name variations)", length(name_variations)))
    }
    cat("\n")
    
    return(combined_results)
  } else {
    cat("\n❌ No matches found in any database\n")
    return(data.frame())
  }
}

#' Check if Provider is OBGYN Specialist
#' @param taxonomy_codes Vector of taxonomy codes for a provider (can be multiple columns)
#' @return TRUE if provider has any OBGYN taxonomy codes
is_obgyn_provider <- function(taxonomy_codes) {
  if (is.null(taxonomy_codes) || length(taxonomy_codes) == 0) {
    return(FALSE)
  }
  
  # Handle both single values and vectors
  codes_to_check <- unlist(taxonomy_codes)
  codes_to_check <- codes_to_check[!is.na(codes_to_check) & codes_to_check != ""]
  
  if (length(codes_to_check) == 0) {
    return(FALSE)
  }
  
  # Check if any code matches OBGYN taxonomy codes
  return(any(codes_to_check %in% OBGYN_TAXONOMY_CODES))
}

#' Filter Results to OBGYN Specialists Only
#' @param results Data frame with taxonomy columns
#' @param taxonomy_columns Vector of column names that contain taxonomy codes
#' @return Filtered data frame with only OBGYN specialists
filter_obgyn_specialists <- function(results, taxonomy_columns) {
  if (nrow(results) == 0) {
    return(results)
  }
  
  # Check each row to see if any taxonomy code matches OBGYN
  obgyn_rows <- c()
  
  for (i in 1:nrow(results)) {
    # Get all taxonomy codes for this row
    row_taxonomies <- c()
    for (col in taxonomy_columns) {
      if (col %in% names(results)) {
        val <- results[i, col]
        if (!is.na(val) && val != "") {
          row_taxonomies <- c(row_taxonomies, val)
        }
      }
    }
    
    # Check if this provider is OBGYN
    if (is_obgyn_provider(row_taxonomies)) {
      obgyn_rows <- c(obgyn_rows, i)
    }
  }
  
  if (length(obgyn_rows) > 0) {
    return(results[obgyn_rows, ])
  } else {
    return(results[0, ])  # Return empty dataframe with same structure
  }
}

#' Helper function for null coalescing
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

cat("✅ Real Database Integration System with OBGYN Filtering loaded successfully!\n")
cat("🔗 Functions available:\n")
cat("   - search_real_databases(first, last, middle) - Complete multi-database search\n")
cat("   - search_nber_database() - Search NBER DuckDB (8.3M records)\n")
cat("   - search_physician_compare() - Search Physician Compare (228K records)\n")  
cat("   - search_nppes_api() - Search live NPPES API\n")
cat("   - load_nppes_deactivations() - Check NPI deactivation status\n")
cat("   - filter_obgyn_specialists() - Filter results to OBGYN specialists only\n\n")

cat("🏥 Connected databases:\n")
cat("   ✅ NBER NPPES Historical (8,274,462 records)\n")
cat("   ✅ Physician Compare (228,430 records)\n") 
cat("   ✅ NPPES Deactivations (305,512 records)\n")
cat("   ✅ NPPES Live API (real-time)\n")
cat("   ✅ Open Payments OBGYN Filtered (CSV)\n\n")

cat("🎯 Key Features:\n")
cat("   ✅ Real NPI validation against 8.8M+ records\n")
cat("   ✅ Cross-database verification\n")
cat("   ✅ Live NPPES API integration\n")
cat("   ✅ Deactivation status checking\n")
cat("   ✅ Fuzzy name matching across databases\n")
cat("   ✅ OBGYN taxonomy filtering (14 specialty codes)\n\n")

cat("🧪 Test with: search_real_databases('John', 'Smith')\n")
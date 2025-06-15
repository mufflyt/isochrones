#######################
source("R/01-setup.R")
#######################

#==============================================================================
# Extracting and Processing NPPES Provider Data for OBGYN Practitioners
#==============================================================================

#------------------------------------------------------------------------------
# 1. Load Required Packages
#------------------------------------------------------------------------------
library(DBI)
library(duckdb)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(tyler)
library(logger)
library(assertthat)
library(tibble)

# Load custom functions for NPPES data processing
source("R/bespoke_functions.R")
invisible(gc())

#------------------------------------------------------------------------------
# 2. Connect to DuckDB and Get Table-Year Mapping
#------------------------------------------------------------------------------
db_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"
con <- DBI::dbConnect(duckdb::duckdb(), db_path)

table_year_mapping <- create_nppes_table_mapping(con)

#------------------------------------------------------------------------------
# 3. Define OBGYN Taxonomy Codes
#------------------------------------------------------------------------------
obgyn_taxonomy_codes <- c(
  "207V00000X", "207VX0201X", "207VE0102X", "207VG0400X",
  "207VM0101X", "207VF0040X", "207VB0002X", "207VC0200X",
  "207VC0040X", "207VC0300X", "207VH0002X", "207VX0000X"
)

#' Find physicians across multiple years using taxonomy codes
#'
#' @description
#' Queries database tables across multiple years to find physicians matching 
#' specified taxonomy codes. Only processes tables with "npidata" in their name
#' from the specified NPPES data directory.
#'
#' @param con A DBI database connection object
#' @param table_year_mapping A data frame mapping table names to years with
#'                          columns 'table_name' and 'year'
#' @param taxonomy_codes A character vector of taxonomy codes to search for
#' @param years_to_include Optional numeric vector of years to include in search
#' @param verbose Logical indicating whether to display detailed log messages
#' @param batch_size Integer specifying the number of records to process in each batch
#' @param required_columns Character vector of additional columns that must be preserved
#'                        in the output (beyond the default essential columns)
#' @param nppes_pattern Character pattern to filter tables by (default: "npidata")
#'
#' @return A tibble containing physician data across requested years
#'
#' @examples
#' # Connect to a database
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' 
#' # Create sample table with physician data including country code columns
#' DBI::dbExecute(con, "
#'   CREATE TABLE npidata_pfile_20050523_20220410 (
#'     NPI INTEGER,
#'     'Entity Type Code' INTEGER,
#'     'Provider Business Practice Location Address Postal Code' TEXT,
#'     'Provider Business Mailing Address Country Code (If outside U.S.)' TEXT,
#'     'Provider Business Practice Location Address Country Code (If outside U.S.)' TEXT,
#'     'Healthcare Provider Taxonomy Code_1' TEXT
#'   )
#' ")
#' 
#' # Create non-NPPES table that should be skipped
#' DBI::dbExecute(con, "
#'   CREATE TABLE y2022_National_Downloadable_File (
#'     NPI INTEGER,
#'     'Entity Type Code' INTEGER
#'   )
#' ")
#' 
#' # Create mapping dataframe with both tables
#' mapping <- tibble::tibble(
#'   table_name = c("npidata_pfile_20050523_20220410", 
#'                  "y2022_National_Downloadable_File"),
#'   year = c(2022, 2022)
#' )
#' 
#' # Find physicians - only processes the npidata table
#' physicians <- find_physicians_across_years(
#'   con = con, 
#'   table_year_mapping = mapping,
#'   taxonomy_codes = c("207R00000X"),
#'   verbose = TRUE,
#'   nppes_pattern = "npidata"
#' )
#' physicians
#' 
#' # Clean up
#' DBI::dbDisconnect(con)
#'
#' @importFrom dplyr tbl filter mutate select %>% bind_rows all_of distinct
#' @importFrom purrr reduce map_dfr
#' @importFrom rlang expr .data
#' @importFrom logger log_info log_warn log_error log_threshold INFO WARN
#' @importFrom stringr str_sub str_detect
#' @importFrom assertthat assert_that
#' @importFrom DBI dbIsValid dbListTables dbListFields dbGetQuery
#' @importFrom tibble tibble as_tibble
#' @importFrom stats setNames
#' @importFrom utils gc
#'
#' @export
find_physicians_across_years <- function(con,
                                         table_year_mapping,
                                         taxonomy_codes,
                                         years_to_include = NULL,
                                         verbose = TRUE,
                                         batch_size = 10000,
                                         required_columns = NULL,
                                         nppes_pattern = "npidata") {
  # Set log threshold based on verbose parameter
  logger::log_threshold(if (verbose) logger::INFO else logger::WARN)
  logger::log_info("Starting physician search across years")
  logger::log_info("Using batch size: %d", batch_size)
  logger::log_info("Filtering tables to include pattern: '%s'", nppes_pattern)
  
  # Validate inputs
  validate_inputs(con, table_year_mapping, taxonomy_codes, years_to_include, 
                  batch_size, required_columns)
  
  # Filter mapping to requested years
  filtered_mapping <- filter_year_mapping(table_year_mapping, years_to_include)
  
  # Further filter mapping to include only tables with the NPPES pattern
  filtered_mapping <- filter_nppes_tables(filtered_mapping, nppes_pattern)
  
  if (nrow(filtered_mapping) == 0) {
    logger::log_warn("No matching tables found after filtering")
    return(create_empty_physician_tibble(required_columns))
  }
  
  # Include default essential columns plus any required ones
  essential_columns <- get_essential_columns(required_columns)
  logger::log_info("Essential columns to preserve: %s", 
                   paste(shorten_column_names(essential_columns), collapse = ", "))
  
  # Process each year with batching for memory efficiency
  logger::log_info("Processing %d filtered tables with batching", nrow(filtered_mapping))
  physician_combined <- query_all_years_batch(
    con, 
    filtered_mapping, 
    taxonomy_codes, 
    batch_size, 
    verbose,
    essential_columns
  )
  
  # Return combined physicians data
  logger::log_info("Completed physician search with %d total records", nrow(physician_combined))
  return(physician_combined)
}

#' @noRd
validate_inputs <- function(con, table_year_mapping, taxonomy_codes, years_to_include, 
                            batch_size, required_columns) {
  assertthat::assert_that(DBI::dbIsValid(con), 
                          msg = "Database connection is not valid")
  assertthat::assert_that(is.data.frame(table_year_mapping),
                          msg = "table_year_mapping must be a data frame")
  assertthat::assert_that(all(c("table_name", "year") %in% colnames(table_year_mapping)),
                          msg = "table_year_mapping must contain 'table_name' and 'year' columns")
  assertthat::assert_that(is.character(taxonomy_codes), 
                          msg = "taxonomy_codes must be a character vector")
  assertthat::assert_that(length(taxonomy_codes) > 0, 
                          msg = "At least one taxonomy code must be provided")
  if (!is.null(years_to_include)) {
    assertthat::assert_that(is.numeric(years_to_include),
                            msg = "years_to_include must be a numeric vector")
  }
  assertthat::assert_that(is.numeric(batch_size) && batch_size > 0,
                          msg = "batch_size must be a positive numeric value")
  if (!is.null(required_columns)) {
    assertthat::assert_that(is.character(required_columns),
                            msg = "required_columns must be a character vector")
  }
  logger::log_info("Input validation completed successfully")
}

#' @noRd
filter_year_mapping <- function(table_year_mapping, years_to_include) {
  if (is.null(years_to_include)) {
    logger::log_info("No specific years requested, using all years in mapping")
    return(table_year_mapping)
  }
  
  filtered_mapping <- table_year_mapping[table_year_mapping$year %in% years_to_include, ]
  
  logger::log_info("Filtered mapping to %d years: %s", 
                   nrow(filtered_mapping),
                   paste(filtered_mapping$year, collapse = ", "))
  
  return(filtered_mapping)
}

#' @noRd
filter_nppes_tables <- function(table_year_mapping, nppes_pattern) {
  original_count <- nrow(table_year_mapping)
  
  # Filter tables to only include those with the NPPES pattern
  filtered_mapping <- table_year_mapping %>%
    dplyr::filter(stringr::str_detect(table_name, nppes_pattern))
  
  # Check for the specified directory pattern as well
  nppes_dir_pattern <- "Volumes_Video_Projects_Muffly_1_nppes_historical_downloads_unzipped_p_files"
  dir_matches <- table_year_mapping %>%
    dplyr::filter(stringr::str_detect(table_name, nppes_dir_pattern))
  
  # Combine both patterns (tables with either pattern should be included)
  filtered_mapping <- dplyr::bind_rows(filtered_mapping, dir_matches) %>%
    dplyr::distinct()
  
  filtered_count <- nrow(filtered_mapping)
  excluded_count <- original_count - filtered_count
  
  logger::log_info("Filtered tables by NPPES pattern: kept %d, excluded %d", 
                   filtered_count, excluded_count)
  
  if (filtered_count > 0) {
    logger::log_info("Sample of included tables: %s", 
                     paste(head(filtered_mapping$table_name, 3), collapse = ", "))
  }
  
  return(filtered_mapping)
}

#' @noRd
get_essential_columns <- function(required_columns) {
  # Base essential columns that should always be included
  base_columns <- c(
    "NPI", 
    "Entity Type Code", 
    "Provider Business Practice Location Address Postal Code"
  )
  
  # Add country code columns by default (these are critical for filtering)
  country_columns <- c(
    "Provider Business Mailing Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Country Code (If outside U.S.)"
  )
  
  # Combine with any required columns
  all_columns <- unique(c(base_columns, country_columns, required_columns))
  
  return(all_columns)
}

#' @noRd
shorten_column_names <- function(column_names, max_length = 30) {
  # Shorten column names for more readable logging
  vapply(column_names, function(name) {
    if (nchar(name) > max_length) {
      paste0(substr(name, 1, max_length), "...")
    } else {
      name
    }
  }, character(1))
}

#' @noRd
create_empty_physician_tibble <- function(required_columns = NULL) {
  logger::log_info("Creating empty physician result tibble")
  
  # Basic columns
  columns <- c(
    "NPI" = character(),
    "Entity Type Code" = integer(),
    "Year" = integer(),
    "Zip" = character(),
    "Provider Business Mailing Address Country Code (If outside U.S.)" = character(),
    "Provider Business Practice Location Address Country Code (If outside U.S.)" = character()
  )
  
  # Add required columns if specified
  if (!is.null(required_columns) && length(required_columns) > 0) {
    for (col in required_columns) {
      if (!col %in% names(columns)) {
        columns <- c(columns, setNames(list(character()), col))
      }
    }
  }
  
  # Create and return empty tibble
  empty_tibble <- tibble::tibble(!!!columns)
  return(empty_tibble)
}

#' @noRd
query_all_years_batch <- function(con, filtered_mapping, taxonomy_codes, batch_size, 
                                  verbose, essential_columns) {
  logger::log_info("Starting batched query across %d tables", nrow(filtered_mapping))
  
  # Use map_dfr to efficiently combine results while processing one year at a time
  physician_combined <- purrr::map_dfr(1:nrow(filtered_mapping), function(i) {
    current_table <- filtered_mapping$table_name[i]
    current_year <- filtered_mapping$year[i]
    
    logger::log_info("Processing year %d (table: %s)", current_year, current_table)
    
    # Check if table exists
    if (!current_table %in% DBI::dbListTables(con)) {
      logger::log_warn("Table '%s' not found in database, skipping", current_table)
      return(tibble::tibble())
    }
    
    # Process table in batches
    yearly_physicians <- process_table_in_batches(
      con, 
      current_table, 
      current_year, 
      taxonomy_codes, 
      batch_size, 
      verbose,
      essential_columns
    )
    
    # Force garbage collection after processing each year
    if (i %% 2 == 0) {
      logger::log_info("Running garbage collection after year %d", current_year)
      invisible(gc())
    }
    
    return(yearly_physicians)
  })
  
  # Final cleanup
  invisible(gc())
  
  return(physician_combined)
}

#' @noRd
process_table_in_batches <- function(con, current_table, current_year, taxonomy_codes, 
                                     batch_size, verbose, essential_columns) {
  # Get taxonomy columns
  taxonomy_columns <- get_taxonomy_columns(con, current_table)
  
  if (length(taxonomy_columns) == 0) {
    logger::log_warn("No taxonomy columns found in table '%s'", current_table)
    return(tibble::tibble())
  }
  
  # Determine total count for batching
  logger::log_info("Identifying total qualifying records")
  total_count <- get_physician_count(con, current_table, current_year, taxonomy_codes, 
                                     taxonomy_columns)
  
  if (total_count == 0) {
    logger::log_info("No matching physicians found for year %d", current_year)
    return(tibble::tibble())
  }
  
  logger::log_info("Found %d matching physicians for year %d", total_count, current_year)
  logger::log_info("Will process in %d batches of size %d", 
                   ceiling(total_count / batch_size), batch_size)
  
  # Initialize empty tibble for this year's results
  yearly_results <- tibble::tibble()
  
  # Process in batches
  offset <- 0
  batch_number <- 1
  
  while (offset < total_count) {
    logger::log_info("Processing batch at offset %d (year %d)", offset, current_year)
    
    # Fetch batch with specific columns preservation
    batch_data <- fetch_physician_batch(
      con, 
      current_table, 
      current_year, 
      taxonomy_codes, 
      taxonomy_columns, 
      batch_size, 
      offset,
      essential_columns
    )
    
    if (nrow(batch_data) > 0) {
      # Standardize and process batch
      batch_data <- standardize_physician_data(batch_data, current_year, essential_columns)
      
      # Append to results
      yearly_results <- dplyr::bind_rows(yearly_results, batch_data)
      
      logger::log_info("Added batch of %d physicians (total so far: %d)", 
                       nrow(batch_data), nrow(yearly_results))
    } else {
      logger::log_warn("Empty batch returned at offset %d", offset)
      break
    }
    
    # Update offset for next batch
    offset <- offset + batch_size
    batch_number <- batch_number + 1
    
    # Force garbage collection every 5 batches
    if (batch_number %% 5 == 0) {
      logger::log_info("Running garbage collection during batch processing")
      invisible(gc())
    }
  }
  
  return(yearly_results)
}

#' @noRd
get_taxonomy_columns <- function(con, table_name) {
  taxonomy_columns <- DBI::dbListFields(con, table_name) %>%
    grep("Healthcare Provider Taxonomy Code_[0-9]+", ., value = TRUE)
  
  logger::log_info("Found %d taxonomy columns in table '%s'", 
                   length(taxonomy_columns), table_name)
  
  return(taxonomy_columns)
}

#' @noRd
get_physician_count <- function(con, current_table, current_year, taxonomy_codes, 
                                taxonomy_columns) {
  count <- 0
  tryCatch({
    # Create SQL for counting
    taxonomy_filter <- create_taxonomy_filter(taxonomy_columns, taxonomy_codes)
    
    count_query <- sprintf('
      SELECT COUNT(*) AS count FROM "%s"
      WHERE (%s) AND "Entity Type Code" = 1', 
                           current_table, taxonomy_filter)
    
    count_result <- DBI::dbGetQuery(con, count_query)
    count <- count_result$count[1]
  }, error = function(e) {
    logger::log_error("Error getting physician count: %s", e$message)
    count <- 0
  })
  
  return(count)
}

#' @noRd
create_taxonomy_filter <- function(taxonomy_columns, taxonomy_codes) {
  taxonomy_filter <- paste(sapply(taxonomy_columns, function(col) {
    sprintf('"%s" IN (%s)', col, 
            paste(sprintf("'%s'", taxonomy_codes), collapse = ","))
  }), collapse = " OR ")
  
  return(taxonomy_filter)
}

#' @noRd
fetch_physician_batch <- function(con, current_table, current_year, taxonomy_codes,
                                  taxonomy_columns, batch_size, offset, essential_columns) {
  batch_data <- tibble::tibble()
  
  tryCatch({
    # Create taxonomy filter
    taxonomy_filter <- create_taxonomy_filter(taxonomy_columns, taxonomy_codes)
    
    # Add taxonomy columns to ensure we have them
    columns_to_select <- c(essential_columns, taxonomy_columns)
    
    # Check which columns actually exist in the table
    available_columns <- DBI::dbListFields(con, current_table)
    columns_to_select <- intersect(columns_to_select, available_columns)
    
    # If any essential columns are missing, log a warning
    missing_columns <- setdiff(essential_columns, available_columns)
    if (length(missing_columns) > 0) {
      logger::log_warn("Essential columns missing from table: %s", 
                       paste(shorten_column_names(missing_columns), collapse = ", "))
    }
    
    # Create column selection string - add quotes for SQL safety
    columns_str <- paste(sprintf('"%s"', columns_to_select), collapse = ", ")
    
    # Create query with LIMIT and OFFSET for batching
    query <- sprintf('
      SELECT %s, %d AS Year 
      FROM "%s"
      WHERE (%s) AND "Entity Type Code" = 1
      LIMIT %d OFFSET %d', 
                     columns_str, current_year, current_table, 
                     taxonomy_filter, batch_size, offset)
    
    batch_data <- DBI::dbGetQuery(con, query)
    logger::log_info("Retrieved batch with %d records", nrow(batch_data))
    
    # Make sure missing essential columns exist as NA
    for (col in missing_columns) {
      batch_data[[col]] <- NA
    }
    
  }, error = function(e) {
    logger::log_error("Error fetching physician batch: %s", e$message)
  })
  
  return(batch_data)
}

#' @noRd
standardize_physician_data <- function(physician_data, current_year, essential_columns) {
  # Ensure we have a tibble
  physician_data <- tibble::as_tibble(physician_data)
  
  # Add year if not present
  if (!"Year" %in% colnames(physician_data)) {
    physician_data$Year <- current_year
  }
  
  # Extract ZIP code
  if ("Provider Business Practice Location Address Postal Code" %in% colnames(physician_data)) {
    physician_data <- physician_data %>%
      dplyr::mutate(Zip = stringr::str_sub(
        .data$`Provider Business Practice Location Address Postal Code`, 1, 5))
  } else if (!"Zip" %in% colnames(physician_data)) {
    physician_data$Zip <- NA_character_
  }
  
  # Ensure all essential columns exist
  for (col in essential_columns) {
    if (!col %in% colnames(physician_data)) {
      physician_data[[col]] <- NA
    }
  }
  
  # Ensure country code columns have "US" as default value when missing
  # This prevents filtering errors downstream
  for (col in c("Provider Business Mailing Address Country Code (If outside U.S.)",
                "Provider Business Practice Location Address Country Code (If outside U.S.)")) {
    if (col %in% colnames(physician_data)) {
      # Replace NAs with "US" as a safe default (these are US physician records)
      physician_data <- physician_data %>%
        dplyr::mutate(!!col := ifelse(is.na(.data[[col]]), "US", as.character(.data[[col]])))
    }
  }
  
  # Convert column types for consistency
  physician_data <- physician_data %>%
    dplyr::mutate(
      NPI = as.character(.data$NPI),
      `Entity Type Code` = as.integer(.data$`Entity Type Code`),
      Year = as.integer(.data$Year),
      Zip = as.character(.data$Zip)
    )
  
  return(physician_data)
}

#------------------------------------------------------------------------------
# 4. Extract OBGYN Providers from NPPES
#------------------------------------------------------------------------------
obgyn_physicians_all_years <- find_physicians_across_years(
  con = con,
  table_year_mapping = table_year_mapping,
  taxonomy_codes = obgyn_taxonomy_codes,
  verbose = TRUE,
  nppes_pattern = "npidata"  # This will filter to only include tables with "npidata" in the name
) %>%
  dplyr::mutate(NPI = as.character(NPI)) %>%
  dplyr::distinct(NPI, Year, .keep_all = TRUE); obgyn_physicians_all_years #%>%
  # dplyr::filter(
  #   `Provider Business Mailing Address Country Code (If outside U.S.)` %in% c("US") &
  #     `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US"
  # ) %>%
  # dplyr::select(
  #   -`Provider Business Mailing Address Country Code (If outside U.S.)`,
  #   -`Provider Business Practice Location Address Country Code (If outside U.S.)`
  # )
glimpse(obgyn_physicians_all_years)

#------------------------------------------------------------------------------
# 5. Clean State Names and Year Formatting
#------------------------------------------------------------------------------
# obgyn_physicians_all_years <- obgyn_physicians_all_years %>%
#   dplyr::mutate(
#     `Provider Business Practice Location Address State Name` = tyler::phase0_convert_state_abbreviations(
#       `Provider Business Practice Location Address State Name`
#     ),
#     `Provider Business Mailing Address State Name` = tyler::phase0_convert_state_abbreviations(
#       `Provider Business Mailing Address State Name`
#     ),
#     Year = as.character(Year)
#   )

#------------------------------------------------------------------------------
# 6. Save Processed NPPES OBGYN Data
#------------------------------------------------------------------------------

# "/Users/tylermuffly/Dropbox (Personal)/isochrones"
output_path <- "data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.rds"
saveRDS(obgyn_physicians_all_years, output_path)

output_path <- "data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.csv"
readr::write_csv(obgyn_physicians_all_years, output_path)

logger::log_info("Saved NPPES OBGYN providers to: {output_path}")

#------------------------------------------------------------------------------
# 7. Disconnect Database Connection
#------------------------------------------------------------------------------
DBI::dbDisconnect(con)
logger::log_info("Disconnected from DuckDB")

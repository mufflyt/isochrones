#==============================================================================
# Extracting and Processing NPPES Provider Data
#
# Purpose: This script connects to a DuckDB database of National Provider 
# Identifier (NPI) data, analyzes the tables, and extracts information about
# OBGYN practitioners across multiple years.
#==============================================================================

#------------------------------------------------------------------------------
# 1. Load Required Packages
#------------------------------------------------------------------------------
# Core database packages
library(DBI)
library(duckdb)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(tyler)      # For state abbreviation conversion
library(logger)     # For logging operations
library(assertthat) # For input validation
library(tibble)     # For enhanced data frames

# Load custom functions for NPPES data processing
source("R/bespoke_functions.R"); invisible(gc()); invisible(gc())

#------------------------------------------------------------------------------
# 2. Database Connection and Exploration
#------------------------------------------------------------------------------
# Define database path
db_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_path)

# List all tables in the database
tables <- dbListTables(con)
print("Tables in the database:")
print(tables)

# Explore each table's structure and content
# for (table_name in tables) {
#   cat("\n\n=============================================\n")
#   cat("Table:", table_name, "\n")
#   cat("=============================================\n")
#   
#   # Get table structure
#   table_structure <- dbGetQuery(con, paste0("DESCRIBE ", table_name))
#   print("Table structure:")
#   print(table_structure)
#   
#   # Count rows
#   row_count <- dbGetQuery(con, paste0("SELECT COUNT(*) FROM ", table_name))
#   print(paste0("Row count: ", row_count))
#   
#   # Show sample data (first 5 rows)
#   sample_data <- dbGetQuery(con, paste0("SELECT * FROM ", table_name, " LIMIT 5"))
#   print("Sample data (first 5 rows):")
#   print(sample_data)
# }

#------------------------------------------------------------------------------
# 3. Year-to-Table Mapping
#------------------------------------------------------------------------------

# Create a mapping between years and table names for historical NPPES data
# Generate the table mapping automatically
table_year_mapping <- create_nppes_table_mapping(con)

# Print the mapping to verify
print(table_year_mapping)

#------------------------------------------------------------------------------
# 4. Define OBGYN Taxonomy Codes
#------------------------------------------------------------------------------
# These codes identify different specialties within obstetrics and gynecology
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology
  "207VX0201X",    # Gynecologic Oncology
  "207VE0102X",    # Reproductive Endocrinology
  "207VG0400X",    # Gynecology
  "207VM0101X",    # Maternal & Fetal Medicine
  "207VF0040X",    # Female Pelvic Medicine
  "207VB0002X",    # Bariatric Medicine
  "207VC0200X",    # Critical care medicine
  "207VC0040X", 
  "207VC0300X",    # Complex family planning
  "207VH0002X",    # Palliative care
  "207VX0000X"     # Obstetrics only
)

#' #------------------------------------------------------------------------------
#' # 5. Find and Process OBGYN Practitioners
#' #------------------------------------------------------------------------------
#' # Extract OBGYN practitioners from all available years
#' # Note: The find_physicians_across_years function is defined in bespoke_functions.R
#' ###
#' #' Search for healthcare providers across multiple years of NPPES data
#' #'
#' #' This function searches across multiple years of National Plan and Provider
#' #' Enumeration System (NPPES) data to find healthcare providers matching the
#' #' specified taxonomy codes. It provides a comprehensive approach to track 
#' #' providers over time.
#' #'
#' #' @param connection A valid DBI database connection object
#' #' @param table_year_mapping A data frame mapping table names to years with
#' #'   columns 'table_name' and 'year'
#' #' @param taxonomy_codes A character vector of healthcare provider taxonomy codes
#' #'   to search for
#' #' @param years_to_include Optional numeric vector of years to include in the 
#' #'   search. If NULL (default), all years in table_year_mapping are searched.
#' #' @param max_results_per_year Maximum number of results to return per year
#' #'   (default: 1000000)
#' #' @param verbose Logical; if TRUE (default), detailed logging information is 
#' #'   displayed
#' #'
#' #' @return A tibble containing healthcare provider information across all 
#' #'   queried years
#' #'
#' #' @examples
#' #' # Connect to a DuckDB database
#' #' db_path <- "path/to/nppes_database.duckdb"
#' #' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#' #'
#' #' # Create a table-year mapping
#' #' table_year_mapping <- data.frame(
#' #'   table_name = c("nppes_2020", "nppes_2021", "nppes_2022"),
#' #'   year = c(2020, 2021, 2022)
#' #' )
#' #'
#' #' # Define taxonomy codes for OB-GYN specialties
#' #' obgyn_taxonomy_codes <- c(
#' #'   "207V00000X",  # Obstetrics & Gynecology
#' #'   "207VF0040X"   # Female Pelvic Medicine
#' #' )
#' #'
#' #' # Search for OB-GYN providers in all available years
#' #' provider_results <- find_providers_across_years(
#' #'   connection = connection,
#' #'   table_year_mapping = table_year_mapping,
#' #'   taxonomy_codes = obgyn_taxonomy_codes,
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Search for providers only in specific years with limited results
#' #' recent_providers <- find_providers_across_years(
#' #'   connection = connection,
#' #'   table_year_mapping = table_year_mapping,
#' #'   taxonomy_codes = c("207V00000X"),
#' #'   years_to_include = c(2021, 2022),
#' #'   max_results_per_year = 5000,
#' #'   verbose = FALSE
#' #' )
#' #'
#' #' # Clean up connection
#' #' DBI::dbDisconnect(connection)
#' #'
#' #' @importFrom assertthat assert_that
#' #' @importFrom logger log_threshold log_info log_warn log_error log_debug
#' #' @importFrom DBI dbIsValid dbListTables dbGetQuery
#' #' @importFrom dplyr tbl filter select rename mutate collect all_of bind_rows
#' #' @importFrom tibble tibble as_tibble
#' #' @importFrom stringr str_sub
#' #' @importFrom purrr reduce
#' #' @importFrom rlang expr
#' find_providers_across_years <- function(connection,
#'                                         table_year_mapping,
#'                                         taxonomy_codes,
#'                                         years_to_include = NULL,
#'                                         max_results_per_year = 1000000,
#'                                         verbose = TRUE) {
#'   # Set up logging threshold based on verbose parameter
#'   logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
#'   logger::log_info("Starting provider search across years")
#'   
#'   # Validate all inputs
#'   validate_inputs(connection, table_year_mapping, taxonomy_codes, 
#'                   years_to_include, max_results_per_year)
#'   
#'   # Filter mapping to requested years
#'   filtered_mapping <- filter_year_mapping(table_year_mapping, years_to_include)
#'   
#'   # Return empty result if no matching years found
#'   if (nrow(filtered_mapping) == 0) {
#'     logger::log_warn("No matching years found in the mapping")
#'     return(create_empty_provider_tibble())
#'   }
#'   
#'   # Query all years in the filtered mapping
#'   provider_listings <- query_all_years(connection, filtered_mapping, taxonomy_codes, 
#'                                        max_results_per_year, verbose)
#'   
#'   # Combine results with consistent data types
#'   combined_providers <- combine_provider_results(provider_listings)
#'   
#'   # Check for important providers that might be missing from the results
#'   combined_providers <- check_for_missing_providers(
#'     connection, 
#'     combined_providers, 
#'     filtered_mapping, 
#'     taxonomy_codes,
#'     verbose
#'   )
#'   
#'   logger::log_info("Provider search completed successfully")
#'   return(combined_providers)
#' }
#' 
#' #' Validate all function inputs
#' #'
#' #' @noRd
#' validate_inputs <- function(connection, table_year_mapping, taxonomy_codes, 
#'                             years_to_include, max_results_per_year) {
#'   # Check database connection
#'   assertthat::assert_that(DBI::dbIsValid(connection), 
#'                           msg = "Database connection is not valid")
#'   
#'   # Check table year mapping
#'   assertthat::assert_that(is.data.frame(table_year_mapping),
#'                           msg = "table_year_mapping must be a data frame")
#'   assertthat::assert_that(all(c("table_name", "year") %in% colnames(table_year_mapping)),
#'                           msg = "table_year_mapping must contain 'table_name' and 'year' columns")
#'   
#'   # Check taxonomy codes
#'   assertthat::assert_that(is.character(taxonomy_codes), 
#'                           msg = "taxonomy_codes must be a character vector")
#'   assertthat::assert_that(length(taxonomy_codes) > 0, 
#'                           msg = "At least one taxonomy code must be provided")
#'   
#'   # Check years to include (if provided)
#'   if (!is.null(years_to_include)) {
#'     assertthat::assert_that(is.numeric(years_to_include),
#'                             msg = "years_to_include must be a numeric vector")
#'     assertthat::assert_that(length(years_to_include) > 0,
#'                             msg = "At least one year must be provided if years_to_include is not NULL")
#'   }
#'   
#'   # Check max_results_per_year
#'   assertthat::assert_that(is.numeric(max_results_per_year),
#'                           msg = "max_results_per_year must be numeric")
#'   assertthat::assert_that(max_results_per_year > 0,
#'                           msg = "max_results_per_year must be greater than 0")
#'   
#'   logger::log_info("Input validation completed successfully")
#' }
#' 
#' #' Filter the table year mapping to include only requested years
#' #'
#' #' @noRd
#' filter_year_mapping <- function(table_year_mapping, years_to_include) {
#'   filtered_mapping <- table_year_mapping
#'   if (!is.null(years_to_include)) {
#'     filtered_mapping <- table_year_mapping[table_year_mapping$year %in% years_to_include, ]
#'     logger::log_info("Filtered to %d years from the mapping", nrow(filtered_mapping))
#'   }
#'   return(filtered_mapping)
#' }
#' 
#' #' Create an empty provider tibble with the expected column structure
#' #'
#' #' @noRd
#' create_empty_provider_tibble <- function() {
#'   return(tibble::tibble(
#'     NPI = character(),
#'     LastName = character(),
#'     FirstName = character(),
#'     MiddleName = character(),
#'     Gender = character(),
#'     TaxonomyCode1 = character(),
#'     City = character(),
#'     State = character(),
#'     Zip = character(),
#'     Year = integer(),
#'     `NPI Deactivation Date` = character(),
#'     `Last Update Date` = character(),
#'     `Provider Enumeration Date` = character(),
#'     `Provider Organization Name (Legal Business Name)` = character(),
#'     `Provider Credential Text` = character(),
#'     `Provider First Line Business Mailing Address` = character(),
#'     `Provider Business Mailing Address City Name` = character(),
#'     `Provider Business Mailing Address State Name` = character(),
#'     `Provider Business Mailing Address Postal Code` = character(),
#'     `Provider Business Mailing Address Country Code (If outside U.S.)` = character(),
#'     `Provider Business Practice Location Address Country Code (If outside U.S.)` = character()
#'   ))
#' }
#' 
#' #' Query all years in the filtered mapping
#' #'
#' #' @noRd
#' query_all_years <- function(connection, filtered_mapping, taxonomy_codes, 
#'                             max_results_per_year, verbose) {
#'   logger::log_info("Querying %d tables/years: %s", 
#'                    nrow(filtered_mapping),
#'                    paste(filtered_mapping$year, collapse = ", "))
#'   
#'   all_providers <- list()
#'   
#'   for (i in 1:nrow(filtered_mapping)) {
#'     current_table <- filtered_mapping$table_name[i]
#'     current_year <- filtered_mapping$year[i]
#'     
#'     logger::log_info("Querying table for year %d: %s", current_year, current_table)
#'     
#'     if (!current_table %in% DBI::dbListTables(connection)) {
#'       logger::log_warn("Table '%s' not found in database, skipping", current_table)
#'       next
#'     }
#'     
#'     providers_data <- try_query_methods(connection, current_table, current_year, 
#'                                         taxonomy_codes, max_results_per_year, verbose)
#'     
#'     if (!is.null(providers_data) && nrow(providers_data) > 0) {
#'       providers_data <- standardize_column_types(providers_data)
#'       all_providers[[i]] <- providers_data
#'       logger::log_info("Added %d providers for year %d", 
#'                        nrow(providers_data), current_year)
#'     } else {
#'       logger::log_info("No providers found for year %d", current_year)
#'     }
#'   }
#'   
#'   return(all_providers)
#' }
#' 
#' #' Try different query methods to retrieve provider data
#' #'
#' #' @noRd
#' try_query_methods <- function(connection, current_table, current_year, taxonomy_codes, 
#'                               max_results_per_year, verbose) {
#'   providers_data <- try_dplyr_approach(connection, current_table, current_year, 
#'                                        taxonomy_codes, max_results_per_year, verbose)
#'   
#'   if (is.null(providers_data)) {
#'     providers_data <- try_sql_approach(connection, current_table, current_year, 
#'                                        taxonomy_codes, max_results_per_year, verbose)
#'   }
#'   
#'   if (is.null(providers_data) && verbose) {
#'     providers_data <- try_direct_approach(connection, current_table, current_year, 
#'                                           taxonomy_codes, max_results_per_year)
#'   }
#'   
#'   return(providers_data)
#' }
#' 
#' #' Try to retrieve provider data using dplyr approach
#' #'
#' #' @noRd
#' try_dplyr_approach <- function(connection, current_table, current_year, taxonomy_codes, 
#'                                max_results_per_year, verbose) {
#'   providers_data <- NULL
#'   
#'   tryCatch({
#'     logger::log_info("Attempting dplyr-based approach")
#'     
#'     nppes_table <- dplyr::tbl(connection, current_table)
#'     
#'     col_names <- colnames(nppes_table)
#'     taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
#'                              col_names, value = TRUE)
#'     
#'     if (length(taxonomy_columns) > 0) {
#'       logger::log_info("Found %d taxonomy code columns", length(taxonomy_columns))
#'       
#'       entity_type_col_exists <- "Entity Type Code" %in% col_names
#'       
#'       query <- nppes_table
#'       
#'       filter_expr <- NULL
#'       
#'       for (i in seq_along(taxonomy_columns)) {
#'         tax_col <- taxonomy_columns[i]
#'         
#'         if (i == 1) {
#'           filter_expr <- rlang::expr(.data[[tax_col]] %in% !!taxonomy_codes)
#'         } else {
#'           filter_expr <- rlang::expr(!!filter_expr | .data[[tax_col]] %in% !!taxonomy_codes)
#'         }
#'       }
#'       
#'       query <- dplyr::filter(query, !!filter_expr)
#'       logger::log_info("Applied combined filter across all %d taxonomy columns", 
#'                        length(taxonomy_columns))
#'       
#'       if (entity_type_col_exists) {
#'         query <- query %>% dplyr::filter(`Entity Type Code` == 1L)
#'         logger::log_info("Applied Entity Type Code filter for providers (1)")
#'       } else {
#'         logger::log_warn("Entity Type Code column not found, skipping filter")
#'       }
#'       
#'       cols_to_select <- c(
#'         "NPI", 
#'         "Provider Last Name (Legal Name)", 
#'         "Provider First Name",
#'         "Provider Middle Name", 
#'         "Provider Gender Code",
#'         taxonomy_columns[1],
#'         "Provider Business Practice Location Address City Name",
#'         "Provider Business Practice Location Address State Name",
#'         "Provider Business Practice Location Address Postal Code",
#'         "NPI Deactivation Date", 
#'         "Last Update Date", 
#'         "Provider Enumeration Date", 
#'         "Provider Organization Name (Legal Business Name)", 
#'         "Provider Credential Text", 
#'         "Provider First Line Business Mailing Address", 
#'         "Provider Business Mailing Address City Name", 
#'         "Provider Business Mailing Address State Name", 
#'         "Provider Business Mailing Address Postal Code", 
#'         "Provider Business Mailing Address Country Code (If outside U.S.)", 
#'         "Provider Business Practice Location Address Country Code (If outside U.S.)"
#'       )
#'       
#'       if (length(taxonomy_columns) > 1) {
#'         cols_to_select <- c(cols_to_select, taxonomy_columns[-1])
#'         logger::log_info("Including all %d taxonomy columns in output", length(taxonomy_columns))
#'       }
#'       
#'       available_cols <- cols_to_select[cols_to_select %in% col_names]
#'       logger::log_info("Found %d of %d requested columns", 
#'                        length(available_cols), length(cols_to_select))
#'       
#'       query <- query %>% dplyr::select(dplyr::all_of(available_cols))
#'       
#'       rename_map <- c(
#'         NPI = "NPI",
#'         LastName = "Provider Last Name (Legal Name)",
#'         FirstName = "Provider First Name",
#'         MiddleName = "Provider Middle Name",
#'         Gender = "Provider Gender Code",
#'         TaxonomyCode1 = taxonomy_columns[1],
#'         City = "Provider Business Practice Location Address City Name",
#'         State = "Provider Business Practice Location Address State Name",
#'         Zip = "Provider Business Practice Location Address Postal Code"
#'       )
#'       
#'       rename_cols <- rename_map[names(rename_map) %in% colnames(query)]
#'       
#'       providers_data <- query %>%
#'         dplyr::rename(!!!rename_cols) %>%
#'         dplyr::mutate(Year = as.integer(current_year)) %>%
#'         dplyr::collect(n = max_results_per_year)
#'       
#'       if ("Zip" %in% colnames(providers_data)) {
#'         providers_data <- providers_data %>%
#'           dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
#'         logger::log_info("Cleaned Zip codes to 5 digits")
#'       }
#'       
#'       if (nrow(providers_data) > 0) {
#'         logger::log_info("Dplyr approach succeeded, found %d providers", 
#'                          nrow(providers_data))
#'       } else {
#'         logger::log_info("No results from dplyr approach")
#'         providers_data <- NULL
#'       }
#'     }
#'   }, error = function(e) {
#'     logger::log_error("Dplyr approach failed: %s", e$message)
#'     providers_data <- NULL
#'   })
#'   
#'   return(providers_data)
#' }
#' 
#' #' Try to retrieve provider data using direct SQL approach
#' #'
#' #' @noRd
#' try_sql_approach <- function(connection, current_table, current_year, taxonomy_codes, 
#'                              max_results_per_year, verbose) {
#'   providers_data <- NULL
#'   
#'   tryCatch({
#'     logger::log_info("Attempting direct SQL approach")
#'     
#'     sample_data <- DBI::dbGetQuery(connection, sprintf("SELECT * FROM \"%s\" LIMIT 1", current_table))
#'     taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
#'                              colnames(sample_data), value = TRUE)
#'     
#'     logger::log_info("Found %d taxonomy columns in database", length(taxonomy_columns))
#'     
#'     if (length(taxonomy_columns) > 0) {
#'       taxonomy_values <- paste(sprintf("'%s'", taxonomy_codes), collapse = ",")
#'       
#'       taxonomy_filters <- c()
#'       for (i in 1:length(taxonomy_columns)) {
#'         taxonomy_filters <- c(
#'           taxonomy_filters,
#'           sprintf("\"%s\" IN (%s)", taxonomy_columns[i], taxonomy_values)
#'         )
#'       }
#'       
#'       taxonomy_filter <- paste(taxonomy_filters, collapse = " OR ")
#'       
#'       entity_filter <- ""
#'       if ("Entity Type Code" %in% colnames(sample_data)) {
#'         entity_filter <- "AND \"Entity Type Code\" = 1"
#'         logger::log_info("Added Entity Type Code filter for providers (1)")
#'       } else {
#'         logger::log_warn("Entity Type Code column not found in SQL approach, skipping filter")
#'       }
#'       
#'       core_columns <- c(
#'         "\"NPI\"", 
#'         "\"Provider Last Name (Legal Name)\" AS LastName",
#'         "\"Provider First Name\" AS FirstName",
#'         "\"Provider Middle Name\" AS MiddleName",
#'         "\"Provider Gender Code\" AS Gender",
#'         sprintf("\"%s\" AS TaxonomyCode1", taxonomy_columns[1]),
#'         "\"Provider Business Practice Location Address City Name\" AS City",
#'         "\"Provider Business Practice Location Address State Name\" AS State",
#'         "\"Provider Business Practice Location Address Postal Code\" AS Zip"
#'       )
#'       
#'       additional_columns <- c(
#'         "\"NPI Deactivation Date\"",
#'         "\"Last Update Date\"",
#'         "\"Provider Enumeration Date\"",
#'         "\"Provider Organization Name (Legal Business Name)\"",
#'         "\"Provider Credential Text\"",
#'         "\"Provider First Line Business Mailing Address\"",
#'         "\"Provider Business Mailing Address City Name\"",
#'         "\"Provider Business Mailing Address State Name\"",
#'         "\"Provider Business Mailing Address Postal Code\"",
#'         "\"Provider Business Mailing Address Country Code (If outside U.S.)\"",
#'         "\"Provider Business Practice Location Address Country Code (If outside U.S.)\""
#'       )
#'       
#'       if (length(taxonomy_columns) > 1) {
#'         for (i in 2:length(taxonomy_columns)) {
#'           additional_columns <- c(
#'             additional_columns,
#'             sprintf("\"%s\"", taxonomy_columns[i])
#'           )
#'         }
#'       }
#'       
#'       available_additional_cols <- c()
#'       for (col in additional_columns) {
#'         col_name <- gsub("\"(.*)\".*", "\\1", col)
#'         if (col_name %in% colnames(sample_data)) {
#'           available_additional_cols <- c(available_additional_cols, col)
#'         }
#'       }
#'       
#'       logger::log_info("Found %d of %d additional columns", 
#'                        length(available_additional_cols), length(additional_columns))
#'       
#'       all_columns <- c(core_columns, available_additional_cols)
#'       column_sql <- paste(all_columns, collapse = ", ")
#'       
#'       logger::log_info("Building SQL query")
#'       query <- sprintf(
#'         "SELECT %s, %d AS Year
#'          FROM \"%s\"
#'          WHERE (%s) %s
#'          LIMIT %d",
#'         column_sql,
#'         current_year,
#'         current_table,
#'         taxonomy_filter,
#'         entity_filter,
#'         max_results_per_year
#'       )
#'       
#'       if (verbose) {
#'         logger::log_debug("SQL Query: %s", query)
#'       }
#'       
#'       logger::log_info("Executing SQL query")
#'       providers_data <- DBI::dbGetQuery(connection, query)
#'       
#'       if ("Zip" %in% colnames(providers_data)) {
#'         providers_data <- providers_data %>%
#'           dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
#'         logger::log_info("Cleaned Zip codes to 5 digits")
#'       }
#'       
#'       if (nrow(providers_data) > 0) {
#'         logger::log_info("SQL approach succeeded, found %d providers", 
#'                          nrow(providers_data))
#'       } else {
#'         logger::log_info("No results from SQL approach")
#'         providers_data <- NULL
#'       }
#'     }
#'   }, error = function(e) {
#'     logger::log_error("SQL approach failed: %s", e$message)
#'     providers_data <- NULL
#'   })
#'   
#'   return(providers_data)
#' }
#' 
#' #' Try to retrieve provider data using a fallback direct query approach
#' #'
#' #' @noRd
#' try_direct_approach <- function(connection, current_table, current_year, taxonomy_codes, max_results_per_year) {
#'   providers_data <- NULL
#'   
#'   tryCatch({
#'     logger::log_info("Attempting fallback direct approach")
#'     
#'     taxonomy_values <- paste(sprintf("'%s'", taxonomy_codes), collapse = ", ")
#'     
#'     query <- sprintf(
#'       "SELECT * 
#'        FROM \"%s\" 
#'        WHERE \"Entity Type Code\" = 1
#'        AND (\"Healthcare Provider Taxonomy Code_1\" IN (%s) OR
#'             \"Healthcare Provider Taxonomy Code_2\" IN (%s) OR
#'             \"Healthcare Provider Taxonomy Code_3\" IN (%s) OR
#'             \"Healthcare Provider Taxonomy Code_4\" IN (%s) OR
#'             \"Healthcare Provider Taxonomy Code_5\" IN (%s))
#'        LIMIT %d",
#'       current_table,
#'       taxonomy_values, taxonomy_values, taxonomy_values, taxonomy_values, taxonomy_values,
#'       max_results_per_year
#'     )
#'     
#'     logger::log_info("Executing direct approach SQL query")
#'     direct_results <- DBI::dbGetQuery(connection, query)
#'     
#'     if (nrow(direct_results) > 0) {
#'       direct_results$Year <- as.integer(current_year)
#'       
#'       column_mapping <- c(
#'         LastName = "Provider Last Name (Legal Name)",
#'         FirstName = "Provider First Name",
#'         MiddleName = "Provider Middle Name",
#'         Gender = "Provider Gender Code",
#'         TaxonomyCode1 = "Healthcare Provider Taxonomy Code_1",
#'         City = "Provider Business Practice Location Address City Name",
#'         State = "Provider Business Practice Location Address State Name",
#'         Zip = "Provider Business Practice Location Address Postal Code"
#'       )
#'       
#'       for (new_name in names(column_mapping)) {
#'         old_name <- column_mapping[new_name]
#'         if (old_name %in% colnames(direct_results)) {
#'           direct_results[[new_name]] <- direct_results[[old_name]]
#'         }
#'       }
#'       
#'       if ("Zip" %in% colnames(direct_results)) {
#'         direct_results$Zip <- stringr::str_sub(direct_results$Zip, 1, 5)
#'       }
#'       
#'       providers_data <- direct_results
#'       logger::log_info("Direct approach succeeded, found %d providers", 
#'                        nrow(providers_data))
#'     } else {
#'       logger::log_info("No results from direct approach")
#'     }
#'   }, error = function(e) {
#'     logger::log_error("Direct approach failed: %s", e$message)
#'   })
#'   
#'   return(providers_data)
#' }
#' 
#' #' Standardize column types for consistency
#' #'
#' #' @noRd
#' standardize_column_types <- function(provider_data) {
#'   if (is.null(provider_data) || nrow(provider_data) == 0) {
#'     return(provider_data)
#'   }
#'   
#'   logger::log_info("Standardizing column types for a result set with %d rows", 
#'                    nrow(provider_data))
#'   
#'   char_columns <- c(
#'     "NPI", "LastName", "FirstName", "MiddleName", "Gender", "TaxonomyCode1",
#'     "City", "State", "Zip", "NPI Deactivation Date", "Last Update Date", 
#'     "Provider Enumeration Date", "Provider Organization Name (Legal Business Name)",
#'     "Provider Credential Text", "Provider First Line Business Mailing Address",
#'     "Provider Business Mailing Address City Name", "Provider Business Mailing Address State Name",
#'     "Provider Business Mailing Address Postal Code",
#'     "Provider Business Mailing Address Country Code (If outside U.S.)",
#'     "Provider Business Practice Location Address Country Code (If outside U.S.)"
#'   )
#'   
#'   taxonomy_cols <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
#'                         names(provider_data), value = TRUE)
#'   if (length(taxonomy_cols) > 0) {
#'     char_columns <- c(char_columns, taxonomy_cols)
#'   }
#'   
#'   int_columns <- c("Year")
#'   
#'   # Convert character columns
#'   for (col in char_columns) {
#'     if (col %in% names(provider_data)) {
#'       if (is.factor(provider_data[[col]])) {
#'         provider_data[[col]] <- as.character(provider_data[[col]])
#'       } else if (is.numeric(provider_data[[col]])) {
#'         provider_data[[col]] <- as.character(provider_data[[col]])
#'       } else if (inherits(provider_data[[col]], "Date") || 
#'                  inherits(provider_data[[col]], "POSIXt")) {
#'         provider_data[[col]] <- as.character(provider_data[[col]])
#'       } else if (!is.character(provider_data[[col]])) {
#'         provider_data[[col]] <- as.character(provider_data[[col]])
#'       }
#'     }
#'   }
#'   
#'   # Convert integer columns
#'   for (col in int_columns) {
#'     if (col %in% names(provider_data)) {
#'       if (is.factor(provider_data[[col]]) || is.character(provider_data[[col]])) {
#'         provider_data[[col]] <- as.integer(as.character(provider_data[[col]]))
#'       } else if (is.numeric(provider_data[[col]]) && !is.integer(provider_data[[col]])) {
#'         provider_data[[col]] <- as.integer(provider_data[[col]])
#'       } else if (!is.integer(provider_data[[col]])) {
#'         provider_data[[col]] <- as.integer(as.character(provider_data[[col]]))
#'       }
#'     }
#'   }
#'   
#'   # Add missing columns with NA values
#'   expected_columns <- c(char_columns, int_columns)
#'   missing_columns <- setdiff(expected_columns, names(provider_data))
#'   
#'   if (length(missing_columns) > 0) {
#'     for (col in missing_columns) {
#'       if (col %in% char_columns) {
#'         provider_data[[col]] <- NA_character_
#'       } else if (col %in% int_columns) {
#'         provider_data[[col]] <- NA_integer_
#'       }
#'     }
#'   }
#'   
#'   return(provider_data)
#' }
#' 
#' #' Combine provider results from multiple years ensuring consistent types
#' #'
#' #' @noRd
#' combine_provider_results <- function(provider_listings) {
#'   valid_listings <- Filter(function(x) {
#'     return(!is.null(x) && (is.data.frame(x) || is.list(x) && length(x) > 0))
#'   }, provider_listings)
#'   
#'   if (length(valid_listings) == 0) {
#'     logger::log_warn("No valid provider data found after filtering")
#'     return(create_empty_provider_tibble())
#'   }
#'   
#'   logger::log_info("Combining %d valid data sources out of %d total", 
#'                    length(valid_listings), length(provider_listings))
#'   
#'   all_columns <- unique(unlist(lapply(valid_listings, names)))
#'   
#'   # Standardize all listings to have the same columns
#'   standardized_listings <- lapply(valid_listings, function(df) {
#'     missing_cols <- setdiff(all_columns, names(df))
#'     for (col in missing_cols) {
#'       if (col %in% c("Year")) {
#'         df[[col]] <- NA_integer_
#'       } else {
#'         df[[col]] <- NA_character_
#'       }
#'     }
#'     return(df)
#'   })
#'   
#'   tryCatch({
#'     # Try primary approach to combine data
#'     all_providers <- purrr::reduce(standardized_listings, function(acc, x) {
#'       x <- x[, names(acc)]
#'       return(rbind(acc, x))
#'     })
#'     
#'     logger::log_info("Found a total of %d providers across all years", 
#'                      nrow(all_providers))
#'     
#'     all_providers <- tibble::as_tibble(all_providers)
#'     
#'     return(all_providers)
#'   }, error = function(e) {
#'     logger::log_error("Error combining provider data: %s", e$message)
#'     
#'     logger::log_warn("Attempting fallback approach for combining data")
#'     
#'     # Try fallback approach if the primary approach fails
#'     tryCatch({
#'       template <- create_empty_provider_tibble()
#'       
#'       processed_listings <- lapply(standardized_listings, function(df) {
#'         common_cols <- intersect(names(df), names(template))
#'         new_df <- df[, common_cols, drop = FALSE]
#'         
#'         missing_cols <- setdiff(names(template), names(new_df))
#'         for (col in missing_cols) {
#'           if (col %in% c("Year")) {
#'             new_df[[col]] <- NA_integer_
#'           } else {
#'             new_df[[col]] <- NA_character_
#'           }
#'         }
#'         
#'         new_df <- new_df[, names(template)]
#'         return(new_df)
#'       })
#'       
#'       all_providers <- do.call(rbind, processed_listings)
#'       all_providers <- tibble::as_tibble(all_providers)
#'       
#'       logger::log_info("Fallback approach succeeded with %d providers", 
#'                        nrow(all_providers))
#'       return(all_providers)
#'     }, error = function(inner_e) {
#'       logger::log_error("Fallback approach also failed: %s", inner_e$message)
#'       
#'       # Last resort fallback - return just the first data source
#'       if (length(valid_listings) > 0) {
#'         logger::log_warn("Returning only the first data source as final fallback")
#'         return(valid_listings[[1]])
#'       } else {
#'         return(create_empty_provider_tibble())
#'       }
#'     })
#'   })
#' }
#' 
#' #' Check for specific important providers that might be missing and add them
#' #'
#' #' @noRd
#' check_for_missing_providers <- function(connection, combined_providers, filtered_mapping, 
#'                                         taxonomy_codes, verbose) {
#'   if (!verbose) {
#'     return(combined_providers)
#'   }
#'   
#'   logger::log_info("Checking for known providers that might be missing")
#'   
#'   # List of critical NPIs to ensure are included
#'   important_npis <- c(
#'     "1689603763"  # Known important provider
#'     # Add other critical NPIs as needed
#'   )
#'   
#'   found_npis <- intersect(important_npis, combined_providers$NPI)
#'   missing_npis <- setdiff(important_npis, found_npis)
#'   
#'   if (length(missing_npis) > 0) {
#'     logger::log_warn("Found %d missing important providers. Attempting direct look up.", 
#'                      length(missing_npis))
#'     
#'     for (npi in missing_npis) {
#'       logger::log_info("Looking for NPI %s in all tables", npi)
#'       
#'       for (i in 1:nrow(filtered_mapping)) {
#'         year <- filtered_mapping$year[i]
#'         table_name <- filtered_mapping$table_name[i]
#'         
#'         # Try direct lookup by NPI
#'         direct_query <- sprintf(
#'           "SELECT * FROM \"%s\" 
#'            WHERE \"NPI\" = '%s' 
#'            AND \"Entity Type Code\" = 1",
#'           table_name, npi
#'         )
#'         
#'         direct_result <- DBI::dbGetQuery(connection, direct_query)
#'         
#'         if (nrow(direct_result) > 0) {
#'           logger::log_info("Found missing NPI %s in year %d", npi, year)
#'           
#'           # Add year column
#'           direct_result$Year <- as.integer(year)
#'           
#'           # Standardize column names
#'           column_mapping <- c(
#'             LastName = "Provider Last Name (Legal Name)",
#'             FirstName = "Provider First Name",
#'             MiddleName = "Provider Middle Name",
#'             Gender = "Provider Gender Code",
#'             TaxonomyCode1 = "Healthcare Provider Taxonomy Code_1",
#'             City = "Provider Business Practice Location Address City Name",
#'             State = "Provider Business Practice Location Address State Name",
#'             Zip = "Provider Business Practice Location Address Postal Code"
#'           )
#'           
#'           # Rename columns
#'           for (new_name in names(column_mapping)) {
#'             old_name <- column_mapping[new_name]
#'             if (old_name %in% colnames(direct_result)) {
#'               direct_result[[new_name]] <- direct_result[[old_name]]
#'             }
#'           }
#'           
#'           # Clean zip
#'           if ("Zip" %in% colnames(direct_result)) {
#'             direct_result$Zip <- stringr::str_sub(direct_result$Zip, 1, 5)
#'           }
#'           
#'           # Standardize data types
#'           direct_result <- standardize_column_types(direct_result)
#'           
#'           # Bind to results
#'           combined_providers <- dplyr::bind_rows(combined_providers, direct_result)
#'           logger::log_info("Added missing provider with NPI %s for year %d", npi, year)
#'         }
#'       }
#'     }
#'   }
#'   
#'   return(combined_providers)
#' }
#' 
#' #' Generate a mapping between NPPES database tables and years
#' #'
#' #' Analyzes table names in a DuckDB database to create a mapping between
#' #' tables containing NPPES data and the years they represent. This is particularly
#' #' useful for tracking healthcare providers across multiple years.
#' #'
#' #' @param connection A valid DBI database connection
#' #' @param verbose Logical; if TRUE, provides detailed logs during execution
#' #'
#' #' @return A data frame with columns 'table_name' and 'year'
#' #'
#' #' @examples
#' #' # Connect to a DuckDB database
#' #' db_path <- "path/to/nppes_database.duckdb"
#' #' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#' #'
#' #' # Generate table-year mapping
#' #' mapping <- create_nppes_table_mapping(connection)
#' #'
#' #' # View the mapping
#' #' print(mapping)
#' #'
#' #' # Clean up connection
#' #' DBI::dbDisconnect(connection)
#' #'
#' #' @importFrom logger log_info log_warn
#' #' @importFrom DBI dbListTables
#' #' @importFrom tibble tibble
#' create_nppes_table_mapping <- function(connection, verbose = TRUE) {
#'   # Set up logging threshold based on verbose parameter
#'   logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
#'   logger::log_info("Generating NPPES table-year mapping")
#'   
#'   # Get all tables in the database
#'   all_tables <- DBI::dbListTables(connection)
#'   logger::log_info("Found %d total tables in database", length(all_tables))
#'   
#'   # Initialize the mapping data frame
#'   table_year_mapping <- tibble::tibble(
#'     table_name = character(),
#'     year = integer()
#'   )
#'   
#'   # Years to check for
#'   expected_years <- 2010:format(Sys.Date(), "%Y") %>% as.integer()
#'   found_years <- c()
#'   
#'   # Try to identify NPPES tables and map them to years
#'   for (table_name in all_tables) {
#'     table_year <- NULL
#'     
#'     # Check for year in table name
#'     for (year in expected_years) {
#'       year_str <- as.character(year)
#'       if (grepl(year_str, table_name)) {
#'         table_year <- year
#'         found_years <- c(found_years, year)
#'         break
#'       }
#'     }
#'     
#'     # If no year found in name, try to check a sample row
#'     if (is.null(table_year)) {
#'       tryCatch({
#'         # See if this looks like an NPPES table by checking for NPI column
#'         sample_query <- sprintf("SELECT * FROM \"%s\" LIMIT 1", table_name)
#'         sample_data <- DBI::dbGetQuery(connection, sample_query)
#'         
#'         # Check if this looks like an NPPES table
#'         if ("NPI" %in% colnames(sample_data) && 
#'             any(grepl("Healthcare Provider Taxonomy Code", colnames(sample_data)))) {
#'           # Look for date-containing columns
#'           date_cols <- c("Last Update Date", "Provider Enumeration Date")
#'           for (date_col in date_cols) {
#'             if (date_col %in% colnames(sample_data)) {
#'               date_value <- sample_data[[date_col]][1]
#'               if (!is.na(date_value)) {
#'                 # Extract year from date string
#'                 year_match <- regexpr("20[0-9]{2}", date_value)
#'                 if (year_match > 0) {
#'                   extracted_year <- as.integer(substr(date_value, year_match, year_match + 3))
#'                   if (extracted_year %in% expected_years) {
#'                     table_year <- extracted_year
#'                     found_years <- c(found_years, extracted_year)
#'                     logger::log_info("Assigned year %d to table %s based on date column", 
#'                                      table_year, table_name)
#'                     break
#'                   }
#'                 }
#'               }
#'             }
#'           }
#'         }
#'       }, error = function(e) {
#'         logger::log_warn("Error examining table %s: %s", table_name, e$message)
#'       })
#'     }
#'     
#'     # Add to mapping if a year was found
#'     if (!is.null(table_year)) {
#'       table_year_mapping <- rbind(
#'         table_year_mapping,
#'         tibble::tibble(table_name = table_name, year = table_year)
#'       )
#'     }
#'   }
#'   
#'   # Sort the mapping by year
#'   table_year_mapping <- table_year_mapping[order(table_year_mapping$year), ]
#'   
#'   # Check which expected years weren't found
#'   missing_years <- setdiff(expected_years, found_years)
#'   if (length(missing_years) > 0) {
#'     warning(sprintf("Could not find tables for the following years: %s", 
#'                     paste(missing_years, collapse = ", ")))
#'   }
#'   
#'   logger::log_info("Generated mapping with %d tables across %d years", 
#'                    nrow(table_year_mapping), length(unique(table_year_mapping$year)))
#'   
#'   return(table_year_mapping)
#' }
#' 
#' #' Analyze provider specialty changes over time
#' #'
#' #' Analyzes how healthcare providers' taxonomy codes (specialties) change over time.
#' #' This is useful for tracking career transitions, subspecialization, and other 
#' #' professional changes.
#' #'
#' #' @param provider_data A tibble containing provider data across multiple years
#' #'   as returned by find_providers_across_years()
#' #' @param include_unchanged Logical; whether to include providers who didn't change
#' #'   specialties (default: FALSE)
#' #' @param verbose Logical; if TRUE, provides detailed logs during execution
#' #'
#' #' @return A tibble with columns tracking provider information and specialty changes
#' #'
#' #' @examples
#' #' # Connect to a DuckDB database
#' #' db_path <- "path/to/nppes_database.duckdb"
#' #' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#' #'
#' #' # Create table-year mapping
#' #' table_year_mapping <- create_nppes_table_mapping(connection)
#' #'
#' #' # Define taxonomy codes for OB-GYN specialties
#' #' obgyn_taxonomy_codes <- c(
#' #'   "207V00000X",  # Obstetrics & Gynecology
#' #'   "207VF0040X"   # Female Pelvic Medicine
#' #' )
#' #'
#' #' # Search for providers
#' #' provider_results <- find_providers_across_years(
#' #'   connection = connection,
#' #'   table_year_mapping = table_year_mapping,
#' #'   taxonomy_codes = obgyn_taxonomy_codes
#' #' )
#' #'
#' #' # Analyze specialty changes
#' #' specialty_changes <- analyze_specialty_changes(
#' #'   provider_data = provider_results,
#' #'   include_unchanged = FALSE,
#' #'   verbose = TRUE
#' #' )
#' #'
#' #' # Examine results
#' #' head(specialty_changes)
#' #'
#' #' # Count transitions to Female Pelvic Medicine
#' #' fpm_transitions <- specialty_changes %>%
#' #'   dplyr::filter(
#' #'     ChangedSpecialty == TRUE,
#' #'     TaxonomyCode1 == "207VF0040X"
#' #'   )
#' #'
#' #' # Clean up connection
#' #' DBI::dbDisconnect(connection)
#' #'
#' #' @importFrom logger log_info log_threshold
#' #' @importFrom dplyr arrange group_by mutate filter
#' analyze_specialty_changes <- function(provider_data, include_unchanged = FALSE, 
#'                                       verbose = TRUE) {
#'   # Set up logging threshold based on verbose parameter
#'   logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
#'   logger::log_info("Analyzing provider specialty changes")
#'   
#'   # Validate inputs
#'   assertthat::assert_that(is.data.frame(provider_data),
#'                           msg = "provider_data must be a data frame")
#'   assertthat::assert_that("NPI" %in% colnames(provider_data),
#'                           msg = "provider_data must contain an 'NPI' column")
#'   assertthat::assert_that("TaxonomyCode1" %in% colnames(provider_data),
#'                           msg = "provider_data must contain a 'TaxonomyCode1' column")
#'   assertthat::assert_that("Year" %in% colnames(provider_data),
#'                           msg = "provider_data must contain a 'Year' column")
#'   
#'   # Process the data to identify specialty changes
#'   specialty_changes <- provider_data %>%
#'     dplyr::arrange(NPI, Year) %>%
#'     dplyr::group_by(NPI) %>%
#'     dplyr::mutate(
#'       PreviousTaxonomyCode = dplyr::lag(TaxonomyCode1),
#'       PreviousYear = dplyr::lag(Year),
#'       YearsSincePrevious = Year - PreviousYear,
#'       ChangedSpecialty = TaxonomyCode1 != PreviousTaxonomyCode & !is.na(PreviousTaxonomyCode)
#'     )
#'   
#'   # Optionally filter to only include providers who changed specialties
#'   if (!include_unchanged) {
#'     logger::log_info("Filtering to include only providers who changed specialties")
#'     
#'     # First get the list of NPIs that had at least one change
#'     changing_npis <- specialty_changes %>%
#'       dplyr::filter(ChangedSpecialty == TRUE) %>%
#'       dplyr::select(NPI) %>%
#'       dplyr::distinct() %>%
#'       dplyr::pull(NPI)
#'     
#'     # Then filter the full dataset to only include those NPIs
#'     specialty_changes <- specialty_changes %>%
#'       dplyr::filter(NPI %in% changing_npis)
#'     
#'     logger::log_info("Found %d providers who changed specialties at least once", 
#'                      length(changing_npis))
#'   }
#'   
#'   logger::log_info("Specialty change analysis complete")
#'   return(specialty_changes)
#' }

####
# Load required libraries
library(dplyr)
library(ggplot2)
library(logger)
library(DBI)
library(duckdb)
library(tidyr)

# Set up database connection
db_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"
connection <- dbConnect(duckdb::duckdb(), db_path)

# Auto-generate table mapping
table_year_mapping <- create_nppes_table_mapping(connection)

# Define taxonomy codes (including all possible OBGYN-related codes)
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology
  "207VF0040X",    # Female Pelvic Medicine
  "207VG0400X",    # Gynecology
  "207VM0101X"     # Maternal & Fetal Medicine
)

# Retrieve all physicians with these taxonomy codes
obgyn_physicians_all_years <- find_providers_across_years(
  connection = connection,
  table_year_mapping = table_year_mapping,
  taxonomy_codes = obgyn_taxonomy_codes,
  verbose = TRUE
) %>% 






######
# obgyn_physicians_all_years <- find_physicians_across_years(
#   con = con,
#   table_year_mapping = table_year_mapping,
#   taxonomy_codes = obgyn_taxonomy_codes,
#   max_results_per_year = 6000000L,
#   years_to_include = NULL,  # Include all years
#   verbose = TRUE
# ) 

#cleaned_all_providers <- all_providers %>%
  dplyr::mutate(NPI = as.character(NPI)) %>%
  # Remove duplicate entries for the same NPI in the same year
  dplyr::distinct(NPI, Year, .keep_all = TRUE) %>%
  # Filter for US-based providers only
  dplyr::filter(
    `Provider Business Mailing Address Country Code (If outside U.S.)` %in% c("US") &
      `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US"
  ) %>%
  # Remove unnecessary country code columns
  dplyr::select(
    -`Provider Business Mailing Address Country Code (If outside U.S.)`,
    -`Provider Business Practice Location Address Country Code (If outside U.S.)`
  ) 

View(obgyn_physicians_all_years)


# THIS CODE REALLY FUCKED THINGS UP BY REMOVING PEOPLE LIKE TYLER MUFFLY ALL THE TIME FOR NO REASONS.  
#%>%
  # Clean up credential text by removing special characters
  # dplyr::mutate(`Provider Credential Text` = stringr::str_remove_all(
  #   `Provider Credential Text`, "[[\\p{P}][\\p{S}]]")
  # ) %>%
  # # Keep only MD and DO providers
  # dplyr::filter(
  #   stringr::str_detect(`Provider Credential Text`, regex("MD|DO", ignore_case = TRUE))
  # ) %>%
  # # Standardize credential format to either "MD" or "DO"
  # dplyr::mutate(`Provider Credential Text` = if_else(
  #   stringr::str_detect(`Provider Credential Text`, regex("MD", ignore_case = TRUE)),
  #   "MD",
  #   if_else(
  #     stringr::str_detect(`Provider Credential Text`, regex("DO", ignore_case = TRUE)),
  #     "DO",
  #     NA_character_
  #   )
  # )) #%>%
  # # Extract just the year from date fields
  # dplyr::mutate(across(
  #   c(
  #     `NPI Deactivation Date`,
  #     `Last Update Date`,
  #     `Provider Enumeration Date`
  #   ),
  #   lubridate::year
  # ))

# Check dimensions of the resulting dataset
dim(obgyn_physicians_all_years)
View(obgyn_physicians_all_years)

# Display a sample of the data
print(head(obgyn_physicians_all_years, 10))
glimpse(obgyn_physicians_all_years)
hist(obgyn_physicians_all_years$Year)

#------------------------------------------------------------------------------
# 6. Final Data Processing
#------------------------------------------------------------------------------
# Convert state abbreviations to full state names
obgyn_physicians_all_years$`Provider Business Practice Location Address State Name` <- 
  tyler::phase0_convert_state_abbreviations(
    obgyn_physicians_all_years$`Provider Business Practice Location Address State Name`
  )

obgyn_physicians_all_years$`Provider Business Mailing Address State Name` <- 
  tyler::phase0_convert_state_abbreviations(
    obgyn_physicians_all_years$`Provider Business Mailing Address State Name`
  )

# Ensure Year is stored as character for consistency
obgyn_physicians_all_years$Year <- as.character(obgyn_physicians_all_years$Year)

#------------------------------------------------------------------------------
# 7. Save Results and Cleanup
#------------------------------------------------------------------------------
# Save the processed data to an RDS file
saveRDS(obgyn_physicians_all_years, "data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.rds"); invisible(gc())

# Close the database connection
dbDisconnect(con)

# End of script





###############################################################################
# Reference tables in DuckDB
###############################################################################

# NUCC TAXONOMY CODES
#Read in reference files to DuckDB
# First check if DuckDB can see the file
file_exists <- file.exists("/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/nucc/nucc_taxonomy_201.csv")
print(paste("File exists:", file_exists))

# Import the CSV file into DuckDB
if(file_exists) {
  # Read the CSV into R first
  nucc_data <- read.csv("/Users/tylermuffly/Dropbox (Personal)/workforce/Master_References/nucc/nucc_taxonomy_201.csv", 
                        stringsAsFactors = FALSE, 
                        na.strings = c("", "NA", "<NA>"))
  
  # Write to DuckDB
  dbWriteTable(con, "nucc_taxonomy", nucc_data, overwrite = TRUE)
}

# Verify the table was created
tables <- dbListTables(con)
print(tables)

# Check the table structure
if("nucc_taxonomy" %in% tables) {
  nucc_taxonomy_table <- tbl(con, "nucc_taxonomy")
  print(head(nucc_taxonomy_table))
}

# Rename the table
dbExecute(con, "ALTER TABLE nucc_taxonomy RENAME TO nucc_taxonomy_table;")

# Verify the table was renamed
tables <- dbListTables(con)
print(tables)




# PHYSICIAN COMPARE
#### Physician compare read into the database
# Load CSV file with error handling

physician_data <- readr::read_csv(
  "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/DAC_NationalDownloadableFile.csv",
  col_names = TRUE,
  na = c(''),
  locale = readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver"),
  trim_ws = TRUE,
  progress = FALSE,
  show_col_types = FALSE,
  lazy = FALSE
)

# Clean and transform the data in R
clean_physician_data <- physician_data %>%
  select(-Ind_PAC_ID, -Ind_enrl_ID, -sec_spec_3, -sec_spec_4, -org_pac_id, -adr_ln_2, -ln_2_sprs) %>%
  filter(!Cred %in% c("AA", "AU", "CNA", "CNM", "CNS", "CP", "CSW", "DC", "DDM", "DDS", "DPM", "MNT", "NP", "OD", "OT", "PA", "PSY", "PT", "SCW")) %>%
  #select(-lst_nm, -frst_nm, -mid_nm, -suff, -gndr, -pri_spec, -sec_spec_1, -sec_spec_2, -sec_spec_all, -org_nm, -adr_ln_1, -cty, -st, -zip, -phn_numbr, -ind_assgn, -grp_assgn, -adrs_id) %>%
  distinct(NPI, .keep_all = TRUE)

# Write to DuckDB
dbWriteTable(con, "physician_compare", clean_physician_data, overwrite = TRUE)

# Check the final table
if("physician_compare" %in% dbListTables(con)) {
  physician_compare <- tbl(con, "physician_compare")
  print(head(physician_compare))
  print(paste("Row count:", physician_compare %>% count() %>% pull()))
}

# Extract data for gynecologists and oncologists
physician_compare_data <- clean_physician_data %>%
  distinct(NPI, .keep_all = TRUE) %>%
  #mutate(`Zip Code` = str_sub(`Zip Code`, 1, 5)) %>%
  filter(pri_spec %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY")) %>% 
  mutate(year = "2023") %>%
  collect()  # This executes the query and brings results into R

# Save extracted data to CSV
write_csv(physician_compare_data, "data/C_physician_compare_data.csv")


#############
# Look at a table
#############
# Connect to database
con <- dbConnect(duckdb::duckdb(), "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb")

# View tables
all_tables <- dbListTables(con)
print(all_tables)

# Get table reference
table_name <- "_Volumes_Video_Projects_Muffly_1_nppes_historical_downloads_unzipped_p_files__NPPES_Data_Disseminat_April_2022_npidata_pfile_20050523_20220410"
nppes_table <- tbl(con, table_name)

# See first few rows
nppes_table %>% 
  head(5) %>%
  collect() %>%
  glimpse()

# Look for OBGYN records
nppes_table %>%
  filter(`Healthcare Provider Taxonomy Code_1` %in% c("207V00000X", "207VX0201X")) %>%
  head(10) %>%
  collect() %>%
  select(NPI, `Healthcare Provider Taxonomy Code_1`, starts_with("Provider")) %>%
  glimpse()



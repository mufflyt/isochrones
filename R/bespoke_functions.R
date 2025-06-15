
# Define database path via environment variable
db_path <- Sys.getenv("NPPES_DB_PATH")
if (!nzchar(db_path)) {
  stop("NPPES_DB_PATH environment variable not set")
}

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_path)


# Make table to year mapping automatic because the names of the files are so fucking long
create_nppes_table_mapping <- function(con) {
  # Get all tables in the database
  all_tables <- DBI::dbListTables(con)
  
  # Filter for NPPES tables (looking for patterns in table names)
  nppes_tables <- all_tables[
    grepl("npidata|NPPES_Data_Dissemination", all_tables, ignore.case = TRUE)
  ]
  
  # Initialize the mapping dataframe
  mapping_df <- data.frame(
    table_name = character(0),
    year = integer(0),
    stringsAsFactors = FALSE
  )
  
  # Process each table to extract the year
  for (table in nppes_tables) {
    # Try to extract year from the table name using different patterns
    
    # Method 1: Look for year patterns like 20XX in the table name
    year_matches <- regmatches(
      table, 
      gregexpr("20[0-9]{2}", table)
    )[[1]]
    
    if (length(year_matches) > 0) {
      # If multiple year patterns found (like start and end dates), use the second one
      # which is typically the publication year
      extracted_year <- NULL
      
      # Check if there's an April/Apr pattern followed by a year
      month_year_match <- regexpr("(?:April|Apr)\\D*20[0-9]{2}", table, perl = TRUE)
      if (month_year_match > 0) {
        month_year_str <- regmatches(table, regexpr("(?:April|Apr)\\D*20[0-9]{2}", table, perl = TRUE))
        year_in_month <- regmatches(month_year_str, regexpr("20[0-9]{2}", month_year_str))
        if (length(year_in_month) > 0) {
          extracted_year <- as.integer(year_in_month)
        }
      }
      
      # If we couldn't extract from month pattern, try to get year from the filename
      if (is.null(extracted_year)) {
        # Look for the pattern: digits_digits at the end of the filename
        date_match <- regexpr("[0-9]{8}$", table)
        if (date_match > 0) {
          date_str <- regmatches(table, date_match)
          # Extract year from YYYYMMDD format
          extracted_year <- as.integer(substr(date_str, 1, 4))
        } else {
          # Otherwise use the last year pattern found in the string
          extracted_year <- as.integer(year_matches[length(year_matches)])
        }
      }
      
      if (!is.null(extracted_year)) {
        mapping_df <- rbind(mapping_df, data.frame(
          table_name = table,
          year = extracted_year,
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Sort by year
  mapping_df <- mapping_df[order(mapping_df$year), ]
  
  # Check if we have entries for each year from 2010 to current
  current_year <- as.integer(format(Sys.Date(), "%Y"))
  expected_years <- 2010:current_year
  found_years <- unique(mapping_df$year)
  missing_years <- setdiff(expected_years, found_years)
  
  if (length(missing_years) > 0) {
    warning(sprintf(
      "Could not find tables for the following years: %s", 
      paste(missing_years, collapse = ", ")
    ))
  }
  
  # Remove duplicate years (keep the most recent table for each year)
  mapping_df <- mapping_df[!duplicated(mapping_df$year, fromLast = TRUE), ]
  
  return(mapping_df)
}

find_physicians_across_years <- function(con,
                                         table_year_mapping,
                                         taxonomy_codes,
                                         years_to_include = NULL,
                                         verbose = TRUE) {
  logger::log_threshold(if (verbose) logger::INFO else logger::WARN)
  logger::log_info("Starting physician search across years")
  
  validate_inputs(con, table_year_mapping, taxonomy_codes, years_to_include)
  
  filtered_mapping <- filter_year_mapping(table_year_mapping, years_to_include)
  
  if (nrow(filtered_mapping) == 0) {
    logger::log_warn("No matching years found in the mapping")
    return(create_empty_physician_tibble())
  }
  
  physician_listings <- query_all_years(con, filtered_mapping, taxonomy_codes, verbose)
  
  combined_physicians <- combine_physician_results_with_consistent_types(physician_listings)
  
  return(combined_physicians)
}

validate_inputs <- function(con, table_year_mapping, taxonomy_codes, years_to_include) {
  assertthat::assert_that(DBI::dbIsValid(con), msg = "Database connection is not valid")
  assertthat::assert_that(is.data.frame(table_year_mapping),
                          msg = "table_year_mapping must be a data frame")
  assertthat::assert_that(all(c("table_name", "year") %in% colnames(table_year_mapping)),
                          msg = "table_year_mapping must contain 'table_name' and 'year' columns")
  assertthat::assert_that(is.character(taxonomy_codes), 
                          msg = "Taxonomy codes must be a character vector")
  assertthat::assert_that(length(taxonomy_codes) > 0, 
                          msg = "At least one taxonomy code must be provided")
  if (!is.null(years_to_include)) {
    assertthat::assert_that(is.numeric(years_to_include),
                            msg = "years_to_include must be a numeric vector")
  }
  logger::log_info("Input validation completed successfully")
}

query_all_years <- function(con, filtered_mapping, taxonomy_codes, verbose) {
  logger::log_info("Querying %d tables/years: %s", 
                   nrow(filtered_mapping),
                   paste(filtered_mapping$year, collapse = ", "))
  
  all_physicians <- list()
  
  for (i in 1:nrow(filtered_mapping)) {
    current_table <- filtered_mapping$table_name[i]
    current_year <- filtered_mapping$year[i]
    
    logger::log_info("Querying table for year %d: %s", current_year, current_table)
    
    if (!current_table %in% DBI::dbListTables(con)) {
      logger::log_warn("Table '%s' not found in database, skipping", current_table)
      next
    }
    
    physicians_data <- try_query_methods(con, current_table, current_year, taxonomy_codes)
    
    if (!is.null(physicians_data) && nrow(physicians_data) > 0) {
      physicians_data <- standardize_column_types(physicians_data)
      all_physicians[[i]] <- physicians_data
      logger::log_info("Added %d physicians for year %d", nrow(physicians_data), current_year)
    } else {
      logger::log_info("No physicians found for year %d", current_year)
    }
  }
  
  return(all_physicians)
}

try_query_methods <- function(con, current_table, current_year, taxonomy_codes) {
  physicians_data <- try_dplyr_approach(con, current_table, current_year, taxonomy_codes)
  
  if (is.null(physicians_data)) {
    physicians_data <- try_sql_approach(con, current_table, current_year, taxonomy_codes)
  }
  
  return(physicians_data)
}

try_dplyr_approach <- function(con, current_table, current_year, taxonomy_codes) {
  physicians_data <- NULL
  tryCatch({
    logger::log_info("Attempting dplyr-based approach")
    nppes_table <- dplyr::tbl(con, current_table)
    
    taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", colnames(nppes_table), value = TRUE)
    
    if (length(taxonomy_columns) > 0) {
      filter_expr <- purrr::reduce(taxonomy_columns, ~rlang::expr(!!.x | .data[[.y]] %in% !!taxonomy_codes), 
                                   .init = rlang::expr(.data[[taxonomy_columns[1]]] %in% !!taxonomy_codes))
      
      query <- nppes_table %>%
        dplyr::filter(!!filter_expr) %>%
        dplyr::filter(`Entity Type Code` == 1L) %>%
        dplyr::mutate(Year = current_year)
      
      physicians_data <- dplyr::collect(query)
      
      if ("Provider Business Practice Location Address Postal Code" %in% colnames(physicians_data)) {
        physicians_data <- physicians_data %>%
          dplyr::mutate(Zip = stringr::str_sub(`Provider Business Practice Location Address Postal Code`, 1, 5))
      }
      
      logger::log_info("Dplyr approach retrieved %d physicians", nrow(physicians_data))
    }
  }, error = function(e) {
    logger::log_error("Dplyr approach failed: %s", e$message)
  })
  
  return(physicians_data)
}

try_sql_approach <- function(con, current_table, current_year, taxonomy_codes) {
  physicians_data <- NULL
  tryCatch({
    logger::log_info("Attempting SQL approach")
    taxonomy_columns <- DBI::dbListFields(con, current_table) %>%
      grep("Healthcare Provider Taxonomy Code_[0-9]+", ., value = TRUE)
    
    if (length(taxonomy_columns) > 0) {
      taxonomy_filter <- paste(sapply(taxonomy_columns, function(col) {
        sprintf("\"%s\" IN ('%s')", col, paste(taxonomy_codes, collapse = "','"))
      }), collapse = " OR ")
      
      query <- sprintf('
        SELECT *, %d AS Year FROM "%s"
        WHERE (%s) AND "Entity Type Code" = 1', 
                       current_year, current_table, taxonomy_filter)
      
      physicians_data <- DBI::dbGetQuery(con, query)
      
      if ("Provider Business Practice Location Address Postal Code" %in% colnames(physicians_data)) {
        physicians_data$Zip <- stringr::str_sub(physicians_data$`Provider Business Practice Location Address Postal Code`, 1, 5)
      }
      
      logger::log_info("SQL approach retrieved %d physicians", nrow(physicians_data))
    }
  }, error = function(e) {
    logger::log_error("SQL approach failed: %s", e$message)
  })
  
  return(physicians_data)
}


#' #' Find Medical Specialty Practitioners Across Multiple Years
#' #'
#' #' This function queries NPPES data across multiple years to find practitioners 
#' #' with specified taxonomy codes, filtering specifically for physicians.
#' #' It ensures consistent data types across all years of data.
#' #' 
#' #' @param con A DBI connection object to a DuckDB database containing NPPES data
#' #' @param table_year_mapping Data frame. Mapping between table names and years.
#' #'        Must contain columns 'table_name' and 'year'.
#' #' @param taxonomy_codes Character vector. The taxonomy codes to search for.
#' #' @param years_to_include Numeric vector. Optional. Years to include from the mapping.
#' #'        If NULL, all years in the mapping will be included.
#' #' @param max_results_per_year Numeric. Maximum number of results per year. Default 1000.
#' #' @param verbose Logical. Whether to print additional logging information. Default TRUE.
#' #'
#' #' @return A tibble containing physicians matching the specified taxonomy codes
#' #'         across all specified years, with a 'Year' column.
#' #'
#' #' @examples
#' #' # Example 1: Query all years with default settings
#' #' \dontrun{
#' #' # Create a DuckDB connection
#' #' nppes_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "nppes_data.duckdb")
#' #' 
#' #' # Create a table-year mapping
#' #' year_map <- tibble::tibble(
#' #'   table_name = c("nppes_2019", "nppes_2020", "nppes_2021"),
#' #'   year = c(2019, 2020, 2021)
#' #' )
#' #' 
#' #' # Find cardiology practitioners across all available years
#' #' cardio_practitioners <- find_physicians_across_years(
#' #'   con = nppes_con,
#' #'   table_year_mapping = year_map,
#' #'   taxonomy_codes = c("207RC0000X", "207RI0011X"),
#' #'   verbose = TRUE
#' #' )
#' #' 
#' #' # Results will contain physician data filtered by taxonomy codes with Year column
#' #' print(cardio_practitioners)
#' #' 
#' #' # Close the connection
#' #' DBI::dbDisconnect(nppes_con)
#' #' }
#' #'
#' #' # Example 2: Query specific years with limit per year
#' #' \dontrun{
#' #' # Create a DuckDB connection
#' #' nppes_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "nppes_data.duckdb")
#' #' 
#' #' # Create a table-year mapping
#' #' year_map <- tibble::tibble(
#' #'   table_name = c("nppes_2019", "nppes_2020", "nppes_2021", "nppes_2022"),
#' #'   year = c(2019, 2020, 2021, 2022)
#' #' )
#' #' 
#' #' # Find neurology practitioners for specific years with lower limit
#' #' neuro_practitioners <- find_physicians_across_years(
#' #'   con = nppes_con,
#' #'   table_year_mapping = year_map,
#' #'   taxonomy_codes = c("2084N0400X", "2084P0804X"),
#' #'   years_to_include = c(2020, 2022),
#' #'   max_results_per_year = 500,
#' #'   verbose = FALSE
#' #' )
#' #' 
#' #' # Results will contain physicians from 2020 and 2022 only
#' #' print(neuro_practitioners)
#' #' 
#' #' # Close the connection
#' #' DBI::dbDisconnect(nppes_con)
#' #' }
#' #'
#' #' # Example 3: Empty result handling
#' #' \dontrun{
#' #' # Create a DuckDB connection
#' #' nppes_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "nppes_data.duckdb")
#' #' 
#' #' # Create a table-year mapping
#' #' year_map <- tibble::tibble(
#' #'   table_name = c("nppes_2020", "nppes_2021", "nppes_2022"),
#' #'   year = c(2020, 2021, 2022)
#' #' )
#' #' 
#' #' # Search for non-existent taxonomy code
#' #' empty_result <- find_physicians_across_years(
#' #'   con = nppes_con,
#' #'   table_year_mapping = year_map,
#' #'   taxonomy_codes = c("NONEXISTENT"),
#' #'   years_to_include = 2022,
#' #'   max_results_per_year = 100,
#' #'   verbose = TRUE
#' #' )
#' #' 
#' #' # Will return an empty tibble with appropriate structure
#' #' print(nrow(empty_result))
#' #' print(colnames(empty_result))
#' #' 
#' #' # Close the connection
#' #' DBI::dbDisconnect(nppes_con)
#' #' }
#' #'
#' #' @importFrom dplyr bind_rows filter select rename mutate collect left_join
#' #' @importFrom logger log_info log_debug log_error log_threshold
#' #' @importFrom assertthat assert_that
#' #' @importFrom stringr str_sub
#' #' @importFrom tibble tibble
#' #' @importFrom purrr map reduce map_df
#' find_physicians_across_years <- function(con,
#'                                          table_year_mapping,
#'                                          taxonomy_codes,
#'                                          years_to_include = NULL,
#'                                          #max_results_per_year = 1000000,
#'                                          verbose = TRUE) {
#'   # Initialize logger
#'   logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
#'   logger::log_info("Starting physician search across years")
#'   
#'   # Validate inputs
#'   validate_inputs(con, table_year_mapping, taxonomy_codes, years_to_include, 
#'                   max_results_per_year)
#'   
#'   # Filter to specified years if provided
#'   filtered_mapping <- filter_year_mapping(table_year_mapping, years_to_include)
#'   
#'   if (nrow(filtered_mapping) == 0) {
#'     logger::log_warn("No matching years found in the mapping")
#'     return(create_empty_physician_tibble())
#'   }
#'   
#'   # Query each year/table and collect results
#'   physician_listings <- query_all_years(con, filtered_mapping, taxonomy_codes, 
#'                                         max_results_per_year, verbose)
#'   
#'   # Combine all results with consistent data types
#'   combined_physicians <- combine_physician_results_with_consistent_types(physician_listings)
#'   
#'   # Return the combined results
#'   return(combined_physicians)
#' }
#' 
#' #' @noRd
#' validate_inputs <- function(con, table_year_mapping, taxonomy_codes, 
#'                             years_to_include #max_results_per_year
#'                             ) {
#'   # Database connection validation
#'   assertthat::assert_that(DBI::dbIsValid(con), 
#'                           msg = "Database connection is not valid")
#'   
#'   # Table mapping validation
#'   assertthat::assert_that(is.data.frame(table_year_mapping),
#'                           msg = "table_year_mapping must be a data frame")
#'   assertthat::assert_that(all(c("table_name", "year") %in% colnames(table_year_mapping)),
#'                           msg = "table_year_mapping must contain 'table_name' and 'year' columns")
#'   
#'   # Taxonomy codes validation
#'   assertthat::assert_that(is.character(taxonomy_codes), 
#'                           msg = "Taxonomy codes must be a character vector")
#'   assertthat::assert_that(length(taxonomy_codes) > 0, 
#'                           msg = "At least one taxonomy code must be provided")
#'   
#'   # Years validation (if provided)
#'   if (!is.null(years_to_include)) {
#'     assertthat::assert_that(is.numeric(years_to_include),
#'                             msg = "years_to_include must be a numeric vector")
#'     assertthat::assert_that(length(years_to_include) > 0,
#'                             msg = "At least one year must be provided if years_to_include is not NULL")
#'   }
#'   
#' #   # Max results validation
#' #   assertthat::assert_that(is.numeric(max_results_per_year),
#' #                           msg = "max_results_per_year must be numeric")
#' #   assertthat::assert_that(max_results_per_year > 0,
#' #                           msg = "max_results_per_year must be greater than 0")
#' #   
#' #   logger::log_info("Input validation completed successfully")
#' # }
#' 
#' #' @noRd
#' filter_year_mapping <- function(table_year_mapping, years_to_include) {
#'   # Filter to years_to_include if provided
#'   filtered_mapping <- table_year_mapping
#'   if (!is.null(years_to_include)) {
#'     filtered_mapping <- table_year_mapping[table_year_mapping$year %in% years_to_include, ]
#'     logger::log_info("Filtered to %d years from the mapping", nrow(filtered_mapping))
#'   }
#'   return(filtered_mapping)
#' }
#' 
#' #' @noRd
#' create_empty_physician_tibble <- function() {
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
#' #' @noRd
#' query_all_years <- function(con, filtered_mapping, taxonomy_codes, 
#'                             #max_results_per_year, 
#'                             verbose) {
#'   # Log the tables that will be queried
#'   logger::log_info("Querying %d tables/years: %s", 
#'                    nrow(filtered_mapping),
#'                    paste(filtered_mapping$year, collapse = ", "))
#'   
#'   # Initialize list to store results from each year
#'   all_physicians <- list()
#'   
#'   # Query each table/year
#'   for (i in 1:nrow(filtered_mapping)) {
#'     current_table <- filtered_mapping$table_name[i]
#'     current_year <- filtered_mapping$year[i]
#'     
#'     logger::log_info("Querying table for year %d: %s", current_year, current_table)
#'     
#'     # Check if table exists
#'     if (!current_table %in% DBI::dbListTables(con)) {
#'       logger::log_warn("Table '%s' not found in database, skipping", current_table)
#'       next
#'     }
#'     
#'     # Try to query the table
#'     physicians_found <- FALSE
#'     physicians_data <- try_query_methods(con, current_table, current_year, 
#'                                          taxonomy_codes#, max_results_per_year
#'                                          )
#'     
#'     # Add to results list if any physicians were found
#'     if (!is.null(physicians_data) && nrow(physicians_data) > 0) {
#'       # Apply consistent types for each data frame as they're retrieved
#'       physicians_data <- standardize_column_types(physicians_data)
#'       all_physicians[[i]] <- physicians_data
#'       logger::log_info("Added %d physicians for year %d", 
#'                        nrow(physicians_data), current_year)
#'     } else {
#'       logger::log_info("No physicians found for year %d", current_year)
#'     }
#'   }
#'   
#'   return(all_physicians)
#' }
#' 
#' #' @noRd
#' try_query_methods <- function(con, current_table, current_year, taxonomy_codes#, 
#'                               #max_results_per_year
#'                               ) {
#'   # Try dplyr approach first
#'   physicians_data <- try_dplyr_approach(con, current_table, current_year, 
#'                                         taxonomy_codes, max_results_per_year)
#'   
#'   # If dplyr approach failed, try direct SQL
#'   if (is.null(physicians_data)) {
#'     physicians_data <- try_sql_approach(con, current_table, current_year, 
#'                                         taxonomy_codes, max_results_per_year)
#'   }
#'   
#'   return(physicians_data)
#' }
#' 
#' #' @noRd
#' try_dplyr_approach <- function(con, current_table, current_year, taxonomy_codes, 
#'                                max_results_per_year) {
#'   physicians_data <- NULL
#'   
#'   tryCatch({
#'     logger::log_info("Attempting dplyr-based approach")
#'     
#'     # Create reference to the table
#'     nppes_table <- dplyr::tbl(con, current_table)
#'     
#'     # Find taxonomy code columns
#'     col_names <- colnames(nppes_table)
#'     taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
#'                              col_names, value = TRUE)
#'     
#'     if (length(taxonomy_columns) > 0) {
#'       logger::log_info("Found %d taxonomy code columns", length(taxonomy_columns))
#'       
#'       # Determine if Entity Type Code column exists
#'       entity_type_col_exists <- "Entity Type Code" %in% col_names
#'       
#'       # Build the query with a composite filter for ALL taxonomy columns
#'       query <- nppes_table
#'       
#'       # Create a dynamic OR-based filter for all taxonomy columns
#'       # Initialize filter expression
#'       filter_expr <- NULL
#'       
#'       # Loop through all taxonomy columns and build a combined OR filter
#'       for (i in seq_along(taxonomy_columns)) {
#'         tax_col <- taxonomy_columns[i]
#'         
#'         # For the first column, start the filter
#'         if (i == 1) {
#'           filter_expr <- rlang::expr(.data[[tax_col]] %in% !!taxonomy_codes)
#'         } else {
#'           # For subsequent columns, add with OR operator
#'           filter_expr <- rlang::expr(!!filter_expr | .data[[tax_col]] %in% !!taxonomy_codes)
#'         }
#'       }
#'       
#'       # Apply the combined filter
#'       query <- dplyr::filter(query, !!filter_expr)
#'       logger::log_info("Applied combined filter across all %d taxonomy columns", 
#'                        length(taxonomy_columns))
#'       
#'       # Add entity type filter if column exists
#'       if (entity_type_col_exists) {
#'         query <- query %>% dplyr::filter(`Entity Type Code` == 1L)
#'         logger::log_info("Applied Entity Type Code filter for physicians (1)")
#'       } else {
#'         logger::log_warn("Entity Type Code column not found, skipping filter")
#'       }
#'       
#'       # Prepare list of all columns to select
#'       cols_to_select <- c(
#'         "NPI", 
#'         "Provider Last Name (Legal Name)", 
#'         "Provider First Name",
#'         "Provider Middle Name", 
#'         "Provider Gender Code",
#'         taxonomy_columns[1],  # Use first taxonomy column for consistency in output
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
#'       # Also include all other taxonomy columns
#'       if (length(taxonomy_columns) > 1) {
#'         cols_to_select <- c(cols_to_select, taxonomy_columns[-1])
#'         logger::log_info("Including all %d taxonomy columns in output", length(taxonomy_columns))
#'       }
#'       
#'       # Ensure all columns exist
#'       available_cols <- cols_to_select[cols_to_select %in% col_names]
#'       logger::log_info("Found %d of %d requested columns", 
#'                        length(available_cols), length(cols_to_select))
#'       
#'       # Select available columns
#'       query <- query %>% dplyr::select(dplyr::all_of(available_cols))
#'       
#'       # Define column renames
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
#'       # Prepare rename command (only for columns that exist)
#'       rename_cols <- rename_map[names(rename_map) %in% col_names]
#'       
#'       # Execute the query
#'       physicians_data <- query %>%
#'         dplyr::rename(!!!rename_cols) %>%
#'         dplyr::mutate(Year = as.integer(current_year)) %>%
#'         dplyr::collect(n = max_results_per_year)
#'       
#'       # Clean zip code
#'       if ("Zip" %in% colnames(physicians_data)) {
#'         physicians_data <- physicians_data %>%
#'           dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
#'         logger::log_info("Cleaned Zip codes to 5 digits")
#'       }
#'       
#'       # Check if we found results
#'       if (nrow(physicians_data) > 0) {
#'         logger::log_info("Dplyr approach succeeded, found %d physicians", 
#'                          nrow(physicians_data))
#'       } else {
#'         logger::log_info("No results from dplyr approach")
#'         physicians_data <- NULL
#'       }
#'     }
#'   }, error = function(e) {
#'     logger::log_error("Dplyr approach failed: %s", e$message)
#'     physicians_data <- NULL
#'   })
#'   
#'   return(physicians_data)
#' }
#' 
#' #' @noRd
#' try_sql_approach <- function(con, current_table, current_year, taxonomy_codes, 
#'                              max_results_per_year) {
#'   physicians_data <- NULL
#'   
#'   tryCatch({
#'     logger::log_info("Attempting direct SQL approach")
#'     
#'     # First find taxonomy columns for the query
#'     sample_data <- DBI::dbGetQuery(con, sprintf("SELECT * FROM \"%s\" LIMIT 1", current_table))
#'     taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
#'                              colnames(sample_data), value = TRUE)
#'     
#'     logger::log_info("Found %d taxonomy columns in database", length(taxonomy_columns))
#'     
#'     if (length(taxonomy_columns) > 0) {
#'       # Build taxonomy code filter strings for ALL available taxonomy columns
#'       taxonomy_filter_parts <- c()
#'       
#'       # Create a filter for each taxonomy column
#'       for (tax_col in taxonomy_columns) {
#'         taxonomy_filter_parts <- c(
#'           taxonomy_filter_parts,
#'           sprintf("\"%s\" IN ('%s')", 
#'                   tax_col, 
#'                   paste(taxonomy_codes, collapse = "','"))
#'         )
#'       }
#'       
#'       # Combine with OR
#'       taxonomy_filter <- paste(taxonomy_filter_parts, collapse = " OR ")
#'       
#'       # Add entity type filter if column exists
#'       entity_filter <- ""
#'       if ("Entity Type Code" %in% colnames(sample_data)) {
#'         entity_filter <- "AND \"Entity Type Code\" = 1"
#'         logger::log_info("Added Entity Type Code filter for physicians (1)")
#'       } else {
#'         logger::log_warn("Entity Type Code column not found in SQL approach, skipping filter")
#'       }
#'       
#'       # Define the columns to select
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
#'       # Add other taxonomy columns
#'       if (length(taxonomy_columns) > 1) {
#'         for (i in 2:length(taxonomy_columns)) {
#'           additional_columns <- c(
#'             additional_columns,
#'             sprintf("\"%s\"", taxonomy_columns[i])
#'           )
#'         }
#'       }
#'       
#'       # Filter additional columns to those that exist
#'       available_additional_cols <- c()
#'       for (col in additional_columns) {
#'         # Extract column name from SQL expression (remove quotes and AS part)
#'         col_name <- gsub("\"(.*)\".*", "\\1", col)
#'         if (col_name %in% colnames(sample_data)) {
#'           available_additional_cols <- c(available_additional_cols, col)
#'         }
#'       }
#'       
#'       logger::log_info("Found %d of %d additional columns", 
#'                        length(available_additional_cols), length(additional_columns))
#'       
#'       # Combine all columns
#'       all_columns <- c(core_columns, available_additional_cols)
#'       column_sql <- paste(all_columns, collapse = ", ")
#'       
#'       # Build the query
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
#'       # Execute query
#'       logger::log_info("Executing SQL query")
#'       physicians_data <- DBI::dbGetQuery(con, query)
#'       
#'       # Clean zip code
#'       if ("Zip" %in% colnames(physicians_data)) {
#'         physicians_data <- physicians_data %>%
#'           dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
#'         logger::log_info("Cleaned Zip codes to 5 digits")
#'       }
#'       
#'       if (nrow(physicians_data) > 0) {
#'         logger::log_info("SQL approach succeeded, found %d physicians", 
#'                          nrow(physicians_data))
#'       } else {
#'         logger::log_info("No results from SQL approach")
#'         physicians_data <- NULL
#'       }
#'     }
#'   }, error = function(e) {
#'     logger::log_error("SQL approach failed: %s", e$message)
#'     physicians_data <- NULL
#'   })
#'   
#'   return(physicians_data)
#' }
#' 
#' #' Standardize column types for a data frame
#' #'
#' #' @param df Data frame to standardize
#' #'
#' #' @noRd
#' standardize_column_types <- function(df) {
#'   if (is.null(df) || nrow(df) == 0) {
#'     return(df)
#'   }
#'   
#'   logger::log_info("Standardizing column types for a result set with %d rows", nrow(df))
#'   
#'   # Columns to ensure are character
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
#'   # Add additional taxonomy columns if they exist
#'   taxonomy_cols <- grep("Healthcare Provider Taxonomy Code_[0-9]+", names(df), value = TRUE)
#'   if (length(taxonomy_cols) > 0) {
#'     char_columns <- c(char_columns, taxonomy_cols)
#'   }
#'   
#'   # Columns to ensure are integer
#'   int_columns <- c("Year")
#'   
#'   # Standardize character columns
#'   for (col in char_columns) {
#'     if (col %in% names(df)) {
#'       # Convert various types to character
#'       if (is.factor(df[[col]])) {
#'         logger::log_debug("Converting factor column %s to character", col)
#'         df[[col]] <- as.character(df[[col]])
#'       } else if (is.numeric(df[[col]])) {
#'         logger::log_debug("Converting numeric column %s to character", col)
#'         df[[col]] <- as.character(df[[col]])
#'       } else if (inherits(df[[col]], "Date") || inherits(df[[col]], "POSIXt")) {
#'         logger::log_debug("Converting date/time column %s to character", col)
#'         df[[col]] <- as.character(df[[col]])
#'       } else if (!is.character(df[[col]])) {
#'         logger::log_debug("Converting column %s from %s to character", 
#'                           col, class(df[[col]])[1])
#'         df[[col]] <- as.character(df[[col]])
#'       }
#'     }
#'   }
#'   
#'   # Standardize integer columns
#'   for (col in int_columns) {
#'     if (col %in% names(df)) {
#'       # Convert various types to integer
#'       if (is.factor(df[[col]]) || is.character(df[[col]])) {
#'         logger::log_debug("Converting factor/character column %s to integer", col)
#'         df[[col]] <- as.integer(as.character(df[[col]]))
#'       } else if (is.numeric(df[[col]]) && !is.integer(df[[col]])) {
#'         logger::log_debug("Converting numeric column %s to integer", col)
#'         df[[col]] <- as.integer(df[[col]])
#'       } else if (!is.integer(df[[col]])) {
#'         logger::log_debug("Converting column %s from %s to integer", 
#'                           col, class(df[[col]])[1])
#'         df[[col]] <- as.integer(as.character(df[[col]]))
#'       }
#'     }
#'   }
#'   
#'   # Handle missing columns by adding them with appropriate default values
#'   expected_columns <- c(char_columns, int_columns)
#'   missing_columns <- setdiff(expected_columns, names(df))
#'   
#'   if (length(missing_columns) > 0) {
#'     logger::log_debug("Adding %d missing columns with appropriate default values", 
#'                       length(missing_columns))
#'     
#'     for (col in missing_columns) {
#'       # Add with appropriate type
#'       if (col %in% char_columns) {
#'         df[[col]] <- NA_character_
#'       } else if (col %in% int_columns) {
#'         df[[col]] <- NA_integer_
#'       }
#'     }
#'   }
#'   
#'   return(df)
#' }
#' 
#' #' Combine physician results with consistent data types
#' #'
#' #' @param physician_listings List of data frames with physician data
#' #'
#' #' @noRd
#' combine_physician_results_with_consistent_types <- function(physician_listings) {
#'   # Filter out NULL or non-data frame entries
#'   valid_listings <- Filter(function(x) {
#'     return(!is.null(x) && (is.data.frame(x) || is.list(x) && length(x) > 0))
#'   }, physician_listings)
#'   
#'   # If no valid data remains, return empty data frame
#'   if (length(valid_listings) == 0) {
#'     logger::log_warn("No valid physician data found after filtering")
#'     return(create_empty_physician_tibble())
#'   }
#'   
#'   # Log how many valid entries remain
#'   logger::log_info("Combining %d valid data sources out of %d total", 
#'                    length(valid_listings), length(physician_listings))
#'   
#'   # Get a list of all column names across all data frames
#'   all_columns <- unique(unlist(lapply(valid_listings, names)))
#'   logger::log_debug("Found a total of %d unique columns across all data sources", 
#'                     length(all_columns))
#'   
#'   # Standardize each data frame to have all columns with consistent types
#'   standardized_listings <- lapply(valid_listings, function(df) {
#'     # Add any missing columns
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
#'   # Combine all results
#'   tryCatch({
#'     # Use safer approach with reduce instead of bind_rows
#'     all_physicians <- purrr::reduce(standardized_listings, function(acc, x) {
#'       # Ensure column order matches
#'       x <- x[, names(acc)]
#'       return(rbind(acc, x))
#'     })
#'     
#'     logger::log_info("Found a total of %d physicians across all years", 
#'                      nrow(all_physicians))
#'     
#'     # Convert to tibble for consistent output
#'     all_physicians <- tibble::as_tibble(all_physicians)
#'     
#'     return(all_physicians)
#'   }, error = function(e) {
#'     logger::log_error("Error combining physician data: %s", e$message)
#'     
#'     # Fallback to a more cautious approach
#'     logger::log_warn("Attempting fallback approach for combining data")
#'     
#'     tryCatch({
#'       # Try using a template for consistent structure
#'       template <- create_empty_physician_tibble()
#'       
#'       # Process each data frame to match the template
#'       processed_listings <- lapply(standardized_listings, function(df) {
#'         # Select only columns in template
#'         common_cols <- intersect(names(df), names(template))
#'         new_df <- df[, common_cols, drop = FALSE]
#'         
#'         # Add missing columns from template
#'         missing_cols <- setdiff(names(template), names(new_df))
#'         for (col in missing_cols) {
#'           if (col %in% c("Year")) {
#'             new_df[[col]] <- NA_integer_
#'           } else {
#'             new_df[[col]] <- NA_character_
#'           }
#'         }
#'         
#'         # Ensure correct column order
#'         new_df <- new_df[, names(template)]
#'         return(new_df)
#'       })
#'       
#'       # Bind the processed data frames
#'       all_physicians <- do.call(rbind, processed_listings)
#'       all_physicians <- tibble::as_tibble(all_physicians)
#'       
#'       logger::log_info("Fallback approach succeeded with %d physicians", 
#'                        nrow(all_physicians))
#'       return(all_physicians)
#'     }, error = function(inner_e) {
#'       logger::log_error("Fallback approach also failed: %s", inner_e$message)
#'       
#'       # Last resort: return just the first data source
#'       if (length(valid_listings) > 0) {
#'         logger::log_warn("Returning only the first data source as final fallback")
#'         return(valid_listings[[1]])
#'       } else {
#'         return(create_empty_physician_tibble())
#'       }
#'     })
#'   })
#' }




#' Generate Physician Statistics by State, Taxonomy Code, and Year
#'
#' Create summary statistics from physician data, including counts by state,
#' taxonomy code, and year.
#'
#' @param physician_data A tibble of physician data from find_physicians_across_years
#' @param verbose Logical. Whether to print additional logging information.
#'        Default TRUE.
#'
#' @return A list containing tibbles with various counts: state_counts, 
#'         taxonomy_counts, year_counts, and state_by_year_counts
#'
#' @examples
#' # Example 1: Generate statistics for physician data
#' \dontrun{
#' # First get physician data
#' nppes_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "nppes_data.duckdb")
#' year_map <- tibble::tibble(
#'   table_name = c("nppes_2020", "nppes_2021", "nppes_2022"),
#'   year = c(2020, 2021, 2022)
#' )
#' physician_data <- find_physicians_across_years(
#'   con = nppes_con,
#'   table_year_mapping = year_map,
#'   taxonomy_codes = c("207RC0000X", "207RI0011X"),
#'   verbose = TRUE
#' )
#' 
#' # Generate statistics from the data
#' stats <- generate_physician_stats(
#'   physician_data = physician_data,
#'   verbose = TRUE
#' )
#' 
#' # Inspect the results
#' print(stats$state_counts)
#' print(stats$taxonomy_counts)
#' print(stats$year_counts)
#' print(stats$state_by_year_counts)
#' 
#' # Close the connection
#' DBI::dbDisconnect(nppes_con)
#' }
#'
#' # Example 2: Generate statistics with verbose off
#' \dontrun{
#' # Generate statistics from previously queried data with logging off
#' stats_quiet <- generate_physician_stats(
#'   physician_data = physician_data,
#'   verbose = FALSE
#' )
#' 
#' # Inspect specific statistics - top 10 states
#' top_states <- head(stats_quiet$state_counts, 10)
#' print(top_states)
#' }
#'
#' # Example 3: Handle empty data case
#' \dontrun{
#' # Create an empty physician dataset
#' empty_data <- tibble::tibble(
#'   NPI = character(0),
#'   State = character(0),
#'   TaxonomyCode1 = character(0),
#'   Year = integer(0)
#' )
#' 
#' # Generate statistics for empty dataset
#' empty_stats <- generate_physician_stats(
#'   physician_data = empty_data,
#'   verbose = TRUE
#' )
#' 
#' # Verify empty result structure
#' print(names(empty_stats))
#' print(nrow(empty_stats$state_counts))
#' }
#'
#' @importFrom dplyr group_by summarize n arrange desc
#' @importFrom assertthat assert_that
#' @importFrom tibble tibble
generate_physician_stats <- function(physician_data, verbose = TRUE) {
  # Initialize logger
  logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
  
  # Input validation
  assertthat::assert_that(is.data.frame(physician_data), 
                          msg = "Physician data must be a data frame")
  
  # Check if empty dataframe
  if (nrow(physician_data) == 0) {
    logger::log_info("No physician data to analyze")
    return(create_empty_stats())
  }
  
  # Log inputs
  logger::log_info("Generating statistics for %d physicians", 
                   nrow(physician_data))
  
  # Check for required columns
  validate_stats_columns(physician_data)
  
  # Create result list with individual statistics
  stats_result <- list()
  
  # Generate state counts
  stats_result$state_counts <- get_state_counts(physician_data)
  
  # Generate taxonomy code counts
  stats_result$taxonomy_counts <- get_taxonomy_counts(physician_data)
  
  # Generate year counts
  stats_result$year_counts <- get_year_counts(physician_data)
  
  # Generate state by year counts
  stats_result$state_by_year_counts <- get_state_by_year_counts(physician_data)
  
  # Return all summary statistics
  return(stats_result)
}

#' @noRd
create_empty_stats <- function() {
  return(list(
    state_counts = tibble::tibble(State = character(), Count = integer()),
    taxonomy_counts = tibble::tibble(TaxonomyCode1 = character(), Count = integer()),
    year_counts = tibble::tibble(Year = integer(), Count = integer()),
    state_by_year_counts = tibble::tibble(Year = integer(), State = character(), Count = integer())
  ))
}

#' @noRd
validate_stats_columns <- function(physician_data) {
  required_cols <- c("State", "TaxonomyCode1", "Year")
  missing_cols <- required_cols[!required_cols %in% colnames(physician_data)]
  
  if (length(missing_cols) > 0) {
    error_msg <- sprintf("Missing required columns: %s", 
                         paste(missing_cols, collapse = ", "))
    logger::log_error(error_msg)
    stop(error_msg)
  }
  
  logger::log_info("All required columns present for analysis")
}

#' @noRd
get_state_counts <- function(physician_data) {
  logger::log_debug("Generating state counts")
  state_counts <- physician_data %>%
    dplyr::group_by(State) %>%
    dplyr::summarize(Count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(Count))
  
  logger::log_info("Generated counts for %d states", nrow(state_counts))
  return(state_counts)
}

#' @noRd
get_taxonomy_counts <- function(physician_data) {
  logger::log_debug("Generating taxonomy code counts")
  taxonomy_counts <- physician_data %>%
    dplyr::group_by(TaxonomyCode1) %>%
    dplyr::summarize(Count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(Count))
  
  logger::log_info("Generated counts for %d taxonomy codes", nrow(taxonomy_counts))
  return(taxonomy_counts)
}

#' @noRd
get_year_counts <- function(physician_data) {
  logger::log_debug("Generating year counts")
  year_counts <- physician_data %>%
    dplyr::group_by(Year) %>%
    dplyr::summarize(Count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(Year)
  
  logger::log_info("Generated counts for %d years", nrow(year_counts))
  return(year_counts)
}

#' @noRd
get_state_by_year_counts <- function(physician_data) {
  logger::log_debug("Generating state by year counts")
  state_by_year_counts <- physician_data %>%
    dplyr::group_by(Year, State) %>%
    dplyr::summarize(Count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(Year, dplyr::desc(Count))
  
  logger::log_info("Generated counts for %d state-year combinations", 
                   nrow(state_by_year_counts))
  return(state_by_year_counts)
}


#' Import, Filter, and Process NPPES (National Provider Identifier) Data
#'
#' This function imports NPPES data from a CSV file into a DuckDB database, filters
#' the data for individual providers in the US, and optionally saves the filtered
#' data to an RDS file.
#'
#' @param csv_file_path Character string. Path to the NPPES CSV file to import.
#' @param db_path Character string. Path where the DuckDB database file should be 
#'        created or accessed.
#' @param raw_table_name Character string. Name for the raw imported table in DuckDB.
#'        Default is "nppes_raw".
#' @param filtered_table_name Character string. Name for the filtered table in DuckDB.
#'        Default is "npidata_filtered".
#' @param save_rds Logical. Whether to save the filtered data to an RDS file. 
#'        Default is FALSE.
#' @param rds_output_path Character string. Path where the RDS file should be saved.
#'        Only used if save_rds = TRUE. Default is NULL.
#' @param verbose Logical. Whether to print additional information during processing.
#'        Default is FALSE.
#'
#' @return A list containing connection to the database and the name of the created
#'         table.
#'
#' @examples
#' # Example 1: Import and filter data without saving to RDS
#' nppes_result <- process_nppes_data(
#'   csv_file_path = "path/to/npidata_pfile.csv",
#'   db_path = "path/to/nppes_database.duckdb",
#'   filtered_table_name = "my_nppes_data",
#'   save_rds = FALSE,
#'   verbose = TRUE
#' )
#' # Use the database connection
#' nppes_data <- dplyr::tbl(nppes_result$connection, nppes_result$table_name)
#' # Don't forget to close the connection when finished
#' DBI::dbDisconnect(nppes_result$connection)
#'
#' # Example 2: Import, filter, and save to RDS with custom table names
#' nppes_result <- process_nppes_data(
#'   csv_file_path = "path/to/npidata_pfile.csv",
#'   db_path = "path/to/nppes_database.duckdb",
#'   raw_table_name = "nppes_import_2024",
#'   filtered_table_name = "nppes_physicians_2024",
#'   save_rds = TRUE,
#'   rds_output_path = "path/to/nppes_data.rds",
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Using the function with minimal parameters
#' nppes_result <- process_nppes_data(
#'   csv_file_path = "path/to/npidata_pfile.csv",
#'   db_path = "path/to/nppes_database.duckdb"
#' )
#' # Access the data in the database
#' provider_count <- dplyr::tbl(nppes_result$connection, nppes_result$table_name) %>%
#'   dplyr::count() %>%
#'   dplyr::collect() %>%
#'   dplyr::pull(n)
#' # Close the connection
#' DBI::dbDisconnect(nppes_result$connection)
#'
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @importFrom duckdb duckdb
#' @importFrom dplyr tbl filter compute count collect pull
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_success log_error log_debug
#'
#' @export
process_nppes_data <- function(csv_file_path, 
                               db_path, 
                               raw_table_name = "nppes_raw",
                               filtered_table_name = "npidata_filtered",
                               save_rds = FALSE, 
                               rds_output_path = NULL,
                               verbose = FALSE) {
  
  # Set up logging
  logger::log_layout(logger::layout_simple)
  log_level <- if (verbose) logger::DEBUG else logger::INFO
  logger::log_threshold(log_level)
  
  # Validate inputs
  validate_nppes_inputs(csv_file_path, db_path, raw_table_name, 
                        filtered_table_name, save_rds, rds_output_path)
  
  # Log function parameters
  logger::log_info("Starting NPPES data processing with parameters:")
  logger::log_debug("CSV File Path: {csv_file_path}")
  logger::log_debug("DB Path: {db_path}")
  logger::log_debug("Raw Table Name: {raw_table_name}")
  logger::log_debug("Filtered Table Name: {filtered_table_name}")
  logger::log_debug("Save to RDS: {save_rds}")
  if (save_rds) {
    logger::log_debug("RDS Output Path: {rds_output_path}")
  }
  
  # Setup database connection
  nppes_db_connection <- setup_duckdb_connection(db_path)
  
  # Import data to DuckDB
  import_nppes_to_duckdb(nppes_db_connection, csv_file_path, raw_table_name)
  
  # Filter data
  filter_nppes_data(nppes_db_connection, raw_table_name, filtered_table_name)
  
  # Get row count
  provider_count <- count_nppes_providers(nppes_db_connection, filtered_table_name)
  logger::log_success("Imported and filtered {provider_count} providers to DuckDB table: {filtered_table_name}")
  
  # Save to RDS if requested
  if (save_rds) {
    save_nppes_to_rds(nppes_db_connection, filtered_table_name, rds_output_path)
  }
  
  # Return the connection and table name for further use
  return(list(
    connection = nppes_db_connection,
    table_name = filtered_table_name
  ))
}

#' Validate inputs for NPPES data processing
#'
#' @param csv_file_path CSV file path
#' @param db_path DuckDB database path
#' @param raw_table_name Raw table name
#' @param filtered_table_name Filtered table name
#' @param save_rds Whether to save to RDS
#' @param rds_output_path RDS output path
#'
#' @noRd
validate_nppes_inputs <- function(csv_file_path, db_path, raw_table_name,
                                  filtered_table_name, save_rds, rds_output_path) {
  assertthat::assert_that(is.character(csv_file_path), 
                          msg = "CSV file path must be a character string")
  assertthat::assert_that(file.exists(csv_file_path), 
                          msg = paste("CSV file not found at:", csv_file_path))
  assertthat::assert_that(is.character(db_path), 
                          msg = "Database path must be a character string")
  assertthat::assert_that(is.character(raw_table_name),
                          msg = "Raw table name must be a character string")
  assertthat::assert_that(is.character(filtered_table_name),
                          msg = "Filtered table name must be a character string")
  assertthat::assert_that(raw_table_name != filtered_table_name,
                          msg = "Raw and filtered table names must be different")
  assertthat::assert_that(is.logical(save_rds), 
                          msg = "save_rds must be logical (TRUE/FALSE)")
  
  # Validate table names for SQL compatibility
  validate_table_name(raw_table_name)
  validate_table_name(filtered_table_name)
  
  if (save_rds) {
    assertthat::assert_that(!is.null(rds_output_path), 
                            msg = "rds_output_path must be provided when save_rds = TRUE")
    assertthat::assert_that(is.character(rds_output_path), 
                            msg = "RDS output path must be a character string")
    
    # Verify directory exists
    rds_dir <- dirname(rds_output_path)
    assertthat::assert_that(dir.exists(rds_dir), 
                            msg = paste("Output directory does not exist:", rds_dir))
  }
}

#' Validate table name for SQL compatibility
#'
#' @param table_name Table name to validate
#'
#' @noRd
validate_table_name <- function(table_name) {
  # Check for valid SQL table name
  assertthat::assert_that(grepl("^[a-zA-Z0-9_]+$", table_name),
                          msg = paste("Invalid table name:", table_name, 
                                      "- Use only letters, numbers, and underscores"))
}

#' Set up DuckDB connection
#'
#' @param db_path Path to DuckDB database file
#'
#' @noRd
setup_duckdb_connection <- function(db_path) {
  logger::log_info("Connecting to DuckDB at {db_path}")
  tryCatch({
    connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
    logger::log_success("Successfully connected to DuckDB")
    return(connection)
  }, error = function(e) {
    logger::log_error("Failed to connect to DuckDB: {e$message}")
    stop("Failed to connect to DuckDB database", call. = FALSE)
  })
}

#' Import NPPES data to DuckDB
#'
#' @param connection DuckDB connection
#' @param csv_file_path Path to NPPES CSV file
#' @param raw_table_name Name for the raw data table
#'
#' @noRd
import_nppes_to_duckdb <- function(connection, csv_file_path, raw_table_name) {
  logger::log_info("Importing NPPES data from CSV to DuckDB table: {raw_table_name}")
  
  # Check if table already exists
  tables <- DBI::dbListTables(connection)
  if (raw_table_name %in% tables) {
    logger::log_warn("Table {raw_table_name} already exists, dropping it")
    DBI::dbExecute(connection, paste0("DROP TABLE ", raw_table_name))
  }
  
  import_query <- paste0(
    "CREATE TABLE ", raw_table_name, " AS ",
    "SELECT * FROM read_csv_auto('", csv_file_path, "', ",
    "all_varchar=TRUE, strict_mode=FALSE, ignore_errors=TRUE)"
  )
  
  tryCatch({
    DBI::dbExecute(connection, import_query)
    logger::log_success("Successfully imported raw data to table: {raw_table_name}")
  }, error = function(e) {
    logger::log_error("Failed to import NPPES data: {e$message}")
    DBI::dbDisconnect(connection)
    stop("Failed to import NPPES data to DuckDB", call. = FALSE)
  })
}

#' Filter NPPES data
#'
#' @param connection DuckDB connection
#' @param raw_table_name Name of the raw data table
#' @param filtered_table_name Name for the filtered data table
#'
#' @noRd
filter_nppes_data <- function(connection, raw_table_name, filtered_table_name) {
  logger::log_info("Filtering NPPES data for individual US providers")
  
  # Check if filtered table already exists
  tables <- DBI::dbListTables(connection)
  if (filtered_table_name %in% tables) {
    logger::log_warn("Table {filtered_table_name} already exists, dropping it")
    DBI::dbExecute(connection, paste0("DROP TABLE ", filtered_table_name))
  }
  
  tryCatch({
    filtered_data <- dplyr::tbl(connection, raw_table_name) %>%
      dplyr::filter(
        `Entity Type Code` == "1" &
          `Provider Business Mailing Address Country Code (If outside U.S.)` == "US" &
          `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US"
      )
    
    filtered_data %>%
      dplyr::compute(name = filtered_table_name, temporary = FALSE)
    
    logger::log_info("Dropping raw data table to save space")
    DBI::dbExecute(connection, paste0("DROP TABLE ", raw_table_name))
    
    logger::log_success("Successfully filtered data to table: {filtered_table_name}")
  }, error = function(e) {
    logger::log_error("Failed to filter NPPES data: {e$message}")
    DBI::dbDisconnect(connection)
    stop("Failed to filter NPPES data", call. = FALSE)
  })
}

#' Count NPPES providers
#'
#' @param connection DuckDB connection
#' @param table_name Table name
#'
#' @noRd
count_nppes_providers <- function(connection, table_name) {
  logger::log_info("Counting records in filtered data")
  
  tryCatch({
    provider_count <- dplyr::tbl(connection, table_name) %>%
      dplyr::count() %>%
      dplyr::collect() %>%
      dplyr::pull(n)
    
    return(provider_count)
  }, error = function(e) {
    logger::log_error("Failed to count providers: {e$message}")
    return(NA)
  })
}

#' Save NPPES data to RDS file
#'
#' @param connection DuckDB connection
#' @param table_name Table name
#' @param output_path Path to save RDS file
#'
#' @noRd
save_nppes_to_rds <- function(connection, table_name, output_path) {
  logger::log_info("Collecting data for RDS export")
  
  tryCatch({
    logger::log_debug("Reading data from DuckDB table: {table_name}")
    nppes_provider_data <- dplyr::tbl(connection, table_name) %>%
      dplyr::collect()
    
    logger::log_info("Saving data to RDS file: {output_path}")
    saveRDS(
      nppes_provider_data, 
      file = output_path,
      compress = TRUE
    )
    
    logger::log_success("Successfully saved data to RDS file: {output_path}")
  }, error = function(e) {
    logger::log_error("Failed to save data to RDS: {e$message}")
    stop("Failed to save NPPES data to RDS file", call. = FALSE)
  })
}



####
#The check_physician_presence function is a well-designed utility for tracking physicians across temporal data. It efficiently analyzes a dataset containing physician information to determine when specific providers appear in the records. The function accepts a list of National Provider Identifiers (NPIs), optionally paired with provider names, and methodically examines each NPI's presence throughout different years. It returns a structured data frame summarizing each provider's representation in the dataset, including their total record count and a chronological listing of years in which they appear. This function is particularly valuable for longitudinal analyses of healthcare provider data, enabling researchers to identify patterns in physician presence, track career trajectories, or validate data completeness across multiple years of NPI records.

# Function to check for specific NPIs and their years in dataset
check_physician_presence <- function(physician_data, npi_list, names_list = NULL) {
  logger::log_info("Checking presence of {length(npi_list)} specific NPIs in dataset")
  
  # Create a data frame to store results
  results <- data.frame(
    NPI = character(),
    Name = character(),
    Years_Present = character(),
    Count = integer(),
    stringsAsFactors = FALSE
  )
  
  # Check each NPI
  for (i in seq_along(npi_list)) {
    npi <- npi_list[i]
    name <- if (!is.null(names_list)) names_list[i] else "Unknown"
    
    # Filter data for this NPI
    physician_records <- physician_data %>%
      dplyr::filter(NPI == npi)
    
    # Get count and years present
    record_count <- nrow(physician_records)
    years_present <- "None"
    
    if (record_count > 0) {
      # Get unique years and sort them
      unique_years <- sort(unique(physician_records$Year))
      years_present <- paste(unique_years, collapse = ", ")
      
      # Get the physician's name from the data if available
      if (record_count > 0 && is.null(names_list)) {
        first_record <- physician_records[1, ]
        if ("Provider First Name" %in% names(first_record) && 
            "Provider Last Name (Legal Name)" %in% names(first_record)) {
          name <- paste(
            first_record[["Provider First Name"]], 
            first_record[["Provider Last Name (Legal Name)"]]
          )
        }
      }
    }
    
    # Add to results
    results <- rbind(results, data.frame(
      NPI = npi,
      Name = name,
      Years_Present = years_present,
      Count = record_count,
      stringsAsFactors = FALSE
    ))
    
    # Log the result
    if (record_count > 0) {
      logger::log_info("NPI {npi} ({name}) found in {record_count} records across years: {years_present}")
    } else {
      logger::log_warn("NPI {npi} ({name}) not found in dataset")
    }
  }
  
  return(results)
}


### Newer
###
#' Search for healthcare providers across multiple years of NPPES data
#'
#' This function searches across multiple years of National Plan and Provider
#' Enumeration System (NPPES) data to find healthcare providers matching the
#' specified taxonomy codes. It provides a comprehensive approach to track 
#' providers over time.
#'
#' @param connection A valid DBI database connection object
#' @param table_year_mapping A data frame mapping table names to years with
#'   columns 'table_name' and 'year'
#' @param taxonomy_codes A character vector of healthcare provider taxonomy codes
#'   to search for
#' @param years_to_include Optional numeric vector of years to include in the 
#'   search. If NULL (default), all years in table_year_mapping are searched.
#' @param max_results_per_year Maximum number of results to return per year
#'   (default: 1000000)
#' @param verbose Logical; if TRUE (default), detailed logging information is 
#'   displayed
#'
#' @return A tibble containing healthcare provider information across all 
#'   queried years
#'
#' @examples
#' # Connect to a DuckDB database
#' db_path <- "path/to/nppes_database.duckdb"
#' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#'
#' # Create a table-year mapping
#' table_year_mapping <- data.frame(
#'   table_name = c("nppes_2020", "nppes_2021", "nppes_2022"),
#'   year = c(2020, 2021, 2022)
#' )
#'
#' # Define taxonomy codes for OB-GYN specialties
#' obgyn_taxonomy_codes <- c(
#'   "207V00000X",  # Obstetrics & Gynecology
#'   "207VF0040X"   # Female Pelvic Medicine
#' )
#'
#' # Search for OB-GYN providers in all available years
#' provider_results <- find_providers_across_years(
#'   connection = connection,
#'   table_year_mapping = table_year_mapping,
#'   taxonomy_codes = obgyn_taxonomy_codes,
#'   verbose = TRUE
#' )
#'
#' # Search for providers only in specific years with limited results
#' recent_providers <- find_providers_across_years(
#'   connection = connection,
#'   table_year_mapping = table_year_mapping,
#'   taxonomy_codes = c("207V00000X"),
#'   years_to_include = c(2021, 2022),
#'   max_results_per_year = 5000,
#'   verbose = FALSE
#' )
#'
#' # Clean up connection
#' DBI::dbDisconnect(connection)
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_threshold log_info log_warn log_error log_debug
#' @importFrom DBI dbIsValid dbListTables dbGetQuery
#' @importFrom dplyr tbl filter select rename mutate collect all_of bind_rows
#' @importFrom tibble tibble as_tibble
#' @importFrom stringr str_sub
#' @importFrom purrr reduce
#' @importFrom rlang expr
find_providers_across_years <- function(connection,
                                        table_year_mapping,
                                        taxonomy_codes,
                                        years_to_include = NULL,
                                        max_results_per_year = 1000000,
                                        verbose = TRUE) {
  # Set up logging threshold based on verbose parameter
  logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
  logger::log_info("Starting provider search across years")
  
  # Validate all inputs
  validate_inputs(connection, table_year_mapping, taxonomy_codes, 
                  years_to_include, max_results_per_year)
  
  # Filter mapping to requested years
  filtered_mapping <- filter_year_mapping(table_year_mapping, years_to_include)
  
  # Return empty result if no matching years found
  if (nrow(filtered_mapping) == 0) {
    logger::log_warn("No matching years found in the mapping")
    return(create_empty_provider_tibble())
  }
  
  # Query all years in the filtered mapping
  provider_listings <- query_all_years(connection, filtered_mapping, taxonomy_codes, 
                                       max_results_per_year, verbose)
  
  # Combine results with consistent data types
  combined_providers <- combine_provider_results(provider_listings)
  
  # Check for important providers that might be missing from the results
  combined_providers <- check_for_missing_providers(
    connection, 
    combined_providers, 
    filtered_mapping, 
    taxonomy_codes,
    verbose
  )
  
  logger::log_info("Provider search completed successfully")
  return(combined_providers)
}

#' Validate all function inputs
#'
#' @noRd
validate_inputs <- function(connection, table_year_mapping, taxonomy_codes, 
                            years_to_include, max_results_per_year) {
  # Check database connection
  assertthat::assert_that(DBI::dbIsValid(connection), 
                          msg = "Database connection is not valid")
  
  # Check table year mapping
  assertthat::assert_that(is.data.frame(table_year_mapping),
                          msg = "table_year_mapping must be a data frame")
  assertthat::assert_that(all(c("table_name", "year") %in% colnames(table_year_mapping)),
                          msg = "table_year_mapping must contain 'table_name' and 'year' columns")
  
  # Check taxonomy codes
  assertthat::assert_that(is.character(taxonomy_codes), 
                          msg = "taxonomy_codes must be a character vector")
  assertthat::assert_that(length(taxonomy_codes) > 0, 
                          msg = "At least one taxonomy code must be provided")
  
  # Check years to include (if provided)
  if (!is.null(years_to_include)) {
    assertthat::assert_that(is.numeric(years_to_include),
                            msg = "years_to_include must be a numeric vector")
    assertthat::assert_that(length(years_to_include) > 0,
                            msg = "At least one year must be provided if years_to_include is not NULL")
  }
  
  # Check max_results_per_year
  assertthat::assert_that(is.numeric(max_results_per_year),
                          msg = "max_results_per_year must be numeric")
  assertthat::assert_that(max_results_per_year > 0,
                          msg = "max_results_per_year must be greater than 0")
  
  logger::log_info("Input validation completed successfully")
}

#' Filter the table year mapping to include only requested years
#'
#' @noRd
filter_year_mapping <- function(table_year_mapping, years_to_include) {
  filtered_mapping <- table_year_mapping
  if (!is.null(years_to_include)) {
    filtered_mapping <- table_year_mapping[table_year_mapping$year %in% years_to_include, ]
    logger::log_info("Filtered to %d years from the mapping", nrow(filtered_mapping))
  }
  return(filtered_mapping)
}

#' Create an empty provider tibble with the expected column structure
#'
#' @noRd
create_empty_provider_tibble <- function() {
  return(tibble::tibble(
    NPI = character(),
    LastName = character(),
    FirstName = character(),
    MiddleName = character(),
    Gender = character(),
    TaxonomyCode1 = character(),
    City = character(),
    State = character(),
    Zip = character(),
    Year = integer(),
    `NPI Deactivation Date` = character(),
    `Last Update Date` = character(),
    `Provider Enumeration Date` = character(),
    `Provider Organization Name (Legal Business Name)` = character(),
    `Provider Credential Text` = character(),
    `Provider First Line Business Mailing Address` = character(),
    `Provider Business Mailing Address City Name` = character(),
    `Provider Business Mailing Address State Name` = character(),
    `Provider Business Mailing Address Postal Code` = character(),
    `Provider Business Mailing Address Country Code (If outside U.S.)` = character(),
    `Provider Business Practice Location Address Country Code (If outside U.S.)` = character()
  ))
}

#' Query all years in the filtered mapping
#'
#' @noRd
query_all_years <- function(connection, filtered_mapping, taxonomy_codes, 
                            max_results_per_year, verbose) {
  logger::log_info("Querying %d tables/years: %s", 
                   nrow(filtered_mapping),
                   paste(filtered_mapping$year, collapse = ", "))
  
  all_providers <- list()
  
  for (i in 1:nrow(filtered_mapping)) {
    current_table <- filtered_mapping$table_name[i]
    current_year <- filtered_mapping$year[i]
    
    logger::log_info("Querying table for year %d: %s", current_year, current_table)
    
    if (!current_table %in% DBI::dbListTables(connection)) {
      logger::log_warn("Table '%s' not found in database, skipping", current_table)
      next
    }
    
    providers_data <- try_query_methods(connection, current_table, current_year, 
                                        taxonomy_codes, max_results_per_year, verbose)
    
    if (!is.null(providers_data) && nrow(providers_data) > 0) {
      providers_data <- standardize_column_types(providers_data)
      all_providers[[i]] <- providers_data
      logger::log_info("Added %d providers for year %d", 
                       nrow(providers_data), current_year)
    } else {
      logger::log_info("No providers found for year %d", current_year)
    }
  }
  
  return(all_providers)
}

#' Try different query methods to retrieve provider data
#'
#' @noRd
try_query_methods <- function(connection, current_table, current_year, taxonomy_codes, 
                              max_results_per_year, verbose) {
  providers_data <- try_dplyr_approach(connection, current_table, current_year, 
                                       taxonomy_codes, max_results_per_year, verbose)
  
  if (is.null(providers_data)) {
    providers_data <- try_sql_approach(connection, current_table, current_year, 
                                       taxonomy_codes, max_results_per_year, verbose)
  }
  
  if (is.null(providers_data) && verbose) {
    providers_data <- try_direct_approach(connection, current_table, current_year, 
                                          taxonomy_codes, max_results_per_year)
  }
  
  return(providers_data)
}

#' Try to retrieve provider data using dplyr approach
#'
#' @noRd
try_dplyr_approach <- function(connection, current_table, current_year, taxonomy_codes, 
                               max_results_per_year, verbose) {
  providers_data <- NULL
  
  tryCatch({
    logger::log_info("Attempting dplyr-based approach")
    
    nppes_table <- dplyr::tbl(connection, current_table)
    
    col_names <- colnames(nppes_table)
    taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
                             col_names, value = TRUE)
    
    if (length(taxonomy_columns) > 0) {
      logger::log_info("Found %d taxonomy code columns", length(taxonomy_columns))
      
      entity_type_col_exists <- "Entity Type Code" %in% col_names
      
      query <- nppes_table
      
      filter_expr <- NULL
      
      for (i in seq_along(taxonomy_columns)) {
        tax_col <- taxonomy_columns[i]
        
        if (i == 1) {
          filter_expr <- rlang::expr(.data[[tax_col]] %in% !!taxonomy_codes)
        } else {
          filter_expr <- rlang::expr(!!filter_expr | .data[[tax_col]] %in% !!taxonomy_codes)
        }
      }
      
      query <- dplyr::filter(query, !!filter_expr)
      logger::log_info("Applied combined filter across all %d taxonomy columns", 
                       length(taxonomy_columns))
      
      if (entity_type_col_exists) {
        query <- query %>% dplyr::filter(`Entity Type Code` == 1L)
        logger::log_info("Applied Entity Type Code filter for providers (1)")
      } else {
        logger::log_warn("Entity Type Code column not found, skipping filter")
      }
      
      cols_to_select <- c(
        "NPI", 
        "Provider Last Name (Legal Name)", 
        "Provider First Name",
        "Provider Middle Name", 
        "Provider Gender Code",
        taxonomy_columns[1],
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Provider Business Practice Location Address Postal Code",
        "NPI Deactivation Date", 
        "Last Update Date", 
        "Provider Enumeration Date", 
        "Provider Organization Name (Legal Business Name)", 
        "Provider Credential Text", 
        "Provider First Line Business Mailing Address", 
        "Provider Business Mailing Address City Name", 
        "Provider Business Mailing Address State Name", 
        "Provider Business Mailing Address Postal Code", 
        "Provider Business Mailing Address Country Code (If outside U.S.)", 
        "Provider Business Practice Location Address Country Code (If outside U.S.)"
      )
      
      if (length(taxonomy_columns) > 1) {
        cols_to_select <- c(cols_to_select, taxonomy_columns[-1])
        logger::log_info("Including all %d taxonomy columns in output", length(taxonomy_columns))
      }
      
      available_cols <- cols_to_select[cols_to_select %in% col_names]
      logger::log_info("Found %d of %d requested columns", 
                       length(available_cols), length(cols_to_select))
      
      query <- query %>% dplyr::select(dplyr::all_of(available_cols))
      
      rename_map <- c(
        NPI = "NPI",
        LastName = "Provider Last Name (Legal Name)",
        FirstName = "Provider First Name",
        MiddleName = "Provider Middle Name",
        Gender = "Provider Gender Code",
        TaxonomyCode1 = taxonomy_columns[1],
        City = "Provider Business Practice Location Address City Name",
        State = "Provider Business Practice Location Address State Name",
        Zip = "Provider Business Practice Location Address Postal Code"
      )
      
      rename_cols <- rename_map[names(rename_map) %in% colnames(query)]
      
      providers_data <- query %>%
        dplyr::rename(!!!rename_cols) %>%
        dplyr::mutate(Year = as.integer(current_year)) %>%
        dplyr::collect(n = max_results_per_year)
      
      if ("Zip" %in% colnames(providers_data)) {
        providers_data <- providers_data %>%
          dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
        logger::log_info("Cleaned Zip codes to 5 digits")
      }
      
      if (nrow(providers_data) > 0) {
        logger::log_info("Dplyr approach succeeded, found %d providers", 
                         nrow(providers_data))
      } else {
        logger::log_info("No results from dplyr approach")
        providers_data <- NULL
      }
    }
  }, error = function(e) {
    logger::log_error("Dplyr approach failed: %s", e$message)
    providers_data <- NULL
  })
  
  return(providers_data)
}

#' Try to retrieve provider data using direct SQL approach
#'
#' @noRd
try_sql_approach <- function(connection, current_table, current_year, taxonomy_codes, 
                             max_results_per_year, verbose) {
  providers_data <- NULL
  
  tryCatch({
    logger::log_info("Attempting direct SQL approach")
    
    sample_data <- DBI::dbGetQuery(connection, sprintf("SELECT * FROM \"%s\" LIMIT 1", current_table))
    taxonomy_columns <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
                             colnames(sample_data), value = TRUE)
    
    logger::log_info("Found %d taxonomy columns in database", length(taxonomy_columns))
    
    if (length(taxonomy_columns) > 0) {
      taxonomy_values <- paste(sprintf("'%s'", taxonomy_codes), collapse = ",")
      
      taxonomy_filters <- c()
      for (i in 1:length(taxonomy_columns)) {
        taxonomy_filters <- c(
          taxonomy_filters,
          sprintf("\"%s\" IN (%s)", taxonomy_columns[i], taxonomy_values)
        )
      }
      
      taxonomy_filter <- paste(taxonomy_filters, collapse = " OR ")
      
      entity_filter <- ""
      if ("Entity Type Code" %in% colnames(sample_data)) {
        entity_filter <- "AND \"Entity Type Code\" = 1"
        logger::log_info("Added Entity Type Code filter for providers (1)")
      } else {
        logger::log_warn("Entity Type Code column not found in SQL approach, skipping filter")
      }
      
      core_columns <- c(
        "\"NPI\"", 
        "\"Provider Last Name (Legal Name)\" AS LastName",
        "\"Provider First Name\" AS FirstName",
        "\"Provider Middle Name\" AS MiddleName",
        "\"Provider Gender Code\" AS Gender",
        sprintf("\"%s\" AS TaxonomyCode1", taxonomy_columns[1]),
        "\"Provider Business Practice Location Address City Name\" AS City",
        "\"Provider Business Practice Location Address State Name\" AS State",
        "\"Provider Business Practice Location Address Postal Code\" AS Zip"
      )
      
      additional_columns <- c(
        "\"NPI Deactivation Date\"",
        "\"Last Update Date\"",
        "\"Provider Enumeration Date\"",
        "\"Provider Organization Name (Legal Business Name)\"",
        "\"Provider Credential Text\"",
        "\"Provider First Line Business Mailing Address\"",
        "\"Provider Business Mailing Address City Name\"",
        "\"Provider Business Mailing Address State Name\"",
        "\"Provider Business Mailing Address Postal Code\"",
        "\"Provider Business Mailing Address Country Code (If outside U.S.)\"",
        "\"Provider Business Practice Location Address Country Code (If outside U.S.)\""
      )
      
      if (length(taxonomy_columns) > 1) {
        for (i in 2:length(taxonomy_columns)) {
          additional_columns <- c(
            additional_columns,
            sprintf("\"%s\"", taxonomy_columns[i])
          )
        }
      }
      
      available_additional_cols <- c()
      for (col in additional_columns) {
        col_name <- gsub("\"(.*)\".*", "\\1", col)
        if (col_name %in% colnames(sample_data)) {
          available_additional_cols <- c(available_additional_cols, col)
        }
      }
      
      logger::log_info("Found %d of %d additional columns", 
                       length(available_additional_cols), length(additional_columns))
      
      all_columns <- c(core_columns, available_additional_cols)
      column_sql <- paste(all_columns, collapse = ", ")
      
      logger::log_info("Building SQL query")
      query <- sprintf(
        "SELECT %s, %d AS Year
         FROM \"%s\"
         WHERE (%s) %s
         LIMIT %d",
        column_sql,
        current_year,
        current_table,
        taxonomy_filter,
        entity_filter,
        max_results_per_year
      )
      
      if (verbose) {
        logger::log_debug("SQL Query: %s", query)
      }
      
      logger::log_info("Executing SQL query")
      providers_data <- DBI::dbGetQuery(connection, query)
      
      if ("Zip" %in% colnames(providers_data)) {
        providers_data <- providers_data %>%
          dplyr::mutate(Zip = stringr::str_sub(Zip, 1, 5))
        logger::log_info("Cleaned Zip codes to 5 digits")
      }
      
      if (nrow(providers_data) > 0) {
        logger::log_info("SQL approach succeeded, found %d providers", 
                         nrow(providers_data))
      } else {
        logger::log_info("No results from SQL approach")
        providers_data <- NULL
      }
    }
  }, error = function(e) {
    logger::log_error("SQL approach failed: %s", e$message)
    providers_data <- NULL
  })
  
  return(providers_data)
}

#' Try to retrieve provider data using a fallback direct query approach
#'
#' @noRd
try_direct_approach <- function(connection, current_table, current_year, taxonomy_codes, max_results_per_year) {
  providers_data <- NULL
  
  tryCatch({
    logger::log_info("Attempting fallback direct approach")
    
    taxonomy_values <- paste(sprintf("'%s'", taxonomy_codes), collapse = ", ")
    
    query <- sprintf(
      "SELECT * 
       FROM \"%s\" 
       WHERE \"Entity Type Code\" = 1
       AND (\"Healthcare Provider Taxonomy Code_1\" IN (%s) OR
            \"Healthcare Provider Taxonomy Code_2\" IN (%s) OR
            \"Healthcare Provider Taxonomy Code_3\" IN (%s) OR
            \"Healthcare Provider Taxonomy Code_4\" IN (%s) OR
            \"Healthcare Provider Taxonomy Code_5\" IN (%s))
       LIMIT %d",
      current_table,
      taxonomy_values, taxonomy_values, taxonomy_values, taxonomy_values, taxonomy_values,
      max_results_per_year
    )
    
    logger::log_info("Executing direct approach SQL query")
    direct_results <- DBI::dbGetQuery(connection, query)
    
    if (nrow(direct_results) > 0) {
      direct_results$Year <- as.integer(current_year)
      
      column_mapping <- c(
        LastName = "Provider Last Name (Legal Name)",
        FirstName = "Provider First Name",
        MiddleName = "Provider Middle Name",
        Gender = "Provider Gender Code",
        TaxonomyCode1 = "Healthcare Provider Taxonomy Code_1",
        City = "Provider Business Practice Location Address City Name",
        State = "Provider Business Practice Location Address State Name",
        Zip = "Provider Business Practice Location Address Postal Code"
      )
      
      for (new_name in names(column_mapping)) {
        old_name <- column_mapping[new_name]
        if (old_name %in% colnames(direct_results)) {
          direct_results[[new_name]] <- direct_results[[old_name]]
        }
      }
      
      if ("Zip" %in% colnames(direct_results)) {
        direct_results$Zip <- stringr::str_sub(direct_results$Zip, 1, 5)
      }
      
      providers_data <- direct_results
      logger::log_info("Direct approach succeeded, found %d providers", 
                       nrow(providers_data))
    } else {
      logger::log_info("No results from direct approach")
    }
  }, error = function(e) {
    logger::log_error("Direct approach failed: %s", e$message)
  })
  
  return(providers_data)
}

#' Standardize column types for consistency
#'
#' @noRd
standardize_column_types <- function(provider_data) {
  if (is.null(provider_data) || nrow(provider_data) == 0) {
    return(provider_data)
  }
  
  logger::log_info("Standardizing column types for a result set with %d rows", 
                   nrow(provider_data))
  
  char_columns <- c(
    "NPI", "LastName", "FirstName", "MiddleName", "Gender", "TaxonomyCode1",
    "City", "State", "Zip", "NPI Deactivation Date", "Last Update Date", 
    "Provider Enumeration Date", "Provider Organization Name (Legal Business Name)",
    "Provider Credential Text", "Provider First Line Business Mailing Address",
    "Provider Business Mailing Address City Name", "Provider Business Mailing Address State Name",
    "Provider Business Mailing Address Postal Code",
    "Provider Business Mailing Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Country Code (If outside U.S.)"
  )
  
  taxonomy_cols <- grep("Healthcare Provider Taxonomy Code_[0-9]+", 
                        names(provider_data), value = TRUE)
  if (length(taxonomy_cols) > 0) {
    char_columns <- c(char_columns, taxonomy_cols)
  }
  
  int_columns <- c("Year")
  
  # Convert character columns
  for (col in char_columns) {
    if (col %in% names(provider_data)) {
      if (is.factor(provider_data[[col]])) {
        provider_data[[col]] <- as.character(provider_data[[col]])
      } else if (is.numeric(provider_data[[col]])) {
        provider_data[[col]] <- as.character(provider_data[[col]])
      } else if (inherits(provider_data[[col]], "Date") || 
                 inherits(provider_data[[col]], "POSIXt")) {
        provider_data[[col]] <- as.character(provider_data[[col]])
      } else if (!is.character(provider_data[[col]])) {
        provider_data[[col]] <- as.character(provider_data[[col]])
      }
    }
  }
  
  # Convert integer columns
  for (col in int_columns) {
    if (col %in% names(provider_data)) {
      if (is.factor(provider_data[[col]]) || is.character(provider_data[[col]])) {
        provider_data[[col]] <- as.integer(as.character(provider_data[[col]]))
      } else if (is.numeric(provider_data[[col]]) && !is.integer(provider_data[[col]])) {
        provider_data[[col]] <- as.integer(provider_data[[col]])
      } else if (!is.integer(provider_data[[col]])) {
        provider_data[[col]] <- as.integer(as.character(provider_data[[col]]))
      }
    }
  }
  
  # Add missing columns with NA values
  expected_columns <- c(char_columns, int_columns)
  missing_columns <- setdiff(expected_columns, names(provider_data))
  
  if (length(missing_columns) > 0) {
    for (col in missing_columns) {
      if (col %in% char_columns) {
        provider_data[[col]] <- NA_character_
      } else if (col %in% int_columns) {
        provider_data[[col]] <- NA_integer_
      }
    }
  }
  
  return(provider_data)
}

#' Combine provider results from multiple years ensuring consistent types
#'
#' @noRd
combine_provider_results <- function(provider_listings) {
  valid_listings <- Filter(function(x) {
    return(!is.null(x) && (is.data.frame(x) || is.list(x) && length(x) > 0))
  }, provider_listings)
  
  if (length(valid_listings) == 0) {
    logger::log_warn("No valid provider data found after filtering")
    return(create_empty_provider_tibble())
  }
  
  logger::log_info("Combining %d valid data sources out of %d total", 
                   length(valid_listings), length(provider_listings))
  
  all_columns <- unique(unlist(lapply(valid_listings, names)))
  
  # Standardize all listings to have the same columns
  standardized_listings <- lapply(valid_listings, function(df) {
    missing_cols <- setdiff(all_columns, names(df))
    for (col in missing_cols) {
      if (col %in% c("Year")) {
        df[[col]] <- NA_integer_
      } else {
        df[[col]] <- NA_character_
      }
    }
    return(df)
  })
  
  tryCatch({
    # Try primary approach to combine data
    all_providers <- purrr::reduce(standardized_listings, function(acc, x) {
      x <- x[, names(acc)]
      return(rbind(acc, x))
    })
    
    logger::log_info("Found a total of %d providers across all years", 
                     nrow(all_providers))
    
    all_providers <- tibble::as_tibble(all_providers)
    
    return(all_providers)
  }, error = function(e) {
    logger::log_error("Error combining provider data: %s", e$message)
    
    logger::log_warn("Attempting fallback approach for combining data")
    
    # Try fallback approach if the primary approach fails
    tryCatch({
      template <- create_empty_provider_tibble()
      
      processed_listings <- lapply(standardized_listings, function(df) {
        common_cols <- intersect(names(df), names(template))
        new_df <- df[, common_cols, drop = FALSE]
        
        missing_cols <- setdiff(names(template), names(new_df))
        for (col in missing_cols) {
          if (col %in% c("Year")) {
            new_df[[col]] <- NA_integer_
          } else {
            new_df[[col]] <- NA_character_
          }
        }
        
        new_df <- new_df[, names(template)]
        return(new_df)
      })
      
      all_providers <- do.call(rbind, processed_listings)
      all_providers <- tibble::as_tibble(all_providers)
      
      logger::log_info("Fallback approach succeeded with %d providers", 
                       nrow(all_providers))
      return(all_providers)
    }, error = function(inner_e) {
      logger::log_error("Fallback approach also failed: %s", inner_e$message)
      
      # Last resort fallback - return just the first data source
      if (length(valid_listings) > 0) {
        logger::log_warn("Returning only the first data source as final fallback")
        return(valid_listings[[1]])
      } else {
        return(create_empty_provider_tibble())
      }
    })
  })
}

#' Check for specific important providers that might be missing and add them
#'
#' @noRd
check_for_missing_providers <- function(connection, combined_providers, filtered_mapping, 
                                        taxonomy_codes, verbose) {
  if (!verbose) {
    return(combined_providers)
  }
  
  logger::log_info("Checking for known providers that might be missing")
  
  # List of critical NPIs to ensure are included
  important_npis <- c(
    "1689603763"  # Known important provider
    # Add other critical NPIs as needed
  )
  
  found_npis <- intersect(important_npis, combined_providers$NPI)
  missing_npis <- setdiff(important_npis, found_npis)
  
  if (length(missing_npis) > 0) {
    logger::log_warn("Found %d missing important providers. Attempting direct look up.", 
                     length(missing_npis))
    
    for (npi in missing_npis) {
      logger::log_info("Looking for NPI %s in all tables", npi)
      
      for (i in 1:nrow(filtered_mapping)) {
        year <- filtered_mapping$year[i]
        table_name <- filtered_mapping$table_name[i]
        
        # Try direct lookup by NPI
        direct_query <- sprintf(
          "SELECT * FROM \"%s\" 
           WHERE \"NPI\" = '%s' 
           AND \"Entity Type Code\" = 1",
          table_name, npi
        )
        
        direct_result <- DBI::dbGetQuery(connection, direct_query)
        
        if (nrow(direct_result) > 0) {
          logger::log_info("Found missing NPI %s in year %d", npi, year)
          
          # Add year column
          direct_result$Year <- as.integer(year)
          
          # Standardize column names
          column_mapping <- c(
            LastName = "Provider Last Name (Legal Name)",
            FirstName = "Provider First Name",
            MiddleName = "Provider Middle Name",
            Gender = "Provider Gender Code",
            TaxonomyCode1 = "Healthcare Provider Taxonomy Code_1",
            City = "Provider Business Practice Location Address City Name",
            State = "Provider Business Practice Location Address State Name",
            Zip = "Provider Business Practice Location Address Postal Code"
          )
          
          # Rename columns
          for (new_name in names(column_mapping)) {
            old_name <- column_mapping[new_name]
            if (old_name %in% colnames(direct_result)) {
              direct_result[[new_name]] <- direct_result[[old_name]]
            }
          }
          
          # Clean zip
          if ("Zip" %in% colnames(direct_result)) {
            direct_result$Zip <- stringr::str_sub(direct_result$Zip, 1, 5)
          }
          
          # Standardize data types
          direct_result <- standardize_column_types(direct_result)
          
          # Bind to results
          combined_providers <- dplyr::bind_rows(combined_providers, direct_result)
          logger::log_info("Added missing provider with NPI %s for year %d", npi, year)
        }
      }
    }
  }
  
  return(combined_providers)
}

#' Generate a mapping between NPPES database tables and years
#'
#' Analyzes table names in a DuckDB database to create a mapping between
#' tables containing NPPES data and the years they represent. This is particularly
#' useful for tracking healthcare providers across multiple years.
#'
#' @param connection A valid DBI database connection
#' @param verbose Logical; if TRUE, provides detailed logs during execution
#'
#' @return A data frame with columns 'table_name' and 'year'
#'
#' @examples
#' # Connect to a DuckDB database
#' db_path <- "path/to/nppes_database.duckdb"
#' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#'
#' # Generate table-year mapping
#' mapping <- create_nppes_table_mapping(connection)
#'
#' # View the mapping
#' print(mapping)
#'
#' # Clean up connection
#' DBI::dbDisconnect(connection)
#'
#' @importFrom logger log_info log_warn
#' @importFrom DBI dbListTables
#' @importFrom tibble tibble
create_nppes_table_mapping <- function(connection, verbose = TRUE) {
  # Set up logging threshold based on verbose parameter
  logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
  logger::log_info("Generating NPPES table-year mapping")
  
  # Get all tables in the database
  all_tables <- DBI::dbListTables(connection)
  logger::log_info("Found %d total tables in database", length(all_tables))
  
  # Initialize the mapping data frame
  table_year_mapping <- tibble::tibble(
    table_name = character(),
    year = integer()
  )
  
  # Years to check for
  expected_years <- 2010:format(Sys.Date(), "%Y") %>% as.integer()
  found_years <- c()
  
  # Try to identify NPPES tables and map them to years
  for (table_name in all_tables) {
    table_year <- NULL
    
    # Check for year in table name
    for (year in expected_years) {
      year_str <- as.character(year)
      if (grepl(year_str, table_name)) {
        table_year <- year
        found_years <- c(found_years, year)
        break
      }
    }
    
    # If no year found in name, try to check a sample row
    if (is.null(table_year)) {
      tryCatch({
        # See if this looks like an NPPES table by checking for NPI column
        sample_query <- sprintf("SELECT * FROM \"%s\" LIMIT 1", table_name)
        sample_data <- DBI::dbGetQuery(connection, sample_query)
        
        # Check if this looks like an NPPES table
        if ("NPI" %in% colnames(sample_data) && 
            any(grepl("Healthcare Provider Taxonomy Code", colnames(sample_data)))) {
          # Look for date-containing columns
          date_cols <- c("Last Update Date", "Provider Enumeration Date")
          for (date_col in date_cols) {
            if (date_col %in% colnames(sample_data)) {
              date_value <- sample_data[[date_col]][1]
              if (!is.na(date_value)) {
                # Extract year from date string
                year_match <- regexpr("20[0-9]{2}", date_value)
                if (year_match > 0) {
                  extracted_year <- as.integer(substr(date_value, year_match, year_match + 3))
                  if (extracted_year %in% expected_years) {
                    table_year <- extracted_year
                    found_years <- c(found_years, extracted_year)
                    logger::log_info("Assigned year %d to table %s based on date column", 
                                     table_year, table_name)
                    break
                  }
                }
              }
            }
          }
        }
      }, error = function(e) {
        logger::log_warn("Error examining table %s: %s", table_name, e$message)
      })
    }
    
    # Add to mapping if a year was found
    if (!is.null(table_year)) {
      table_year_mapping <- rbind(
        table_year_mapping,
        tibble::tibble(table_name = table_name, year = table_year)
      )
    }
  }
  
  # Sort the mapping by year
  table_year_mapping <- table_year_mapping[order(table_year_mapping$year), ]
  
  # Check which expected years weren't found
  missing_years <- setdiff(expected_years, found_years)
  if (length(missing_years) > 0) {
    warning(sprintf("Could not find tables for the following years: %s", 
                    paste(missing_years, collapse = ", ")))
  }
  
  logger::log_info("Generated mapping with %d tables across %d years", 
                   nrow(table_year_mapping), length(unique(table_year_mapping$year)))
  
  return(table_year_mapping)
}

#' Analyze provider specialty changes over time
#'
#' Analyzes how healthcare providers' taxonomy codes (specialties) change over time.
#' This is useful for tracking career transitions, subspecialization, and other 
#' professional changes.
#'
#' @param provider_data A tibble containing provider data across multiple years
#'   as returned by find_providers_across_years()
#' @param include_unchanged Logical; whether to include providers who didn't change
#'   specialties (default: FALSE)
#' @param verbose Logical; if TRUE, provides detailed logs during execution
#'
#' @return A tibble with columns tracking provider information and specialty changes
#'
#' @examples
#' # Connect to a DuckDB database
#' db_path <- "path/to/nppes_database.duckdb"
#' connection <- DBI::dbConnect(duckdb::duckdb(), db_path)
#'
#' # Create table-year mapping
#' table_year_mapping <- create_nppes_table_mapping(connection)
#'
#' # Define taxonomy codes for OB-GYN specialties
#' obgyn_taxonomy_codes <- c(
#'   "207V00000X",  # Obstetrics & Gynecology
#'   "207VF0040X"   # Female Pelvic Medicine
#' )
#'
#' # Search for providers
#' provider_results <- find_providers_across_years(
#'   connection = connection,
#'   table_year_mapping = table_year_mapping,
#'   taxonomy_codes = obgyn_taxonomy_codes
#' )
#'
#' # Analyze specialty changes
#' specialty_changes <- analyze_specialty_changes(
#'   provider_data = provider_results,
#'   include_unchanged = FALSE,
#'   verbose = TRUE
#' )
#'
#' # Examine results
#' head(specialty_changes)
#'
#' # Count transitions to Female Pelvic Medicine
#' fpm_transitions <- specialty_changes %>%
#'   dplyr::filter(
#'     ChangedSpecialty == TRUE,
#'     TaxonomyCode1 == "207VF0040X"
#'   )
#'
#' # Clean up connection
#' DBI::dbDisconnect(connection)
#'
#' @importFrom logger log_info log_threshold
#' @importFrom dplyr arrange group_by mutate filter
analyze_specialty_changes <- function(provider_data, include_unchanged = FALSE, 
                                      verbose = TRUE) {
  # Set up logging threshold based on verbose parameter
  logger::log_threshold(if(verbose) logger::INFO else logger::WARN)
  logger::log_info("Analyzing provider specialty changes")
  
  # Validate inputs
  assertthat::assert_that(is.data.frame(provider_data),
                          msg = "provider_data must be a data frame")
  assertthat::assert_that("NPI" %in% colnames(provider_data),
                          msg = "provider_data must contain an 'NPI' column")
  assertthat::assert_that("TaxonomyCode1" %in% colnames(provider_data),
                          msg = "provider_data must contain a 'TaxonomyCode1' column")
  assertthat::assert_that("Year" %in% colnames(provider_data),
                          msg = "provider_data must contain a 'Year' column")
  
  # Process the data to identify specialty changes
  specialty_changes <- provider_data %>%
    dplyr::arrange(NPI, Year) %>%
    dplyr::group_by(NPI) %>%
    dplyr::mutate(
      PreviousTaxonomyCode = dplyr::lag(TaxonomyCode1),
      PreviousYear = dplyr::lag(Year),
      YearsSincePrevious = Year - PreviousYear,
      ChangedSpecialty = TaxonomyCode1 != PreviousTaxonomyCode & !is.na(PreviousTaxonomyCode)
    )
  
  # Optionally filter to only include providers who changed specialties
  if (!include_unchanged) {
    logger::log_info("Filtering to include only providers who changed specialties")
    
    # First get the list of NPIs that had at least one change
    changing_npis <- specialty_changes %>%
      dplyr::filter(ChangedSpecialty == TRUE) %>%
      dplyr::select(NPI) %>%
      dplyr::distinct() %>%
      dplyr::pull(NPI)
    
    # Then filter the full dataset to only include those NPIs
    specialty_changes <- specialty_changes %>%
      dplyr::filter(NPI %in% changing_npis)
    
    logger::log_info("Found %d providers who changed specialties at least once", 
                     length(changing_npis))
  }
  
  logger::log_info("Specialty change analysis complete")
  return(specialty_changes)
}


# D-Qualitty_check

# Function to check for specific NPIs and their years in dataset
phase0_check_physician_presence <- function(physician_data, npi_list, names_list = NULL) {
  logger::log_info("Checking presence of {length(npi_list)} specific NPIs in dataset")
  
  # Create a data frame to store results
  results <- data.frame(
    NPI = character(),
    Name = character(),
    Years_Present = character(),
    Count = integer(),
    stringsAsFactors = FALSE
  )
  
  # Check each NPI
  for (i in seq_along(npi_list)) {
    npi <- npi_list[i]
    name <- if (!is.null(names_list)) names_list[i] else "Unknown"
    
    # Filter data for this NPI
    physician_records <- physician_data %>%
      dplyr::filter(NPI == npi)
    
    # Get count and years present
    record_count <- nrow(physician_records)
    years_present <- "None"
    
    if (record_count > 0) {
      # Get unique years and sort them
      unique_years <- sort(unique(physician_records$Year))
      years_present <- paste(unique_years, collapse = ", ")
      
      # Get the physician's name from the data if available
      if (record_count > 0 && is.null(names_list)) {
        first_record <- physician_records[1, ]
        if ("Provider First Name" %in% names(first_record) && 
            "Provider Last Name (Legal Name)" %in% names(first_record)) {
          name <- paste(
            first_record[["Provider First Name"]], 
            first_record[["Provider Last Name (Legal Name)"]]
          )
        }
      }
    }
    
    # Add to results
    results <- rbind(results, data.frame(
      NPI = npi,
      Name = name,
      Years_Present = years_present,
      Count = record_count,
      stringsAsFactors = FALSE
    ))
    
    # Log the result
    if (record_count > 0) {
      logger::log_info("NPI {npi} ({name}) found in {record_count} records across years: {years_present}")
    } else {
      logger::log_warn("NPI {npi} ({name}) not found in dataset")
    }
  }
  
  return(results)
}

###
# E-retirement_year_confirmation

#' Process physician retirement data from multiple sources
#'
#' @description
#' This function processes physician retirement data from multiple sources including
#' prescription data, hospital affiliations, NPI deactivation records, and combines
#' them to determine retirement years for physicians. The output contains only three
#' columns: calculated_retirement_npi, calculated_retirement_year_of_retirement, and
#' calculated_retirement_data_source.
#'
#' @param data_directory Character string specifying the directory containing input data files.
#' @param output_directory Character string specifying where to save output files. 
#'        Defaults to a subdirectory named 'output' within data_directory.
#' @param max_year Integer specifying the maximum retirement year to consider (default: 24).
#' @param retirement_method Character string specifying how to calculate retirement year
#'        when multiple sources are available. Options are 'earliest', 'latest', or 'mode'
#'        (default: 'earliest').
#' @param verbose Logical indicating whether to print detailed logging (default: FALSE).
#'
#' @return A data frame containing physician retirement information, with one row per 
#'         physician. The output contains only three columns, all prefixed with 
#'         "calculated_retirement_":
#'         - calculated_retirement_npi: The National Provider Identifier
#'         - calculated_retirement_year_of_retirement: The retirement year for the physician
#'         - calculated_retirement_data_source: The data source file name
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_threshold log_info log_debug log_warn log_error
#' @importFrom tidyr replace_na
#' @importFrom dplyr select mutate filter arrange group_by summarize ungroup left_join
#' @importFrom dplyr row_number n
#' @importFrom data.table as.data.table fread setDT
#' @importFrom stats median na.omit
#' @importFrom utils read.csv write.csv
#'
#' @examples
#' # Basic usage with default parameters
#' retirement_data <- process_physician_retirement(
#'   data_directory = "/path/to/data/",
#'   verbose = TRUE
#' )
#' 
#' # The result includes only three prefixed columns:
#' # - calculated_retirement_npi
#' # - calculated_retirement_year_of_retirement
#' # - calculated_retirement_data_source
#'
#' # Specify output directory and custom retirement calculation method
#' retirement_data <- process_physician_retirement(
#'   data_directory = "/path/to/data/",
#'   output_directory = "/path/to/custom/output/",
#'   retirement_method = "latest",
#'   verbose = TRUE
#' )
#'
#' # Process with specific maximum year
#' retirement_data <- process_physician_retirement(
#'   data_directory = "/path/to/data/",
#'   output_directory = "/path/to/outputs/",
#'   max_year = 23,
#'   retirement_method = "mode",
#'   verbose = TRUE
#' )
process_physician_retirement <- function(data_directory,
                                         output_directory = file.path(data_directory, "output"),
                                         max_year = 24,
                                         retirement_method = "earliest",
                                         verbose = FALSE) {
  # Initialize logger
  initialize_logger(verbose)
  
  # Log function start and parameters
  logger::log_info("Starting process_physician_retirement with parameters:")
  logger::log_info("  - data_directory: {data_directory}")
  logger::log_info("  - output_directory: {output_directory}")
  logger::log_info("  - max_year: {max_year}")
  logger::log_info("  - retirement_method: {retirement_method}")
  
  # Validate inputs
  validate_inputs(data_directory, output_directory, max_year, retirement_method)
  
  # Ensure output directory exists
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
  }
  
  # Get file paths for all data sources
  file_paths <- get_file_paths(data_directory)
  
  # Load data sources
  data_sources <- load_data_sources(file_paths)
  
  # Process retirement data
  processed_retirement_data <- process_retirement_data(data_sources, retirement_method)
  
  # Merge retirement data with NIPS data
  merged_data <- merge_with_nips_data(
    processed_retirement_data,
    data_sources$updated_processed_nips,
    max_year
  )
  
  # Deduplicate to ensure one row per physician
  physician_records <- deduplicate_physician_data(merged_data)
  
  # Keep only required columns and add prefixes
  final_records <- create_final_output(physician_records)
  
  # Write output
  output_file <- file.path(output_directory, "processed_retirement_data.csv")
  tryCatch({
    utils::write.csv(final_records, output_file, row.names = FALSE)
    logger::log_info("Successfully wrote processed retirement data: {nrow(final_records)} records")
  }, error = function(e) {
    logger::log_error("Failed to write output file: {e$message}")
  })
  
  logger::log_info("Physician retirement processing completed successfully")
  
  return(final_records)
}

#' @noRd
initialize_logger <- function(verbose) {
  log_level <- if (verbose) "DEBUG" else "INFO"
  logger::log_threshold(log_level)
  logger::log_info("Logger initialized with level: {log_level}")
}

#' @noRd
validate_inputs <- function(data_directory, output_directory, max_year, retirement_method) {
  tryCatch({
    assertthat::assert_that(is.character(data_directory), 
                            msg = "data_directory must be a character string")
    assertthat::assert_that(dir.exists(data_directory), 
                            msg = "data_directory does not exist")
    assertthat::assert_that(is.character(output_directory), 
                            msg = "output_directory must be a character string")
    assertthat::assert_that(is.numeric(max_year), msg = "max_year must be numeric")
    assertthat::assert_that(
      retirement_method %in% c("earliest", "latest", "mode"),
      msg = "retirement_method must be one of: 'earliest', 'latest', 'mode'"
    )
  }, error = function(e) {
    logger::log_error("Validation error: {e$message}")
    stop(e$message)
  })
}

#' @noRd
get_file_paths <- function(data_directory) {
  list(
    updated_processed_nips = file.path(data_directory, "updated_processed_nips.csv"),
    prescription = file.path(data_directory, "prescription_data.csv"),
    hospital = file.path(data_directory, "facility_affiliations.csv"),
    npi_deactivation = file.path(data_directory, "npi_deactivations.csv"),
    npi_database = file.path(data_directory, "npi_database.csv"),
    goba = file.path(data_directory, "goba_records.csv")
  )
}

#' @noRd
safe_read_csv <- function(file_path, required = FALSE) {
  if (!file.exists(file_path)) {
    if (required) {
      logger::log_error("Required data file not found: {file_path}")
      stop(paste("Required data file not found:", file_path))
    } else {
      return(NULL)
    }
  }
  
  tryCatch({
    # Use data.table::fread for faster reading of large files
    df <- data.table::fread(file_path, data.table = TRUE, showProgress = TRUE)
    return(df)
  }, error = function(e) {
    if (required) {
      logger::log_error("Failed to read required file {file_path}: {e$message}")
      stop(paste("Failed to read required file:", file_path, "-", e$message))
    } else {
      logger::log_warn("Failed to read optional file {file_path}: {e$message}")
      return(NULL)
    }
  })
}

#' @noRd
load_data_sources <- function(file_paths) {
  data_sources <- list()
  
  # Load updated processed NIPS data (required)
  data_sources$updated_processed_nips <- safe_read_csv(file_paths$updated_processed_nips, 
                                                       required = TRUE)
  logger::log_info("Loaded updated processed NIPS data: {nrow(data_sources$updated_processed_nips)} rows")
  
  # Load optional data sources
  data_sources$prescription <- safe_read_csv(file_paths$prescription)
  data_sources$hospital <- safe_read_csv(file_paths$hospital)
  data_sources$npi_deactivation <- safe_read_csv(file_paths$npi_deactivation)
  data_sources$npi_database <- safe_read_csv(file_paths$npi_database)
  data_sources$goba <- safe_read_csv(file_paths$goba)
  
  return(data_sources)
}

#' @noRd
process_retirement_data <- function(data_sources, retirement_method) {
  retirement_years <- list()
  
  # Process data from each source
  if (!is.null(data_sources$hospital)) {
    retirement_years$hospital <- process_hospital_data(data_sources$hospital)
    if (!is.null(retirement_years$hospital)) {
      # Set the file name as the source identifier
      retirement_years$hospital$file_source <- "facility_affiliations.csv"
    }
  }
  
  if (!is.null(data_sources$prescription)) {
    retirement_years$prescription <- process_prescription_data(data_sources$prescription)
    if (!is.null(retirement_years$prescription)) {
      # Set the file name as the source identifier
      retirement_years$prescription$file_source <- "prescription_data.csv"
    }
  }
  
  if (!is.null(data_sources$npi_deactivation)) {
    retirement_years$npi_deactivation <- 
      process_npi_deactivation_data(data_sources$npi_deactivation)
    if (!is.null(retirement_years$npi_deactivation)) {
      # Set the file name as the source identifier
      retirement_years$npi_deactivation$file_source <- "npi_deactivations.csv"
    }
  }
  
  # Merge all retirement data
  merged_retirement <- merge_retirement_data(retirement_years, retirement_method)
  
  return(merged_retirement)
}

#' @noRd
standardize_npi_column <- function(data) {
  if (is.null(data)) return(NULL)
  
  data <- data.table::setDT(data)
  if ("NPI" %in% names(data) && !"npi" %in% names(data)) {
    data.table::setnames(data, "NPI", "npi")
  }
  
  return(data)
}

#' @noRd
process_hospital_data <- function(hospital_data) {
  tryCatch({
    hospital_data <- standardize_npi_column(hospital_data)
    
    if (!"npi" %in% names(hospital_data)) {
      logger::log_warn("No NPI column found in hospital data")
      return(NULL)
    }
    
    # Check for year column
    year_cols <- c("last_year", "retirement_year", "end_year", "termination_year")
    year_col <- NULL
    for (col in year_cols) {
      if (col %in% names(hospital_data)) {
        year_col <- col
        break
      }
    }
    
    if (is.null(year_col)) {
      logger::log_warn("No retirement year column found in hospital data")
      return(NULL)
    }
    
    retirement_data <- hospital_data[, c("npi", year_col), with = FALSE]
    data.table::setnames(retirement_data, year_col, "retirement_year")
    
    retirement_data <- retirement_data[!is.na(retirement_year) & retirement_year > 0]
    retirement_data$source <- "Hospital"
    
    return(retirement_data)
  }, error = function(e) {
    logger::log_error("Error processing hospital data: {e$message}")
    return(NULL)
  })
}

#' @noRd
process_prescription_data <- function(prescription_data) {
  tryCatch({
    prescription_data <- standardize_npi_column(prescription_data)
    
    if (!"npi" %in% names(prescription_data)) {
      logger::log_warn("No NPI column found in prescription data")
      return(NULL)
    }
    
    # Check for year column
    year_cols <- c("last_year", "retirement_year", "end_year", "year")
    year_col <- NULL
    for (col in year_cols) {
      if (col %in% names(prescription_data)) {
        year_col <- col
        break
      }
    }
    
    if (is.null(year_col)) {
      logger::log_warn("No year column found in prescription data")
      return(NULL)
    }
    
    retirement_data <- prescription_data[, c("npi", year_col), with = FALSE]
    data.table::setnames(retirement_data, year_col, "retirement_year")
    
    retirement_data <- retirement_data[!is.na(retirement_year) & retirement_year > 0]
    retirement_data$source <- "Medicare"
    
    return(retirement_data)
  }, error = function(e) {
    logger::log_error("Error processing prescription data: {e$message}")
    return(NULL)
  })
}

#' @noRd
process_npi_deactivation_data <- function(npi_deactivation_data) {
  tryCatch({
    npi_deactivation_data <- standardize_npi_column(npi_deactivation_data)
    
    if (!"npi" %in% names(npi_deactivation_data)) {
      logger::log_warn("No NPI column found in NPI deactivation data")
      return(NULL)
    }
    
    # Check for date column
    date_cols <- c("deactivation_date", "retirement_date", "end_date", "termination_date")
    date_col <- NULL
    for (col in date_cols) {
      if (col %in% names(npi_deactivation_data)) {
        date_col <- col
        break
      }
    }
    
    if (is.null(date_col)) {
      logger::log_warn("No date column found in NPI deactivation data")
      return(NULL)
    }
    
    retirement_data <- npi_deactivation_data[, c("npi", date_col), with = FALSE]
    
    # Extract year from date field
    if (inherits(retirement_data[[date_col]], "Date")) {
      retirement_data$retirement_year <- as.numeric(format(retirement_data[[date_col]], "%y"))
    } else if (is.character(retirement_data[[date_col]])) {
      dates <- as.Date(retirement_data[[date_col]], format = "%Y-%m-%d")
      if (all(is.na(dates))) {
        dates <- as.Date(retirement_data[[date_col]], format = "%m/%d/%Y")
      }
      if (all(is.na(dates))) {
        logger::log_warn("Could not parse dates in NPI deactivation data")
        return(NULL)
      }
      retirement_data$retirement_year <- as.numeric(format(dates, "%y"))
    } else {
      logger::log_warn("Date column in NPI deactivation data is not in a recognized format")
      return(NULL)
    }
    
    retirement_data <- retirement_data[, c("npi", "retirement_year")]
    retirement_data <- retirement_data[!is.na(retirement_year) & retirement_year > 0]
    retirement_data$source <- "retirement_NPPES Deactivated"
    
    return(retirement_data)
  }, error = function(e) {
    logger::log_error("Error processing NPI deactivation data: {e$message}")
    return(NULL)
  })
}

#' @noRd
merge_retirement_data <- function(retirement_years, retirement_method) {
  # Initialize an empty data frame for the merged data
  merged_retirement <- NULL
  
  # Add each available data source to the merged data
  for (source_name in names(retirement_years)) {
    source_data <- retirement_years[[source_name]]
    
    if (is.null(source_data) || nrow(source_data) == 0) {
      next
    }
    
    if (is.null(merged_retirement)) {
      merged_retirement <- data.table::copy(source_data)
    } else {
      merged_retirement <- data.table::rbindlist(
        list(merged_retirement, source_data),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }
  
  if (is.null(merged_retirement) || nrow(merged_retirement) == 0) {
    logger::log_warn("No retirement data found in any source")
    return(data.table::data.table(npi = character(0), retirement_year = integer(0)))
  }
  
  # Calculate composite retirement year based on the specified method
  retirement_summary <- merged_retirement[, {
    years <- retirement_year
    
    if (retirement_method == "earliest") {
      idx <- which.min(years)
      result_year <- min(years, na.rm = TRUE)
      result_source <- source[idx]
      result_file <- file_source[idx]
    } else if (retirement_method == "latest") {
      idx <- which.max(years)
      result_year <- max(years, na.rm = TRUE)
      result_source <- source[idx]
      result_file <- file_source[idx]
    } else if (retirement_method == "mode") {
      # Find the most common year
      tab <- table(years)
      if (length(tab) > 0) {
        mode_year <- as.numeric(names(tab)[which.max(tab)])
        idx <- which(years == mode_year)[1]
        result_year <- mode_year
        result_source <- source[idx]
        result_file <- file_source[idx]
      } else {
        result_year <- NA
        result_source <- NA
        result_file <- NA
      }
    } else {
      # Default to earliest if method is not recognized
      idx <- which.min(years)
      result_year <- min(years, na.rm = TRUE)
      result_source <- source[idx]
      result_file <- file_source[idx]
    }
    
    list(
      retirement_year = result_year,
      source = result_source,
      file_source = result_file
    )
  }, by = "npi"]
  
  # Remove any NAs
  retirement_summary <- retirement_summary[!is.na(retirement_year)]
  
  return(retirement_summary)
}

#' @noRd
merge_with_nips_data <- function(processed_retirement_data, nips_data, max_year) {
  tryCatch({
    # Ensure we have data.tables
    processed_retirement_data <- data.table::setDT(processed_retirement_data)
    nips_data <- data.table::setDT(nips_data)
    
    # Standardize NPI column in NIPS data
    nips_data <- standardize_npi_column(nips_data)
    
    # Filter out retirement years beyond max_year
    if (nrow(processed_retirement_data) > 0) {
      processed_retirement_data <- processed_retirement_data[retirement_year <= max_year]
    }
    
    # Make sure npi is character type in both datasets
    processed_retirement_data$npi <- as.character(processed_retirement_data$npi)
    nips_data$npi <- as.character(nips_data$npi)
    
    # Create copies to avoid modifying the original data
    retirement_copy <- data.table::copy(processed_retirement_data)
    nips_copy <- data.table::copy(nips_data)
    
    # Deduplicate NPIs in retirement data
    if (nrow(retirement_copy) > 0 && any(duplicated(retirement_copy$npi))) {
      retirement_copy <- retirement_copy[!duplicated(retirement_copy$npi)]
    }
    
    # Prepare NIPS data for merging
    # Standardize column names if needed
    if ("year_of_retirement" %in% names(nips_copy)) {
      # Rename to a standard name for later processing
      data.table::setnames(nips_copy, "year_of_retirement", "retirement_year_nips")
    }
    
    if ("data_source" %in% names(nips_copy)) {
      # Rename to a standard name for later processing
      data.table::setnames(nips_copy, "data_source", "file_source_nips")
    }
    
    # Get only the most recent record for each NPI in NIPS data
    latest_nips <- nips_copy[, .SD[which.max(year)], by = npi]
    
    # Perform left join
    merged_data <- merge(retirement_copy, latest_nips, by = "npi", all.x = TRUE)
    
    if (is.null(merged_data) || nrow(merged_data) == 0) {
      return(latest_nips)
    }
    
    # Fill in the retirement year from NIPS if not found from other sources
    if ("retirement_year_nips" %in% names(merged_data) && 
        sum(is.na(merged_data$retirement_year)) > 0) {
      merged_data[is.na(retirement_year) & !is.na(retirement_year_nips), 
                  `:=`(retirement_year = retirement_year_nips,
                       source = "NIPS Data",
                       file_source = "updated_processed_nips.csv")]
    }
    
    return(merged_data)
  }, error = function(e) {
    logger::log_error("Error merging with NIPS data: {e$message}")
    return(nips_data[, .SD[which.max(year)], by = npi])
  })
}

#' @noRd
deduplicate_physician_data <- function(physician_data) {
  tryCatch({
    physician_data <- data.table::setDT(physician_data)
    
    if (!"npi" %in% names(physician_data)) {
      logger::log_error("No NPI column found for deduplication")
      return(physician_data)
    }
    
    # Determine which columns to use for ordering
    order_cols <- c()
    for (col in c("year", "lastupdatestr", "retirement_year")) {
      if (col %in% names(physician_data)) {
        order_cols <- c(order_cols, col)
      }
    }
    
    # If we have ordering columns, use them to select the most recent record per NPI
    if (length(order_cols) > 0) {
      primary_order_col <- order_cols[1]
      deduplicated_data <- physician_data[order(-get(primary_order_col)), .SD[1], by = npi]
    } else {
      deduplicated_data <- physician_data[!duplicated(physician_data$npi)]
    }
    
    return(deduplicated_data)
  }, error = function(e) {
    logger::log_error("Error during deduplication: {e$message}")
    return(physician_data[!duplicated(physician_data$npi)])
  })
}

#' @noRd
create_final_output <- function(physician_data) {
  tryCatch({
    logger::log_debug("Creating final output with required columns and prefixes")
    
    physician_data <- data.table::setDT(physician_data)
    
    # Create a new data table with just the required columns
    required_cols <- c("npi", "retirement_year", "file_source")
    
    # Check if we have the required columns or their alternatives
    if (!"retirement_year" %in% names(physician_data) && 
        "retirement_year_nips" %in% names(physician_data)) {
      data.table::setnames(physician_data, "retirement_year_nips", "retirement_year")
    }
    
    if (!"file_source" %in% names(physician_data)) {
      if ("source" %in% names(physician_data)) {
        # Use source as data_source if file_source is not available
        physician_data$file_source <- physician_data$source
      } else if ("data_source" %in% names(physician_data)) {
        # Use data_source if available
        data.table::setnames(physician_data, "data_source", "file_source")
      } else if ("file_source_nips" %in% names(physician_data)) {
        # Use NIPS source if available
        data.table::setnames(physician_data, "file_source_nips", "file_source")
      }
    }
    
    # Create a new data table with just the required columns
    available_cols <- intersect(required_cols, names(physician_data))
    if (length(available_cols) < length(required_cols)) {
      missing_cols <- setdiff(required_cols, available_cols)
      logger::log_warn("Missing required columns: {paste(missing_cols, collapse=', ')}")
      
      # Create empty columns for missing ones
      for (col in missing_cols) {
        physician_data[, (col) := NA]
      }
    }
    
    # Rename the columns with the prefix
    final_data <- data.table::data.table(
      calculated_retirement_npi = physician_data$npi,
      calculated_retirement_year_of_retirement = physician_data$retirement_year,
      calculated_retirement_data_source = physician_data$file_source
    )
    
    logger::log_info("Created final output with three prefixed columns: {nrow(final_data)} records")
    
    return(final_data)
  }, error = function(e) {
    logger::log_error("Error creating final output: {e$message}")
    
    # Return a minimal valid output in case of error
    if ("npi" %in% names(physician_data)) {
      return(data.table::data.table(
        calculated_retirement_npi = physician_data$npi,
        calculated_retirement_year_of_retirement = NA,
        calculated_retirement_data_source = NA
      ))
    } else {
      return(data.table::data.table(
        calculated_retirement_npi = character(0),
        calculated_retirement_year_of_retirement = integer(0),
        calculated_retirement_data_source = character(0)
      ))
    }
  })
}

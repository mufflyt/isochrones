# DISAPPEARANCE FROM DATABASES STRATEGY

#' Determine Physician Retirement Year from NPI Data Sources
#'
#' @description
#' Analyzes physician data sources that contain NPI numbers to determine the most
#' likely retirement year. Focuses specifically on NPPES deactivation data and
#' facility affiliation data, with improved data validation.
#'
#' @param nppes_deactivation_file Character. File path to NPPES Deactivation data
#'   (XLSX format). Default: NULL (no NPPES deactivation data)  
#' @param facility_affiliation_file Character. File path to Facility Affiliation data
#'   (CSV format). Default: NULL (no facility affiliation data)
#' @param year_min Numeric. Minimum plausible year to consider valid. Default: 1950
#' @param year_max Numeric. Maximum plausible year to consider valid. Default: current
#'   year + 1
#' @param output_file Character. Optional file path to save results.
#'   Default: NULL (no file output)
#' @param verbose Logical. Whether to provide detailed logging. Default: TRUE
#'
#' @return A tibble with physician identifiers and determined retirement years
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr select mutate filter left_join group_by summarize arrange 
#' @importFrom dplyr distinct if_else coalesce rename_with bind_rows
#' @importFrom readr read_csv write_csv
#' @importFrom readxl read_xlsx
#' @importFrom logger log_info log_warn log_error log_debug
#' @importFrom stringr str_trim str_extract
#' @importFrom tibble tibble as_tibble
#' @importFrom purrr map map_lgl
#'
#' @examples
#' # Example 1: Using NPPES deactivation data
#' retirement_years <- improved_npi_retirement_determination(
#'   nppes_deactivation_file = "data/nppes_deactivated_downloads/NPPES Deactivated NPI Report.xlsx",
#'   verbose = TRUE
#' )
#' 
#' print(head(retirement_years))
#'
#' # Example 2: Using both data sources with file output
#' retirement_results <- improved_npi_retirement_determination(
#'   nppes_deactivation_file = "data/nppes_deactivated_downloads/NPPES Deactivated NPI Report.xlsx",
#'   facility_affiliation_file = "data/facility_affiliation/Facility_Affiliation.csv",
#'   output_file = "physician_retirement_years.csv",
#'   verbose = TRUE
#' )
#' 
#' print(head(retirement_results))
#'
improved_npi_retirement_determination <- function(nppes_deactivation_file = NULL,
                                                  facility_affiliation_file = NULL,
                                                  year_min = 1950,
                                                  year_max = as.numeric(format(Sys.Date(), "%Y")) + 1,
                                                  output_file = NULL,
                                                  verbose = TRUE) {
  # Initialize logger
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::WARN)
  }
  
  logger::log_info("Starting improved NPI-based physician retirement year determination")
  logger::log_info("Valid year range: {year_min} to {year_max}")
  
  # Initialize results structure
  retirement_data <- list()
  
  # Process NPPES deactivation file
  if (!is.null(nppes_deactivation_file)) {
    logger::log_info("Processing NPPES deactivation data from: {nppes_deactivation_file}")
    
    tryCatch({
      # Read the XLSX file
      nppes_data <- readxl::read_xlsx(nppes_deactivation_file)
      logger::log_debug("NPPES file loaded successfully with {ncol(nppes_data)} columns and {nrow(nppes_data)} rows")
      
      # Debug: show column names
      logger::log_debug("NPPES columns: {paste(colnames(nppes_data), collapse=', ')}")
      
      # The file appears to have unusual structure - investigate the content
      # Look for NPI pattern in first 100 rows of each column
      npi_found <- FALSE
      npi_column <- NULL
      date_column <- NULL
      
      for (col_name in colnames(nppes_data)) {
        col_data <- head(nppes_data[[col_name]], 100)
        col_data <- col_data[!is.na(col_data)]  # Remove NA values
        
        # Convert to character for pattern matching
        col_data_char <- as.character(col_data)
        
        # Check for NPI pattern (10 digits)
        if (!npi_found && any(grepl("^\\d{10}$", col_data_char))) {
          logger::log_info("Found potential NPI column: {col_name}")
          npi_column <- col_name
          npi_found <- TRUE
        }
        
        # Check for date pattern
        if (is.null(date_column) && any(grepl("\\d{1,2}/\\d{1,2}/\\d{4}", col_data_char))) {
          logger::log_info("Found potential date column: {col_name}")
          date_column <- col_name
        }
      }
      
      # If first approach fails, try to extract NPIs and dates from text content
      if (is.null(npi_column) || is.null(date_column)) {
        logger::log_info("Trying alternative approach for NPPES data extraction")
        
        # Try to handle the case where the file has a header row with combined content
        if (ncol(nppes_data) <= 3) {  # Typically these files have very few columns
          # Extract from the first column which might contain the full text content
          main_col <- colnames(nppes_data)[1]
          content_lines <- as.character(nppes_data[[main_col]])
          content_lines <- content_lines[!is.na(content_lines)]  # Remove NA values
          
          # Extract NPIs (10 digit numbers)
          npis <- stringr::str_extract(content_lines, "\\d{10}")
          
          # Extract dates (MM/DD/YYYY format)
          dates <- stringr::str_extract(content_lines, "\\d{1,2}/\\d{1,2}/\\d{4}")
          
          # Create a cleaned data frame
          clean_data <- data.frame(
            NPI = npis,
            Deactivation_Date = dates,
            stringsAsFactors = FALSE
          )
          
          # Filter out rows where either NPI or date is NA
          clean_data <- clean_data[!is.na(clean_data$NPI) & !is.na(clean_data$Deactivation_Date),]
          
          if (nrow(clean_data) > 0) {
            logger::log_info("Successfully extracted {nrow(clean_data)} NPI/date pairs from text content")
            
            retirement_data[["nppes_deactivation"]] <- clean_data %>%
              dplyr::mutate(
                # Ensure NPI is a character type
                npi = as.character(NPI),
                # Extract year from date (MM/DD/YYYY format)
                retirement_year = as.numeric(stringr::str_extract(Deactivation_Date, "\\d{4}$")),
                data_source = "nppes_deactivation"
              ) %>%
              # Filter for valid years
              dplyr::filter(
                retirement_year >= year_min,
                retirement_year <= year_max
              ) %>%
              dplyr::select(npi, retirement_year, data_source)
          } else {
            logger::log_warn("Could not extract valid NPI/date pairs from NPPES file content")
          }
        } else {
          logger::log_warn("Could not identify NPI and date columns in NPPES data")
        }
      } else {
        # Process using identified columns
        retirement_data[["nppes_deactivation"]] <- nppes_data %>%
          dplyr::select(dplyr::all_of(c(npi_column, date_column))) %>%
          dplyr::filter(!is.na(.data[[npi_column]]), !is.na(.data[[date_column]])) %>%
          dplyr::mutate(
            # Ensure NPI is a character type
            npi = as.character(.data[[npi_column]]),
            # Extract year from date
            retirement_year = as.numeric(stringr::str_extract(
              as.character(.data[[date_column]]), "\\d{4}"
            )),
            data_source = "nppes_deactivation"
          ) %>%
          # Filter for valid years
          dplyr::filter(
            retirement_year >= year_min,
            retirement_year <= year_max
          ) %>%
          dplyr::select(npi, retirement_year, data_source)
        
        logger::log_info("Extracted retirement years from NPPES deactivation data for {nrow(retirement_data[['nppes_deactivation']])} physicians")
      }
    }, error = function(e) {
      logger::log_error("Failed to process NPPES deactivation data: {e$message}")
    })
  }
  
  # Process Facility Affiliation data
  if (!is.null(facility_affiliation_file)) {
    logger::log_info("Processing Facility Affiliation data from: {facility_affiliation_file}")
    
    tryCatch({
      # Read the CSV file
      facility_data <- readr::read_csv(facility_affiliation_file, show_col_types = FALSE)
      logger::log_debug("Facility Affiliation file loaded successfully with {ncol(facility_data)} columns and {nrow(facility_data)} rows")
      
      # Debug: show column names
      logger::log_debug("Facility Affiliation columns: {paste(colnames(facility_data), collapse=', ')}")
      
      # Verify it has the NPI column
      if ("NPI" %in% colnames(facility_data)) {
        # Convert NPI to character immediately to avoid type issues later
        facility_data <- facility_data %>%
          dplyr::mutate(NPI = as.character(NPI))
        
        # Since we don't have explicit year column in this dataset, 
        # we'll use the current year for active facilities
        # This assumes these are active physicians as of the current data
        current_year <- as.numeric(format(Sys.Date(), "%Y"))
        
        # Create a dataset with the current year for each physician
        retirement_data[["facility_affiliation"]] <- facility_data %>%
          dplyr::select(NPI) %>%
          dplyr::distinct() %>%
          dplyr::mutate(
            retirement_year = current_year,
            data_source = "facility_affiliation"
          ) %>%
          dplyr::rename(npi = "NPI")
        
        logger::log_info("Assigned current year ({current_year}) as retirement year for {nrow(retirement_data[['facility_affiliation']])} unique physicians in Facility Affiliation data")
      } else {
        logger::log_warn("Facility Affiliation data missing NPI column")
      }
    }, error = function(e) {
      logger::log_error("Failed to process Facility Affiliation data: {e$message}")
    })
  }
  
  # Combine data from all sources
  if (length(retirement_data) == 0) {
    logger::log_warn("No retirement data extracted from any source")
    result_data <- tibble::tibble(
      npi = character(),
      retirement_year = numeric(),
      data_source = character()
    )
  } else {
    # Combine all data sources, ensuring all NPIs are character type
    all_retirement_data <- dplyr::bind_rows(
      retirement_data %>%
        purrr::map(function(df) {
          if ("npi" %in% colnames(df)) {
            df %>% dplyr::mutate(npi = as.character(npi))
          } else {
            df
          }
        })
    )
    
    if (nrow(all_retirement_data) == 0) {
      logger::log_warn("Combined retirement data is empty")
      result_data <- tibble::tibble(
        npi = character(),
        retirement_year = numeric(),
        data_source = character()
      )
    } else {
      # Priority order: NPPES deactivation > Facility Affiliation
      result_data <- all_retirement_data %>%
        dplyr::mutate(
          priority = dplyr::case_when(
            data_source == "nppes_deactivation" ~ 1,
            data_source == "facility_affiliation" ~ 2,
            TRUE ~ 3
          )
        ) %>%
        dplyr::arrange(npi, priority) %>%
        dplyr::group_by(npi) %>%
        dplyr::slice(1) %>%  # Take highest priority record
        dplyr::ungroup() %>%
        dplyr::select(-priority)
      
      logger::log_info("Combined retirement data for {nrow(result_data)} unique physicians")
    }
  }
  
  # Save to file if requested
  if (!is.null(output_file) && nrow(result_data) > 0) {
    logger::log_info("Saving results to file: {output_file}")
    readr::write_csv(result_data, output_file)
    logger::log_info("Results successfully saved to {output_file}")
  } else if (!is.null(output_file)) {
    logger::log_warn("No data to save to file")
  }
  
  return(result_data)
}

###
# Create the directory if it doesn't exist
dir.create("data/retirement", showWarnings = FALSE, recursive = TRUE)

# Then run the function
retirement_results <- simplified_npi_retirement_determination(
  nppes_deactivation_file = "data/nppes_deactivated_downloads/NPPES Deactivated NPI Report 20250414.xlsx",
  facility_affiliation_file = "data/facility_affiliation/Facility_Affiliation.csv",
  output_file = "data/retirement/physician_retirement_years.csv",
  verbose = TRUE
)
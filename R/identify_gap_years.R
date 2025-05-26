# 1434 version ----
#' Identify Physician Retirement Based on Medicare Part D Prescription Gaps
#'
#' @description
#' This function analyzes physician prescribing patterns in Medicare Part D data
#' to identify potential retirement years based on gaps between consecutive years
#' of prescribing activity.
#'
#' @param prescriber_data A data frame containing Medicare Part D prescribing data
#' @param npi_column Character string specifying the name of the column containing
#'        physician NPI numbers. Default is "PRSCRBR_NPI".
#' @param year_column Character string specifying the name of the column containing
#'        the activity year. Default is "year".
#' @param gap_threshold Numeric value specifying the minimum gap (in years) that
#'        suggests potential retirement. Default is 2.
#' @param completeness_threshold Numeric value between 0 and 1 specifying the maximum
#'        completeness ratio for suspected retirement. Default is 0.8.
#' @param verbose Logical indicating whether to print detailed information. 
#'        Default is TRUE.
#'
#' @return A list containing two data frames:
#'         1. physician_summary: One row per physician with gap analysis and 
#'            retirement metrics
#'         2. complete_data: Original data with retirement information joined
#'
#' @importFrom dplyr filter mutate select left_join group_by summarize arrange
#' @importFrom dplyr distinct n case_when desc any_of
#' @importFrom assertthat assert_that
#' @importFrom rlang sym
#' @importFrom logger log_info log_debug log_threshold
#'
#' @examples
#' # Create sample Medicare Part D data
#' medicare_sample <- data.frame(
#'   PRSCRBR_NPI = rep(c(1000000001, 1000000002, 1000000003), each = 5),
#'   year = c(2016:2020, 2016:2018, 2020:2021, 2016:2020),
#'   Tot_Clms = sample(10:100, 15)
#' )
#' 
#' # Basic usage with default parameters
#' retirement_analysis <- analyze_physician_retirement(
#'   prescriber_data = medicare_sample,
#'   npi_column = "PRSCRBR_NPI",
#'   year_column = "year",
#'   gap_threshold = 2,
#'   completeness_threshold = 0.8,
#'   verbose = TRUE
#' )
#' 
#' # Examine the physician summary
#' head(retirement_analysis$physician_summary)
#' 
#' # More restrictive gap threshold
#' strict_analysis <- analyze_physician_retirement(
#'   prescriber_data = medicare_sample,
#'   gap_threshold = 3,
#'   completeness_threshold = 0.7,
#'   verbose = FALSE
#' )
#' 
#' # Filter for high confidence retirements
#' high_confidence <- dplyr::filter(
#'   strict_analysis$physician_summary,
#'   retirement_category == "High confidence"
#' )
#' 
#' # Example with custom column names
#' custom_data <- data.frame(
#'   provider_id = rep(c(1000000001, 1000000002), each = 4),
#'   activity_year = c(2017:2020, 2017:2018, 2020:2021),
#'   Total_Claims = sample(10:100, 8)
#' )
#' 
#' custom_analysis <- analyze_physician_retirement(
#'   prescriber_data = custom_data,
#'   npi_column = "provider_id",
#'   year_column = "activity_year",
#'   gap_threshold = 1,
#'   completeness_threshold = 0.9,
#'   verbose = TRUE
#' )
#' 
#' # Filter for suspected retirements
#' retired_providers <- dplyr::filter(
#'   custom_analysis$physician_summary, 
#'   suspected_retirement == TRUE
#' )
analyze_physician_retirement <- function(prescriber_data, 
                                         npi_column = "PRSCRBR_NPI", 
                                         year_column = "year",
                                         gap_threshold = 2,
                                         completeness_threshold = 0.8,
                                         verbose = TRUE) {
  # Set up logging if verbose
  if (verbose) {
    logger::log_threshold(logger::INFO)
    logger::log_info(sprintf("Starting physician retirement analysis"))
    logger::log_info(sprintf("Parameters: gap_threshold=%d, completeness_threshold=%.2f", 
                             gap_threshold, completeness_threshold))
  } else {
    logger::log_threshold(logger::WARN)
  }
  
  # Validate inputs
  validate_inputs(prescriber_data, npi_column, year_column, 
                  gap_threshold, completeness_threshold, verbose)
  
  # Convert column names to symbols for proper evaluation
  npi_col <- rlang::sym(npi_column)
  year_col <- rlang::sym(year_column)
  
  # Ensure year is numeric
  if (verbose) {
    logger::log_info(sprintf("Preparing data: Converting year to numeric"))
  }
  
  prescriber_clean <- prescriber_data %>%
    dplyr::mutate(
      numeric_year = as.numeric(!!year_col)
    )
  
  # Calculate gaps for each physician
  physician_gaps <- calculate_physician_gaps(prescriber_clean, npi_col, year_col, verbose)
  
  # Analyze retirement patterns
  retirement_analysis <- analyze_retirement_patterns(
    physician_gaps, 
    gap_threshold, 
    completeness_threshold, 
    verbose
  )
  
  # Print summary statistics if verbose
  print_summary_statistics(retirement_analysis, gap_threshold, verbose)
  
  # Create a clean, simplified version for joining back to original dataset
  if (verbose) {
    logger::log_info(sprintf("Preparing retirement info for joining with original data"))
  }
  
  physician_retirement_info <- retirement_analysis %>%
    dplyr::select(!!npi_col, max_gap, retirement_year, confidence_score, retirement_category)
  
  # Join retirement data back to original dataset
  if (verbose) {
    logger::log_info(sprintf("Joining retirement info with original prescriber data"))
  }
  
  complete_prescriber_data <- prescriber_clean %>%
    dplyr::select(-dplyr::any_of(c("gap_years", "year_of_suspected_retirement"))) %>%
    dplyr::left_join(physician_retirement_info, by = npi_column)
  
  if (verbose) {
    logger::log_info(sprintf("Analysis complete"))
  }
  
  # Return both the physician summary and the complete data
  return(list(
    physician_summary = retirement_analysis,
    complete_data = complete_prescriber_data
  ))
}

#' @noRd
validate_inputs <- function(prescriber_data, npi_column, year_column, 
                            gap_threshold, completeness_threshold, verbose) {
  # Validate prescriber_data is a data frame
  assertthat::assert_that(is.data.frame(prescriber_data),
                          msg = "prescriber_data must be a data frame")
  
  # Validate column names exist in the data frame
  assertthat::assert_that(npi_column %in% names(prescriber_data),
                          msg = paste("Column", npi_column, "not found in prescriber_data"))
  assertthat::assert_that(year_column %in% names(prescriber_data),
                          msg = paste("Column", year_column, "not found in prescriber_data"))
  
  # Validate threshold parameters
  assertthat::assert_that(is.numeric(gap_threshold), 
                          msg = "gap_threshold must be numeric")
  assertthat::assert_that(gap_threshold > 0,
                          msg = "gap_threshold must be greater than 0")
  assertthat::assert_that(is.numeric(completeness_threshold),
                          msg = "completeness_threshold must be numeric")
  assertthat::assert_that(completeness_threshold >= 0 && completeness_threshold <= 1,
                          msg = "completeness_threshold must be between 0 and 1")
  
  # Validate verbose parameter
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be logical (TRUE/FALSE)")
}

#' @noRd
calculate_physician_gaps <- function(prescriber_data, npi_col, year_col, verbose) {
  if (verbose) {
    logger::log_info(sprintf("Calculating year gaps for each physician"))
  }
  
  physician_gaps <- prescriber_data %>%
    dplyr::select(!!npi_col, numeric_year) %>%
    dplyr::distinct() %>%
    dplyr::arrange(!!npi_col, numeric_year) %>%
    dplyr::group_by(!!npi_col) %>%
    dplyr::summarize(
      first_year = min(numeric_year),
      last_year = max(numeric_year),
      # Calculate maximum gap between consecutive years
      max_gap = ifelse(
        length(numeric_year) > 1,
        max(diff(sort(numeric_year))),
        0
      ),
      n_years_observed = dplyr::n(),
      years_span = last_year - first_year + 1,
      year_sequence = list(sort(numeric_year)),
      .groups = "drop"
    )
  
  if (verbose) {
    logger::log_info(sprintf("Identified %d unique physicians", nrow(physician_gaps)))
    logger::log_debug(sprintf("Year span range: %d to %d", 
                              min(physician_gaps$years_span), 
                              max(physician_gaps$years_span)))
    logger::log_debug(sprintf("Maximum gap range: %d to %d", 
                              min(physician_gaps$max_gap), 
                              max(physician_gaps$max_gap)))
  }
  
  return(physician_gaps)
}

#' @noRd
analyze_retirement_patterns <- function(physician_gaps, gap_threshold, 
                                        completeness_threshold, verbose) {
  if (verbose) {
    logger::log_info(sprintf("Analyzing retirement patterns with gap threshold %d", 
                             gap_threshold))
  }
  
  retirement_analysis <- physician_gaps %>%
    dplyr::mutate(
      # Calculate completeness ratio (observed years / span)
      completeness_ratio = n_years_observed / years_span,
      # Flag physicians with significant gaps
      has_significant_gap = max_gap >= gap_threshold,
      # Determine suspected retirement
      suspected_retirement = has_significant_gap & completeness_ratio < completeness_threshold,
      # Determine year of suspected retirement
      retirement_year = ifelse(
        suspected_retirement,
        last_year,
        NA_real_
      ),
      # Calculate confidence score based on gap size and completeness
      confidence_score = ifelse(
        suspected_retirement,
        pmin(100, (max_gap/gap_threshold * 50) + ((1 - completeness_ratio) * 50)),
        0
      ),
      # Add a retirement category for easier filtering
      retirement_category = dplyr::case_when(
        confidence_score >= 80 ~ "High confidence",
        confidence_score >= 60 ~ "Medium confidence",
        confidence_score >= 40 ~ "Low confidence",
        confidence_score > 0 ~ "Very low confidence",
        TRUE ~ "Not retired"
      )
    )
  
  if (verbose) {
    suspected_count <- sum(retirement_analysis$suspected_retirement, na.rm = TRUE)
    logger::log_info(sprintf("Identified %d physicians with suspected retirement", 
                             suspected_count))
    
    high_conf_count <- sum(retirement_analysis$retirement_category == "High confidence", 
                           na.rm = TRUE)
    logger::log_info(sprintf("High confidence retirement cases: %d", high_conf_count))
  }
  
  return(retirement_analysis)
}

#' @noRd
print_summary_statistics <- function(retirement_analysis, gap_threshold, verbose) {
  if (!verbose) {
    return(invisible(NULL))
  }
  
  logger::log_info("=== Physician Retirement Analysis ===")
  logger::log_info(sprintf("Total physicians analyzed: %d", nrow(retirement_analysis)))
  logger::log_info(sprintf("Physicians with gaps >= %d years: %d", 
                           gap_threshold, 
                           sum(retirement_analysis$has_significant_gap, na.rm = TRUE)))
  logger::log_info(sprintf("Physicians with suspected retirement: %d", 
                           sum(retirement_analysis$suspected_retirement, na.rm = TRUE)))
  
  # Create a gap distribution table
  gap_distribution <- retirement_analysis %>%
    dplyr::group_by(max_gap) %>%
    dplyr::summarize(count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(max_gap)
  
  logger::log_info("Distribution of maximum gaps between consecutive years:")
  print(gap_distribution)
  
  # Show retirement category distribution
  retirement_distribution <- retirement_analysis %>%
    dplyr::group_by(retirement_category) %>%
    dplyr::summarize(count = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(count))
  
  logger::log_info("Retirement confidence categories:")
  print(retirement_distribution)
}

# ### Execute ----
# # Path to Medicare Part D data
# medicare_file <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
# 
# data <- read_csv("/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv")
# 
# 
# 
# prescriber_data <- 
#   analyze_physician_retirement(data, 
#                                npi_column = "PRSCRBR_NPI", 
#                                year_column = "year",
#                                gap_threshold = 2,
#                                completeness_threshold = 0.8,
#                                verbose = TRUE)  

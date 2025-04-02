#' Assign Last Update Year for Physicians
#'
#' This function assigns the latest update year for a specific physician based on the provided list of update years.
#' The function takes a physician's NPI (National Provider Identifier), a given year, and a data frame of updates, and returns the most recent update year that occurred before or during the given year.
#' Extensive logging is included for better understanding of the operations.
#'
#' @param physician_npi A numeric value representing the National Provider Identifier (NPI) of the physician.
#' @param given_year A numeric value representing the year for which the latest update is being sought.
#' @param updates_data A data frame containing updates data. It must include the columns 'npi' and 'lastupdatestr'.
#'
#' @return The most recent update year for the given physician and year.
#' @examples
#' # Example 1: Assign the last update year for a physician with NPI 1003002627 for the year 2015.
#' updates_data_example <- tibble::tibble(
#'   npi = c(1003002627, 1003002627, 1003002627, 1003006651, 1003006651),
#'   lastupdatestr = c(2010, 2014, 2016, 2007, 2011)
#' )
#' assign_lastupdate(physician_npi = 1003002627, given_year = 2015, updates_data = updates_data_example)
#'
#' # Example 2: Assign the last update year for a physician with NPI 1003006651 for the year 2012.
#' assign_lastupdate(physician_npi = 1003006651, given_year = 2012, updates_data = updates_data_example)
#'
#' # Example 3: Assign the last update year for a physician with NPI 1003002627 for the year 2010.
#' assign_lastupdate(physician_npi = 1003002627, given_year = 2010, updates_data = updates_data_example)
#'
#' @importFrom dplyr filter pull arrange collect
#' @importFrom glue glue
#' @importFrom purrr map
#' @export
assign_lastupdate_vectorized <- function(npi_list, year_list, updates_data) {
  # Load required packages
  library(dplyr)
  library(glue)
  library(purrr)
  
  # Log the function call
  message(glue::glue("Function assign_lastupdate_vectorized called."))
  
  # Define a function to handle missing or invalid values
  get_last_update <- function(npi, year, updates_data) {
    # Return NA if npi or year are missing
    if (is.na(npi) || is.na(year)) {
      return(NA_integer_)
    }
    
    # Extract the updates for the given npi
    update_values <- updates_data %>%
      dplyr::filter(npi == !!npi) %>%
      dplyr::pull(lastupdatestr) %>%
      unlist() %>%
      sort(decreasing = FALSE)
    
    # If no update values are found, return NA
    if (length(update_values) == 0) {
      return(NA_integer_)
    }
    
    # Initialize the last value as NA_integer_
    last_update_year <- NA_integer_
    
    # Determine the correct last update year
    for (update_year in update_values) {
      if (!is.na(year) && year >= update_year) {
        last_update_year <- update_year
      } else {
        break
      }
    }
    
    return(last_update_year)
  }
  
  # Use purrr::map2 to iterate over npi_list and year_list
  last_update_list <- purrr::map2_int(npi_list, year_list, ~ get_last_update(.x, .y, updates_data))
  
  # Log the completion of the function
  message(glue::glue("Vectorized function assign_lastupdate_vectorized completed."))
  
  return(last_update_list)
}

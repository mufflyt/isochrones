#' Track API Calls and Estimate Cost
#'
#' This function calculates the total number of API calls and the estimated cost for those calls. The cost is calculated based on a given rate after accounting for a certain number of free API calls.
#' Additionally, this function includes logging for each major step, validating the inputs, and writing the output to a CSV file if a path is provided.
#'
#' @param api_range_list A non-empty vector specifying the range of API calls made for each row.
#' @param number_of_rows A positive numeric value specifying the number of rows for which the API calls need to be tracked.
#' @param output_csv_path Optional. A character string specifying the path to save the output as a CSV file. Default is NULL, meaning no file is written.
#'
#' @return A list containing total API calls (`total_api_calls`) and the estimated cost (`estimated_total_cost`).
#' @importFrom dplyr %>%
#' @importFrom dplyr mutate
#' @importFrom glue glue
#' @importFrom readr write_csv
#' @importFrom tibble tibble
#' @examples
#' 
#' # Example 1: Calculate API calls and cost without saving output
#' api_calls <- c("range1", "range2", "range3")
#' num_rows <- 1500
#' here_track_api_calls_and_cost(api_calls, num_rows)
#'
#' # Example 2: Calculate API calls and cost with output saved to CSV
#' output_path <- "api_calls_cost_output.csv"
#' here_track_api_calls_and_cost(api_calls, num_rows, output_path)
#'
#' # Example 3: Calculate API calls when the total number of calls does not exceed the free limit
#' here_track_api_calls_and_cost(api_calls, 500)
here_track_api_calls_and_cost <- function(api_range_list, number_of_rows, output_csv_path = NULL) {
  # Load required packages
  library(dplyr)
  library(glue)
  library(readr)
  
  # Log the function call with input arguments
  message(glue::glue("Function track_api_calls_and_cost called with inputs: api_range_list = {toString(api_range_list)}, number_of_rows = {number_of_rows}"))
  
  # Validate inputs
  if (!is.vector(api_range_list) || length(api_range_list) == 0) {
    stop("api_range_list must be a non-empty vector.")
  }
  if (!is.numeric(number_of_rows) || number_of_rows <= 0) {
    stop("number_of_rows must be a positive numeric value.")
  }
  
  # Define constants for pricing
  free_calls_per_month <- 2500
  cost_per_thousand_calls <- 5.50
  message("Constants set: free_calls_per_month = 2500, cost_per_thousand_calls = 5.50")
  
  # Calculate total API calls per row
  total_api_calls_per_row <- length(api_range_list)
  message(glue::glue("API calls per row calculated: {total_api_calls_per_row}"))
  
  # Calculate total API calls for all rows
  total_api_calls <- total_api_calls_per_row * number_of_rows
  message(glue::glue("Total API calls calculated: {total_api_calls}"))
  
  # Determine if API calls exceed the free limit
  if (total_api_calls <= free_calls_per_month) {
    estimated_total_cost <- 0
    message("Total API calls are within the free limit. Estimated total cost set to 0.")
  } else {
    # Calculate cost for additional API calls
    additional_api_calls <- total_api_calls - free_calls_per_month
    estimated_total_cost <- additional_api_calls * cost_per_thousand_calls / 1000
    message(glue::glue("Total API calls exceed the free limit. Additional calls: {additional_api_calls}, Estimated total cost: {estimated_total_cost}"))
  }
  
  # Create output as a list
  api_cost_summary <- list(total_api_calls = total_api_calls, estimated_total_cost = estimated_total_cost)
  
  # Log the final output
  message(glue::glue("Function output: total_api_calls = {api_cost_summary$total_api_calls}, estimated_total_cost = {api_cost_summary$estimated_total_cost}"))
  
  # Write output to a file if output_csv_path is provided
  if (!is.null(output_csv_path)) {
    message(glue::glue("Writing output to file: {output_csv_path}"))
    api_cost_summary_df <- tibble::tibble(total_api_calls = total_api_calls, estimated_total_cost = estimated_total_cost)
    tryCatch(
      {
        readr::write_csv(api_cost_summary_df, output_csv_path)
        message(glue::glue("Output successfully written to {output_csv_path}"))
      },
      error = function(e) {
        message(glue::glue("Failed to write output to {output_csv_path}: {e$message}"))
      }
    )
  }
  
  # Return total API calls and estimated cost
  return(api_cost_summary)
}

# # Example usage with logging
# example_api_range_list <- c("range1", "range2", "range3")
# example_number_of_rows <- 1500
# example_output_csv_path <- "api_calls_cost_estimate.csv"
# 
# # Test the function
# here_track_api_calls_and_cost(example_api_range_list, example_number_of_rows, example_output_csv_path)
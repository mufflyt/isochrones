#' Analyze and Visualize Marginal Benefits of Travel Time Threshold Extensions
#'
#' @description This function analyzes the marginal benefits of extending travel 
#' time thresholds by extracting time thresholds and population coverage data 
#' from CSV files. It calculates population increase percentages and visualizes 
#' the results.
#'
#' @param csv_path Character. Path to the CSV file containing isochrone data.
#'   Default is "Organized_Data_by_Isochrone_Range.csv".
#' @param year_filter Integer. Filter data for a specific year. If NULL, uses the
#'   most recent year. Default is NULL.
#' @param race_filter Character. Filter data for a specific race/demographic.
#'   If NULL, uses total population. Default is NULL.
#' @param verbose Logical. If TRUE, provide detailed logging information.
#'   Default is FALSE.
#'
#' @return A list containing:
#'   \item{threshold_extensions}{A data frame with time threshold extensions and 
#'   their corresponding population increase percentages}
#'   \item{plot}{A ggplot2 object showing the marginal benefits visualization}
#'   \item{raw_data}{A data frame with the raw data used for the analysis}
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter mutate arrange desc group_by summarize select
#' @importFrom readr read_csv
#' @importFrom tidyr pivot_longer
#' @importFrom ggplot2 ggplot aes geom_col theme_minimal labs scale_fill_viridis_d
#' @importFrom logger log_info log_debug log_error
#' @importFrom stringr str_remove str_detect
#'
#' @examples
#' # Example 1: Basic usage with default settings
#' travel_analysis <- analyze_time_threshold_benefits(
#'   csv_path = "Organized_Data_by_Isochrone_Range.csv",
#'   verbose = TRUE
#' )
#' print(travel_analysis$threshold_extensions)
#' # Shows threshold extensions with population increase percentages
#' 
#' # Example 2: Filter for a specific year
#' travel_analysis2 <- analyze_time_threshold_benefits(
#'   csv_path = "Organized_Data_by_Isochrone_Range.csv",
#'   year_filter = 2020,
#'   verbose = FALSE
#' )
#' print(travel_analysis2$threshold_extensions)
#' # Shows threshold extensions for 2020 data
#' 
#' # Example 3: Filter for a specific demographic group
#' travel_analysis3 <- analyze_time_threshold_benefits(
#'   csv_path = "Organized_Data_by_Isochrone_Range.csv", 
#'   race_filter = "White",
#'   verbose = TRUE
#' )
#' # View the visualization
#' print(travel_analysis3$plot)
#' # Shows visualization for White demographic group
analyze_time_threshold_benefits <- function(csv_path = "Organized_Data_by_Isochrone_Range.csv",
                                            year_filter = NULL,
                                            race_filter = NULL,
                                            verbose = FALSE) {
  # Setup logging
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::INFO)
  }
  
  logger::log_info("Starting analysis of travel time threshold benefits")
  logger::log_debug("Input parameters: csv_path = {csv_path}")
  logger::log_debug("Input parameters: year_filter = {year_filter}")
  logger::log_debug("Input parameters: race_filter = {race_filter}")
  
  # Load and process the data
  threshold_data <- load_threshold_data(csv_path, year_filter, race_filter)
  
  # Calculate marginal benefits
  threshold_extensions <- calculate_marginal_benefits(threshold_data)
  
  # Create visualization
  benefits_plot <- create_benefits_visualization(threshold_extensions)
  
  # Prepare output
  analysis_results <- list(
    threshold_extensions = threshold_extensions,
    plot = benefits_plot,
    raw_data = threshold_data$population_data
  )
  
  logger::log_info("Analysis completed successfully")
  logger::log_info("Highest benefit: {threshold_extensions$extension[which.max(threshold_extensions$population_increase_pct)]} with {max(threshold_extensions$population_increase_pct)}% increase")
  
  return(analysis_results)
}

#' Load and process threshold data from CSV file
#' 
#' @param csv_path Path to the CSV file
#' @param year_filter Optional year filter
#' @param race_filter Optional race/demographic filter
#' 
#' @return A list with processed data
#' 
#' @noRd
load_threshold_data <- function(csv_path, year_filter, race_filter) {
  logger::log_debug("Loading data from {csv_path}")
  
  # Check if file exists
  assertthat::assert_that(file.exists(csv_path), 
                          msg = paste("File not found:", csv_path))
  
  # Read CSV file
  population_data <- readr::read_csv(csv_path, show_col_types = FALSE)
  logger::log_debug("Loaded {nrow(population_data)} rows from {csv_path}")
  
  # Verify expected column names
  expected_columns <- c("pop_1800", "pop_3600", "pop_7200", "pop_10800")
  for (col in expected_columns) {
    assertthat::assert_that(col %in% colnames(population_data),
                            msg = paste("Expected column", col, "not found in CSV"))
  }
  
  # Convert time in seconds to minutes for better interpretation
  time_thresholds <- c(30, 60, 120, 180)
  names(time_thresholds) <- c("pop_1800", "pop_3600", "pop_7200", "pop_10800")
  
  logger::log_debug("Time thresholds: {paste(time_thresholds, collapse=', ')} minutes")
  
  # Filter data if specified
  if (!is.null(year_filter)) {
    assertthat::assert_that("year" %in% colnames(population_data),
                            msg = "Year column not found in CSV, cannot apply year_filter")
    population_data <- dplyr::filter(population_data, year == year_filter)
    logger::log_debug("Filtered for year {year_filter}: {nrow(population_data)} rows remaining")
  }
  
  if (!is.null(race_filter)) {
    assertthat::assert_that("race" %in% colnames(population_data),
                            msg = "Race column not found in CSV, cannot apply race_filter")
    population_data <- dplyr::filter(population_data, race == race_filter)
    logger::log_debug("Filtered for race {race_filter}: {nrow(population_data)} rows remaining")
  }
  
  # Check that we have data after filtering
  assertthat::assert_that(nrow(population_data) > 0,
                          msg = "No data remaining after applying filters")
  
  # If we have multiple rows (e.g., multiple years or demographics), aggregate
  if (nrow(population_data) > 1) {
    logger::log_debug("Aggregating data from {nrow(population_data)} rows")
    
    population_summary <- dplyr::summarize(population_data,
                                           pop_1800 = mean(pop_1800, na.rm = TRUE),
                                           pop_3600 = mean(pop_3600, na.rm = TRUE),
                                           pop_7200 = mean(pop_7200, na.rm = TRUE),
                                           pop_10800 = mean(pop_10800, na.rm = TRUE))
    
    logger::log_debug("Aggregated to summary values")
  } else {
    # Just use the single row
    population_summary <- dplyr::select(population_data, 
                                        pop_1800, pop_3600, pop_7200, pop_10800)
  }
  
  # Get the population values as a vector
  population_values <- as.numeric(as.vector(population_summary[1, ]))
  names(population_values) <- colnames(population_summary)
  
  logger::log_debug("Population values: {paste(population_values, collapse=', ')}")
  
  return(list(
    time_thresholds = time_thresholds,
    population_values = population_values,
    population_data = population_data
  ))
}

#' Calculate marginal benefits of extending time thresholds
#' 
#' @param threshold_data List with time thresholds and population values
#' 
#' @return A data frame with threshold extensions and population increase percentages
#' 
#' @noRd
calculate_marginal_benefits <- function(threshold_data) {
  logger::log_debug("Calculating marginal benefits of threshold extensions")
  
  time_thresholds <- threshold_data$time_thresholds
  population_values <- threshold_data$population_values
  
  # Create empty vectors to store results
  from_threshold <- numeric(length(time_thresholds) - 1)
  to_threshold <- numeric(length(time_thresholds) - 1)
  population_increase_pct <- numeric(length(time_thresholds) - 1)
  
  # Calculate percentage increases between adjacent thresholds
  for (i in 1:(length(time_thresholds) - 1)) {
    from_threshold[i] <- time_thresholds[i]
    to_threshold[i] <- time_thresholds[i + 1]
    
    # Get population values for this threshold and the next
    from_col <- names(time_thresholds)[i]
    to_col <- names(time_thresholds)[i + 1]
    
    prev_pop <- population_values[from_col]
    curr_pop <- population_values[to_col]
    
    # Calculate percentage increase
    population_increase_pct[i] <- round((curr_pop - prev_pop) / prev_pop * 100, 1)
    
    logger::log_debug("Extension from {from_threshold[i]} to {to_threshold[i]} minutes: {population_increase_pct[i]}% increase")
  }
  
  # Create data frame
  threshold_extensions <- data.frame(
    extension = paste(from_threshold, "to", to_threshold),
    from_threshold = from_threshold,
    to_threshold = to_threshold,
    population_increase_pct = population_increase_pct
  )
  
  # Add ranking
  threshold_extensions <- dplyr::mutate(threshold_extensions, 
                                        rank = rank(-population_increase_pct))
  
  logger::log_info("Identified {nrow(threshold_extensions)} threshold extensions")
  logger::log_info("Highest benefit: {threshold_extensions$extension[which.max(threshold_extensions$population_increase_pct)]} with {max(threshold_extensions$population_increase_pct)}% increase")
  
  return(threshold_extensions)
}

#' Create visualization of marginal benefits
#' 
#' @param threshold_extensions Data frame with threshold extensions data
#' 
#' @return A ggplot2 object with the visualization
#' 
#' @noRd
create_benefits_visualization <- function(threshold_extensions) {
  logger::log_debug("Creating visualization of marginal benefits")
  
  # Order extensions by their 'from' threshold for the plot
  threshold_extensions$extension <- factor(threshold_extensions$extension, 
                                           levels = threshold_extensions$extension[order(threshold_extensions$from_threshold)])
  
  # Create plot
  benefits_plot <- ggplot2::ggplot(
    threshold_extensions, 
    ggplot2::aes(x = extension, y = population_increase_pct, 
                 fill = factor(rank))
  ) +
    ggplot2::geom_col() +
    ggplot2::labs(
      title = "Marginal Benefits of Extending Travel Time Thresholds",
      subtitle = "Population Increase Percentage by Time Threshold Extension",
      x = "Time Threshold Extension (minutes)",
      y = "Population Increase (%)",
      fill = "Rank"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::scale_fill_viridis_d(direction = -1)
  
  logger::log_debug("Visualization created")
  
  return(benefits_plot)
}


####
travel_analysis <- analyze_time_threshold_benefits(
  csv_path = "data/Tannous/Organized_Data_by_Isochrone_Range.csv",
  verbose = TRUE)
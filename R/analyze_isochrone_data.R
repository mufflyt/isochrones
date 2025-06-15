# This file defines a comprehensive R function called analyze_isochrone_data() for analyzing healthcare accessibility data, specifically focusing on isochrone analysis (areas accessible within certain travel times).
# The function is designed to process datasets that contain information about population access to healthcare services at different time thresholds (like 30, 60, 120, and 180 minutes). It performs several key operations:
#   
#   Data Processing: Reads and validates CSV input data, handling percentage conversions and column validation
# Summary Statistics: Calculates key metrics (mean, median, min, max, standard deviation) of access values grouped by specified categories
# Trend Analysis: If year data is available, it performs temporal trend analysis to identify changes in accessibility over time
# Visualization: Creates several plots (bar charts, time series, boxplots) to visualize accessibility patterns

#' Analyze Isochrone Access Data Across Different Dimensions
#' 
#' @description This function analyzes isochrone access data, which measures population 
#'   accessibility at different time thresholds (e.g., 30min, 60min). The function can 
#'   process various data formats including access by category, by race, or by rural/urban 
#'   classification. The function produces summary statistics and optional visualizations.
#'
#' @param data_file_path Character string specifying the path to the input data file. Must be 
#'   a CSV file with appropriate structure containing isochrone access data.
#' @param group_column Character string specifying the column used for grouping the data 
#'   (e.g., "category", "race"). Default is "category".
#' @param time_thresholds Character vector specifying the time threshold columns to analyze. 
#'   Default is c("access_30min", "access_60min", "access_120min", "access_180min").
#' @param output_dir Character string specifying the directory for output files. Default is 
#'   the current working directory.
#' @param create_plots Logical value indicating whether to create visualization plots. 
#'   Default is TRUE.
#' @param calculate_trends Logical value indicating whether to calculate temporal trends 
#'   when year data is available. Default is TRUE.
#' @param verbose Logical value indicating whether to provide detailed logging. Default is 
#'   FALSE.
#'
#' @return A list containing:
#'   \item{summary_stats}{A data frame with summary statistics by group and time threshold}
#'   \item{trend_analysis}{A data frame with trend analysis results if calculate_trends is TRUE}
#'   \item{plot_paths}{A character vector with paths to saved plots if create_plots is TRUE}
#'
#' @examples
#' # Example 1: Basic analysis with default parameters
#' isochrone_results <- analyze_isochrone_data(
#'   data_file_path = "Access_Data.csv",
#'   group_column = "category",
#'   time_thresholds = c("access_30min", "access_60min", "access_120min", "access_180min"),
#'   output_dir = getwd(),
#'   create_plots = TRUE,
#'   calculate_trends = TRUE,
#'   verbose = FALSE
#' )
#' # The function returns summary statistics by category for each time threshold
#' # and generates plots visualizing the accessibility patterns
#' 
#' # Example 2: Analyzing racial disparities in access with verbose logging
#' race_analysis <- analyze_isochrone_data(
#'   data_file_path = "Organized_Data_by_Isochrone_Range.csv",
#'   group_column = "race",
#'   time_thresholds = c("pop_1800", "pop_3600", "pop_7200", "pop_10800"),
#'   output_dir = "results/race_analysis",
#'   create_plots = TRUE,
#'   calculate_trends = TRUE,
#'   verbose = TRUE
#' )
#' # The function provides detailed logging of each step and generates
#' # trend analysis showing changes in accessibility by racial group
#' 
#' # Example 3: Comparing rural vs urban accessibility without plots
#' rural_urban_comparison <- analyze_isochrone_data(
#'   data_file_path = "RuralUrban_Data.csv",
#'   group_column = "urban",
#'   time_thresholds = c("access_30min", "access_60min", "access_120min"),
#'   output_dir = "results/rural_urban",
#'   create_plots = FALSE,
#'   calculate_trends = TRUE,
#'   verbose = TRUE
#' )
#' # Analysis focuses on comparing access between rural and urban areas
#' # without generating visualization plots
#'
#' @importFrom dplyr filter select group_by summarize mutate arrange n
#' @importFrom tidyr pivot_longer pivot_wider
#' @importFrom readr read_csv write_csv
#' @importFrom ggplot2 ggplot aes geom_bar geom_line geom_point facet_wrap theme_minimal labs theme ggsave
#' @importFrom stringr str_replace str_extract
#' @importFrom stats lm coef
#' @importFrom logger log_info log_debug log_warn log_error
#' @export
analyze_isochrone_data <- function(data_file_path,
                                   group_column = "category",
                                   time_thresholds = c("access_30min", "access_60min", 
                                                       "access_120min", "access_180min"),
                                   output_dir = getwd(),
                                   create_plots = TRUE,
                                   calculate_trends = TRUE,
                                   verbose = FALSE) {
  
  # Setup logging based on verbose parameter
  setup_logging(verbose)
  
  # Log function inputs
  logger::log_info("Starting isochrone data analysis")
  logger::log_info("Parameters: data_file_path = %s", data_file_path)
  logger::log_info("Parameters: group_column = %s", group_column)
  logger::log_info("Parameters: time_thresholds = [%s]", 
                   paste(time_thresholds, collapse = ", "))
  logger::log_info("Parameters: output_dir = %s", output_dir)
  logger::log_info("Parameters: create_plots = %s", create_plots)
  logger::log_info("Parameters: calculate_trends = %s", calculate_trends)
  
  # Validate inputs
  validate_inputs(data_file_path, group_column, time_thresholds, output_dir)
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    logger::log_info("Created output directory: %s", output_dir)
  }
  
  # Read and process data
  isochrone_data <- read_input_data(data_file_path)
  logger::log_info("Successfully read input data with %d rows and %d columns",
                   nrow(isochrone_data), ncol(isochrone_data))
  
  # Check if required columns exist in the data
  validate_columns(isochrone_data, group_column, time_thresholds)
  
  # Generate summary statistics
  logger::log_info("Generating summary statistics")
  summary_statistics <- calculate_summary_statistics(isochrone_data, 
                                                     group_column, 
                                                     time_thresholds)
  
  logger::log_info("Summary statistics generated with %d rows", 
                   nrow(summary_statistics))
  
  # Save summary statistics
  summary_path <- file.path(output_dir, "summary_statistics.csv")
  readr::write_csv(summary_statistics, summary_path)
  logger::log_info("Summary statistics saved to: %s", summary_path)
  
  # Initialize results list
  analysis_results <- list(
    summary_stats = summary_statistics,
    trend_analysis = NULL,
    plot_paths = NULL
  )
  
  # Calculate trends if requested and if year column exists
  if (calculate_trends && "year" %in% colnames(isochrone_data)) {
    logger::log_info("Calculating temporal trends")
    trend_analysis <- calculate_trends_analysis(isochrone_data, 
                                                group_column, 
                                                time_thresholds)
    
    # Save trend analysis
    if (!is.null(trend_analysis)) {
      trend_path <- file.path(output_dir, "trend_analysis.csv")
      readr::write_csv(trend_analysis, trend_path)
      logger::log_info("Trend analysis saved to: %s", trend_path)
      analysis_results$trend_analysis <- trend_analysis
    }
  }
  
  # Create plots if requested
  if (create_plots) {
    logger::log_info("Creating visualization plots")
    plot_paths <- create_visualization_plots(isochrone_data, 
                                             group_column, 
                                             time_thresholds, 
                                             output_dir)
    
    logger::log_info("Created %d plots", length(plot_paths))
    analysis_results$plot_paths <- plot_paths
  }
  
  logger::log_info("Isochrone data analysis completed successfully")
  
  return(analysis_results)
}

#' Set up logging based on verbose parameter
#' 
#' @param verbose Logical indicating whether to provide detailed logging
#' @noRd
setup_logging <- function(verbose) {
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
    logger::log_debug("Verbose logging enabled")
  } else {
    logger::log_threshold(logger::INFO)
  }
}

#' Validate function inputs
#' 
#' @param data_file_path Path to data file
#' @param group_column Column used for grouping
#' @param time_thresholds Time threshold columns
#' @param output_dir Output directory
#' @noRd
validate_inputs <- function(data_file_path, group_column, time_thresholds, output_dir) {
  # Check if file exists
  assertthat::assert_that(
    file.exists(data_file_path),
    msg = paste("Data file does not exist:", data_file_path)
  )
  
  # Check file extension
  assertthat::assert_that(
    tools::file_ext(data_file_path) == "csv",
    msg = "Data file must be a CSV file"
  )
  
  # Check that group column and time thresholds are characters
  assertthat::assert_that(
    is.character(group_column) && length(group_column) == 1,
    msg = "group_column must be a single character string"
  )
  
  assertthat::assert_that(
    is.character(time_thresholds) && length(time_thresholds) >= 1,
    msg = "time_thresholds must be a character vector with at least one element"
  )
  
  # Check output directory
  assertthat::assert_that(
    is.character(output_dir) && length(output_dir) == 1,
    msg = "output_dir must be a single character string"
  )
  
  logger::log_debug("Input validation passed")
}

#' Read and process the input data
#' 
#' @param data_file_path Path to the data file
#' @return A tibble containing the data
#' @noRd
read_input_data <- function(data_file_path) {
  logger::log_debug("Reading data file: %s", data_file_path)
  
  # Read the CSV file
  isochrone_data <- readr::read_csv(data_file_path, show_col_types = FALSE)
  
  # Convert percentage strings to numeric if needed
  isochrone_data <- convert_percentage_columns(isochrone_data)
  
  return(isochrone_data)
}

#' Convert percentage strings to numeric values
#' 
#' @param isochrone_data Data frame with potential percentage columns
#' @return Data frame with percentage columns converted to numeric
#' @noRd
convert_percentage_columns <- function(isochrone_data) {
  # Find columns that might contain percentage strings
  for (col in colnames(isochrone_data)) {
    if (is.character(isochrone_data[[col]])) {
      # Check if the column has percentage strings
      if (any(grepl("%", isochrone_data[[col]], fixed = TRUE), na.rm = TRUE)) {
        logger::log_debug("Converting percentage column to numeric: %s", col)
        
        # Convert percentage strings to numeric
        isochrone_data[[col]] <- as.numeric(stringr::str_replace(
          isochrone_data[[col]], "%", "")) / 100
      }
    }
  }
  
  return(isochrone_data)
}

#' Validate that required columns exist in the data
#' 
#' @param isochrone_data Data frame to validate
#' @param group_column Column used for grouping
#' @param time_thresholds Time threshold columns
#' @noRd
validate_columns <- function(isochrone_data, group_column, time_thresholds) {
  # Check if group column exists
  assertthat::assert_that(
    group_column %in% colnames(isochrone_data),
    msg = paste("Group column", group_column, "not found in data")
  )
  
  # Check if time threshold columns exist
  missing_columns <- setdiff(time_thresholds, colnames(isochrone_data))
  
  if (length(missing_columns) > 0) {
    logger::log_warn("The following time threshold columns are missing: %s",
                     paste(missing_columns, collapse = ", "))
    
    # Filter out missing columns
    valid_thresholds <- intersect(time_thresholds, colnames(isochrone_data))
    
    assertthat::assert_that(
      length(valid_thresholds) > 0,
      msg = "No valid time threshold columns found in data"
    )
    
    logger::log_info("Proceeding with valid time thresholds: %s",
                     paste(valid_thresholds, collapse = ", "))
  }
  
  logger::log_debug("Column validation passed")
}

#' Calculate summary statistics
#' 
#' @param isochrone_data Data frame with isochrone data
#' @param group_column Column used for grouping
#' @param time_thresholds Time threshold columns
#' @return Data frame with summary statistics
#' @noRd
calculate_summary_statistics <- function(isochrone_data, group_column, time_thresholds) {
  logger::log_debug("Calculating summary statistics by %s", group_column)
  
  # Filter to only valid time thresholds that exist in the data
  valid_thresholds <- intersect(time_thresholds, colnames(isochrone_data))
  
  # Convert data to long format for easier processing
  long_data <- tidyr::pivot_longer(
    isochrone_data,
    cols = dplyr::all_of(valid_thresholds),
    names_to = "time_threshold",
    values_to = "access_value"
  )
  
  logger::log_debug("Converted data to long format with %d rows", nrow(long_data))
  
  # Calculate summary statistics
  summary_stats <- long_data %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(group_column)), .data$time_threshold) %>%
    dplyr::summarize(
      mean_access = mean(.data$access_value, na.rm = TRUE),
      median_access = stats::median(.data$access_value, na.rm = TRUE),
      min_access = min(.data$access_value, na.rm = TRUE),
      max_access = max(.data$access_value, na.rm = TRUE),
      sd_access = stats::sd(.data$access_value, na.rm = TRUE),
      n_observations = dplyr::n(),
      .groups = "drop"
    )
  
  logger::log_debug("Generated summary statistics with dimensions: %d x %d", 
                    nrow(summary_stats), ncol(summary_stats))
  
  return(summary_stats)
}

#' Calculate trend analysis if year data is available
#' 
#' @param isochrone_data Data frame with isochrone data
#' @param group_column Column used for grouping
#' @param time_thresholds Time threshold columns
#' @return Data frame with trend analysis results
#' @noRd
calculate_trends_analysis <- function(isochrone_data, group_column, time_thresholds) {
  if (!"year" %in% colnames(isochrone_data)) {
    logger::log_warn("Year column not found in data. Cannot calculate trends.")
    return(NULL)
  }
  
  logger::log_debug("Calculating trend analysis")
  
  # Filter to only valid time thresholds that exist in the data
  valid_thresholds <- intersect(time_thresholds, colnames(isochrone_data))
  
  # Convert data to long format for processing
  long_data <- tidyr::pivot_longer(
    isochrone_data,
    cols = dplyr::all_of(valid_thresholds),
    names_to = "time_threshold",
    values_to = "access_value"
  )
  
  # Initialize results data frame
  trend_results <- data.frame()
  
  # For each group and time threshold, calculate trend
  for (group_val in unique(long_data[[group_column]])) {
    for (threshold in unique(long_data$time_threshold)) {
      # Filter data for this group and threshold
      filtered_data <- long_data %>%
        dplyr::filter(.data[[group_column]] == group_val, 
                      .data$time_threshold == threshold)
      
      # Only calculate trends if we have enough data points
      if (nrow(filtered_data) >= 3) {
        # Fit linear model
        trend_model <- stats::lm(access_value ~ year, data = filtered_data)
        
        # Extract trend coefficient
        trend_coef <- stats::coef(trend_model)[2]
        
        # Get R-squared
        r_squared <- summary(trend_model)$r.squared
        
        # Create row with results
        result_row <- data.frame(
          group = group_val,
          time_threshold = threshold,
          slope = trend_coef,
          r_squared = r_squared,
          p_value = summary(trend_model)$coefficients[2, 4],
          start_year = min(filtered_data$year, na.rm = TRUE),
          end_year = max(filtered_data$year, na.rm = TRUE),
          start_value = filtered_data %>%
            dplyr::filter(.data$year == min(.data$year, na.rm = TRUE)) %>%
            dplyr::pull(.data$access_value) %>%
            mean(na.rm = TRUE),
          end_value = filtered_data %>%
            dplyr::filter(.data$year == max(.data$year, na.rm = TRUE)) %>%
            dplyr::pull(.data$access_value) %>%
            mean(na.rm = TRUE)
        )
        
        # Add results to the output data frame
        trend_results <- rbind(trend_results, result_row)
      } else {
        logger::log_warn("Not enough data points to calculate trend for %s=%s, %s",
                         group_column, group_val, threshold)
      }
    }
  }
  
  # Rename group column to match input
  if (nrow(trend_results) > 0) {
    names(trend_results)[names(trend_results) == "group"] <- group_column
    
    logger::log_debug("Generated trend analysis with dimensions: %d x %d", 
                      nrow(trend_results), ncol(trend_results))
  } else {
    logger::log_warn("No trend results generated. Insufficient data.")
    return(NULL)
  }
  
  return(trend_results)
}

#' Create visualization plots
#' 
#' @param isochrone_data Data frame with isochrone data
#' @param group_column Column used for grouping
#' @param time_thresholds Time threshold columns
#' @param output_dir Output directory
#' @return Character vector with paths to saved plots
#' @noRd
create_visualization_plots <- function(isochrone_data, group_column, time_thresholds, output_dir) {
  logger::log_debug("Creating visualization plots")
  
  # Filter to only valid time thresholds that exist in the data
  valid_thresholds <- intersect(time_thresholds, colnames(isochrone_data))
  
  # Convert data to long format for plotting
  long_data <- tidyr::pivot_longer(
    isochrone_data,
    cols = dplyr::all_of(valid_thresholds),
    names_to = "time_threshold",
    values_to = "access_value"
  )
  
  # Initialize vector to store plot paths
  plot_paths <- character()
  
  # Plot 1: Bar chart of mean access values by group
  if (nrow(long_data) > 0) {
    logger::log_debug("Creating bar chart of mean access values by group")
    
    mean_access_by_group <- long_data %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(group_column)), .data$time_threshold) %>%
      dplyr::summarize(
        mean_access = mean(.data$access_value, na.rm = TRUE),
        .groups = "drop"
      )
    
    # Create plot
    bar_plot <- ggplot2::ggplot(mean_access_by_group, 
                                ggplot2::aes(x = .data[[group_column]], 
                                             y = .data$mean_access, 
                                             fill = .data$time_threshold)) +
      ggplot2::geom_bar(stat = "identity", position = "dodge") +
      ggplot2::labs(
        title = "Mean Access Values by Group and Time Threshold",
        x = group_column,
        y = "Mean Access Value",
        fill = "Time Threshold"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
    
    # Save plot
    bar_plot_path <- file.path(output_dir, "mean_access_by_group.png")
    ggplot2::ggsave(bar_plot_path, bar_plot, width = 10, height = 6)
    logger::log_info("Bar chart saved to: %s", bar_plot_path)
    
    plot_paths <- c(plot_paths, bar_plot_path)
  }
  
  # Plot 2: Time series plot if year column exists
  if ("year" %in% colnames(long_data) && length(unique(long_data$year)) > 1) {
    logger::log_debug("Creating time series plot of access values over years")
    
    yearly_means <- long_data %>%
      dplyr::group_by(.data$year, 
                      dplyr::across(dplyr::all_of(group_column)), 
                      .data$time_threshold) %>%
      dplyr::summarize(
        mean_access = mean(.data$access_value, na.rm = TRUE),
        .groups = "drop"
      )
    
    # Create plot
    time_plot <- ggplot2::ggplot(yearly_means, 
                                 ggplot2::aes(x = .data$year, 
                                              y = .data$mean_access, 
                                              color = .data[[group_column]], 
                                              group = .data[[group_column]])) +
      ggplot2::geom_line() +
      ggplot2::geom_point() +
      ggplot2::facet_wrap(~ time_threshold) +
      ggplot2::labs(
        title = "Access Values Over Time by Group and Time Threshold",
        x = "Year",
        y = "Mean Access Value",
        color = group_column
      ) +
      ggplot2::theme_minimal()
    
    # Save plot
    time_plot_path <- file.path(output_dir, "access_over_time.png")
    ggplot2::ggsave(time_plot_path, time_plot, width = 10, height = 6)
    logger::log_info("Time series plot saved to: %s", time_plot_path)
    
    plot_paths <- c(plot_paths, time_plot_path)
  }
  
  # Plot 3: Boxplot of access values by group
  if (nrow(long_data) > 0) {
    logger::log_debug("Creating boxplot of access value distribution by group")
    
    box_plot <- ggplot2::ggplot(long_data, 
                                ggplot2::aes(x = .data[[group_column]], 
                                             y = .data$access_value, 
                                             fill = .data$time_threshold)) +
      ggplot2::geom_boxplot() +
      ggplot2::labs(
        title = "Distribution of Access Values by Group",
        x = group_column,
        y = "Access Value",
        fill = "Time Threshold"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
    
    # Save plot
    box_plot_path <- file.path(output_dir, "access_distribution.png")
    ggplot2::ggsave(box_plot_path, box_plot, width = 10, height = 6)
    logger::log_info("Boxplot saved to: %s", box_plot_path)
    
    plot_paths <- c(plot_paths, box_plot_path)
  }
  
  return(plot_paths)
}


##
if (FALSE) {
library(tidyverse)

# Custom function to clean percentage columns
clean_percentage_column <- function(column) {
  # Extract numeric values from strings like "72362517 (44.49%)"
  as.numeric(stringr::str_extract(column, "^\\d+"))
}

# Read and clean the data first
cleaned_data <- read_csv("data/Tannous/Access_Data.csv") %>%
  mutate(
    access_30min_cleaned = clean_percentage_column(access_30min),
    access_60min_cleaned = clean_percentage_column(access_60min),
    access_120min_cleaned = clean_percentage_column(access_120min),
    access_180min_cleaned = clean_percentage_column(access_180min)
  ) %>%
  select(-`...8`)  # Remove the empty column

# Write the cleaned data to a new CSV
write_csv(cleaned_data, "data/Tannous/cleaned_access_data.csv")

# Now use the cleaned CSV in the function
isochrone_results <- analyze_isochrone_data(
  data_file_path = "data/Tannous/cleaned_access_data.csv",
  group_column = "category",
  time_thresholds = c("access_30min_cleaned", "access_60min_cleaned", 
                      "access_120min_cleaned", "access_180min_cleaned"),
  output_dir = "figures/",
  create_plots = TRUE,
  calculate_trends = TRUE,
  verbose = TRUE
)


isochrone_results$summary_stats
#   1. Summary Statistics (`$summary_stats`):
#   - The dataset covers multiple demographic categories (total_female, total_female_aian, total_female_asian, etc.)
# - For each category, it provides mean, median, min, max, and standard deviation of access values across different time thresholds (30, 60, 120, and 180 minutes)
# - For example, for "total_female" category:
#   * 30-min access: mean of 63,511,863 people
# * 180-min access: mean of 146,853,211 people
# 

isochrone_results$trend_analysis
# 2. Trend Analysis (`$trend_analysis`):
#   - Analyzes how access changes from 2013 to 2022
# - Includes slope (trend direction), R-squared (fit of the trend), and p-value
# - Some notable trends:
#   * Total female white population shows significant declines in access across all time thresholds
# * Some categories like total_female_asian show significant increases in longer-time thresholds

##################
# Potential explanations could include:
# Demographic shifts
# Changes in transportation infrastructure
# Migration patterns
# Socioeconomic transformations
# Differential urban/rural development
}

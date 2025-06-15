#' @title Analyze Racial Disparities in Accessibility Ratios
#' @description Analyzes and visualizes accessibility ratios across racial groups,
#'   comparing the proportion of accessible population at different time intervals.
#' @param data_path Character string specifying path to isochrone data file.
#'   Default is "Organized_Data_by_Isochrone_Range.csv".
#' @param output_dir Character string specifying directory for saving outputs.
#'   Default is "results".
#' @param min_threshold_minutes Integer specifying the lower time threshold in minutes.
#'   Default is 30 (1800 seconds).
#' @param max_threshold_minutes Integer specifying the upper time threshold in minutes.
#'   Default is 180 (10800 seconds).
#' @param create_plots Logical indicating whether to create visualization plots.
#'   Default is TRUE.
#' @param verbose Logical indicating whether to print detailed log messages.
#'   Default is FALSE.
#' @return A list containing the analyzed accessibility data and calculated ratios.
#' @importFrom dplyr filter mutate select arrange group_by summarize left_join
#' @importFrom tidyr pivot_wider pivot_longer
#' @importFrom ggplot2 ggplot aes geom_bar geom_text theme_minimal labs theme
#'   element_text scale_fill_viridis_d
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_debug log_success log_error
#' @importFrom readr read_csv write_csv
#' @examples
#' # Example 1: Basic usage with default parameters
#' accessibility_results <- analyze_racial_accessibility_disparities(
#'   data_path = "Organized_Data_by_Isochrone_Range.csv",
#'   output_dir = "results",
#'   min_threshold_minutes = 30,
#'   max_threshold_minutes = 180,
#'   create_plots = TRUE,
#'   verbose = FALSE
#' )
#' # Output:
#' # A list containing accessibility ratio data and statistics
#' 
#' # Example 2: Analyze with different time thresholds
#' accessibility_results <- analyze_racial_accessibility_disparities(
#'   data_path = "Organized_Data_by_Isochrone_Range.csv",
#'   output_dir = "results/60min_analysis",
#'   min_threshold_minutes = 60,
#'   max_threshold_minutes = 180,
#'   create_plots = TRUE,
#'   verbose = TRUE
#' )
#' # Output:
#' # Detailed log messages + analysis using 60 min threshold
#' 
#' # Example 3: Run only data analysis for the most recent year
#' accessibility_stats <- analyze_racial_accessibility_disparities(
#'   data_path = "Organized_Data_by_Isochrone_Range.csv",
#'   output_dir = "results/latest_year",
#'   min_threshold_minutes = 30,
#'   max_threshold_minutes = 180,
#'   create_plots = FALSE,
#'   verbose = TRUE
#' )
#' # Output:
#' # Statistical analysis for most recent year without plots
analyze_racial_accessibility_disparities <- function(data_path = "Organized_Data_by_Isochrone_Range.csv",
                                                     output_dir = "results",
                                                     min_threshold_minutes = 30,
                                                     max_threshold_minutes = 180,
                                                     create_plots = TRUE,
                                                     verbose = FALSE) {
  # Setup logging based on verbose parameter
  if (verbose) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::INFO)
  }
  
  logger::log_info("Starting racial accessibility disparity analysis")
  logger::log_debug("Function parameters: data_path={data_path}, output_dir={output_dir}, 
                    min_threshold={min_threshold_minutes}, max_threshold={max_threshold_minutes},
                    create_plots={create_plots}, verbose={verbose}")
  
  # Validate inputs
  assertthat::assert_that(is.character(data_path))
  assertthat::assert_that(is.character(output_dir))
  assertthat::assert_that(is.numeric(min_threshold_minutes))
  assertthat::assert_that(is.numeric(max_threshold_minutes))
  assertthat::assert_that(is.logical(create_plots))
  assertthat::assert_that(is.logical(verbose))
  
  # Convert minutes to seconds for column names
  min_seconds <- min_threshold_minutes * 60
  max_seconds <- max_threshold_minutes * 60
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    logger::log_info("Created output directory: {output_dir}")
  }
  
  # Read and process data
  accessibility_data <- read_and_validate_isochrone_data(data_path, verbose)
  
  # Filter to only include the most recent year if multiple years exist
  most_recent_year <- max(accessibility_data$year)
  latest_data <- accessibility_data %>%
    dplyr::filter(year == most_recent_year)
  
  logger::log_info("Analyzing data for year {most_recent_year}")
  
  # Calculate accessibility ratios
  accessibility_ratios <- calculate_isochrone_ratios(
    latest_data, 
    min_seconds, 
    max_seconds, 
    verbose
  )
  
  # Compare ratios to white population
  comparison_data <- compare_to_white_population(accessibility_ratios, verbose)
  
  # Create visualizations if requested
  if (create_plots) {
    plot_data <- create_visualizations(
      comparison_data, 
      output_dir, 
      min_threshold_minutes,
      max_threshold_minutes,
      verbose
    )
    logger::log_success("Visualizations created and saved to {output_dir}")
  }
  
  # Save results
  output_file <- file.path(output_dir, "racial_accessibility_analysis.csv")
  readr::write_csv(comparison_data, output_file)
  logger::log_info("Analysis results saved to {output_file}")
  
  # Return analysis results
  logger::log_success("Racial accessibility analysis completed successfully")
  return(comparison_data)
}

#' @noRd
read_and_validate_isochrone_data <- function(data_path, verbose) {
  logger::log_info("Reading data from {data_path}")
  
  # Check if file exists
  if (!file.exists(data_path)) {
    logger::log_error("File not found: {data_path}")
    potential_files <- list.files(pattern = "\\.csv$", recursive = TRUE)
    
    if (length(potential_files) > 0) {
      logger::log_info("Found potential data files: {paste(potential_files, collapse = ', ')}")
    }
    
    stop("File not found. See log for alternatives.")
  }
  
  # Read the data file
  tryCatch({
    isochrone_data <- readr::read_csv(data_path)
    
    # Check for required columns
    required_columns <- c("year", "race", "pop_1800", "pop_3600", "pop_7200", "pop_10800")
    missing_columns <- setdiff(required_columns, colnames(isochrone_data))
    
    if (length(missing_columns) > 0) {
      logger::log_error("Missing required columns: {paste(missing_columns, collapse = ', ')}")
      stop("Missing required columns in the data file")
    }
    
    # Validate data types
    assertthat::assert_that(is.data.frame(isochrone_data))
    
    # Clean race column if needed
    isochrone_data <- isochrone_data %>%
      dplyr::mutate(race = as.character(race))
    
    if (verbose) {
      logger::log_debug("Data summary: {nrow(isochrone_data)} rows and {ncol(isochrone_data)} columns")
      logger::log_debug("Years: {paste(unique(isochrone_data$year), collapse = ', ')}")
      logger::log_debug("Race groups: {paste(unique(isochrone_data$race), collapse = ', ')}")
    }
    
    return(isochrone_data)
    
  }, error = function(e) {
    logger::log_error("Error reading data: {e$message}")
    stop("Failed to read or validate data")
  })
}

#' @noRd
calculate_isochrone_ratios <- function(isochrone_data, min_seconds, max_seconds, verbose) {
  logger::log_info("Calculating accessibility ratios by racial group")
  
  # Determine column names based on seconds
  min_col <- paste0("pop_", min_seconds)
  max_col <- paste0("pop_", max_seconds)
  
  # Ensure columns exist
  if (!min_col %in% colnames(isochrone_data) || !max_col %in% colnames(isochrone_data)) {
    logger::log_error("Required time threshold columns not found")
    available_thresholds <- grep("pop_", colnames(isochrone_data), value = TRUE)
    logger::log_debug("Available time thresholds: {paste(available_thresholds, collapse = ', ')}")
    
    # Use closest available thresholds
    min_thresholds <- as.numeric(gsub("pop_", "", available_thresholds))
    min_col <- paste0("pop_", min_thresholds[which.min(abs(min_thresholds - min_seconds))])
    max_col <- paste0("pop_", min_thresholds[which.min(abs(min_thresholds - max_seconds))])
    
    logger::log_info("Using closest available thresholds: {min_col} and {max_col}")
  }
  
  # Calculate ratios
  accessibility_ratios <- isochrone_data %>%
    dplyr::mutate(
      min_threshold_pop = .data[[min_col]],
      max_threshold_pop = .data[[max_col]],
      accessibility_ratio = min_threshold_pop / max_threshold_pop * 100,
      race = factor(race)
    ) %>%
    dplyr::select(race, min_threshold_pop, max_threshold_pop, accessibility_ratio)
  
  logger::log_debug("Calculated accessibility ratios for each racial group")
  
  # Verify results
  assertthat::assert_that(nrow(accessibility_ratios) > 0)
  
  if (verbose) {
    logger::log_debug("Accessibility ratios summary:")
    for (group in unique(accessibility_ratios$race)) {
      ratio <- accessibility_ratios %>% 
        dplyr::filter(race == group) %>% 
        dplyr::pull(accessibility_ratio)
      logger::log_debug("  {group}: {round(ratio, 1)}%")
    }
  }
  
  return(accessibility_ratios)
}

#' @noRd
compare_to_white_population <- function(accessibility_ratios, verbose) {
  logger::log_info("Comparing accessibility ratios to white population")
  
  # Check for white population data
  white_labels <- c("White", "white", "WHITE", "Wht", "W")
  white_found <- FALSE
  
  for (label in white_labels) {
    if (label %in% accessibility_ratios$race) {
      white_label <- label
      white_found <- TRUE
      break
    }
  }
  
  if (!white_found) {
    logger::log_warning("No white population found for comparison")
    logger::log_debug("Available racial groups: {paste(accessibility_ratios$race, collapse = ', ')}")
    
    # Use the first group as baseline if no white group found
    white_label <- accessibility_ratios$race[1]
    logger::log_info("Using {white_label} as baseline for comparison")
  }
  
  # Get white population ratio as baseline
  white_ratio <- accessibility_ratios %>%
    dplyr::filter(race == white_label) %>%
    dplyr::pull(accessibility_ratio)
  
  logger::log_debug("{white_label} population accessibility ratio: {round(white_ratio, 1)}%")
  
  # Calculate percentage differences compared to white population
  comparison_data <- accessibility_ratios %>%
    dplyr::mutate(
      white_ratio = white_ratio,
      difference_from_white = accessibility_ratio - white_ratio,
      percent_higher = (accessibility_ratio / white_ratio - 1) * 100
    ) %>%
    dplyr::arrange(desc(percent_higher))
  
  logger::log_debug("Calculated percentage differences from {white_label} population baseline")
  
  # Log the comparison results
  if (verbose) {
    logger::log_debug("Racial disparities in accessibility ratios:")
    for (i in 1:nrow(comparison_data)) {
      row <- comparison_data[i, ]
      if (row$race != white_label) {
        logger::log_debug("  {row$race}: {round(row$percent_higher, 1)}% higher than {white_label} population")
      }
    }
  }
  
  return(comparison_data)
}

#' @noRd
create_visualizations <- function(comparison_data, output_dir, min_minutes, max_minutes, verbose) {
  logger::log_info("Creating visualizations of racial accessibility disparities")
  
  # Create bar chart of accessibility ratios
  ratio_plot <- ggplot2::ggplot(comparison_data, ggplot2::aes(x = reorder(race, -accessibility_ratio), 
                                                              y = accessibility_ratio,
                                                              fill = race)) +
    ggplot2::geom_bar(stat = "identity") +
    ggplot2::geom_text(ggplot2::aes(label = sprintf("%.1f%%", accessibility_ratio)), 
                       vjust = -0.5) +
    ggplot2::theme_minimal() +
    ggplot2::labs(
      title = paste0(min_minutes, "-Minute Accessibility Ratio by Racial Group"),
      subtitle = paste0("Proportion of accessible population at ", min_minutes, 
                        " minutes vs. ", max_minutes, " minutes"),
      x = "Racial Group",
      y = "Accessibility Ratio (%)",
      caption = "Higher percentages indicate greater local accessibility"
    ) +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
      legend.position = "none"
    ) +
    ggplot2::scale_fill_viridis_d()
  
  # Create bar chart for comparison to white population
  # Find white label
  white_labels <- c("White", "white", "WHITE", "Wht", "W")
  white_label <- NULL
  
  for (label in white_labels) {
    if (label %in% comparison_data$race) {
      white_label <- label
      break
    }
  }
  
  if (!is.null(white_label)) {
    comparison_plot_data <- comparison_data %>%
      dplyr::filter(race != white_label)
    
    comparison_plot <- comparison_plot_data %>%
      ggplot2::ggplot(ggplot2::aes(x = reorder(race, -percent_higher), 
                                   y = percent_higher,
                                   fill = race)) +
      ggplot2::geom_bar(stat = "identity") +
      ggplot2::geom_text(ggplot2::aes(label = sprintf("+%.1f%%", percent_higher)), 
                         vjust = -0.5) +
      ggplot2::theme_minimal() +
      ggplot2::labs(
        title = paste0("Accessibility Disparity Compared to ", white_label, " Population"),
        subtitle = paste0("How much higher each group's ", min_minutes, "-minute accessibility ratio is"),
        x = "Racial Group",
        y = paste0("Percent Higher than ", white_label, " Population (%)"),
        caption = "Comparison of accessibility ratios across racial groups"
      ) +
      ggplot2::theme(
        axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
        legend.position = "none"
      ) +
      ggplot2::scale_fill_viridis_d()
    
    # Save comparison plot
    comparison_plot_path <- file.path(output_dir, "accessibility_comparison_to_white.png")
    ggplot2::ggsave(comparison_plot_path, comparison_plot, width = 10, height = 7)
    logger::log_debug("Comparison plot saved to {comparison_plot_path}")
  } else {
    logger::log_warning("No white population reference found, skipping comparison plot")
  }
  
  # Save ratio plot
  ratio_plot_path <- file.path(output_dir, "accessibility_ratios_by_race.png")
  ggplot2::ggsave(ratio_plot_path, ratio_plot, width = 10, height = 7)
  logger::log_debug("Ratio plot saved to {ratio_plot_path}")
  
  return(list(ratio_plot = ratio_plot))
}



# Example usage with Organized_Data_by_Isochrone_Range.csv
accessibility_results <- analyze_racial_accessibility_disparities(
  data_path = "data/Tannous/Organized_Data_by_Isochrone_Range.csv",
  output_dir = "results",
  min_threshold_minutes = 30,  # 1800 seconds
  max_threshold_minutes = 180, # 10800 seconds
  create_plots = TRUE,
  verbose = TRUE
)


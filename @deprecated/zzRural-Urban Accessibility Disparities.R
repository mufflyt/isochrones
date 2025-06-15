#' Analyze Rural-Urban Accessibility Disparities
#'
#' @description
#' This function analyzes accessibility disparities between rural and urban populations
#' based on different time thresholds. It produces summary statistics, visualizations, 
#' and an optional export of analysis results.
#'
#' @param data_path Character string specifying the path to the CSV file containing
#'   rural-urban accessibility data. Must be a valid file path.
#' @param area_type_col Character string specifying the column name that identifies
#'   rural/urban areas. Default is "category".
#' @param region_id_col Character string specifying the column name that identifies
#'   regions. Default is "year".
#' @param time_thresholds Numeric vector of time thresholds (in minutes) to analyze.
#'   Default is c(30, 60, 120, 180).
#' @param export_results Logical indicating whether to export analysis results.
#'   Default is FALSE.
#' @param export_path Character string specifying the directory path for exported files.
#'   Only used when export_results is TRUE. Default is the current working directory.
#' @param verbose Logical indicating whether to display additional diagnostic messages.
#'   Default is FALSE.
#'
#' @return A list containing:
#'   \item{summary_stats}{A tibble with summary statistics by area type and time threshold}
#'   \item{gap_analysis}{A tibble with rural-urban gaps by time threshold}
#'   \item{plots}{A list of ggplot objects}
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter group_by summarize mutate arrange desc inner_join select n
#' @importFrom tidyr pivot_longer
#' @importFrom ggplot2 ggplot aes geom_col geom_point geom_line theme_minimal labs
#'   scale_y_continuous ggsave
#' @importFrom readr read_csv write_csv
#' @importFrom logger log_info log_debug log_error log_warn log_threshold INFO WARN
#' @importFrom scales percent
#'
#' @examples
#' # Basic usage with default parameters
#' rural_urban_analysis <- analyze_accessibility_disparities(
#'   data_path = "RuralUrban_Data.csv",
#'   time_thresholds = c(30, 60, 120, 180),
#'   verbose = TRUE
#' )
#' 
#' # Print the summary statistics
#' print(rural_urban_analysis$summary_stats)
#'
#' # Analyze specific time thresholds and export results
#' rural_urban_analysis <- analyze_accessibility_disparities(
#'   data_path = "RuralUrban_Data.csv",
#'   time_thresholds = c(30, 60),
#'   export_results = TRUE,
#'   export_path = "output/accessibility_analysis",
#'   verbose = TRUE
#' )
#' 
#' # Display the rural-urban gap plot
#' print(rural_urban_analysis$plots$gap_plot)
#'
#' # Focus on 30-minute threshold and export detailed analysis
#' thirty_min_analysis <- analyze_accessibility_disparities(
#'   data_path = "RuralUrban_Data.csv", 
#'   time_thresholds = c(30),
#'   export_results = TRUE,
#'   export_path = "output/thirty_min_analysis",
#'   verbose = TRUE
#' )
analyze_accessibility_disparities <- function(data_path,
                                              area_type_col = "category",
                                              region_id_col = "year",
                                              time_thresholds = c(30, 60, 120, 180),
                                              export_results = FALSE,
                                              export_path = getwd(),
                                              verbose = FALSE) {
  # Set up logging
  logger::log_threshold(if (verbose) logger::INFO else logger::WARN)
  logger::log_info("Starting accessibility disparities analysis")
  logger::log_info("Input parameters: data_path = {data_path}, time_thresholds = {paste(time_thresholds, collapse=', ')}")
  logger::log_info("Column parameters: area_type_col = {area_type_col}, region_id_col = {region_id_col}")
  logger::log_info("Export settings: export_results = {export_results}, export_path = {export_path}")
  
  # Validate inputs
  validate_inputs(data_path, area_type_col, region_id_col, time_thresholds, 
                  export_results, export_path, verbose)
  
  # Read data
  accessibility_data <- read_data(data_path, area_type_col, region_id_col, verbose)
  
  # Analyze data
  summary_stats <- compute_summary_statistics(accessibility_data, time_thresholds, verbose)
  
  # Calculate rural-urban gaps
  gap_analysis <- compute_rural_urban_gaps(summary_stats, verbose)
  
  # Create visualizations
  plots <- create_visualizations(summary_stats, gap_analysis, verbose)
  
  # Export results if requested
  if (export_results) {
    export_analysis_results(summary_stats, gap_analysis, plots, export_path, verbose)
  }
  
  # Prepare and return results
  analysis_results <- list(
    summary_stats = summary_stats,
    gap_analysis = gap_analysis,
    plots = plots
  )
  
  logger::log_info("Accessibility disparities analysis completed successfully")
  
  return(analysis_results)
}

#' @noRd
validate_inputs <- function(data_path, area_type_col, region_id_col, time_thresholds, 
                            export_results, export_path, verbose) {
  logger::log_debug("Validating input parameters")
  
  # Validate data_path
  assertthat::assert_that(is.character(data_path), 
                          msg = "data_path must be a character string")
  assertthat::assert_that(file.exists(data_path), 
                          msg = paste("File not found:", data_path))
  
  # Validate column names
  assertthat::assert_that(is.character(area_type_col),
                          msg = "area_type_col must be a character string")
  assertthat::assert_that(is.character(region_id_col),
                          msg = "region_id_col must be a character string")
  
  # Validate time_thresholds
  assertthat::assert_that(is.numeric(time_thresholds),
                          msg = "time_thresholds must be a numeric vector")
  assertthat::assert_that(length(time_thresholds) > 0,
                          msg = "time_thresholds must contain at least one value")
  assertthat::assert_that(all(time_thresholds > 0),
                          msg = "All time_thresholds must be positive")
  
  # Validate export_results
  assertthat::assert_that(is.logical(export_results),
                          msg = "export_results must be logical (TRUE/FALSE)")
  
  # Validate export_path
  if (export_results) {
    assertthat::assert_that(is.character(export_path),
                            msg = "export_path must be a character string")
    if (!dir.exists(export_path)) {
      logger::log_info("Creating export directory: {export_path}")
      dir.create(export_path, recursive = TRUE)
    }
  }
  
  # Validate verbose
  assertthat::assert_that(is.logical(verbose),
                          msg = "verbose must be logical (TRUE/FALSE)")
  
  logger::log_debug("Input validation completed successfully")
}

#' @noRd
read_data <- function(data_path, area_type_col, region_id_col, verbose) {
  logger::log_debug("Reading data from {data_path}")
  
  tryCatch({
    accessibility_data <- readr::read_csv(data_path, show_col_types = FALSE)
    
    assertthat::assert_that(is.data.frame(accessibility_data),
                            msg = "Failed to read data as a data frame")
    
    # Log available columns
    logger::log_debug("Available columns: {paste(names(accessibility_data), collapse=', ')}")
    
    # Verify essential columns exist
    assertthat::assert_that(area_type_col %in% names(accessibility_data),
                            msg = paste("Area type column not found:", area_type_col))
    assertthat::assert_that(region_id_col %in% names(accessibility_data),
                            msg = paste("Region ID column not found:", region_id_col))
    
    # Check for access time columns
    access_columns <- grep("^access_", names(accessibility_data), value = TRUE)
    assertthat::assert_that(length(access_columns) > 0,
                            msg = "No access time columns (access_*) found in data")
    
    logger::log_info("Data loaded successfully: {nrow(accessibility_data)} rows, {ncol(accessibility_data)} columns")
    logger::log_info("Time threshold columns found: {paste(access_columns, collapse=', ')}")
    
    # Return processed data with column mapping
    return(list(
      data = accessibility_data,
      area_type_col = area_type_col,
      region_id_col = region_id_col,
      access_columns = access_columns
    ))
  }, error = function(e) {
    logger::log_error("Error reading data: {conditionMessage(e)}")
    stop(paste("Error reading data:", conditionMessage(e)))
  })
}

#' @noRd
compute_summary_statistics <- function(accessibility_info, time_thresholds, verbose) {
  logger::log_debug("Computing summary statistics")
  
  # Extract data and columns from list
  accessibility_data <- accessibility_info$data
  area_type_col <- accessibility_info$area_type_col
  access_columns <- accessibility_info$access_columns
  
  # Extract relevant time threshold columns based on input
  if (!is.null(time_thresholds) && length(time_thresholds) > 0) {
    # Create column names based on threshold pattern in data
    threshold_cols <- paste0("access_", time_thresholds, "min")
    valid_columns <- intersect(threshold_cols, access_columns)
    
    if (length(valid_columns) == 0) {
      logger::log_warn("None of the specified time thresholds match column names")
      logger::log_info("Using all available access columns instead")
      valid_columns <- access_columns
    } else if (length(valid_columns) < length(threshold_cols)) {
      missing_thresholds <- setdiff(threshold_cols, valid_columns)
      logger::log_warn("Some specified time thresholds not found: {paste(missing_thresholds, collapse=', ')}")
    }
  } else {
    valid_columns <- access_columns
  }
  
  logger::log_info("Computing statistics for columns: {paste(valid_columns, collapse=', ')}")
  
  # Convert percentages to numeric values if they're stored as strings
  for (col in valid_columns) {
    if (is.character(accessibility_data[[col]])) {
      accessibility_data[[col]] <- as.numeric(gsub("[^0-9.]", "", 
                                                   accessibility_data[[col]])) / 100
      logger::log_debug("Converted string percentages to numeric values for column: {col}")
    }
  }
  
  # Prepare data for analysis - pivot longer
  accessibility_long <- tidyr::pivot_longer(
    accessibility_data,
    cols = dplyr::all_of(valid_columns),
    names_to = "time_column",
    values_to = "accessibility"
  ) %>%
    dplyr::mutate(time_threshold = as.numeric(gsub("access_|min", "", time_column)))
  
  logger::log_debug("Data transformed to long format for analysis")
  
  # Check if 'urban' column exists alongside 'category' column
  if ("urban" %in% names(accessibility_data)) {
    # If urban column is YES/NO or similar, use it directly
    if (all(unique(accessibility_data$urban) %in% c("YES", "NO", "Yes", "No", "yes", "no", "Y", "N", "TRUE", "FALSE", "True", "False", "true", "false", "1", "0"))) {
      # Create standardized area type 
      accessibility_long <- accessibility_long %>%
        dplyr::mutate(area_type = dplyr::case_when(
          tolower(urban) %in% c("yes", "y", "true", "1") ~ "Urban",
          TRUE ~ "Rural"
        ))
    } else {
      # Use the urban column as is - might contain values like "Urban", "Rural"
      accessibility_long <- accessibility_long %>%
        dplyr::rename(area_type = urban)
    }
  } else {
    # Use area_type_col
    accessibility_long <- accessibility_long %>%
      dplyr::rename(area_type = !!rlang::sym(area_type_col))
  }
  
  # Compute summary statistics by area type and time threshold
  summary_stats <- accessibility_long %>%
    dplyr::group_by(area_type, time_threshold) %>%
    dplyr::summarize(
      mean_accessibility = mean(accessibility, na.rm = TRUE),
      median_accessibility = median(accessibility, na.rm = TRUE),
      min_accessibility = min(accessibility, na.rm = TRUE),
      max_accessibility = max(accessibility, na.rm = TRUE),
      regions_below_20pct = sum(accessibility < 0.2, na.rm = TRUE),
      pct_regions_below_20pct = mean(accessibility < 0.2, na.rm = TRUE) * 100,
      sample_size = dplyr::n(),
      .groups = "drop"
    )
  
  logger::log_info("Summary statistics computed by area type and time threshold")
  
  # Check for 30-minute data
  thirty_min_stats <- summary_stats %>%
    dplyr::filter(time_threshold == 30)
  
  if (nrow(thirty_min_stats) > 0) {
    rural_30min <- thirty_min_stats %>%
      dplyr::filter(area_type == "Rural") %>%
      dplyr::pull(mean_accessibility)
    
    urban_30min <- thirty_min_stats %>%
      dplyr::filter(area_type == "Urban") %>%
      dplyr::pull(mean_accessibility)
    
    if (length(rural_30min) > 0 && length(urban_30min) > 0) {
      logger::log_info("Key finding: 30-minute accessibility - Rural: {round(rural_30min * 100, 1)}%, Urban: {round(urban_30min * 100, 1)}%")
      
      if (rural_30min < 0.2) {
        logger::log_info("Key finding: Rural areas have particularly low 30-minute accessibility (<20%)")
      }
    }
  }
  
  return(summary_stats)
}

#' @noRd
compute_rural_urban_gaps <- function(summary_stats, verbose) {
  logger::log_debug("Computing rural-urban accessibility gaps")
  
  # Check if we have both Rural and Urban data
  if (!all(c("Rural", "Urban") %in% unique(summary_stats$area_type))) {
    area_types <- unique(summary_stats$area_type)
    logger::log_warn("Expected 'Rural' and 'Urban' area types, but found: {paste(area_types, collapse=', ')}")
    
    # Try to guess which values correspond to rural/urban
    rural_candidates <- area_types[grepl("rural|country|non-urban", area_types, ignore.case = TRUE)]
    urban_candidates <- area_types[grepl("urban|city|town", area_types, ignore.case = TRUE)]
    
    rural_value <- if (length(rural_candidates) > 0) rural_candidates[1] else area_types[1]
    urban_value <- if (length(urban_candidates) > 0) urban_candidates[1] else area_types[2]
    
    logger::log_info("Using '{rural_value}' as Rural and '{urban_value}' as Urban")
  } else {
    rural_value <- "Rural"
    urban_value <- "Urban"
  }
  
  # Compute accessibility gaps between urban and rural areas
  urban_stats <- summary_stats %>%
    dplyr::filter(area_type == urban_value) %>%
    dplyr::select(time_threshold, urban_accessibility = mean_accessibility)
  
  rural_stats <- summary_stats %>%
    dplyr::filter(area_type == rural_value) %>%
    dplyr::select(time_threshold, rural_accessibility = mean_accessibility)
  
  gap_analysis <- dplyr::inner_join(urban_stats, rural_stats, 
                                    by = "time_threshold") %>%
    dplyr::mutate(
      absolute_gap = urban_accessibility - rural_accessibility,
      relative_gap = absolute_gap / urban_accessibility,
      time_threshold = factor(time_threshold)
    ) %>%
    dplyr::arrange(dplyr::desc(absolute_gap))
  
  if (nrow(gap_analysis) > 0) {
    logger::log_info("Gap analysis completed: largest gap at {gap_analysis$time_threshold[1]}-minute threshold")
    
    # Check if 30-minute threshold exists
    thirty_min_gap <- gap_analysis %>%
      dplyr::filter(time_threshold == "30") %>%
      dplyr::pull(absolute_gap)
    
    if (length(thirty_min_gap) > 0) {
      logger::log_info("30-minute absolute gap: {round(thirty_min_gap * 100, 1)}%")
    }
  } else {
    logger::log_warn("No gap analysis could be computed due to missing data")
  }
  
  return(gap_analysis)
}

#' @noRd
create_visualizations <- function(summary_stats, gap_analysis, verbose) {
  logger::log_debug("Creating visualizations")
  
  # Check if we have data to visualize
  if (nrow(summary_stats) == 0) {
    logger::log_warn("No data available to create visualizations")
    return(list())
  }
  
  # 1. Accessibility by area type and time threshold
  accessibility_plot <- ggplot2::ggplot(summary_stats, 
                                        ggplot2::aes(x = factor(time_threshold), 
                                                     y = mean_accessibility, 
                                                     fill = area_type)) +
    ggplot2::geom_col(position = "dodge") +
    ggplot2::scale_y_continuous(labels = scales::percent) +
    ggplot2::labs(
      title = "Accessibility by Area Type and Time Threshold",
      x = "Time Threshold (minutes)",
      y = "Mean Accessibility",
      fill = "Area Type"
    ) +
    ggplot2::theme_minimal()
  
  # Only create gap plot if we have gap analysis data
  plots <- list(accessibility_plot = accessibility_plot)
  
  if (nrow(gap_analysis) > 0) {
    # 2. Rural-urban gap by time threshold
    gap_plot <- ggplot2::ggplot(gap_analysis, 
                                ggplot2::aes(x = time_threshold, 
                                             y = absolute_gap)) +
      ggplot2::geom_col(fill = "steelblue") +
      ggplot2::scale_y_continuous(labels = scales::percent) +
      ggplot2::labs(
        title = "Rural-Urban Accessibility Gap by Time Threshold",
        x = "Time Threshold (minutes)",
        y = "Absolute Gap (Urban - Rural)"
      ) +
      ggplot2::theme_minimal()
    
    plots$gap_plot <- gap_plot
  }
  
  # 3. Line plot showing accessibility trends
  trend_plot <- ggplot2::ggplot(summary_stats, 
                                ggplot2::aes(x = time_threshold, 
                                             y = mean_accessibility, 
                                             color = area_type, 
                                             group = area_type)) +
    ggplot2::geom_line(size = 1) +
    ggplot2::geom_point(size = 3) +
    ggplot2::scale_y_continuous(labels = scales::percent) +
    ggplot2::labs(
      title = "Accessibility Trends by Area Type",
      x = "Time Threshold (minutes)",
      y = "Mean Accessibility",
      color = "Area Type"
    ) +
    ggplot2::theme_minimal()
  
  plots$trend_plot <- trend_plot
  
  logger::log_info("Created visualizations: {paste(names(plots), collapse=', ')}")
  
  return(plots)
}

#' @noRd
export_analysis_results <- function(summary_stats, gap_analysis, plots, 
                                    export_path, verbose) {
  logger::log_debug("Exporting analysis results")
  
  # Create timestamp for file names
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  # Export summary statistics
  summary_file <- file.path(export_path, 
                            paste0("accessibility_summary_", timestamp, ".csv"))
  readr::write_csv(summary_stats, summary_file)
  logger::log_info("Summary statistics exported to {summary_file}")
  
  # Export gap analysis if we have it
  if (nrow(gap_analysis) > 0) {
    gap_file <- file.path(export_path, 
                          paste0("rural_urban_gap_", timestamp, ".csv"))
    readr::write_csv(gap_analysis, gap_file)
    logger::log_info("Gap analysis exported to {gap_file}")
  }
  
  # Export plots
  for (plot_name in names(plots)) {
    plot_file <- file.path(export_path, 
                           paste0(plot_name, "_", timestamp, ".png"))
    ggplot2::ggsave(plot_file, plot = plots[[plot_name]], 
                    width = 10, height = 7, dpi = 300)
    logger::log_info("Plot '{plot_name}' exported to {plot_file}")
  }
  
  # Export combined report (optional)
  report_file <- file.path(export_path, 
                           paste0("accessibility_report_", timestamp, ".html"))
  
  if (requireNamespace("rmarkdown", quietly = TRUE)) {
    export_rmarkdown_report(summary_stats, gap_analysis, plots, 
                            report_file, verbose)
  } else {
    logger::log_warn("Package 'rmarkdown' not available. HTML report not created.")
  }
  
  logger::log_info("All analysis results exported successfully")
}

#' @noRd
export_rmarkdown_report <- function(summary_stats, gap_analysis, plots, 
                                    report_file, verbose) {
  # Create a temporary Rmd file
  temp_rmd <- tempfile(fileext = ".Rmd")
  
  # Build report content based on available data
  rmd_content <- c(
    "---",
    "title: Rural-Urban Accessibility Disparities Analysis",
    paste0("date: \"", Sys.Date(), "\""),
    "output: html_document",
    "---",
    "",
    "```{r setup, include=FALSE}",
    "knitr::opts_chunk$set(echo = FALSE)",
    "```",
    "",
    "## Summary Statistics",
    "",
    "```{r}",
    "knitr::kable(summary_stats)",
    "```"
  )
  
  # Only include gap analysis if available
  if (nrow(gap_analysis) > 0) {
    gap_content <- c(
      "",
      "## Rural-Urban Accessibility Gap",
      "",
      "```{r}",
      "knitr::kable(gap_analysis)",
      "```"
    )
    rmd_content <- c(rmd_content, gap_content)
  }
  
  # Add visualizations
  viz_content <- c(
    "",
    "## Visualizations",
    ""
  )
  rmd_content <- c(rmd_content, viz_content)
  
  # Add each available plot
  for (plot_name in names(plots)) {
    plot_title <- gsub("_", " ", plot_name)
    plot_title <- paste0(toupper(substr(plot_title, 1, 1)), 
                         substr(plot_title, 2, nchar(plot_title)))
    
    plot_content <- c(
      paste0("### ", plot_title),
      "",
      "```{r fig.width=10, fig.height=6}",
      paste0("plots$", plot_name),
      "```",
      ""
    )
    rmd_content <- c(rmd_content, plot_content)
  }
  
  # Write to file
  cat(rmd_content, file = temp_rmd, sep = "\n")
  
  # Render the report
  tryCatch({
    rmarkdown::render(
      input = temp_rmd,
      output_file = basename(report_file),
      output_dir = dirname(report_file),
      envir = new.env(parent = environment())
    )
    logger::log_info("Comprehensive report exported to {report_file}")
  }, error = function(e) {
    logger::log_warn("Could not create HTML report: {conditionMessage(e)}")
  })
  
  # Clean up
  unlink(temp_rmd)
}

###
rural_urban_analysis <- analyze_accessibility_disparities(
  data_path = "data/Tannous/Rural-Urban_Data.csv",
  time_thresholds = c(30, 60, 120, 180),
  verbose = TRUE
)

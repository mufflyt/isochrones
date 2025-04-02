#' Generate Summary Sentence for Race and Drive Time
#'
#' This function generates a summary sentence indicating the level of access to gynecologic oncologists for a specified race and drive time.
#' It can run for individual races or all races in the dataset.
#'
#' @param tabulated_data A data frame containing the data to analyze. Must include columns `Driving Time (minutes)`, `Year`, `total_female_026`, and columns for race proportions like `White_prop`, `Black_prop`, etc.
#' @param driving_time_minutes A numeric value specifying the driving time in minutes to filter the data. Default is 180.
#' @param race A character string specifying the race for which to generate the summary sentence. Supported values are "White", "Black", "American Indian/Alaska Native", "Asian", "Native Hawaiian or Pacific Islander", or "all" to generate sentences for all supported races. Default is "American Indian/Alaska Native".
#'
#' @return A character string containing the summary sentence, or a list of summary sentences if `race = "all"`.
#' @examples
#' # Example usage
#' summary_sentence <- race_drive_time_generate_summary_sentence(
#'   tabulated_data = tabulated_all_years_numeric,
#'   driving_time_minutes = 180,
#'   race = "American Indian/Alaska Native"
#' )
#' print(summary_sentence)
#' @import dplyr
#' @import beepr
#' @export
race_drive_time_generate_summary_sentence <- function(tabulated_data, driving_time_minutes = 180, race = "American Indian/Alaska Native") {
  # Log the function call with input arguments
  message("Function race_drive_time_generate_summary_sentence called with inputs:")
  message("Driving Time (minutes): ", driving_time_minutes)
  message("Race: ", race)

  # Define a mapping of race names to their corresponding columns in the data
  race_column_map <- list(
    "White" = "White_prop",
    "Black" = "Black_prop",
    "American Indian/Alaska Native" = "AIAN_prop",
    "Asian" = "Asian_prop",
    "Native Hawaiian or Pacific Islander" = "NHPI_prop"
  )

  # If race is "all", loop through all supported races
  if (race == "all") {
    all_summaries <- lapply(names(race_column_map), function(current_race) {
      message("Processing race: ", current_race)
      race_drive_time_generate_summary_sentence(
        tabulated_data = tabulated_data,
        driving_time_minutes = driving_time_minutes,
        race = current_race
      )
    })
    names(all_summaries) <- names(race_column_map)
    return(all_summaries)
  }

  # Check if the race is supported
  if (!race %in% names(race_column_map)) {
    stop("Unsupported race specified. Supported values are: ", paste(names(race_column_map), collapse = ", "), " or 'all'.")
  }

  # Determine the correct column for the race proportion
  race_column <- race_column_map[[race]]

  # Log the race column being used
  message("Race column used for analysis: ", race_column)

  # Validate input data
  if (!"Driving Time (minutes)" %in% names(tabulated_data)) {
    stop("The input data must contain the 'Driving Time (minutes)' column.")
  }
  if (!race_column %in% names(tabulated_data)) {
    stop("The input data must contain the column: ", race_column)
  }
  if (!"Year" %in% names(tabulated_data)) {
    stop("The input data must contain the 'Year' column.")
  }
  if (!"total_female_026" %in% names(tabulated_data)) {
    stop("The input data must contain the 'total_female_026' column.")
  }

  # Filter the dataframe to include only rows where Driving Time is as specified
  filtered_data <- tabulated_data %>%
    dplyr::filter(`Driving Time (minutes)` == driving_time_minutes)

  # Log the number of rows after filtering
  message("Number of rows after filtering for Driving Time (minutes) = ", driving_time_minutes, ": ", nrow(filtered_data))

  # Check if the filtered data is empty
  if (nrow(filtered_data) == 0) {
    stop("No data available for the specified 'Driving Time (minutes)'.")
  }

  # Progress beep
  beepr::beep(1)

  # Log variance in the race proportions
  race_variance <- var(filtered_data[[race_column]], na.rm = TRUE)
  message("Variance of ", race, " proportions across years: ", race_variance)

  # Run Kruskal-Wallis test for the specified race
  tryCatch({
    # Correctly apply the Kruskal-Wallis test to compare race proportions across years
    kruskal_test <- stats::kruskal.test(filtered_data[[race_column]] ~ as.factor(filtered_data$Year))
    p_value <- kruskal_test$p.value
  }, error = function(e) {
    stop("Error in Kruskal-Wallis test: ", e$message)
  })

  # Log the Kruskal-Wallis test result
  message("Kruskal-Wallis test completed. P-value: ", p_value)

  # Format the p-value
  formatted_p_value <- ifelse(p_value < 0.01, "<0.01", format(p_value, digits = 2))

  # Ensure the Year column is numeric
  filtered_data <- filtered_data %>%
    dplyr::mutate(Year = as.numeric(as.character(Year)))

  # Log the transformation of the Year column
  message("Year column converted to numeric.")

  # Progress beep
  beepr::beep(1)

  # Define initial and later years based on the median year
  median_year <- median(filtered_data$Year, na.rm = TRUE)
  initial_years <- filtered_data %>% dplyr::filter(Year <= median_year) %>% dplyr::pull(Year) %>% unique()
  later_years <- filtered_data %>% dplyr::filter(Year > median_year) %>% dplyr::pull(Year) %>% unique()

  # Log the median year and the initial/later year groups
  message("Median Year: ", median_year)
  message("Initial Years: ", paste(initial_years, collapse = ", "))
  message("Later Years: ", paste(later_years, collapse = ", "))

  # Calculate the median proportions for initial and later years
  median_initial <- median(filtered_data %>% dplyr::filter(Year %in% initial_years) %>% dplyr::pull(!!sym(race_column)), na.rm = TRUE)
  median_later <- median(filtered_data %>% dplyr::filter(Year %in% later_years) %>% dplyr::pull(!!sym(race_column)), na.rm = TRUE)

  # Log the calculated median proportions
  message("Median proportion for initial years: ", median_initial)
  message("Median proportion for later years: ", median_later)

  # Calculate total number of women affected
  total_women_initial <- sum(as.numeric(filtered_data$total_female_026[filtered_data$Year %in% initial_years]) * median_initial, na.rm = TRUE)
  total_women_later <- sum(as.numeric(filtered_data$total_female_026[filtered_data$Year %in% later_years]) * median_later, na.rm = TRUE)

  # Format the numbers with thousandths commas
  total_women_initial_formatted <- format(round(total_women_initial), big.mark = ",")
  total_women_later_formatted <- format(round(total_women_later), big.mark = ",")

  # Log the total number of women affected
  message("Total women affected in initial years: ", total_women_initial_formatted)
  message("Total women affected in later years: ", total_women_later_formatted)

  # Progress beep
  beepr::beep(1)

  # Determine the direction of change (improved, worsened, or stayed the same)
  change_direction <- ifelse(median_later > median_initial, "improved",
                             ifelse(median_later < median_initial, "worsened", "stayed the same"))

  # Determine if the number has increased, decreased, or stayed the same
  change_direction_number <- ifelse(total_women_later > total_women_initial, "more",
                                    ifelse(total_women_later < total_women_initial, "less", "the same number of"))

  # Extract the drive time value in hours
  drive_time_hours <- driving_time_minutes / 60

  # Combine the summary sentence
  summary_sentence <- paste(
    "For example, approximately",
    round(median_initial * 100, 2), "% of", race, "women did not have access to a physician within a", drive_time_hours, "hour drive from",
    paste(min(initial_years), max(initial_years), sep = "-"), ". This",
    change_direction, "to",
    round(median_later * 100, 2), "% by",
    paste(min(later_years), max(later_years), sep = "-"), "(p-value =", formatted_p_value, ").",
    "This change represents approximately",
    total_women_initial_formatted, "to",
    total_women_later_formatted, change_direction_number,
    race, "women receiving care from a GYN oncologist."
  )

  # Log the generated summary sentence
  message("Generated summary sentence: ", summary_sentence)

  # Completion beep
  beepr::beep(2)

  # Return the summary sentence
  return(summary_sentence)
}

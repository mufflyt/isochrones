summarize_state_counts <- function(file_path = "state_data.csv") {
  data <- readr::read_csv(file_path, show_col_types = FALSE)
  total <- sum(data$count, na.rm = TRUE)
  average <- mean(data$count, na.rm = TRUE)
  median <- stats::median(data$count, na.rm = TRUE)
  tibble::tibble(
    total_count = total,
    average_count = average,
    median_count = median
  )
}

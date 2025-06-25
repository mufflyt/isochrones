#' @title Format a numeric vector as a percentage string
#' @description Wraps `formatC()` to create a character vector with a fixed
#'   number of decimal places. Primarily used when reporting percentages but
#'   works with any numeric input.
#'
#' @param x Numeric vector to format.
#' @param my_digits Integer number of digits to display after the decimal
#'   point (default `0`).
#'
#' @return Character vector with the formatted values.
#'
#' @family formatting
#'
#' @examples
#' format_pct(0.1234, my_digits = 2)
#' format_pct(c(0.1, 0.25), my_digits = 1)
#'
#' @export
format_pct <- function(x, my_digits = 0) {
  formatC(x, format = "f", digits = my_digits)
}

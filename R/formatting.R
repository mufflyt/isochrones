#' Format a numeric vector as a percentage string
#'
#' This helper simply wraps `formatC()` to return a character vector with a
#' fixed number of decimal places.  It is primarily used in reporting
#' percentages but works with any numeric input.
#'
#' @param x Numeric vector to format.
#' @param my_digits Integer number of digits to display after the decimal
#'   point. Defaults to `0`.
#'
#' @return A character vector with the formatted values.
#'
#' @examples
#' format_pct(0.1234, my_digits = 2)
#' format_pct(c(0.1, 0.25), my_digits = 1)
format_pct <- function(x, my_digits = 0) {
  formatC(x, format = "f", digits = my_digits)
}

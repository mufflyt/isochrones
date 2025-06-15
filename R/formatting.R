#' Format a number with a fixed number of digits
#'
#' @param x Numeric vector to format
#' @param my_digits Integer number of digits to show
#' @return Character vector with formatted numbers
#' @examples
#' format_pct(0.1234, my_digits = 2)
format_pct <- function(x, my_digits = 0) {
  format(x, digits = my_digits, nsmall = my_digits)
}

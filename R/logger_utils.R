#' Safe glue-based log formatter
#'
#' Uses `glue::glue` for string interpolation but falls back to the
#' raw message if interpolation fails. This prevents logging from
#' aborting when variables referenced in braces are missing.
formatter_glue_safe <- function(...,
                               .logcall = sys.call(),
                               .topcall = sys.call(-1),
                               .topenv = parent.frame()) {
  tryCatch(
    glue::glue(..., .envir = .topenv),
    error = function(e) paste0(...)
  )
}

#' Format numbers with thousands separators
#'
#' A small helper wrapper around `scales::comma` that returns a
#' character representation of a numeric value with commas for
#' thousands. Useful for making large values easier to read in
#' log messages.
#'
#' @param x Numeric vector to format
#' @return A character vector of formatted numbers
#' @export
format_with_commas <- function(x) {
  assertthat::assert_that(is.numeric(x),
                          msg = "x must be numeric for formatting with commas")

  return(scales::comma(x))
}

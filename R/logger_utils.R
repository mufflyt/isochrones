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

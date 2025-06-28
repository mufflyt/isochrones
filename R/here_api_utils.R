#' Initialize HERE API Key
#'
#' Sets the HERE API key used by the `hereR` package. The key is stored in the
#' `HERE_API_KEY` environment variable and loaded into the current session.  This
#' helper is mainly a convenience wrapper around `hereR::set_key()` with a small
#' amount of validation and optional logging.
#'
#' @param api_key Character string containing the HERE API key. If omitted, the
#'   function looks for a `HERE_API_KEY` environment variable.
#' @param verbose Logical. If `TRUE`, emit a message via the logger when the key
#'   is set.
#'
#' @examples
#' \dontrun{
#' initialize_here_api_key("my_api_key")
#' }
#' @export
initialize_here_api_key <- function(api_key = Sys.getenv("HERE_API_KEY"), verbose = FALSE) {
  if (!nzchar(api_key)) {
    stop("HERE API key must be provided via argument or HERE_API_KEY env var.")
  }

  Sys.setenv(HERE_API_KEY = api_key)
  hereR::set_key(api_key)

  if (verbose) {
    logger::log_info("HERE API key successfully initialized")
  }

  invisible(api_key)
}

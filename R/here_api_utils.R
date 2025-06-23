#' Initialize HERE API Key
#'
#' @param api_key Character. The HERE API key. Defaults to `HERE_API_KEY` env var.
#' @param verbose Logical. If TRUE, log additional information.
#' @export
initialize_here_api_key <- function(api_key = Sys.getenv("HERE_API_KEY"), verbose = FALSE) {
  if (is.null(api_key) || api_key == "") {
    stop("HERE API key must be provided via argument or HERE_API_KEY env var.")
  }

  Sys.setenv(HERE_API_KEY = api_key)
  hereR::set_key(api_key)

  if (verbose) {
    logger::log_info("HERE API key successfully initialized")
  }
}

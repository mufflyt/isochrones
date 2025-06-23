# Utility functions for API keys
#' Retrieve an environment variable or stop with an informative error
#'
#' @param var Name of the environment variable
#' @param msg Optional custom message
#' @param verbose Logical. If TRUE, log additional information.
#' @return The value of the environment variable
#' @examples
#' api_key <- get_env_or_stop("HERE_API_KEY")
get_env_or_stop <- function(var, msg = NULL, verbose = FALSE) {
  if (verbose) {
    logger::log_info("Retrieving environment variable '%s'", var)
  }

  val <- Sys.getenv(var)

  if (val == "") {
    if (is.null(msg)) {
      msg <- paste0(var, " environment variable is not set. Please add it to your .Renviron or .env file")
    }
    stop(msg, call. = FALSE)
  }

  if (verbose) {
    logger::log_info("Successfully retrieved environment variable '%s'", var)
  }

  val
}

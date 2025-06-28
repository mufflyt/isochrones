# Utility functions for API keys
#' Retrieve an environment variable or throw a helpful error
#'
#' This utility fetches the value of an environment variable and stops with a
#' clear message if the variable is not set. It is useful for obtaining API
#' keys that are expected to be configured in a `.Renviron` or `.env` file.
#'
#' @param var Name of the environment variable to retrieve.
#' @param msg Optional custom message to display if the variable is missing.
#' @param verbose Logical. If `TRUE`, log additional information about the
#'   retrieval process.
#'
#' @return The value of the requested environment variable.
#'
#' @examples
#' api_key <- get_env_or_stop("HERE_API_KEY")
get_env_or_stop <- function(var, msg = NULL, verbose = FALSE) {
  if (verbose) {
    logger::log_info("Retrieving environment variable '%s'", var)
  }

  val <- Sys.getenv(var)

  if (!nzchar(val)) {
    if (is.null(msg)) {
      msg <- paste0(var, " environment variable is not set. Please add it to your .Renviron or .env file")
    }
    stop(msg, call. = FALSE)
  }

  if (verbose) {
    logger::log_info("Successfully retrieved environment variable '%s'", var)
  }

  return(val)
}

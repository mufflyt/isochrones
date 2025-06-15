initialize_here_api_key <- function(api_key = Sys.getenv("HERE_API_KEY")) {
  if (is.null(api_key) || api_key == "") {
    stop("HERE API key must be provided via argument or HERE_API_KEY env var.")
  }
  Sys.setenv(HERE_API_KEY = api_key)
  hereR::set_key(api_key)
}

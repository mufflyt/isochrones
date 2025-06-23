#' Memoized function to try a location with geocode
#' #'
#' This function calculates isolines for a given location using the hereR package.
#'
#' @param location An sf object representing the location for which isolines will be calculated.
#' @param range A numeric vector of time ranges in seconds.
#' @param posix_time A POSIXct object representing the date and time of calculation. Default is "2023-10-20 08:00:00".
#'
#' @return A list of isolines for different time ranges, or an error message if the calculation fails.
#'
#' @examples
#' \dontrun{
#'
#' # Set your HERE API key in your Renviron file using the following steps:
#' # 1. Add key to .Renviron
#' Sys.setenv(HERE_API_KEY = "your_api_key_here")
#' # 2. Reload .Renviron
#' readRenviron("~/.Renviron")
#'
#' # Define a sf object for the location
#' location <- sf::st_point(c(-73.987, 40.757))
#'
#' # Calculate isolines for the location with a 30-minute, 60-minute, 120-minute, and 180-minute range
#' isolines <- create_isochrones(location = location, range = c(1800, 3600, 7200, 10800))
#'
#' # Print the isolines
#' print(isolines)
#'
#' }
#' @import memoise
#' @import hereR
#' @import dplyr
#'
#' @export
source("R/here_api_utils.R")
#'

create_geocode <- function(csv_file, output_file) {
  # Set your HERE API key from environment variable
  api_key <- Sys.getenv("HERE_API_KEY")
  if (identical(api_key, "")) {
    stop("HERE_API_KEY environment variable is not set.")
  }

  hereR::set_key(api_key)


  # Check if the CSV file exists
  if (!file.exists(csv_file)) {
    stop("CSV file not found.")
  }

  # Read the CSV file into a data frame
  data <- read.csv(csv_file)

  # Check if the data frame contains a column named "address"
  if (!"address" %in% colnames(data)) {
    stop("The CSV file must have a column named 'address' for geocoding.")
  }

  # Perform geocoding
  result <- hereR::geocode(data$address)

  # Save the geocoded data to an output file
  write.csv(result, file = output_file, row.names = FALSE)
  message(paste("Saved geocoded results to:", output_file))

  return(result)
}

# Example usage:
# csv_file <- "data/short_complete_npi_for_subspecialists.csv"
# output_file <- "data/geocoded_addresses.csv"
# geocoded_data <- create_geocode(csv_file, output_file)
# View(geocoded_data)

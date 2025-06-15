expand_and_fill_years <- function(input_data, start_year = 2008, end_year = 2023) {
  # Log the function call with arguments
  message(glue::glue("Function expand_and_fill_years called with start_year: {start_year}, end_year: {end_year}"))
  
  # Generate a sequence of years and create a dataframe
  years <- tibble(year = seq(start_year, end_year))
  
  # Cross join the input data with the years
  expanded_data <- tidyr::expand_grid(input_data, year = years$year) %>%
    arrange(npi, year)
  
  # Update `lastupdatestr` only on the specified years, and fill other columns
  expanded_data <- expanded_data %>%
    group_by(npi, year) %>%
    mutate(
      lastupdatestr = ifelse(year >= lastupdatestr, lastupdatestr, NA_integer_),
      penumdatestr = ifelse(year >= penumdatestr, penumdatestr, NA_integer_)
    ) %>%
    tidyr::fill(lastupdatestr, penumdatestr, pfname, plname, address, plocline1, ploccityname,
                plocstatename, ploczip, ploctel, pmailline1, .direction = "down") %>%  # Fill values down to subsequent years
    ungroup()
  
  # Keep only one unique record per NPI per year
  unique_data <- expanded_data %>%
    group_by(npi, year) %>%
    slice(1) %>%  # Select the first occurrence per year
    ungroup()
  
  # Return the expanded and filled dataframe
  return(unique_data)
}

# Example usage
input_data <- tibble(
  npi = c(1689603763, 1689603763, 1689603763),
  lastupdatestr = c(2016, 2009, 2012),
  penumdatestr = c(2006, 2006, 2006),
  pfname = c("TYLER", "TYLER", "TYLER"),
  plname = c("MUFFLY", "MUFFLY", "MUFFLY"),
  address = c("777 BANNOCK ST", "9500 EUCLID AV", "12605 E 16TH AV"),
  plocline1 = c("777 BANNOCK ST", "9500 EUCLID AV", "12605 E 16TH AV"),
  ploccityname = c("DENVER", "CLEVELAND", "AURORA"),
  plocstatename = c("CO", "OH", "CO"),
  ploczip = c("80204", "44195", "80045"),
  ploctel = c("303602XXXX", "216445XXXX", "720848XXXX"),
  pmailline1 = c("777 BANNOCK ST", "2400 SAYBROOK RD", "PO BOX 11")
)

# Apply the function to expand and fill the dataframe
expanded_filled_data <- expand_and_fill_years(input_data)

# Print the result
print(expanded_filled_data, n = 100)

#######################
library(dplyr)
library(tidyr)

transform_to_fill_in_correct <- function(input_data, start_year = 2008, end_year = 2023) {
  # Create a sequence of years
  year_sequence <- seq(start_year, end_year)
  
  # Expand the data to include rows for all years
  expanded_data <- input_data %>%
    dplyr::arrange(lastupdatestr) %>%
    dplyr::distinct(npi, lastupdatestr, .keep_all = TRUE) %>% # Keep only unique npi, lastupdatestr rows
    tidyr::expand(., npi, year = year_sequence) %>%
    dplyr::left_join(input_data, by = c("npi", "year" = "lastupdatestr")) %>% # Join back the original data
    dplyr::group_by(npi) %>%
    tidyr::fill(everything(), .direction = "down") %>%
    # Ensure there is one distinct row per npi, year, lastupdatestr
    dplyr::distinct(., .keep_all = TRUE) %>%
    dplyr::ungroup() %>%
    # Replace remaining NAs with "unknown"
    dplyr::mutate(across(c(address, plocline1, ploccityname, plocstatename, ploczip, ploctel), ~tidyr::replace_na(., "unknown")))
  
  return(expanded_data)
}

# Example input
input_data <- tibble(
  npi = c(1.69e9, 1.69e9, 1.69e9),
  lastupdatestr = c(2016, 2009, 2012),
  penumdatestr = c(2006, 2006, 2006),
  pfname = c("TYLER", "TYLER", "TYLER"),
  plname = c("MUFFLY", "MUFFLY", "MUFFLY"),
  address = c("777 BANNOCK", "9500 EUCLID", "12605 E 16TH"),
  plocline1 = c("777 BANNOCK", "9500 EUCLID", "12605 E 16TH"),
  ploccityname = c("DENVER", "CLEVELAND", "AURORA"),
  plocstatename = c("CO", "OH", "CO"),
  ploczip = c("80204", "44195", "80045"),
  ploctel = c("303602XXXX", "216445XXXX", "720848XXXX"),
  pmailline1 = c("NA", "NA", "NA")
)

output <- transform_to_fill_in_correct(input_data)
print(output)

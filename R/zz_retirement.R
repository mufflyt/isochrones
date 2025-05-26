# Retirement

library(dplyr)

library(dplyr)

find_retirement_year <- function(data, retirement_data, cert_status_data) {
  # Sort data by NPI and year
  data <- data %>% arrange(npi, year)
  
  # Convert retirement date to date object
  retirement_data$`NPPES Deactivation Date` <- as.Date(retirement_data$`NPPES Deactivation Date`, format = "%m/%d/%Y")
  
  # Find the retirement date from the second source
  retirement_data <- retirement_data %>%
    group_by(NPI) %>%
    summarise(retirement_date = min(`NPPES Deactivation Date`, na.rm = TRUE)) %>%
    ungroup()
  
  # Find the year of retirement for each physician
  retirement_years <- data %>%
    left_join(retirement_data, by = "npi") %>%
    mutate(year_of_death = if_else(!is.na(retirement_date), year(retirement_date), max(year, na.rm = TRUE))) %>%
    select(npi, year_of_death)
  
  # Merge retirement years back into original data
  data <- left_join(data, retirement_years, by = "npi")
  
  # Use certStatus data if no retirement information from the first two sources
  data <- data %>%
    group_by(npi) %>%
    mutate(cert_status_retired = if_else(all(is.na(year_of_death) & is.na(retirement_date)),
                                         max(cert_status_data$certStatus, na.rm = TRUE), NA_character_)) %>%
    ungroup()
  
  # Fill in missing retirement years using certStatus data
  data <- data %>%
    mutate(year_of_death = if_else(is.na(year_of_death) & cert_status_retired != "A", year, year_of_death))
  
  # Create column indicating whether a physician has retired
  data <- data %>%
    mutate(retired = if_else(year == year_of_death, TRUE, FALSE))
  
  return(data)
}

your_data_frame <- read_csv("~/Dropbox (Personal)/isochrones/data/processed_nips.csv")
retirement_data_frame <- read_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/retirement_NPPES Deactivated NPI Report 20240408.csv")
cert_status_data_frame <- read_csv("~/Dropbox (Personal)/isochrones/data/02.5-subspecialists_over_time/goba_unique_goba_deceased_retired.csv")

# Example usage:
df_with_retirement_year <- find_retirement_year(your_data_frame, retirement_data_frame, cert_status_data_frame)

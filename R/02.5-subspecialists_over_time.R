# Not used locally but on a different machine with a Postico database.  

#######################
source("R/01-setup.R")
#######################

##########################################################################
db_details <- list(
  host = "localhost",
  port = 5433,
  name = "template1",
  user = "postgres",
  password = "fatbastard"
)
#postico_database_obgyns_by_year(year = 2023, db_details)

library(dbplyr)
library(dplyr)
library(DBI)  # This is required for connecting to the database
library(RPostgres)  # If using PostgreSQL
library(tidyr)
library(stringr)
library(tidyverse)

#NPI filees are from: https://data.nber.org/npi/webdir/csv/
'%nin%' <- function(x, table) {
  !(x %in% table)
}

# Here are some of the common dplyr functions that are supported by dbplyr when working with database tables:
#   
#   filter(): Used to filter rows based on specified conditions.
# 
# select(): Used to select specific columns from a table.
# 
# mutate(): Used to create new columns or modify existing ones.
# 
# arrange(): Used to sort rows based on one or more columns.
# 
# group_by(): Used to group rows by one or more columns.
# 
# summarize(): Used to compute summary statistics on grouped data.
# 
# left_join(), inner_join(), right_join(), full_join(): Used for joining tables.
# 
# distinct(): Used to select distinct rows.
# 
# n(): Used to count the number of rows.
# 
# case_when(): Used for conditional operations.
# 
# between(): Used to filter rows within a range of values.
# 
# inlist(): Used to filter rows where a column's value matches any value in a list.
# 
# sql_translate_env(): Used to execute custom SQL expressions.

################
##Define your database connection details
db_host <- "localhost"
db_port <- 5433  # Default PostgreSQL port
db_name <- "template1"
db_user <- "postgres"
db_password <- "fatbastard"

################
## Create a database connection
db_connection <- dbConnect(
  RPostgres::Postgres(),
  dbname = db_name,
  host = db_host,
  port = db_port,
  user = db_user,
  password = db_password
)

################
## Reference the "NPPES" table using tbl
nppes_table <- dplyr::tbl(db_connection, "2022")
nucc_taxonomy_201 <- dplyr::tbl(db_connection, "nucc_taxonomy_201")

#nppes_table <- nppes_table %>% collect()
# View(nppes_table)
# write_csv(nppes_table, "temp_nppees_table.csv")

# Fetch the data you need from the database
nppes_data <- nppes_table %>%
  distinct(NPI, .keep_all = TRUE) %>%
  mutate(`Zip Code` = str_sub(`Zip Code`,1 ,5)) %>%
  filter(`Primary Specialty` %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY")) %>% 
  mutate(year = "2023") %>%
  collect()

View(nppes_data)
write_csv(nppes_data, "nppes_data.csv")

#######
postico_database_obgyns_by_year <- function(year, db_details) {
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(stringr)
  library(readr)
  
  # Database connection details
  db_host <- db_details$host
  db_port <- db_details$port
  db_name <- db_details$name
  db_user <- db_details$user
  db_password <- db_details$password
  
  # Create a database connection
  db_connection <- dbConnect(
    RPostgres::Postgres(),
    dbname = db_name,
    host = db_host,
    port = db_port,
    user = db_user,
    password = db_password
  )
  
  # Reference the tables using tbl
  nppes_table_name <- as.character(year)
  nppes_table <- dplyr::tbl(db_connection, nppes_table_name)
  nucc_taxonomy_table_name <- "nucc_taxonomy_201"
  nucc_taxonomy_201 <- dplyr::tbl(db_connection, nucc_taxonomy_table_name)
  
  # Fetch and process the data from the database
  nppes_data <- nppes_table %>%
    distinct(NPI, .keep_all = TRUE) %>%
    mutate(`Zip Code` = str_sub(`Zip Code`,1 ,5)) %>%
    filter(`Primary Specialty` %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY")) %>% 
    #mutate(year = "2023") %>%
    collect()
  
  # Write the processed data to a CSV file
  #write_csv(nppes_data, paste0("data/02.5-subspecialists_over_time.R/Postico_output_", year, "_nppes_data_filtered.csv"))
  write_csv(nppes_data, paste0("Postico_output_", year, "_nppes_data_filtered.csv"))
}

# Example usage
db_details <- list(
  host = "localhost",
  port = 5433,
  name = "template1",
  user = "postgres",
  password = "fatbastard"
)
postico_database_obgyns_by_year(year = 2023, db_details)
postico_database_obgyns_by_year(year = 2022, db_details)
postico_database_obgyns_by_year(year = 2021, db_details)
postico_database_obgyns_by_year(year = 2020, db_details)
postico_database_obgyns_by_year(year = 2019, db_details)
postico_database_obgyns_by_year(year = 2018, db_details)
postico_database_obgyns_by_year(year = 2017, db_details)
postico_database_obgyns_by_year(year = 2016, db_details) #this one fails because no Primary Specialty was listed
postico_database_obgyns_by_year(year = 2015, db_details)
postico_database_obgyns_by_year(year = 2014, db_details)
postico_database_obgyns_by_year(year = 2013, db_details)

#####

# # Apply unite to the fetched data
# nppes_data_collected <- nppes_data %>%
#   dplyr::mutate(across(c(last, c, d, f, g, h, i, j, k, l, m, n, q, z), .fns = ~stringr::str_remove_all(., "[[\\p{P}][\\p{S}]]"))) %>%
#   dplyr::filter(stringr::str_detect(Classification.x, fixed("gyn", ignore_case = TRUE))) %>%
#   dplyr::filter(n == "US") %>%
#   dplyr::mutate(i = stringr::str_sub(i, 1, 5)) %>%
#   dplyr::filter(j == "US") %>%
#   tidyr::unite(address_1, f, g, h, i, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
#   tidyr::unite(address2, k, l, m, sep = "_", remove = FALSE, na.rm = FALSE) %>%
#   dplyr::rename(address = address_1) %>%
#   dplyr::filter(!h %in% c("AE", "AP", "AA", "AS", "GU", "VI", "FM", "MH", "MP", "PW", "UM", "TT")) %>%
#   dplyr::rename(c("first" = "c", "middle" = "d", "street" = "f", "city" = "g", "state" = "h", "zip1" = "i", "street2" = "k", "city2" = "l",
#                   "state2" = "m", "gender" = "q", "taxonomy1" = "r", "taxonomy2" = "s")) %>%
#   dplyr::select(-Grouping.x, -Grouping.y, -Classification.x, - id.y, -j, -n, -id) %>%
#   dplyr::mutate(middle = stringr::str_remove_all(middle, "[\\p{P}\\p{S}]")) %>%
#   dplyr::mutate(honorrific = stringr::str_remove_all(honorrific, "[\\p{P}\\p{S}]"))

#View(nppes_data_collected)
#This is ready to do geocoding.  

write_csv(nppes_data_collected, "NPPES_November_filtered_data.csv")


######
#Function 1: validate_and_remove_invalid_npi
validate_and_remove_invalid_npi <- function(input_data) {
  
  if (is.data.frame(input_data)) {
    # Input is a dataframe
    df <- input_data
  } else if (is.character(input_data)) {
    # Input is a file path to a CSV
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Remove rows with missing or empty NPIs
  df <- df %>%
    #head(5) %>%. #for testing only
    dplyr::filter(!is.na(npi) & npi != "")
  
  # Add a new column "npi_is_valid" to indicate NPI validity
  df <- df %>%
    dplyr::mutate(npi_is_valid = sapply(npi, function(x) {
      if (is.numeric(x) && nchar(x) == 10) {
        npi::npi_is_valid(as.character(x))
      } else {
        FALSE
      }
    })) %>%
    dplyr::filter(!is.na(npi_is_valid) & npi_is_valid)
  
  # Return the valid dataframe with the "npi_is_valid" column
  return(df)
}

#####################
#Function 2: retrieve_clinician_data
## Output
df_updated <- NULL
library(provider)
library(npi)

validate_and_remove_invalid_npi <- function(input_data) {
  
  if (is.data.frame(input_data)) {
    # Input is a dataframe
    df <- input_data
  } else if (is.character(input_data)) {
    # Input is a file path to a CSV
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Remove rows with missing or empty NPIs
  df <- df %>%
    #head(5) %>%. #for testing only
    dplyr::filter(!is.na(npi) & npi != "")
  
  # Add a new column "npi_is_valid" to indicate NPI validity
  df <- df %>%
    dplyr::mutate(npi_is_valid = sapply(npi, function(x) {
      if (is.numeric(x) && nchar(x) == 10) {
        npi::npi_is_valid(as.character(x))
      } else {
        FALSE
      }
    })) %>%
    dplyr::filter(!is.na(npi_is_valid) & npi_is_valid)
  
  # Return the valid dataframe with the "npi_is_valid" column
  return(df)
}

retrieve_clinician_data <- function(input_data) {
  # Load libraries
  #remotes::install_github("andrewallenbruce/provider")
  
  if (is.data.frame(input_data)) {
    # Input is a dataframe
    df <- input_data
  } else if (is.character(input_data)) {
    # Input is a file path to a CSV
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Clean the NPI numbers
  df <- validate_and_remove_invalid_npi(df)
  
  # Function to retrieve clinician data for a single NPI
  get_clinician_data <- function(npi) {
    if (!is.numeric(npi) || nchar(npi) != 10) {
      cat("Invalid NPI:", npi, "\n")
      return(NULL)  # Skip this NPI
    }
    
    clinician_info <- provider::clinicians(npi = npi)
    if (is.null(clinician_info)) {
      cat("No results for NPI:", npi, "\n")
    } else {
      return(clinician_info)  # Print the clinician data as it comes out
    }
    Sys.sleep(1)
  }
  
  #df <- df %>% head(5) #test
  
  # Loop through the "npi" column and get clinician data
  df_updated <- df %>%
    dplyr::mutate(row_number = row_number()) %>%
    dplyr::mutate(clinician_data = purrr::map(npi, get_clinician_data)) %>%
    tidyr::unnest(clinician_data, names_sep = "_") %>%
    dplyr::distinct(npi, .keep_all = TRUE)
  
  return(df_updated)
}

# #
# # Call the retrieve_clinician_data function with an NPI value
input_data <- ("NPPES_November_filtered_data.csv")
clinician_data <- retrieve_clinician_data(input_data)

################
## `complete_npi_for_subspecialists`, WE want to get the zip codes
complete_npi_for_subspecialists <- readr::read_csv("Desktop/complete_npi_for_subspecialists.csv") %>%
  as_tibble() %>%
  distinct(NPI, .keep_all = TRUE)

class(complete_npi_for_subspecialists$NPI)
sum(is.na(complete_npi_for_subspecialists))

################
## left_join
add_zip <- complete_npi_for_subspecialists %>%
  dplyr::left_join(filtered_data, by = c("NPI" = "npi")) 

View(add_zip)

################
## Collect and save as an RDS
add_zip_collect <- collect(add_zip)
collected_data <- collect(filtered_data)

# Specify the file path for the RDS file
rds_file_path <- "nppes_november_2023.rds"

# Write the collected data to the RDS file
saveRDS(collected_data, file = rds_file_path)



####################
read_and_clean_tables <- function(db_connection, years) {
  # Initialize an empty list to store the data frames
  cleaned_tables <- list()
  
  # Loop through the specified years
  for (year in years) {
    # Extract the year from the table name
    year_num <- as.numeric(str_extract(year, "\\d+"))
    
    # Create the table name
    table_name <- paste0(year)
    
    # Read the table from the database
    table_data <- dplyr::tbl(db_connection, table_name)
    
    # Perform cleaning operations
    cleaned_data <- table_data %>%
      filter(`Primary Specialty` %in% c("OBSTETRICS/GYNECOLOGY", "GYNECOLOGICAL ONCOLOGY")) %>%
      mutate(`Zip Code` = str_sub(`Zip Code`, 1, 5)) %>%
      distinct(NPI, .keep_all = TRUE) %>%
      mutate(Year = as.character(year_num))
    
    # Ensure NPI is of integer type
    cleaned_data$NPI <- as.integer(dplyr::pull(cleaned_data, NPI))
    
    # Store the cleaned data frame in the list
    cleaned_tables[[year]] <- cleaned_data
  }
  
  # Return a named list of cleaned data frames
  names(cleaned_tables) <- years
  return(cleaned_tables)
}

# Usage example: Read and clean tables for multiple years
years_to_process <- c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022")

cleaned_tables <- read_and_clean_tables(db_connection, years_to_process)

# Access the cleaned data frames for individual years, e.g., cleaned_tables$`2017`


#########################################
full_join_cleaned_tables <- function(cleaned_tables) {
  # Initialize the result data frame with the first cleaned table
  result_data <- cleaned_tables[[1]]
  
  # Loop through the remaining cleaned tables and perform full joins
  for (i in 2:length(cleaned_tables)) {
    year <- names(cleaned_tables)[i]
    
    # Perform a full join with a suffix of the year
    result_data <- full_join(result_data, cleaned_tables[[i]], by = "NPI", suffix = c("", paste0(".", year)))
  }
  
  # Return the result data frame
  return(result_data)
}



# Usage example: Read and clean tables for multiple years
years_to_process <- c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022")
cleaned_tables <- read_and_clean_tables(db_connection, years_to_process)

# Perform a full join with suffixes
result_full_join <- full_join_cleaned_tables(cleaned_tables)

# Print the resulting data frame
print(result_full_join)

collect_result_full_join <- collect(result_full_join)
glimpse(collect_result_full_join)

dim(collect_result_full_join)

readr::write_rds(collect_result_full_join, "Desktop/collect_result_full_join.rds")

dbDisconnect(db_connection)

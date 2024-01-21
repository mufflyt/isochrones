# Here we are trying to get year-specific physicians.  

#######################
source("R/01-setup.R")
#######################

# See code below to do database work.  

#****************************************************************************
#* CLEAN EACH YEAR OF DATA TO CREATE 'year_by_year_nppes_data_collected'.  WE ONLY BROUGHT IN MINIMAL INFO ABOUT THE PHYSICIANS FROM THE POSTICO DATABASE LIKE NAME AND NPI NUMBER.  
#****************************************************************************
 
year_by_year_nppes_data_collected <- exploratory::searchAndReadDelimFiles(folder = "data/02.5-subspecialists_over_time", pattern = "*.csv|*.tsv|*.txt|*.text|*.tab", delim = ",", # I had to manually specify the delimiter here.  
quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(year_1 = list_to_text(str_extract_all(id.new, "[:digit:]+")), .after = ifelse("id.new" %in% names(.), "id.new", last_col())) %>%
  rename() %>%
  select(-id.new) %>%
  arrange(NPI) %>%
  mutate(`Primary Specialty` = coalesce(`Primary Specialty`, `Primary Speciality`)) %>%
  mutate(`Last Name` = coalesce(`Last Name`, `Last Namee`)) %>%
  select(-year, -`Last Namee`, -`Primary Speciality`) %>%
  rename(year = year_1) %>%
  group_by(NPI) %>%
  arrange(year) %>%
  exploratory::reorder_cols(year, NPI, `Last Name`, `First Name`, `Middle Name`, Gender, City, State, `Zip Code`, Credential, `Primary Specialty`, `Secondary Specialty`, id) %>%
  exploratory::reorder_cols(year, NPI, `Last Name`, `First Name`, `Middle Name`, Gender, City, State, `Zip Code`, `Primary Specialty`, id, Credential, `Secondary Specialty`) %>%
  select(-Credential, -`Secondary Specialty`) %>%
  rename(goba_id = id) %>%
  readr::write_csv(., "data/02.5-subspecialists_over_time/year_by_year_nppes_data_collected.csv")
# year_by_year_nppes_data_collected <- readr::read_csv("data/02.5-subspecialists_over_time/year_by_year_nppes_data_collected.csv") # for testing

#**************************
#* VALIDATE THE NPI NUMBERS
#**************************
year_by_year_nppes_data_validated_npi <- validate_and_remove_invalid_npi(input_data = "data/02.5-subspecialists_over_time/year_by_year_nppes_data_collected.csv") %>% 
  dplyr::filter(npi_is_valid == TRUE) %>%
  readr::write_csv(., "data/02.5-subspecialists_over_time/year_by_year_nppes_data_validated_npi.csv")
#year_by_year_nppes_data_validates_npi <- readr::read_csv("data/02.5-subspecialists_over_time/year_by_year_nppes_data_validated_npi.csv") # for testing

#**************************
#* RETURN THE CONTEMPORARY DEMOGRAPHICS OF THE PHYSICIAN DATA WITH PREFIX OF "CLINICIAN_DATA_" (GENDER, MEDICAL SCHOOL, GRAD YEAR) ARE THE ONLY TIMELESS VARIABLES.  PAST SUBSPECIALISTS WHO ARE NO LONGER PRACTICING ARE NOT SEARCHABLE VIA CONTEMPORARY NPPES NPI DATABASE SO WE WILL NEED TO USE THE POSTICO DATABASE FOR ADDRESS, PRACTICE NAME, ETC.  PEOPLE WHO RETIRED ARE GOING TO BE "NO RESULTS"
#**************************
## Call the retrieve_clinician_data function with an NPI value
distinct_year_by_year_nppes_data_validated_npi <- readr::read_csv("data/02.5-subspecialists_over_time/year_by_year_nppes_data_validated_npi.csv") %>% 
  dplyr::distinct(NPI, .keep_all = TRUE) %>%
  #plyr::rename (npi = NPI) %>%
  arrange(year, goba_id) %>%
  #head(101) %>% # FOR TESTING
  #filter(npi > 1124057377) %>% # FOR RESTARTING and using part of the other data
  write_csv(., "data/02.5-subspecialists_over_time/distinct_year_by_year_nppes_data_validated_npi.csv") 
#View(distinct_year_by_year_nppes_data_validated_npi)

dim(distinct_year_by_year_nppes_data_validated_npi)[1]

paste0("There are ", dim(distinct_year_by_year_nppes_data_validated_npi)[1], " unique OBGYNs represented from 2013 to 2023.")

#**************************
#* BRING IN SUBSPECIALIST DATA WITH NPI NUMBER FROM GOBA
#**************************
complete_npi_for_subspecialists <- readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted.csv") %>%
  as_tibble() %>%
  #distinct(NPI_goba, .keep_all = TRUE) %>%
  #dplyr::rename(npi = NPI_goba) %>%
  filter(!is.na(npi))

complete_npi_for_subspecialists_cleaned <- complete_npi_for_subspecialists %>%
  readr::write_csv(., "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") 

#**************************
#* retrieve_clinician_data FUNCTION FOR GETTING DEMOGRAPHICS BY MATCHING NPI NUMBERS TO THE NPPES DATABASE
#**************************
input_data <- ("data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") 

retrieve_clinician_data(input_data, 
                        chunk_size = 5, 
                        output_dir = "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results")

# Files where the NPI number was found during our contemporary/2024 search
# Merged all files from 02.5-subspecialists_over_time
retrieve_clinician_data_output <- exploratory::searchAndReadDelimFiles(folder = "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results", pattern = "*.csv|*.tsv|*.txt|*.text|*.tab", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  select(-id, -year, -npi_is_valid, -clinician_data_suffix)

# TODO:  Antijoin between distinct_year_by_year_nppes_data_validated_npi.csv and retrieve_clinician_data_output ot find those who had no results for the NPI number with the API.  

#View(retrieve_clinician_data_output)

#**************************
#* WAS NOT RUN LOCALLY!
#**************************

# Not used locally but on a different machine with a Postico database.  
##########################################################################
db_details <- list(
  host = "localhost",
  port = 5433,
  name = "template1",
  user = "postgres",
  password = "????"
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
db_password <- "???"

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

#**************************
#* DOWNLOADS PHYSICIAN DATA FROM NPI DATABASE FOR EACH YEAR
#* THE RESULTS OF POSTICO_DATABASE_OBGYNS_BY_YEAR BELOW WERE BROUGHT IN TO THE LOCAL MACHINE!
#**************************
db_details <- list(
  host = "localhost",
  port = 5433,
  name = "template1",
  user = "postgres",
  password = "????"
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

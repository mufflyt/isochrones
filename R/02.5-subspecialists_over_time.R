# Here we are trying to get year-specific physicians.  

#######################
source("R/01-setup.R")
#######################

# See code below to do database work.  

#****************************************************************************
#* CLEAN EACH YEAR OF DATA TO CREATE 'year_by_year_nppes_data_collected'.  WE ONLY BROUGHT IN MINIMAL INFO ABOUT THE PHYSICIANS FROM THE POSTICO DATABASE LIKE NAME AND NPI NUMBER.  
#****************************************************************************
year_by_year_nppes_data_collected <- exploratory::searchAndReadDelimFiles(folder = "data/02.5-subspecialists_over_time/Postico", pattern = "*.csv|*.tsv|*.txt|*.text|*.tab", delim = ",", # I had to manually specify the delimiter here.  
quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(year_1 = list_to_text(str_extract_all(id.new, "[:digit:]+")), .after = ifelse("id.new" %in% names(.), "id.new", last_col())) %>%
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
  readr::write_csv(., "data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_collected.csv")
dim(year_by_year_nppes_data_collected)[1]
# year_by_year_nppes_data_collected <- readr::read_csv("data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_collected.csv") # for testing

# > year_by_year_nppes_data_collected
# # A tibble: 323,193 × 11
# # Groups:   NPI [49,232]
# year        NPI `Last Name` `First Name` `Middle Name` Gender City  State `Zip Code` `Primary Specialty` goba_id
# <chr>     <dbl> <chr>       <chr>        <chr>         <chr>  <chr> <chr> <chr>      <chr>                 <dbl>
#   1 2013     1.00e9 VELOTTA     JENNIFER     A             F      MENT… OH    44060      OBSTETRICS/GYNECOL…      31
# 2 2013     1.00e9 KADIYALA    SAMATHA      K             F      LAKE… TX    77566      OBSTETRICS/GYNECOL…     156
# 3 2013     1.00e9 PALASZEWSKI DAWN         NA            F      TAMPA FL    33612      OBSTETRICS/GYNECOL…     200
# 4 2013     1.00e9 HOMAN       ZENA         K             F      FARGO ND    58103      OBSTETRICS/GYNECOL…     256
# 5 2013     1.00e9 EASTERLIN   MARIE        NA            F      SAIN… GA    31522      OBSTETRICS/GYNECOL…     564
# 6 2013     1.00e9 CHANG       EMILY        CHIA CHUN     F      BRITT IA    50423      OBSTETRICS/GYNECOL…     680
# 7 2013     1.00e9 MALLEK      GREGORY      W             M      SALEM OR    97301      OBSTETRICS/GYNECOL…     723
# 8 2013     1.00e9 FAKIH       MONA         Y             F      DETR… MI    48201      OBSTETRICS/GYNECOL…     900
# 9 2013     1.00e9 COTWRIGHT   ANTONIA      NA            F      REDO… CA    90277      OBSTETRICS/GYNECOL…    1046
# 10 2013     1.00e9 YUZEFOVICH  MICHAEL      NA            M      ALEX… VA    22306      OBSTETRICS/GYNECOL…    1164

# Calculate the needed values
total_physicians <- nrow(year_by_year_nppes_data_collected)
start_year <- min(year_by_year_nppes_data_collected$year)
end_year <- max(year_by_year_nppes_data_collected$year)
unique_physicians <- year_by_year_nppes_data_collected %>%
  distinct(NPI) %>%
  nrow()

# Use glue to create the message with formatted numbers
glue::glue("There are {format(total_physicians, big.mark = ',')} general and GO OBGYN physicians from {start_year} to {end_year}. But there were only {format(unique_physicians, big.mark = ',')} unique physicians by NPI number in the dataset.")

# Group the data by NPI and count the number of unique years for each physician
physician_years <- year_by_year_nppes_data_collected %>%
  group_by(NPI) %>%
  summarise(years_present = n_distinct(year))

# Count the number of physicians for each number of years present
physician_counts <- physician_years %>%
  group_by(years_present) %>%
  summarise(physician_count = n())

# Calculate the percentage of physicians for each number of years present
total_physicians <- n_distinct(year_by_year_nppes_data_collected$NPI)
physician_counts <- physician_counts %>%
  mutate(percentage = (physician_count / total_physicians) * 100)

# Group by NPI and count the number of unique years each physician appears
year_counts <- year_by_year_nppes_data_collected %>%
  group_by(NPI) %>%
  summarize(year_count = n_distinct(year)) %>%
  pull(year_count)

# Count the number of physicians who appear in every year
num_physicians_all_years <- sum(year_counts == max(year_counts))

# Calculate the percentage of physicians who appear in every year
percent_all_years <- (num_physicians_all_years / unique_physicians) * 100

print(physician_counts)
glue::glue("There were {format(num_physicians_all_years, big.mark = ',')} OBGYN and GO physicians ({round(percent_all_years, 1)}% of the total physicians) were present in every year from 2013 to 2023.")

#**************************
#* VALIDATE THE NPI NUMBERS
#**************************
year_by_year_nppes_data_validated_npi <- validate_and_remove_invalid_npi(input_data = "data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_collected.csv") %>% 
  dplyr::filter(npi_is_valid == TRUE) %>% 
  readr::write_csv(., "data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_validated_npi.csv")
dim(year_by_year_nppes_data_validated_npi)[1]
#year_by_year_nppes_data_validates_npi <- readr::read_csv("data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_validated_npi.csv") # for testing
paste0("There are ", dim(year_by_year_nppes_data_validated_npi)[1], " validated non-unique NPI numbers from 2013 to 2023.")


# > year_by_year_nppes_data_validated_npi
# # A tibble: 323,193 × 12
# year       NPI `Last Name` `First Name` `Middle Name` Gender City  State `Zip Code` `Primary Specialty` goba_id
# <dbl>     <dbl> <chr>       <chr>        <chr>         <chr>  <chr> <chr> <chr>      <chr>                 <dbl>
#   1  2013    1.00e9 VELOTTA     JENNIFER     A             F      MENT… OH    44060      OBSTETRICS/GYNECOL…      31
# 2  2013    1.00e9 KADIYALA    SAMATHA      K             F      LAKE… TX    77566      OBSTETRICS/GYNECOL…     156
# 3  2013    1.00e9 PALASZEWSKI DAWN         NA            F      TAMPA FL    33612      OBSTETRICS/GYNECOL…     200

#**************************
#* RETURN THE CONTEMPORARY DEMOGRAPHICS OF THE PHYSICIAN DATA WITH PREFIX OF "CLINICIAN_DATA_" (GENDER, MEDICAL SCHOOL, GRAD YEAR) ARE THE ONLY TIMELESS VARIABLES.  PAST SUBSPECIALISTS WHO ARE NO LONGER PRACTICING ARE NOT SEARCHABLE VIA CONTEMPORARY NPPES NPI DATABASE SO WE WILL NEED TO USE THE POSTICO PAST NPI DATABASE FOR ADDRESS, PRACTICE NAME, ETC.  PEOPLE WHO RETIRED ARE GOING TO BE "NO RESULTS"
#**************************
## Call the retrieve_clinician_data function with an NPI value
distinct_year_by_year_nppes_data_validated_npi <- readr::read_csv("data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_validated_npi.csv") %>% 
  dplyr::distinct(NPI, .keep_all = TRUE) %>%
  arrange(year, goba_id) %>%
  #head(101) %>% # FOR TESTING
  write_csv(., "data/02.5-subspecialists_over_time/end_distinct_year_by_year_nppes_data_validated_npi.csv") 
#View(distinct_year_by_year_nppes_data_validated_npi)
dim(distinct_year_by_year_nppes_data_validated_npi)[1]

# Calculate the start and end year
start_year <- min(distinct_year_by_year_nppes_data_validated_npi$year)
end_year <- max(distinct_year_by_year_nppes_data_validated_npi$year)
# Get the number of unique OBGYNs
num_unique_obgyns <- nrow(distinct_year_by_year_nppes_data_validated_npi)
glue("There are {format(num_unique_obgyns, big.mark = ',')} unique OBGYNs represented from {start_year} to {end_year}.")

# > distinct_year_by_year_nppes_data_validated_npi
# # A tibble: 49,232 × 12
# year       NPI `Last Name` `First Name` `Middle Name` Gender City  State `Zip Code` `Primary Specialty` goba_id
# <dbl>     <dbl> <chr>       <chr>        <chr>         <chr>  <chr> <chr> <chr>      <chr>                 <dbl>
#   1  2013    1.00e9 VELOTTA     JENNIFER     A             F      MENT… OH    44060      OBSTETRICS/GYNECOL…      31
# 2  2013    1.00e9 KADIYALA    SAMATHA      K             F      LAKE… TX    77566      OBSTETRICS/GYNECOL…     156
# 3  2013    1.00e9 PALASZEWSKI DAWN         NA            F      TAMPA FL    33612      OBSTETRICS/GYNECOL…     200

#**************************
#* BRING IN SUBSPECIALIST DATA WITH NPI NUMBER FROM GOBA
#**************************
complete_npi_for_subspecialists <- readr::read_csv("data/02.5-subspecialists_over_time/goba_unrestricted.csv") %>%
  as_tibble() %>%
  #distinct(NPI_goba, .keep_all = TRUE) %>%
  #dplyr::rename(npi = NPI_goba) %>%
  filter(!is.na(npi)) %>%
  readr::write_csv(., "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv") 

# Calculate the start and end year from the dataset
start_year <- min(year(complete_npi_for_subspecialists$sub1startDate))
end_year <- max(year(complete_npi_for_subspecialists$sub1startDate))

# Get the number of unique OBGYNs with NPI numbers
num_unique_obgyns_with_npi <- nrow(complete_npi_for_subspecialists)

# Create sentences using glue
glue("The dataframe contains information about {num_unique_obgyns_with_npi} unique OBGYN subspecialists with NPI numbers.")
glue("The data includes physicians who were board certified to practice their subspecialty in {start_year} to {end_year} and were practicing from 2013 to 2023.")

# Exclude territories and Washington, DC # Identify non-states
states <- complete_npi_for_subspecialists$state_goba[complete_npi_for_subspecialists$state_goba %in% state.name]
#non_states <- non_states[!is.na(non_states)]
glue("Subspecialist OBGYNS are present in {length(unique(states))} states plus {paste(unique(non_states), collapse = ', ')}.")


#**************************
#* retrieve_clinician_data FUNCTION FOR GETTING DEMOGRAPHICS BY MATCHING NPI NUMBERS TO THE NPPES DATABASE.  GETS DEMOGRAPHICS USING THE NPPES API VIA PROVIDER::CLINICIANS FUNCTION.  
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
  distinct(npi, .keep_all = TRUE) %>%
  select(-id, -year, -npi_is_valid, -clinician_data_suffix)

write_rds(retrieve_clinician_data_output, "data/02.5-subspecialists_over_time/end_retrieve_clinician_data_chunk_results.rds")

# Specific column names from the NPPES data starting with "clinician_data_"
filtered_columns <- grep("^clinician_data_", names(retrieve_clinician_data_output), value = TRUE); filtered_columns

paste0("There are ", dim(retrieve_clinician_data_output)[1], " unique subspecialist OBGYNs WITH NPI NUMBERS and timeless DEMOGRAPHIC DATA represented from 2013 to 2023.")



# Calculate the number of females
num_females <- sum(retrieve_clinician_data_output$clinician_data_gender == "Female", na.rm = TRUE)
# Calculate the proportion of females
proportion_females <- (num_females / nrow(retrieve_clinician_data_output)) * 100

# Create the sentence with the formatted values
glue("There are {format(num_females, big.mark = ',')} female physicians in the dataset, making up {sprintf('%.1f', proportion_females)}% of the total.")

# Get the number of unique OBGYNs with NPI numbers
num_unique_obgyns_with_npi <- nrow(retrieve_clinician_data_output)

# Create the message with formatted number of OBGYNs and dynamic year range
glue("There are {format(num_unique_obgyns_with_npi, big.mark = ',')} unique OBGYNs WITH NPI NUMBERS represented.")

#**************************
#* GYN ONC Specific look at the data
#**************************
#retrieve_clinician_data_output <- read_rds("data/02.5-subspecialists_over_time/end_retrieve_clinician_data_chunk_results.rds")
# Brought over from old Mac with the Postico database.  
gyn_onc_over_the_years <- readr::read_csv("data/02.5-subspecialists_over_time/end_year_by_year_nppes_data_collected.csv") %>%
  filter(`Primary Specialty` == "GYNECOLOGICAL ONCOLOGY") 

write_csv(gyn_onc_over_the_years, "data/02.5-subspecialists_over_time/gyn_onc_over_the_years.csv")

# Calculate initial count of gynecologic oncologists at the beginning of the study (2013)
initial_count <- gyn_onc_over_the_years %>%
  filter(year == 2013) %>%
  distinct(NPI) %>%
  nrow(); initial_count

# Calculate total count of gynecologic oncologists by the end of the study (2023)
total_count <- gyn_onc_over_the_years %>%
  filter(year == 2023) %>%
  distinct(NPI) %>%
  nrow(); total_count

# Calculate the added count over the study period
# Assuming every new NPI after 2013 is an addition
added_count <- gyn_onc_over_the_years %>%
  distinct(NPI) %>%
  nrow() - initial_count; added_count

# The difference in years covered in the study
study_years_diff <- max(gyn_onc_over_the_years$year) - min(gyn_onc_over_the_years$year); study_years_diff

# Print the results
cat("Initial count of gynecologic oncologists in 2013:", initial_count, "\n")
cat("Total count of gynecologic oncologists by 2023:", total_count, "\n")
cat("Added count over the study period:", added_count, "\n")
cat("Difference in years covered in the study:", study_years_diff, "\n")

# Assuming gyn_onc_over_the_years has been previously defined and filtered for GYNECOLOGICAL ONCOLOGY as shown

# Get unique NPIs for 2013
npi_2013 <- gyn_onc_over_the_years %>%
  filter(year == 2013) %>%
  distinct(NPI) %>%
  pull(NPI)

# Get unique NPIs for 2023
npi_2023 <- gyn_onc_over_the_years %>%
  filter(year == 2022) %>%
  distinct(NPI) %>%
  pull(NPI)

# Calculate retired gynecologic oncologists
# These are NPIs present in 2013 but not in 2023
retired_count <- setdiff(npi_2013, npi_2023) %>%
  length()

# Print the result
cat("Number of gynecologic oncologists no longer clinically active/retired from 2013 to 2023:", retired_count, "\n")

# Assuming gyn_onc_over_the_years has been previously defined and filtered for GYNECOLOGICAL ONCOLOGY as shown
# Create a table of the last year each NPI appears, assuming retirement if not present in 2023
retirement_table <- gyn_onc_over_the_years %>%
  group_by(NPI) %>%
  summarise(Name = paste(`First Name`, `Last Name`, City, State),
            LastYear = max(year),
            .groups = 'drop') %>%
  filter(LastYear < 2022) %>%
  arrange(desc(LastYear)) %>%
  distinct(NPI, .keep_all = TRUE)

# Print the table
print(retirement_table)



# TODO:  Antijoin between end_distinct_year_by_year_nppes_data_validated_npi.csv and retrieve_clinician_data_output ot find those who had no results for the NPI number with the API.  

#View(retrieve_clinician_data_output)

# c("Load raw data of all generalists for all years", year_by_year_nppes_data_collected, #Previous years NPI files, Postico files
#   "Validate NPI numbers", distinct_year_by_year_nppes_data_validated_npi, #Number of individual physicians
#   "Narrow to subspecialists only", complete_npi_for_subspecialists, 
#   "NPPES Demographics present for subspecialists", retrieve_clinician_data_output,
#   "Finalize output")

all_docs <- nrow(year_by_year_nppes_data_collected %>% distinct(NPI)); all_docs
subspecialists <- nrow(complete_npi_for_subspecialists); subspecialists
excluded_bc_generalists <- all_docs - subspecialists; excluded_bc_generalists
non_onc_subspecialists <- nrow(complete_npi_for_subspecialists %>% filter(sub1 != "ONC"))

# Set the values which will go into each label.
a1 <- paste0('Number of of all OBGYNs \n for all years, n = ', format(all_docs, big.mark = ",", scientific = FALSE)) 
b1 <- ''
c1 <- ''
d1 <- paste0('Number of Gynecologic\n Oncologists included\n for analysis,\n n = ', format(nrow(complete_npi_for_subspecialists %>% filter(sub1 == "ONC")), big.mark = ",", scientific = FALSE))
e1 <- paste0('Number of Gynecologic\n Oncologists with \nNPPES Demographics,\n n = ', format(nrow(onc_with_demographics), big.mark = ",", scientific = FALSE))
a2 <- ''
b2 <- paste0('Excluded because\n General OBGYN,\n n = ', format(excluded_bc_generalists, big.mark = ","))
c2 <- paste0('Non-Gynecologic\n Oncology Subspecialist\n n = ', format(non_onc_subspecialists, big.mark = ","))
d2 <- ''
e2 <- ''


#Create a node dataframe
ndf <- create_node_df(
  n = 10,
  label = c(a1, b1, c1, d1, e1, #Column 1
            a2, b2, c2, d2, e2), #Column 2
  style = c('solid', 'invis', 'invis', 'solid', 'solid', #Column 1
            'invis', 'solid', 'solid', 'invis', 'invis'), #Column 2
  shape = c('box', 'point', 'point', 'box', 'box', #Column 1 
            'plaintext', 'box', 'box', 'point', 'point'), #Column 2
  width = c(3, 0.001, 0.001, 3, 3, #Column 1
            2, 2.5, 2.5, 0.001, 0.001), #Column 2
  height = c(1, 0.001, 0.001, 1, 1, #Column 1
             1, 1, 1, 0.001, 0.001), #Column 2
  fontsize = c(rep(14, 10)),
  fontname = c(rep('Helvetica', 10)),
  penwidth = 1.5,
  fixedsize = 'true')

#Create an edge dataframe
edf <- create_edge_df(
  from = c(1, 2, 3, 4, #Column 1
           6, 7, 8, 9, #Column 2
           2, 3 #Horizontals
  ),
  to = c(2, 3, 4, 5, #Column 1
         7, 8, 9, 10, #Column 2
         7, 8 #Horizontals
  ),
  arrowhead = c('none', 'none', 'normal', 'normal', #Column 1
                'none', 'none', 'none', 'none', #Column 2
                'normal', 'normal' #Horizontals
  ),
  color = c('black', 'black', 'black', 'black', #Column 1
            '#00000000', '#00000000', '#00000000', '#00000000', #Column 2
            'black', 'black' #Horizontals
  ),
  constraint = c(rep('true', 8), #Columns
                 rep('false', 2) #Horizontals
  )
)

g <- DiagrammeR::create_graph(ndf,
                              edf,
                              attr_theme = NULL)


DiagrammeR::render_graph(g)
export_graph(g, file_name = "data/02.5-subspecialists_over_time/flowchart.png")


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

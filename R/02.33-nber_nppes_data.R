# The script provided performs a series of operations primarily focusing on data management and analysis of healthcare provider data from the National Provider Identifier (NPI) and other sources. It starts by setting up the environment in R, specifying preferences for certain functions from various packages such as `duckplyr`, `lubridate`, and `conflicted`, to handle potential naming conflicts within the libraries used.
# 
# Key activities in the script include:
#   
#   1. **Database Connection and Setup:**
#   The script establishes a connection to a DuckDB database, which is a lightweight SQL database. It points to a specific file path where the database is located, ensuring all data operations are performed within this database context.
# 
# 2. **Table Processing and Data Extraction:**
#   It lists several table names from healthcare data sources (e.g., physician compare files, doctors and clinicians data over several years). For each table, it retrieves column names and checks the commonality of these columns across tables, identifying which columns appear frequently. This is used to standardize data structures across different datasets.
# 
# 3. **Data Loading and Table Creation:**
#   The script includes commands to create tables by reading CSV files, especially from the NBER (National Bureau of Economic Research) website, which provides a comprehensive collection of provider data. It uses SQL commands within the DuckDB connection to execute these operations, handling possible CSV format issues like headers, quotes, and null values.
# 
# 4. **Data Filtering and Transformation:**
#   It filters and transforms the data, selecting specific columns relevant to the analysis, such as provider names, locations, and specialties. The data is then cleaned and manipulated, e.g., converting names to uppercase, and filtering based on specific criteria like tax codes that correspond to certain medical specialties.
# 
# 5. **Analytical Operations:**
#   The script performs a series of checks and analytical operations. For example, it filters the data to include only certain types of providers based on taxonomy codes, checks for data completeness, and refines the dataset to focus on specific attributes.
# 
# 6. **Output and Data Export:**
#   Processed data is then written back to CSV files for further use, possibly in other analyses or reporting tools. The script also includes commands to handle errors gracefully during the data processing steps, ensuring that any issues are logged and do not halt the execution of the script.
# 
# Overall, the script is an example of complex data handling, merging, and analysis operations typical in data science projects involving large, diverse datasets. It emphasizes the use of database operations, custom R functions, and data manipulation techniques to prepare data for detailed analysis or reporting.

# NBER DATA IS ok but it does have each change in address noted.  Maybe we assume that everyone stayed in the same place until the end.  https://www.nber.org/research/data/npinppes-cumulative-collection-preliminary

# Required Input Files for Script Execution ----

# 1. NBER Historical Downloads:
#    - Database File: "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"

# 2. Doctors and Clinicians Archives:
#    - CSV Files: Files from "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/"
#      - "NPPES_Data_Dissemination_April_2010_npidata_20050523-20100208.csv"
#      - "NPPES_Data_Dissemination_April_2021_npidata_pfile_20050523-20210411.csv"
#      - "NPPES_Data_Dissemination_April_2022_npidata_pfile_20050523-20220410.csv"
#      - "NPPES_Data_Dissemination_April_2023_npidata_pfile_20050523-20230410.csv"
#      - "NPPES_Data_Dissemination_April_2024_npidata_pfile_20050523-20240410.csv"

# 3. Facility Affiliation Files:
#    - Directory: "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files"
#      - "facility_affiliation_file_1.csv"
#      - "facility_affiliation_file_2.csv"
#      - "facility_affiliation_file_3.csv"



# Setup and conflicts -----------------------------------------------------
# https://data.cms.gov/provider-data/search?theme=Doctors%20and%20clinicians
# https://data.cms.gov/provider-data/search?keyword=Clinicians

source("R/01-setup.R")
conflict_prefer("filter", "duckplyr")
conflict_prefer("year", "lubridate")
conflicted::conflicts_prefer(exploratory::str_detect)
DBI::dbDisconnect(con)
conflicted::conflicts_prefer(dplyr::recode)
conflicted::conflicts_prefer(exploratory::recode)
conflicted::conflicts_prefer(lubridate::year)
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)


# Similiar columns across tables ------------------------------------------
# Table names
table_names <- c(
  "doc_archive_06_2014_National_Downloadable_File_csv_1",
  "doc_archive_12_2017_Physician_Compare_National_Downloadable_File_csv_3",
  "doc_archive_12_2018_National_Downloadable_File_2018_12_csv_2",
  "doc_archive_12_2019_Physician_Compare_National_Downloadable_File_201912_csv_3",
  "doctors_and_clinicians_archive_12_2020_mj5m_pzi6_csv_10",
  "doctors_and_clinicians_12_2021_DAC_NationalDownloadableFile_csv_7",
  "doctors_and_clinicians_12_2022_DAC_NationalDownloadableFile_csv_8",
  "doctors_and_clinicians_12_2023_DAC_NationalDownloadableFile_csv_9",
  "doctors_and_clinicians_05_2024_DAC_NationalDownloadableFile_csv_6"
)

# Initialize an empty list to store column names for each table
all_column_names <- list()

# Iterate over table names and retrieve column names
for (table_name in table_names) {
  table_ref <- tbl(con, dplyr::sql(paste("SELECT * FROM", dplyr::sql(table_name), "LIMIT 0")))
  column_names <- table_ref %>%
    duckplyr::collect() %>%
    names()
  all_column_names[[table_name]] <- column_names
}

# Flatten the list into a single vector
all_columns_vector <- unlist(all_column_names)

# Count the frequency of each column name
column_frequency <- table(all_columns_vector)

# Determine the threshold for "most" tables, e.g., appears in at least 75% of the tables
threshold <- length(table_names) * 0.85

# Identify columns that meet or exceed the threshold
common_columns_most <- names(column_frequency[column_frequency >= threshold])

# Print the results
cat("Columns common to most tables:\n", paste(common_columns_most, collapse = ", "), "\n")


# NBER section ------------------------------------------------------------
# 'nber_all' read in to duckdb --------------------------------------------
# 'nber_all' Load the entire NBER file from http://data.nber.org/nppes/csv-zip/npiall. 
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

# Check if the table exists
if (!"nber_all" %in% dbListTables(con)) {
  # Adjust SQL command with explicit CSV reading settings
  sql_command <- sprintf(
    "CREATE TABLE nber_all AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
    file_path)
  
  # Execute the modified SQL command
  dbExecute(con, sql_command)
}

# 'nber_all' create reference to table --------------------------------------------
# Use duckplyr to create a reference to the table without loading it into R
nber_all <- dplyr::tbl(con, "nber_all"); glimpse(nber_all)

# 'nber_all' filtering ----------------------------------------------------
conflicted::conflicts_prefer(lubridate::year)

nber_all_basic <- nber_all %>%
  duckplyr::select(
    npi,
    aofname,
    aolname,
    aomname,
    aoname_suffix,
    aotelnum,
    entity,
    lastupdatestr,
    penumdatestr,
    pfname,
    plname,
    pfnameoth,
    plocline1,
    ploccityname,
    plocstatename,
    ploczip,
    ploccountry,
    ploctel,
    pmailline1,
    pmailcityname,
    pmailstatename,
    pmailcountry,
    pmailzip,
    pgender,
    aocredential,
    pcredential,
    porgname,
    ptaxcode1,
    ptaxcode2,
    soleprop
  ) %>%
  duckplyr::filter(entity == 1,
                   pmailcountry == "US",
                   ploccountry == "US",
                   ptaxcode1 %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X", "207VB0002X") |
                    ptaxcode2 %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X")) %>%
  duckplyr::select(-aofname, -aolname, -aomname, -aoname_suffix, -aotelnum, -aocredential, -porgname) %>%
  duckplyr::mutate(across(c(lastupdatestr, penumdatestr), lubridate::year)) %>%
  duckplyr::select(-entity, -pfnameoth, -ploccountry, -pmailcountry) %>%
  duckplyr::mutate(across(c(pfname, plname, plocline1, ploccityname, pmailline1, pmailcityname), .fns = ~str_to_upper(.))) 

# 'nber_all' sanity check --------------------------------------------
nber_all_basic %>% 
  dplyr::filter(plname == "MUFFLY") #There are three MUFFLY versions here.  CORRECT

# 'nber_all' collect the data --------------------------------------------
nber_all_collected <- nber_all_basic %>%
  duckplyr::collect() %>%
  duckplyr::filter(pmailstatename %nin% c("AA", "ae", "AE", "AP", "APO", "GU", "VI", "FM", "MP", "AS") & plocstatename %nin% c("AA", "ae", "AE", "AP", "APO", "AS", "GU", "MP", "VI", "FM")) %>%
  mutate(pcredential = stringr::str_remove_all(pcredential, "[[\\p{P}][\\p{S}]]")) %>%
  # Remove MD punctuation.  
  mutate(pcredential = stringr::str_remove_all(pcredential, "[:blank:]")) %>%
  mutate(across(c(pmailzip, ploczip), .fns = ~stringr::str_sub(.,1 ,5))) %>%
  tidyr::unite(address, plocline1, ploccityname, plocstatename, ploczip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  mutate(pcredential = str_to_upper(pcredential)) %>%
  mutate(pgender = dplyr::recode(pgender, "F" = "Female", "M" = "Male")) %>%
  dplyr::arrange(npi) %>%
  group_by(npi) %>%
  fill(pcredential, .direction = "down") %>%
  ungroup()

# 'nber_all' sanity check --------------------------------------------
nber_all_collected %>% 
  dplyr::filter(plname == "MUFFLY") #There are three MUFFLY versions here.  CORRECT

nber_all_collected <- nber_all_collected[grepl("MD|DO", nber_all_collected$pcredential), , drop = FALSE]

# Adjusting the 'pcredential' within nber_all_collected1 itself
nber_all_collected$pcredential <- ifelse(grepl("MD", nber_all_collected$pcredential), "MD",
         ifelse(grepl("DO", nber_all_collected$pcredential), "DO", nber_all_collected$pcredential))

# 'nber_all' write csv --------------------------------------------
nber_all_collected <- nber_all_collected %>%
  readr::write_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/end_sp_duckdb_nber_all.csv"); gc()

# 'nber_all' Sanity Check ------------------------------------------------------------
nber_all_collected %>% 
  dplyr::filter(plname == "MUFFLY") #!!!!!!!!!!!! TRIPLE CHECK

glimpse(nber_all_collected)

# 'nber_all' Exploratory Data Analysis -----------------------------------------------
nber_all_collected <- readr::read_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/end_sp_duckdb_nber_all.csv")

# nber_all_collected %>%
#   DataExplorer::create_report(
#     output_file  = "EDA_DataExplorer_NBER_OBGYN_only",
#     output_dir   = "data/02.33-nber_nppes_data",
#     y            =  NULL,
#     report_title = "EDA Report - NBER"
#   )

nber <- arsenal::tableby(~., data = nber_all_collected %>% dplyr::select(penumdatestr, lastupdatestr, pgender, pcredential, soleprop, plocstatename))

nber_summary <- summary(nber, text=T, pfootnote=TRUE); nber_summary

# Calculate the needed values
total_physicians <- nrow(nber_all_collected)
start_year <- min(nber_all_collected$lastupdatestr)
end_year <- max(nber_all_collected$lastupdatestr)
unique_physicians <- nber_all_collected %>%
  distinct(npi, .keep_all = FALSE) %>%
  nrow()

# Use glue to create the message with formatted numbers
glue::glue("There are {format(total_physicians, big.mark = ',')} OBGYN physicians listed from {start_year} to {end_year}. But there were only {format(unique_physicians, big.mark = ',')} unique physicians by NPI number in the NBER dataset.")

# Group the data by npi and count the number of unique years for each physician
physician_years <- nber_all_collected %>%
  group_by(npi) %>%
  summarise(years_present = n_distinct(lastupdatestr))

# Count the number of physicians for each number of years present
physician_counts <- physician_years %>%
  group_by(years_present) %>%
  summarise(physician_count = n())

# Calculate the percentage of physicians for each number of years present
total_physicians <- n_distinct(nber_all_collected$npi)
physician_counts <- physician_counts %>%
  mutate(percentage = (physician_count / total_physicians) * 100)

# Group by npi and count the number of unique years each physician appears
year_counts <- nber_all_collected %>%
  group_by(npi) %>%
  summarize(year_count = n_distinct(lastupdatestr)) %>%
  pull(year_count)

# Count the number of physicians who appear in every year
num_physicians_all_years <- sum(year_counts == max(year_counts))

# Calculate the percentage of physicians who appear in every year
percent_all_years <- (num_physicians_all_years / unique_physicians) * 100

print(physician_counts)
glue::glue("There were {format(num_physicians_all_years, big.mark = ',')} OBGYN physicians ({round(percent_all_years, 1)}% of the total physicians) were present in all 15 years from {start_year} to {end_year}.")

# ' ---------------------------------------------------------
# NPPES Section -----------------------------------------------

# `nppes_all` Batch read in NPPES data to duckDB------------------------------
con <- dbConnect(duckdb::duckdb(), "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb")  
directory_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/"

file_names <- c(
  "NPPES_Data_Disseminat_April_2010_npidata_20050523-20100208.csv",
  "NPPES_Data_Disseminat_April_2021_npidata_pfile_20050523-20210411.csv",
  "NPPES_Data_Disseminat_April_2022_npidata_pfile_20050523-20220410.csv",
  "NPPES_Data_Dissemination_Apr_2012_npidata_20050523-20120409.csv",
  "NPPES_Data_Dissemination_Apr_2013_npidata_20050523-20130407.csv",
  "NPPES_Data_Dissemination_Apr_2014_npidata_20050523-20140413.csv",
  "NPPES_Data_Dissemination_Apr_2016_npidata_20050523-20160410.csv",
  "NPPES_Data_Dissemination_April_2011_npidata_20050523-20110411.csv",
  "NPPES_Data_Dissemination_April_2015_npidata_20050523-20150412.csv",
  "NPPES_Data_Dissemination_April_2017_npidata_20050523-20170409.csv",
  "NPPES_Data_Dissemination_April_2019_npidata_pfile_20050523-20190407.csv",
  "NPPES_Data_Dissemination_July_2020_npidata_pfile_20050523-20200712.csv",
  "NPPES_Data_Dissemination_May_2008_npidata_20050523-20080512.csv",
  "NPPES_Data_Dissemination_Nov_2007_npidata_20050523-20071112.csv",
  "SR74456_npidata_20050523-20090607.csv"
)

# Run the function
tables_created <- create_duckdb_tables(directory_path, file_names, con)
print(tables_created)

# List all tables in DuckDB to confirm
dbListTables(con)

# 'nppes_all'  cleaning ---------------------------------------------
# Define the list of table names
table_names <- dbListTables(con)

# Run the function
tables_processed <- process_tables(con, table_names)
# Writes to 
# all_data_NPPES <- readr::read_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/nppes_years/all_nppes_files.csv")
# View(all_data_NPPES)


# 'nppes_2024' read in one file at a time file to DuckDB ----
file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/NPPES_Data_Dissemination_April_2024/npidata_pfile_20050523-20240407.csv"

# Adjust SQL command with explicit CSV reading settings
 sql_command <- sprintf(
   "CREATE TABLE npi_2024 AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
   file_path
 )

# Execute the modified SQL command
dbExecute(con, sql_command)

# List all tables in DuckDB to confirm
dbListTables(con)

# Query some data to check
result <- dbGetQuery(con, "SELECT
                              NPI,
                              \"Entity Type Code\",
                              \"Provider First Name\",
                              \"Provider Last Name (Legal Name)\",
                              \"Provider Other First Name\",
                              \"Provider First Line Business Practice Location Address\",
                              \"Provider Business Practice Location Address City Name\",
                              \"Provider Business Practice Location Address State Name\",
                              \"Provider Business Practice Location Address Postal Code\",
                              \"Provider Business Practice Location Address Country Code (If outside U.S.)\",
                              \"Provider Business Practice Location Address Telephone Number\",
                              \"Provider First Line Business Mailing Address\",
                              \"Provider Business Mailing Address City Name\",
                              \"Provider Business Mailing Address State Name\",
                              \"Provider Business Mailing Address Country Code (If outside U.S.)\",
                              \"Provider Business Mailing Address Postal Code\",
                              \"Provider Gender Code\",
                              \"Provider Credential Text\",
                              \"Provider Organization Name (Legal Business Name)\",
                              \"Healthcare Provider Taxonomy Code_1\",
                              \"Healthcare Provider Taxonomy Code_2\",
                              \"Is Sole Proprietor\"
                            FROM npi_2024 LIMIT 5"); print(result)

# Use duckplyr to create a reference to the table without loading it into R
npi_2024 <- dplyr::tbl(con, "npi_2024"); glimpse(npi_2024)

nber_all_basic <- npi_2024 %>%
  duckplyr::select(
    NPI, # npi,
    `Entity Type Code`, # entity,
    # lastupdatestr,
    # penumdatestr,
    `Provider First Name`, # pfname,
    `Provider Last Name (Legal Name)`, # plname,
    `Provider Other First Name`, # pfnameoth,
    `Provider First Line Business Practice Location Address`, # plocline1,
    `Provider Business Practice Location Address City Name`, # ploccityname,
    `Provider Business Practice Location Address State Name`, # plocstatename,
    `Provider Business Practice Location Address Postal Code`, # ploczip,
    `Provider Business Practice Location Address Country Code (If outside U.S.)`, # ploccountry,
    `Provider Business Practice Location Address Telephone Number`, # ploctel,
    `Provider First Line Business Mailing Address`, # pmailline1,
    `Provider Business Mailing Address City Name`, # pmailcityname,
    `Provider Business Mailing Address State Name`, # pmailstatename,
    `Provider Business Mailing Address Country Code (If outside U.S.)`, # pmailcountry,
    `Provider Business Mailing Address Postal Code`, # pmailzip,
    `Provider Gender Code`, # pgender,
    `Provider Credential Text`, # pcredential,
    `Provider Organization Name (Legal Business Name)`, # porgname,
    `Healthcare Provider Taxonomy Code_1`, # ptaxcode1,
    `Healthcare Provider Taxonomy Code_2`, # ptaxcode2,
    `Is Sole Proprietor`# soleprop,
    # source
  ) %>%
  duckplyr::filter(`Entity Type Code` == 1,
                   `Provider Business Mailing Address Country Code (If outside U.S.)` == "US",
                   `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US",
                   `Healthcare Provider Taxonomy Code_1` %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X", "207VB0002X") |
                     `Healthcare Provider Taxonomy Code_2` %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X")) %>%
  #duckplyr::select(-aofname, -aolname, -aomname, -aoname_suffix, -aotelnum, -aocredential, -porgname) %>%
  #duckplyr::mutate(across(c(lastupdatestr, penumdatestr), lubridate::year)) %>%
  duckplyr::select(-`Entity Type Code`, -`Provider Other First Name`, -`Provider Business Practice Location Address Country Code (If outside U.S.)`, -`Provider Business Mailing Address Country Code (If outside U.S.)`) %>%
  duckplyr::mutate(across(c(`Provider First Name`, `Provider Last Name (Legal Name)`, `Provider First Line Business Practice Location Address` , `Provider Business Practice Location Address City Name`, `Provider First Line Business Mailing Address`, `Provider Business Mailing Address City Name`), .fns = ~str_to_upper(.))) %>%
  #Keeps only unique city,state,npi combinations so if someone moves in town we don't catch it.  
  duckplyr::distinct(NPI, `Provider Business Practice Location Address City Name`, `Provider Business Practice Location Address State Name`, .keep_all = TRUE)

npi_2024_all_collected <- nber_all_basic %>%
  duckplyr::collect() %>%
  mutate(`Provider Credential Text` = str_remove_all(`Provider Credential Text`, "[[\\p{P}][\\p{S}]]")) %>%
  # Remove MD punctuation.  
  mutate(`Provider Credential Text` = str_remove_all(`Provider Credential Text`, "[:blank:]")) %>%
  mutate(`Provider Credential Text` = str_to_upper(`Provider Credential Text`)) %>% 
  mutate(across(c(`Provider Business Mailing Address Postal Code`, `Provider Business Practice Location Address Postal Code`), .fns = ~str_sub(.,1 ,5)))  %>%
  filter(str_detect(`Provider Credential Text`, "MD") | str_detect(`Provider Credential Text`, fixed("DO"))) %>%
  mutate(`Provider Credential Text` = dplyr::recode(`Provider Credential Text`, "DO" = "DO", "DOFACOG" = "DO", "DOFACOGPA" = "DO", "DOFACOOG" = "DO", "DOFACOSDACFE" = "DO", "DOMD" = "DO", "DOMPH" = "DO", "DOMPHFAC" = "DO", "DOMS" = "DO", "DOPA" = "DO", "DOPC" = "DO", "DOPLLC" = "DO", .default = "MD")) %>%
  mutate(`Provider Gender Code` = dplyr::recode(`Provider Gender Code`, "F" = "Female", "M" = "Male")) %>%
  dplyr::arrange(NPI) %>%
  readr::write_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/sp_duckdb_npi_2024.csv"); gc()

glimpse(npi_2024_all_collected)

#Disconnect from the database when it is no longer needed
dbDisconnect(con)

# NB:  Augment the NBER data with the current NPPES files that were downloaded by 02.25-downloader:   "NPPES_Data_Dissemination_July_2020.zip",
#"NPPES_Data_Dissemination_October_2020.zip",
# "NPPES_Data_Disseminat_April_2021.zip",
# "NPPES_Data_Disseminat_April_2022.zip"


# ` -----------------------------------------------------------------------



# facility_affiliation Section----------------------------------------------------
create_facility_affiliation_tables <- function(directory_path, con) {
  conflicted::conflicts_prefer(stringr::str_remove_all)
  
  # Retrieve file names from the directory
  file_names <- list.files(directory_path)
  
  # Iterate over each file name provided
  for (i in seq_along(file_names)) {
    file_name <- file_names[i]
    full_path <- file.path(directory_path, file_name)
    table_name <- tools::file_path_sans_ext(gsub("[^A-Za-z0-9]", "_", file_name))  # Clean and create a valid table name
    table_name <- paste0(table_name, "_", i)  # Add counter to make table name unique
    
    # Construct SQL command to create a table
    sql_command <- sprintf(
      "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
      table_name, full_path
    )
    
    # Execute the SQL command
    dbExecute(con, sql_command)
    
    # Optional: output the list of tables after creation
    cat(sprintf("Table '%s' created from '%s'.\n", table_name, file_name))
  }
  
  # List all tables in DuckDB to confirm
  return(dbListTables(con))
}

# `facility_affiliation` Batch read in NPPES data to duckDB-------------------------
source("R/01-setup.R")
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

directory_path <- "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

created_tables <- create_facility_affiliation_tables(directory_path, con);created_tables

# `facility_affiliation` filtering ------------------------------------------
output_csv_path <- "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/facility_affiliation_merged_data.csv" #This can be loaded into exploratory to see how to use non-duckplyr verbs to clean it.  

# Call the process_duckdb_tables function
process_facility_affiliation_tables <- function(con, table_names, output_csv_path) {
  # Initialize an empty list to store the results
  results <- list()
  
  # Initialize an empty data frame to store merged data
  all_data <- data.frame()
  
  # Loop through each table name
  for (i in 1:length(table_names)) {
    table_name <- table_names[i]
    cat("Processing table:", table_name, "\n")
    
    # Use tryCatch to handle errors
    tryCatch({
      # Use duckplyr to create a reference to the table without loading it into R
      table_ref <- tbl(con, table_name)
      
      # Perform the data processing steps
      processed_data <- table_ref %>%
        dplyr::select(NPI) %>%
        # SELECT COLUMNS OF INTEREST
        #dplyr::filter(pri_spec %in% c("OBSTETRICS/GYNECOLOGY", "GYNECOLOGICAL ONCOLOGY") | sec_spec_1 %in% c("OBSTETRICS/GYNECOLOGY", "GYNECOLOGICAL ONCOLOGY") | sec_spec_2 %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY") | sec_spec_3 %in% c("OBSTETRICS/GYNECOLOGY", "GYNECOLOGICAL ONCOLOGY")) %>%
        dplyr::distinct(NPI, .keep_all = TRUE) %>%
        #dplyr::select(NPI, lst_nm, frst_nm, mid_nm, gndr, Cred, Med_sch, Grd_yr, hosp_afl_lbn_1) %>%
        dplyr::mutate(year = table_name)  # Add a new column "year" with the table name
      
      cat("Processed table:", table_name, "\n")
      cat("Writing processed data to CSV...\n")
      
      # Collect the processed data
      processed_data_df <- processed_data %>% collect()
      
      # Append the processed data to the merged data frame
      all_data <- dplyr::bind_rows(all_data, processed_data_df)
      
      # Store the table name in the results list
      results[[table_name]] <- processed_data
    }, error = function(e) {
      cat("Error processing table:", table_name, "\n")
      message("Error message:", e$message, "\n\n")
    })
  }
  
  cat("All tables processed.\n")
  
  # Write the merged data frame to disk
  write_csv(all_data, output_csv_path)
  print(output_csv_path)
  
  # Return the list of processed tables
  return(results)
}

processed_tables <- process_facility_affiliation_tables(con, table_names, output_csv_path)

# `facility_affiliation` sanity check --------------------------------------------
# Opens up the first table in the list that is 2017
first_processed_table <- processed_tables[[1]]

# Filter the data frame
first_processed_table %>%
  dplyr::filter(NPI == 1689603763L)

facility_affiliation_combined_df <- dplyr::bind_rows(processed_tables)

# 'facility_affiliation' collect the data --------------------------------------------
# Function to ensure full collection of each table
collect_and_convert <- function(tbl) {
  tbl %>%
    collect() %>%
    as.data.frame()
}

# Convert the list of tbl_duckdb_connection to a single dataframe
facility_affiliation_combined_df <- processed_tables %>%
  lapply(collect_and_convert) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(year = str_extract_after(year, "20")) %>%
  dplyr::mutate(year = str_remove_after(year, "_")) %>%
  dplyr::mutate(year = factor(year)) %>%
  dplyr::distinct(NPI, year, .keep_all = TRUE) %>%
  dplyr::ungroup()


# facility_affiliation_combined_df <- readr::read_csv("/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/end_facility_affiliation_combined_df.csv")

facility_affiliation_combined_df %>%
  dplyr::filter(NPI == 1689603763L)

# 'facility_affiliation' write csv --------------------------------------------
facility_affiliation_combined_df %>%
  readr::write_csv("/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/end_facility_affiliation_combined_df.csv"); gc()


# 'facility_affiliation' Consecutive function ----------------------------------------------------
# Read the data from the CSV file
data <- read.csv("/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/end_facility_affiliation_combined_df.csv")

# Convert year to numeric if it's read as character
data$year <- as.numeric(data$year)

# Group by NPI and calculate the last consecutive year
last_consecutive_year <- data %>%
  arrange(NPI, year) %>%
  group_by(NPI) %>%
  summarise(last_consecutive_year_facility_affiliation = max(base::cumsum(c(0, diff(year) != 1)) + year))

# Print the result
print(last_consecutive_year)

# Me :)
last_consecutive_year %>%
  dplyr::filter(NPI == 1689603763L)

# Sue Davidson
last_consecutive_year %>%
  dplyr::filter(NPI == 1508953654)

# Chris Carey
last_consecutive_year %>%
  dplyr::filter(NPI == 1972523165)

# Karlotta
last_consecutive_year %>%
  dplyr::filter(NPI == 1548363484)


last_consecutive_year <- last_consecutive_year %>%
  dplyr::mutate(last_consecutive_year = exploratory::recode(last_consecutive_year, `25` = 15, `26` = 16, `27` = 17, type_convert = TRUE)) %>%
  dplyr::rename(facility_affiliation_last_consecutive_year = last_consecutive_year) %>%
  
  # Removes 2023 and 2024 because those people likely have a facility affiliation.  
  dplyr::filter(facility_affiliation_last_consecutive_year <= 23) %>%
  dplyr::mutate(facility_affiliation_last_consecutive_year = facility_affiliation_last_consecutive_year + 2000L)

write_csv(last_consecutive_year, "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/end_facility_affiliation_last_consecutive_year.csv")

# last_consecutive_year <- read_csv("/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files/facility_affiliation_merged/end_facility_affiliation_last_consecutive_year.csv")

# ' -----------------------------------------------------------------------
# ' -----------------------------------------------------------------------


# Medicare_part_D_prescribers Section----------------------------------------------------
create_Medicare_part_D_prescribers_tables <- function(directory_path, con) {
  conflicted::conflicts_prefer(stringr::str_remove_all)
  
  # Retrieve file names from the directory
  file_names <- list.files(directory_path)
  
  # Iterate over each file name provided
  for (i in seq_along(file_names)) {
    file_name <- file_names[i]
    full_path <- file.path(directory_path, file_name)
    table_name <- tools::file_path_sans_ext(gsub("[^A-Za-z0-9]", "_", file_name))  # Clean and create a valid table name
    table_name <- paste0(table_name, "_", i)  # Add counter to make table name unique
    
    # Construct SQL command to create a table
    sql_command <- sprintf(
      "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
      table_name, full_path
    )
    
    # Execute the SQL command
    dbExecute(con, sql_command)
    
    # Optional: output the list of tables after creation
    cat(sprintf("Table '%s' created from '%s'.\n", table_name, file_name))
  }
  
  # List all tables in DuckDB to confirm
  return(dbListTables(con))
}

# `Medicare_part_D_prescribers` Batch read in NPPES data to duckDB-------------------------
source("R/01-setup.R")
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

directory_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

created_tables <- create_Medicare_part_D_prescribers_tables(directory_path, con);created_tables

# 'Medicare_part_D_prescribers_all'  cleaning ---------------------------------------------
# Specify the table names to process
dbListTables(con)
# table_names <- dbListTables(con)
# table_names <- table_names[grep("^doc_|^doctors_", table_names)]; table_names
table_names <-  
  c("MUP_DPR_RY21_P04_V10_DY13_NPI_csv_1",
    "MUP_DPR_RY21_P04_V10_DY14_NPI_csv_2",
    "MUP_DPR_RY21_P04_V10_DY15_NPI_csv_3",
    "MUP_DPR_RY21_P04_V10_DY16_NPI_csv_4",
    "MUP_DPR_RY21_P04_V10_DY17_NPI_csv_5",
    "MUP_DPR_RY21_P04_V10_DY18_NPI_csv_6",
    "MUP_DPR_RY21_P04_V10_DY19_NPI_csv_7",
    "MUP_DPR_RY22_P04_V10_DY20_NPI_csv_8",
    "MUP_DPR_RY23_P04_V10_DY21_NPI_csv_9")


# `Medicare_part_D_prescribers` Synchronizing column names ----------------------------------------------
# Initialize an empty list to store column names for each table
all_column_names <- list()

# Iterate over table names and retrieve column names
for (table_name in table_names) {
  # Read table from the database
  table_data <- dbReadTable(con, table_name)
  
  # Get column names
  column_names <- names(table_data)
  
  # Store column names in the list
  all_column_names[[table_name]] <- column_names
}

# Print column names for each table
for (table_name in table_names) {
  print(paste("Column names for table", table_name, ":", paste(all_column_names[[table_name]], collapse = ", ")))
}


# `Medicare_part_D_prescribers` filtering ------------------------------------------
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

output_csv_path <- "/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv" #This can be loaded into exploratory to see how to use non-duckplyr verbs to clean it.  
#read_csv("/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged_data.csv")

# Call the process_duckdb_tables function
process_Medicare_part_D_prescribers_tables <- function(con, table_names, output_csv_path) {
  # Initialize an empty list to store the results
  results <- list()
  
  # Initialize an empty data frame to store merged data
  all_data <- data.frame()
  
  # Check if the directory exists, create it if not
  if (!dir.exists(dirname(output_csv_path))) {
    dir.create(dirname(output_csv_path), recursive = TRUE)
  }
  
  # Loop through each table name
  for (i in 1:length(table_names)) {
    table_name <- table_names[i]
    cat("Processing table:", table_name, "\n")
    
    # Use tryCatch to handle errors
    tryCatch({
      # Use duckplyr to create a reference to the table without loading it into R
      table_ref <- tbl(con, table_name)
      
      # Perform the data processing steps
      processed_data <- table_ref %>%
        dplyr::filter(Prscrbr_Type %in% c("Gynecological Oncology", "Obstetrics & Gynecology") & Prscrbr_Cntry == "US") %>%
        dplyr::select(PRSCRBR_NPI, Tot_Clms) %>%
        dplyr::filter(Tot_Clms < 50000) %>%
        dplyr::mutate(Prescribed = "Prescription written") %>%
        dplyr::distinct(PRSCRBR_NPI, .keep_all = TRUE) %>%
        dplyr::mutate(year = table_name)  # Add a new column "year" with the table name
      
      cat("Processed table:", table_name, "\n")
      cat("Writing processed data to CSV...\n")
      
      # Collect the processed data
      processed_data_df <- processed_data %>% collect()
      
      # Append the processed data to the merged data frame
      all_data <- dplyr::bind_rows(all_data, processed_data_df)
      
      # Store the table name in the results list
      results[[table_name]] <- processed_data
    }, error = function(e) {
      cat("Error processing table:", table_name, "\n")
      message("Error message:", e$message, "\n\n")
    })
  }
  
  cat("All tables processed.\n")
  
  # Write the merged data frame to disk
  tryCatch({
    write_csv(all_data, output_csv_path)
    print(output_csv_path)
  }, error = function(e) {
    cat("Error writing to CSV:", e$message, "\n")
  })
  
  # Return the list of processed tables
  return(results)
}

processed_tables <- process_Medicare_part_D_prescribers_tables(con, table_names, output_csv_path)

# `Medicare_part_D_prescribers` sanity check --------------------------------------------
# Opens up the first table in the list that is 2017
first_processed_table <- processed_tables[[1]]

# Filter the data frame
first_processed_table %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L)

# 'Medicare_part_D_prescribers' collect the data --------------------------------------------
# Function to ensure full collection of each table
collect_and_convert <- function(tbl) {
  tbl %>%
    collect() %>%
    as.data.frame()
}

# Convert the list of tbl_duckdb_connection to a single dataframe
Medicare_part_D_prescribers_combined_df <- processed_tables %>%
  lapply(collect_and_convert) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(year = str_extract_after(year, "DY")) %>%
  dplyr::mutate(year = factor(year)) %>%
  dplyr::distinct(PRSCRBR_NPI, year, .keep_all = TRUE) %>%
  dplyr::ungroup()


# Medicare_part_D_prescribers_combined_df <- readr::read_csv("/Volumes/Video Projects Muffly 1/Medicare_part_D_prescribers/unzipped_files/Medicare_part_D_prescribers_merged/end_Medicare_part_D_prescribers_combined_df.csv")

Medicare_part_D_prescribers_combined_df %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L)

Medicare_part_D_prescribers_combined_df %>%
  dplyr::filter(PRSCRBR_NPI == 1508953654)

# 'Medicare_part_D_prescribers' write csv --------------------------------------------
Medicare_part_D_prescribers_combined_df %>%
  readr::write_csv("/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"); gc()
# readr::read_csv("/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv")


# 'Medicare_part_D_prescribers' Consecutive function ----------------------------------------------------
# Read the data from the CSV file
data <- read.csv("/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv")

# Convert year to numeric if it's read as character
data$year <- sub(".*RY(\\d+).*", "\\1", data$year)
# Prepend "20" to the year to convert it to a four-digit year
data$year <- paste0("20", data$year)
# Remove everything after "_" using regex
data$year <- sub("_.*", "", data$year)
data$year <- as.numeric(data$year)

# Group by NPI and calculate the last consecutive year
last_consecutive_year <- data %>%
  arrange(PRSCRBR_NPI, year) %>%
  group_by(PRSCRBR_NPI) %>%
  summarise(last_consecutive_year_Medicare_part_D_prescribers = max(base::cumsum(c(0, diff(year) != 1)) + year))

# Print the result
print(last_consecutive_year)

# Me :)
last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1689603763L)

# Sue Davidson
last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1508953654)

# Chris Carey
last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1972523165)

# Karlotta
last_consecutive_year %>%
  dplyr::filter(PRSCRBR_NPI == 1548363484)

write_csv(last_consecutive_year, "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_last_consecutive_year.csv")


# ' -----------------------------------------------------------------------
# ' -----------------------------------------------------------------------

# open_payments Section----------------------------------------------------

# `open_payments` Batch read in NPPES data to duckDB-------------------------
source("R/01-setup.R")
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

directory_path <- "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files"
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

create_open_payments_tables <- function(directory_path, con, clean_names = TRUE, overwrite = FALSE, notify_every = 10) {
  # Ensure str_remove_all from stringr is preferred in case of conflicts
  conflicted::conflicts_prefer(stringr::str_remove_all)
  
  # Retrieve file names from the directory
  file_names <- list.files(directory_path, full.names = TRUE)
  
  # Function to create valid table names
  clean_table_name <- function(file_name, clean_names) {
    base_name <- file_path_sans_ext(basename(file_name))
    if (clean_names) {
      cleaned_name <- str_remove_all(base_name, "[^A-Za-z0-9]")
    } else {
      cleaned_name <- base_name
    }
    cleaned_name
  }
  
  # Initialize a vector to store created table names
  created_tables <- character()
  skipped_tables <- character()
  error_messages <- list()
  
  # Capture start time
  start_time <- Sys.time()
  last_notify_time <- start_time
  
  # Function to log messages with timestamp
  log_message <- function(message) {
    cat(sprintf("[%s] %s\n", Sys.time(), message))
  }
  
  # Function to process each file
  process_file <- function(full_path, clean_names, overwrite, con, i) {
    table_name <- clean_table_name(full_path, clean_names)
    
    if (overwrite && table_name %in% dbListTables(con)) {
      dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", table_name))
      log_message(sprintf("Table '%s' dropped.", table_name))
    }
    
    # Construct SQL command to create a table
    sql_command <- sprintf(
      "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
      table_name, full_path
    )
    
    # Execute the SQL command with error handling
    tryCatch({
      dbExecute(con, sql_command)
      log_message(sprintf("Table '%s' created from '%s'.", table_name, basename(full_path)))
      list(success = TRUE, table_name = table_name, error = NULL)
    }, error = function(e) {
      log_message(sprintf("Error creating table from '%s': %s", basename(full_path), e$message))
      list(success = FALSE, table_name = table_name, error = e$message)
    })
  }
  
  # Process files sequentially
  for (i in seq_along(file_names)) {
    full_path <- file_names[i]
    result <- process_file(full_path, clean_names, overwrite, con, i)
    
    if (result$success) {
      created_tables <- c(created_tables, result$table_name)
    } else {
      skipped_tables <- c(skipped_tables, result$table_name)
      error_messages <- c(error_messages, result$error)
    }
    
    # Print elapsed time every 10 seconds
    current_time <- Sys.time()
    if (as.numeric(difftime(current_time, last_notify_time, units = "secs")) >= 10) {
      elapsed_time <- current_time - start_time
      log_message(sprintf("Elapsed time after %d files: %s", i, as.character(elapsed_time)))
      beepr::beep(1)  # System beep to indicate progress
      last_notify_time <- current_time
    }
  }
  
  # List all tables in DuckDB to confirm
  all_tables <- dbListTables(con)
  
  # Ensure connection is properly closed
  dbDisconnect(con)
  
  # Final beep to indicate completion
  beepr::beep(2)
  
  # Summary of results
  summary <- list(
    created_tables = created_tables,
    skipped_tables = skipped_tables,
    error_messages = error_messages,
    all_tables = all_tables
  )
  
  return(summary)
}

#Usage:  
created_tables <- create_open_payments_tables(
  directory_path,
  con = con,
  clean_names = FALSE,
  overwrite = TRUE,
  notify_every = 1
)
created_tables

# 'open_payments_all'  cleaning ---------------------------------------------
# Specify the table names to process
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
dbListTables(con)
# table_names <- dbListTables(con)
# table_names <- table_names[grep("^OP_DTL_GNRL_|^OP_DTL_RSRCH_", table_names)]; table_names
table_names <- c(
    "OP_DTL_GNRL_PGYR2014_P06302021",
    "OP_DTL_GNRL_PGYR2015_P06302021",
    "OP_DTL_GNRL_PGYR2016_P01182024",
    "OP_DTL_GNRL_PGYR2017_P01182024",
    "OP_DTL_GNRL_PGYR2018_P01182024",
    "OP_DTL_GNRL_PGYR2019_P01182024",
    "OP_DTL_GNRL_PGYR2020_P01182024",
    "OP_DTL_GNRL_PGYR2021_P01182024",
    "OP_DTL_GNRL_PGYR2022_P01182024",
    "OP_DTL_OWNRSHP_PGYR2016_P01182024",
    "OP_DTL_OWNRSHP_PGYR2017_P01182024",
    "OP_DTL_OWNRSHP_PGYR2018_P01182024",
    "OP_DTL_OWNRSHP_PGYR2019_P01182024",
    "OP_DTL_OWNRSHP_PGYR2020_P01182024",
    "OP_DTL_OWNRSHP_PGYR2021_P01182024",
    "OP_DTL_OWNRSHP_PGYR2022_P01182024",
    "OP_DTL_RSRCH_PGYR2016_P01182024",
    "OP_DTL_RSRCH_PGYR2017_P01182024",
    "OP_DTL_RSRCH_PGYR2018_P01182024",
    "OP_DTL_RSRCH_PGYR2019_P01182024",
    "OP_DTL_RSRCH_PGYR2020_P01182024",
    "OP_DTL_RSRCH_PGYR2021_P01182024",
    "OP_DTL_RSRCH_PGYR2022_P01182024")

# `open_payments` Synchronizing column names ----------------------------------------------

# Load necessary libraries
library(tidyverse)
library(openxlsx)

# Function to convert the list into a data frame with each column name in a separate column
convert_list_to_df_expanded <- function(column_list) {
  filtered_list <- column_list[grep("^OP_DTL_GNRL_", names(column_list))]
  
  # Create a data frame with Table names
  df <- tibble(Table = names(filtered_list))
  
  # Add each column name as a separate column in the data frame
  max_cols <- max(sapply(filtered_list, length))
  for (i in seq_len(max_cols)) {
    df[[paste0("Column_", i)]] <- sapply(filtered_list, function(x) if (length(x) >= i) x[i] else NA)
  }
  
  return(df)
}

# Convert the filtered and expanded list to a data frame
columns_df_expanded <- convert_list_to_df_expanded(all_columns_list)

# Specify the output path for the Excel file
output_excel_path <- "/Users/tylermuffly/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/retirement/op_dtl_gnrl_columns_list.xlsx"

# Save the data frame as an Excel file
write.xlsx(columns_df_expanded, file = output_excel_path, asTable = TRUE)

# Log that the file has been saved
log_message(sprintf("The columns list has been saved to: %s", output_excel_path))


################################
# `open_payments` filtering ------------------------------------------
duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)

output_csv_path <- "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/final/open_payments_merged_data.csv" 

# All these tables had the same columns.  See "/Users/tylermuffly/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/retirement/op_dtl_gnrl_columns_list.xlsx"
table_names <- c("OP_DTL_GNRL_PGYR2017_P01182024",
                   "OP_DTL_GNRL_PGYR2018_P01182024",
                   "OP_DTL_GNRL_PGYR2019_P01182024",
                   "OP_DTL_GNRL_PGYR2020_P01182024",
                   "OP_DTL_GNRL_PGYR2021_P01182024",
                   "OP_DTL_GNRL_PGYR2022_P01182024")

# Custom function to log messages with timestamps
log_message <- function(message) {
  cat(sprintf("[%s] %s\n", Sys.time(), message))
}

process_open_payments_tables <- function(con, table_names, output_csv_path) {
  # Initialize an empty list to store the results
  results <- list()
  
  # Check if the directory exists, create it if not
  if (!dir.exists(dirname(output_csv_path))) {
    dir.create(dirname(output_csv_path), recursive = TRUE)
  }
  
  # Initialize the output file by writing the header
  write_csv(data.frame(), output_csv_path, append = FALSE, col_names = TRUE)
  
  # Loop through each table name
  for (i in seq_along(table_names)) {
    table_name <- table_names[i]
    log_message(sprintf("Processing table: %s", table_name))
    
    # Use tryCatch to handle errors
    tryCatch({
      # Use dbplyr to create a reference to the table without loading it into R
      table_ref <- tbl(con, table_name)
      
      log_message(sprintf("Successfully created table reference for: %s", table_name))
      
      # Log the table structure
      table_structure <- dbGetQuery(con, sprintf("PRAGMA table_info(%s)", table_name))
      log_message(sprintf("Table structure of %s: %s", table_name, paste(colnames(table_structure), collapse = ", ")))
      
      # Check if 'Recipient_Country' exists in the table structure
      if ("Recipient_Country" %in% table_structure$name) {
        country_filter <- table_ref %>%
          duckplyr::filter(Recipient_Country == "United States")
        log_message(sprintf("'Recipient_Country' filter applied for table: %s", table_name))
      } else {
        country_filter <- table_ref
        log_message(sprintf("'Recipient_Country' column does not exist in table: %s. Skipping this filter.", table_name))
      }
      
      # Perform the data processing steps to extract NPI/PPI and payment date
      filtered_data <- country_filter %>%
        duckplyr::select(
          Covered_Recipient_NPI, 
          Date_of_Payment, 
          Covered_Recipient_Specialty_1
        ) %>%
        duckplyr::filter(
          Covered_Recipient_Specialty_1 %in% c(
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Critical Care Medicine",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Female Pelvic Medicine and Reconstructive Surgery",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Gynecologic Oncology",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Gynecology",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Hospice and Palliative Medicine",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Maternal & Fetal Medicine",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Obstetrics",
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Reproductive Endocrinology"
          )
        )
      log_message(sprintf("Filtered data for table: %s", table_name))
      
      # Get the count of filtered data
      filtered_data_count <- filtered_data %>% summarise(count = n()) %>% collect() %>% pull(count)
      log_message(sprintf("Number of rows in filtered data for table %s: %d", table_name, filtered_data_count))
      
      # If filtered data is not empty, proceed to save it
      if (filtered_data_count > 0) {
        # Collect and write the filtered data in chunks
        chunk_size <- 100000  # Define the chunk size
        num_chunks <- base::ceiling(filtered_data_count / chunk_size)  # Use base::ceiling to avoid conflicts
        
        for (j in seq_len(num_chunks)) {
          chunk_start <- (j - 1) * chunk_size + 1
          chunk_end <- min(j * chunk_size, filtered_data_count)
          
          chunk_data <- filtered_data %>%
            filter(row_number() >= chunk_start & row_number() <= chunk_end) %>%
            collect()
          
          log_message(sprintf("Writing %d rows for table: %s, chunk: %d", nrow(chunk_data), table_name, j))
          write_csv(chunk_data, output_csv_path, append = TRUE, col_names = (i == 1 && j == 1))
        }
        
        # Store the table name in the results list
        results[[table_name]] <- filtered_data
      } else {
        log_message(sprintf("No filtered data to write for table: %s", table_name))
      }
    }, error = function(e) {
      log_message(sprintf("Error processing table: %s", table_name))
      log_message(sprintf("Error message: %s", e$message))
    })
  }
  
  log_message("All tables processed.")
  
  # Return the list of processed tables
  return(results)
}

processed_tables <- process_open_payments_tables(con, table_names, output_csv_path)

# Check if there is data written to the CSV file
file_info <- file.info(output_csv_path)
if (file_info$size == 0) {
  log_message("The output CSV file is empty. No data was written.")
} else {
  log_message("Data successfully written to the output CSV file.")
}

head(read_csv(output_csv_path))

# `open_payments` sanity check --------------------------------------------
# Opens up the first table in the list that is 2017
first_processed_table <- processed_tables[[1]]

# Filter the data frame
first_processed_table %>%
  dplyr::filter(Covered_Recipient_NPI == 1689603763L)

# 'open_payments' collect the data --------------------------------------------
# Function to ensure full collection of each table
collect_and_convert <- function(tbl) {
  tbl %>%
    collect() %>%
    as.data.frame()
}

# Convert the list of tbl_duckdb_connection to a single dataframe
open_payments_combined_df <- processed_tables %>%
  lapply(collect_and_convert) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(Payment_Year = lubridate::year(Date_of_Payment)) %>%
  group_by(Covered_Recipient_NPI) %>%
  summarize(Latest_Payment_Year = max(Payment_Year, na.rm = TRUE)); open_payments_combined_df

readr::write_csv(open_payments_combined_df, "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/open_payments_merged/end_open_payments_combined_df.csv")

# open_payments_combined_df <- readr::read_csv("/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/open_payments_merged/end_open_payments_combined_df.csv")

open_payments_combined_df %>%
  dplyr::filter(Covered_Recipient_NPI == 1689603763L)

open_payments_combined_df %>%
  dplyr::filter(Covered_Recipient_NPI == 1508953654)

# 'open_payments' write csv --------------------------------------------
open_payments_combined_df %>%
  readr::write_csv("/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/final/end_open_payments_combined_df.csv"); gc()



# 'open_payments' Consecutive function ----------------------------------------------------
# Read the data from the CSV file
data <- read.csv("/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/final/end_open_payments_combined_df.csv")

# Convert year to numeric if it's read as character
data$year <- sub(".*RY(\\d+).*", "\\1", data$year)
# Prepend "20" to the year to convert it to a four-digit year
data$year <- paste0("20", data$year)
# Remove everything after "_" using regex
data$year <- sub("_.*", "", data$year)
data$year <- as.numeric(data$year)

# Group by NPI and calculate the last consecutive year
last_consecutive_year <- data %>%
  arrange(Covered_Recipient_NPI, Date_of_Payment) %>%
  group_by(Covered_Recipient_NPI) %>%
  summarise(last_consecutive_year_open_payments = max(base::cumsum(c(0, diff(year) != 1)) + year))

# Print the result
print(last_consecutive_year)

# Me :)
last_consecutive_year %>%
  dplyr::filter(Covered_Recipient_NPI == 1689603763L)

# Sue Davidson
last_consecutive_year %>%
  dplyr::filter(Covered_Recipient_NPI == 1508953654)

# Chris Carey
last_consecutive_year %>%
  dplyr::filter(Covered_Recipient_NPI == 1972523165)

# Karlotta
last_consecutive_year %>%
  dplyr::filter(Covered_Recipient_NPI == 1548363484)

write_csv(last_consecutive_year, "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/final/end_open_payments_last_consecutive_year.csv")

#last_consecutive_year <- read_csv("/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_open_payments_last_consecutive_year.csv")

# Disconnect from the database when it is no longer needed ----------------
dbDisconnect(con)

# # 'nppes' read in NPPES data one NPI file at a time
# # Specify the path for the DuckDB database file
# duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/my_duckdb.duckdb"  
# 
# # Connect to DuckDB with the specified database file
# con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
# 
# # List all tables in DuckDB
# dbListTables(con)


# `nppes` Crosswalk from NPPES to NBER headers ----------------------------
c(
  `aocredential` = `Authorized Official Credential Text`,
  `aofname` = `Authorized Official First Name`,
  `aolname` = `Authorized Official Last Name`,
  `aomname` = `Authorized Official Middle Name`,
  `aoname_prefix` = `Authorized Official Name Prefix Text`,
  `aoname_suffix` = `Authorized Official Name Suffix Text`,
  `aotelnum` = `Authorized Official Telephone Number`,
  `aotitle` = `Authorized Official Title or Position`,
  `ein` = `Employer Identification Number (EIN)`,
  `entity` = `Entity Type Code`,
  `ptaxcode1` = `Healthcare Provider Taxonomy Code_1`,
  #`ptaxcode10` = `Healthcare Provider Taxonomy Code_10`,
  `ptaxcode11` = `Healthcare Provider Taxonomy Code_11`,
  `ptaxcode12` = `Healthcare Provider Taxonomy Code_12`,
  `ptaxcode13` = `Healthcare Provider Taxonomy Code_13`,
  `ptaxcode14` = `Healthcare Provider Taxonomy Code_14`,
  `ptaxcode15` = `Healthcare Provider Taxonomy Code_15`,
  `ptaxcode2` = `Healthcare Provider Taxonomy Code_2`,
  `ptaxcode3` = `Healthcare Provider Taxonomy Code_3`,
  `ptaxcode4` = `Healthcare Provider Taxonomy Code_4`,
  `ptaxcode5` = `Healthcare Provider Taxonomy Code_5`,
  `ptaxcode6` = `Healthcare Provider Taxonomy Code_6`,
  `ptaxcode7` = `Healthcare Provider Taxonomy Code_7`,
  `ptaxcode8` = `Healthcare Provider Taxonomy Code_8`,
  `ptaxcode9` = `Healthcare Provider Taxonomy Code_9`,
  `ptaxgroup1` = `Healthcare Provider Taxonomy Group_1`,
  `ptaxgroup10` = `Healthcare Provider Taxonomy Group_10`,
  `ptaxgroup11` = `Healthcare Provider Taxonomy Group_11`,
  `ptaxgroup12` = `Healthcare Provider Taxonomy Group_12`,
  `ptaxgroup13` = `Healthcare Provider Taxonomy Group_13`,
  `ptaxgroup14` = `Healthcare Provider Taxonomy Group_14`,
  `ptaxgroup15` = `Healthcare Provider Taxonomy Group_15`,
  `ptaxgroup2` = `Healthcare Provider Taxonomy Group_2`,
  `ptaxgroup3` = `Healthcare Provider Taxonomy Group_3`,
  `ptaxgroup4` = `Healthcare Provider Taxonomy Group_4`,
  `ptaxgroup5` = `Healthcare Provider Taxonomy Group_5`,
  `ptaxgroup6` = `Healthcare Provider Taxonomy Group_6`,
  `ptaxgroup7` = `Healthcare Provider Taxonomy Group_7`,
  `ptaxgroup8` = `Healthcare Provider Taxonomy Group_8`,
  `ptaxgroup9` = `Healthcare Provider Taxonomy Group_9`,
  `orgsubpart` = `Is Organization Subpart`,
  `soleprop` = `Is Sole Proprietor`,
  `lastupdatestr` = `Last Update Date`,
  `npi` = `NPI`,
  `npideactdate` = `NPI Deactivation Date`,
  `npideactreason` = `NPI Deactivation Reason Code`,
  `npireactdate` = `NPI Reactivation Date`,
  `othpid1` = `Other Provider Identifier_1`,
  `othpid10` = `Other Provider Identifier_10`,
  `othpid11` = `Other Provider Identifier_11`,
  `othpid12` = `Other Provider Identifier_12`,
  `othpid13` = `Other Provider Identifier_13`,
  `othpid14` = `Other Provider Identifier_14`,
  `othpid15` = `Other Provider Identifier_15`,
  `othpid16` = `Other Provider Identifier_16`,
  `othpid17` = `Other Provider Identifier_17`,
  `othpid18` = `Other Provider Identifier_18`,
  `othpid19` = `Other Provider Identifier_19`,
  `othpid2` = `Other Provider Identifier_2`,
  `othpid20` = `Other Provider Identifier_20`,
  `othpid21` = `Other Provider Identifier_21`,
  `othpid22` = `Other Provider Identifier_22`,
  `othpid23` = `Other Provider Identifier_23`,
  `othpid24` = `Other Provider Identifier_24`,
  `othpid25` = `Other Provider Identifier_25`,
  `othpid26` = `Other Provider Identifier_26`,
  `othpid27` = `Other Provider Identifier_27`,
  `othpid28` = `Other Provider Identifier_28`,
  `othpid29` = `Other Provider Identifier_29`,
  `othpid3` = `Other Provider Identifier_3`,
  `othpid30` = `Other Provider Identifier_30`,
  `othpid31` = `Other Provider Identifier_31`,
  `othpid32` = `Other Provider Identifier_32`,
  `othpid33` = `Other Provider Identifier_33`,
  `othpid34` = `Other Provider Identifier_34`,
  `othpid35` = `Other Provider Identifier_35`,
  `othpid36` = `Other Provider Identifier_36`,
  `othpid37` = `Other Provider Identifier_37`,
  `othpid38` = `Other Provider Identifier_38`,
  `othpid39` = `Other Provider Identifier_39`,
  `othpid4` = `Other Provider Identifier_4`,
  `othpid40` = `Other Provider Identifier_40`,
  `othpid41` = `Other Provider Identifier_41`,
  `othpid42` = `Other Provider Identifier_42`,
  `othpid43` = `Other Provider Identifier_43`,
  `othpid44` = `Other Provider Identifier_44`,
  `othpid45` = `Other Provider Identifier_45`,
  `othpid46` = `Other Provider Identifier_46`,
  `othpid47` = `Other Provider Identifier_47`,
  `othpid48` = `Other Provider Identifier_48`,
  `othpid49` = `Other Provider Identifier_49`,
  `othpid5` = `Other Provider Identifier_5`,
  `othpid50` = `Other Provider Identifier_50`,
  `othpid6` = `Other Provider Identifier_6`,
  `othpid7` = `Other Provider Identifier_7`,
  `othpid8` = `Other Provider Identifier_8`,
  `othpid9` = `Other Provider Identifier_9`,
  `othpidiss1` = `Other Provider Identifier Issuer_1`,
  `othpidiss10` = `Other Provider Identifier Issuer_10`,
  `othpidiss11` = `Other Provider Identifier Issuer_11`,
  `othpidiss12` = `Other Provider Identifier Issuer_12`,
  `othpidiss13` = `Other Provider Identifier Issuer_13`,
  `othpidiss14` = `Other Provider Identifier Issuer_14`,
  `othpidiss15` = `Other Provider Identifier Issuer_15`,
  `othpidiss16` = `Other Provider Identifier Issuer_16`,
  `othpidiss17` = `Other Provider Identifier Issuer_17`,
  `othpidiss18` = `Other Provider Identifier Issuer_18`,
  `othpidiss19` = `Other Provider Identifier Issuer_19`,
  `othpidiss2` = `Other Provider Identifier Issuer_2`,
  `othpidiss20` = `Other Provider Identifier Issuer_20`,
  `othpidiss21` = `Other Provider Identifier Issuer_21`,
  `othpidiss22` = `Other Provider Identifier Issuer_22`,
  `othpidiss23` = `Other Provider Identifier Issuer_23`,
  `othpidiss24` = `Other Provider Identifier Issuer_24`,
  `othpidiss25` = `Other Provider Identifier Issuer_25`,
  `othpidiss26` = `Other Provider Identifier Issuer_26`,
  `othpidiss27` = `Other Provider Identifier Issuer_27`,
  `othpidiss28` = `Other Provider Identifier Issuer_28`,
  `othpidiss29` = `Other Provider Identifier Issuer_29`,
  `othpidiss3` = `Other Provider Identifier Issuer_3`,
  `othpidiss30` = `Other Provider Identifier Issuer_30`,
  `othpidiss31` = `Other Provider Identifier Issuer_31`,
  `othpidiss32` = `Other Provider Identifier Issuer_32`,
  `othpidiss33` = `Other Provider Identifier Issuer_33`,
  `othpidiss34` = `Other Provider Identifier Issuer_34`,
  `othpidiss35` = `Other Provider Identifier Issuer_35`,
  `othpidiss36` = `Other Provider Identifier Issuer_36`,
  `othpidiss37` = `Other Provider Identifier Issuer_37`,
  `othpidiss38` = `Other Provider Identifier Issuer_38`,
  `othpidiss39` = `Other Provider Identifier Issuer_39`,
  `othpidiss4` = `Other Provider Identifier Issuer_4`,
  `othpidiss40` = `Other Provider Identifier Issuer_40`,
  `othpidiss41` = `Other Provider Identifier Issuer_41`,
  `othpidiss42` = `Other Provider Identifier Issuer_42`,
  `othpidiss43` = `Other Provider Identifier Issuer_43`,
  `othpidiss44` = `Other Provider Identifier Issuer_44`,
  `othpidiss45` = `Other Provider Identifier Issuer_45`,
  `othpidiss46` = `Other Provider Identifier Issuer_46`,
  `othpidiss47` = `Other Provider Identifier Issuer_47`,
  `othpidiss48` = `Other Provider Identifier Issuer_48`,
  `othpidiss49` = `Other Provider Identifier Issuer_49`,
  `othpidiss5` = `Other Provider Identifier Issuer_5`,
  `othpidiss50` = `Other Provider Identifier Issuer_50`,
  `othpidiss6` = `Other Provider Identifier Issuer_6`,
  `othpidiss7` = `Other Provider Identifier Issuer_7`,
  `othpidiss8` = `Other Provider Identifier Issuer_8`,
  `othpidiss9` = `Other Provider Identifier Issuer_9`,
  `othpidst1` = `Other Provider Identifier State_1`,
  `othpidst10` = `Other Provider Identifier State_10`,
  `othpidst11` = `Other Provider Identifier State_11`,
  `othpidst12` = `Other Provider Identifier State_12`,
  `othpidst13` = `Other Provider Identifier State_13`,
  `othpidst14` = `Other Provider Identifier State_14`,
  `othpidst15` = `Other Provider Identifier State_15`,
  `othpidst16` = `Other Provider Identifier State_16`,
  `othpidst17` = `Other Provider Identifier State_17`,
  `othpidst18` = `Other Provider Identifier State_18`,
  `othpidst19` = `Other Provider Identifier State_19`,
  `othpidst2` = `Other Provider Identifier State_2`,
  `othpidst20` = `Other Provider Identifier State_20`,
  `othpidst21` = `Other Provider Identifier State_21`,
  `othpidst22` = `Other Provider Identifier State_22`,
  `othpidst23` = `Other Provider Identifier State_23`,
  `othpidst24` = `Other Provider Identifier State_24`,
  `othpidst25` = `Other Provider Identifier State_25`,
  `othpidst26` = `Other Provider Identifier State_26`,
  `othpidst27` = `Other Provider Identifier State_27`,
  `othpidst28` = `Other Provider Identifier State_28`,
  `othpidst29` = `Other Provider Identifier State_29`,
  `othpidst3` = `Other Provider Identifier State_3`,
  `othpidst30` = `Other Provider Identifier State_30`,
  `othpidst31` = `Other Provider Identifier State_31`,
  `othpidst32` = `Other Provider Identifier State_32`,
  `othpidst33` = `Other Provider Identifier State_33`,
  `othpidst34` = `Other Provider Identifier State_34`,
  `othpidst35` = `Other Provider Identifier State_35`,
  `othpidst36` = `Other Provider Identifier State_36`,
  `othpidst37` = `Other Provider Identifier State_37`,
  `othpidst38` = `Other Provider Identifier State_38`,
  `othpidst39` = `Other Provider Identifier State_39`,
  `othpidst4` = `Other Provider Identifier State_4`,
  `othpidst40` = `Other Provider Identifier State_40`,
  `othpidst41` = `Other Provider Identifier State_41`,
  `othpidst42` = `Other Provider Identifier State_42`,
  `othpidst43` = `Other Provider Identifier State_43`,
  `othpidst44` = `Other Provider Identifier State_44`,
  `othpidst45` = `Other Provider Identifier State_45`,
  `othpidst46` = `Other Provider Identifier State_46`,
  `othpidst47` = `Other Provider Identifier State_47`,
  `othpidst48` = `Other Provider Identifier State_48`,
  `othpidst49` = `Other Provider Identifier State_49`,
  `othpidst5` = `Other Provider Identifier State_5`,
  `othpidst50` = `Other Provider Identifier State_50`,
  `othpidst6` = `Other Provider Identifier State_6`,
  `othpidst7` = `Other Provider Identifier State_7`,
  `othpidst8` = `Other Provider Identifier State_8`,
  `othpidst9` = `Other Provider Identifier State_9`,
  `othpidty1` = `Other Provider Identifier Type Code_1`,
  `othpidty10` = `Other Provider Identifier Type Code_10`,
  `othpidty11` = `Other Provider Identifier Type Code_11`,
  `othpidty12` = `Other Provider Identifier Type Code_12`,
  `othpidty13` = `Other Provider Identifier Type Code_13`,
  `othpidty14` = `Other Provider Identifier Type Code_14`,
  `othpidty15` = `Other Provider Identifier Type Code_15`,
  `othpidty16` = `Other Provider Identifier Type Code_16`,
  `othpidty17` = `Other Provider Identifier Type Code_17`,
  `othpidty18` = `Other Provider Identifier Type Code_18`,
  `othpidty19` = `Other Provider Identifier Type Code_19`,
  `othpidty2` = `Other Provider Identifier Type Code_2`,
  `othpidty20` = `Other Provider Identifier Type Code_20`,
  `othpidty21` = `Other Provider Identifier Type Code_21`,
  `othpidty22` = `Other Provider Identifier Type Code_22`,
  `othpidty23` = `Other Provider Identifier Type Code_23`,
  `othpidty24` = `Other Provider Identifier Type Code_24`,
  `othpidty25` = `Other Provider Identifier Type Code_25`,
  `othpidty26` = `Other Provider Identifier Type Code_26`,
  `othpidty27` = `Other Provider Identifier Type Code_27`,
  `othpidty28` = `Other Provider Identifier Type Code_28`,
  `othpidty29` = `Other Provider Identifier Type Code_29`,
  `othpidty3` = `Other Provider Identifier Type Code_3`,
  `othpidty30` = `Other Provider Identifier Type Code_30`,
  `othpidty31` = `Other Provider Identifier Type Code_31`,
  `othpidty32` = `Other Provider Identifier Type Code_32`,
  `othpidty33` = `Other Provider Identifier Type Code_33`,
  `othpidty34` = `Other Provider Identifier Type Code_34`,
  `othpidty35` = `Other Provider Identifier Type Code_35`,
  `othpidty36` = `Other Provider Identifier Type Code_36`,
  `othpidty37` = `Other Provider Identifier Type Code_37`,
  `othpidty38` = `Other Provider Identifier Type Code_38`,
  `othpidty39` = `Other Provider Identifier Type Code_39`,
  `othpidty4` = `Other Provider Identifier Type Code_4`,
  `othpidty40` = `Other Provider Identifier Type Code_40`,
  `othpidty41` = `Other Provider Identifier Type Code_41`,
  `othpidty42` = `Other Provider Identifier Type Code_42`,
  `othpidty43` = `Other Provider Identifier Type Code_43`,
  `othpidty44` = `Other Provider Identifier Type Code_44`,
  `othpidty45` = `Other Provider Identifier Type Code_45`,
  `othpidty46` = `Other Provider Identifier Type Code_46`,
  `othpidty47` = `Other Provider Identifier Type Code_47`,
  `othpidty48` = `Other Provider Identifier Type Code_48`,
  `othpidty49` = `Other Provider Identifier Type Code_49`,
  `othpidty5` = `Other Provider Identifier Type Code_5`,
  `othpidty50` = `Other Provider Identifier Type Code_50`,
  `othpidty6` = `Other Provider Identifier Type Code_6`,
  `othpidty7` = `Other Provider Identifier Type Code_7`,
  `othpidty8` = `Other Provider Identifier Type Code_8`,
  `othpidty9` = `Other Provider Identifier Type Code_9`,
  `parent_org_lbn` = `Parent Organization LBN`,
  `parent_org_tin` = `Parent Organization TIN`,
  `pmailcityname` = `Provider Business Mailing Address City Name`,
  `pmailcountry` = `Provider Business Mailing Address Country Code (If outside U.S.)`,
  `pmailfax` = `Provider Business Mailing Address Fax Number`,
  `pmailzip` = `Provider Business Mailing Address Postal Code`,
  `pmailstatename` = `Provider Business Mailing Address State Name`,
  `pmailtel` = `Provider Business Mailing Address Telephone Number`,
  `ploccityname` = `Provider Business Practice Location Address City Name`,
  `ploccountry` = `Provider Business Practice Location Address Country Code (If outside U.S.)`,
  `plocfax` = `Provider Business Practice Location Address Fax Number`,
  `ploczip` = `Provider Business Practice Location Address Postal Code`,
  `plocstatename` = `Provider Business Practice Location Address State Name`,
  `ploctel` = `Provider Business Practice Location Address Telephone Number`,
  `pcredential` = `Provider Credential Text`,
  `penumdatestr` = `Provider Enumeration Date`,
  `pmailline1` = `Provider First Line Business Mailing Address`,
  `plocline1` = `Provider First Line Business Practice Location Address`,
  `pfname` = `Provider First Name`,
  `pgender` = `Provider Gender Code`,
  `plname` = `Provider Last Name (Legal Name)`,
  `plicnum1` = `Provider License Number_1`,
  `plicnum10` = `Provider License Number_10`,
  `plicnum11` = `Provider License Number_11`,
  `plicnum12` = `Provider License Number_12`,
  `plicnum13` = `Provider License Number_13`,
  `plicnum14` = `Provider License Number_14`,
  `plicnum15` = `Provider License Number_15`,
  `plicnum2` = `Provider License Number_2`,
  `plicnum3` = `Provider License Number_3`,
  `plicnum4` = `Provider License Number_4`,
  `plicnum5` = `Provider License Number_5`,
  `plicnum6` = `Provider License Number_6`,
  `plicnum7` = `Provider License Number_7`,
  `plicnum8` = `Provider License Number_8`,
  `plicnum9` = `Provider License Number_9`,
  `plicstate1` = `Provider License Number State Code_1`,
  `plicstate10` = `Provider License Number State Code_10`,
  `plicstate11` = `Provider License Number State Code_11`,
  `plicstate12` = `Provider License Number State Code_12`,
  `plicstate13` = `Provider License Number State Code_13`,
  `plicstate14` = `Provider License Number State Code_14`,
  `plicstate15` = `Provider License Number State Code_15`,
  `plicstate2` = `Provider License Number State Code_2`,
  `plicstate3` = `Provider License Number State Code_3`,
  `plicstate4` = `Provider License Number State Code_4`,
  `plicstate5` = `Provider License Number State Code_5`,
  `plicstate6` = `Provider License Number State Code_6`,
  `plicstate7` = `Provider License Number State Code_7`,
  `plicstate8` = `Provider License Number State Code_8`,
  `plicstate9` = `Provider License Number State Code_9`,
  `pmname` = `Provider Middle Name`,
  `pnameprefix` = `Provider Name Prefix Text`,
  `pnamesuffix` = `Provider Name Suffix Text`,
  `porgname` = `Provider Organization Name (Legal Business Name)`,
  `pcredentialoth` = `Provider Other Credential Text`,
  `pfnameoth` = `Provider Other First Name`,
  `plnameoth` = `Provider Other Last Name`,
  `plnamecode` = `Provider Other Last Name Type Code`,
  `pmnameoth` = `Provider Other Middle Name`,
  `pnameprefixoth` = `Provider Other Name Prefix Text`,
  `pnamesuffixoth` = `Provider Other Name Suffix Text`,
  `porgnameoth` = `Provider Other Organization Name`,
  `porgnameothcode` = `Provider Other Organization Name Type Code`,
  `pmailline2` = `Provider Second Line Business Mailing Address`,
  `plocline2` = `Provider Second Line Business Practice Location Address`,
  `replacement_npi` = `Replacement NPI`
)


################
# NPI Deactivation file: https://download.cms.gov/nppes/NPPESDeactivatedNPIReport040824.zip
npi_deactivation = file.path(data_dir, 'retirement_NPPES Deactivated NPI Report 20240408.csv')
if (!file.exists(npi_deactivation)) stop(glue::glue("The file '{npi_deactivation}' does not exist!"))

# Loading Data ----
threads = 4L
npi_deactivation = data.table::fread(file = npi_deactivation, header = T, stringsAsFactors = F, nThread = threads)

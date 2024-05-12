# NBER DATA IS ok but it does have each change in address noted.  Maybe we assume that everyone stayed in the same place until the end.  https://www.nber.org/research/data/npinppes-cumulative-collection-preliminary

# Setup and conflicts -----------------------------------------------------
source("R/01-setup.R")
conflict_prefer("filter", "duckplyr")
conflict_prefer("year", "lubridate")
conflicted::conflicts_prefer(exploratory::str_detect)
DBI::dbDisconnect(con)
conflicted::conflicts_prefer(dplyr::recode)
conflicted::conflicts_prefer(exploratory::recode)
conflicted::conflicts_prefer(lubridate::year)


# NBER section ------------------------------------------------------------
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

# Use duckplyr to create a reference to the table without loading it into R
nber_all <- dplyr::tbl(con, "nber_all"); glimpse(nber_all)

# 'nber_all' filtering ----------------------------------------------------
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

nber_all_basic %>% 
  dplyr::filter(plname == "MUFFLY") #There are three MUFFLY versions here.  CORRECT

nber_all_collected <- nber_all_basic %>%
  duckplyr::collect() %>%
  filter(pmailstatename %nin% c("AA", "ae", "AE", "AP", "APO", "GU", "VI", "FM", "MP", "AS") & plocstatename %nin% c("AA", "ae", "AE", "AP", "APO", "AS", "GU", "MP", "VI", "FM")) %>%
  mutate(pcredential = stringr::str_remove_all(pcredential, "[[\\p{P}][\\p{S}]]")) %>%
  # Remove MD punctuation.  
  mutate(pcredential = stringr::str_remove_all(pcredential, "[:blank:]")) %>%
  mutate(across(c(pmailzip, ploczip), .fns = ~stringr::str_sub(.,1 ,5))) %>%
  tidyr::unite(address, plocline1, ploccityname, plocstatename, ploczip, sep = ", ", remove = FALSE, na.rm = FALSE) %>%
  mutate(pcredential = str_to_upper(pcredential)) %>%
  mutate(pgender = recode(pgender, "F" = "Female", "M" = "Male")) %>%
  dplyr::arrange(npi) %>%
  group_by(npi) %>%
  fill(pcredential, .direction = "down") %>%
  ungroup()

nber_all_collected <- nber_all_collected[grepl("MD|DO", nber_all_collected$pcredential), , drop = FALSE]

# Adjusting the 'pcredential' within nber_all_collected1 itself
nber_all_collected$pcredential <- ifelse(grepl("MD", nber_all_collected$pcredential), "MD",
         ifelse(grepl("DO", nber_all_collected$pcredential), "DO", nber_all_collected$pcredential))

nber_all_collected <- nber_all_collected %>%
  readr::write_csv("~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/end_sp_duckdb_nber_all.csv"); gc()

# 'nber_all' Sanity Check ------------------------------------------------------------
nber_all_collected %>% 
  filter(plname == "MUFFLY") #!!!!!!!!!!!! TRIPLE CHECK

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

nber <- arsenal::tableby(~., data = nber_all_collected %>% dplyr::select(penumdatestr, lastupdatestr, pgender, pcredential, soleprop))

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


# # 'nppes' read in NPPES data one NPI file at a time
# # Specify the path for the DuckDB database file
# duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/my_duckdb.duckdb"  
# 
# # Connect to DuckDB with the specified database file
# con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
# 
# # List all tables in DuckDB
# dbListTables(con)

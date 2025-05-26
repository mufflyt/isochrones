# NPPES

# The process_nppes_data function efficiently imports, filters, and processes National Provider Identifier (NPI) data from a CSV file into a DuckDB database while focusing on individual US-based healthcare providers. It provides robust database management with comprehensive logging and validation, creating and managing optimized database tables specifically for healthcare provider analytics. Additionally, it offers the flexibility to export the processed data to an RDS file for further analysis in R, returning a connection to the database for immediate use in subsequent operations.

library(duckplyr)
library(dbplyr)
library(dplyr)
library(DBI)  # This is required for connecting to the database
library(RPostgres)  # If using PostgreSQL
library(tidyr)
library(stringr)
library(tidyverse)
'%nin%' <- function(x, table) {
  !(x %in% table)
}

source("R/bespoke_functions.R") # This loads up the process_nppes_data function

process_nppes_data(
  csv_file_path = "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/npidata_pfile_20050523-20201011.csv",
  db_path = "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb",
  filtered_table_name = "npidata_pfile_20050523_20201011",
  save_rds = FALSE,
  verbose = TRUE
)

# List all tables in the database
all_tables <- dbListTables(con)
print(all_tables)

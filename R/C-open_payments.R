#######################
source("R/01-setup.R")
#######################

#' Explore Open Payments Merged Table in DuckDB
#'
#' This script connects to the specified DuckDB database, inspects the
#' structure and content of the `open_payments_merged` table, and prints
#' summaries for exploratory analysis.
#'
#' Logging is printed to the console. No files are written.

# Load required packages
library(duckdb)
library(dplyr)
library(dbplyr)
gc()

# Connect to the database
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)

# List all tables in the database
all_tables <- DBI::dbListTables(conn)
print("Available tables in the database:")
print(all_tables)

# Create a reference to the open_payments_merged table
payments_tbl <- tbl(conn, "open_payments_merged")

# Check the table structure
glimpse(payments_tbl)

# Preview the first 10 rows
head(payments_tbl, 10)

payments_df <- collect(payments_tbl)

######
# Explicitly collect the data from the database with all columns as text
payments_query <- paste0(
  "SELECT ",
  paste(sprintf("CAST(\"%s\" AS VARCHAR) AS \"%s\"", 
                DBI::dbListFields(con, "open_payments_merged"),
                DBI::dbListFields(con, "open_payments_merged")),
        collapse = ", "),
  " FROM open_payments_merged"
)

payments_df <- DBI::dbGetQuery(con, payments_query)

# Verify the NPI data
npi_count <- sum(!is.na(payments_df$Covered_Recipient_NPI))
print(paste("Rows with NPI data:", npi_count))

readr::write_csv(payments_df, "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged_short.csv")

########
# Get a count of total rows
row_count <- payments_tbl %>% 
  count() %>% 
  collect()
print(paste("Total number of rows:", row_count$n))

# Check available specialty values
specialties <- payments_tbl %>%
  select(Covered_Recipient_Specialty_1) %>%
  distinct() %>%
  collect()

# Print the first few specialties
head(specialties, 20)

# Count rows by recipient type
recipient_types <- payments_tbl %>%
  group_by(Covered_Recipient_Type, Covered_Recipient_Primary_Type_1) %>%
  summarize(count = n()) %>%
  arrange(desc(count)) %>%
  collect()

print("Distribution by recipient type:")
print(recipient_types)

# Optional: Check for any OB/GYN related specialties
obgyn_specialties <- specialties %>%
  filter(grepl("Obstetrics|Gynecology", Covered_Recipient_Specialty_1, ignore.case = TRUE))

print("OB/GYN related specialties:")
print(obgyn_specialties)

########

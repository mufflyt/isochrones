#######################
source("R/01-setup.R")
#######################

# Load required packages
library(duckdb)
library(dplyr)
library(dbplyr) # This is needed for working with database connections

# Recreate connection to the database
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)

# Set table name
table_name <- "open_payments_merged"

# Create a remote table reference
payments_tbl <- tbl(conn, table_name)

# List tables to confirm connection
all_tables <- DBI::dbListTables(conn)
cat("Available tables:", paste(all_tables, collapse=", "), "\n")

# Set your data paths
# Define paths
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"
duckdb_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"

# Create a remote table reference
payments_tbl <- tbl(conn, table_name)

specialty_string <- c(
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Female Pelvic Medicine and Reconstructive Surgery",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Gynecologic Oncology",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Maternal & Fetal Medicine",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Reproductive Endocrinology",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Critical Care Medicine",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Gynecology",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Critical Care Medicine",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Hospice and Palliative Medicine",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Obesity Medicine",
  "Allopathic & Osteopathic Physicians | Obstetrics & Gynecology | Obstetrics"
  )

# Use mutate to create a numeric version of the column
filtered_doctors_data <- payments_tbl %>%
  dplyr::distinct(Record_ID, .keep_all = TRUE) %>%
  # First filter for doctors
  dplyr::filter(Covered_Recipient_Primary_Type_1 %in% c("Medical Doctor", "Doctor of Osteopathy")) %>%
  dplyr::filter(Covered_Recipient_Type == "Covered Recipient Physician") %>%
  # Filter for OB/GYN specialty in any specialty column
  dplyr::filter(Covered_Recipient_Specialty_1 %like% "%Obstetrics & Gynecology%") %>%
  # Create a numeric version of the payment column
  dplyr::mutate(Payment_Amount_Numeric = as.numeric(Total_Amount_of_Payment_USDollars)) %>%
  # Filter based on the numeric column
  dplyr::filter(Payment_Amount_Numeric > 1) %>%
  # Sort by the numeric column
  dplyr::arrange(desc(Payment_Amount_Numeric)) %>%
  # Materialize then pull the data into R
  dplyr::compute() %>%
  dplyr::collect()

View(filtered_doctors_data)


# Print the number of rows
cat("Number of doctor records found:", nrow(filtered_doctors_data), "\n")

# Optional: Save to CSV if needed
# write.csv(filtered_doctors_data, "filtered_payments_doctors_only.csv", row.names = FALSE)
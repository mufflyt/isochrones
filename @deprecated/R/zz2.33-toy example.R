
source("R/01-setup.R")
conflicted::conflicts_prefer(exploratory::left_join)
conflicted::conflicts_prefer(dplyr::case_when)
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

duckdb_file_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/nber/nber_my_duckdb.duckdb"
con <- dbConnect(duckdb::duckdb(), duckdb_file_path)
output_csv_path <- "/Volumes/Video Projects Muffly 1/open_payments/unzipped_files/open_payments_merged_data.csv" 
table_names <- c(
  "OP_DTL_GNRL_PGYR2014_P06302021",
  "OP_DTL_GNRL_PGYR2015_P06302021",
  "OP_DTL_GNRL_PGYR2016_P01182024",
  "OP_DTL_GNRL_PGYR2017_P01182024",
  "OP_DTL_GNRL_PGYR2018_P01182024",
  "OP_DTL_GNRL_PGYR2019_P01182024",
  "OP_DTL_GNRL_PGYR2020_P01182024",
  "OP_DTL_GNRL_PGYR2021_P01182024",
  "OP_DTL_GNRL_PGYR2022_P01182024")
dbListTables(con)

process_open_payments_tables <- function(con, table_names, output_csv_path) {
  # Initialize an empty data frame to store merged data
  all_data <- data.frame()
  
  # Check if the directory exists, create it if not
  if (!dir.exists(dirname(output_csv_path))) {
    dir.create(dirname(output_csv_path), recursive = TRUE)
  }
  
  # Loop through each table name
  for (table_name in table_names) {
    message("Processing table: ", table_name)
    
    tryCatch({
      # Use duckplyr to create a reference to the table without loading it into R
      table_ref <- tbl(con, table_name)
      
      # Log the table structure
      table_structure <- DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s)", table_name))
      message("Table structure of ", table_name, ": ", paste(names(table_structure), collapse = ", "))
      
      # Ensure that required columns exist
      required_columns <- c("Physician_Primary_Type", "Physician_Specialty")  # Add more as needed
      existing_columns <- names(table_structure)
      if (!all(required_columns %in% existing_columns)) {
        message("Missing required columns in ", table_name, ": ", paste(base::setdiff(required_columns, existing_columns), collapse = ", "))
        next  # Skip this table
      }
      
      # Proceed with data manipulation
      filtered_data <- table_ref %>%
        duckplyr::filter(Physician_Primary_Type %in% c("Doctor of Osteopathy", "Medical Doctor")) %>%
        duckplyr::filter(Physician_Specialty %in% c(
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Critical Care Medicine",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Female Pelvic Medicine and Reconstructive Surgery",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Gynecologic Oncology",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Gynecology",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Hospice and Palliative Medicine",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Maternal & Fetal Medicine",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Obstetrics",
          "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Reproductive Endocrinology"
        ))
      
      # Collect the filtered data
      filtered_data_df <- duckplyr::collect(filtered_data)
      message("Collected filtered data for table: ", table_name)
      
      # Append the processed data to the merged data frame
      all_data <- dplyr::bind_rows(all_data, filtered_data_df)
      
    }, error = function(e) {
      message("Error processing table: ", table_name)
      message("Error message: ", e$message)
    })
  }
  
  message("All tables processed.")
  
  # Write the merged data frame to disk
  tryCatch({
    readr::write_csv(all_data, output_csv_path)
    message("Data written to: ", output_csv_path)
  }, error = function(e) {
    message("Error writing to CSV: ", e$message)
  })
  
  # Return the list of processed tables
  return(invisible(all_data))
}

# Usage:
processed_tables <- process_open_payments_tables(con, table_names, output_csv_path)

######
# Troubleshooting column names that are different in the 2024 database.
# Retrieve the schema of the table
table_info <- dbGetQuery(con, "PRAGMA table_info(OP_DTL_GNRL_PGYR2021_P01182024)"); table_info

# Replace 'possible_new_column_name' with your guessed column names
sample_data <- dbGetQuery(con, "SELECT * FROM OP_DTL_GNRL_PGYR2014_P06302021 LIMIT 1000")
print(sample_data)
write_csv(sample_data, "/Volumes/Video Projects Muffly 1/openpayments/unzipped_files/short_OP_DTL_GNRL_PGYR2014_P06302021.csv")


######
tables <- DBI::dbListTables(con)
print(tables)  # This will show all tables in the database
#######

# ## The error you're encountering is due to attempting to overwrite the table directly while it is being used, which is not allowed in many database systems including DuckDB, which Duckplyr is based upon.
# 
# # Directly query to see if the table exists and accessible
# result <- DBI::dbGetQuery(con, "SELECT * FROM OP_DTL_GNRL_PGYR2022_P01182024_standardized LIMIT 1")
# print(result)
# 
# # Create a reference to the table
# table_ref <- tbl(con, "OP_DTL_GNRL_PGYR2022_P01182024_standardized")
# 
# # Rename the column and create a temporary table
# temp_table_name <- "temp_OP_DTL_GNRL_PGYR2022_P01182024"
# table_ref %>%
#   duckplyr::rename(Physician_Specialty = Covered_Recipient_Specialty_1) %>%
#   compute(name = temp_table_name, temporary = TRUE)
# 
# # Drop the original table
# DBI::dbExecute(con, "DROP TABLE OP_DTL_GNRL_PGYR2020_P01182024")
# 
# # Rename the temporary table to the original name
# DBI::dbExecute(con, sprintf("ALTER TABLE %s RENAME TO OP_DTL_GNRL_PGYR2022_P01182024", temp_table_name))


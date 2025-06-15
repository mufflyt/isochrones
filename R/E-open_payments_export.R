#######################
source("R/01-setup.R")
#######################

# Connect to the existing database
library(duckdb)
library(dplyr)
library(dbplyr)

# Connect to the database
db_path <- "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged.duckdb"
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)

# Create a reference to the open_payments_merged table
payments_tbl <- tbl(conn, "open_payments_merged")

# Look for OB/GYN specialties with a more flexible approach
obgyn_data <- payments_tbl %>%
  filter(
    Covered_Recipient_Type == "Covered Recipient Physician",
    Covered_Recipient_Primary_Type_1 %in% c("Medical Doctor", "Doctor of Osteopathy"),
    (Covered_Recipient_Specialty_1 %like% "%Obstetrics%" | 
       Covered_Recipient_Specialty_1 %like% "%Gynecology%")
  ) %>%
  mutate(Payment_Amount_Numeric = as.numeric(Total_Amount_of_Payment_USDollars)) %>%
  arrange(desc(Payment_Amount_Numeric)) %>%
  collect()

print(paste("Found", nrow(obgyn_data), "OB/GYN-related payments"))

library(readr)
readr::write_csv(obgyn_data, "/Volumes/Video Projects Muffly 1/open_payments_data/open_payments_merged_filtered_to_obgyns.csv")


#' Export merged data to CSV
#'
#' @param connection DuckDB connection
#' @param table_name Table to export
#' @param output_path Path to save CSV file
#' @param verbose Whether to print verbose output
#'
#' @noRd
export_to_csv <- function(connection, table_name, output_path, verbose) {
  logger::log_info("Exporting merged data to CSV: {output_path}")
  
  # Ensure directory exists
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    logger::log_info("Created output directory: {output_dir}")
  }
  
  # Export to CSV using optimized approach
  export_query <- paste0(
    "COPY (SELECT * FROM ", table_name, ") TO '", output_path, 
    "' (DELIMITER ',', HEADER, FORMAT CSV, COMPRESSION 'gzip')"
  )
  
  tryCatch({
    DBI::dbExecute(connection, export_query)
    
    # Get file size
    file_size_mb <- round(as.numeric(fs::file_size(output_path)) / (1024 * 1024), 2)
    logger::log_success("Successfully exported data to {output_path} ({file_size_mb} MB)")
  }, error = function(e) {
    logger::log_error("Failed to export to CSV: {e$message}")
    
    # Try a chunked export if the single export fails
    logger::log_info("Trying chunked CSV export...")
    chunked_export_success <- export_to_csv_in_chunks(connection, table_name, output_path, verbose)
    
    if (chunked_export_success) {
      logger::log_success("Successfully exported data in chunks to {output_path}")
    }
  })
}

#' Export data to CSV in chunks
#'
#' @param connection DuckDB connection
#' @param table_name Table to export
#' @param output_path Path to save CSV file
#' @param verbose Whether to print verbose output
#'
#' @return Logical indicating if export was successful
#' @noRd
export_to_csv_in_chunks <- function(connection, table_name, output_path, verbose) {
  # Get row count of table
  count_query <- paste0("SELECT COUNT(*) AS row_count FROM ", table_name)
  count_result <- DBI::dbGetQuery(connection, count_query)
  total_rows <- count_result$row_count[1]
  
  # Define chunk size
  chunk_size <- 500000  # Adjust based on memory constraints
  
  # Calculate number of chunks
  num_chunks <- ceiling(total_rows / chunk_size)
  logger::log_info("Exporting {total_rows} rows in {num_chunks} chunks")
  
  # Create output directory if it doesn't exist
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    logger::log_info("Created output directory: {output_dir}")
  }
  
  # Export each chunk to a separate file
  chunk_files <- character(num_chunks)
  
  for (chunk in 1:num_chunks) {
    offset <- (chunk - 1) * chunk_size
    
    # Create chunk file path
    chunk_file <- paste0(tools::file_path_sans_ext(output_path), 
                         "_chunk", chunk, "of", num_chunks, 
                         ".", tools::file_ext(output_path))
    
    # Export chunk
    chunk_query <- paste0(
      "COPY (SELECT * FROM ", table_name, " LIMIT ", chunk_size, " OFFSET ", offset, ") ",
      "TO '", chunk_file, "' (DELIMITER ',', HEADER, FORMAT CSV)"
    )
    
    tryCatch({
      DBI::dbExecute(connection, chunk_query)
      chunk_files[chunk] <- chunk_file
      logger::log_debug("Exported chunk {chunk}/{num_chunks} to {basename(chunk_file)}")
    }, error = function(e) {
      logger::log_error("Failed to export chunk {chunk}/{num_chunks}: {e$message}")
    })
  }
  
  # Combine chunks if all exports were successful
  if (all(file.exists(chunk_files))) {
    logger::log_info("Combining {length(chunk_files)} chunks into a single file")
    
    tryCatch({
      # Combine files, keeping header only from first file
      first_chunk <- readLines(chunk_files[1])
      header <- first_chunk[1]
      combined_data <- c(header)
      
      for (i in 1:length(chunk_files)) {
        chunk_data <- readLines(chunk_files[i])
        if (i > 1) {
          # Skip header for all but first chunk
          chunk_data <- chunk_data[-1]
        }
        combined_data <- c(combined_data, chunk_data)
        
        # Remove chunk file after reading
        file.remove(chunk_files[i])
      }
      
      # Write combined file
      writeLines(combined_data, output_path)
      
      file_size_mb <- round(as.numeric(fs::file_size(output_path)) / (1024 * 1024), 2)
      logger::log_success("Successfully combined chunks into {output_path} ({file_size_mb} MB)")
      
      return(TRUE)
    }, error = function(e) {
      logger::log_error("Failed to combine chunks: {e$message}")
      return(FALSE)
    })
  } else {
    logger::log_error("Not all chunks were exported successfully")
    return(FALSE)
  }
}

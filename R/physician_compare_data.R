#######################
source("R/01-setup.R")
#######################

#' Standardize CMS Physician Data Across Years
#'
#' @description
#' Creates a standardized table from various years of CMS physician data
#' by identifying and mapping common fields across different formats.
#'
#' @param db_conn DuckDB connection object
#' @param output_table Character. Name for the standardized output table.
#'        Default: "physician_data_standardized"
#' @param sample_size Integer. Number of rows to sample from each table for analysis.
#'        Default: 1000
#' @param max_memory_mb Integer. Maximum memory to use in MB. Default: 2000
#' @param verbose Logical. Show detailed logs. Default: TRUE
#'
#' @return Logical indicating success
#'
#' @importFrom DBI dbExecute dbGetQuery dbListTables dbListFields
#' @importFrom logger log_info log_error log_warn
#' @importFrom dplyr %>% filter mutate
#' @export
standardize_cms_data <- function(db_conn,
                                 output_table = "physician_data_standardized",
                                 sample_size = 1000,
                                 max_memory_mb = 2000,
                                 verbose = TRUE) {
  
  if (verbose) logger::log_threshold(logger::INFO) else logger::log_threshold(logger::WARN)
  
  # Start timing and memory tracking
  start_time <- Sys.time()
  start_mem <- gc(reset = TRUE, full = TRUE)
  start_mem_used <- sum(start_mem[, 2]) * 1024^2 / 1000  # in MB
  
  logger::log_info("Starting CMS data standardization at {format(start_time)}")
  logger::log_info("Initial memory usage: {round(start_mem_used, 1)} MB")
  
  # Get database info
  db_info <- dbGetInfo(db_conn)
  db_path <- db_info$dbname
  logger::log_info("Database path: {db_path}")
  
  # Find all physician tables
  all_tables <- DBI::dbListTables(db_conn)
  cms_tables <- all_tables[grepl("^y[0-9]{4}_", all_tables)]
  
  if (length(cms_tables) == 0) {
    logger::log_error("No CMS physician tables found with year prefix")
    return(FALSE)
  }
  
  logger::log_info("Found {length(cms_tables)} CMS tables to standardize")
  for (table in cms_tables) {
    logger::log_info("- {table}")
  }
  
  # Common field patterns to look for across years
  field_patterns <- list(
    npi = c("npi", "national.provider.identifier", "national provider identifier", "provider_id"),
    first_name = c("first.name", "first name", "firstname", "fname"),
    middle_name = c("middle.name", "middle name", "middlename", "mname", "mi"),
    last_name = c("last.name", "last name", "lastname", "lname"),
    suffix = c("suffix", "name suffix"),
    gender = c("gender", "sex"),
    credential = c("credential", "credentials", "credential text"),
    medical_school = c("medical.school", "medical school", "med school", "medicalschool"),
    graduation_year = c("graduation.year", "year of graduation", "graduationyear"),
    primary_specialty = c("primary.specialty", "primary specialty", "primaryspecialty", "specialty"),
    organization_name = c("organization.name", "organization name", "org name", "organization legal name"),
    address_line_1 = c("address.line.1", "address line 1", "address1", "line 1 street address"),
    address_line_2 = c("address.line.2", "address line 2", "address2", "line 2 street address"),
    city = c("city", "practice city"),
    state = c("state", "st", "practice state"),
    zip_code = c("zip.code", "zip code", "zip", "zipcode", "postal code")
  )
  
  # Create temporary table for analysis
  temp_analysis_table <- "temp_header_analysis"
  DBI::dbExecute(db_conn, sprintf("DROP TABLE IF EXISTS %s", temp_analysis_table))
  DBI::dbExecute(db_conn, sprintf(
    "CREATE TABLE %s (
      table_name VARCHAR,
      year VARCHAR,
      column_name VARCHAR,
      column_index INTEGER,
      sample_values VARCHAR
    )", temp_analysis_table
  ))
  
  # Process each table to analyze structure
  logger::log_info("Analyzing table structures...")
  
  for (table_name in cms_tables) {
    # Extract year from table name
    year_match <- regmatches(table_name, regexpr("^y([0-9]{4})_", table_name))
    table_year <- if (length(year_match) > 0) substring(year_match, 2, 5) else "unknown"
    
    logger::log_info("Analyzing {table_name} (Year: {table_year})")
    
    # Get column names
    columns <- DBI::dbListFields(db_conn, table_name)
    logger::log_info("Table has {length(columns)} columns")
    
    # Check if this table appears to have a single raw data column
    if (length(columns) <= 2) {
      # This is likely a raw pipe-delimited data column
      logger::log_info("Table {table_name} has raw data format")
      
      # Get header row
      header_query <- sprintf("SELECT * FROM %s LIMIT 1", table_name)
      header_row <- DBI::dbGetQuery(db_conn, header_query)
      
      if (nrow(header_row) > 0) {
        raw_col <- names(header_row)[1]
        header_text <- as.character(header_row[1, 1])
        
        if (grepl("\\|", header_text)) {
          logger::log_info("Found pipe-delimited header: {substr(header_text, 1, 100)}...")
          
          # Split header by pipe
          header_fields <- unlist(strsplit(header_text, "\\|"))
          logger::log_info("Header has {length(header_fields)} fields")
          
          # Sample some data to analyze values
          sample_query <- sprintf("SELECT * FROM %s LIMIT 1, %d", table_name, sample_size)
          sample_data <- DBI::dbGetQuery(db_conn, sample_query)
          
          # Analyze each position in pipe-delimited data
          for (i in 1:min(length(header_fields), 50)) {  # Limit to first 50 fields
            field_name <- header_fields[i]
            
            # Extract sample values for this position
            sample_values <- tryCatch({
              # Use SQL to extract values at this position
              value_query <- sprintf(
                "SELECT DISTINCT SPLIT_PART(%s, '|', %d) AS value 
                 FROM %s 
                 WHERE ROWID > 1 
                 LIMIT 10",
                raw_col, i, table_name
              )
              
              values <- DBI::dbGetQuery(db_conn, value_query)$value
              values <- values[!is.na(values) & values != ""]
              paste(head(values, 5), collapse = ", ")
            }, error = function(e) {
              "Error extracting values"
            })
            
            # Add to analysis table
            DBI::dbExecute(db_conn, sprintf(
              "INSERT INTO %s VALUES ('%s', '%s', '%s', %d, '%s')",
              temp_analysis_table,
              table_name,
              table_year,
              gsub("'", "''", field_name),  # Escape single quotes
              i,
              gsub("'", "''", sample_values)
            ))
          }
        }
      }
    } else {
      # Table already has structured columns
      logger::log_info("Table {table_name} already has structured format")
      
      # Sample some data to analyze values
      sample_query <- sprintf("SELECT * FROM %s LIMIT %d", table_name, sample_size)
      sample_data <- tryCatch({
        DBI::dbGetQuery(db_conn, sample_query)
      }, error = function(e) {
        logger::log_error("Error sampling data: {conditionMessage(e)}")
        data.frame()
      })
      
      if (nrow(sample_data) > 0) {
        for (i in seq_along(columns)) {
          col_name <- columns[i]
          
          # Get sample values
          sample_values <- tryCatch({
            values <- as.character(sample_data[[col_name]])
            values <- values[!is.na(values) & values != ""]
            paste(head(values, 5), collapse = ", ")
          }, error = function(e) {
            "Error extracting values"
          })
          
          # Add to analysis table
          DBI::dbExecute(db_conn, sprintf(
            "INSERT INTO %s VALUES ('%s', '%s', '%s', %d, '%s')",
            temp_analysis_table,
            table_name,
            table_year,
            gsub("'", "''", col_name),  # Escape single quotes
            i,
            gsub("'", "''", sample_values)
          ))
        }
      }
    }
    
    # Free memory
    gc(reset = TRUE, full = TRUE)
  }
  
  # Create standardized table structure
  logger::log_info("Creating standardized table: {output_table}")
  
  # Drop if exists
  DBI::dbExecute(db_conn, sprintf("DROP TABLE IF EXISTS %s", output_table))
  
  # Create table with standardized schema
  create_sql <- sprintf(
    "CREATE TABLE %s (
      source_table VARCHAR,
      year VARCHAR,
      npi VARCHAR,
      first_name VARCHAR,
      middle_name VARCHAR,
      last_name VARCHAR,
      suffix VARCHAR,
      gender VARCHAR,
      credential VARCHAR,
      medical_school VARCHAR,
      graduation_year VARCHAR,
      primary_specialty VARCHAR,
      organization_name VARCHAR,
      address_line_1 VARCHAR,
      address_line_2 VARCHAR,
      city VARCHAR,
      state VARCHAR,
      zip_code VARCHAR
    )",
    output_table
  )
  
  DBI::dbExecute(db_conn, create_sql)
  logger::log_info("Created standardized table schema")
  
  # Function to find matching fields in a table
  find_field_mappings <- function(db_conn, table_name, field_patterns, temp_analysis_table) {
    mappings <- list()
    
    # Get the column structure data
    mapping_query <- sprintf(
      "SELECT column_name, column_index FROM %s WHERE table_name = '%s'",
      temp_analysis_table, table_name
    )
    
    column_data <- DBI::dbGetQuery(db_conn, mapping_query)
    
    if (nrow(column_data) == 0) {
      logger::log_warn("No column data found for {table_name}")
      return(mappings)
    }
    
    # Check if this is a structured or raw table
    raw_format <- nrow(column_data) > 5 && max(column_data$column_index) > 5
    
    # For each field we want to map
    for (field_name in names(field_patterns)) {
      # Get patterns to look for
      patterns <- field_patterns[[field_name]]
      
      # Look for exact or partial matches
      for (pattern in patterns) {
        # Try exact match first
        exact_matches <- which(tolower(column_data$column_name) == tolower(pattern))
        
        if (length(exact_matches) > 0) {
          # Use the first exact match
          mappings[[field_name]] <- list(
            index = column_data$column_index[exact_matches[1]],
            name = column_data$column_name[exact_matches[1]],
            raw_format = raw_format
          )
          break
        }
        
        # Try partial match
        partial_matches <- which(grepl(tolower(pattern), tolower(column_data$column_name), fixed = TRUE))
        
        if (length(partial_matches) > 0) {
          # Use the first partial match
          mappings[[field_name]] <- list(
            index = column_data$column_index[partial_matches[1]],
            name = column_data$column_name[partial_matches[1]],
            raw_format = raw_format
          )
          break
        }
      }
    }
    
    return(mappings)
  }
  
  # Process each table to extract standardized data
  for (table_name in cms_tables) {
    # Extract year from table name
    year_match <- regmatches(table_name, regexpr("^y([0-9]{4})_", table_name))
    table_year <- if (length(year_match) > 0) substring(year_match, 2, 5) else "unknown"
    
    logger::log_info("Processing {table_name} (Year: {table_year})")
    
    # Find field mappings for this table
    field_mappings <- find_field_mappings(db_conn, table_name, field_patterns, temp_analysis_table)
    
    # Log field mappings
    logger::log_info("Found {length(field_mappings)} field mappings for {table_name}")
    for (field_name in names(field_mappings)) {
      mapping <- field_mappings[[field_name]]
      logger::log_info("  {field_name} -> {mapping$name} (index: {mapping$index})")
    }
    
    # Check if this is a raw or structured table
    is_raw_format <- length(field_mappings) > 0 && field_mappings[[1]]$raw_format
    
    if (is_raw_format) {
      logger::log_info("Table {table_name} has raw pipe-delimited format")
      
      # Get the column name for raw data
      raw_col_query <- sprintf("SELECT * FROM %s LIMIT 1", table_name)
      raw_sample <- DBI::dbGetQuery(db_conn, raw_col_query)
      raw_col <- names(raw_sample)[1]
      
      # Build field extraction expressions
      field_exprs <- list()
      for (field_name in names(field_patterns)) {
        if (field_name %in% names(field_mappings)) {
          position <- field_mappings[[field_name]]$index
          field_exprs[[field_name]] <- sprintf("SPLIT_PART(%s, '|', %d)", raw_col, position)
        } else {
          field_exprs[[field_name]] <- "NULL"
        }
      }
      
      # Count rows in source table (skip header row)
      count_query <- sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE ROWID > 1", table_name)
      source_count <- DBI::dbGetQuery(db_conn, count_query)$cnt
      logger::log_info("Table {table_name} has {source_count} data rows")
      
      # Process in chunks to manage memory
      chunk_size <- min(100000, ceiling(max_memory_mb * 1000 / 50))
      
      # Calculate number of chunks
      num_chunks <- ceiling(source_count / chunk_size)
      logger::log_info("Processing {source_count} rows in {num_chunks} chunks of {chunk_size} rows")
      
      # Process in chunks
      for (chunk in 1:num_chunks) {
        chunk_start <- (chunk - 1) * chunk_size + 2  # +2 to skip header
        chunk_end <- min(chunk * chunk_size + 1, source_count + 1)
        
        logger::log_info("Processing chunk {chunk}/{num_chunks}: rows {chunk_start-1}-{chunk_end-1}")
        
        # Build insert SQL for raw format
        insert_sql <- sprintf(
          "INSERT INTO %s (
            source_table, year, npi, first_name, middle_name, last_name,
            suffix, gender, credential, medical_school, graduation_year, 
            primary_specialty, organization_name, address_line_1, 
            address_line_2, city, state, zip_code
          )
          SELECT 
            '%s' AS source_table,
            '%s' AS year,
            %s AS npi,
            %s AS first_name,
            %s AS middle_name,
            %s AS last_name,
            %s AS suffix,
            %s AS gender,
            %s AS credential,
            %s AS medical_school,
            %s AS graduation_year,
            %s AS primary_specialty,
            %s AS organization_name,
            %s AS address_line_1,
            %s AS address_line_2,
            %s AS city,
            %s AS state,
            %s AS zip_code
          FROM %s
          WHERE ROWID >= %d AND ROWID < %d",
          output_table,
          table_name,
          table_year,
          field_exprs$npi,
          field_exprs$first_name,
          field_exprs$middle_name,
          field_exprs$last_name,
          field_exprs$suffix,
          field_exprs$gender,
          field_exprs$credential,
          field_exprs$medical_school,
          field_exprs$graduation_year,
          field_exprs$primary_specialty,
          field_exprs$organization_name,
          field_exprs$address_line_1,
          field_exprs$address_line_2,
          field_exprs$city,
          field_exprs$state,
          field_exprs$zip_code,
          table_name,
          chunk_start,
          chunk_end
        )
        
        # Execute the insert
        tryCatch({
          DBI::dbExecute(db_conn, insert_sql)
          
          # Update progress
          rows_in_chunk <- chunk_end - chunk_start
          
          logger::log_info("Added {rows_in_chunk} rows from chunk {chunk}")
        }, error = function(e) {
          logger::log_error("Error processing chunk {chunk}: {conditionMessage(e)}")
          logger::log_error("SQL error: {substr(e$message, 1, 500)}")
        })
        
        # Free memory
        current_mem <- gc(reset = TRUE, full = TRUE)
        current_mem_used <- sum(current_mem[, 2]) * 1024^2 / 1000
        
        logger::log_info("Memory usage after chunk {chunk}: {round(current_mem_used, 1)} MB")
        
        # Emergency memory protection
        if (current_mem_used > max_memory_mb * 0.9) {
          logger::log_warn("Approaching memory limit. Pausing processing.")
          break
        }
      }
    } else {
      # Table has structured columns
      logger::log_info("Table {table_name} has structured column format")
      
      # Build field mapping expressions
      field_exprs <- list()
      for (field_name in names(field_patterns)) {
        if (field_name %in% names(field_mappings)) {
          col_name <- field_mappings[[field_name]]$name
          field_exprs[[field_name]] <- sprintf('"%s"', col_name)
        } else {
          field_exprs[[field_name]] <- "NULL"
        }
      }
      
      # Count rows in source table
      count_query <- sprintf("SELECT COUNT(*) AS cnt FROM %s", table_name)
      source_count <- DBI::dbGetQuery(db_conn, count_query)$cnt
      logger::log_info("Table {table_name} has {source_count} data rows")
      
      # Process in chunks
      chunk_size <- min(100000, ceiling(max_memory_mb * 1000 / 50))
      
      # Calculate number of chunks
      num_chunks <- ceiling(source_count / chunk_size)
      logger::log_info("Processing {source_count} rows in {num_chunks} chunks of {chunk_size} rows")
      
      # Process in chunks
      for (chunk in 1:num_chunks) {
        chunk_start <- (chunk - 1) * chunk_size
        
        logger::log_info("Processing chunk {chunk}/{num_chunks}")
        
        # Build insert SQL for structured format
        insert_sql <- sprintf(
          "INSERT INTO %s (
            source_table, year, npi, first_name, middle_name, last_name,
            suffix, gender, credential, medical_school, graduation_year, 
            primary_specialty, organization_name, address_line_1, 
            address_line_2, city, state, zip_code
          )
          SELECT 
            '%s' AS source_table,
            '%s' AS year,
            %s AS npi,
            %s AS first_name,
            %s AS middle_name,
            %s AS last_name,
            %s AS suffix,
            %s AS gender,
            %s AS credential,
            %s AS medical_school,
            %s AS graduation_year,
            %s AS primary_specialty,
            %s AS organization_name,
            %s AS address_line_1,
            %s AS address_line_2,
            %s AS city,
            %s AS state,
            %s AS zip_code
          FROM %s
          LIMIT %d OFFSET %d",
          output_table,
          table_name,
          table_year,
          field_exprs$npi,
          field_exprs$first_name,
          field_exprs$middle_name,
          field_exprs$last_name,
          field_exprs$suffix,
          field_exprs$gender,
          field_exprs$credential,
          field_exprs$medical_school,
          field_exprs$graduation_year,
          field_exprs$primary_specialty,
          field_exprs$organization_name,
          field_exprs$address_line_1,
          field_exprs$address_line_2,
          field_exprs$city,
          field_exprs$state,
          field_exprs$zip_code,
          table_name,
          chunk_size,
          chunk_start
        )
        
        # Execute the insert
        tryCatch({
          DBI::dbExecute(db_conn, insert_sql)
          logger::log_info("Added chunk {chunk} from {table_name}")
        }, error = function(e) {
          logger::log_error("Error processing chunk {chunk}: {conditionMessage(e)}")
        })
        
        # Free memory
        gc(reset = TRUE, full = TRUE)
      }
    }
    
    # Create indexes after each table
    logger::log_info("Creating indexes after processing {table_name}")
    
    tryCatch({
      # Create indexes on key fields for performance
      DBI::dbExecute(db_conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_npi ON %s(npi)",
                                      gsub("[^a-zA-Z0-9]", "", output_table), output_table))
      DBI::dbExecute(db_conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_year ON %s(year)",
                                      gsub("[^a-zA-Z0-9]", "", output_table), output_table))
      DBI::dbExecute(db_conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_state ON %s(state)",
                                      gsub("[^a-zA-Z0-9]", "", output_table), output_table))
      
      logger::log_info("Created indexes on npi, year, and state")
    }, error = function(e) {
      logger::log_warn("Error creating indexes: {conditionMessage(e)}")
    })
  }
  
  # Clean up temporary table
  DBI::dbExecute(db_conn, sprintf("DROP TABLE IF EXISTS %s", temp_analysis_table))
  
  # Get final row count
  count_query <- sprintf("SELECT COUNT(*) AS cnt FROM %s", output_table)
  final_count <- DBI::dbGetQuery(db_conn, count_query)$cnt
  
  # Get memory usage
  final_mem <- gc(reset = TRUE, full = TRUE)
  final_mem_used <- sum(final_mem[, 2]) * 1024^2 / 1000
  
  # Calculate processing time
  end_time <- Sys.time()
  total_time <- difftime(end_time, start_time, units = "mins")
  
  # Final report
  logger::log_info("-------- Processing Summary --------")
  logger::log_info("Database path: {db_path}")
  logger::log_info("Tables processed: {length(cms_tables)}")
  logger::log_info("Final standardized table row count: {final_count}")
  logger::log_info("Initial memory usage: {round(start_mem_used, 1)} MB")
  logger::log_info("Final memory usage: {round(final_mem_used, 1)} MB")
  logger::log_info("Total processing time: {round(total_time, 2)} minutes")
  
  # Suggest some sample queries
  logger::log_info("")
  logger::log_info("Standardization complete! Try these example queries:")
  logger::log_info("")
  logger::log_info("1. Count physicians by state:")
  logger::log_info("   SELECT state, COUNT(*) FROM {output_table} GROUP BY state ORDER BY COUNT(*) DESC")
  logger::log_info("")
  logger::log_info("2. Top specialties by year:")
  logger::log_info("   SELECT year, primary_specialty, COUNT(*) FROM {output_table} GROUP BY year, primary_specialty ORDER BY year, COUNT(*) DESC")
  logger::log_info("")
  logger::log_info("3. Medical school distribution:")
  logger::log_info("   SELECT medical_school, COUNT(*) FROM {output_table} GROUP BY medical_school ORDER BY COUNT(*) DESC LIMIT 20")
  
  return(TRUE)
}

###
# Connect to your database
library(dplyr)
library(DBI)
library(duckdb)
library(logger)

# Set up logging if not already done
log_threshold(logger::INFO)

# Connect to the database
db_path <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/nppes_my_duckdb.duckdb"
con <- dbConnect(duckdb(), dbdir = db_path)

# Run the standardization function
standardize_cms_data(
  db_conn = con, 
  output_table = "physician_data_standardized",
  max_memory_mb = 2000
)

# Example: Track physician counts by year
yearly_counts <- tbl(con, "physician_data_standardized") %>%
  group_by(year) %>%
  summarize(count = n()) %>%
  arrange(year) %>%
  dplyr::compute() %>%
  collect()

print(yearly_counts)

# When done
dbDisconnect(con)

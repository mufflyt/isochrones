#######################
source("R/01-setup.R")
#######################

# Simple script to download Open Payments data files
# Simple script to download Open Payments data files
# Load required packages
library(logger)
library(tidyverse)
library(fs)

# Configure logger
logger::log_layout(logger::layout_glue_generator(
  format = "{time} [{level}] {msg}"
))

# Define base directory for downloads
base_dir <- "/Volumes/Video Projects Muffly 1/open_payments_data"

# Create base directory if it doesn't exist
if (!dir.exists(base_dir)) {
  fs::dir_create(base_dir)
  logger::log_info("Created base directory: %s", base_dir)
}

# Define Open Payments URLs
op_hyperlinks <- c(
  "https://download.cms.gov/openpayments/PGYR2023_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2022_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2021_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2020_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2019_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2018_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR2017_P01302025_01212025.zip",
  "https://download.cms.gov/openpayments/PGYR16_P011824.ZIP",
  "https://download.cms.gov/openpayments/PGYR15_P012023.ZIP",
  "https://download.cms.gov/openpayments/PGYR14_P012122.ZIP",
  "https://download.cms.gov/openpayments/PGYR13_P012221.ZIP",
  "https://download.cms.gov/openpayments/PHPRFL_P01302025_01212025.zip"
)

# Create a tibble with URLs and extract year information
op_files <- tibble(url = op_hyperlinks) %>%
  mutate(
    filename = basename(url),
    # Extract year information from filename
    year = case_when(
      # For formats like PGYR2023_P01302025_01212025.zip
      str_detect(filename, "PGYR20[0-9]{2}") ~ 
        str_extract(filename, "(?<=PGYR)20[0-9]{2}"),
      # For formats like PGYR16_P011824.ZIP
      str_detect(filename, "PGYR[0-9]{2}") ~ 
        paste0("20", str_extract(filename, "(?<=PGYR)[0-9]{2}")),
      # Profile data
      str_detect(filename, "PHPRFL") ~ "profiles",
      TRUE ~ NA_character_
    )
  )

logger::log_info("Preparing to download %d files", nrow(op_files))

# Process each file
for (i in 1:nrow(op_files)) {
  current_file <- op_files[i, ]
  url <- current_file$url
  year_dir <- current_file$year
  filename <- current_file$filename
  
  logger::log_info("Processing file %d of %d: %s (Year: %s)", 
                   i, nrow(op_files), filename, year_dir)
  
  # Create year-specific directory
  target_dir <- file.path(base_dir, year_dir)
  if (!dir.exists(target_dir)) {
    fs::dir_create(target_dir, recurse = TRUE)
    logger::log_info("Created directory: %s", target_dir)
  }
  
  # Define paths
  dest_file <- file.path(target_dir, filename)
  extraction_dir <- file.path(target_dir, "unzipped_files")
  
  # Skip download if file already exists
  if (file.exists(dest_file) && file.size(dest_file) > 0) {
    logger::log_info("File already exists: %s - Skipping download", filename)
  } else {
    # Download file with retry logic
    logger::log_info("Downloading: %s", filename)
    
    # Remove incomplete/empty file if it exists
    if (file.exists(dest_file) && file.size(dest_file) == 0) {
      unlink(dest_file)
      logger::log_info("Removed empty file: %s", filename)
    }
    
    # Set options for download - longer timeout
    download_options <- list(
      timeout = 300,  # 5 minutes timeout
      retries = 3,    # Number of retry attempts
      quiet = FALSE   # Show progress
    )
    
    # Try download with curl if available (better for large files)
    download_success <- FALSE
    
    # Try curl method first (handles large files better)
    if (Sys.which("curl") != "") {
      logger::log_info("Attempting download with curl...")
      
      for (attempt in 1:download_options$retries) {
        if (download_success) break
        
        tryCatch({
          # Create curl command with continue option
          curl_cmd <- paste0(
            "curl -L --connect-timeout 30 --max-time ", download_options$timeout,
            " -C - ", shQuote(url), " -o ", shQuote(dest_file)
          )
          
          logger::log_info("Download attempt %d of %d using curl...", 
                           attempt, download_options$retries)
          
          system_result <- system(curl_cmd)
          
          if (system_result == 0 && file.exists(dest_file) && file.size(dest_file) > 0) {
            download_success <- TRUE
            logger::log_info("Successfully downloaded using curl: %s (%.2f MB)", 
                             filename, file.size(dest_file) / 1024^2)
          } else {
            logger::log_warn("Curl download attempt %d failed for %s", attempt, filename)
            Sys.sleep(5)  # Wait before retry
          }
        }, error = function(e) {
          logger::log_warn("Error in curl download attempt %d for %s: %s", 
                           attempt, filename, e$message)
          Sys.sleep(5)  # Wait before retry
        })
      }
    }
    
    # Fall back to download.file if curl failed or not available
    if (!download_success) {
      logger::log_info("Attempting download with download.file...")
      
      for (attempt in 1:download_options$retries) {
        if (download_success) break
        
        tryCatch({
          utils::download.file(
            url, 
            dest_file, 
            mode = "wb",
            quiet = download_options$quiet,
            method = "auto",
            cacheOK = FALSE,
            extra = paste0("--connect-timeout 30 --max-time ", download_options$timeout)
          )
          
          if (file.exists(dest_file) && file.size(dest_file) > 0) {
            download_success <- TRUE
            logger::log_info("Successfully downloaded using download.file: %s (%.2f MB)", 
                             filename, file.size(dest_file) / 1024^2)
          } else {
            logger::log_warn("Download.file attempt %d failed for %s", attempt, filename)
            Sys.sleep(5)  # Wait before retry
          }
        }, error = function(e) {
          logger::log_warn("Error in download.file attempt %d for %s: %s", 
                           attempt, filename, e$message)
          Sys.sleep(5)  # Wait before retry
        })
      }
    }
    
    # Check final download status
    if (!download_success) {
      logger::log_error("All download attempts failed for: %s", filename)
      if (file.exists(dest_file) && file.size(dest_file) > 0) {
        logger::log_warn("Partial file exists and may be usable: %s (%.2f MB)", 
                         filename, file.size(dest_file) / 1024^2)
      } else {
        logger::log_error("No usable file downloaded for %s", filename)
        next  # Skip to next file
      }
    }
  }
  
  # Create extraction directory if it doesn't exist
  if (!dir.exists(extraction_dir)) {
    fs::dir_create(extraction_dir, recurse = TRUE)
    logger::log_info("Created extraction directory: %s", extraction_dir)
  }
  
  # Extract files only if download was successful
  if (file.exists(dest_file)) {
    logger::log_info("Extracting: %s to %s", filename, extraction_dir)
    tryCatch({
      utils::unzip(dest_file, exdir = extraction_dir)
      extracted_files <- list.files(extraction_dir, recursive = TRUE, full.names = TRUE)
      logger::log_info("Successfully extracted %d files from %s", 
                       length(extracted_files), filename)
      
      # Find general payments file using both pattern matching and file size
      if (length(extracted_files) > 0) {
        logger::log_info("Looking for general payments file in %s", extraction_dir)
        
        # Get file information including patterns for general payment files
        file_info <- tibble(
          path = extracted_files,
          size = file.size(path),
          filename = basename(path)
        ) %>%
          mutate(
            # Common patterns for general payments files
            is_general = case_when(
              str_detect(tolower(filename), "gnrl|general") ~ TRUE,
              str_detect(tolower(filename), "research") ~ FALSE, 
              str_detect(tolower(filename), "ownership") ~ FALSE,
              str_detect(tolower(filename), "tof") ~ FALSE,  # Type of payment
              str_detect(tolower(filename), "pof") ~ FALSE,  # Form of payment
              # If year after 2016, look for OP_DTL_GNRL pattern
              as.numeric(year_dir) >= 2017 & str_detect(filename, "OP_DTL_GNRL") ~ TRUE,
              # For earlier years (2013-2016)
              as.numeric(year_dir) < 2017 & str_detect(filename, "OPPR_GNRL") ~ TRUE,
              # Default to FALSE if no pattern matches
              TRUE ~ FALSE
            )
          )
        
        # Find general payments files based on pattern matching
        general_files <- file_info %>% filter(is_general == TRUE)
        
        if (nrow(general_files) > 0) {
          # If multiple general files found, use the largest one
          if (nrow(general_files) > 1) {
            general_files <- general_files %>% arrange(desc(size))
            logger::log_info("Found %d potential general payment files, selecting largest one.", 
                             nrow(general_files))
          }
          
          keep_file <- general_files$path[1]
          keep_file_name <- general_files$filename[1]
          keep_file_size_mb <- round(general_files$size[1] / (1024^2), 2)
          
          logger::log_info("Selected general payments file: %s (%.2f MB)", 
                           keep_file_name, keep_file_size_mb)
        } else {
          # If no general pattern found, fall back to largest file
          file_info <- file_info %>% arrange(desc(size))
          logger::log_info("No clear general payments file pattern found. Using largest file as fallback.")
          
          keep_file <- file_info$path[1]
          keep_file_name <- file_info$filename[1]
          keep_file_size_mb <- round(file_info$size[1] / (1024^2), 2)
          
          logger::log_info("Selected largest file: %s (%.2f MB)", 
                           keep_file_name, keep_file_size_mb)
        }
        
        # Get files to remove (all except the one we're keeping)
        files_to_remove <- extracted_files[extracted_files != keep_file]
        
        if (length(files_to_remove) > 0) {
          logger::log_info("Removing %d other files...", length(files_to_remove))
          lapply(files_to_remove, function(f) {
            unlink(f)
            logger::log_info("Removed file: %s", basename(f))
          })
          
          logger::log_info("Kept only the selected file: %s", keep_file_name)
        } else {
          logger::log_info("Only one file found, keeping it: %s", keep_file_name)
        }
      }
      
      # Force garbage collection to manage memory
      gc_result <- gc(verbose = FALSE)
      mem_used_mb <- sum(gc_result[, 2]) / 1024  # Convert to MB
      logger::log_info("Current memory usage: %.2f MB", mem_used_mb)
      
    }, error = function(e) {
      logger::log_error("Error extracting %s: %s", filename, e$message)
    })
  }
  
  # Small delay between downloads to avoid overloading the server
  Sys.sleep(2)
}

logger::log_info("Download process completed")

# List all downloaded files
downloaded_files <- tibble(
  year = character(),
  file = character(),
  size_mb = numeric()
)

for (year_dir in unique(op_files$year)) {
  dir_path <- file.path(base_dir, year_dir)
  if (dir.exists(dir_path)) {
    zip_files <- list.files(dir_path, pattern = "\\.zip$|\\.ZIP$", full.names = TRUE)
    for (file in zip_files) {
      downloaded_files <- downloaded_files %>%
        add_row(
          year = year_dir,
          file = basename(file),
          size_mb = round(file.size(file) / 1024^2, 2)
        )
    }
  }
}

# Display summary of downloaded files
logger::log_info("Download summary:")
print(downloaded_files %>% arrange(year))




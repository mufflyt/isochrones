#==============================================================================
# NPPES Data Acquisition and Processing Pipeline
#==============================================================================
# 
# PART 1: Data Acquisition from NBER
# This script downloads National Provider Identifier (NPI) data files from
# the National Bureau of Economic Research (NBER) repository, providing
# historical snapshots of all healthcare providers in the United States.
#
# DATA SOURCE:
# Files obtained from NBER: http://data.nber.org/nppes/zip-orig/
#==============================================================================

library(tidyverse)  # For data manipulation and visualization
library(downloader)  # For downloading large files efficiently

#==============================================================================
# Stage 1: Download Historical NPI Files
#==============================================================================

# Base URL for the NBER repository containing historical NPI datasets
base_url <- "http://data.nber.org/nppes/zip-orig/"

# Complete list of files to download - covering 2007-2022 timespan
# These files contain complete snapshots of the entire NPI registry at different points in time
file_names <- c(
  "NPPES_Data_Disseminat_April_2021.zip",
  "NPPES_Data_Disseminat_April_2022.zip",
  "NPPES_Data_Dissemination_Apr_2009.zip",
  "NPPES_Data_Dissemination_Apr_2012.zip",
  "NPPES_Data_Dissemination_Apr_2013.zip",
  "NPPES_Data_Dissemination_Apr_2014.zip",
  "NPPES_Data_Dissemination_Apr_2016.zip",
  "NPPES_Data_Dissemination_April_2011.zip",
  "NPPES_Data_Dissemination_April_2015.zip",
  "NPPES_Data_Dissemination_April_2017.zip",
  "NPPES_Data_Dissemination_April_2018.zip",
  "NPPES_Data_Dissemination_April_2019.zip",
  "NPPES_Data_Dissemination_Feb_2010.zip",
  "NPPES_Data_Dissemination_February_2020.zip",
  "NPPES_Data_Dissemination_July_2020.zip",
  "NPPES_Data_Dissemination_June_2009.zip",
  "NPPES_Data_Dissemination_May_2008.zip",
  "NPPES_Data_Dissemination_Nov_2007.zip",
  "NPPES_Data_Dissemination_October_2020.zip"
)

# Create directory for storing large downloaded files
dest_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"
if (!dir.exists(dest_dir)) {
  dir.create(dest_dir, recursive = TRUE)
  message("Created download directory: ", dest_dir)
}

#' Download and extract NPI data files with error handling
#'
#' @param file_name Name of the zip file to download
#' @param base_url Base URL for the NBER repository
#' @param dest_dir Local directory for storing downloaded files
#' @return Logical indicating success (invisibly)
download_and_unzip <- function(file_name, base_url, dest_dir) {
  # Construct full URL and destination path
  file_url <- paste0(base_url, file_name)
  dest_path <- file.path(dest_dir, file_name)
  
  message("Downloading ", file_name, "...")
  
  # Download file with error handling
  download_status <- tryCatch({
    download(file_url, destfile = dest_path, mode = "wb")
    message("Download complete: ", file_name)
    TRUE
  }, error = function(e) {
    message("Error downloading ", file_name, ": ", e$message)
    FALSE
  })
  
  # Extract file contents if download successful
  if (download_status) {
    message("Extracting ", file_name, "...")
    unzip_status <- tryCatch({
      unzip(dest_path, exdir = dest_dir)
      message("Extraction complete: ", file_name)
      TRUE
    }, error = function(e) {
      message("Error unzipping ", file_name, ": ", e$message)
      FALSE
    })
    
    # File cleanup commented out to preserve original files
    # if (unzip_status) {
    #   unlink(dest_path)
    # }
  }
  
  invisible(download_status)
}

# Process each file sequentially
# This may take several hours depending on internet connection and file sizes
for (file_name in file_names) {
  download_and_unzip(file_name, base_url, dest_dir)
}

#==============================================================================
# Stage 2: Prepare Current NPI Data File
#==============================================================================

# Path to the most recent consolidated NPI data file
npi_filepath <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files/npidata_pfile_20050523-20240407.csv"

# Extract column names without loading the entire (very large) dataset
column_names <- names(read.csv(npi_filepath, nrow = 1)[FALSE, , ])

message("NPI data file contains ", length(column_names), " columns")
message("First 10 columns: ", paste(head(column_names, 10), collapse=", "))

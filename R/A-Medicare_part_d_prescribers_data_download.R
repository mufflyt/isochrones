# ------------------------------------------------------------------------------
# 1. Initial Setup
# ------------------------------------------------------------------------------
# Load global setup
source("R/01-setup.R")
# Libraries loaded include logging (logger), data handling (duckdb, dplyr, readr),
# string manipulation (stringr), and validation (assertthat). The logger is set
# up to document each operation clearly, facilitating troubleshooting and auditing.

# ------------------------------------------------------------------------------
# 2. Data Download (wget)
# ------------------------------------------------------------------------------
# The script uses wget to download Medicare Part D CSV files from data.cms.gov.
# URLs are explicitly defined for each file year (RY13 to RY22). Files are saved
# to a predefined directory, and wget ensures retries and continuation of downloads
# in case of interruptions.


# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------
# Load libraries
library(logger)
library(duckdb)
library(dplyr)
library(readr)
library(stringr)
library(assertthat)
library(tidyverse)  # For data manipulation and visualization
library(downloader)  # For downloading large files efficiently

# Conflict preferences
conflicted::conflicts_prefer(dplyr::filter)
conflicted::conflicts_prefer(lubridate::year)

#==============================================================================
# Stage 1: Download Medicare Part D files
#==============================================================================

# Directory to save the files
download_dir <- "/Volumes/Video Projects Muffly 1/medicare_part_d_ry24_files"
if (!dir.exists(download_dir)) {
  dir.create(download_dir, recursive = TRUE)
  logger::log_info("Created download directory: {download_dir}")
}

# Named vector of filenames and URLs
# Data Dictionary: https://data.cms.gov/resources/medicare-part-d-prescribers-by-provider-data-dictionary
# https://catalog.data.gov/dataset/medicare-part-d-prescribers-by-provider-and-drug-ad73e
# years 2013 to 2022

# Years after 2022!!!!!!!!!!!!!!!!!!!!! Thanks President Trump for not updating https://catalog.data.gov/
# The Medicare Part D Prescribers by Provider and Drug dataset
# Also you need to get the more recent data from: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider

file_urls <- c(
  "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/18f82097-61a6-4889-9941-9a0b6ad7523c/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv", 
  "MUP_DPR_RY24_P04_V10_DY21_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/43359391-e7fa-40b9-9bd4-5dc295e18712/MUP_DPR_RY24_P04_V10_DY21_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY20_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/75fecc51-c9e8-4904-b570-9da9dc101721/MUP_DPR_RY24_P04_V10_DY20_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY19_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/129e7c21-d492-425b-be03-d2e59d933ab6/MUP_DPR_RY24_P04_V10_DY19_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY18_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/be3019b4-1164-4b40-af5d-cab40847d222/MUP_DPR_RY24_P04_V10_DY18_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY17_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/d9d685a0-dc49-4da5-9416-4cf3e6349296/MUP_DPR_RY24_P04_V10_DY17_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY16_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/67c457c6-b62d-424f-ad85-2bb8117c928d/MUP_DPR_RY24_P04_V10_DY16_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY15_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/bc1caf7f-dcbb-4258-b243-ee3666b6a20b/MUP_DPR_RY24_P04_V10_DY15_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY14_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/06e87540-68a5-4a10-bf9c-a0521dc4ebed/MUP_DPR_RY24_P04_V10_DY14_NPIBN.csv",
  "MUP_DPR_RY24_P04_V10_DY13_NPIBN.csv" = "https://data.cms.gov/sites/default/files/2024-05/5fb694b1-2ec5-4e00-8efe-14161bdbdbea/MUP_DPR_RY24_P04_V10_DY13_NPIBN.csv"
)

# Check if wget is available
if (Sys.which("wget") == "") {
  stop("wget is not available on your system. Please install wget to proceed.")
}

# Loop through each file URL and download with retry logic
# Using wget because CMS file links are unstable in direct R downloading
for (file_name in names(file_urls)) {
  url <- file_urls[[file_name]]
  dest_file <- shQuote(file.path(download_dir, file_name))
  download_dir_quoted <- shQuote(download_dir)
  
  logger::log_info("Downloading {file_name} ...")
  
  cmd <- "wget"
  args <- c(
    "--tries=3",
    "--continue",
    "--timeout=600",
    "--directory-prefix", download_dir_quoted,
    "--output-document", dest_file,
    url
  )
  
  result <- system2(cmd, args, stdout = TRUE, stderr = TRUE)
  
  if (file.exists(gsub('\"', '', dest_file))) {
    logger::log_info("✅ Successfully downloaded: {file_name}")
  } else {
    logger::log_error("❌ Failed to download: {file_name}. Output:\n{paste(result, collapse = '\n')}")
  }
}

logger::log_info("🚚 All download attempts complete. Files saved in: {download_dir}")

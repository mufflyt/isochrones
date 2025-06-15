# ------------------------------------------------------------------------------
# Script: download_latest_nppes_deactivation_wget.R
# Purpose: Automatically download the most recent NPPES Deactivated NPI Report (Version 1)
# ------------------------------------------------------------------------------

# Load required libraries
library(rvest)
library(httr)
library(stringr)
library(logger)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

npi_page_url <- "https://download.cms.gov/nppes/NPI_Files.html"
download_dir <- "/Volumes/Video Projects Muffly 1/nppes_deactivation_reports"

# Initialize logger
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(format = "{level} [{time}] {msg}"))

# Check if wget is available
if (Sys.which("wget") == "") {
  stop("❌ 'wget' is not installed or not found in your system PATH.")
}

# Create download directory if it doesn't exist
if (!dir.exists(download_dir)) {
  dir.create(download_dir, recursive = TRUE)
  logger::log_info("Created directory: {download_dir}")
}

# ------------------------------------------------------------------------------
# Step 1: Load and parse the CMS NPPES download page
# ------------------------------------------------------------------------------

logger::log_info("Accessing NPPES download page: {npi_page_url}")
response <- httr::GET(npi_page_url, httr::user_agent("Mozilla/5.0"))

if (httr::status_code(response) != 200) {
  stop("❌ Failed to access CMS download page. Status code: ", httr::status_code(response))
}

html_page <- read_html(response)
logger::log_info("✅ Successfully read the NPPES HTML content")

# ------------------------------------------------------------------------------
# Step 2: Find the most recent Monthly NPI Deactivation Report (Version 1)
# ------------------------------------------------------------------------------

all_links <- html_page %>%
  html_nodes("a") %>%
  html_attr("href")

# Keep only Version 1 deactivation ZIPs
v1_links <- all_links[str_detect(all_links, "NPPES_Deactivated_NPI_Report_\\d{6}\\.zip$")]

# Build full URLs
v1_full_links <- str_c("https://download.cms.gov/nppes/", v1_links)

# Extract date codes and find the most recent file
dates <- str_extract(v1_full_links, "\\d{6}")
parsed_dates <- as.Date(dates, format = "%m%d%y")
latest_index <- which.max(parsed_dates)
latest_url <- v1_full_links[latest_index]

# Clean up "./" if present
latest_url <- str_replace(latest_url, "\\./", "")
latest_file <- basename(latest_url)
dest_file <- file.path(download_dir, latest_file)

# Quote the paths to safely handle spaces
quoted_dest_file <- shQuote(dest_file)
quoted_latest_url <- shQuote(latest_url)

logger::log_info("Preparing to download latest file: {latest_file}")
logger::log_info("Download URL: {latest_url}")
logger::log_info("Saving to: {dest_file}")

# ------------------------------------------------------------------------------
# Step 3: Use wget to download the file
# ------------------------------------------------------------------------------

wget_args <- c(
  "--continue",                    # resume support
  "--tries=3",                     # retry up to 3 times
  "--timeout=600",                 # 10-minute timeout
  "--output-document", quoted_dest_file,
  quoted_latest_url
)

result <- system2("wget", args = wget_args, stdout = TRUE, stderr = TRUE)

# ------------------------------------------------------------------------------
# Step 4: Confirm download success and log result
# ------------------------------------------------------------------------------

if (file.exists(dest_file)) {
  logger::log_info("✅ Successfully downloaded: {latest_file}")
} else {
  logger::log_error("❌ Download failed.")
  logger::log_error("wget output:\n{paste(result, collapse = '\n')}")
}

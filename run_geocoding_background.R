# Background geocoding execution script
library(job)
library(logger)

# Setup logging to file
log_appender(appender_file("geocoding_background.log"))

log_info("Starting background geocoding job...")

# Run the geocoding as a background job
geocoding_job <- job::job({
  # Source all required libraries and functions
  library(dplyr)
  library(readr)
  library(hereR)
  library(logger)
  
  # Setup file logging inside the job
  log_appender(appender_file("geocoding_progress.log"))
  
  # Load and run the geocoding
  source("geocode_optimized_addresses.R")
  
}, title = "Physician Address Geocoding", 
   export = c(), # Add any variables you need to export
   packages = c("dplyr", "readr", "hereR", "logger"))

log_info("Background job started with ID: {geocoding_job}")
log_info("Monitor progress with: tail -f geocoding_progress.log")
log_info("Check job status in RStudio Jobs pane or with job::job_status({geocoding_job})")

# Function to check job status
check_geocoding_status <- function() {
  status <- job::job_status(geocoding_job)
  cat("Job Status:", status, "\n")
  
  if (file.exists("geocoding_progress.log")) {
    cat("\n--- Recent Progress ---\n")
    system("tail -10 geocoding_progress.log")
  }
  
  return(status)
}

# Print instructions
cat("Background geocoding started!\n")
cat("Commands to monitor:\n")
cat("- check_geocoding_status()  # Check job status\n")  
cat("- tail -f geocoding_progress.log  # Follow progress\n")
cat("- View > Jobs pane in RStudio    # Visual monitoring\n")
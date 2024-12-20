# Downloading multiple years of NPPES files from NBER, unzip, and gather the appropriate files into one directory.  Many thanks to NBER for caching the NPI file for multiple years at http://data.nber.org/nppes/zip-orig/.  These files are quite large so I used an external hard drive to save the data.  

# Required Input Files for Script Execution ----

# 1. NPPES Files from CMS:
#    - Base URL: "https://download.cms.gov/nppes/"
#    - Files:
#      - "NPPES_Data_Dissemination_April_2024.zip"

# 2. NPPES Files from NBER:
#    - Base URL: "http://data.nber.org/nppes/zip-orig/"
#    - Files:
#      - "NPPES_Data_Dissemination_Nov_2007.zip"
#      - "NPPES_Data_Dissemination_May_2008.zip"
#      - "NPPES_Data_Dissemination_June_2009.zip"
#      - "NPPES_Data_Dissemination_Feb_2010.zip"
#      - "NPPES_Data_Dissemination_April_2011.zip"
#      - "NPPES_Data_Dissemination_April_2012.zip"
#      - "NPPES_Data_Dissemination_April_2013.zip"
#      - "NPPES_Data_Dissemination_April_2014.zip"
#      - "NPPES_Data_Dissemination_April_2015.zip"
#      - "NPPES_Data_Dissemination_April_2016.zip"
#      - "NPPES_Data_Dissemination_April_2017.zip"
#      - "NPPES_Data_Dissemination_April_2018.zip"
#      - "NPPES_Data_Dissemination_April_2019.zip"
#      - "NPPES_Data_Dissemination_July_2020.zip"
#      - "NPPES_Data_Dissemination_April_2021.zip"
#      - "NPPES_Data_Dissemination_April_2022.zip"



# Setup -------------------------------------------------------------------
source("R/01-setup.R")


# NPPES file download -----------------------------------------------------
# https://download.cms.gov/nppes/NPI_Files.html
# https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip
base_url <- "https://download.cms.gov/nppes/"
file_names <- c("NPPES_Data_Dissemination_July_2024.zip")
dest_dir <- "Video Projects Muffly 1/nppes_historical_downloads"

# Ensure the destination directory exists - create using system call
dir_create_command <- sprintf("mkdir -p %s", shQuote(dest_dir))
system(dir_create_command)

# Loop over the file names and download each
for (file_name in file_names) {
  download_file(file_name, base_url, dest_dir)
}

# NPPES unzip -------------------------------------------------------------
# Unzip the file
unzip(file.path(dest_dir, file_name), exdir = dest_dir)

# After downloading all files, loop over the file names and unzip each into its own subdirectory
for (file_name in file_names) {
  unzip_file(file_name, dest_dir)
}

# Copy largest file to a single directory
base_unzip_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"

# Target directory for the largest files from each year
target_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files"

# Ensure the target directory exists
if (!dir_exists(target_dir)) {
  dir_create(target_dir)
}

# NPPES Copy largest file to folder ---------------------------------------
# Execute the function
copy_largest_file_from_each_year(base_unzip_dir, target_dir)

#######

########################################
# Multiple downloads from NBER
# Base URL for the files
base_url <- "http://data.nber.org/nppes/zip-orig/"

# Destination directory on the external hard drive
dest_dir <- "/Volumes/Video\\ Projects\\ Muffly\\ 1/nppes_historical_downloads"

# Ensure the destination directory exists - create using system call
dir_create_command <- sprintf("mkdir -p %s", shQuote(dest_dir))
system(dir_create_command)

# Complete list of files to download
file_names <- c(
  "NPPES_Data_Dissemination_Nov_2007.zip",
  "NPPES_Data_Dissemination_May_2008.zip",
  "NPPES_Data_Dissemination_June_2009.zip",
  #"NPPES_Data_Dissemination_Apr_2009.zip",
  "NPPES_Data_Dissemination_Feb_2010.zip",
  "NPPES_Data_Dissemination_April_2011.zip",
  "NPPES_Data_Dissemination_Apr_2012.zip",
  "NPPES_Data_Dissemination_Apr_2013.zip",
  "NPPES_Data_Dissemination_Apr_2014.zip",
  "NPPES_Data_Dissemination_April_2015.zip",
  "NPPES_Data_Dissemination_Apr_2016.zip",
  "NPPES_Data_Dissemination_April_2017.zip",
  "NPPES_Data_Dissemination_April_2018.zip",
  "NPPES_Data_Dissemination_April_2019.zip",
  #"NPPES_Data_Dissemination_February_2020.zip",
  "NPPES_Data_Dissemination_July_2020.zip",
  #"NPPES_Data_Dissemination_October_2020.zip",
  "NPPES_Data_Disseminat_April_2021.zip",
  "NPPES_Data_Disseminat_April_2022.zip"
)

# Destination directory on the external hard drive, without escaping spaces
dest_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"

# Ensuring the directory creation command does not use backslashes to escape spaces
dir_create_command <- sprintf("mkdir -p '%s'", dest_dir)
system(dir_create_command)

# Loop over the file names and download each
for (file_name in file_names) {
  download_file(file_name, base_url, dest_dir)
}


#### UNZIP
# Destination directory on the external hard drive, without escaping spaces
dest_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"

# Ensuring the directory creation command does not use backslashes to escape spaces
dir_create_command <- sprintf("mkdir -p '%s'", dest_dir)
system(dir_create_command)

# Complete list of files to download (make sure this matches your list)
file_names <- c(
  "NPPES_Data_Dissemination_Nov_2007.zip",
  "NPPES_Data_Dissemination_May_2008.zip",
  "NPPES_Data_Dissemination_June_2009.zip",
  #"NPPES_Data_Dissemination_Apr_2009.zip",
  "NPPES_Data_Dissemination_Feb_2010.zip",
  "NPPES_Data_Dissemination_April_2011.zip",
  "NPPES_Data_Dissemination_Apr_2012.zip",
  "NPPES_Data_Dissemination_Apr_2013.zip",
  "NPPES_Data_Dissemination_Apr_2014.zip",
  "NPPES_Data_Dissemination_April_2015.zip",
  "NPPES_Data_Dissemination_Apr_2016.zip",
  "NPPES_Data_Dissemination_April_2017.zip",
  "NPPES_Data_Dissemination_April_2018.zip",
  "NPPES_Data_Dissemination_April_2019.zip",
  #"NPPES_Data_Dissemination_February_2020.zip",
  "NPPES_Data_Dissemination_July_2020.zip",
  #"NPPES_Data_Dissemination_October_2020.zip",
  "NPPES_Data_Disseminat_April_2021.zip",
  "NPPES_Data_Disseminat_April_2022.zip"
)

# After downloading all files, loop over the file names and unzip each into its own subdirectory
for (file_name in file_names) {
  unzip_file(file_name, dest_dir)
}

############################################################
# Copy largest file to a single directory
# Base directory where files were unzipped
base_unzip_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"

# Target directory for the largest files from each year
target_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads/unzipped_p_files"

# Ensure the target directory exists
if (!dir_exists(target_dir)) {
  dir_create(target_dir)
}

# Execute the function
copy_largest_file_from_each_year(base_unzip_dir, target_dir)

#######


# Facility Affiliation ----------------------------------------------------
file_dir <- "/Volumes/Video Projects Muffly 1/facility_affiliation"
file_names <- list.files(file_dir)
dest_dir <- "/Volumes/Video Projects Muffly 1/facility_affiliation"

# Ensure the destination directory exists - create using system call
dir_create_command <- sprintf("mkdir -p %s", shQuote(dest_dir))
system(dir_create_command)

# Facility affiliation unzip ---------------------------------------------------------
# After downloading all files, loop over the file names and unzip each into its own subdirectory
for (file_name in file_names) {
  unzip_file(file_name, dest_dir)
}

# Copy largest file to a single directory
base_unzip_dir <- "/Volumes/Video Projects Muffly 1/facility_affiliation"

# Target directory for the largest files from each year
target_dir <- "/Volumes/Video Projects Muffly 1/facility_affiliation/unzipped_files"

# Ensure the target directory exists
if (!dir_exists(target_dir)) {
  dir_create(target_dir)
}

# Facility affiliation Copy largest file to folder ---------------------------------
# Execute the function
conflicted::conflicts_prefer(dplyr::bind_rows)
copy_largest_file_from_each_year(base_unzip_dir, target_dir)



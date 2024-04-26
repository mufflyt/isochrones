# Downloading multiple years of NPPES files from NBER, unzip, and gather the appropriate files into one directory.  Many thanks to NBER for caching the NPI file for multiple years at http://data.nber.org/nppes/zip-orig/.  These files are quite large so I used an external hard drive to save the data.  

# NBER Directory: /homes/data/nppes/zip-orig/
#   Name	Last modified	Size	Description
# Parent Directory	 	-	 
# NPPES_Data_Disseminat_April_2021.zip	2021-12-06 16:06	772M	 
# NPPES_Data_Disseminat_April_2022.zip	2022-06-04 06:21	832M	 
# NPPES_Data_Dissemination_Apr_2009.zip	2020-08-08 14:59	322M	 
# NPPES_Data_Dissemination_Apr_2012.zip	2020-08-08 14:59	403M	 
# NPPES_Data_Dissemination_Apr_2013.zip	2020-08-08 14:59	434M	 
# NPPES_Data_Dissemination_Apr_2014.zip	2020-08-08 14:59	463M	 
# NPPES_Data_Dissemination_Apr_2016.zip	2020-08-08 14:59	559M	 
# NPPES_Data_Dissemination_April_2011.zip	2020-08-08 14:59	381M	 
# NPPES_Data_Dissemination_April_2015.zip	2020-08-08 14:59	494M	 
# NPPES_Data_Dissemination_April_2017.zip	2020-08-08 14:59	594M	 
# NPPES_Data_Dissemination_April_2018.zip	2020-08-08 14:59	594M	 
# NPPES_Data_Dissemination_April_2019.zip	2020-08-08 14:59	664M	 
# NPPES_Data_Dissemination_Feb_2010.zip	2020-08-09 10:12	346M	 
# NPPES_Data_Dissemination_February_2020.zip	2020-08-08 14:59	705M	 
# NPPES_Data_Dissemination_July_2020.zip	2020-08-08 14:59	725M	 
# NPPES_Data_Dissemination_June_2009.zip	2020-08-08 14:59	326M	 
# NPPES_Data_Dissemination_May_2008.zip	2020-08-08 15:00	296M	 
# NPPES_Data_Dissemination_Nov_2007.zip	2020-08-08 14:59	256M	 
# NPPES_Data_Dissemination_October_2020.zip	2020-10-30 13:53	740M	 

# The omission of short-lived providers may be a source of bias in certain applications, such as studies of fraudulent providers, the presence of all reasonably persistent providers with their historical data is an improvement over using only survivors.

source("R/01-setup.R")

#### Download the NPPES file at https://download.cms.gov/nppes/NPI_Files.html
# https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip
base_url <- "https://download.cms.gov/nppes/"
file_names <- c("NPPES_Data_Dissemination_April_2024.zip")
dest_dir <- "Video Projects Muffly 1/nppes_historical_downloads"

# Ensure the destination directory exists - create using system call
dir_create_command <- sprintf("mkdir -p %s", shQuote(dest_dir))
system(dir_create_command)

# Loop over the file names and download each
for (file_name in file_names) {
  download_file(file_name, base_url, dest_dir)
}

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
library(tidyverse)

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
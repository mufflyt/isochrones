I am interested in looking at the availability of access to over 5,000 OBGYN subspecialists over time (2013 to 2022) in the United States.  The subspecialists include: Female Pelvic Medicine and Reconstructive Surgery/Urogynecology, Gynecologic Oncology, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.  The topic is physician workforce planning, where there is the greatest need for more physicians to serve women 18 years and older.  Patients will be driving by car to create the isochrones.  I do not have access to ArcGIS due to cost and so I would like to create everything using R.  Ideally we would use the “sf” package as “sp” is being deprecated.  I'm hiring because I have too much work to do alone. I would like to start work next week.  

What I am providing:
1) A github repository with the scripts and the data needed to create the code: www.github.com/mufflyt.  Most of this project is based off of two projects by Hannah Recht.  www.github.com/hrecht

Deliverables:
1) Source code in R
2) Scripts that create year-specific isochrones and year-specific physicians and year specific block groups
3) Gather Census data for each of the years for all the block groups.  
4) Create a static map of each year (2013 to 2022) plotting the year-specific physicians and year-specific isochrones
a. Isochrones of different colors for 15 minutes, 30 minutes, 60 minutes, 180 minutes, and 240 minutes
b. Physicians as dots with different colors based on specialty (sub1)
c. ACOG Districts outlined with no fill
d. A compass rose and a scale bar in miles
e. Legend of isochrones ordered from 15 minutes on top to 240 minutes on bottom
f. Title at the top center of the map
g. Saved as a TIFF file
5) Leaflet map with the same features as above please
6) Counts of the number of women (Total, White, Black , Asian, American Indian/Alaska Native, Native Hawaiian/Pacific Islander) who are within each of the 15 minutes, 30 minutes, 60 minutes, 180 minutes, and 240 minutes from each subspecialty(Female Pelvic Medicine and Reconstructive Surgery/Urogynecology, Gynecologic Oncology, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.)

Sample Abstract

Geographic Disparities in Potential Accessibility to Gynecologic Oncologists in the United States from 2013 to 2022.  

Objective: To use a spatial modeling approach to capture potential disparities of gynecologic oncologist accessibility in the United States at the block group level between 2013 and 2022.

Methods: Physician registries identified the 2013 to 2022 gynecologic oncology workforce and were aggregated to each county.  The at-risk cohort (women aged 18 years or older) was stratified by race and rurality demographics.  We computed the distance from at-risk women to physicians.  We set drive time to 30, 60, 180, and 240 minutes.  

Results: Between 2013 and 2022, the gynecologic oncology workforce increased.  By 2022, there were x active physicians, and x% practiced in urban block groups.  Geographic disparities were identified, with x physicians per 100,000 women in urban areas compared with 0.1 physicians per 100,000 women in rural areas.  In total, x block groups (x million at-risk women) lacked a gynecologic oncologist.  Additionally, there was no increase in rural physicians, with only x% practicing in rural areas in 2013-2015 relative to ??% in 2016-2022 (p=?).  Women in racial minority populations exhibited the lowest level of access to physicians across all periods.  For example, xx% of American Indian or Alaska Native women did not have access to a physician with a 3-hour drive from 2013-2015, which did not improve over time.  Black women experience an increase in relative accessibility, with a ??% increase by 2016-2022.  However, Asian or Pacific Islander women exhibited significantly better access than ???  women across all periods.  

Conclusion:  Although the US gynecologic oncologist workforce has increased steadily over 20 years, this has not translated into evidence of improved access for many women from rural and underrepresented areas.  However, distance and availability may not influence healthcare utilization and cancer outcomes only.  



Methods:
There are subspecialists in obstetrics and gynecology across the United States in four categories: Gynecologic Oncology, Female Pelvic Medicine and Reconstructive Surgery, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.  We gathered data from the National Plan and Provider Enumeration System (NPPES) to extract each obstetrician-gynecologist and their address of practice (available at https://www.nber.org/research/data/national-plan-and-provider-enumeration-system-nppes).  The NPPES file contains a list of every physician practicing in the United States and is closest to a physician data clearinghouse.  This data also includes their graduation and license year.  Therefore, we identified the approximate year that each subspecialist entered the workforce as a physician.  We had data for NPPES files from 2013 to 2023.  

Utilization data for each physician were unavailable, so our analysis focused on at-risk women aged 18 years or older in the United States at the block group level (Appendix 0).  Analysis was also done at the American College of Obstetricians and Gynecologists District level (Appendix 1).  We collected American Community Survey 5-year estimates and decennial census data.  Along with the total adult female population in each cross-section, we also collected population on race to identify potential disparities among women in minority populations (Appendix 2).  The race categories included White, Black, Asian or Pacific Islander, and American Indian or Alaska Native.  

To measure the distance between the patient location and the point of care (hospital or emergency department), the shortest or "straight-line" distance (i.e., the geodetic or great circle distance) is commonly used because it can be readily calculated (e.g., through statistical software programs such as SAS®).  An alternative distance metric is the driving distance or driving times obtained from various mapping software such as HERE, Google Maps, MapQuest, OpenStreetMaps, and ArcGIS Network Analyst.  Multiple organizations, such as the Veteran’s Administration and the Department of Transportation, prefer drive time for measuring access.  We use the HERE API to calculate optimal routes and directions for driving with traffic on the third Friday in October at 0900 (Appendix 3). 

Additionally, our analysis examined potential access to obstetrician-gynecologist subspecialists across the United States.  We used drive time isochrones.  

Appendix 0:
Choosing block groups over counties for data analysis could be driven by several factors, depending on the nature of the analysis you are conducting:

•	Resolution of Data: Block groups provide a finer data resolution than counties. 
•	Local Trends and Patterns: Block groups are small enough to reveal local trends and patterns that might be obscured when data is aggregated at the county level.
•	Socioeconomic Analysis: Block groups can provide more precise information about demographic and economic conditions for studies involving socioeconomic factors since they represent smaller communities.
•	Policy Impact Assessment: Block groups may be more appropriate when assessing the impact of local policies or interventions because policies might vary significantly within a county.
•	Spatial Analysis: For spatial analyses that require precision, such as identifying hotspots or conducting proximity analysis, the smaller geographic units of block groups are more valuable.

We utilized the tigris package to get the block group geometries for different years.  This required downloading each state’s data and joining them into one national file for years 2018 and earlier.  

Appendix 1: The American College of Obstetricians and Gynecologists (ACOG) is a professional organization representing obstetricians and gynecologists in the United States.  ACOG divides its membership into various geographical regions known as "ACOG Districts."

```r
State	        ACOG_District 	State_Abbreviations
Alabama	District VII	AL
Alaska		District VIII	AK
Arizona	District VIII	AZ
Arkansas	District VII	AR
California	District IX	CA
Colorado	District VIII	CO
Connecticut	District I	CT
District of Columbia	District IV	DC
Florida		District XII	FL
Georgia	District IV	GA
Hawaii		District VIII	HI
Illinois		District VI	IL
Indiana	District V	IN
Iowa		District VI	IA
Kansas		District VII	KS
Kentucky	District V	KY
Louisiana	District VII	LA
Maryland	District IV	MD
Massachusetts	District I	MA
Michigan	District V	MI
Minnesota	District VI	MN
Mississippi	District VII	MS
Missouri	District VII	MO
Nebraska	District VI	NE
Nevada	District VIII	NV
New Hampshire District I	NH
New Jersey	District III	NJ
New Mexico	District VIII	NM
New York	District II	NY
North Carolina	District IV	NC
North Dakota	District VI	ND
Ohio		District V	OH
Oklahoma	District VII	OK
Oregon	District VIII	OR
Pennsylvania	District III	PA
Puerto Rico	District IV	PR
Rhode Island	District I	RI
South Carolina	District IV	SC
South Dakota	District VI	SD
Tennessee	District VII	TN
Texas		District XI	TX
Utah		District VIII	UT
Vermont	District I	VT
Virginia	District IV	VA
Washington	District VIII	WA
West Virginia	District IV	WV
Wisconsin	District VI	WI
```

Appendix 2: US Census Bureau Codes Decennial Census and the American Community Survey.
Decennial Census – Demographic and Housing Characteristics File (API variables: https://api.census.gov/data/2020/dec/dhc/variables.html)

(https://www2.census.gov/programs-surveys/decennial/2020/technical-documentation/complete-tech-docs/demographic-and-housing-characteristics-file-and-demographic-profile/2020census-demographic-and-housing-characteristics-file-and-demographic-profile-techdoc.pdf )


American Community Survey 
(API variables: https://api.census.gov/data/2022/acs/acs1/variables.html )
```r
B01001_026E  Estimate _Total _Female
    female_10_to_14 = "B01001_029",
    female_15_to_17 = "B01001_030",
    female_18_to_19 = "B01001_031",
    female_20years = "B01001_032",
    female_21years = "B01001_033",
    female_22_to_24 = "B01001_034",
    female_25_to_29 = "B01001_035",
    female_30_to_34 = "B01001_036",
    female_35_to_39 = "B01001_037",
    female_40_to_44 = "B01001_038",
    female_45_to_49 = "B01001_039",
    female_50_to_54 = "B01001_040",
    female_55_to_59 = "B01001_041",
    female_60_to_61 = "B01001_042",
    female_62_to_64 = "B01001_043",
    female_65_to_66 = "B01001_044",
    female_67_to_69 = "B01001_045",
    female_70_to_74 = "B01001_046",
    female_75_to_79 = "B01001_047",
    female_80_to_84 = "B01001_048",
    female_85_over = "B01001_049"
````

## US Decennial Census Demographic and Housing
```r
    name	label	concept
P12Y_026N	Female:	AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12Z_026N	Female:	ASIAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12X_026N	Female:	BLACK OR AFRICAN AMERICAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12AA_026N	Female:	NHPI ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12W_026N	Female:	WHITE ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
```

Appendix 3: Characteristics of Isochrones
The bespoke R code generates individual maps for each drive time, visually representing the accessible areas on a map.  The function shapefiles are geospatial data files used for storing geographic information, including the boundaries of the reachable areas.  The HERE API (here.com) was utilized because traffic and time could be standardized yearly.  Each year, the isochrones are built on the third Friday in October at 0900, defined as “posix_time”.  We imagined that patients would see their primary care provider at this time of year for an influenza vaccination or other issues.  The hereR package (https://github.com/munterfi/hereR/ ) is a wrapper around the R code that calls the HERE REST API for isoline routing (platform.here.com) and returns it as an sf object.  There is a cost of $5.50 for every 1,000 isolines created (https://www.here.com/get-started/pricing#here---platform---pricing---page-title ).  
![Screenshot 2024-05-03 at 8 51 55 PM](https://github.com/mufflyt/isochrones/assets/44621942/afbb3f48-2e6c-43bb-a324-c56beb937ede)

```r
iso_datetime_yearly <- tibble(
  date = c(
    "2013-10-18 09:00:00",
    "2014-10-17 09:00:00",
    "2015-10-16 09:00:00",
    "2016-10-21 09:00:00",
    "2017-10-20 09:00:00",
    "2018-10-19 09:00:00",
    "2019-10-18 09:00:00",
    "2020-10-16 09:00:00",
    "2021-10-15 09:00:00",
    "2022-10-21 09:00:00",
    "2023-10-20 09:00:00"
  ),
  year = c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023")
)
```

R code utilizing the hereR package with the isoline library.  The range of isochrones was 30 minutes, 60 minutes, 120 minutes, and 180 minutes.  
```r
isochrones_sf <- process_and_save_isochrones(input_file_no_error_rows, 
                                             chunk_size = 25, 
                                             iso_datetime = "2023-10-20 09:00:00",
                                             iso_ranges = c(30*60, 60*60, 120*60, 180*60),
                                             crs = 4326, 
                                             transport_mode = "car",
                                             file_path_prefix = "data/06-isochrones/isochrones_")
```

![image](https://github.com/mufflyt/Geography/assets/44621942/d246f85e-4b77-463e-9cb9-69c0e6623e2e)


####
# Tools

We used github LFS (large file storage).  2GB is the largest that LFS can handle.  Do NOT exceed. Upload ONE directory at a time after checking the size.  

```r
brew install git-lfs
git lfs install
sudo git lfs install --system
git config http.postBuffer 524288000
git lfs track "*.dbf" "*.shp"
git ls-tree -r -t -l HEAD | sort -k 4 -n -r | head -n 10
git add .gitattributes
git commit -m "Track large files with Git LFS"
git push origin main
```

```r
git lfs track "bg_shp_joined_intersect_pct.rds"
git add .gitattributes
git commit -m "Track bg_shp_joined_intersect_pct.rds with Git LFS"
git push origin main
```

```r
git add .gitattributes
git add *.shp
git add *.dbf
git add *.shx
git add *.prj
git commit -m "Reconfigure Git LFS for large files"
git push origin main
```

# Download the NPPES files.  
* I am using Terminal to download the NPPES files with `libcurl` since it has better handling of large data files.  If you're comfortable with the command line, wget or curl can be used directly from Terminal to download files. These tools often provide more robust handling of large file downloads and network issues:
![Screenshot 2024-04-07 at 9 51 41 PM](https://github.com/mufflyt/isochrones/assets/44621942/89b70218-88a6-4c31-997e-ad77bdf765dc)

* You can download the data from NPPES directly.  This downloads directly to the external hard drive called `Video Projects Muffly 1`.
```r
wget -P "/Volumes/Video Projects Muffly 1/nppes_historical_downloads" "https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip"
```



* Here we are downloading `npiall` file from NBER from the command line:  
```r
diskutil list
# External drives are mounted under /Volumes on macOS
ls /Volumes
options(timeout = 100000)  # Set timeout to a high value
mkdir -p /Volumes/Video\ Projects\ Muffly\ 1/nppes_historical_downloads
curl -o /Volumes/Video\ Projects\ Muffly\ 1/nppes_historical_downloads/NPPES_Data_Disseminat_April_2021.zip http://data.nber.org/nppes/zip-orig/NPPES_Data_Disseminat_April_2021.zip
```

* R code to run the Terminal for multiple large downloads in the `code/02.25-downloader.R' file.  
```
library(tidyverse)

# Ensure necessary package is installed
if (!require("downloader")) install.packages("downloader")
library(downloader)

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

# Function to construct and execute curl command for each file
download_file <- function(file_name, base_url, dest_dir) {
  file_url <- paste0(base_url, file_name)
  
  # Adjusting for the correct handling of spaces in the file path
  # Notice the use of shQuote() for the entire destination path including the file name
  dest_path <- file.path(dest_dir, file_name)
  full_dest_path <- shQuote(dest_path)
  
  # Construct the curl command
  curl_command <- sprintf("curl -o %s %s", full_dest_path, shQuote(file_url))
  
  # Execute the command
  system(curl_command)
  
  cat("Attempted download: ", file_name, "\n")
}

# Destination directory on the external hard drive, without escaping spaces
dest_dir <- "/Volumes/Video Projects Muffly 1/nppes_historical_downloads"

# Ensuring the directory creation command does not use backslashes to escape spaces
dir_create_command <- sprintf("mkdir -p '%s'", dest_dir)
system(dir_create_command)

# Loop over the file names and download each
for (file_name in file_names) {
  download_file(file_name, base_url, dest_dir)
}
```

### Unzip
We then unzipped each zip file and moved the largest file (usually the pfile .csv) to a separate directory where we can store them all together.  This needs to run overnight.  
```
### Unzip
library(tidyverse)

# Function to unzip a file into a separate subdirectory
unzip_file <- function(file_name, dest_dir) {
  # Construct the full path to the zip file
  zip_path <- file.path(dest_dir, file_name)
  
  # Create a unique subdirectory based on the file name (without the .zip extension)
  sub_dir_name <- tools::file_path_sans_ext(basename(file_name))
  sub_dir_path <- file.path(dest_dir, sub_dir_name)
  
  # Ensure the subdirectory exists
  if (!dir.exists(sub_dir_path)) {
    dir.create(sub_dir_path, recursive = TRUE)
  }
  
  # Unzip the file into the subdirectory
  unzip_status <- tryCatch({
    unzip(zip_path, exdir = sub_dir_path)
    TRUE
  }, warning = function(w) {
    message("Warning unzipping ", file_name, ": ", w$message)
    FALSE
  }, error = function(e) {
    message("Error unzipping ", file_name, ": ", e$message)
    FALSE
  })
  
  if (unzip_status) {
    message("Unzipped: ", file_name, " into ", sub_dir_path)
  }
}

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
```

### Moving files:
```
############################################################
# Copy largest file to a single directory

library(fs)
library(dplyr)

# Function to find and copy the largest file from each year's unzipped subdirectory
copy_largest_file_from_each_year <- function(base_unzip_dir, target_dir) {
  # List all subdirectories within the base directory
  subdirs <- dir_ls(base_unzip_dir, recurse = TRUE, type = "directory")
  
  # Loop through each subdirectory
  for (subdir in subdirs) {
    # Define an empty tibble to hold file info
    file_info_df <- tibble(filepath = character(), filesize = numeric())
    
    # List all files in the subdirectory
    files <- dir_ls(subdir, recurse = TRUE, type = "file")
    
    # Skip if no files found
    if (length(files) == 0) next
    
    # Get sizes of all files
    file_sizes <- file_info(files)$size
    
    # Append to the dataframe
    file_info_df <- bind_rows(file_info_df, tibble(filepath = files, filesize = file_sizes))
    
    # Identify the largest file in the current subdirectory
    largest_file <- file_info_df %>% 
      arrange(desc(filesize)) %>% 
      slice(1) %>% 
      pull(filepath)
    
    # Extract year and filename for the target path
    year_subdir_name <- basename(dirname(largest_file))
    largest_file_name <- basename(largest_file)
    target_file_name <- paste(year_subdir_name, largest_file_name, sep="_")
    target_file_path <- file.path(target_dir, target_file_name)
    
    # Copy the largest file to the target directory with modified name
    if (!is.na(largest_file) && file_exists(largest_file)) {
      file_copy(largest_file, target_file_path, overwrite = TRUE)
      message("Copied the largest file from ", year_subdir_name, ": ", largest_file_name, " to ", target_dir)
    }
  }
}

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
```

# Read the data into duckDB
![image](https://github.com/mufflyt/isochrones/assets/44621942/f9348ac4-72d4-4149-8d08-38a9c1fabf32)
We are using duckDB because it is lightning fast and works with R through 'duckplyr' so dplyr verbs can be used.  Then a schema can be built so that only the rows of interest are read in to R.  


# Postico
We needed a database program to house each of the NPI files from each year due to RAM restrictions.  Postico is a client for PostgreSQL, and it allows you to execute raw SQL queries, including DDL statements._full_
<img width="700" alt="Screenshot 2024-01-13 at 8 25 35 PM" src="https://github.com/mufflyt/isochrones/assets/44621942/86e54895-3358-4ff7-96d6-0e3e9b120a46">

The database is Postgres, template1.  Horrible naming job Tyler.  This was a real pain in the ass because there were several rows of the same physician but with different addresses.  From what I could tell, the best data was in the Primary Specialty == GYNECOLOGIC ONCOLOGY, so a rank system was put in place to preferentially take that data.  
<img width="1440" alt="Screenshot 2024-04-07 at 10 29 19 AM" src="https://github.com/mufflyt/isochrones/assets/44621942/82a4ba0a-6301-488d-a6a3-80f437191fd8">

### Create a database and start the postgresql database in terminal:
```r
# This command connects to the default PostgreSQL database 'postgres' as the 'postgres' user and executes the SQL command to create a new database named 'nppes_isochrones'.
psql -d postgres -U postgres -c "CREATE DATABASE nppes_isochrones;"

# This command connects to the 'nppes_isochrones' database as the 'postgres' user. 
# It opens the PostgreSQL command line interface for the 'nppes_isochrones' database, allowing the user to execute SQL commands directly.
psql -d nppes_isochrones -U postgres
```
<img width="573" alt="Screenshot 2024-04-07 at 7 15 16 PM" src="https://github.com/mufflyt/isochrones/assets/44621942/b13e35d6-46df-46c5-bf52-d7bce1368abc">

#### Reading in the files to postgresql
The original .csv files have a header with variable descriptions unsuitable for variable names in a database or statistical package. Therefore we have created variable names and turned the supplied header into variable labels.  Wow huge thanks to feenberg@nber.org for a ton of work on standardizing the headers for NPPES files over the years.  

```r
DROP TABLE IF EXISTS nppes_2023;

CREATE TABLE nppes_2023 (
    "NPI" TEXT,
    "Entity Type Code" TEXT,
    "Provider Organization Name (Legal Business Name)" TEXT,
    "Provider Last Name (Legal Name)" TEXT,
    "Provider First Name" TEXT,
    "Provider Middle Name" TEXT,
    "Provider Name Prefix Text" TEXT,
    "Provider Name Suffix Text" TEXT,
    "Provider Credential Text" TEXT,
    "Provider Other Organization Name" TEXT,
    "Provider Other Organization Name Type Code" TEXT,
    "Provider First Line Business Mailing Address" TEXT,
    "Provider Second Line Business Mailing Address" TEXT,
    "Provider Business Mailing Address City Name" TEXT,
    "Provider Business Mailing Address State Name" TEXT,
    "Provider Business Mailing Address Postal Code" TEXT,
    "Provider Business Mailing Address Country Code (If outside U.S.)" TEXT,
    "Provider Business Mailing Address Telephone Number" TEXT,
    "Provider Business Mailing Address Fax Number" TEXT,
    "Provider First Line Business Practice Location Address" TEXT,
    "Provider Second Line Business Practice Location Address" TEXT,
    "Provider Business Practice Location Address City Name" TEXT,
    "Provider Business Practice Location Address State Name" TEXT,
    "Provider Business Practice Location Address Postal Code" TEXT,
    "Provider Business Practice Location Address Country Code (If outside U.S.)" TEXT,
    "Provider Business Practice Location Address Telephone Number" TEXT,
    "Provider Business Practice Location Address Fax Number" TEXT,
    "Provider Enumeration Date" DATE,
    "Last Update Date" DATE,
    "Provider Gender Code" TEXT,
    "Healthcare Provider Taxonomy Code_1" TEXT,
    "Provider License Number_1" TEXT,
    "Provider License Number State Code_1" TEXT,
    "Healthcare Provider Primary Taxonomy Switch_1" TEXT,
    "Healthcare Provider Taxonomy Code_2" TEXT,
    "Provider License Number_2" TEXT,
    "Provider License Number State Code_2" TEXT,
    "Healthcare Provider Primary Taxonomy Switch_2" TEXT
);
```

Using postgresql directly without using Postico reading in huge databases is possible.  
```r
library(DBI)
library(RPostgres)

# Establish a connection
con <- dbConnect(RPostgres::Postgres(),
                 dbname = "nppes_isochrones",
                 host = "localhost",
                 port = 5432, # Default port for PostgreSQL
                 user = "postgres",
                 password = "postgres")

# First SQL command to create the table
sql_command1 <- "
CREATE TABLE \"nppes_2023\" (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    \"NPI\" integer,
    \"Last Name\" text,
    \"First Name\" text,
    \"Middle Name\" text,
    \"Gender\" text,
    \"City\" text,
    \"State\" text,
    \"Zip Code\" text,
    \"Credential\" text,
    \"Primary Speciality\" text,
    \"Secondary Specialty\" text
);
"

# Second SQL command to create the unique index
sql_command2 <- "
CREATE UNIQUE INDEX \"nppes_2023_pkey\" ON \"nppes_2023\"(id int4_ops);
"

# Execute the first SQL command
dbExecute(con, sql_command1)

# Execute the second SQL command
dbExecute(con, sql_command2)

# Reed in using R

# Define the command to be executed
# Replace placeholders with your actual file path, table name, and PostgreSQL connection details
psql_command <- sprintf("psql -d %s -U %s -c \"\\copy %s FROM '%s' WITH CSV HEADER\"",
                         "nppes_isochrones", "postgres", "nppes_2023", "/Users/tylermuffly/Documents/nppes_historical/npidata_pfile_20050523-20231112.csv")

# Execute the command from R
# This will prompt for the database user's password
system(psql_command)
```
I am trying to figure out why both Mastroyannis physicians are NOT in the Postico database but are in the NPI registry.  Because the Postico database was Physician Compare and NOT NPPES.  Mastroyannis senior may not take Medicare patients so he would not be in the Physician Compare data.  

![Screenshot 2024-04-07 at 1 31 14 PM](https://github.com/mufflyt/isochrones/assets/44621942/72e51596-28af-4303-97c8-50d8738aaa9e)


# tyler package
[tyler_1.1.0.tar.gz](https://github.com/mufflyt/isochrones/files/13914538/tyler_1.1.0.tar.gz)

# Entire Repository
https://drive.google.com/drive/folders/1l0VURFtAXZwmzSjz1R4OCQ8Tvl6FShZ8?usp=sharing. Here is the repository uploaded on 1/15/2024.

# Exploratory.io
```r
install.packages("devtools")
devtools::install_github("exploratory-io/exploratory_func")
library(exploratory)
```
Exploratory is a graphical wrapper over R.  It is impressive, and I use it for a lot of data wrangling.  

# NPPES API
`search_and_process_npi' is a function that searches the name and returns the NPI number.  I need to run this function for every year of physician data created by `postico_database_obgyns_by_year(year = 2023, db_details)` in `02.5-subspecialists_over_time`.  I need to read in the physician files `data/02.5-subspecialists_over_time/Postico_output_**year**_nppes_data_filtered.csv` apply the `search_and_process_npi' function and then save the output with a new column called "year" with the year of the data in the column.  

# Geocoding 
Thank you to Lampros for this review.  https://superface.ai/blog/geocoding-apis-comparison-1 is a great article.  

# HERE API
I pay the HERE API for geocoding and building isochrones as I feel uncomfortable given my limited knowledge base maintaining an osrm instance on Amazon or in a docker file.  I do not understand either, so this is an easy way to geocode and build isochrones.  Costs for a Basic plan:  https://www.here.com/get-started/pricing#storageandtransferrates are reasonable.  Many thanks to Merlin.

https://platform.here.com/management/usage
![Screenshot 2024-05-07 at 11 10 56 AM](https://github.com/mufflyt/isochrones/assets/44621942/4b30a85e-0dba-46a9-9902-48d02904cc39)


## HERE Geocoding and Search
HERE Geocoding and Search costs $0.83 per 1,000 searches after 30,000 free geocodes per month.  Each physician for each year will need to be geocoded.  

We need to find a way to geocode the physician address NPI data created by the `02.5-subspecialists_over_time` function in `data/02.5-subspecialists_over_time`.  Each file is named `Postico_output_year_nppes_data_filtered.csv`.  Consider adding a unique_id variable.   Here is a use case from the author of the hereR package that we implemented for the isoline function: 

```r
# Add IDs (or use row.names if they are the default sequence)
poi$id <- seq_len(nrow(poi))

# Request isolines, without aggregating
iso = isoline(poi, aggregate = FALSE)

# non-spatial join
(iso_attr <- st_sf(merge(as.data.frame(poi), iso, by = "id", all = TRUE)))
```

HERE Geocoding and Search costs $0.83 per 1,000 searches after 30,000 free geocodes per month. Each physician for each year will need to be geocoded. The data for each year (2013-2023) of physicians is located in "02.5-subspecialists_over_time". I would like to keep costs down as I am funding this project out of my own pocket. Could we only geocode unique addresses? 

## HERE Isoline 
The HERE Isoline Routing costs $5.50 per 1,000 after 2,500 free isoline routings per month.  Ioslines are the same thing as isochrones/drive time maps.  Isolines are more expensive in dollars, and we need about four isolines per physician address (30-minute isoline, 60-minute isoline, 120-minute isoline, and 180-minute isoline).  The `process_and_save_isochrones` function takes an argument for the date and I would like to use the same third Monday in November. I like using the HERE API because it allows us to set different dates and traffic for those dates.  So we can do year-matched isochrones. Dates:
```r
c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
  "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
  "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
  "2022-10-21 09:00:00", "2023-10-20 09:00:00")
```
We run the `process_and_save_isochrones` function in chunks of 25 because we were losing the entire searched isochrones when one error hit. We also test for isochrone errors by doing a one-second isochrone first: `test_and_process_isochrones` function.  #TODO:  I need help figuring out how to merge all the chunks of 25 into one file for an entire year of isochrones.  

Output:  I would like the output sf file to be placed in a directory named by the year and then matched back to the original input.  

# Very helpful e-mail from Dr. Unterfinger
The issue mentioned about an ID in the hereR response has has been raised by others:
https://github.com/munterfi/hereR/issues/153

One potential solution I had considered is the option to specify an ID column in the request to hereR. This would tell the package to use this ID column from the input data.frame and append these IDs to the response. However, there are a few issues with this approach. Firstly, it results in the duplication of information. Secondly, the package would need to check the suitability of the column for use as an ID (e.g. duplicate IDs), which I think should not be the responsibility of the package.

The hereR package sends the requests asynchronously to the Here API and considers the rate limits in the case of a freemium plan. Although the responses from the API will not be received in the same order, the package guarantees that the responses are returned in the same order as the the rows in the input sf data.frame (or sfc column). Therefore, joining the output with the input based on the length should be straightforward and error-free, as long as the input data isn't altered between the request and the response from the package:

# Add IDs (or use row.names if they are the default sequence)
poi$id <- seq_len(nrow(poi))

# Request isolines, without aggregating
iso = isoline(poi, aggregate = FALSE)
#> Sending 8 request(s) with 1 RPS to: 'https://isoline.router.hereapi.com/v8/isolines?...'
#> Received 8 response(s) with total size: 82.9 Kb

# non-spatial join
(iso_attr <- st_sf(merge(as.data.frame(poi), iso,  by = "id", all = TRUE)))

The request for the 5000 physicians will take some time, especially if you are using a freemium account (the rate limit there is 1 request per second). It might help to send the queries in smaller batches, e.g. 100 rows per request. This does not make it any faster, but the intermediate results can be joined and saved and would not be lost in the event of a network interruption or other errors. Try something like this:

library(hereR)
library(sf)
library(dplyr)
set_verbose(TRUE)


process_batch <- function(batch, start_idx) {
  iso <- isoline(batch, aggregate = FALSE)
  iso$id <- iso$id + start_idx
  iso_attr <- st_sf(merge(as.data.frame(batch) |> select(-geometry), iso, by = "id", all = TRUE))
  return(iso_attr)
}

batch_start <- 1 # in case of error, set last successful batch nr
batch_size <- 2
poi$id <- seq_len(nrow(poi))
batches <- split(poi, ceiling(seq_len(nrow(poi)) / batch_size))

# initialize an empty sf data frame for the results
all_results <- st_sf(geometry = st_sfc(crs = st_crs(4326)), stringsAsFactors = FALSE)

# process batches
for (batch_count in seq(1, length(batches))) {
  message("Processing batch ", batch_count)
  result <- process_batch(batches[[batch_count]], (batch_count - 1) * batch_size)
  all_results <- rbind(all_results, result)

  # save intermediate results to a GeoPackage
  st_write(result, "iso.gpkg", append = TRUE)

  batch_count <- batch_count + 1
  Sys.sleep(2) # avoid hitting rate limit between batches
}

# final results
print(all_results)


Best,
Merlin



# Retirement
* ACOG Committee Opinion No. 739: The Late-Career Obstetrician-Gynecologist
* NPPES listing seems to continue forever even when they are retired (Chris Carey listed in 2022.  Sue Davidson listed in 2022.  Karlotta Davis listed in 2022.)
* Maybe Physician Compare because if they do not bill for Medicare patients they would not be in there?  There is what I think is an early version of Physician Compare at NBER but it does not have the year for each row.
* See python code.  
* www.docinfo.org through FSMB search only tells you if they have a license or not
* Future idea:  Opioid prescription as a marker for surgeons who are actively practicing.  Malpractice insurance coverage????
  * Medicare PArt D Prescribing data: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider
The 'provider' package (SWEET 📦 ) has:
```r
prescribers(year = 2021,
            type = 'Provider',
            npi = 1003000423)
```


### 01-setup
[Setup file](https://youtu.be/4eStfNf9qjk)

### 02-search_taxonomy
[Setup file](https://youtu.be/4eStfNf9qjk)

### 02.5-subspecialists_over_time
[subspecialists over time](https://youtu.be/-WfEeK8gpH8)

### 03-search_and_process_npi
[search_and_process_npi](https://youtu.be/AZVw-7y01BE)

### 04-geocode
[04-geocode](https://youtu.be/S2vXh8-fzKs)

### 05-geocode-cleaning
[05-geocode-cleaning](https://youtu.be/CwUaD5tRWo4)

### 06-isochrones
[isochrones](https://youtu.be/crZuELWtrTA)

### 07-isochrone-mapping
[07-isochrone-mapping](https://youtu.be/Hs2Nab7Imoc)

### 07.5-prep-get-block-group-overlap
[07.5-prep-get-block-group-overlap]()

### 08-get-block-group-overlap

### 08.5-prep-the census-variables

### 09-get-census-population

### 10-calculate-polygon-demographcs

# Geography

#TODO: 
1) 01-setup: Setup will need to be updated with any changes you make to the bespoke functions.  Teh get_census_data function is not working.  
2) 02.5-subspecialists_over_time: Year-specific physicians should be contained in "data/02.5-subspecialists_over_time/distinct_year_by_year_nppes_data_validated_npi.csv".  This file was run on a different machine with a Postico database so the file does not need to be run.  
3) 03-search_and_process_npi.R should not need to be run.  
4) 04-geocode: We need to geocode all the physicians from "distinct_year_by_year_nppes_data_validated_npi.csv."  I would like to avoid doing too much geocoding as using the hereR package can get expensive, so I would prefer we only code distinct addresses.  There is an error at this line of code that needs help: "merged_data_sp <- as(merged_data, "Spatial")".  
5) 05-geocode-cleaning: This file is a very hacky workaround to parse then match the address parts, and now that we have a unique identifier going in and coming out of the 06-isochrones.R file.  I get an error in the sanity check section with I get an error:  Error in leaflet::addLegend(., position = "bottomright", colors = district_colors,  :  'colors' and 'labels' must be of the same length
 In addition: Warning message: Unknown or uninitialised column: `ACOG_District`. 

5) 05-geocode-cleaning: Running this file may not be necessary.
6) 06-isochrones: Please fix the process_and_save_isochrones function to give it multiple dates (`iso_datetime_yearly`).  
7) 07-isochrone-mapping: To make year-specific physicians, we must figure out the "subspecialists_lat_long" variable.  
7) 07.5-prep-get-block-group-overlap: This contains the year-specific block groups and should work well as it is.  
8) 08-get-block-group-overlap: We need to look at the entire USA.  Currently, the code reads "Colorado" as a smaller state for the toy example.
8) 08.5-prep-the census-variables: Please download all races ("BLACK", "American Indian/Alaska Native (AI/AN)", "ASIAN", "NHPI" = "Native Hawaiian/Pacific Islander", "WHITE") of the total female population per block group for all years.  This is going to need some work.  We will need to find the American Community Survey (ACS) variables associated with the races.  
# SMBA 
To run this program, you must install PyCharm, Python, Scrapy framework, and API-Credits in your system. And how to use all these, you must have to take knowledge of all these.  I downloaded the data in 5/2024.  The nice thing is that the data says if the physician is retired or not and the year of most recent certification.  Never got this working.  

![Screenshot 2024-08-18 at 1 53 28 PM](https://github.com/user-attachments/assets/fa875d1b-6618-449b-8672-37b652ffda46)


# This code is designed to perform several data management tasks on healthcare-related data, specifically concerning physicians' affiliations and prescriptions. The script begins by establishing a default working directory and checking the existence of required data directories and files, such as physician facility affiliations and Medicare Part D prescribers. It loads these data files using the `data.table` package, which is optimized for large data sets and offers fast data manipulation capabilities.

# After loading the data, the script cleans and processes it by standardizing NPI (National Provider Identifier) column names across different datasets to enable merging. It also adjusts year formats and filters out records for years beyond a specified threshold to focus on relevant data. The key part of the data processing involves merging the facility affiliation data with Medicare Part D prescription data based on the NPI, prioritizing the prescription data when both sources provide overlapping information.
# 
# Subsequent steps involve handling missing values and deciding the retirement year of a physician based on the last year they prescribed medication or had a hospital affiliation. The final processed data, which integrates physician retirement years with their NPIs, is then merged back with additional physician data from an updated processed file and written out to a new CSV file for further use.
# 
# Throughout the script, various checks are implemented to ensure the integrity of the data, such as confirming the existence of files and directories, checking for missing values, and validating the range and completeness of the year data. This approach helps maintain the quality and reliability of the data processing workflow.

# Required Input Files for Script Execution ----

# 1. Facility Affiliation Files:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "end_facility_affiliation_last_consecutive_year.csv"

# 2. Medicare Part D Prescribers Files:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "end_Medicare_part_D_prescribers_last_consecutive_year.csv"

# 3. Updated Processed NIPs:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "updated_processed_nips.csv"

# 4. NPI Deactivation File:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "retirement_NPPES Deactivated NPI Report 20240408.csv"

# 5. End SP DuckDB NPI All:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "end_sp_duckdb_npi_all.csv"

# 6. GOBA File:
#    - Directory: "/Volumes/Video Projects Muffly 1/retirement/"
#      - "goba_unique_goba_deceased_retired.csv"


# Setup ----
source("R/01-setup.R")

# default directory
def_dir = here::here()
def_dir

## Check that the Files and directories exist ----
data_dir = ("/Volumes/Video Projects Muffly 1/retirement/")
if (!dir.exists(data_dir)) stop(glue::glue("The directory '{data_dir}' does not exist!"))

# Defining Data Directories and File Paths ----
# Facility Affiliation:  https://data.cms.gov/provider-data/dataset/27ea-46a8.  When was the last time that a physician was facility with a hopsital?  "end_facility_affiliation_last_consecutive_year.csv" is the last year the physician had a documented affiliation with a hospital. It is a list of many physicians, not just obgyns, and the last year they had a hospital affiliation.
fac_aff_pth = file.path(data_dir, 'end_facility_affiliation_last_consecutive_year.csv')
if (!file.exists(fac_aff_pth)) stop(glue::glue("The file '{fac_aff_pth}' does not exist!"))

# Medicare Part D Prescribers: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider.  When was the last time that provider prescribed to a patient over 65 years old?  "end_Medicare_part_D_prescribers_last_consecutive_year.csv" is a dataframe of dates when the physician prescribed medication to a patient over 65. The final year is 2021.
med_presc_pth = file.path(data_dir, 'end_Medicare_part_D_prescribers_last_consecutive_year.csv')
if (!file.exists(med_presc_pth)) stop(glue::glue("The file '{med_presc_pth}' does not exist!"))

# NBER Data?  - I think thais has column named "last_consecutive_year"
upd_proc_nips_pth = file.path(data_dir, 'updated_processed_nips.csv')
if (!file.exists(upd_proc_nips_pth)) stop(glue::glue("The file '{upd_proc_nips_pth}' does not exist!"))

# NPI Deactivation file: https://download.cms.gov/nppes/NPPESDeactivatedNPIReport040824.zip
npi_deactivation = file.path(data_dir, 'retirement_NPPES Deactivated NPI Report 20240408.csv')
if (!file.exists(npi_deactivation)) stop(glue::glue("The file '{npi_deactivation}' does not exist!"))

# Loading Data ----
threads = 4L
fac_aff_dat = data.table::fread(file = fac_aff_pth, header = T, stringsAsFactors = F)
med_presc_dat = data.table::fread(file = med_presc_pth, header = T, stringsAsFactors = F)
upd_proc_nips_dat = data.table::fread(file = upd_proc_nips_pth, header = T, stringsAsFactors = F, nThread = threads)
npi_deactivation = data.table::fread(file = npi_deactivation, header = T, stringsAsFactors = F, nThread = threads)

# preference will be given to the "end_Medicare_part_D_prescribers_last_consecutive_year.csv" over the "end_facility_affiliation_last_consecutive_year.csv"
# first we'll merge these 2 files by the "npi" column

# Data Cleaning and Processing ----
# rename the NPI column to match the lower case 'npi' of the 'updated_processed_nips.csv' file
colnames(fac_aff_dat)[which(colnames(fac_aff_dat)== 'NPI')] = 'npi'
colnames(med_presc_dat)[which(colnames(med_presc_dat)== 'PRSCRBR_NPI')] = 'npi'

# from the 2-digit "last_consecutive_year" column we'll remove years greater than 2024
idx_keep = which(fac_aff_dat$last_consecutive_year <= 24)
if (length(idx_keep) == 0) stop("There are no observations before year 2024!")
fac_aff_dat = fac_aff_dat[idx_keep, , drop = F]; table(fac_aff_dat$last_consecutive_year)

# we'll add the 2 digits '20' infront of the 2-digits number to form a year
fac_aff_dat$last_consecutive_year = glue::glue("20{fac_aff_dat$last_consecutive_year}") |>
  as.integer(); table(fac_aff_dat$last_consecutive_year)

# we'll merge the 2 .csv (prescription) files by 'npi'
merg_presc = merge(x = fac_aff_dat, y = med_presc_dat, by = 'npi', all = TRUE)
# colSums(is.na(merg_presc))
# str(merg_presc)

cols_keep = c("last_consecutive_year", "last_consecutive_year_Medicare_part_D_prescribers")
merg_presc$na_row_sums = as.vector(rowSums(is.na(merg_presc[, ..cols_keep])))

# verify that we don't have 2 missing values in the required columns but only 0 or 1
if (!all(unique(merg_presc$na_row_sums) %in% c(0,1))) stop("We expect either 0 or 1 NA values but not 2!")

# vectorization is way faster compared to lapply or a parallel for-loop
presc_ret_year = ifelse(merg_presc$na_row_sums == 0, merg_presc$last_consecutive_year_Medicare_part_D_prescribers, 
                        ifelse(is.na(merg_presc$last_consecutive_year_Medicare_part_D_prescribers), merg_presc$last_consecutive_year, merg_presc$last_consecutive_year_Medicare_part_D_prescribers))
# str(presc_ret_year)

# include the column to the data
merg_presc$prescription_retirement_year = presc_ret_year

# verify visually using a sample of rows
set.seed(2)
samp = sample(1:nrow(merg_presc), 60)
merg_presc[samp, , drop = F]

# Data Merging and Writing ----
merg_presc_keep = merg_presc[, c('npi', 'prescription_retirement_year')]
if (!all(colSums(is.na(merg_presc_keep)) == 0)) stop("We don't expect any missing values!")

# keep the order of the column names once merged
cols_upd = c(colnames(upd_proc_nips_dat), 'prescription_retirement_year')

# merge the 'updated_processed_nips.csv' file with the newly created dataset of the prescription retirement year columns
upd_proc_nips_dat_presc = merge(x = upd_proc_nips_dat, y = merg_presc_keep, by = 'npi', all.x = T)
# colSums(is.na(upd_proc_nips_dat_presc))

# re-order columns
upd_proc_nips_dat_presc = upd_proc_nips_dat_presc[, ..cols_upd]
# upd_proc_nips_dat_presc

# write the new data.frame to a file
data.table::fwrite(x = upd_proc_nips_dat_presc, file = file.path(data_dir, 'output/processed_nips_prescription.csv'), row.names = F)


#Okay, I am working on how to find when a physician retires or dies and translate that into the year they stopped working. I think we have three sources for this. 

#First, we have the NPI deactivation table: https://download.cms.gov/nppes/NPPESDeactivatedNPIReport040824.zip. The file is attached. This matches NPI to NPI; a date is given for when the NPI number was deactivated. 

#The second approach is when the patient does not appear in the next year's NPI database. I downloaded the NPI database every year, so it is a duplicate database instead of the deduplicated one from NBER. The file is called end_sp_duckdb_npi_all.csv and is attached. 

#The third way is a goba file (goba_unique_goba_deceased_retired.csv) where there is a "certStatus" %in% c("Deceased Diplomate", "Retired"). This requires a first name and last name left join and does not include the year that the physician died or retired but knowing the status would be helpful.

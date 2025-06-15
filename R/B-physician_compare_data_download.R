#######################
source("R/01-setup.R")
#######################

# https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project
# You have to download this file manually.  


###
# Load libraries
library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(fs)

# Define base directory
base_dir <- "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/"

# Get all year folders
year_folders <- fs::dir_ls(base_dir, type = "directory")

# Pattern for filtering specialties
pattern <- "(?i)OBSTETR|GYNECOLOGICAL ONCOLOGY"

# Function to read, filter, and write CSV
filter_spec_file <- function(folder_path) {
  logger::log_info("Processing folder: {folder_path}")
  
  csv_files <- fs::dir_ls(folder_path, glob = "*.csv")
  
  purrr::walk(csv_files, function(file_path) {
    logger::log_info("Reading file: {file_path}")
    
    filtered <- readr::read_csv(file_path, show_col_types = FALSE) %>%
      dplyr::filter(
        dplyr::if_any(
          dplyr::contains("_spec"),
          ~ stringr::str_detect(.x, pattern)
        )
      )
    
    out_path <- stringr::str_replace(file_path, "\\.csv$", "_filtered.csv")
    logger::log_info("Writing filtered file to: {out_path}")
    readr::write_csv(filtered, out_path)
  })
}

# Apply to each year folder
purrr::walk(year_folders, filter_spec_file)

beepr::beep(2)

###
# Load libraries
library(readr)
library(dplyr)
library(stringr)
library(fs)
library(logger)
library(purrr)
library(scales)
library(beepr)

# Define base directory
base_dir <- "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/"

# Find all *_filtered.csv files recursively (get path + size)
filtered_files_info <- fs::dir_info(
  path = base_dir,
  recurse = TRUE,
  regexp = "_filtered\\.csv$"
) %>%
  dplyr::mutate(
    year = stringr::str_extract(path, "(?<=unzipped_files/)[0-9]{4}"),
    file_size_MB = round(size / 1024^2, 2)
  )

# Pick the largest file by year
largest_per_year <- filtered_files_info %>%
  dplyr::group_by(year) %>%
  dplyr::slice_max(order_by = file_size_MB, n = 1, with_ties = FALSE) %>%
  dplyr::ungroup()

# Log and print selected files
logger::log_info("Selected {nrow(largest_per_year)} largest _filtered.csv files (one per year):")
print(largest_per_year %>% dplyr::select(year, path, file_size_MB), n = Inf)

# Read and combine all largest _filtered.csv files
combined_physician_compare <- largest_per_year$path %>%
  purrr::map_dfr(~ {
    logger::log_info("Reading file: {.x}")
    readr::read_csv(.x, col_types = readr::cols(.default = "c"))
  })


# Log total rows and columns in merged dataset
logger::log_info(
  "Merged dataset has {scales::comma(nrow(combined_physician_compare))} rows and {ncol(combined_physician_compare)} columns."
)

# Save merged file with timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
output_path <- stringr::str_glue(
  "/Volumes/Video Projects Muffly 1/physician_compare/merged_filtered_physician_compare_{timestamp}.csv"
)

readr::write_csv(combined_physician_compare, output_path)
logger::log_info("Saved merged file to: {output_path}")

beepr::beep(2)


#############################
## FINAL
#############################

#' Merge Largest Physician Compare Filtered Files by Year
#'
#' @description
#' Identifies and merges the largest filtered CSV file from each year in the 
#' Physician Compare dataset. The function processes files with the '_filtered.csv'
#' suffix, identifies the largest file per year, and combines them into a single
#' CSV output file with a timestamp. The year_source column is set to the year of 
#' the directory where the data file was found. Column names are standardized
#' across different years for consistency and all unique columns are preserved.
#'
#' @param base_dir Character string specifying the base directory containing
#'        year-organized subdirectories with filtered CSV files.
#'        Default: "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/"
#' @param output_dir Character string specifying the directory for storing
#'        intermediate chunk files.
#'        Default: "/Volumes/Video Projects Muffly 1/physician_compare/merged_chunks/"
#' @param verbose Logical indicating whether to print additional status messages
#'        beyond logging. Default: TRUE
#' @param deduplicate Logical indicating whether to remove duplicate physician 
#'        entries that differ only in address formatting. Default: FALSE
#' @param preserve_all_columns Logical indicating whether to preserve all unique
#'        columns from all files in the final output. Default: TRUE
#'
#' @return Character string containing the path to the merged output file
#'
#' @importFrom readr read_csv write_csv cols
#' @importFrom dplyr mutate select group_by slice_max ungroup filter distinct rename_with bind_rows
#' @importFrom stringr str_extract str_glue str_detect
#' @importFrom fs dir_create dir_info path dir_ls
#' @importFrom logger log_info log_warn log_error
#' @importFrom purrr map_chr map reduce
#' @importFrom assertthat assert_that
#'
#' @examples
#' \dontrun{
#' # Example 1: Basic usage with default parameters
#' merged_file <- merge_largest_physician_compare_filtered_files(
#'   base_dir = "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/",
#'   output_dir = "/Volumes/Video Projects Muffly 1/physician_compare/merged_chunks/",
#'   verbose = TRUE,
#'   deduplicate = FALSE,
#'   preserve_all_columns = TRUE
#' )
#' # Returns: "/Volumes/Video Projects Muffly 1/physician_compare/
#' #           merged_filtered_physician_compare_20250503_1030.csv"
#'
#' # Example 2: Specifying custom directories with deduplication
#' merged_file <- merge_largest_physician_compare_filtered_files(
#'   base_dir = "~/data/physician_compare/raw/",
#'   output_dir = "~/data/physician_compare/temp/",
#'   verbose = FALSE,
#'   deduplicate = TRUE,
#'   preserve_all_columns = TRUE
#' )
#' # Returns: "/Volumes/Video Projects Muffly 1/physician_compare/
#' #           merged_filtered_physician_compare_20250503_1045.csv"
#'
#' # Example 3: Using custom paths with no deduplication and standard columns only
#' merged_file <- merge_largest_physician_compare_filtered_files(
#'   base_dir = "/data/projects/healthcare/physician_compare/source/",
#'   output_dir = "/data/projects/healthcare/physician_compare/intermediate/",
#'   verbose = TRUE,
#'   deduplicate = FALSE,
#'   preserve_all_columns = FALSE
#' )
#' # Returns: "/Volumes/Video Projects Muffly 1/physician_compare/
#' #           merged_filtered_physician_compare_20250503_1100.csv"
#' }
merge_largest_physician_compare_filtered_files <- function(
    base_dir = "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/",
    output_dir = "/Volumes/Video Projects Muffly 1/physician_compare/merged_chunks/",
    verbose = TRUE,
    deduplicate = FALSE,
    preserve_all_columns = TRUE
) {
  # Validate inputs
  assertthat::assert_that(is.character(base_dir))
  assertthat::assert_that(is.character(output_dir))
  assertthat::assert_that(is.logical(verbose))
  assertthat::assert_that(is.logical(deduplicate))
  assertthat::assert_that(is.logical(preserve_all_columns))
  
  # Configure logger
  logger::log_info("Starting merge process for largest _filtered.csv files")
  logger::log_info("Base directory: {base_dir}")
  logger::log_info("Output directory: {output_dir}")
  logger::log_info("Deduplication enabled: {deduplicate}")
  logger::log_info("Preserve all columns: {preserve_all_columns}")
  
  # Create directory for intermediate chunks
  ensure_directory_exists(output_dir)
  
  # Find and analyze filtered files
  filtered_file_metadata <- find_filtered_files(base_dir)
  
  if (nrow(filtered_file_metadata) == 0) {
    logger::log_error("No _filtered.csv files found in {base_dir}")
    stop("No _filtered.csv files found in the specified directory.")
  }
  
  # Select largest file per year
  largest_yearly_files <- identify_largest_files_per_year(filtered_file_metadata, output_dir)
  
  if (verbose) {
    logger::log_info("Identified {nrow(largest_yearly_files)} largest _filtered.csv files to merge")
    print(largest_yearly_files %>% dplyr::select(year, path, file_size_MB), n = Inf)
  }
  
  # Process each file to create year-specific chunks
  process_yearly_chunks(largest_yearly_files, output_dir, deduplicate)
  
  # Combine chunks into final output
  final_output_path <- merge_chunks_to_final_output(output_dir, preserve_all_columns)
  
  logger::log_info("Merge process completed successfully")
  logger::log_info("Final merged file saved to: {final_output_path}")
  
  return(final_output_path)
}

#' @noRd
ensure_directory_exists <- function(directory_path) {
  logger::log_info("Ensuring directory exists: {directory_path}")
  fs::dir_create(directory_path)
}

#' @noRd
find_filtered_files <- function(base_directory) {
  logger::log_info("Searching for _filtered.csv files in: {base_directory}")
  
  filtered_files_info <- fs::dir_info(
    path = base_directory,
    recurse = TRUE,
    regexp = "_filtered\\.csv$"
  )
  
  if (nrow(filtered_files_info) == 0) {
    logger::log_warn("No files matching '_filtered.csv' found in {base_directory}")
    return(filtered_files_info)
  }
  
  # Extract year from directory path - apply function row by row
  filtered_files_info <- filtered_files_info %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      year = extract_year_from_path(path, base_directory),
      file_size_MB = round(size / 1024^2, 2)
    ) %>%
    dplyr::ungroup()
  
  logger::log_info("Found {nrow(filtered_files_info)} _filtered.csv files")
  
  # Check if any years couldn't be extracted
  missing_years <- sum(is.na(filtered_files_info$year))
  if (missing_years > 0) {
    logger::log_warn("Could not extract year from {missing_years} file paths")
  }
  
  return(filtered_files_info)
}

#' @noRd
extract_year_from_path <- function(file_path, base_dir) {
  # This function now expects to be called on a single path at a time
  # Normalize paths to ensure consistent handling
  base_dir <- normalizePath(base_dir, mustWork = FALSE, winslash = "/")
  file_path <- normalizePath(file_path, mustWork = FALSE, winslash = "/")
  
  # Extract the relative path from base_dir
  rel_path <- gsub(paste0("^", gsub("([.|()\\^{}+$*?])", "\\\\\\1", base_dir)), "", file_path)
  
  # First try to get the year directly after base_dir (standard pattern)
  year <- stringr::str_extract(rel_path, "^/([0-9]{4})")
  if (!is.na(year) && length(year) == 1) {
    year <- stringr::str_extract(year, "[0-9]{4}")
    return(year)
  }
  
  # If that fails, look for any 4-digit sequence in the path that could be a year
  all_years <- stringr::str_extract_all(rel_path, "(?<=/)[0-9]{4}(?=/)")[[1]]
  if (length(all_years) > 0) {
    return(all_years[1])  # Take the first year found
  }
  
  # Final fallback: any 4-digit sequence that could be a year (2000-2030)
  potential_years <- stringr::str_extract_all(
    rel_path, 
    "(?<=/|^)(20[0-2][0-9])(?=/|$)"
  )[[1]]
  
  if (length(potential_years) > 0) {
    return(potential_years[1])
  }
  
  logger::log_warn("Could not extract year from path: {file_path}")
  return(NA_character_)
}

#' @noRd
identify_largest_files_per_year <- function(file_metadata, output_dir) {
  logger::log_info("Identifying largest file per year")
  
  # Check if there are files with valid years
  if (sum(!is.na(file_metadata$year)) == 0) {
    logger::log_error("No files with valid year information found")
    stop("Cannot identify largest files - no valid year information available")
  }
  
  largest_per_year <- file_metadata %>%
    dplyr::filter(!is.na(year)) %>%
    dplyr::group_by(year) %>%
    dplyr::slice_max(order_by = file_size_MB, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(temp_output = fs::path(output_dir, paste0("chunk_", year, ".csv")))
  
  logger::log_info("Identified {nrow(largest_per_year)} largest files (one per year)")
  
  return(largest_per_year)
}

#' @noRd
process_yearly_chunks <- function(file_metadata, output_directory, should_deduplicate) {
  logger::log_info("Processing {nrow(file_metadata)} files to create year-specific chunks")
  
  # Get column mapping for standardization
  column_mapping <- get_column_mapping()
  
  for (i in seq_len(nrow(file_metadata))) {
    file_path <- file_metadata$path[i]
    out_path <- file_metadata$temp_output[i]
    year_value <- as.character(file_metadata$year[i])
    
    logger::log_info("Processing file {i}/{nrow(file_metadata)}: {file_path}")
    logger::log_info("Year source: {year_value}, Output: {out_path}")
    
    tryCatch({
      # Read the data
      yearly_data <- readr::read_csv(
        file_path, 
        col_types = readr::cols(.default = "c"),
        show_col_types = FALSE
      ) 
      
      logger::log_info("Read {nrow(yearly_data)} rows from {file_path}")
      
      # Add year_source column based on the year from the directory
      yearly_data <- yearly_data %>% 
        dplyr::mutate(year_source = year_value)
      
      logger::log_info("Added year_source column: {year_value} based on directory year")
      
      # Extract and save original column names for later reference
      original_columns <- colnames(yearly_data)
      
      # Standardize column names based on year
      yearly_data <- standardize_column_names(yearly_data, year_value, column_mapping)
      
      # Add metadata about original column names
      attr(yearly_data, "original_columns") <- original_columns
      attr(yearly_data, "year") <- year_value
      
      # Deduplicate if requested
      if (should_deduplicate) {
        initial_rows <- nrow(yearly_data)
        
        # Create a robust deduplication method that handles missing columns
        yearly_data <- deduplicate_physician_data(yearly_data)
        
        removed_rows <- initial_rows - nrow(yearly_data)
        logger::log_info("Deduplication removed {removed_rows} duplicate rows ({round(removed_rows/initial_rows*100, 2)}%)")
      }
      
      # Write to chunk file
      readr::write_csv(yearly_data, out_path)
      logger::log_info("Wrote {nrow(yearly_data)} rows to yearly chunk {out_path}")
      
      # Explicit cleanup
      rm(yearly_data)
      gc()
    }, 
    error = function(e) {
      logger::log_error("Failed to process file {file_path}: {e$message}")
      stop(paste("Error processing file:", file_path, "-", e$message))
    })
  }
}

#' @noRd
get_column_mapping <- function() {
  # Create mapping of common column variations to standardized names
  column_mapping <- list(
    # Identifiers
    "NPI" = c("NPI", "Provider_NPI", "Provider NPI", "npi", "ProviderNPI"),
    "Ind_PAC_ID" = c("Ind_PAC_ID", "PAC_ID", "Provider_PAC_ID", "Provider PAC ID", "ProviderPACID"),
    "Ind_enrl_ID" = c("Ind_enrl_ID", "enrl_id", "Enrollment_ID", "Provider_Enrollment_ID", "Provider Enrollment ID", "EnrollmentID"),
    
    # Name fields
    "lst_nm" = c("lst_nm", "Last_Name", "LastName", "Provider Last Name", "LAST_NAME", "Provider_Last_Name", "Last"),
    "frst_nm" = c("frst_nm", "First_Name", "FirstName", "Provider First Name", "FIRST_NAME", "Provider_First_Name", "First"),
    "mid_nm" = c("mid_nm", "Middle_Name", "MiddleName", "Provider Middle Name", "MIDDLE_NAME", "Provider_Middle_Name", "Middle"),
    "suff" = c("suff", "Suffix", "name_suffix", "Provider Suffix", "Provider_Suffix", "NameSuffix"),
    "gndr" = c("gndr", "Gender", "gender", "Provider Gender", "Provider_Gender", "ProviderGender"),
    
    # Enhanced credential mapping
    "Cred" = c("Cred", "Credential", "credential", "Provider Credential", "Provider_Credential", 
               "Credentials", "credentials", "Professional_Credentials", "Prof_Credentials", 
               "Provider_Credentials", "ProviderCredential", "MD_DO", "MD/DO", "Degree", 
               "Medical_Degree", "Professional_Suffix", "Degree_Type", "DegreeType", 
               "Provider_Title", "Title", "QualType", "Qualification_Type", "ProfessionalTitle",
               "ProfTitle", "Provider_Designation", "MedicalTitle", "PostNominals", "CredentialText"),
    
    # Education and specialty
    "Med_sch" = c("Med_sch", "Med_School", "Medical_School", "Medical School", "Provider Medical School", 
                  "Provider_Medical_School", "GraduateMedicalEducation", "MedicalEducation", "GraduationSchool"),
    "Grd_yr" = c("Grd_yr", "Graduation_Year", "Graduate_Year", "Medical School Graduation Year", 
                 "Provider_Graduate_Year", "GradYear", "YearOfGraduation", "MedicalEducationYear"),
    "pri_spec" = c("pri_spec", "Primary_Specialty", "PrimarySpecialty", "Primary Specialty", 
                   "Provider_Primary_Specialty", "MainSpecialty", "Specialty", "ProviderSpecialty"),
    "sec_spec_1" = c("sec_spec_1", "Secondary_Specialty_1", "Secondary Specialty 1", 
                     "Provider_Secondary_Specialty_1", "SecondarySpecialty1", "OtherSpecialty1"),
    "sec_spec_2" = c("sec_spec_2", "Secondary_Specialty_2", "Secondary Specialty 2", 
                     "Provider_Secondary_Specialty_2", "SecondarySpecialty2", "OtherSpecialty2"),
    "sec_spec_3" = c("sec_spec_3", "Secondary_Specialty_3", "Secondary Specialty 3", 
                     "Provider_Secondary_Specialty_3", "SecondarySpecialty3", "OtherSpecialty3"),
    "sec_spec_4" = c("sec_spec_4", "Secondary_Specialty_4", "Secondary Specialty 4", 
                     "Provider_Secondary_Specialty_4", "SecondarySpecialty4", "OtherSpecialty4"),
    "sec_spec_all" = c("sec_spec_all", "Secondary_Specialties", "Secondary Specialties", 
                       "All_Secondary_Specialties", "AllSecondarySpecialties", "OtherSpecialties"),
    
    # Organization info
    "org_nm" = c("org_nm", "Organization_Name", "Facility Name", "Practice_Name", "FacilityName", 
                 "Organization Name", "GroupName", "PracticeName", "BusinessName"),
    "org_pac_id" = c("org_pac_id", "Organization_PAC_ID", "Org_PAC_ID", "Facility PAC ID", 
                     "Practice_PAC_ID", "GroupPACID", "FacilityPACID", "OrganizationPACID"),
    "num_org_mem" = c("num_org_mem", "Number_Organization_Members", "Number of Members", 
                      "Practice_Member_Count", "GroupSize", "OrganizationSize", "MemberCount"),
    
    # Address and contact
    "adr_ln_1" = c("adr_ln_1", "Address_Line_1", "Street Address 1", "Practice_Address_1", 
                   "Address Line 1", "AddressLine1", "StreetAddr1", "FirstLineAddress"),
    "adr_ln_2" = c("adr_ln_2", "Address_Line_2", "Street Address 2", "Practice_Address_2", 
                   "Address Line 2", "AddressLine2", "StreetAddr2", "SecondLineAddress"),
    "ln_2_sprs" = c("ln_2_sprs", "Line_2_Suppressed", "Address_Line_2_Suppressed", 
                    "Street Address 2 Suppressed", "Line2Suppressed", "AddrLine2Suppressed"),
    "cty" = c("cty", "City", "city", "City/Town", "Practice_City", "CityName", "Town", "Municipality"),
    "st" = c("st", "State", "state", "STATE", "Practice_State", "StateName", "StateAbbreviation", "StateCode"),
    "zip" = c("zip", "ZIP", "Zip", "ZIP Code", "Practice_Zip", "PostalCode", "ZipCode", "ZIPCode"),
    "phn_numbr" = c("phn_numbr", "Phone", "Phone_Number", "Telephone Number", "Practice_Phone", 
                    "PhoneNumber", "ContactNumber", "TelephoneNumber", "PracticePhone"),
    
    # Hospital affiliations
    "hosp_afl_1" = c("hosp_afl_1", "Hospital_Affiliation_1", "Hospital Affiliation 1", 
                     "Hospital_Affiliation_CCN_1", "HospitalAffiliation1", "HospCCN1"),
    "hosp_afl_lbn_1" = c("hosp_afl_lbn_1", "Hospital_Name_1", "Hospital Affiliation Name 1", 
                         "Hospital_LBN_1", "HospitalName1", "AffiliatedHospital1"),
    "hosp_afl_2" = c("hosp_afl_2", "Hospital_Affiliation_2", "Hospital Affiliation 2", 
                     "Hospital_Affiliation_CCN_2", "HospitalAffiliation2", "HospCCN2"),
    "hosp_afl_lbn_2" = c("hosp_afl_lbn_2", "Hospital_Name_2", "Hospital Affiliation Name 2", 
                         "Hospital_LBN_2", "HospitalName2", "AffiliatedHospital2"),
    "hosp_afl_3" = c("hosp_afl_3", "Hospital_Affiliation_3", "Hospital Affiliation 3", 
                     "Hospital_Affiliation_CCN_3", "HospitalAffiliation3", "HospCCN3"),
    "hosp_afl_lbn_3" = c("hosp_afl_lbn_3", "Hospital_Name_3", "Hospital Affiliation Name 3", 
                         "Hospital_LBN_3", "HospitalName3", "AffiliatedHospital3"),
    "hosp_afl_4" = c("hosp_afl_4", "Hospital_Affiliation_4", "Hospital Affiliation 4", 
                     "Hospital_Affiliation_CCN_4", "HospitalAffiliation4", "HospCCN4"),
    "hosp_afl_lbn_4" = c("hosp_afl_lbn_4", "Hospital_Name_4", "Hospital Affiliation Name 4", 
                         "Hospital_LBN_4", "HospitalName4", "AffiliatedHospital4"),
    "hosp_afl_5" = c("hosp_afl_5", "Hospital_Affiliation_5", "Hospital Affiliation 5", 
                     "Hospital_Affiliation_CCN_5", "HospitalAffiliation5", "HospCCN5"),
    "hosp_afl_lbn_5" = c("hosp_afl_lbn_5", "Hospital_Name_5", "Hospital Affiliation Name 5", 
                         "Hospital_LBN_5", "HospitalName5", "AffiliatedHospital5"),
    
    # Program participation fields
    "assgn" = c("assgn", "Assignment", "Medicare_Assignment", "Accepts Medicare Assignment", 
                "MedicareAssignment", "AcceptsMedicare", "AssignmentIndicator"),
    "ind_assgn" = c("ind_assgn", "Individual_Assignment", "Individual Assignment", 
                    "Individual_Medicare_Assignment", "IndiMedicareAssignment", "IndividualMedicareIndicator"),
    "grp_assgn" = c("grp_assgn", "Group_Assignment", "Group Assignment", 
                    "Group_Medicare_Assignment", "GroupMedicareAssignment", "GroupMedicareIndicator"),
    "PQRS" = c("PQRS", "PQRS_Participant", "PQRS Participant", "Participates_In_PQRS", 
               "PQRSIndicator", "PQRSParticipation", "PQRSQualityMeasures"),
    "EHR" = c("EHR", "EHR_Participant", "EHR Participant", "Uses_EHR", 
              "EHRIndicator", "ElectronicHealthRecord", "ParticipatesInEHR"),
    "MHI" = c("MHI", "MHI_Participant", "MHI Participant", "Participates_In_MHI", 
              "MHIIndicator", "MeaningfulHealthIT", "MeaningfulUse"),
    "Telehlth" = c("Telehlth", "Telehealth", "TelehealthServices", "Offers Telehealth", 
                   "Telehealth_Services", "TelehealthIndicator", "OffersVirtualVisits", "VirtualCare")
  )
  
  return(column_mapping)
}

#' @noRd
standardize_column_names <- function(data_frame, year, column_mapping) {
  logger::log_info("Standardizing column names for {year} data")
  
  # Get original column names
  original_columns <- colnames(data_frame)
  logger::log_info("Original columns: {length(original_columns)}")
  
  # Create a new data frame with standardized column names
  standardized_data <- data_frame
  
  # Rename columns based on mapping
  for (standard_name in names(column_mapping)) {
    possible_names <- column_mapping[[standard_name]]
    
    # Find if any of the possible names is in the original columns
    for (alt_name in possible_names) {
      if (alt_name %in% original_columns && alt_name != standard_name) {
        logger::log_info("Renaming {alt_name} to {standard_name}")
        standardized_data <- dplyr::rename(standardized_data, !!standard_name := alt_name)
        break
      }
    }
  }
  
  # Get standardized columns
  standardized_columns <- colnames(standardized_data)
  
  # Add any missing columns as NA
  for (standard_name in names(column_mapping)) {
    if (!standard_name %in% standardized_columns) {
      logger::log_info("Adding missing column: {standard_name}")
      standardized_data[[standard_name]] <- NA_character_
    }
  }
  
  # For 2023-2024 data, handle specific column transformations
  if (year %in% c("2023", "2024")) {
    # Special case: merge ind_assgn and grp_assgn into assgn if both exist
    if ("ind_assgn" %in% standardized_columns && "grp_assgn" %in% standardized_columns && 
        (!"assgn" %in% standardized_columns || all(is.na(standardized_data$assgn)))) {
      logger::log_info("Creating assgn column from ind_assgn and grp_assgn")
      standardized_data <- standardized_data %>%
        dplyr::mutate(assgn = dplyr::case_when(
          ind_assgn == "Y" | grp_assgn == "Y" ~ "Y",
          ind_assgn == "N" & grp_assgn == "N" ~ "N",
          TRUE ~ NA_character_
        ))
    }
  }
  
  # Log the standardization results
  added_columns <- setdiff(colnames(standardized_data), original_columns)
  if (length(added_columns) > 0) {
    logger::log_info("Added {length(added_columns)} new columns: {paste(added_columns, collapse=', ')}")
  }
  
  renamed_columns <- intersect(names(column_mapping), colnames(standardized_data))
  renamed_columns <- renamed_columns[!renamed_columns %in% original_columns]
  if (length(renamed_columns) > 0) {
    logger::log_info("Renamed {length(renamed_columns)} columns: {paste(renamed_columns, collapse=', ')}")
  }
  
  return(standardized_data)
}

#' @noRd
deduplicate_physician_data <- function(physician_data) {
  # Get available columns for deduplication
  available_columns <- colnames(physician_data)
  logger::log_info("Available columns for deduplication: {length(available_columns)}")
  
  # Define possible identifier columns with fallbacks
  id_columns <- list(
    npi = c("NPI", "npi", "Npi", "Provider_NPI"),
    pac_id = c("Ind_PAC_ID", "PAC_ID", "pac_id", "Provider_PAC_ID"),
    enrl_id = c("Ind_enrl_ID", "enrl_id", "Enrollment_ID", "Provider_Enrollment_ID"),
    last_name = c("lst_nm", "Last_Name", "last_name", "LastName", "LAST_NAME"),
    first_name = c("frst_nm", "First_Name", "first_name", "FirstName", "FIRST_NAME"),
    middle_name = c("mid_nm", "Middle_Name", "middle_name", "MiddleName", "MIDDLE_NAME"),
    city = c("cty", "City", "city", "CITY", "Practice_City"),
    state = c("st", "State", "state", "STATE", "Practice_State"),
    zip = c("zip", "Zip", "ZIP", "Zip_Code", "zip_code", "ZIP_CODE", "Practice_Zip")
  )
  
  # Find available columns for each identifier type
  selected_columns <- list()
  for (id_type in names(id_columns)) {
    possible_columns <- id_columns[[id_type]]
    found_column <- NULL
    
    for (col in possible_columns) {
      if (col %in% available_columns) {
        found_column <- col
        break
      }
    }
    
    if (!is.null(found_column)) {
      selected_columns[[id_type]] <- found_column
    }
  }
  
  # If we have at least NPI or a combination of name+location, proceed with deduplication
  if ("npi" %in% names(selected_columns) || 
      (all(c("last_name", "first_name") %in% names(selected_columns)) && 
       any(c("city", "state", "zip") %in% names(selected_columns)))) {
    
    logger::log_info("Using columns for deduplication: {paste(unlist(selected_columns), collapse=', ')}")
    
    # Create deduplication key using available columns
    dedup_expression <- paste(
      paste0("physician_data$", unlist(selected_columns)),
      collapse = ", "
    )
    
    dedup_cmd <- paste0("physician_data %>% dplyr::mutate(dedup_key = paste(", 
                        dedup_expression, ", sep = '||')) %>% ",
                        "dplyr::distinct(dedup_key, .keep_all = TRUE) %>% ",
                        "dplyr::select(-dedup_key)")
    
    # Execute the dynamic deduplication command
    deduplicated_data <- eval(parse(text = dedup_cmd))
    
    return(deduplicated_data)
  } else {
    # Not enough identifiers for reliable deduplication
    logger::log_warn("Insufficient identifier columns for deduplication - skipping")
    return(physician_data)
  }
}

#' @noRd
merge_chunks_to_final_output <- function(chunk_directory, preserve_all_columns = TRUE) {
  logger::log_info("Merging chunks into final output file")
  
  # Create timestamp for output filename
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
  
  # Define final output path
  final_output_path <- stringr::str_glue(
    "/Volumes/Video Projects Muffly 1/physician_compare/merged_filtered_physician_compare_{timestamp}.csv"
  )
  
  logger::log_info("Final output will be saved to: {final_output_path}")
  logger::log_info("Preserving all columns: {preserve_all_columns}")
  
  # Get all chunk files
  chunk_paths <- fs::dir_ls(chunk_directory, glob = "*.csv")
  
  if (length(chunk_paths) == 0) {
    logger::log_error("No chunk files found in {chunk_directory}")
    stop("No chunk files found for merging")
  }
  
  logger::log_info("Found {length(chunk_paths)} chunk files to merge")
  
  # If preserving all columns, we need a different approach
  if (preserve_all_columns) {
    # First, read all chunks to identify all unique columns
    all_chunks <- list()
    all_columns <- c()
    
    for (i in seq_along(chunk_paths)) {
      chunk_path <- chunk_paths[i]
      logger::log_info("Reading chunk {i}/{length(chunk_paths)}: {chunk_path}")
      
      tryCatch({
        chunk_data <- readr::read_csv(
          chunk_path, 
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )
        
        # Add to list of chunks
        all_chunks[[i]] <- chunk_data
        
        # Update set of all columns
        all_columns <- union(all_columns, colnames(chunk_data))
        
        # Explicit cleanup
        rm(chunk_data)
        gc()
      }, 
      error = function(e) {
        logger::log_error("Failed to read chunk {chunk_path}: {e$message}")
        stop(paste("Error reading chunk:", chunk_path, "-", e$message))
      })
    }
    
    logger::log_info("Found {length(all_columns)} unique columns across all chunks")
    
    # Now process each chunk, ensuring it has all columns
    all_data <- NULL
    total_rows_written <- 0
    
    for (i in seq_along(all_chunks)) {
      chunk_data <- all_chunks[[i]]
      chunk_path <- chunk_paths[i]
      
      logger::log_info("Processing chunk {i}/{length(all_chunks)}: {chunk_path}")
      
      # Ensure all columns exist
      missing_columns <- setdiff(all_columns, colnames(chunk_data))
      if (length(missing_columns) > 0) {
        logger::log_info("Adding {length(missing_columns)} missing columns to chunk {i}")
        for (col in missing_columns) {
          chunk_data[[col]] <- NA_character_
        }
      }
      
      # Ensure column order is consistent
      chunk_data <- chunk_data %>% dplyr::select(all_of(all_columns))
      
      # Add to combined data or write directly to file
      if (is.null(all_data)) {
        all_data <- chunk_data
      } else {
        all_data <- dplyr::bind_rows(all_data, chunk_data)
      }
      
      # Update row count
      total_rows_written <- total_rows_written + nrow(chunk_data)
      
      # Explicit cleanup
      rm(chunk_data)
      gc()
    }
    
    # Write the combined data to the final output file
    readr::write_csv(all_data, final_output_path)
    logger::log_info("Wrote {total_rows_written} total rows to final output")
    
    # Clean up
    rm(all_data, all_chunks)
    gc()
  } else {
    # Traditional approach - use first chunk as template
    is_first_chunk <- TRUE
    total_rows_written <- 0
    
    for (chunk_path in chunk_paths) {
      logger::log_info("Processing chunk: {chunk_path}")
      
      tryCatch({
        chunk_data <- readr::read_csv(
          chunk_path, 
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )
        
        row_count <- nrow(chunk_data)
        logger::log_info("Read {row_count} rows from chunk {chunk_path}")
        
        # Verify year_source column exists and has values
        if (!"year_source" %in% colnames(chunk_data)) {
          logger::log_error("year_source column missing in chunk {chunk_path}")
          stop("year_source column is missing in a chunk file")
        }
        
        year_source_values <- unique(chunk_data$year_source)
        logger::log_info("Year source values in chunk: {paste(year_source_values, collapse=', ')}")
        
        # Check for and handle any column issues
        if (is_first_chunk) {
          first_chunk_columns <- colnames(chunk_data)
        } else {
          # Ensure consistent columns across chunks
          current_columns <- colnames(chunk_data)
          missing_columns <- setdiff(first_chunk_columns, current_columns)
          extra_columns <- setdiff(current_columns, first_chunk_columns)
          
          if (length(missing_columns) > 0) {
            logger::log_warn("Missing columns in chunk {chunk_path}: {paste(missing_columns, collapse=', ')}")
            for (col in missing_columns) {
              chunk_data[[col]] <- NA_character_
            }
          }
          
          if (length(extra_columns) > 0) {
            logger::log_warn("Extra columns in chunk {chunk_path}: {paste(extra_columns, collapse=', ')}")
            chunk_data <- chunk_data %>% dplyr::select(-all_of(extra_columns))
          }
          
          # Reorder columns to match first chunk
          chunk_data <- chunk_data %>% dplyr::select(all_of(first_chunk_columns))
        }
        
        # Write to final output
        readr::write_csv(
          chunk_data, 
          final_output_path, 
          append = !is_first_chunk
        )
        
        total_rows_written <- total_rows_written + row_count
        logger::log_info("Wrote {row_count} rows to final output (running total: {total_rows_written})")
        
        is_first_chunk <- FALSE
        
        # Explicit cleanup
        rm(chunk_data)
        gc()
      }, 
      error = function(e) {
        logger::log_error("Failed to process chunk {chunk_path}: {e$message}")
        stop(paste("Error processing chunk:", chunk_path, "-", e$message))
      })
    }
  }
  
  logger::log_info("Successfully merged {length(chunk_paths)} chunks ({total_rows_written} total rows)")
  
  return(final_output_path)
}

####



merge_largest_physician_compare_filtered_files(
    base_dir = "/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files/",
    output_dir = "/Volumes/Video Projects Muffly 1/physician_compare/merged_chunks/",
    verbose = TRUE,
    deduplicate = TRUE,
    strict_NPI_year_dedup = TRUE, # FYI : It looks like there are one or two physicians who go to over 100 different hospitals so they have a huge number of duplicates for each NPI, Year.  
    preserve_all_columns = TRUE
) 

physician_compare_output <- readr::read_csv("/Volumes/Video Projects Muffly 1/physician_compare/merged_filtered_physician_compare_20250504_0959.csv")

names(physician_compare_output)
dim(physician_compare_output)
glimpse((physician_compare_output))

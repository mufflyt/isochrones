#==============================================================================
# Check Physician Presence Across NPPES Data
#==============================================================================

#------------------------------------------------------------------------------
# 1. Load Required Packages
#------------------------------------------------------------------------------
# Load all required packages with error handling
required_packages <- c(
  "DBI",          # Database interface
  "duckdb",       # DuckDB database
  "dplyr",        # Data manipulation
  "stringr",      # String operations
  "readr",        # File reading
  "lubridate",    # Date handling
  "tyler",        # State abbreviation conversion
  "logger",       # Logging operations
  "assertthat",   # Input validation
  "tibble",       # Enhanced data frames
  "knitr",        # Table formatting
  "ggplot2"       # Visualization
)

# Function to safely load packages
load_packages <- function(pkg_list) {
  not_installed <- character()
  for (pkg in pkg_list) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      not_installed <- c(not_installed, pkg)
      message(paste("Package", pkg, "is not installed."))
    } else {
      suppressPackageStartupMessages(library(pkg, character.only = TRUE))
      message(paste("Loaded package:", pkg))
    }
  }
  
  if (length(not_installed) > 0) {
    warning(paste("Some required packages are not installed:", 
                  paste(not_installed, collapse = ", ")))
  }
}

# Load all packages
load_packages(required_packages)

# Clean memory
invisible(gc())

#------------------------------------------------------------------------------
# 2. Load Custom Functions
#------------------------------------------------------------------------------
# Load custom functions with error handling
custom_functions_path <- "R/bespoke_functions.R"

if (file.exists(custom_functions_path)) {
  source(custom_functions_path)
  logger::log_info("Successfully loaded custom functions from %s", custom_functions_path)
} else {
  stop(paste("Custom functions file not found at:", custom_functions_path))
}

#------------------------------------------------------------------------------
# 3. Define Physicians to Track
#------------------------------------------------------------------------------
# Create a data frame of physicians to check
#------------------------------------------------------------------------------
# 3. Define Physicians to Track
#------------------------------------------------------------------------------
# Create a data frame of physicians to track
physicians_to_track <- tibble::tibble(
  NPI = c(
    # Original 6 physicians
    "1689603763",   # Tyler Muffly
    "1528060639",   # John Curtin
    "1346355807",   # Pedro Miranda
    "1437904760",   # Lizeth Acosta
    "1568738854",   # Aaron Lazorwitz
    "1194571661",   # Ana Gomez
    
    # Additional 10 physicians
    "1861482333",   # Laura Riley
    "1730162884",   # Charles Lockwood
    "1982619441",   # Eve Espey
    "1598914152",   # Howard Jones
    "1306902762",   # Camran Nezhat
    "1669577409",   # Hal Lawrence
    "1760562193",   # Sandra Carson
    "1518967355",   # Catherine Spong
    "1093802191"    # Nancy Chescheir
  ),
  Name = c(
    # Original 6 physicians
    "Tyler Muffly, MD",
    "John Curtin, MD",
    "Pedro Miranda, MD",
    "Lizeth Acosta, MD",
    "Aaron Lazorwitz, MD",
    "Ana Gomez, MD",
    
    # Additional 10 physicians
    "Laura Riley, MD",
    "Charles Lockwood, MD",
    "Eve Espey, MD",
    "Howard Jones, MD",
    "Camran Nezhat, MD",
    "Hal Lawrence, MD",
    "Sandra Carson, MD",
    "Catherine Spong, MD",
    "Nancy Chescheir, MD"
  ),
  Specialty = c(
    # Original 6 physicians
    "Female Pelvic Medicine",
    "Gynecologic Oncology",
    "General OB-GYN",
    "General OB-GYN",
    "Family Planning",
    "General OB-GYN",
    
    # Additional 10 physicians
    "Maternal-Fetal Medicine",
    "Maternal-Fetal Medicine",
    "Family Planning",
    "Gynecologic Oncology",
    "Minimally Invasive Surgery",
    "General OB-GYN",
    "Reproductive Endocrinology",
    "Maternal-Fetal Medicine",
    "Maternal-Fetal Medicine"
  )
)

#------------------------------------------------------------------------------
# 4. Check Physician Presence
#------------------------------------------------------------------------------
# First validate that the main data exists
if (!exists("obgyn_physicians_all_years") || nrow(obgyn_physicians_all_years) == 0) {
  stop("Dataset 'obgyn_physicians_all_years' not found or empty.")
}

# Log the start of the analysis
logger::log_info("Starting analysis of %d physicians across NPPES data", 
                 nrow(physicians_to_track))

# Check presence of specified physicians
physician_presence <- phase0_check_physician_presence(
  physician_data = obgyn_physicians_all_years, 
  npi_list = physicians_to_track$NPI, 
  names_list = physicians_to_track$Name
); physician_presence

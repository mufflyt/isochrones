#######################
source("R/01-setup.R")
#######################

#The check_physician_presence function is a well-designed utility for tracking physicians across temporal data. It efficiently analyzes a dataset containing physician information to determine when specific providers appear in the records. The function accepts a list of National Provider Identifiers (NPIs), optionally paired with provider names, and methodically examines each NPI's presence throughout different years. It returns a structured data frame summarizing each provider's representation in the dataset, including their total record count and a chronological listing of years in which they appear. This function is particularly valuable for longitudinal analyses of healthcare provider data, enabling researchers to identify patterns in physician presence, track career trajectories, or validate data completeness across multiple years of NPI records.

invisible(gc())
source("R/bespoke_functions.R") # This loads up the process_nppes_data function

# List of NPIs to check
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  "1437904760",   # Lizeth Acosta, MD
  "1568738854",   # Aaron Lazorwitz, MD
  "1194571661",   # Ana Gomez, MD
  "1699237040",   # Erin W. Franks, MD
  "1003311044",   # CATHERINE Callinan, MD
  "1609009943",   # Kristin Powell, MD
  "1114125051",   # Nathan Kow, MD
  "1043432123",   # Elena Tunitsky, MD
  "1215490446",   # PK
  "1487879987"    # Peter Jeppson
)

# List of physician names (optional, for better logging and output)
names_list <- c(
  "Tyler Muffly, MD",
  "John Curtin, MD",
  "Pedro Miranda, MD",
  "Lizeth Acosta, MD",
  "Aaron Lazorwitz, MD",
  "Ana Gomez, MD",
  "Erin W. Franks, MD", 
  "Catherine Callinan, MD",
  "Kristin Powell, MD",
  "Nathan Kow, MD", 
  "Elena Tunitsky, MD",
  "Parisa Khalighi, MD",
  "Peter Jeppson, MD"
)

# Load the processed OB/GYN physicians data from RDS file
obgyn_physicians_all_years <- readRDS("data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.rds")

# Check presence of specified physicians
physician_presence <- check_physician_presence(
  obgyn_physicians_all_years, 
  npi_list, 
  names_list
); knitr::kable(physician_presence, caption = "Presence of Specific Physicians by Year")

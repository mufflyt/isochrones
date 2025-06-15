#The provided code sets up a data analysis environment in R by loading various packages and setting up directories. Here's a summary of what each section of the code does:
# 
# 1. **Setting Up Packages**: The code starts by setting a seed for reproducibility and loading numerous R packages. These packages are essential for data manipulation, visualization, and geospatial analysis.
# 
# 2. **Setting Up Directories**: It sets the working directory to "~/Dropbox (Personal)/isochrones" using the `here` package. It also defines folders for data, results, figures, and R code using the `here::here()` function. These directories are used to organize and manage data and results.
# 
# 3. **Bespoke Functions**: The code defines several custom functions for specific tasks, including:
#   - `search_by_taxonomy`: A function to search the National Provider Identifier (NPI) Database by taxonomy.
# - `search_and_process_npi`: A memoized function to search and process NPI numbers from a specified input file.
# - `create_geocode`: A memoized function to geocode addresses using the HERE API and update a CSV file with geocoded information.
# - `create_and_save_physician_dot_map`: A function to create and save a dot map of physicians with specified options.
# - `test_and_process_isochrones`: A function to test and process isochrones (areas reachable within a specified time) for given locations.
# - `process_and_save_isochrones`: A function to process and save isochrones for a large dataset in chunks.
# 
# 4. **Example Usage**: The code provides comments and example usage for some of the defined functions, such as `search_and_process_npi`, to demonstrate how to use them.

set.seed(1978)
invisible(gc())

library(censusapi)       # Access US Census Bureau data via API
library(conflicted)
library(DataExplorer)
library(data.table)      # Extends data frame capabilities for efficient data manipulation
library(DBI)             # This is required for connecting to the database
library(dbplyr)
library(DiagrammeR)
library(DiagrammeRsvg)
library(downloader)
library(duckdb)
library(DBI)
library(duckplyr)
library(easyr)           # A package for simplifying common R tasks
library(forcats)         # Tools for working with categorical data
library(formattable)     # Format data for tables and charts
library(fs)
library(ggalluvial)
library(ggplot2)         # Data visualization package based on the Grammar of Graphics
library(ggthemes)        # Additional themes for ggplot2
library(glue)
library(here)
library(hereR)           # Helps manage file paths and project directories
library(htmltools)       # Tools for working with HTML widgets
library(htmlwidgets)     # Create interactive HTML widgets from R
library(humaniformat)    # Format numbers as human-readable text
library(leaflet)
library(leaflet.minicharts)  # Mini bar charts for leaflet maps
library(mapview)         # Interactive viewing of spatial data
library(maps)            # Draw maps and add map-based data
library(memoise)         # Provides memoization functions for caching results
library(npi)             # Tools for working with National Provider Identifier (NPI) numbers
library(parallel)
library(purrr)           # Functional programming toolkit
#library(provider)        # Access to healthcare provider data
library(progress)        # Creates progress bars to monitor code execution progress
library(readxl)          # Read Excel files
library(rnaturalearth)   # Access Natural Earth data for mapping
library(RPostgres)       # If using PostgreSQL
library(sf)              # Provides classes and methods for working with spatial data
library(shiny)           # Building interactive web applications
library(skimr)
library(stringi)         # String manipulation functions (UTF-8 aware)
library(stringr)         # String manipulation functions
library(tidyverse)
library(tidygeocoder)    # Geocoding and reverse geocoding of addresses
library(tidycensus)      # Access US Census data via API
library(tidyr)           # Tools for reshaping and tidying data
library(tigris)          # Access US Census Bureau TIGER/Line data
library(viridis)         # Color palettes for data visualization
library(wesanderson)     # Color palettes inspired by Wes Anderson films
library(webshot)         # Takes screenshots of web pages
library(rnaturalearth)


devtools::install_github("cysouw/qlcMatrix")
devtools::install_github("paulhendricks/anonymizer")

#devtools::install_github("exploratory-io/exploratory_func")
#library(exploratory)

#Of note this is a personal package with some bespoke functions and data that we will use occasionally.  It is still under development and it is normal for it to give multiple warnings at libary(tyler).
# devtools::install_github("mufflyt/tyler")
# library(tyler)

# Store tidycensus data on cache
options(tigris_use_cache = TRUE)
options(tigris_class = "sf")
options(tigris_use_cache = TRUE)
getOption("tigris_use_cache")

source("/Users/tylermuffly/Dropbox (Personal)/isochrones/R/api_keys.R")

#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Directory structure with here
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
here::set_here(path = ".", verbose = TRUE)
here::i_am(path = "isochrones.Rproj")
data_folder <- here::here("data")
results_folder <- here::here("results")
images_folder <- here::here("figures")
code_folder <- here::here("R")

# number of threads to use in the R script
threads = parallel::detectCores()

########### Bespoke Functions ----
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Functions for nomogram
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####

# Define an exclusion operator for ease of filtering
`%nin%`<-Negate(`%in%`)

## Bespoke functions.  

#' Search NPI Database by Taxonomy
search_by_taxonomy <- function(taxonomy_to_search) {
  # Create an empty data frame to store search results
  data <- data.frame()

  # Loop over each taxonomy description
  for (taxonomy in taxonomy_to_search) {
    tryCatch({
      # Perform the search for the current taxonomy
      result <- npi::npi_search(
        taxonomy_description = taxonomy,
        country_code = "US",
        enumeration_type = "ind",
        limit = 1200
      )

      if (!is.null(result)) {
        # Process and filter the data for the current taxonomy
        data_taxonomy <- npi::npi_flatten(result) %>%
          dplyr::distinct(npi, .keep_all = TRUE) %>%
          dplyr::mutate(search_term = taxonomy) %>%
          dplyr::filter(addresses_country_name == "United States") %>%
          dplyr::mutate(basic_credential = stringr::str_remove_all(basic_credential, "[[\\p{P}][\\p{S}]]")) %>%
          dplyr::filter(stringr::str_to_lower(basic_credential) %in% stringr::str_to_lower(c("MD", "DO"))) %>%
          dplyr::arrange(basic_last_name) %>%
          dplyr::filter(stringr::str_detect(taxonomies_desc, taxonomy)) %>%
          dplyr::select(-basic_credential, -basic_last_updated, -basic_status, -basic_name_prefix, -basic_name_suffix, -basic_certification_date, -other_names_type, -other_names_code, -other_names_credential, -other_names_first_name, -other_names_last_name, -other_names_prefix, -other_names_suffix, -other_names_middle_name, -identifiers_code, -identifiers_desc, -identifiers_identifier, -identifiers_state, -identifiers_issuer, -taxonomies_code, -taxonomies_taxonomy_group, -taxonomies_state, -taxonomies_license, -addresses_country_code, -addresses_country_name, -addresses_address_purpose, -addresses_address_type, -addresses_address_2, -addresses_fax_number, -endpoints_endpointType, -endpoints_endpointTypeDescription, -endpoints_endpoint, -endpoints_affiliation, -endpoints_useDescription, -endpoints_contentTypeDescription, -endpoints_country_code, -endpoints_country_name, -endpoints_address_type, -endpoints_address_1, -endpoints_city, -endpoints_state, -endpoints_postal_code, -endpoints_use, -endpoints_endpointDescription, -endpoints_affiliationName, -endpoints_contentType, -endpoints_contentOtherDescription, -endpoints_address_2, -endpoints_useOtherDescription) %>%
          dplyr::distinct(npi, .keep_all = TRUE) %>%
          dplyr::distinct(basic_first_name, basic_last_name, basic_middle_name, basic_sole_proprietor, basic_gender, basic_enumeration_date, addresses_state, .keep_all = TRUE) %>%
          dplyr::mutate(full_name = paste(
            stringr::str_to_lower(basic_first_name),
            stringr::str_to_lower(basic_last_name)
          ))

        # Append the data for the current taxonomy to the main data frame
        data <- dplyr::bind_rows(data, data_taxonomy)
      }
    }, error = function(e) {
      message(sprintf("Error in search for %s:\n%s", taxonomy, e$message))
    })
  }

  # Write the combined data frame to an RDS file
  filename <- paste("data/search_taxonomy", format(Sys.time(), format = "%Y-%m-%d_%H-%M-%S"), ".rds", sep = "_")
  readr::write_rds(data, filename)

  return(data)
}

##############################
###############################
#' Search and Process NPI Numbers
# Define a memoization function for search_and_process_npi

search_and_process_npi <- memoise(function(input_file,
                                           enumeration_type = "ind",
                                           limit = 5L,
                                           country_code = "US",
                                           filter_credentials = c("MD", "DO")) {

  cat("Starting search_and_process_npi...\n")

  # Check if the input file exists
  if (!file.exists(input_file)) {
    stop(
      "The specified file with the NAMES to search'", input_file, "' does not exist.\n",
      "Please provide the full path to the file."
    )
  }
  cat("Input file found.\n")

  # Read data from the input file
  file_extension <- tools::file_ext(input_file)

  if (file_extension == "rds") {
    data <- readRDS(input_file)
  } else if (file_extension %in% c("csv", "xls", "xlsx")) {
    if (file_extension %in% c("xls", "xlsx")) {
      data <- readxl::read_xlsx(input_file)
    } else {
      data <- readr::read_csv(input_file)
    }
  } else {
    stop("Unsupported file format. Please provide an RDS, CSV, or XLS/XLSX file of NAMES to search.")
  }
  cat("Data loaded from the input file.\n")

  first_names <- data$first
  last_names <- data$last

  # Define the list of taxonomies to filter
  vc <- c("Allergy & Immunology", "Allergy & Immunology, Allergy", "Anesthesiology", "Anesthesiology, Critical Care Medicine", "Anesthesiology, Hospice and Palliative Medicine", "Anesthesiology, Pain Medicine", "Advanced Practice Midwife", "Colon & Rectal Surgery", "Dermatology", "Dermatology, Clinical & Laboratory Dermatological Immunology", "Dermatology, Dermatopathology", "Dermatology, MOHS-Micrographic Surgery", "Dermatology, Pediatric Dermatology", "Dermatology, Procedural Dermatology", "Doula", "Emergency Medicine", "Emergency Medicine, Emergency Medical Services", "Emergency Medicine, Hospice and Palliative Medicine", "Emergency Medicine, Medical Toxicology", "Emergency Medicine, Pediatric Emergency Medicine", "Emergency Medicine, Undersea and Hyperbaric Medicine", "Family Medicine", "Family Medicine, Addiction Medicine", "Family Medicine, Adolescent Medicine", "Family Medicine, Adult Medicine", "Family Medicine, Geriatric Medicine", "Family Medicine, Hospice and Palliative Medicine", "Family Medicine, Sports Medicine", "Internal Medicine", "Internal Medicine, Addiction Medicine", "Internal Medicine, Adolescent Medicine", "Internal Medicine, Advanced Heart Failure and Transplant Cardiology", "Internal Medicine, Allergy & Immunology", "Internal Medicine, Bariatric Medicine", "Internal Medicine, Cardiovascular Disease", "Internal Medicine, Clinical Cardiac Electrophysiology", "Internal Medicine, Critical Care Medicine", "Internal Medicine, Endocrinology, Diabetes & Metabolism", "Internal Medicine, Gastroenterology", "Internal Medicine, Geriatric Medicine", "Internal Medicine, Hematology", "Internal Medicine, Hematology & Oncology", "Internal Medicine, Hospice and Palliative Medicine", "Internal Medicine, Hypertension Specialist", "Internal Medicine, Infectious Disease", "Internal Medicine, Interventional Cardiology", "Internal Medicine, Medical Oncology", "Internal Medicine, Nephrology", "Internal Medicine, Pulmonary Disease", "Internal Medicine, Rheumatology", "Internal Medicine, Sleep Medicine", "Internal Medicine, Sports Medicine", "Lactation Consultant, Non-RN", "Medical Genetics, Clinical Biochemical Genetics", "Medical Genetics, Clinical Genetics (M.D.)", "Medical Genetics, Ph.D. Medical Genetics", "Midwife", "Nuclear Medicine", "Neuromusculoskeletal Medicine, Sports Medicine", "Neuromusculoskeletal Medicine & OMM", "Nuclear Medicine, Nuclear Cardiology", "Obstetrics & Gynecology", "Obstetrics & Gynecology, Complex Family Planning", "Obstetrics & Gynecology, Critical Care Medicine", "Obstetrics & Gynecology, Gynecologic Oncology", "Obstetrics & Gynecology, Gynecology", "Obstetrics & Gynecology, Hospice and Palliative Medicine", "Obstetrics & Gynecology, Maternal & Fetal Medicine", "Obstetrics & Gynecology, Obstetrics", "Obstetrics & Gynecology, Reproductive Endocrinology", "Ophthalmology", "Ophthalmology, Cornea and External Diseases Specialist", "Ophthalmology, Glaucoma Specialist", "Ophthalmology, Ophthalmic Plastic and Reconstructive Surgery", "Ophthalmology, Pediatric Ophthalmology and Strabismus Specialist", "Ophthalmology, Retina Specialist", "Oral & Maxillofacial Surgery", "Orthopaedic Surgery", "Orthopaedic Surgery, Adult Reconstructive Orthopaedic Surgery", "Orthopaedic Surgery, Foot and Ankle Surgery", "Orthopaedic Surgery, Hand Surgery", "Orthopaedic Surgery, Orthopaedic Surgery of the Spine", "Orthopaedic Surgery, Orthopaedic Trauma", "Orthopaedic Surgery, Pediatric Orthopaedic Surgery", "Orthopaedic Surgery, Sports Medicine", "Otolaryngology, Facial Plastic Surgery", "Otolaryngology, Otolaryngic Allergy", "Otolaryngology, Otolaryngology/Facial Plastic Surgery", "Otolaryngology, Otology & Neurotology", "Otolaryngology, Pediatric Otolaryngology", "Otolaryngology, Plastic Surgery within the Head & Neck", "Pain Medicine, Interventional Pain Medicine", "Pain Medicine, Pain Medicine", "Pathology, Anatomic Pathology", "Pathology, Anatomic Pathology & Clinical Pathology", "Pathology, Anatomic Pathology & Clinical Pathology", "Pathology, Blood Banking & Transfusion Medicine")

  bc <- c("Pathology, Chemical Pathology", "Pathology, Clinical Laboratory Director, Non-physician", "Pathology, Clinical Pathology", "Pathology, Clinical Pathology/Laboratory Medicine", "Pathology, Cytopathology", "Pathology, Dermatopathology", "Pathology, Forensic Pathology", "Pathology, Hematology", "Pathology, Medical Microbiology", "Pathology, Molecular Genetic Pathology", "Pathology, Neuropathology", "Pediatrics", "Pediatrics, Adolescent Medicine", "Pediatrics, Clinical & Laboratory Immunology", "Pediatrics, Child Abuse Pediatrics", "Pediatrics, Developmental - Behavioral Pediatrics", "Pediatrics, Hospice and Palliative Medicine", "Pediatrics, Neonatal-Perinatal Medicine", "Pediatrics, Neurodevelopmental Disabilities", "Pediatrics, Pediatric Allergy/Immunology", "Pediatrics, Pediatric Cardiology", "Pediatrics, Pediatric Critical Care Medicine", "Pediatrics, Pediatric Emergency Medicine", "Pediatrics, Pediatric Endocrinology", "Pediatrics, Pediatric Gastroenterology", "Pediatrics, Pediatric Hematology-Oncology", "Pediatrics, Pediatric Infectious Diseases", "Pediatrics, Pediatric Nephrology", "Pediatrics, Pediatric Pulmonology", "Pediatrics, Pediatric Rheumatology", "Pediatrics, Sleep Medicine", "Physical Medicine & Rehabilitation, Neuromuscular Medicine", "Physical Medicine & Rehabilitation, Pain Medicine", "Physical Medicine & Rehabilitation", "Physical Medicine & Rehabilitation, Pediatric Rehabilitation Medicine", "Physical Medicine & Rehabilitation, Spinal Cord Injury Medicine", "Physical Medicine & Rehabilitation, Sports Medicine", "Plastic Surgery", "Plastic Surgery, Plastic Surgery Within the Head and Neck", "Plastic Surgery, Surgery of the Hand", "Preventive Medicine, Aerospace Medicine", "Preventive Medicine, Obesity Medicine", "Preventive Medicine, Occupational Medicine", "Preventive Medicine, Preventive Medicine/Occupational Environmental Medicine", "Preventive Medicine, Undersea and Hyperbaric Medicine", "Preventive Medicine, Public Health & General Preventive Medicine", "Psychiatry & Neurology, Addiction Medicine", "Psychiatry & Neurology, Addiction Psychiatry", "Psychiatry & Neurology, Behavioral Neurology & Neuropsychiatry", "Psychiatry & Neurology, Brain Injury Medicine", "Psychiatry & Neurology, Child & Adolescent Psychiatry", "Psychiatry & Neurology, Clinical Neurophysiology", "Psychiatry & Neurology, Forensic Psychiatry", "Psychiatry & Neurology, Geriatric Psychiatry", "Psychiatry & Neurology, Neurocritical Care", "Psychiatry & Neurology, Neurology", "Psychiatry & Neurology, Neurology with Special Qualifications in Child Neurology", "Psychiatry & Neurology, Psychiatry", "Psychiatry & Neurology, Psychosomatic Medicine", "Psychiatry & Neurology, Sleep Medicine", "Psychiatry & Neurology, Vascular Neurology", "Radiology, Body Imaging", "Radiology, Diagnostic Neuroimaging", "Radiology, Diagnostic Radiology", "Radiology, Diagnostic Ultrasound", "Radiology, Neuroradiology", "Radiology, Nuclear Radiology", "Radiology, Pediatric Radiology", "Radiology, Radiation Oncology", "Radiology, Vascular & Interventional Radiology", "Specialist", "Surgery", "Surgery, Pediatric Surgery", "Surgery, Plastic and Reconstructive Surgery", "Surgery, Surgery of the Hand", "Surgery, Surgical Critical Care", "Surgery, Surgical Oncology", "Surgery, Trauma Surgery", "Surgery, Vascular Surgery", "Urology", "Urology, Female Pelvic Medicine and Reconstructive Surgery", "Urology, Pediatric Urology","Pathology", "Thoracic Surgery (Cardiothoracic Vascular Surgery)" , "Transplant Surgery")

  # Create a function to search NPI based on first and last names
  search_npi <- function(first_name, last_name) {
    cat("Searching NPI for:", first_name, last_name, "\n")
    tryCatch(
      {
        # NPI search object
        npi_obj <- npi::npi_search(first_name = first_name, last_name = last_name)

        # Retrieve basic and taxonomy data from npi objects
        t <- npi::npi_flatten(npi_obj, cols = c("basic", "taxonomies"))

        # Subset results with taxonomy that matches taxonomies in the lists
        t <- t %>% dplyr::filter(taxonomies_desc %in% vc | taxonomies_desc %in% bc)
      },
      error = function(e) {
        cat("ERROR:", conditionMessage(e), "\n")
        return(NULL)  # Return NULL for error cases
      }
    )
    return(t)
  }

  # Create an empty list to receive the data
  out <- list()

  # Initialize progress bar
  total_names <- length(first_names)
  pb <- progress::progress_bar$new(total = total_names)

  # Search NPI for each name in the input data
  out <- purrr::map2(first_names, last_names, function(first_name, last_name) {
    pb$tick()
    search_npi(first_name, last_name)
  })

  # Filter npi_data to keep only elements that are data frames
  npi_data <- Filter(is.data.frame, out)

  # Combine multiple data frames into a single data frame using data.table::rbindlist()
  result <- data.table::rbindlist(npi_data, fill = TRUE)

  return(result)
})

# Example usage:
# input_file <- "data-raw/acog_presidents.csv"
# output_result <- search_and_process_npi(input_file)
# readr::write_csv(output_result, "results_of_search_and_process_npi.csv")



#**************************
#* create_geocode: 04-geocode.R.  GEOCODE THE DATA USING HERE API.  
#**************************
# geocoding based on Nominatim
# Function for geocoding based on Nominatim


create_geocode_nominatim <- function(csv_file, output_file) {
  
  # Check if the CSV file exists
  if (!file.exists(csv_file)) {
    stop("Error: CSV file not found.")
  }
  
  cat("Reading CSV file...\n")
  
  # Read the CSV file into a data frame
  data <- read.csv(csv_file)
  
  # Check if the data frame contains a column named "address"
  if (!"address" %in% colnames(data)) {
    stop("Error: The CSV file must have a column named 'address' for geocoding.")
  }
  
  cat("Renaming 'address' column to 'addr'...\n")
  
  # We have to rename the 'address' column to 'addr' because of a compatibility issue
  idx_addr <- which(colnames(data) == 'address')
  colnames(data)[idx_addr] <- 'addr'
  
  cat("Geocoding addresses using Nominatim...\n")
  
  # Geocode the addresses using Nominatim
  res_geoc <- tidygeocoder::geocode(
    .tbl = data, 
    method = 'osm',                       # Nominatim geocoding method
    address = addr,                       # Column containing addresses
    lat = "lat",                          # Column for latitude
    long = "long",                        # Column for longitude
    limit = 1,                            # Maximum results per address
    return_input = TRUE,                  # Include input data in results
    progress_bar = TRUE,                 # Show progress bar
    quiet = FALSE                         # Display geocoding messages
  )
  
  cat("Renaming 'addr' column back to 'address'...\n")
  
  # Rename the 'addr' column back to 'address'
  idx_addr <- which(colnames(res_geoc) == 'addr')
  colnames(res_geoc)[idx_addr] <- 'address'
  
  # Create a list to save two sublists:
  # - wo_geocode: contains rows with missing 'lat' or 'long' values
  # - geocode: contains geocoded data with 'lat' and 'long'
  lst_out <- list()
  
  idx_nan <- which(is.na(res_geoc$lat) | is.na(res_geoc$long))
  if (length(idx_nan) > 0) {
    cat("Appending rows without geocode to 'wo_geocode'...\n")
    # Append rows without geocode to 'wo_geocode'
    lst_out[['wo_geocode']] <- res_geoc[idx_nan, , drop = FALSE]
    
    cat("Removing rows without geocode from the main result...\n")
    # Remove these rows from the main result
    res_geoc <- res_geoc[-idx_nan, , drop = FALSE]
  } else {
    lst_out[['wo_geocode']] <- 'There are no missing values'
  }
  
  if (nrow(res_geoc) > 0) {
    cat("Saving the geocoded data to an output CSV file...\n")
    # Save the geocoded data to an output file
    write.csv(res_geoc, file = output_file, row.names = FALSE)
    
    cat("Converting to an 'sf' object with coordinates...\n")
    # Convert to an 'sf' object with coordinates
    res_geoc_sf <- sf::st_as_sf(res_geoc, coords = c('long', 'lat'), crs = 4326)
    lst_out[['geocode']] <- res_geoc_sf
  } else {
    lst_out[['geocode']] <- 'The tidygeocoder::geocode() function returned only NA values'
  }
  
  cat("Geocoding process completed.\n")
  
  return(lst_out)
}


##############################
###############################
create_and_save_physician_dot_map <- function(physician_data, jitter_range = 0.05, color_palette = "magma", popup_var = "name") {
  # Add jitter to latitude and longitude coordinates
  jittered_physician_data <- physician_data %>%
    dplyr::mutate(
      lat = lat + runif(n()) * jitter_range,
      long = long + runif(n()) * jitter_range
    )

  # Create a base map using tyler::create_base_map()
  cat("Setting up the base map...\n")
  base_map <- tyler::create_base_map("Physician Dot Map")
  cat("Map setup complete.\n")

  # Generate ACOG districts using tyler::generate_acog_districts_sf()
  cat("Generating the ACOG district boundaries from tyler::generate_acog_districts_sf...\n")
  acog_districts <- tyler::generate_acog_districts_sf()

  # Define the number of ACOG districts
  num_acog_districts <- 11

  # Create a custom color palette using viridis
  district_colors <- viridis::viridis(num_acog_districts, option = color_palette)

  # Reorder factor levels
  jittered_physician_data$ACOG_District <- factor(
    jittered_physician_data$ACOG_District,
    levels = c("District I", "District II", "District III", "District IV", "District V",
               "District VI", "District VII", "District VIII", "District IX",
               "District XI", "District XII"))

  # Create a Leaflet map
  dot_map <- base_map %>%
    # Add physician markers
    leaflet::addCircleMarkers(
      data = jittered_physician_data,
      lng = ~long,
      lat = ~lat,
      radius = 3,         # Adjust the radius as needed
      stroke = TRUE,      # Add a stroke (outline)
      weight = 1,         # Adjust the outline weight as needed
      color = district_colors[as.numeric(physician_data$ACOG_District)],   # Set the outline color to black
      fillOpacity = 0.8,  # Fill opacity
      popup = as.formula(paste0("~", popup_var))  # Popup text based on popup_var argument
    ) %>%
    # Add ACOG district boundaries
    leaflet::addPolygons(
      data = acog_districts,
      color = "red",      # Boundary color
      weight = 2,         # Boundary weight
      fill = FALSE,       # No fill
      opacity = 0.8,      # Boundary opacity
      popup = ~ACOG_District   # Popup text
    ) %>%
    # Add a legend
    leaflet::addLegend(
      position = "bottomright",   # Position of the legend on the map
      colors = district_colors,   # Colors for the legend
      labels = levels(physician_data$ACOG_District),   # Labels for legend items
      title = "ACOG Districts"   # Title for the legend
    )

  # Generate a timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

  # Define file names with timestamps
  html_file <- paste0("figures/dot_map_", timestamp, ".html")
  png_file <- paste0("figures/dot_map_", timestamp, ".png")

  # Save the Leaflet map as an HTML file
  htmlwidgets::saveWidget(widget = dot_map, file = html_file, selfcontained = TRUE)
  cat("Leaflet map saved as HTML:", html_file, "\n")

  # Capture and save a screenshot as PNG
  webshot::webshot(html_file, file = png_file)
  cat("Screenshot saved as PNG:", png_file, "\n")

  # Return the Leaflet map
  return(dot_map)
}

##############################
###############################
#***************
test_and_process_isochrones <- function(input_file) {
  input_file
  input_file <- input_file %>%
    dplyr::mutate(id = row_number())# %>%
    #filter(postmastr.name.x != "Hye In Park, MD")

  input_file$lat <- as.numeric(input_file$lat)
  input_file$long <- as.numeric(input_file$long)

  input_file_sf <- input_file %>%
    st_as_sf(coords = c("long", "lat"), crs = 4326)

  posix_time <- as.POSIXct("2023-10-20 09:00:00", format = "%Y-%m-%d %H:%M:%S")
  # Here are the dates for the third Friday in October from 2013 to 2022:
  # October 18, 2013
  # October 17, 2014
  # October 16, 2015
  # October 21, 2016
  # October 20, 2017
  # October 19, 2018
  # October 18, 2019
  # October 16, 2020
  # October 15, 2021
  # October 21, 2022

  error_rows <- vector("list", length = nrow(input_file_sf))

  for (i in 1:nrow(input_file_sf)) {
    row_data <- input_file_sf[i, ]

    isochrones <- tryCatch(
      {
        hereR::isoline(
          poi = row_data,
          range = c(1),
          datetime = posix_time,
          routing_mode = "fast",
          range_type = "time",
          transport_mode = "car",
          url_only = FALSE,
          optimize = "balanced",
          traffic = TRUE,
          aggregate = FALSE
        )
      },
      error = function(e) {
        message("Error processing row ", i, ": ", e$message)
        return(NULL)
      }
    )

    if (is.null(isochrones)) {
      error_rows[[i]] <- i
    }
  }

  # Collect the rows that caused errors
  error_rows <- unlist(error_rows, use.names = FALSE)

  if (length(error_rows) > 0) {
    message("Rows with errors: ", paste(error_rows, collapse = ", "))
  } else {
    message("No errors found.")
  }
}

##############################
###############################
# Define cache filesystem
fc <- cache_filesystem(file.path(".cache"))

# process_and_save_isochrones <- memoise::memoise(function(input_file, chunk_size = 25, 
#                                         iso_datetime_yearly,
#                                         iso_ranges = c(30*60, 60*60, 120*60, 180*60),
#                                         crs = 4326, 
#                                         transport_mode = "car",
#                                         file_path_prefix = "data/06-isochrones/isochrones_") {
#   message("Starting isochrones processing...")
#   conflicted::conflicts_prefer(base::setdiff)
#   
#   # Convert latitude and longitude to numeric
#   input_file <- input_file %>%
#     dplyr::mutate(across(c(lat, long), as.numeric))
#   
#   # Convert input file to sf object
#   input_file_sf <- sf::st_as_sf(input_file, coords = c("long", "lat"), crs = crs)
#   
#   num_chunks <- ceiling(nrow(input_file_sf) / chunk_size)
#   isochrones_list <- list()
#   
#   message("Number of chunks to process: ", num_chunks)
#   for (i in 1:num_chunks) {
#     message("Processing chunk ", i, " of ", num_chunks)
#     start_idx <- (i - 1) * chunk_size + 1
#     end_idx <- min(i * chunk_size, nrow(input_file_sf))
#     chunk_data <- input_file_sf[start_idx:end_idx, ]
#     
#     year_index <- match(format(as.POSIXct(iso_datetime_yearly$date), "%Y"), iso_datetime_yearly$year)
#     posix_time <- as.POSIXct(iso_datetime_yearly$date[year_index], format = "%Y-%m-%d %H:%M:%S")
#     
#     # Process isochrones
#     isochrones <- tryCatch(
#       {
#         hereR::isoline(
#           poi = chunk_data,
#           range = iso_ranges,
#           datetime = posix_time,
#           routing_mode = "fast",
#           range_type = "time",
#           transport_mode = transport_mode,
#           url_only = FALSE,
#           optimize = "balanced",
#           traffic = TRUE,   # We do want traffic to factor in
#           aggregate = FALSE # DO NOT CHANGE
#         )
#       },
#       error = function(e) {
#         message("Error processing chunk ", i, ": ", e$message)
#         return(NULL)
#       }
#     )
#     
#     if (!is.null(isochrones)) {
#       current_datetime <- format(Sys.time(), "%Y%m%d%H%M%S")
#       
#       # Generate file name
#       file_name <- paste(file_path_prefix, current_datetime, "_chunk_", min(chunk_data$id), "_to_", max(chunk_data$id))
#       
#       isochrones <- isochrones %>%
#         dplyr::mutate(arrival = as.POSIXct(arrival, format = "%Y-%m-%d %H:%M:%S"))
#       
#       # Write isochrones to shapefile
#       message("Writing chunk ", i, " to file: ", file_name)
#       sf::st_write(
#         isochrones,
#         dsn = file_name,
#         layer = "isochrones",
#         driver = "ESRI Shapefile",
#         quiet = FALSE
#       )
#       
#       isochrones_list[[i]] <- isochrones
#     }
#   }
#   # Combine isochrones from all chunks
#   isochrones_data <- base::do.call(rbind, isochrones_list)
#   
#   message("Finished processing all chunks.")
#   return(isochrones_data)
# }, cache = fc) #cache argument here

process_and_save_isochrones <- memoise::memoise(function(input_file, chunk_size = 25, 
                                                         iso_datetime_yearly,
                                                         iso_ranges = c(30*60, 60*60, 120*60, 180*60),
                                                         crs = 4326, 
                                                         transport_mode = "car",
                                                         file_path_prefix = "data/06-isochrones/isochrones_",
                                                         output_csv_path = "data/06-isochrones/",
                                                         output_sf_path = "data/06-isochrones/") {
  message("Starting isochrones processing...")
  conflicted::conflicts_prefer(base::setdiff)
  
  # Convert latitude and longitude to numeric
  input_file <- input_file %>%
    dplyr::mutate(across(c(lat, long), as.numeric))
  
  # Convert input file to sf object
  input_file_sf <- sf::st_as_sf(input_file, coords = c("long", "lat"), crs = crs)
  
  num_chunks <- ceiling(nrow(input_file_sf) / chunk_size)
  isochrones_list <- list()
  
  message("Number of chunks to process: ", num_chunks)
  for (i in 1:num_chunks) {
    message("Processing chunk ", i, " of ", num_chunks)
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, nrow(input_file_sf))
    chunk_data <- input_file_sf[start_idx:end_idx, ]
    
    year_index <- match(format(as.POSIXct(iso_datetime_yearly$date), "%Y"), iso_datetime_yearly$year)
    posix_time <- as.POSIXct(iso_datetime_yearly$date[year_index], format = "%Y-%m-%d %H:%M:%S")
    
    # Process isochrones
    isochrones <- tryCatch(
      {
        hereR::isoline(
          poi = chunk_data,
          range = iso_ranges,
          datetime = posix_time,
          routing_mode = "fast",
          range_type = "time",
          transport_mode = transport_mode,
          url_only = FALSE,
          optimize = "balanced",
          traffic = TRUE,   # We do want traffic to factor in
          aggregate = FALSE # DO NOT CHANGE
        )
      },
      error = function(e) {
        message("Error processing chunk ", i, ": ", e$message)
        return(NULL)
      }
    )
    
    if (!is.null(isochrones)) {
      current_datetime <- format(Sys.time(), "%Y%m%d%H%M%S")
      
      # Generate file name
      file_name <- paste(file_path_prefix, current_datetime, "_chunk_", min(chunk_data$id), "_to_", max(chunk_data$id))
      
      isochrones <- isochrones %>%
        dplyr::mutate(arrival = as.POSIXct(arrival, format = "%Y-%m-%d %H:%M:%S"))
      
      # Write isochrones to shapefile
      message("Writing chunk ", i, " to file: ", file_name)
      sf::st_write(
        isochrones,
        dsn = file_name,
        layer = "isochrones",
        driver = "ESRI Shapefile",
        quiet = FALSE
      )
      
      isochrones_list[[i]] <- isochrones
    }
  }
  # Combine isochrones from all chunks
  isochrones_data <- base::do.call(rbind, isochrones_list)
  
  # Generate timestamp for output files
  current_datetime <- format(Sys.time(), "%Y%m%d%H%M%S")
  
  # Write output CSV if specified
  if (!is.null(output_csv_path)) {
    output_csv_full_path <- paste0(output_csv_path, "isochrones_data_", current_datetime, ".csv")
    message("Writing combined isochrones data to CSV: ", output_csv_full_path)
    tryCatch(
      {
        readr::write_csv(isochrones_data, output_csv_full_path)
        message("Output CSV successfully written to ", output_csv_full_path)
      },
      error = function(e) {
        message("Failed to write output CSV: ", e$message)
      }
    )
  }
  
  # Write output SF file if specified
  if (!is.null(output_sf_path)) {
    output_sf_full_path <- paste0(output_sf_path, "isochrones_data_", current_datetime, ".shp")
    message("Writing combined isochrones data to SF file: ", output_sf_full_path)
    tryCatch(
      {
        sf::st_write(isochrones_data, dsn = output_sf_full_path, layer = "isochrones", driver = "ESRI Shapefile", append = FALSE)
        message("Output SF file successfully written to ", output_sf_full_path)
      },
      error = function(e) {
        message("Failed to write output SF file: ", e$message)
      }
    )
  }
  
  message("Finished processing all chunks.")
  return(isochrones_data)
}, cache = fc)

##############################
###############################
track_api_calls_and_cost <- function(iso_ranges, num_rows) {
  # Define constants for pricing
  free_calls_per_month <- 2500
  cost_per_thousand_calls <- 5.50
  
  # Calculate total API calls per row
  api_calls_per_row <- length(iso_ranges)
  
  # Calculate total API calls for all rows
  total_api_calls <- api_calls_per_row * num_rows
  
  # Determine if API calls exceed the free limit
  if (total_api_calls <= free_calls_per_month) {
    total_cost <- 0
  } else {
    # Calculate cost for additional API calls
    additional_calls <- total_api_calls - free_calls_per_month
    total_cost <- additional_calls * cost_per_thousand_calls / 1000
  }
  
  # Return total API calls and estimated cost
  return(list(total_api_calls = total_api_calls, total_cost = total_cost))
}



##############################
###############################

# THIS FUNCTION IS NOW WORKING.  
us_fips <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  dplyr::filter(state_code < 56) %>%
  dplyr::select(state_code) %>%
  dplyr::pull()

get_census_data <- function (us_fips_list) 
{
  state_data <- list()
  for (f in us_fips) {
    us_fips <- tyler::fips
    print(f)
    stateget <- paste("state:", f, "&in=county:*&in=tract:*", 
                      sep = "")
      census_key <- Sys.getenv("CENSUS_API_KEY")
      if (census_key == "") {
        stop("CENSUS_API_KEY environment variable is not set. Please add it to your .Renviron or .env file")
      }
      state_data[[f]] <- getCensus(name = "acs/acs5", vintage = 2019,
                                   vars = c("NAME", paste0("B01001_0", c("01", 26, 33:49),
                                                           "E")), region = "block group:*", regionin = stateget,
                                   key = census_key)
  }
  acs_raw <- dplyr::bind_rows(state_data)
  Sys.sleep(1)
  return(acs_raw)
}

######
#Entire USA national scale block groups only start at 2019
process_block_groups <- function(years) {
  for (year in years) {
    block_groups_by_year <- tigris::block_groups(state = NULL, cb = TRUE, year = year)
    
    sf_block_groups <- st_as_sf(block_groups_by_year)
    
    # Making the geometry valid
    sf_block_groups <- st_make_valid(sf_block_groups)
    
    # Transforming the CRS to EPSG:2163 and simplifying the geometry
    sf_block_groups_transformed <- sf_block_groups %>%
      st_transform(2163) %>%
      st_simplify(preserveTopology = FALSE, dTolerance = 1000)
    
    shapefile_name <- paste0("data/shp/block_groups_tigris_", year, ".shp")
    
    st_write(sf_block_groups_transformed, dsn = shapefile_name, append = FALSE)
  }
}

# Usage example:
#years_to_process <- c(2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022)
#process_block_groups(years_to_process)

######
download_and_merge_block_groups <- function(year) {
  # Specific list of state FIPS codes
  us_fips_list <- c("01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
                    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
                    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
                    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
                    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55")
  
  # Initialize an empty list to store sf objects
  sf_list <- list()
  
  # Loop through each state and download the block group data
  for (state_code in us_fips_list) {
    message("Processing state: ", state_code)
    
    # Download block group data for the state
    state_block_groups <- block_groups(state = state_code, cb = TRUE, year = year)
    
    # Convert to sf object
    state_sf <- st_as_sf(state_block_groups)
    
    # Make the geometry valid
    state_sf <- st_make_valid(state_sf)
    
    # Transform to a common CRS
    state_sf <- st_transform(state_sf, crs = 2163)
    
    # Simplify the geometry
    state_sf <- st_simplify(state_sf, preserveTopology = FALSE, dTolerance = 1000)
    
    # Add a 'year' column
    state_sf$year <- year
    
    # Add to the list
    sf_list[[state_code]] <- state_sf
  }
  
  # Combine all sf objects in the list into one sf object
  usa_block_groups <- do.call(rbind, sf_list)
  
  # Write to a shapefile
  shapefile_name <- paste0("block_groups_tigris_", year, ".shp")
  st_write(usa_block_groups, shapefile_name, append=FALSE)
  
  return(usa_block_groups)
}

# Example usage: download data for the year 2020
# combined_block_groups_2020 <- download_and_merge_block_groups(2020)

##########
format_pct <- function(x, my_digits = 0) {
  format(x, digits = my_digits, nsmall = my_digits)
}


#########
postico_database_obgyns_by_year <- function(year, db_details) {
  # Database connection details
  db_host <- db_details$host
  db_port <- db_details$port
  db_name <- db_details$name
  db_user <- db_details$user
  db_password <- db_details$password
  
  # Create a database connection
  # db_connection <- dbConnect(
  #   RPostgres::Postgres(),
  #   dbname = db_name,
  #   host = db_host,
  #   port = db_port,
  #   user = db_user,
  #   password = db_password
  # )
  
  # Reference the tables using tbl
  nppes_table_name <- as.character(year)
  nppes_table <- dplyr::tbl(db_connection, nppes_table_name)
  nucc_taxonomy_table_name <- "nucc_taxonomy_201"
  nucc_taxonomy_201 <- dplyr::tbl(db_connection, nucc_taxonomy_table_name)
  
  # Fetch and process the data from the database
  nppes_data <- nppes_table %>%
    distinct(NPI, .keep_all = TRUE) %>%
    dplyr::mutate(`Zip Code` = str_sub(`Zip Code`,1 ,5)) %>%
    dplyr::filter(`Primary Specialty` %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY")) %>% 
    collect()
  
  # Write the processed data to a CSV file
  #write_csv(nppes_data, paste0("data/02.5-subspecialists_over_time.R/Postico_output_", year, "_nppes_data_filtered.csv"))
  write_csv(nppes_data, paste0("Postico_output_", year, "_nppes_data_filtered.csv"))
}

# Example usage
# db_details <- list(
#   host = "localhost",
#   port = 5433,
#   name = "template1",
#   user = "postgres",
#   password = "????"
# )

#########
#Function 1: validate_and_remove_invalid_npi
validate_and_remove_invalid_npi <- function(input_data) {
  
  if (is.data.frame(input_data)) {
    # Input is a dataframe
    df <- input_data
  } else if (is.character(input_data)) {
    # Input is a file path to a CSV
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Remove rows with missing or empty NPIs
  df <- df %>%
    #head(5) %>%. #for testing only
    dplyr::filter(!is.na(npi) & npi != "")
  
  # Add a new column "npi_is_valid" to indicate NPI validity
  df <- df %>%
    dplyr::mutate(npi_is_valid = sapply(npi, function(x) {
      if (is.numeric(x) && nchar(x) == 10) {
        npi::npi_is_valid(as.character(x))
      } else {
        FALSE
      }
    })) %>%
    dplyr::filter(!is.na(npi_is_valid) & npi_is_valid)
  
  # Return the valid dataframe with the "npi_is_valid" column
  return(df)
}

############
validate_and_remove_invalid_npi <- function(input_data) {
  
  if (is.data.frame(input_data)) {
    # Input is a dataframe
    df <- input_data
  } else if (is.character(input_data)) {
    # Input is a file path to a CSV
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Standardize NPI column name
  npi_col <- if ("npi" %in% names(df)) {
    "npi"
  } else if ("NPI" %in% names(df)) {
    "NPI"
  } else {
    stop("Dataframe must contain a column named 'npi' or 'NPI'.")
  }
  
  # Remove rows with missing or empty NPIs
  df <- df %>%
    dplyr::filter(!is.na(!!sym(npi_col)) & !!sym(npi_col) != "")
  
  # Add a new column "npi_is_valid" to indicate NPI validity
  df <- df %>%
    dplyr::mutate(npi_is_valid = sapply(!!sym(npi_col), function(x) {
      if (is.numeric(x) && nchar(x) == 10) {
        npi::npi_is_valid(as.character(x))
      } else {
        FALSE
      }
    })) %>%
    dplyr::filter(!is.na(npi_is_valid) & npi_is_valid)
  
  # Return the valid dataframe with the "npi_is_valid" column
  return(df)
}

############
# Define the retrieve_clinician_data function with error handling
retrieve_clinician_data <- function(input_data, chunk_size = 100, output_dir = "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results") {
  message("The data should already have had the NPI numbers validated.")
  
  # Load data or read from file
  if (is.data.frame(input_data)) {
    df <- input_data
  } else if (is.character(input_data)) {
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  # Create the output directory if it doesn't exist
  dir.create(output_dir, showWarnings = FALSE)
  
  # Remove duplicate NPIs
  df <- df %>% distinct(npi, .keep_all = TRUE)
  
  # Function to retrieve clinician data for a single NPI
  get_clinician_data <- function(npi) {
    if (is.na(npi) || !is.numeric(npi) || nchar(as.character(npi)) != 10) {
      cat("Invalid NPI:", npi, "\n")
      return(NULL)  # Skip this NPI
    }
    
    clinician_info <- provider::clinicians(npi = npi)
    if (is.null(clinician_info)) {
      cat("No results for NPI:", npi, "\n")
      return(NULL)
    } else {
      return(clinician_info)  # Return the clinician data
    }
    Sys.sleep(2)  # To avoid rate limits in API calls
  }
  
  # Process in chunks
  n <- nrow(df)  # Get the total number of rows in the dataframe
  for (i in seq(1, n, by = chunk_size)) {  # Loop through the data in chunks
    df_chunk <- df[i:min(i + chunk_size - 1, n), ]  # Extract a chunk of data
    chunk_results <- df_chunk %>%
      dplyr::mutate(clinician_data = purrr::map(.x = npi, .f = get_clinician_data)) %>%
      unnest(clinician_data, names_sep = "_") %>%
      distinct(npi, .keep_all = TRUE)  # Process the chunk: retrieve clinician data, unnest, and remove duplicates
    
    # Export the chunk results to a CSV file within the specified output directory
    chunk_output_csv <- file.path(output_dir, paste0("retrieve_clinician_data_chunk_results_", i, ".csv"))
    write.csv(chunk_results, chunk_output_csv, row.names = FALSE)
    message("Chunk results have been saved to ", chunk_output_csv)
  }
}

# retrieve_clinician_data(input_data, 
#                         chunk_size = 100, 
#                         output_dir = "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results")

# For 02.25-downloader
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
      message("Copied the largest file from folder:", year_subdir_name, ": ", largest_file_name, " to ", target_dir)
    }
  }
}


remove_table <- function(connection, table_name) {
  dbExecute(connection, glue::glue("DROP TABLE IF EXISTS {table_name}"))
  cat("Table", table_name, "has been removed from the database.\n")
}

# Used in 04-create_geocode_nominatim.R
iso_datetime_yearly <- tibble(
  date = c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
           "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
           "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
           "2022-10-21 09:00:00", "2023-10-20 09:00:00"),
  year = c("2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023")
)

# Function to generate range descriptions
generate_range_description <- function(range) {
  if (range >= 0 && range <= 30) {
    return("0-29 minutes")
  } else if (range > 30 && range <= 60) {
    return("30-59 minutes")
  } else if (range > 60 && range <= 120) {
    return("60-119 minutes")
  } else if (range > 120 && range <= 180) {
    return("120-179 minutes")
  } else {
    return("180+ minutes")
  }
}

# 02.33-bner_nppes_data
create_duckdb_tables <- function(directory_path, file_names, con) {
  conflicted::conflicts_prefer(stringr::str_remove_all)
  
  # Iterate over each file name provided
  for (i in seq_along(file_names)) {
    file_name <- file_names[i]
    full_path <- file.path(directory_path, file_name)
    table_name <- tools::file_path_sans_ext(gsub("[^A-Za-z0-9]", "_", file_name))  # Clean and create a valid table name
    table_name <- paste0(table_name, "_", i)  # Add counter to make table name unique
    
    # Construct SQL command to create a table
    sql_command <- sprintf(
      "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, quote='\"', escape='\"', null_padding=true, ignore_errors=true)",
      table_name, full_path
    )
    
    # Execute the SQL command
    dbExecute(con, sql_command)
    
    # Optional: output the list of tables after creation
    cat(sprintf("Table '%s' created from '%s'.\n", table_name, file_name))
  }
  
  # List all tables in DuckDB to confirm
  return(dbListTables(con))
}

# # Define the directory path
# directory_path <- "/path/to/your/directory"
# 
# # Retrieve file names from the directory
# file_names <- list_files_in_directory(directory_path)
# 
# # Database connection (already defined)
# con <- your_existing_database_connection
# 
# # Create DuckDB tables from the files
# created_tables <- create_duckdb_tables(directory_path, file_names, con)
# 
# # Optional: print the list of tables in DuckDB after creation
# cat("List of tables in DuckDB after creation:\n")
# print(created_tables)


# 02.33-bner_nppes_data
# Define a function to process each table in the database
process_tables <- function(con, table_names) {
  # Initialize an empty list to store the results
  results <- list()
  
  # Initialize an empty data frame to store merged data
  all_data <- data.frame()
  
  # Loop through each table name
  for (i in 1:length(table_names)) {
    table_name <- table_names[i]
    cat("Processing table:", table_name, "\n")
    
    # Use tryCatch to handle errors
    tryCatch({
      # Use duckplyr to create a reference to the table without loading it into R
      table_ref <- tbl(con, table_name)
      
      # Perform the data processing steps
      processed_data <- table_ref %>%
        select(
          NPI, # npi,
          `Entity Type Code`, # entity,
          `Provider First Name`, # pfname,
          `Provider Last Name (Legal Name)`, # plname,
          `Provider Other First Name`, # pfnameoth,
          `Provider First Line Business Practice Location Address`, # plocline1,
          `Provider Business Practice Location Address City Name`, # ploccityname,
          `Provider Business Practice Location Address State Name`, # plocstatename,
          `Provider Business Practice Location Address Postal Code`, # ploczip,
          `Provider Business Practice Location Address Country Code (If outside U.S.)`, # ploccountry,
          `Provider Business Practice Location Address Telephone Number`, # ploctel,
          `Provider First Line Business Mailing Address`, # pmailline1,
          `Provider Business Mailing Address City Name`, # pmailcityname,
          `Provider Business Mailing Address State Name`, # pmailstatename,
          `Provider Business Mailing Address Country Code (If outside U.S.)`, # pmailcountry,
          `Provider Business Mailing Address Postal Code`, # pmailzip,
          `Provider Gender Code`, # pgender,
          `Provider Credential Text`, # pcredential,
          `Provider Organization Name (Legal Business Name)`, # porgname,
          `Healthcare Provider Taxonomy Code_1`, # ptaxcode1,
          `Healthcare Provider Taxonomy Code_2`, # ptaxcode2,
          `Is Sole Proprietor`# soleprop,
        ) %>%
        dplyr::filter(
          `Entity Type Code` == 1,
          `Provider Business Mailing Address Country Code (If outside U.S.)` == "US",
          `Provider Business Practice Location Address Country Code (If outside U.S.)` == "US",
          `Healthcare Provider Taxonomy Code_1` %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X", "207VB0002X") |
            `Healthcare Provider Taxonomy Code_2` %in% c("207V00000X", "207VC0200X", "207VE0102X", "207VF0040X", "207VG0400X", "207VM0101X", "207VX0000X", "207VX0201X")
        ) %>%
        select(-`Entity Type Code`, -`Provider Other First Name`, -`Provider Business Practice Location Address Country Code (If outside U.S.)`, -`Provider Business Mailing Address Country Code (If outside U.S.)`) %>%
        dplyr::mutate(across(c(`Provider First Name`, `Provider Last Name (Legal Name)`, `Provider First Line Business Practice Location Address`, `Provider Business Practice Location Address City Name`, `Provider First Line Business Mailing Address`, `Provider Business Mailing Address City Name`), ~str_to_upper(.))) %>%
        distinct(NPI, `Provider Business Practice Location Address City Name`, `Provider Business Practice Location Address State Name`, .keep_all = TRUE) %>%
        dplyr::mutate(year = table_name) # Add a new column "year" with the table name
      
      cat("Processed table:", table_name, "\n")
      cat("Writing processed data to CSV...\n")
      
      # Collect the processed data
      processed_data_df <- processed_data %>% collect()
      
      # Append the processed data to the merged data frame
      all_data <- dplyr::bind_rows(all_data, processed_data_df)
      
      # Store the table name in the results list
      results[[table_name]] <- processed_data
    }, error = function(e) {
      cat("Error processing table:", table_name, "\n")
      message("Error message:", e$message, "\n\n")
    })
  }
  
  cat("All tables processed.\n")
  
  # Write the merged data frame to disk
  write_csv(all_data, "~/Dropbox (Personal)/isochrones/data/02.33-nber_nppes_data/nppes_years/all_nppes_files.csv")
  
  # Write the merged data frame to the database
  #db_write_table(con, "default.all_nppes_files", all_data, overwrite = TRUE)
  
  # Return the list of processed tables
  return(results)
}

#
calculate_intersection_overlap_and_save <- function(block_groups_file, isochrones_file, drive_time_variable, output_dir) {
  
  # Read block groups shapefile and process it
  block_groups <- sf::st_read(block_groups_file) %>%
    dplyr::mutate(bg_area = st_area(.)) %>%  # Calculate area for each block group
    sf::st_transform(2163) %>%
    sf::st_make_valid() %>%
    sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
  
  # Get CRS of block_groups
  block_groups_crs <- st_crs(block_groups)
  
  # Read isochrones shapefile and process it
  isochrones <- sf::st_read(isochrones_file) %>%
    dplyr::arrange(desc(rank)) %>% # This is IMPORTANT for the layering. 
    rename(drive_time = range) %>%
    sf::st_transform(2163) %>%
    sf::st_make_valid() %>%
    sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000) %>%
    sf::st_set_crs(block_groups_crs)  # Set CRS to match block_groups
  
  # Filter isochrones for the specified drive time
  isochrones_filtered <- isochrones %>%
    dplyr::filter(drive_time == drive_time_variable) %>%
    dplyr::mutate(isochrones_drive_time = drive_time_variable) 
  
  # Log the progress
  message(paste("Plot of Isochrones for", drive_time_variable, "minutes..."))
  
  # Function to plot isochrones with USA and state borders, roads, and save as an image file
  plot_isochrones_and_save <- function(isochrones, drive_time_variable, output_dir) {
    # Load USA and state borders data
    usa_border <- ne_countries(scale = "small", country = "United States of America", returnclass = "sf")
    state_border <- ne_states(country = "United States of America", returnclass = "sf")
    
    # Load roads data
    #roads <- ne_download(scale = 110, type = "roads", category = "physical", returnclass = "sf")
    
    # Define limits for the x and y axes to focus on the contiguous USA
    xlim <- c(-125, -65)  # Adjust as needed
    ylim <- c(25, 50)     # Adjust as needed
    
    # Create the ggplot
    p <- ggplot() +
      geom_sf(data = usa_border, color = "black", fill = NA) +  # USA borders
      geom_sf(data = state_border, color = "darkgray", fill = NA) +  # State borders
      #geom_sf(data = block_groups, color = "lightgray") +  
      geom_sf(data = isochrones[1], fill = alpha("blue", 0.5)) +  # Isochrones with fill alpha
      labs(title = paste("Isochrones for", drive_time_variable, "Minutes")) +
      coord_sf(xlim = xlim, ylim = ylim)  # Use coord_sf for spatial data and set limits
    
    # Define the output filename
    output_filename <- paste0("plot_", drive_time_variable, "_minutes.png")
    
    # Save the ggplot as an image file
    ggsave(file.path(output_dir, "isochrone_files", output_filename), plot = p, width = 7, height = 7)
  }
  
  # Call the function to plot and save the isochrones
  plot_isochrones_and_save(isochrones_filtered, drive_time_variable, output_dir)
  
  
  # Call the function to plot and save the isochrones
  plot_isochrones_and_save(isochrones_filtered, drive_time_variable, output_dir)
  
  # Write the filtered isochrones as a shapefile with the drive time variable in the filename
  output_filename <- paste0("filtered_isochrones_", drive_time_variable, "_minutes.shp")
  st_write(
    isochrones_filtered, append = FALSE,
    file.path(
      "data/08-get-block-group-overlap/isochrone_files",
      output_filename
    )
  )
  
  # Calculate intersection between block groups and isochrones
  intersect <- st_intersection(block_groups, isochrones_filtered) %>%
    dplyr::mutate(intersect_area = st_area(.)) %>%
    select(GEOID, intersect_area) %>%
    st_drop_geometry()
  
  # Assign intersect_drive_time after calculating intersection
  intersect <- intersect %>%
    dplyr::mutate(intersect_drive_time = drive_time_variable)
  
  # Log the progress
  message(paste("Calculating intersection for", drive_time_variable, "minutes..."))
  
  # Merge intersection area with block groups
  intersect_block_group <- dplyr::left_join(block_groups, intersect, by = "GEOID")
  
  # Calculate overlap and save cleaned data
  intersect_block_group_cleaned <- intersect_block_group %>% 
    dplyr::mutate(
      intersect_area = ifelse(is.na(intersect_area), 0, intersect_area),  # Assign 0 to NA intersect_area values
      overlap = as.numeric(intersect_area / bg_area)  # Calculate overlap as a proportion
    )
  write_csv(intersect_block_group_cleaned, paste0("data/08-get-block-group-overlap/intersect_block_group_cleaned_", drive_time_variable, "minutes.csv"))
  
  tryCatch(
    {
      # Calculate area in all block groups
      block_groups_intersected <- intersect_block_group_cleaned %>%
        dplyr::mutate(bg_area = st_area(.))
      
      # Summary of the overlap percentiles
      summary_bg <- summary(block_groups_intersected$overlap)
      
      # Print the summary
      message("Summary of Overlap Percentages for", drive_time_variable, "minutes:")
      message(paste0("The isochrones overlap with: ", round(summary_bg[[4]], 4) * 100,"% of the block groups in the block groups provided by area. " ))
    },
    error = function(e) {
      message("Error: ", e)
    }
  )
}

#
add_drive_time_column <- function(file_path, drive_time) {
  df <- readr::read_csv(file_path)
  df <- df %>% dplyr::mutate(intersect_drive_time = as.character(drive_time))
  readr::write_csv(df, file_path)
}


# Redefine the function to use within a dplyr chain
assign_lastupdate <- function(npi, year, updates) {
  update_values <- updates %>%
    dplyr::filter(npi == npi) %>%
    arrange(lastupdatestr) %>%
    pull(lastupdatestr) %>%
    na.omit() %>%
    unique()
  
  last_value <- NA_integer_
  for (update in update_values) {
    if (year >= update) {
      last_value <- update
    } else {
      break
    }
  }
  return(last_value)
}

# 0.8.5 
get_acs_data <- function(us_fips_list, vintage = 2019, acs_variables) {
  state_data <- list()
  for (fips_code in us_fips_list) {
    stateget <- paste("state:", fips_code, "&in=county:*&in=tract:*", sep = "")
      census_key <- Sys.getenv("CENSUS_API_KEY")
      if (census_key == "") {
        stop("CENSUS_API_KEY environment variable is not set. Please add it to your .Renviron or .env file")
      }
      state_data[[fips_code]] <- getCensus(name = "acs/acs5", vintage = vintage,
                                           vars = c("NAME", acs_variables),
                                           region = "block group:*", regionin = stateget,
                                           key = census_key)
  }
  acs_raw <- dplyr::bind_rows(state_data)
  acs_raw <- acs_raw %>%
    dplyr::mutate(year = vintage)
  Sys.sleep(1)
  
  readr::write_csv(acs_raw, paste0("data/08.75-acs/08.75-acs_", vintage, "vintage.csv"))
  return(acs_raw)
}


# fin
print("Setup is complete!")


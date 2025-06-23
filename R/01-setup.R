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

library(hereR)
library(tidyverse)
library(sf)
library(progress)
library(memoise)
library(easyr)
library(data.table)
library(npi)
library(tidyr)
library(leaflet)
library(tigris)
library(censusapi)
library(htmlwidgets)
library(webshot)
library(viridis)
library(wesanderson) # color palettes
library(mapview)
library(shiny) # creation of GUI, needed to change leaflet layers with dropdowns
library(htmltools) # added for saving html widget
## Installation commands removed to avoid side effects
## devtools::install_github('ramnathv/htmlwidgets')
## remotes::install_github("andrewallenbruce/provider")
library(provider)
library(readxl)
library(leaflet.extras)
library(leaflet.minicharts)
library(formattable)
library(tidycensus)
library(rnaturalearth)
library(purrr)
library(stringr)
library(stringi)
library(exploratory)
library(humaniformat)
library(ggplot2)
library(ggthemes)
library(maps)
library(forcats)
source("R/here_api_utils.R")

#Of note this is a personal package with some bespoke functions and data that we will use occasionally.  It is still under development and it is normal for it to give multiple warnings at libary(tyler).
# devtools::install_github("mufflyt/tyler")
# library(tyler)

# Store tidycensus data on cache
options(tigris_use_cache = TRUE)

if (!nzchar(Sys.getenv("HERE_API_KEY"))) {
  stop("HERE_API_KEY environment variable is not set.")
}
readRenviron("~/.Renviron")
hereR::set_key(Sys.getenv("HERE_API_KEY"))


#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Directory structure with here
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
here::set_here(path = ".", verbose = TRUE)
here::i_am("isochrones.Rproj")
data_folder <- here::here("data")
results_folder <- here::here("results")
images_folder <- here::here("figures")
code_folder <- here::here("R")

########### Bespoke Functions ----
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
#####  Functions for nomogram
#### #### #### #### #### #### #### #### #### #### #### #### #### #### #### ####
`%nin%`<-Negate(`%in%`)


#' Search NPI Database by Taxonomy
#'
#' @param taxonomy_to_search A character vector of taxonomy descriptions
#' @return A cleaned and filtered data frame of matched NPIs
search_by_taxonomy <- function(taxonomy_to_search) {
  logger::log_info("Starting taxonomy search for {length(taxonomy_to_search)} terms.")
  
  data <- dplyr::tibble()
  
  for (taxonomy in taxonomy_to_search) {
    logger::log_info("Searching for taxonomy: {taxonomy}")
    
    tryCatch({
      result <- npi::npi_search(
        taxonomy_description = taxonomy,
        country_code = "US",
        enumeration_type = "ind",
        limit = 1200
      )
      
      if (!is.null(result)) {
        flattened <- npi::npi_flatten(result)
        
        if (nrow(flattened) > 0) {
          flattened$search_term <- taxonomy
          
          if ("addresses_country_name" %in% names(flattened)) {
            flattened <- dplyr::filter(flattened, addresses_country_name == "United States")
          }
          
          if ("basic_credential" %in% names(flattened)) {
            flattened <- flattened %>%
              dplyr::mutate(
                basic_credential = stringr::str_remove_all(basic_credential, "[[\\p{P}][\\p{S}]]")
              ) %>%
              dplyr::filter(stringr::str_to_lower(basic_credential) %in% stringr::str_to_lower(c("MD", "DO")))
          }
          
          if ("taxonomies_desc" %in% names(flattened)) {
            flattened <- dplyr::filter(flattened, stringr::str_detect(taxonomies_desc, taxonomy))
          }
          
          if ("basic_last_name" %in% names(flattened)) {
            flattened <- dplyr::arrange(flattened, basic_last_name)
          }
          
          if (all(c("basic_first_name", "basic_last_name") %in% names(flattened))) {
            flattened <- flattened %>%
              dplyr::mutate(full_name = paste(
                stringr::str_to_lower(basic_first_name),
                stringr::str_to_lower(basic_last_name)
              ))
          }
          
          cols_to_remove <- c(
            "basic_last_updated", "basic_status", "basic_name_prefix", "basic_name_suffix",
            "basic_certification_date", "other_names_type", "other_names_code",
            "other_names_credential", "other_names_first_name", "other_names_last_name",
            "other_names_prefix", "other_names_suffix", "other_names_middle_name",
            "identifiers_code", "identifiers_desc", "identifiers_identifier", "identifiers_state",
            "identifiers_issuer", "taxonomies_code", "taxonomies_taxonomy_group",
            "taxonomies_state", "taxonomies_license", "addresses_country_code",
            "addresses_country_name", "addresses_address_purpose", "addresses_address_type",
            "addresses_address_2", "addresses_fax_number", "endpoints_endpointType",
            "endpoints_endpointTypeDescription", "endpoints_endpoint", "endpoints_affiliation",
            "endpoints_useDescription", "endpoints_contentTypeDescription",
            "endpoints_country_code", "endpoints_country_name", "endpoints_address_type",
            "endpoints_address_1", "endpoints_city", "endpoints_state", "endpoints_postal_code",
            "endpoints_use", "endpoints_endpointDescription", "endpoints_affiliationName",
            "endpoints_contentType", "endpoints_contentOtherDescription", "endpoints_address_2",
            "endpoints_useOtherDescription"
          )
          flattened <- flattened %>%
            dplyr::select(-tidyselect::any_of(intersect(cols_to_remove, names(flattened)))) %>%
            dplyr::distinct(npi, .keep_all = TRUE)
          
          if (all(c(
            "basic_first_name", "basic_last_name", "basic_middle_name", "basic_sole_proprietor",
            "basic_gender", "basic_enumeration_date", "addresses_state"
          ) %in% names(flattened))) {
            flattened <- flattened %>%
              dplyr::distinct(
                basic_first_name, basic_last_name, basic_middle_name,
                basic_sole_proprietor, basic_gender, basic_enumeration_date,
                addresses_state, .keep_all = TRUE
              )
          }
          
          data <- dplyr::bind_rows(data, flattened)
        }
      }
    }, error = function(e) {
      logger::log_warn("Error for taxonomy '{taxonomy}': {e$message}")
    })
  }
  
  filename <- paste0("data/search_taxonomy_", format(Sys.time(), "%Y-%m-%d_%H-%M-%S"), ".rds")
  readr::write_rds(data, filename)
  logger::log_info("Saved search results to {filename}")
  beepr::beep(2)
  
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
#* create_geocode: 04-geocode.R.  GEOCODE THE DATA USING HERE API.  The key is hard coded into the function.  
#**************************
create_geocode <- memoise::memoise(function(csv_file) {

  # Set your HERE API key from environment variable
  api_key <- Sys.getenv("HERE_API_KEY")
  if (identical(api_key, "")) {
    stop("HERE_API_KEY environment variable is not set.")
  }

  hereR::set_key(api_key)

  # Check if the CSV file exists
  if (!file.exists(csv_file)) {
    stop("CSV file not found.")
  }

  # Read the CSV file into a data frame
  data <- read.csv(csv_file)

  # Check if the data frame contains a column named "address"
  if (!"address" %in% colnames(data)) {
    stop("The CSV file must have a column named 'address' for geocoding.")
  }

  # Initialize a list to store geocoded results
  geocoded_results <- list()

  # Initialize progress bar
  pb <- progress_bar$new(total = nrow(data), format = "[:bar] :percent :elapsed :eta :rate")

  # Loop through each address and geocode it
  for (i in 1:nrow(data)) {
    address <- data[i, "address"]
    result <- hereR::geocode(address)
    geocoded_results[[i]] <- result
    cat("Geocoded address ", i, " of ", nrow(data), "\n")
    pb$tick()  # Increment the progress bar
  }

  # Combine all geocoded results into one sf object
  geocoded <- do.call(rbind, geocoded_results)

  # Add the geocoded information to the original data frame
  data$latitude <- geocoded$latitude
  data$longitude <- geocoded$longitude

  # Write the updated data frame with geocoded information back to a CSV file
  write.csv(data, csv_file, row.names = FALSE)
  cat("Updated CSV file with geocoded information.\n")

  cat("Geocoding complete.\n")

  return(geocoded)
})

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
  input_file <- input_file %>%
    mutate(id = row_number()) %>%
    filter(postmastr.name.x != "Hye In Park, MD")

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
process_and_save_isochrones <- function(input_file, chunk_size = 25, 
                                        iso_datetime = "2023-10-20 09:00:00",
                                        iso_ranges = c(30*60, 60*60, 120*60, 180*60),
                                        crs = 4326, 
                                        transport_mode = "car",
                                        file_path_prefix = "data/06-isochrones/isochrones_") 
  {
  message("Starting isochrones processing...")
  input_file$lat <- as.numeric(input_file$lat)
  input_file$long <- as.numeric(input_file$long)

  input_file_sf <- input_file %>%
    sf::st_as_sf(coords = c("long", "lat"), crs = crs)

  posix_time <- as.POSIXct(iso_datetime, format = "%Y-%m-%d %H:%M:%S")

  num_chunks <- ceiling(nrow(input_file_sf) / chunk_size)
  isochrones_list <- list()

  message("Processing ", num_chunks, " chunks...")
  for (i in 1:num_chunks) {
    message("Processing chunk ", i, " of ", num_chunks)
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, nrow(input_file_sf))
    chunk_data <- input_file_sf[start_idx:end_idx, ]

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
      # Create the file name with the current date and time
      current_datetime <- format(Sys.time(), "%Y%m%d%H%M%S")

      file_name <- paste0(
        file_path_prefix,
        current_datetime,
        "_chunk_",
        min(chunk_data$id),
        "_to_",
        max(chunk_data$id)
      )
      dir.create(file_name, recursive = TRUE, showWarnings = FALSE)

      # Assuming "arrival" field is originally in character format with both date and time
      # Convert it to a DateTime object
      isochrones$arrival <- as.POSIXct(isochrones$arrival, format = "%Y-%m-%d %H:%M:%S")

      # Save the data as a shapefile with the layer name "isochrones"
      message("Writing chunk ", i, " to file: ", file_name)
      sf::st_write(
        isochrones,
        dsn = file_name,
        layer = "isochrones",
        driver = "ESRI Shapefile",
        quiet = FALSE
      )

      # Store the isochrones in the list
      isochrones_list[[i]] <- isochrones
    }
  }

  # Combine all isochrones from the list into one data frame
  isochrones_data <- do.call(rbind, isochrones_list)

  message("Finished processing all chunks.")
  return(isochrones_data)
}


##############################
###############################

# THIS FUNCTION IS NOW WORKING.  
us_fips_list <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  dplyr::filter(state_code < 56) %>%
  dplyr::select(state_code) %>%
  dplyr::pull()


get_census_data <- function (us_fips_list)
{
  state_data <- list()
  census_key <- get_env_or_stop("CENSUS_API_KEY")
  for (f in us_fips) {
    us_fips <- tyler::fips

    print(f)
    stateget <- paste("state:", f, "&in=county:*&in=tract:*",
                      sep = "")
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
    mutate(`Zip Code` = str_sub(`Zip Code`,1 ,5)) %>%
    filter(`Primary Specialty` %in% c("GYNECOLOGICAL ONCOLOGY", "OBSTETRICS/GYNECOLOGY")) %>% 
    collect()
  
  # Write the processed data to a CSV file
  #write_csv(nppes_data, paste0("data/02.5-subspecialists_over_time.R/Postico_output_", year, "_nppes_data_filtered.csv"))
  write_csv(nppes_data, paste0("Postico_output_", year, "_nppes_data_filtered.csv"))
  return(nppes_data)
}

# Example usage
# db_details <- list(
#   host = "localhost",
#   port = 5433,
#   name = "template1",
#   user = "postgres",
#   password = "????"
# )

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
retrieve_clinician_data <- function(input_data, no_results_csv = "no_results_npi.csv") {
  message("The data should already have had the NPI numbers validated.")
  
  # Load data or read from file
  if (is.data.frame(input_data)) {
    df <- input_data
  } else if (is.character(input_data)) {
    df <- readr::read_csv(input_data)
  } else {
    stop("Input must be a dataframe or a file path to a CSV.")
  }
  
  
  # Remove duplicate NPIs
  df <- df %>% dplyr::distinct(npi, .keep_all = TRUE)
  
  # Initialize a list to store NPIs with no results
  no_results_npi <- vector("list", 0)
  
  # Function to retrieve clinician data for a single NPI
  get_clinician_data <- function(npi) {
    if (!is.numeric(npi) || nchar(npi) != 10) {
      cat("Invalid NPI:", npi, "\n")
      return(NULL)  # Skip this NPI
    }

    clinician_info <- provider::clinicians(npi = npi)
    if (is.null(clinician_info)) {
      cat("No results for NPI:", npi, "\n")
      no_results_npi <<- c(no_results_npi, list(npi))  # Add the NPI to the list
      return(NULL)
    } else {
      return(clinician_info)  # Return the clinician data
    }
    Sys.sleep(1)  # To avoid rate limits in API calls
  }
  
  # Loop through the NPI column and get clinician data
  df_updated <- df %>%
    #dplyr::mutate(row_number = row_number()) %>%
    dplyr::mutate(clinician_data = purrr::map(npi, get_clinician_data)) %>%
    tidyr::unnest(clinician_data, names_sep = "_") %>%
    dplyr::distinct(npi, .keep_all = TRUE)
  
  # Export NPIs without results to a CSV file
  if (length(no_results_npi) > 0) {
    write.csv(data.frame(NPI = unlist(no_results_npi)), no_results_csv, row.names = FALSE)
    message("PEOPLE WHO RETIRED ARE GOING TO BE NO RESULTS. NPIs without results have been saved to ", no_results_csv)
  }
  
  return(df_updated)
}

# fin
print("Setup is complete!")

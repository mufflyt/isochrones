#######################
source("R/01-setup.R")
#######################


source("R/01-setup.R")

search_and_process_npi <- memoise(function(input_file,
                                           enumeration_type = "ind",
                                           limit = 5L,
                                           country_code = "US",
                                           filter_credentials = c("MD", "DO")) {

  cat("Starting search_and_process_npi...\n")

  # Check if the input file exists
  if (!file.exists(input_file)) {
    stop(
      paste0(
        "The specified file with the NAMES to search '",
        input_file,
        "' does not exist.\nPlease provide the full path to the file."
      )
    )
  }
  cat("Input file found.\n")

  # Read data from the input file
  file_extension <- tools::file_ext(input_file)

  if (file_extension == "rds") {
    names_data <- readRDS(input_file)
  } else if (file_extension %in% c("csv", "xls", "xlsx")) {
    if (file_extension %in% c("xls", "xlsx")) {
      names_data <- readxl::read_xlsx(input_file)
    } else {
      names_data <- readr::read_csv(input_file)
    }
  } else {
    stop("Unsupported file format. Please provide an RDS, CSV, or XLS/XLSX file of NAMES to search.")
  }
  cat("Data loaded from the input file.\n")

  first_names <- names_data$first
  last_names <- names_data$last

  # Define the list of taxonomies to filter
  vc <- c("Student in an Organized Health Care Education/Training Program")


  # Create a function to search NPI based on first and last names
  search_npi <- function(first_name, last_name) {
    cat("Searching NPI for:", first_name, last_name, "\n")
    search_result <- tryCatch(
      {
        npi_obj <- npi::npi_search(first_name = first_name, last_name = last_name)
        t <- npi::npi_flatten(npi_obj, cols = c("basic", "taxonomies"))
        t %>% dplyr::filter(taxonomies_desc %in% vc)
      },
      error = function(e) {
        cat("ERROR:", conditionMessage(e), "\n")
        return(NULL)
      }
    )
    return(search_result)
  }

  # Create an empty list to receive the data
  search_results <- list()

  # Initialize progress bar
  total_names <- length(first_names)
  pb <- progress::progress_bar$new(total = total_names)

  # Search NPI for each name in the input data
  search_results <- purrr::map2(first_names, last_names, function(first_name, last_name) {
    pb$tick()
    search_npi(first_name, last_name)
  })

  # Filter npi_data to keep only elements that are data frames
  npi_data <- Filter(is.data.frame, search_results)

  # Combine multiple data frames into a single data frame using data.table::rbindlist()
  combined_results <- data.table::rbindlist(npi_data, fill = TRUE)

  return(combined_results)
})


input_file <- "data/Anuja_OBGYN_resident_name_search___Sheet1.csv"
search_results_df <- search_and_process_npi(input_file)
readr::write_csv(search_results_df, "data/results_of_search_and_process_npi.csv")


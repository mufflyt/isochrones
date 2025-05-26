

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
  vc <- c("Student in an Organized Health Care Education/Training Program")

  bc <- c("Student in an Organized Health Care Education/Training Program")

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


input_file <- "data/Anuja_OBGYN_resident_name_search___Sheet1.csv"
output_result <- search_and_process_npi(input_file)
readr::write_csv(output_result, "data/results_of_search_and_process_npi.csv")


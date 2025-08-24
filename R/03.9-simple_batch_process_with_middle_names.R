# 03.9-run

# Source the file
source("R/03.2-simple_batch_processing.R")

# For testing
short_abog_npi_matched_8_18_2025 <- readr::read_csv("data/02.33-nber_nppes_data/output/abog_npi_matched_8_18_2025.csv") %>%
  head(10) %>%
  readr::write_csv("data/02.33-nber_nppes_data/output/short_abog_npi_matched_8_18_2025.csv")
# 
# names(short_abog_npi_matched_8_18_2025)

# # Your command should now work
# output_simple_batch_process_with_middle_names <- simple_batch_process_with_middle_names(
#   input_csv_file = "data/02.33-nber_nppes_data/output/abog_npi_matched_8_18_2025.csv",
#   output_csv_file = "data/03-search_and_process_npi/output/simple_batch_process_with_middle_names.csv",
#   physician_name_column = "original_input_name",
#   npi_column = "batch_process_npi",
#   num_cores = 8,
#   chunk_size = 10,
#   verbose = TRUE,
#   fill_middle_names = TRUE,
# ); output_simple_batch_process_with_middle_names


source("R/03.2-simple_batch_processing.R")

output_simple_batch_process_with_middle_names <- simple_batch_process_with_middle_names(
  input_csv_file = "data/02.33-nber_nppes_data/output/abog_npi_matched_8_18_2025.csv",
  output_csv_file = "data/03-search_and_process_npi/output/simple_batch_process_with_middle_names.csv",
  physician_name_column = "original_input_name",
  npi_column = "batch_process_npi",
  num_cores = 8,
  chunk_size = 10,
  verbose = TRUE,
  fill_middle_names = TRUE
)


# Runs in the background.  Yes!  
# Create a complete script that sources the function first
script_content <- '
  # Source the required functions first
  source("R/03.2-simple_batch_processing.R")

  # Run the processing
  output_simple_batch_process_with_middle_names <- simple_batch_process_with_middle_names(
    input_csv_file = "data/02.33-nber_nppes_data/output/abog_npi_matched_8_18_2025.csv",
    output_csv_file = "data/03-search_and_process_npi/output/simple_batch_process_with_middle_names.csv",
    physician_name_column = "original_input_name",
    npi_column = "batch_process_npi",
    num_cores = 8,
    chunk_size = 10,
    verbose = TRUE,
    fill_middle_names = TRUE
  )

  # Save the results
  save(output_simple_batch_process_with_middle_names,
       file = "data/03-search_and_process_npi/output/batch_results.RData")

  cat("Background job completed successfully at:", format(Sys.time()), "\n")
  '

# Write the script
writeLines(script_content, "run_batch_processing.R")

# Run as background job in RStudio
rstudioapi::jobRunScript("run_batch_processing.R",
                         name = "NPI Batch Processing",
                         workingDir = getwd())

# Check if the job is complete
file.exists("data/03-search_and_process_npi/output/batch_results.RData")

# Load results when complete
if (file.exists("data/03-search_and_process_npi/output/batch_results.RData")) {
  load("data/03-search_and_process_npi/output/batch_results.RData")
  print(output_simple_batch_process_with_middle_names$summary)
}

# Check the latest output files
list.files("data/03-search_and_process_npi/output", pattern = "simple_batch.*csv$", full.names = TRUE)


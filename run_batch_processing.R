
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

  cat("Background job completed successfully at:", format(Sys.time()), "
")
  

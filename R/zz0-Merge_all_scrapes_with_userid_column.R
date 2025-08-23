# Function to check if a file contains "userid" column
check_for_userid_column <- function(file_path) {
  tryCatch({
    file_ext <- tools::file_ext(file_path)
    
    if (tolower(file_ext) == "csv") {
      # Read just the header for CSV
      header <- names(readr::read_csv(file_path, n_max = 0, show_col_types = FALSE))
      return("userid" %in% tolower(header))
      
    } else if (tolower(file_ext) == "rds") {
      # Read RDS file
      data <- readr::read_rds(file_path)
      if (is.data.frame(data)) {
        return("userid" %in% tolower(names(data)))
      }
      
    } else if (tolower(file_ext) %in% c("xlsx", "xls")) {
      # Read Excel file (first sheet only)
      header <- names(readxl::read_excel(file_path, n_max = 0))
      return("userid" %in% tolower(header))
    }
    
    return(FALSE)
  }, error = function(e) {
    return(FALSE)  # Skip files that can't be read
  })
}

# Search in current directory and subdirectories
search_directories <- c("data/", ".")  # Add your directories here

userid_files <- c()
for (search_dir in search_directories) {
  if (dir.exists(search_dir)) {
    # Get all CSV, RDS, and Excel files recursively
    all_files <- list.files(search_dir, 
                            pattern = "\\.(csv|rds|xlsx|xls)$", 
                            recursive = TRUE, 
                            full.names = TRUE,
                            ignore.case = TRUE)
    
    # Check each file for userid column
    for (file_path in all_files) {
      cat("Checking:", file_path, "\n")
      if (check_for_userid_column(file_path)) {
        userid_files <- c(userid_files, file_path)
        cat("  ✓ Contains 'userid' column\n")
      }
    }
  }
}

# Display results
cat("\n=== FILES CONTAINING 'userid' COLUMN ===\n")
if (length(userid_files) > 0) {
  for (file in userid_files) {
    cat("📁", file, "\n")
  }
} else {
  cat("No files found containing 'userid' column\n")
}



#
# Get all the userid files from your search results
userid_files <- c(
  "data/02.5-subspecialists_over_time/end_retrieve_clinician_data_chunk_results.rds",
  "data/02.5-subspecialists_over_time/from_exploratory_subspecialists_over_time.csv",
  "data/02.5-subspecialists_over_time/goba_unrestricted_cleaned.csv",
  "data/02.5-subspecialists_over_time/goba_unrestricted.csv",
  "data/02.5-subspecialists_over_time/nber_all_collected_go_pre_geocode.rds",
  "data/03-search_and_process_npi/input/GOBA_Scrape_subspecialists_only.csv",
  "data/03-search_and_process_npi/intermediate/filtered_subspecialists_only.csv",
  "data/03-search_and_process_npi/unfiltered_subspecialists_only.csv",
  "data/B-nber_nppes_combine_columns/goba_unrestricted_cleaned.csv"
)

# Add all the chunk files
chunk_pattern <- "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results/"
chunk_files <- list.files(chunk_pattern, pattern = "\\.csv$", full.names = TRUE)
userid_files <- c(userid_files, chunk_files)

# Add discovery results
discovery_files <- list.files("physician_data/discovery_results/", pattern = "\\.(csv|rds)$", full.names = TRUE)
userid_files <- c(userid_files, discovery_files)

# Combine all files
combined_data <- map_dfr(userid_files, ~{
  tryCatch({
    # Read file based on extension
    if (tools::file_ext(.x) == "rds") {
      data <- readr::read_rds(.x)
    } else {
      data <- readr::read_csv(.x, show_col_types = FALSE)
    }
    
    # Add metadata
    data %>%
      dplyr::mutate(
        source_file = .x,
        file_date = file.mtime(.x),
        file_size = file.size(.x)
      )
  }, error = function(e) {
    cat("Failed to read:", .x, "\n")
    return(NULL)
  })
})



# Deduplicate by userid, prioritizing subspecialty info and recency
final_subspecialists <- combined_data %>%
  dplyr::group_by(userid) %>%
  dplyr::arrange(
    dplyr::desc(!is.na(sub1) & sub1 != ""),  # Records with subspecialty first
    dplyr::desc(DateTime),                    # Most recent DateTime
    dplyr::desc(file_date)                    # Most recent file
  ) %>%
  dplyr::slice(1) %>%
  dplyr::ungroup()

# Check results
cat("Total records before deduplication:", nrow(combined_data), "\n")
cat("Unique userids after deduplication:", nrow(final_subspecialists), "\n")
cat("Records with subspecialty info:", sum(!is.na(final_subspecialists$sub1) & final_subspecialists$sub1 != ""), "\n")

# Save the consolidated dataset
readr::write_rds(final_subspecialists, "data/03-search_and_process_npi/output/consolidated_subspecialists_all_sources.rds")
readr::write_csv(final_subspecialists, "data/03-search_and_process_npi/output/consolidated_subspecialists_all_sources.csv")

# Consolidate ----
#' Consolidate Subspecialists From Files With `userid` (char-safe)
#'
#' Scans for files with a `userid` column, appends chunk/discovery files,
#' forces all columns to character, adds metadata, builds a sortable
#' datetime, de-dupes by `userid` (prefers sub1, then recency), logs
#' steps, and writes timestamped outputs.
#'
#' @param search_directories Character vector of directories to scan.
#' @param chunk_pattern CSV directory to append (optional).
#' @param discovery_dir Directory with CSV/RDS to append (optional).
#' @param output_dir Directory where outputs are written.
#' @param write_outputs Logical; write timestamped RDS/CSV if TRUE.
#' @param quiet Logical; reduce console chatter if TRUE.
#'
#' @return Tibble with one row per userid (deduplicated).
consolidate_subspecialists_with_userid_charsafe <- function(
    search_directories = c("data/", "."),
    chunk_pattern =
      "data/02.5-subspecialists_over_time/retrieve_clinician_data_chunk_results/",
    discovery_dir = "physician_data/discovery_results/",
    output_dir = "data/03-search_and_process_npi/output",
    write_outputs = TRUE,
    quiet = FALSE
) {
  # ----- logging level ------------------------------------------------------
  if (!quiet) {
    logger::log_threshold(logger::DEBUG)
  } else {
    logger::log_threshold(logger::INFO)
  }
  
  logger::log_info("Starting consolidation (char-safe).")
  
  logger::log_info(
    paste0(
      "Inputs -> search_directories: ",
      paste(search_directories, collapse = ", ")
    )
  )
  logger::log_info(paste0("Inputs -> chunk_pattern: ", chunk_pattern))
  logger::log_info(paste0("Inputs -> discovery_dir: ", discovery_dir))
  logger::log_info(paste0("Inputs -> output_dir: ", output_dir))
  logger::log_info(paste0("Inputs -> write_outputs: ", write_outputs))
  
  # ----- helper: has userid? -----------------------------------------------
  check_for_userid_column <- function(file_path) {
    tryCatch({
      ext <- tolower(tools::file_ext(file_path))
      if (ext == "csv") {
        hdr <- names(
          readr::read_csv(
            file_path,
            n_max = 0,
            show_col_types = FALSE,
            name_repair = "unique"
          )
        )
        return("userid" %in% tolower(hdr))
      } else if (ext == "rds") {
        rds_obj <- readr::read_rds(file_path)
        if (is.data.frame(rds_obj)) {
          return("userid" %in% tolower(names(rds_obj)))
        }
        return(FALSE)
      } else if (ext %in% c("xlsx", "xls")) {
        hdr <- names(readxl::read_excel(file_path, n_max = 0))
        return("userid" %in% tolower(hdr))
      }
      return(FALSE)
    }, error = function(e) {
      logger::log_warn(paste0("Skipping unreadable file: ", file_path))
      return(FALSE)
    })
  }
  
  # ----- helper: read 1 file -> all character + metadata -------------------
  read_one_file_charsafe <- function(path_in) {
    tryCatch({
      if (!file.exists(path_in) || file.size(path_in) == 0) {
        logger::log_warn(paste0("Empty or missing file: ", path_in))
        return(NULL)
      }
      
      ext <- tolower(tools::file_ext(path_in))
      obj <- switch(
        ext,
        "rds" = readr::read_rds(path_in),
        "csv" = suppressMessages(
          readr::read_csv(
            path_in,
            show_col_types = FALSE,
            name_repair = "unique",
            progress = FALSE,
            locale = readr::locale(encoding = "UTF-8"),
            col_types = readr::cols(.default = readr::col_character())
          )
        ),
        "xlsx" = readxl::read_excel(path_in),
        "xls"  = readxl::read_excel(path_in),
        {
          logger::log_debug(paste0("Unsupported extension: ", path_in))
          return(NULL)
        }
      )
      
      if (!is.data.frame(obj)) {
        logger::log_debug(paste0("Non-data.frame skipped: ", path_in))
        return(NULL)
      }
      
      # Standardize `userid` name if present in any case mix
      lower_names <- tolower(names(obj))
      if ("userid" %in% lower_names && !"userid" %in% names(obj)) {
        idx <- which(lower_names == "userid")[1]
        names(obj)[idx] <- "userid"
      }
      
      # Force every column to character (covers RDS/Excel)
      obj <- obj |>
        dplyr::mutate(
          dplyr::across(dplyr::everything(), ~ as.character(.))
        )
      
      # Create sortable datetime safely (files may lack DateTime)
      lower_names <- tolower(names(obj))
      if ("datetime" %in% lower_names && !"DateTime" %in% names(obj)) {
        idx_dt <- which(lower_names == "datetime")[1]
        names(obj)[idx_dt] <- "DateTime"
      }
      
      if ("DateTime" %in% names(obj)) {
        obj <- obj |>
          dplyr::mutate(
            DateTime_sortable = suppressWarnings(
              lubridate::ymd_hms(.data[["DateTime"]],
                                 tz = "UTC", quiet = TRUE)
            )
          )
        logger::log_debug(paste0("Harmonized DateTime for: ", path_in))
      } else {
        obj <- obj |>
          dplyr::mutate(
            DateTime_sortable = as.POSIXct(NA, tz = "UTC")
          )
        logger::log_debug(
          paste0("No DateTime column; set sortable NA: ", path_in)
        )
      }
      
      # Attach metadata as character to avoid clashes
      obj |>
        dplyr::mutate(
          source_file = path_in,
          file_date = as.character(file.mtime(path_in)),
          file_size = as.character(file.size(path_in))
        )
    }, error = function(e) {
      logger::log_warn(
        paste0("Failed to read: ", path_in, " -> ",
               conditionMessage(e))
      )
      return(NULL)
    })
  }
  
  # ----- discover candidate files ------------------------------------------
  candidate_paths <- character(0)
  
  for (dir_path in search_directories) {
    if (dir.exists(dir_path)) {
      files_found <- list.files(
        dir_path,
        pattern = "\\.(csv|rds|xlsx|xls)$",
        recursive = TRUE,
        full.names = TRUE,
        ignore.case = TRUE
      )
      logger::log_info(
        paste0("Scanning dir: ", dir_path, " (",
               length(files_found), " files)")
      )
      
      for (fp in files_found) {
        if (check_for_userid_column(fp)) {
          candidate_paths <- c(candidate_paths, fp)
          logger::log_debug(paste0("  \u2713 userid in: ", fp))
        }
      }
    } else {
      logger::log_warn(paste0("Directory does not exist: ", dir_path))
    }
  }
  
  logger::log_info(
    paste0("Files with `userid`: ",
           scales::comma(length(candidate_paths)))
  )
  
  # ----- append chunk CSVs --------------------------------------------------
  if (!is.null(chunk_pattern) && dir.exists(chunk_pattern)) {
    chunk_csvs <- list.files(
      chunk_pattern,
      pattern = "\\.csv$",
      recursive = TRUE,
      full.names = TRUE
    )
    logger::log_info(
      paste0("Chunk CSVs found: ",
             scales::comma(length(chunk_csvs)))
    )
    candidate_paths <- c(candidate_paths, chunk_csvs)
  } else {
    logger::log_info("No chunk_pattern dir or NULL. Skipping.")
  }
  
  # ----- append discovery files --------------------------------------------
  if (!is.null(discovery_dir) && dir.exists(discovery_dir)) {
    discovery <- list.files(
      discovery_dir,
      pattern = "\\.(csv|rds)$",
      recursive = FALSE,
      full.names = TRUE
    )
    logger::log_info(
      paste0("Discovery files: ",
             scales::comma(length(discovery)))
    )
    candidate_paths <- c(candidate_paths, discovery)
  } else {
    logger::log_info("No discovery_dir or missing. Skipping.")
  }
  
  candidate_paths <- unique(candidate_paths)
  candidate_paths <- candidate_paths[file.exists(candidate_paths)]
  candidate_paths <- fs::path_norm(candidate_paths)
  
  logger::log_info(
    paste0("Total files queued: ",
           scales::comma(length(candidate_paths)))
  )
  
  if (length(candidate_paths) == 0) {
    logger::log_error("No files to process. Returning empty tibble.")
    if (requireNamespace("beepr", quietly = TRUE)) beepr::beep(2)
    return(tibble::tibble())
  }
  
  if (requireNamespace("beepr", quietly = TRUE)) beepr::beep(1)
  
  # ----- combine (all character) -------------------------------------------
  combined_stack <- purrr::map_dfr(
    candidate_paths,
    read_one_file_charsafe
  )
  
  logger::log_info(
    paste0("Rows after combine: ",
           scales::comma(nrow(combined_stack)))
  )
  logger::log_info(
    paste0("Cols after combine: ",
           scales::comma(ncol(combined_stack)))
  )
  
  # ----- dedupe by userid with priorities ----------------------------------
  if (!"userid" %in% names(combined_stack)) {
    logger::log_error("No `userid` column after combine. Returning.")
    if (requireNamespace("beepr", quietly = TRUE)) beepr::beep(2)
    return(combined_stack)
  }
  
  combined_stack <- combined_stack |>
    dplyr::mutate(
      .has_sub1 = dplyr::if_else(
        "sub1" %in% names(combined_stack) &
          !is.na(.data$sub1) & .data$sub1 != "",
        "1", "0"
      )
    )
  
  arranged_stack <- combined_stack |>
    dplyr::group_by(.data$userid) |>
    dplyr::arrange(
      dplyr::desc(.data$.has_sub1),
      dplyr::desc(.data$DateTime_sortable),
      dplyr::desc(as.POSIXct(.data$file_date, tz = "UTC")),
      .by_group = TRUE
    )
  
  dedup_subspecialists <- arranged_stack |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-.has_sub1)
  
  # ----- summary logs -------------------------------------------------------
  total_rows <- nrow(combined_stack)
  unique_ids <- dplyr::n_distinct(combined_stack$userid)
  kept_rows <- nrow(dedup_subspecialists)
  with_sub1 <- if ("sub1" %in% names(dedup_subspecialists)) {
    sum(!is.na(dedup_subspecialists$sub1) &
          dedup_subspecialists$sub1 != "")
  } else {
    0L
  }
  
  logger::log_info(
    paste0("Total rows combined: ", scales::comma(total_rows))
  )
  logger::log_info(
    paste0("Unique userids seen: ", scales::comma(unique_ids))
  )
  logger::log_info(
    paste0("Unique userids kept: ", scales::comma(kept_rows))
  )
  logger::log_info(
    paste0("Kept with sub1: ", scales::comma(with_sub1))
  )
  
  # ----- write outputs (timestamped) ---------------------------------------
  if (isTRUE(write_outputs)) {
    if (!dir.exists(output_dir)) {
      fs::dir_create(output_dir, recurse = TRUE)
      logger::log_info(paste0("Created output_dir: ", output_dir))
    }
    stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    rds_path <- file.path(
      output_dir,
      paste0(
        "consolidated_subspecialists_all_sources_charsafe_",
        stamp, ".rds"
      )
    )
    csv_path <- file.path(
      output_dir,
      paste0(
        "consolidated_subspecialists_all_sources_charsafe_",
        stamp, ".csv"
      )
    )
    readr::write_rds(dedup_subspecialists, rds_path)
    readr::write_csv(dedup_subspecialists, csv_path)
    logger::log_info(paste0("Saved RDS -> ", rds_path))
    logger::log_info(paste0("Saved CSV -> ", csv_path))
  } else {
    logger::log_info("write_outputs = FALSE. Skipping file writes.")
  }
  
  if (requireNamespace("beepr", quietly = TRUE)) beepr::beep(2)
  logger::log_info("Consolidation complete (char-safe).")
  return(dedup_subspecialists)
}



consolidated <- consolidate_subspecialists_with_userid_charsafe(
  search_directories = c(
    "data/", 
    ".", 
    "~/Dropbox (Personal)/workforce"  # Added workforce dir
  ),
  chunk_pattern = "data/02.5_subspecialists_over_time/retrieve_clinician_data_chunk_results/",
  discovery_dir = "physician_data/discovery_results/",
  output_dir = "data/temp",
  write_outputs = TRUE
)


consolidated$userid <- as.numeric(consolidated$userid) %>%
  dplyr::arrange(userid)

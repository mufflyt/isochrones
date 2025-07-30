#' Add retirement dates to NPI dataset
#'
#' This function merges a manually curated CSV of known retirement dates
#' and/or uses NPPES "deactivation" flag to infer physician retirement.
#'
#' @param npi_dataset A data frame with at least a column `NPI`
#' @param retirement_csv_path Optional path to CSV with columns `NPI` and
#'   `retirement_date`. Defaults to "~/retirement_dates.csv".
#' @param use_nppes_deactivation If TRUE, look for column `deactivated_flag`
#'   and infer retirement from it.
#'
#' @return A data frame with new column `retirement_date` and logical
#'   `is_retired` column.
#' @export
add_retirement_dates_to_npi_dataset <- function(npi_dataset,
                                                retirement_csv_path = "~/retirement_dates.csv",
                                                use_nppes_deactivation = TRUE) {
  logger::log_info("Starting add_retirement_dates_to_npi_dataset()")
  logger::log_info("Inputs: {nrow(npi_dataset)} rows in NPI dataset")
  logger::log_info("CSV path: {retirement_csv_path}")
  logger::log_info("Use NPPES deactivation: {use_nppes_deactivation}")

  stopifnot("NPI" %in% names(npi_dataset))

  if (file.exists(retirement_csv_path)) {
    logger::log_info("Reading retirement CSV from: {retirement_csv_path}")
    retirement_table <- readr::read_csv(
      retirement_csv_path,
      col_types = readr::cols(
        NPI = readr::col_character(),
        retirement_date = readr::col_date()
      )
    ) %>%
      dplyr::distinct(NPI, .keep_all = TRUE)

    logger::log_info("Retirement CSV loaded with {nrow(retirement_table)} rows")

    npi_dataset <- dplyr::left_join(npi_dataset, retirement_table, by = "NPI")
    logger::log_info("Joined retirement CSV to NPI dataset")
  } else {
    logger::log_warn("Retirement CSV not found at path: {retirement_csv_path}")
    npi_dataset <- dplyr::mutate(npi_dataset, retirement_date = as.Date(NA))
  }

  if (use_nppes_deactivation && "deactivated_flag" %in% names(npi_dataset)) {
    logger::log_info("Processing NPPES deactivation flag")
    npi_dataset <- npi_dataset %>%
      dplyr::mutate(
        inferred_retirement = dplyr::case_when(
          is.na(retirement_date) & deactivated_flag == "Y" ~ Sys.Date(),
          TRUE ~ retirement_date
        ),
        retirement_date = inferred_retirement
      ) %>%
      dplyr::select(-inferred_retirement)
  }

  npi_dataset <- npi_dataset %>%
    dplyr::mutate(
      is_retired = !is.na(retirement_date) & retirement_date <= Sys.Date()
    )

  logger::log_info("Added is_retired flag. Total retired: {sum(npi_dataset$is_retired, na.rm = TRUE)}")

  logger::log_info("Finished add_retirement_dates_to_npi_dataset()")
  beepr::beep(2)

  npi_dataset
}

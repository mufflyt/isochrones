#' Create a mapping between NPPES table names and years
#'
#' This helper scans the connected database for NPPES tables and
#' extracts the year embedded in each table name.
#'
#' @param con A `DBI` connection to the NPPES database.
#'
#' @return A data frame with columns `table_name` and `year`.
#' @examples
#' # con <- DBI::dbConnect(RSQLite::SQLite(), "nppes.sqlite")
#' # create_nppes_table_mapping(con)
#' @export
create_nppes_table_mapping <- function(con) {
  all_tables <- DBI::dbListTables(con)
  nppes_tables <- all_tables[grepl("npidata|NPPES_Data_Dissemination", all_tables, ignore.case = TRUE)]
  mapping_df <- data.frame(table_name = character(), year = integer(), stringsAsFactors = FALSE)
  for (table in nppes_tables) {
    year_matches <- regmatches(table, gregexpr("20[0-9]{2}", table))[[1]]
    if (length(year_matches) > 0) {
      year <- as.integer(year_matches[length(year_matches)])
      mapping_df <- rbind(mapping_df, data.frame(table_name = table, year = year, stringsAsFactors = FALSE))
    }
  }
  mapping_df[order(mapping_df$year), ]
}

#' Query physician data across multiple years
#'
#' Retrieves provider information for the supplied taxonomy codes
#' across the NPPES tables in `table_year_mapping`.
#'
#' @param con A `DBI` connection.
#' @param table_year_mapping Data frame from `create_nppes_table_mapping`.
#' @param taxonomy_codes Character vector of taxonomy codes to filter on.
#' @param years_to_include Optional numeric vector of years to include.
#'
#' @return A tibble with provider data.
#' @examples
#' # mapping <- create_nppes_table_mapping(con)
#' # find_physicians_across_years(con, mapping, c("207V00000X"))
#' @export
find_physicians_across_years <- function(con, table_year_mapping, taxonomy_codes,
                                         years_to_include = NULL) {
  assertthat::assert_that(DBI::dbIsValid(con))
  if (!is.null(years_to_include)) {
    table_year_mapping <- table_year_mapping[table_year_mapping$year %in% years_to_include, , drop = FALSE]
  }
  results <- lapply(seq_len(nrow(table_year_mapping)), function(i) {
    tbl <- table_year_mapping$table_name[i]
    yr <- table_year_mapping$year[i]
    cols <- DBI::dbListFields(con, tbl)
    tax_cols <- grep("Healthcare Provider Taxonomy Code_[0-9]+", cols, value = TRUE)
    if (length(tax_cols) == 0) return(NULL)
    filter_sql <- paste(sapply(tax_cols, function(cl) sprintf('"%s" IN (\'%s\')', cl, paste(taxonomy_codes, collapse = "','"))), collapse = ' OR ')
    sql <- sprintf('SELECT *, %d AS Year FROM "%s" WHERE (%s) AND "Entity Type Code" = 1', yr, tbl, filter_sql)
    dat <- DBI::dbGetQuery(con, sql)
    if (nrow(dat) > 0 && "Provider Business Practice Location Address Postal Code" %in% names(dat)) {
      dat$Zip <- stringr::str_sub(dat$`Provider Business Practice Location Address Postal Code`, 1, 5)
    }
    dat
  })
  tibble::as_tibble(do.call(rbind, results))
}

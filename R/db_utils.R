#' Connect to a DuckDB database
#'
#' This helper wraps `DBI::dbConnect` and exposes a simple interface for
#' establishing a connection. The user must manage the returned connection
#' object and call [DBI::dbDisconnect()] when finished.
#'
#' @param db_path Path to the DuckDB database file.
#' @param read_only Logical, open the database in read-only mode? Default `FALSE`.
#'
#' @return A `DBIConnection` object.
#' @export
connect_duckdb <- function(db_path, read_only = FALSE) {
  logger::log_info("Connecting to DuckDB at {db_path}")
  tryCatch({
    connection <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path,
                                  read_only = read_only)
    logger::log_success("Successfully connected to DuckDB")
    connection
  }, error = function(e) {
    logger::log_error("Failed to connect to DuckDB: {e$message}")
    stop("Failed to connect to DuckDB database", call. = FALSE)
  })
}


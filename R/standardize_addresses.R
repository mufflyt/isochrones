#' Standardize addresses using the postmastr package
#'
#' This helper parses U.S. street addresses with the `postmastr` package and
#' reconstructs a standardized address string suitable for geocoding or
#' matching.
#'
#' @param df A data frame containing an address column.
#' @param address_col Name of the address column. Defaults to "address".
#' @param city_dictionary_path Optional path to a postmastr city dictionary. The
#'   repository provides one at `data/05-geocode-cleaning/city.rds`.
#'
#' @return The original data frame with a new column `pm.address_std` containing
#'   the standardized address.
#' @examples
#' \dontrun{
#'   clean_df <- standardize_addresses(df)
#' }
#' @export
standardize_addresses <- function(df,
                                  address_col = "address",
                                  city_dictionary_path =
                                    "data/05-geocode-cleaning/city.rds") {
  assertthat::assert_that(is.data.frame(df),
                          msg = "df must be a data frame")
  assertthat::assert_that(assertthat::is.string(address_col),
                          msg = "address_col must be a single string")
  assertthat::assert_that(address_col %in% names(df),
                          msg = paste("address column", address_col,
                                      "not found in data frame"))
  assertthat::assert_that(
    is.null(city_dictionary_path) ||
      assertthat::is.string(city_dictionary_path),
    msg = "city_dictionary_path must be NULL or a single string"
  )
  assertthat::assert_that(
    is.null(city_dictionary_path) || file.exists(city_dictionary_path),
    msg = paste("City dictionary not found at", city_dictionary_path)
  )

  addr_sym <- rlang::sym(address_col)

  parsed <- df %>%
    dplyr::rename(pm.address = !!addr_sym) %>%
    postmastr::pm_identify(var = "pm.address") %>%
    postmastr::pm_prep(var = "pm.address", type = "street") %>%
    dplyr::mutate(pm.address = stringr::str_squish(pm.address)) %>%
    postmastr::pm_postal_parse()

  state_dict <- postmastr::pm_dictionary(
    locale = "us", type = "state", case = c("title", "upper")
  )
  parsed <- postmastr::pm_state_parse(parsed, dictionary = state_dict)

  if (!is.null(city_dictionary_path) && file.exists(city_dictionary_path)) {
    city_dict <- readr::read_rds(city_dictionary_path)
    parsed <- postmastr::pm_city_parse(parsed, dictionary = city_dict,
                                       locale = "us")
  }

  parsed <- parsed %>%
    postmastr::pm_house_parse(locale = "us") %>%
    postmastr::pm_streetSuf_parse(locale = "us") %>%
    postmastr::pm_streetDir_parse(locale = "us") %>%
    postmastr::pm_street_parse(ordinal = TRUE, drop = TRUE, locale = "us") %>%
    postmastr::pm_reconstruct()

  df$pm.address_std <- parsed$pm.address
  df
}

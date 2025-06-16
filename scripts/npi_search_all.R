npi_search_all <- function(taxonomy_description,
                           enumeration_type = NULL,
                           first_name = NULL,
                           last_name = NULL,
                           organization_name = NULL,
                           state = NULL,
                           city = NULL,
                           postal_code = NULL,
                           country_code = NULL) {
  base_url <- "https://npiregistry.cms.hhs.gov/api/"
  params <- list(version = "2.1",
                 taxonomy_description = taxonomy_description,
                 enumeration_type = enumeration_type,
                 first_name = first_name,
                 last_name = last_name,
                 organization_name = organization_name,
                 state = state,
                 city = city,
                 postal_code = postal_code,
                 country_code = country_code)
  limit <- 200
  skip <- 0
  all_results <- list()
  repeat {
    params$limit <- limit
    params$skip <- skip
    resp <- httr::GET(base_url, query = params)
    httr::stop_for_status(resp)
    data <- httr::content(resp, as = "parsed", type = "application/json")
    if (is.null(data$results)) break
    all_results <- append(all_results, data$results)
    skip <- skip + limit
    if (length(all_results) >= data$result_count) break
  }
  tibble::as_tibble(all_results)
}

# Example usage: search for all Obstetricians and Gynecologists
# results <- npi_search_all(taxonomy_description = "Obstetrics & Gynecology")

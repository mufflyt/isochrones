# Main function: Calculate Aggregated Population Counts in Isochrones by Race/Ethnicity
#' @title Calculate Aggregated Population Counts in Isochrones by Race/Ethnicity
#' @description Aggregates population counts (e.g., ACS data) by race/ethnicity based on spatial overlap
#' with unified isochrones for a specified year and travel time. Outputs include detailed metadata and
#' population insights, including uncovered populations and coverage percentages.
#' @param population_data A tibble with columns:
#'   \itemize{
#'     \item \code{id}: A unique identifier for each population area (e.g., county).
#'     \item \code{population}: The aggregated population count for the area.
#'     \item \code{race_ethnicity}: A character column indicating the racial/ethnic group.
#'     \item \code{geometry}: The geographic boundary of the area (sf object).
#'     \item \code{year}: The year associated with the population data.
#'   }
#'   This must be an \code{sf} object.
#' @param isochrone_geometries A tibble with columns:
#'   \itemize{
#'     \item \code{id}: A unique identifier for each isochrone.
#'     \item \code{year}: The year associated with the isochrone.
#'     \item \code{travel_time}: The travel time in minutes for the isochrone.
#'     \item \code{wkt}: The polygon geometry of the isochrone in WKT format.
#'   }
#' @param year An integer representing the year to filter the data (e.g., 2013, 2014). This is mandatory.
#' @param travel_time A numeric value specifying the travel time to filter isochrones (e.g., 30, 60, 120, 180)
#'   or \code{NULL} to include all travel times. Default is \code{NULL}.
#' @return A tibble with population aggregates by race/ethnicity and additional metadata.
#' @importFrom sf st_as_sf st_join st_intersection st_area st_union
#' @importFrom dplyr filter summarize group_by mutate n_distinct
#' @importFrom tibble tibble
#' @export
calculate_population_in_isochrones_by_race <- function(
    population_data,
    isochrone_geometries,
    year,
    travel_time = NULL
) {
  logger::log_info("Starting the calculation of aggregated population by race/ethnicity in unified isochrones.")

  # Validate inputs
  if (missing(year) || is.null(year)) stop("The 'year' parameter is mandatory and must be specified.")
  if (!inherits(population_data, "sf")) stop("`population_data` must be an sf object.")
  if (!all(c("id", "population", "race_ethnicity", "geometry", "year") %in% names(population_data))) {
    stop("`population_data` must contain columns: 'id', 'population', 'race_ethnicity', 'geometry', and 'year'.")
  }
  valid_travel_times <- c(30, 60, 120, 180)
  if (!is.null(travel_time) && !travel_time %in% valid_travel_times) {
    stop("Invalid travel_time. Must be one of: 30, 60, 120, 180, or NULL for all times.")
  }

  # Memoized helper function to cache results
  cached_intersection <- memoise::memoise(function(population_data, isochrones) {
    sf::st_intersection(population_data, isochrones)
  })

  # Filter population data by year
  logger::log_info("Filtering population data for year: {year}.")
  population_data_filtered <- dplyr::filter(population_data, year == !!year)

  # Filter isochrones by year and travel_time
  logger::log_info("Filtering isochrones by year and travel time.")
  filtered_isochrones <- dplyr::filter(isochrone_geometries, year == !!year)
  if (!is.null(travel_time)) {
    filtered_isochrones <- dplyr::filter(filtered_isochrones, travel_time == !!travel_time)
  }

  # Convert isochrones to sf object and unify overlapping polygons
  isochrones_sf <- convert_to_sf_polygons(filtered_isochrones, wkt_col = "wkt")
  logger::log_info("Unifying overlapping isochrones to prevent double-counting populations.")
  unified_isochrones <- sf::st_union(isochrones_sf)

  # Perform spatial intersection using memoized function
  logger::log_info("Performing spatial intersection to aggregate population.")
  intersected <- cached_intersection(population_data_filtered, unified_isochrones)

  # Check for empty intersections
  if (nrow(intersected) == 0) {
    logger::log_warn("No intersections found between population data and isochrones.")
    return(generate_empty_results(year, travel_time, isochrones_sf, unified_isochrones))
  }

  # Calculate proportional population counts
  intersected <- intersected %>%
    dplyr::mutate(
      overlap_area = sf::st_area(geometry),
      total_area = sf::st_area(sf::st_geometry(population_data_filtered[match(id, population_data_filtered$id), ])),
      proportion = as.numeric(overlap_area / total_area),
      adjusted_population = population * proportion
    )

  # Aggregate population counts by race/ethnicity
  logger::log_info("Aggregating population counts by race/ethnicity.")
  aggregated_population <- intersected %>%
    dplyr::group_by(race_ethnicity) %>%
    dplyr::summarize(
      total_population = sum(adjusted_population, na.rm = TRUE),
      uncovered_population = sum(population, na.rm = TRUE) - total_population,
      coverage_percentage = (total_population / sum(population, na.rm = TRUE)) * 100,
      num_population_areas = dplyr::n_distinct(id),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      num_isochrones = nrow(filtered_isochrones),
      total_isochrone_area = sf::st_area(unified_isochrones) %>% sum(),
      largest_isochrone_area = sf::st_area(isochrones_sf) %>% max(),
      smallest_isochrone_area = sf::st_area(isochrones_sf) %>% min(),
      year = year,
      travel_time = ifelse(is.null(travel_time), "all", as.character(travel_time)),
      timestamp = Sys.time()
    )

  logger::log_info("Completed aggregation of population counts by race/ethnicity.")
  return(aggregated_population)
}

# Helper function: Convert a data frame with WKT to sf polygons
#' @noRd
convert_to_sf_polygons <- function(polygon_data, wkt_col, crs = 4326) {
  logger::log_info("Converting polygon data to sf object.")
  sf_object <- sf::st_as_sf(polygon_data, wkt = wkt_col, crs = crs)
  logger::log_debug("Converted to sf object with {nrow(sf_object)} rows.")
  return(sf_object)
}

# Helper function: Generate empty results
#' @noRd
generate_empty_results <- function(year, travel_time, isochrones_sf, unified_isochrones) {
  tibble::tibble(
    race_ethnicity = character(),
    total_population = numeric(),
    uncovered_population = numeric(),
    coverage_percentage = numeric(),
    num_population_areas = integer(),
    num_isochrones = nrow(isochrones_sf),
    total_isochrone_area = sf::st_area(unified_isochrones) %>% sum(),
    largest_isochrone_area = sf::st_area(isochrones_sf) %>% max(),
    smallest_isochrone_area = sf::st_area(isochrones_sf) %>% min(),
    year = year,
    travel_time = ifelse(is.null(travel_time), "all", as.character(travel_time)),
    timestamp = Sys.time()
  )
}

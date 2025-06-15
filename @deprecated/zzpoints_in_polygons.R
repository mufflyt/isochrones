# Load setup script
#######################
source("R/01-setup.R")

#' @title Fetch Population Data
#' @description Fetches population data for specified states, years, and race/ethnicity variables from the ACS.
#' The function validates inputs, logs progress at various stages, and ensures results are consistent and reliable.
#' @param state A character vector of state abbreviations (e.g., `c("CO", "CA")`) or a single state abbreviation.
#'   Default is `c("CO")`.
#' @param years A numeric vector specifying the years to fetch data for (e.g., `2015:2022`). Default is `2022`.
#' @param race_vars A named character vector mapping race/ethnicity names to variable codes (e.g.,
#'   `c("White" = "B02001_002E")`). Default includes common race categories from ACS.
#' @return A tibble with population data including columns:
#'   \describe{
#'     \item{id}{Unique identifier for each geographic area (GEOID).}
#'     \item{population}{Estimated population for the given year, state, and race/ethnicity.}
#'     \item{race_ethnicity}{Race/ethnicity category for the population estimate.}
#'     \item{geometry}{Geographic boundaries of the area (sf object).}
#'     \item{year}{Year for the population estimate.}
#'   }
#' @importFrom tidycensus get_acs
#' @importFrom dplyr mutate select recode
#' @importFrom purrr map_dfr
#' @importFrom logger log_info log_debug log_error log_warn
#' @importFrom sf st_geometry
#' @export
#' @examples
#' # Example 1: Default behavior (Colorado, year 2022, default race variables)
#' population_tibble <- fetch_population_data()
#' print(population_tibble)
#'
#' # Example 2: Fetch data for multiple states and years
#' race_vars <- c("White" = "B02001_002E", "Black" = "B02001_003E")
#' multi_state_population <- fetch_population_data(state = c("CO", "CA"), years = 2015:2020,
#'                                                 race_vars = race_vars)
#' print(multi_state_population)
#'
#' # Example 3: Use a custom set of race variables
#' custom_race_vars <- c("Asian" = "B02001_005E", "Hispanic" = "B03003_003E")
#' custom_population <- fetch_population_data(state = "TX", years = 2018, race_vars = custom_race_vars)
#' print(custom_population)
fetch_population_data <- function(state = c("CO"),
                                  years = 2022,
                                  race_vars = c(
                                    "White" = "B02001_002E",
                                    "Black" = "B02001_003E",
                                    "Asian" = "B02001_005E",
                                    "Hispanic" = "B03003_003E"
                                  )) {
  logger::log_info("Starting `fetch_population_data` function.")
  
  # Log inputs
  log_inputs(state, years, race_vars)
  
  # Validate inputs
  validate_inputs(state, years, race_vars)
  
  # Fetch population data for all years
  logger::log_info("Fetching population data from ACS...")
  population_by_race_and_year <- purrr::map_dfr(years, function(year) {
    fetch_data_for_year(state, year, race_vars)
  })
  
  # Log outputs
  log_outputs(population_by_race_and_year)
  
  return(population_by_race_and_year)
}

# Helper Functions ------------------------------------------------------

#' @noRd
log_inputs <- function(state, years, race_vars) {
  logger::log_info("Inputs to the function:")
  logger::log_info("States: {paste(state, collapse = ', ')}")
  logger::log_info("Years: {paste(years, collapse = ', ')}")
  logger::log_info("Race/Ethnicity Variables: {paste(names(race_vars), collapse = ', ')}")
}

#' @noRd
validate_inputs <- function(state, years, race_vars) {
  if (missing(state) || missing(years) || missing(race_vars)) {
    logger::log_error("One or more required inputs (`state`, `years`, `race_vars`) are missing.")
    stop("Missing required inputs: `state`, `years`, and/or `race_vars`.")
  }
  
  if (!is.character(state)) {
    logger::log_error("`state` must be a character vector of state abbreviations.")
    stop("`state` must be a character vector of state abbreviations.")
  }
  
  if (!is.numeric(years)) {
    logger::log_error("`years` must be a numeric vector.")
    stop("`years` must be a numeric vector.")
  }
  
  if (!is.character(race_vars) || !length(names(race_vars))) {
    logger::log_error("`race_vars` must be a named character vector.")
    stop("`race_vars` must be a named character vector.")
  }
}

#' @noRd
fetch_data_for_year <- function(state, year, race_vars) {
  logger::log_info("Fetching data for year: {year}")
  tidycensus::get_acs(
    geography = "county",
    variables = race_vars,
    state = state,
    year = year,
    geometry = TRUE
  ) %>%
    dplyr::mutate(
      race_ethnicity = dplyr::recode(variable, !!!race_vars),  # Map variable codes to race/ethnicity
      id = GEOID,                                              # Use GEOID as a unique identifier
      population = estimate,                                   # Use the estimate column for population
      year = year                                              # Add the year column
    ) %>%
    dplyr::select(id, population, race_ethnicity, geometry, year) %>%
    {
      logger::log_debug("Fetched {nrow(.)} rows of data for year {year}.")
      .
    }
}

#' @noRd
log_outputs <- function(population_data) {
  logger::log_info("Successfully fetched population data.")
  logger::log_info("Output contains {nrow(population_data)} rows and {ncol(population_data)} columns.")
  logger::log_debug("Output columns: {paste(colnames(population_data), collapse = ', ')}")
}


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



#######################################
# Load setup script and necessary libraries
source("R/01-setup.R")
library(tidycensus)
library(dplyr)
library(purrr)
library(sf)
library(tigris)
library(ggplot2)
library(mapview)

#--------------------------------------------------
# Step 1: Set Constants and Fetch Population Data
#--------------------------------------------------

# Define constants for states, years, and race variables
state_list <- c(state.abb, "PR")  # All states and Puerto Rico
years_range <- 2013:2022          # Year range for analysis
race_variables <- c(
  "White" = "B02001_002E",
  "Black" = "B02001_003E",
  "Native_American" = "B02001_004E",
  "Asian" = "B02001_005E",
  "Pacific_Islander" = "B02001_006E",
  "Total_Female_By_Race" = "B02001_001E"
)

# Fetch population data for all states and years
message("Fetching population data...")
population_sf <- fetch_population_data(
  state = state_list,
  years = years_range,
  race_vars = race_variables
)
# Output contains 193,170 rows and 5 columns.

# Validate geometries
message("Validating population geometries...")
population_sf <- sf::st_make_valid(population_sf)

#--------------------------------------------------
# Step 2: Load and Prepare Isochrone Geometries
#--------------------------------------------------

# Load isochrone shapefile and transform attributes
isochrone_path <- "data/06-isochrones/20241013161700.shp"
message("Loading and preparing isochrone geometries...")
isochrone_sf <- sf::read_sf(isochrone_path) %>%
  dplyr::mutate(
    travel_time_minutes = range / 60,         # Convert range from seconds to minutes
    year = lubridate::year(departure)         # Extract year from departure
  ) %>%
  dplyr::filter(travel_time_minutes %in% c(30, 60, 120, 180))

# Align CRS for distance calculations (EPSG:3857)
message("Aligning CRS for spatial operations...")
population_sf <- sf::st_transform(population_sf, 3857)
isochrone_sf <- sf::st_transform(isochrone_sf, 3857)

#--------------------------------------------------
# Step 3: Simplify and Validate Geometries
#--------------------------------------------------

# Function to simplify geometries and fix self-intersections
simplify_geometries <- function(sf_object, tolerance = 100) {
  sf_object %>%
    dplyr::mutate(
      geometry = sf::st_simplify(geometry, dTolerance = tolerance),
      geometry = sf::st_buffer(geometry, dist = 0)  # Fix intersections
    ) %>%
    sf::st_make_valid()
}

# Simplify geometries for population and isochrones
message("Simplifying geometries...")
population_sf <- simplify_geometries(population_sf)
isochrone_sf <- simplify_geometries(isochrone_sf)

# Ensure CRS consistency
if (!sf::st_crs(population_sf) == sf::st_crs(isochrone_sf)) {
  stop("CRS mismatch between population data and isochrones.")
}

#--------------------------------------------------
# Step 4: Validate Data
#--------------------------------------------------

# Check validity of geometries
message("Checking geometry validity...")
valid_population <- sf::st_is_valid(population_sf)
valid_isochrones <- sf::st_is_valid(isochrone_sf)

cat("Population data validity:", table(valid_population), "\n")
cat("Isochrone geometries validity:", table(valid_isochrones), "\n")

#--------------------------------------------------
# Step 5: Run Population in Isochrone Calculation
#--------------------------------------------------

message("Running population aggregation by isochrones...")
population_results <- tyler::calculate_population_in_isochrones_by_race(
  population_data = population_sf,
  isochrone_geometries = isochrone_sf,
  year = 2015:2020,
  travel_time = 60
)

# Recode race/ethnicity labels
race_labels <- c(
  "B02001_001" = "Total Female Population by Race",
  "B02001_002" = "White",
  "B02001_003" = "Black or African American",
  "B02001_004" = "American Indian and Alaska Native",
  "B02001_005" = "Asian",
  "B02001_006" = "Native Hawaiian and Other Pacific Islander"
)

population_results <- population_results %>%
  dplyr::mutate(race_ethnicity = dplyr::recode(race_ethnicity, !!!race_labels))

#--------------------------------------------------
# Step 6: Visualization
#--------------------------------------------------

# Retrieve state boundaries
states_sf <- tigris::states(cb = TRUE) %>%
  sf::st_transform(crs = sf::st_crs(population_results))

# Map visualization using ggplot2
ggplot(data = population_results) +
  geom_sf(aes(fill = coverage_percentage), color = "black", size = 0.1) +
  geom_sf(data = states_sf, fill = NA, color = "red", size = 0.5) +
  facet_wrap(~ race_ethnicity) +
  scale_fill_viridis_c(option = "C") +
  theme_minimal() +
  labs(
    title = "Population Coverage by Isochrone",
    fill = "Coverage (%)",
    caption = "Source: ACS Data and HERE Isochrone Geometries"
  )

# Interactive visualization using mapview
isochrones_map <- mapview(population_results, zcol = "coverage_percentage", layer.name = "Coverage (%)")
interactive_map <- isochrones_map + mapview(states_sf, color = "red", alpha.regions = 0)
interactive_map
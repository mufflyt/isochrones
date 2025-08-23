
# Fancy map ----
#' Create Interactive Map of Physician Locations
#'
#' This function creates an interactive map displaying physician locations with
#' various visualization options including heatmaps, clustering, and credential-
#' based color coding. The function can accept either a regular data frame with
#' coordinate columns or an sf spatial object.
#'
#' @param physician_geodata A data.frame or sf object containing physician 
#'   location data. If data.frame, must contain latitude and longitude columns.
#' @param output_file_path Character string specifying the path where the HTML 
#'   map should be saved. If NULL (default), the map is displayed in viewer.
#' @param latitude_col Character string specifying the name of the latitude 
#'   column. Default is "latitude".
#' @param longitude_col Character string specifying the name of the longitude 
#'   column. Default is "longitude".
#' @param credential_col Character string specifying the name of the credential 
#'   column for color coding. Default is "credential".
#' @param credential_colors Named vector of colors for different credentials. 
#'   Default includes MD, DO, and Other.
#' @param base_map_types Character vector of base map options to include. 
#'   Default includes OpenStreetMap, CartoDB Light, and Satellite.
#' @param include_heatmap Logical indicating whether to include a heatmap layer. 
#'   Default is TRUE.
#' @param cluster_markers Logical indicating whether to cluster nearby markers. 
#'   Default is TRUE.
#' @param popup_cols Character vector of column names to include in marker 
#'   popups. If NULL, uses first 5 non-coordinate columns.
#' @param map_title Character string for the map title. Default is 
#'   "Physician Locations".
#' @param verbose Logical indicating whether to display detailed logging. 
#'   Default is FALSE.
#'
#' @return A leaflet map object that can be displayed or saved
#'
#' @examples
#' # Example 1: Basic usage with data frame containing coordinates
#' physician_sample_data <- data.frame(
#'   physician_name = c("Dr. Smith", "Dr. Jones", "Dr. Brown"),
#'   latitude = c(40.7128, 34.0522, 41.8781),
#'   longitude = c(-74.0060, -118.2437, -87.6298),
#'   credential = c("MD", "DO", "MD"),
#'   specialty = c("Cardiology", "Family Medicine", "Neurology"),
#'   stringsAsFactors = FALSE
#' )
#' 
#' physician_map_basic <- create_physician_map(
#'   physician_geodata = physician_sample_data,
#'   output_file_path = NULL,
#'   latitude_col = "latitude",
#'   longitude_col = "longitude",
#'   credential_col = "credential",
#'   credential_colors = c("MD" = "#2E86AB", "DO" = "#A23B72"),
#'   base_map_types = c("OpenStreetMap", "CartoDB Light"),
#'   include_heatmap = TRUE,
#'   cluster_markers = TRUE,
#'   popup_cols = c("physician_name", "specialty"),
#'   map_title = "Sample Physician Map",
#'   verbose = TRUE
#' )
#'
#' # Example 2: Advanced usage with custom settings and file output
#' physician_extended_data <- data.frame(
#'   name = c("Dr. Johnson", "Dr. Williams", "Dr. Davis", "Dr. Miller"),
#'   lat = c(39.9526, 29.7604, 47.6062, 25.7617),
#'   lon = c(-75.1652, -95.3698, -122.3321, -80.1918),
#'   type = c("MD", "DO", "Other", "MD"),
#'   department = c("Emergency", "Pediatrics", "Psychiatry", "Surgery"),
#'   years_experience = c(15, 8, 12, 20),
#'   stringsAsFactors = FALSE
#' )
#' 
#' physician_map_advanced <- create_physician_map(
#'   physician_geodata = physician_extended_data,
#'   output_file_path = "physician_locations_map.html",
#'   latitude_col = "lat",
#'   longitude_col = "lon", 
#'   credential_col = "type",
#'   credential_colors = c("MD" = "#FF6B6B", "DO" = "#4ECDC4", 
#'                        "Other" = "#45B7D1"),
#'   base_map_types = c("OpenStreetMap", "Satellite", "CartoDB Dark"),
#'   include_heatmap = FALSE,
#'   cluster_markers = FALSE,
#'   popup_cols = c("name", "department", "years_experience"),
#'   map_title = "Advanced Physician Location Analysis",
#'   verbose = TRUE
#' )
#'
#' # Example 3: Minimal usage with all defaults
#' physician_minimal_data <- data.frame(
#'   doctor_id = 1:3,
#'   latitude = c(42.3601, 37.7749, 30.2672),
#'   longitude = c(-71.0589, -122.4194, -97.7431),
#'   credential = c("MD", "DO", "MD"),
#'   practice_type = c("Private", "Hospital", "Clinic"),
#'   stringsAsFactors = FALSE
#' )
#' 
#' physician_map_minimal <- create_physician_map(
#'   physician_geodata = physician_minimal_data,
#'   output_file_path = NULL,
#'   latitude_col = "latitude",
#'   longitude_col = "longitude", 
#'   credential_col = "credential",
#'   credential_colors = c("MD" = "#2E86AB", "DO" = "#A23B72", 
#'                        "Other" = "#808080"),
#'   base_map_types = c("OpenStreetMap", "CartoDB Light", "Satellite"),
#'   include_heatmap = TRUE,
#'   cluster_markers = TRUE,
#'   popup_cols = NULL,
#'   map_title = "Physician Locations",
#'   verbose = FALSE
#' )
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr select filter mutate arrange
#' @importFrom leaflet leaflet addTiles addProviderTiles addCircleMarkers
#' @importFrom leaflet.extras addHeatmap
#' @importFrom leaflet saveWidget
#' @importFrom sf st_as_sf st_coordinates st_crs st_transform
#' @importFrom htmlwidgets saveWidget
#' @importFrom glue glue
#' @export
create_physician_map <- function(
    physician_geodata,
    output_file_path = NULL,
    latitude_col = "latitude",
    longitude_col = "longitude", 
    credential_col = "credential",
    credential_colors = c("MD" = "#2E86AB", "DO" = "#A23B72", "Other" = "#808080"),
    base_map_types = c("OpenStreetMap", "CartoDB Light", "Satellite"),
    include_heatmap = TRUE,
    cluster_markers = TRUE,
    popup_cols = NULL,
    map_title = "Physician Locations",
    verbose = FALSE
) {
  
  if (verbose) {
    logger::log_info("Starting physician map creation process")
    logger::log_info(glue::glue(
      "Input parameters: output_file_path={output_file_path}, ",
      "include_heatmap={include_heatmap}, cluster_markers={cluster_markers}"
    ))
  }
  
  # Validate inputs
  processed_physician_data <- validate_and_process_input_data(
    physician_geodata = physician_geodata,
    latitude_col = latitude_col,
    longitude_col = longitude_col,
    credential_col = credential_col,
    verbose = verbose
  )
  
  # Setup popup columns if not specified
  final_popup_cols <- setup_popup_columns(
    physician_data = processed_physician_data,
    popup_cols = popup_cols,
    latitude_col = latitude_col,
    longitude_col = longitude_col,
    verbose = verbose
  )
  
  # Create base leaflet map
  physician_leaflet_map <- create_base_leaflet_map(
    map_title = map_title,
    base_map_types = base_map_types,
    verbose = verbose
  )
  
  # Add physician markers
  physician_leaflet_map <- add_physician_markers_to_map(
    leaflet_map = physician_leaflet_map,
    physician_data = processed_physician_data,
    credential_col = credential_col,
    credential_colors = credential_colors,
    popup_cols = final_popup_cols,
    cluster_markers = cluster_markers,
    verbose = verbose
  )
  
  # Add heatmap if requested
  if (include_heatmap) {
    physician_leaflet_map <- add_heatmap_layer_to_map(
      leaflet_map = physician_leaflet_map,
      physician_data = processed_physician_data,
      verbose = verbose
    )
  }
  
  # Handle output
  handle_map_output(
    leaflet_map = physician_leaflet_map,
    output_file_path = output_file_path,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info("Physician map creation completed successfully")
  }
  
  return(physician_leaflet_map)
}

#' Validate and Process Input Data
#'
#' @noRd
validate_and_process_input_data <- function(physician_geodata, latitude_col, 
                                            longitude_col, credential_col, 
                                            verbose) {
  
  if (verbose) {
    logger::log_info("Validating input physician geodata")
  }
  
  # Basic input validation
  assertthat::assert_that(
    !missing(physician_geodata),
    msg = "physician_geodata parameter is required"
  )
  
  assertthat::assert_that(
    is.data.frame(physician_geodata) || inherits(physician_geodata, "sf"),
    msg = "physician_geodata must be a data.frame or sf spatial object"
  )
  
  assertthat::assert_that(
    nrow(physician_geodata) > 0,
    msg = "physician_geodata must contain at least one row"
  )
  
  # Convert to sf object if needed
  if (!inherits(physician_geodata, "sf")) {
    processed_spatial_data <- convert_dataframe_to_sf(
      input_dataframe = physician_geodata,
      latitude_col = latitude_col,
      longitude_col = longitude_col,
      verbose = verbose
    )
  } else {
    processed_spatial_data <- physician_geodata
    if (verbose) {
      logger::log_info("Input data is already an sf spatial object")
    }
  }
  
  # Validate required columns exist
  processed_spatial_data <- validate_required_columns_exist(
    spatial_data = processed_spatial_data,
    credential_col = credential_col,
    verbose = verbose
  )
  
  # Transform to WGS84 if needed
  transformed_spatial_data <- ensure_wgs84_projection(
    spatial_data = processed_spatial_data,
    verbose = verbose
  )
  
  if (verbose) {
    logger::log_info(glue::glue(
      "Input validation completed. Processing {nrow(transformed_spatial_data)} ",
      "physician records"
    ))
  }
  
  return(transformed_spatial_data)
}

#' Convert Data Frame to SF Object
#'
#' @noRd
convert_dataframe_to_sf <- function(input_dataframe, latitude_col, 
                                    longitude_col, verbose) {
  
  if (verbose) {
    logger::log_info("Converting data frame to sf spatial object")
  }
  
  # Validate coordinate columns exist
  assertthat::assert_that(
    latitude_col %in% names(input_dataframe),
    msg = glue::glue("Latitude column '{latitude_col}' not found in data")
  )
  
  assertthat::assert_that(
    longitude_col %in% names(input_dataframe),
    msg = glue::glue("Longitude column '{longitude_col}' not found in data")
  )
  
  # Check for valid coordinates
  coordinate_validation_result <- validate_coordinate_values(
    input_dataframe = input_dataframe,
    latitude_col = latitude_col,
    longitude_col = longitude_col,
    verbose = verbose
  )
  
  if (!coordinate_validation_result$all_valid) {
    if (verbose) {
      logger::log_warn(glue::glue(
        "Removing {coordinate_validation_result$invalid_count} rows with ",
        "invalid coordinates"
      ))
    }
    cleaned_dataframe <- input_dataframe[coordinate_validation_result$valid_rows, ]
  } else {
    cleaned_dataframe <- input_dataframe
  }
  
  # Convert to sf object
  spatial_dataframe <- sf::st_as_sf(
    cleaned_dataframe,
    coords = c(longitude_col, latitude_col),
    crs = 4326
  )
  
  if (verbose) {
    logger::log_info("Successfully converted data frame to sf spatial object")
  }
  
  return(spatial_dataframe)
}

#' Validate Coordinate Values
#'
#' @noRd
validate_coordinate_values <- function(input_dataframe, latitude_col, 
                                       longitude_col, verbose) {
  
  latitude_values <- input_dataframe[[latitude_col]]
  longitude_values <- input_dataframe[[longitude_col]]
  
  # Check for valid latitude range (-90 to 90)
  valid_latitude <- !is.na(latitude_values) & 
    latitude_values >= -90 & 
    latitude_values <= 90
  
  # Check for valid longitude range (-180 to 180)
  valid_longitude <- !is.na(longitude_values) & 
    longitude_values >= -180 & 
    longitude_values <= 180
  
  valid_coordinates <- valid_latitude & valid_longitude
  invalid_count <- sum(!valid_coordinates)
  
  if (verbose && invalid_count > 0) {
    logger::log_warn(glue::glue(
      "Found {invalid_count} rows with invalid coordinate values"
    ))
  }
  
  return(list(
    valid_rows = valid_coordinates,
    all_valid = invalid_count == 0,
    invalid_count = invalid_count
  ))
}

#' Validate Required Columns Exist
#'
#' @noRd
validate_required_columns_exist <- function(spatial_data, credential_col, 
                                            verbose) {
  
  available_columns <- names(spatial_data)
  
  if (credential_col %in% available_columns) {
    if (verbose) {
      logger::log_info(glue::glue(
        "Credential column '{credential_col}' found in data"
      ))
    }
    return(spatial_data)
  } else {
    if (verbose) {
      logger::log_warn(glue::glue(
        "Credential column '{credential_col}' not found. Creating default values"
      ))
    }
    spatial_data[[credential_col]] <- "Unknown"
    return(spatial_data)
  }
}

#' Ensure WGS84 Projection
#'
#' @noRd
ensure_wgs84_projection <- function(spatial_data, verbose) {
  
  current_crs <- sf::st_crs(spatial_data)
  
  if (is.na(current_crs) || current_crs != sf::st_crs(4326)) {
    if (verbose) {
      logger::log_info("Transforming spatial data to WGS84 projection (EPSG:4326)")
    }
    transformed_data <- sf::st_transform(spatial_data, crs = 4326)
  } else {
    transformed_data <- spatial_data
    if (verbose) {
      logger::log_info("Spatial data already in WGS84 projection")
    }
  }
  
  return(transformed_data)
}

#' Setup Popup Columns
#'
#' @noRd
setup_popup_columns <- function(physician_data, popup_cols, latitude_col, 
                                longitude_col, verbose) {
  
  if (is.null(popup_cols)) {
    available_columns <- names(physician_data)
    excluded_columns <- c("geometry", latitude_col, longitude_col)
    candidate_columns <- setdiff(available_columns, excluded_columns)
    
    final_popup_columns <- head(candidate_columns, 5)
    
    if (verbose) {
      logger::log_info(glue::glue(
        "Auto-selected popup columns: {paste(final_popup_columns, collapse=', ')}"
      ))
    }
  } else {
    # Validate specified popup columns exist
    missing_columns <- setdiff(popup_cols, names(physician_data))
    if (length(missing_columns) > 0) {
      if (verbose) {
        logger::log_warn(glue::glue(
          "Missing popup columns: {paste(missing_columns, collapse=', ')}"
        ))
      }
    }
    
    final_popup_columns <- intersect(popup_cols, names(physician_data))
    
    if (verbose) {
      logger::log_info(glue::glue(
        "Using specified popup columns: {paste(final_popup_columns, collapse=', ')}"
      ))
    }
  }
  
  return(final_popup_columns)
}

#' Create Base Leaflet Map
#'
#' @noRd
create_base_leaflet_map <- function(map_title, base_map_types, verbose) {
  
  if (verbose) {
    logger::log_info("Creating base leaflet map")
  }
  
  base_leaflet_map <- leaflet::leaflet() %>%
    leaflet::addTiles(group = "OpenStreetMap")
  
  # Add additional base map layers
  for (map_type in base_map_types) {
    if (map_type != "OpenStreetMap") {
      provider_name <- get_provider_tile_name(map_type)
      base_leaflet_map <- base_leaflet_map %>%
        leaflet::addProviderTiles(provider_name, group = map_type)
    }
  }
  
  if (verbose) {
    logger::log_info(glue::glue(
      "Added {length(base_map_types)} base map layer options"
    ))
  }
  
  return(base_leaflet_map)
}

#' Get Provider Tile Name
#'
#' @noRd
get_provider_tile_name <- function(map_type) {
  
  provider_mapping <- list(
    "CartoDB Light" = "CartoDB.Positron",
    "CartoDB Dark" = "CartoDB.DarkMatter", 
    "Satellite" = "Esri.WorldImagery",
    "Terrain" = "Stamen.Terrain"
  )
  
  return(provider_mapping[[map_type]] %||% map_type)
}

#' Add Physician Markers to Map
#'
#' @noRd
add_physician_markers_to_map <- function(leaflet_map, physician_data, 
                                         credential_col, credential_colors,
                                         popup_cols, cluster_markers, verbose) {
  
  if (verbose) {
    logger::log_info("Adding physician markers to map")
  }
  
  # Extract coordinates
  coordinate_matrix <- sf::st_coordinates(physician_data)
  physician_coords_df <- data.frame(
    longitude = coordinate_matrix[, 1],
    latitude = coordinate_matrix[, 2]
  )
  
  # Prepare marker data
  marker_data <- prepare_marker_data(
    physician_data = physician_data,
    physician_coords_df = physician_coords_df,
    credential_col = credential_col,
    credential_colors = credential_colors,
    popup_cols = popup_cols,
    verbose = verbose
  )
  
  # Set up clustering options
  cluster_options <- if (cluster_markers) {
    leaflet::markerClusterOptions()
  } else {
    NULL
  }
  
  # Add markers with color coding
  enhanced_leaflet_map <- leaflet_map %>%
    leaflet::addCircleMarkers(
      data = marker_data,
      lng = ~longitude,
      lat = ~latitude,
      color = ~marker_color,
      fillColor = ~marker_color,
      radius = 8,
      fillOpacity = 0.7,
      stroke = TRUE,
      weight = 2,
      popup = ~popup_text,
      clusterOptions = cluster_options,
      group = "Physicians"
    )
  
  if (verbose) {
    logger::log_info(glue::glue(
      "Added {nrow(marker_data)} physician markers to map"
    ))
  }
  
  return(enhanced_leaflet_map)
}

#' Prepare Marker Data
#'
#' @noRd
prepare_marker_data <- function(physician_data, physician_coords_df, 
                                credential_col, credential_colors, popup_cols, verbose) {
  
  # Combine spatial data with coordinates
  marker_dataframe <- dplyr::bind_cols(
    sf::st_drop_geometry(physician_data),
    physician_coords_df
  )
  
  # Create color mapping
  credential_values <- marker_dataframe[[credential_col]]
  marker_colors <- create_color_mapping(
    credential_values = credential_values,
    credential_colors = credential_colors,
    verbose = verbose
  )
  
  # Create popup text
  popup_text_vector <- create_popup_text(
    marker_dataframe = marker_dataframe,
    popup_cols = popup_cols,
    verbose = verbose
  )
  
  # Add colors and popup text to marker data
  final_marker_data <- marker_dataframe %>%
    dplyr::mutate(
      marker_color = marker_colors,
      popup_text = popup_text_vector
    )
  
  return(final_marker_data)
}

#' Create Color Mapping
#'
#' @noRd
create_color_mapping <- function(credential_values, credential_colors, verbose) {
  
  # Handle edge case of empty credential values
  if (length(credential_values) == 0) {
    if (verbose) {
      logger::log_warn("No credential values found, returning empty color vector")
    }
    return(character(0))
  }
  
  unique_credentials <- unique(credential_values)
  
  color_vector <- character(length(credential_values))
  
  for (credential in unique_credentials) {
    credential_indices <- which(credential_values == credential)
    
    if (credential %in% names(credential_colors)) {
      assigned_color <- credential_colors[[credential]]
    } else {
      # Handle "Unknown" or other unlisted credentials
      if (credential == "Unknown") {
        assigned_color <- credential_colors[["Other"]] %||% "#808080"
        if (verbose) {
          logger::log_info(glue::glue(
            "Using 'Other' color for 'Unknown' credential values"
          ))
        }
      } else {
        assigned_color <- credential_colors[["Other"]] %||% "#808080"
        if (verbose) {
          logger::log_warn(glue::glue(
            "No color specified for credential '{credential}', using default"
          ))
        }
      }
    }
    
    color_vector[credential_indices] <- assigned_color
  }
  
  if (verbose) {
    logger::log_info(glue::glue(
      "Created color mapping for {length(unique_credentials)} credential types"
    ))
  }
  
  return(color_vector)
}

#' Create Popup Text
#'
#' @noRd
create_popup_text <- function(marker_dataframe, popup_cols, verbose) {
  
  popup_text_list <- apply(marker_dataframe[popup_cols], 1, function(row_data) {
    info_lines <- character(0)
    
    for (col_name in popup_cols) {
      col_value <- row_data[[col_name]]
      if (!is.na(col_value) && col_value != "") {
        formatted_line <- glue::glue("<b>{col_name}:</b> {col_value}")
        info_lines <- c(info_lines, formatted_line)
      }
    }
    
    return(paste(info_lines, collapse = "<br>"))
  })
  
  if (verbose) {
    logger::log_info("Created popup text for all physician markers")
  }
  
  return(unlist(popup_text_list))
}

#' Add Heatmap Layer to Map
#'
#' @noRd
add_heatmap_layer_to_map <- function(leaflet_map, physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Adding heatmap layer to map")
  }
  
  # Check if leaflet.extras is available
  if (!requireNamespace("leaflet.extras", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("leaflet.extras package not available - skipping heatmap layer")
    }
    return(leaflet_map)
  }
  
  # Extract coordinates for heatmap
  heatmap_coordinates <- sf::st_coordinates(physician_data)
  
  enhanced_map_with_heatmap <- leaflet_map %>%
    leaflet.extras::addHeatmap(
      lng = heatmap_coordinates[, 1],
      lat = heatmap_coordinates[, 2],
      radius = 20,
      blur = 15,
      max = 1.0,
      group = "Density Heatmap"
    )
  
  # Add layer control
  final_map_with_controls <- enhanced_map_with_heatmap %>%
    leaflet::addLayersControl(
      overlayGroups = c("Physicians", "Density Heatmap"),
      options = leaflet::layersControlOptions(collapsed = FALSE)
    )
  
  if (verbose) {
    logger::log_info("Successfully added heatmap layer and layer controls")
  }
  
  return(final_map_with_controls)
}

#' Handle Map Output
#'
#' @noRd
handle_map_output <- function(leaflet_map, output_file_path, verbose) {
  
  if (!is.null(output_file_path)) {
    if (verbose) {
      logger::log_info(glue::glue(
        "Saving map to file: {output_file_path}"
      ))
    }
    
    # Ensure directory exists
    output_directory <- dirname(output_file_path)
    if (!dir.exists(output_directory)) {
      dir.create(output_directory, recursive = TRUE)
      if (verbose) {
        logger::log_info(glue::glue(
          "Created output directory: {output_directory}"
        ))
      }
    }
    
    # Save the map
    htmlwidgets::saveWidget(
      widget = leaflet_map,
      file = output_file_path,
      selfcontained = TRUE
    )
    
    if (verbose) {
      logger::log_info(glue::glue(
        "Map successfully saved to: {normalizePath(output_file_path)}"
      ))
    }
  } else {
    if (verbose) {
      logger::log_info("No output file specified - map will display in viewer")
    }
  }
}

#execute fancy map-----

class(full_results)

# Your original call should now work
create_physician_map(
  physician_geodata = full_results,
  output_file_path = NULL,
  credential_colors = c("MD" = "#2E86AB", "DO" = "#A23B72", "Other" = "#808080"),
  base_map_types = c("OpenStreetMap", "CartoDB Light", "Satellite"),
  include_heatmap = TRUE,
  cluster_markers = TRUE,
  verbose = TRUE
)


## fancy fancy map -----
# Enhanced version of your current map
enhanced_map <- create_physician_map(
  physician_geodata = full_results,
  output_file_path = NULL,
  credential_colors = c("MD" = "#1e88e5", "DO" = "#d32f2f", "Other" = "#757575"),
  base_map_types = c("CartoDB Light", "OpenStreetMap", "Satellite"),
  include_heatmap = TRUE,
  cluster_markers = TRUE,
  
  # Add the missing professional elements
  include_compass_rose = TRUE,
  compass_position = "topright",
  include_county_boundaries = TRUE,
  county_boundary_style = list(
    weight = 1,
    color = "#E0E0E0", 
    fillOpacity = 0.02,
    dashArray = "2,2"
  ),
  
  # Better scale bar
  include_scale_bar = TRUE,
  scale_bar_position = "bottomleft",
  scale_bar_options = list(
    maxWidth = 150,
    metric = TRUE,
    imperial = TRUE
  ),
  
  verbose = TRUE
)

enhanced_map


# Map with female demographics -----
#' Create Interactive Map of Physician Locations
#'
#' This function creates an interactive map displaying physician locations with
#' various customization options including heatmaps, clustering, and boundary
#' overlays. The function automatically converts data frames with lat/lon
#' coordinates to sf spatial objects if needed.
#'
#' @param physician_geodata A data frame or sf object containing physician
#'   location data. Must include latitude and longitude columns (named
#'   'latitude' and 'longitude') if providing a data frame.
#' @param output_file_path Character string specifying the path where the HTML
#'   map file should be saved. If NULL (default), the map is displayed
#'   interactively but not saved to file.
#' @param include_heatmap Logical indicating whether to include a heatmap layer
#'   showing density of physician locations. Default is TRUE.
#' @param cluster_markers Logical indicating whether to cluster nearby markers
#'   for better visualization at different zoom levels. Default is TRUE.
#' @param hrr_boundaries_path Character string specifying the path to Hospital
#'   Referral Region (HRR) boundary shapefiles. If NULL (default), HRR
#'   boundaries are not included.
#' @param include_county_boundaries Logical indicating whether to include
#'   county boundary lines on the map. Default is FALSE.
#' @param acog_boundaries_path Character string specifying the path to ACOG
#'   (American College of Obstetricians and Gynecologists) district boundary
#'   shapefiles. If NULL (default), ACOG boundaries are not included.
#' @param include_compass_rose Logical indicating whether to include a compass
#'   rose on the map for orientation. Default is TRUE.
#' @param include_scale_bar Logical indicating whether to include a scale bar
#'   on the map for distance reference. Default is TRUE.
#' @param verbose Logical indicating whether to provide detailed logging
#'   information during function execution. Default is FALSE.
#'
#' @return A leaflet map object that can be displayed interactively or saved
#'   to HTML file.
#'
#' @examples
#' # Example 1: Basic map with all default settings
#' sample_physician_data <- data.frame(
#'   npi = c("1234567890", "0987654321", "1122334455"),
#'   plname = c("Smith", "Johnson", "Williams"),
#'   pfname = c("John", "Jane", "Bob"),
#'   latitude = c(39.7392, 39.7642, 39.7092),
#'   longitude = c(-104.9903, -104.9551, -105.0178),
#'   pmailcityname = c("Denver", "Denver", "Denver"),
#'   pmailstatename = c("CO", "CO", "CO")
#' )
#' 
#' physician_map_basic <- create_physician_map(
#'   physician_geodata = sample_physician_data,
#'   output_file_path = NULL,
#'   include_heatmap = TRUE,
#'   cluster_markers = TRUE,
#'   hrr_boundaries_path = NULL,
#'   include_county_boundaries = FALSE,
#'   acog_boundaries_path = NULL,
#'   include_compass_rose = TRUE,
#'   include_scale_bar = TRUE,
#'   verbose = FALSE
#' )
#' # Returns: Interactive leaflet map with physician locations
#' 
#' # Example 2: Map with county boundaries and file output
#' physician_map_counties <- create_physician_map(
#'   physician_geodata = sample_physician_data,
#'   output_file_path = "physician_locations_with_counties.html",
#'   include_heatmap = TRUE,
#'   cluster_markers = FALSE,
#'   hrr_boundaries_path = NULL,
#'   include_county_boundaries = TRUE,
#'   acog_boundaries_path = NULL,
#'   include_compass_rose = TRUE,
#'   include_scale_bar = TRUE,
#'   verbose = TRUE
#' )
#' # Returns: Interactive leaflet map saved to HTML file with county boundaries
#' 
#' # Example 3: Detailed map with all boundary layers and verbose logging
#' physician_map_detailed <- create_physician_map(
#'   physician_geodata = sample_physician_data,
#'   output_file_path = "detailed_physician_map.html",
#'   include_heatmap = FALSE,
#'   cluster_markers = TRUE,
#'   hrr_boundaries_path = "path/to/hrr_boundaries.shp",
#'   include_county_boundaries = TRUE,
#'   acog_boundaries_path = "path/to/acog_boundaries.shp",
#'   include_compass_rose = FALSE,
#'   include_scale_bar = FALSE,
#'   verbose = TRUE
#' )
#' # Returns: Comprehensive map with all boundary layers and detailed logging
#'
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr select mutate filter
#' @importFrom sf st_as_sf st_crs st_transform st_read
#' @importFrom leaflet leaflet addTiles addCircleMarkers
#' @importFrom leaflet addPolygons addLayersControl layersControlOptions
#' @importFrom leaflet addScaleBar addControl markerClusterOptions
#' @importFrom leaflet.extras addHeatmap
#' @importFrom htmlwidgets saveWidget
#' @importFrom maps map
#' @export
create_physician_map <- function(physician_geodata,
                                 output_file_path = NULL,
                                 include_heatmap = TRUE,
                                 cluster_markers = TRUE,
                                 hrr_boundaries_path = NULL,
                                 include_county_boundaries = FALSE,
                                 acog_boundaries_path = NULL,
                                 include_compass_rose = TRUE,
                                 include_scale_bar = TRUE,
                                 verbose = FALSE) {
  
  if (verbose) {
    logger::log_info("Starting physician map creation process")
    logger::log_info("Input parameters: output_file_path={output_file_path}, include_heatmap={include_heatmap}, cluster_markers={cluster_markers}, hrr_boundaries_path={hrr_boundaries_path}, include_county_boundaries={include_county_boundaries}, acog_boundaries_path={acog_boundaries_path}, include_compass_rose={include_compass_rose}, include_scale_bar={include_scale_bar}")
  }
  
  # Validate inputs
  validated_spatial_data <- validate_and_prepare_physician_data(
    physician_geodata = physician_geodata,
    verbose = verbose
  )
  
  validate_map_parameters(
    output_file_path = output_file_path,
    include_heatmap = include_heatmap,
    cluster_markers = cluster_markers,
    hrr_boundaries_path = hrr_boundaries_path,
    include_county_boundaries = include_county_boundaries,
    acog_boundaries_path = acog_boundaries_path,
    include_compass_rose = include_compass_rose,
    include_scale_bar = include_scale_bar,
    verbose = verbose
  )
  
  # Create base map
  physician_leaflet_map <- create_base_leaflet_map(
    spatial_physician_data = validated_spatial_data,
    verbose = verbose
  )
  
  # Add physician markers
  physician_leaflet_map <- add_physician_markers(
    map_object = physician_leaflet_map,
    spatial_physician_data = validated_spatial_data,
    cluster_markers = cluster_markers,
    verbose = verbose
  )
  
  # Add heatmap if requested
  if (include_heatmap) {
    physician_leaflet_map <- add_physician_heatmap(
      map_object = physician_leaflet_map,
      spatial_physician_data = validated_spatial_data,
      verbose = verbose
    )
  }
  
  # Add boundary layers
  physician_leaflet_map <- add_boundary_layers(
    map_object = physician_leaflet_map,
    include_county_boundaries = include_county_boundaries,
    hrr_boundaries_path = hrr_boundaries_path,
    acog_boundaries_path = acog_boundaries_path,
    verbose = verbose
  )
  
  # Add map controls
  physician_leaflet_map <- add_map_controls(
    map_object = physician_leaflet_map,
    include_compass_rose = include_compass_rose,
    include_scale_bar = include_scale_bar,
    include_heatmap = include_heatmap,
    include_county_boundaries = include_county_boundaries,
    hrr_boundaries_path = hrr_boundaries_path,
    acog_boundaries_path = acog_boundaries_path,
    verbose = verbose
  )
  
  # Save map if output path provided
  if (!is.null(output_file_path)) {
    save_physician_map(
      map_object = physician_leaflet_map,
      output_file_path = output_file_path,
      verbose = verbose
    )
  }
  
  if (verbose) {
    logger::log_info("Physician map creation process completed successfully")
  }
  
  return(physician_leaflet_map)
}

#' Validate and Prepare Physician Data for Mapping
#' @noRd
validate_and_prepare_physician_data <- function(physician_geodata, verbose) {
  
  if (verbose) {
    logger::log_info("Validating input physician geodata")
  }
  
  # Basic input validation
  assertthat::assert_that(!missing(physician_geodata),
                          msg = "physician_geodata parameter is required")
  assertthat::assert_that(!is.null(physician_geodata),
                          msg = "physician_geodata cannot be NULL")
  
  if (verbose) {
    logger::log_info("Input data class: {class(physician_geodata)}")
    logger::log_info("Input data dimensions: {format_with_commas(nrow(physician_geodata))} rows, {ncol(physician_geodata)} columns")
  }
  
  # Check if already sf object
  if ("sf" %in% class(physician_geodata)) {
    if (verbose) {
      logger::log_info("Input data is already an sf spatial object")
    }
    validated_spatial_data <- physician_geodata
  } else {
    # Convert data frame to sf object
    assertthat::assert_that(is.data.frame(physician_geodata),
                            msg = "physician_geodata must be a data frame or sf spatial object")
    
    required_coordinate_columns <- c("latitude", "longitude")
    assertthat::assert_that(all(required_coordinate_columns %in% names(physician_geodata)),
                            msg = "physician_geodata must contain 'latitude' and 'longitude' columns")
    
    if (verbose) {
      logger::log_info("Converting data frame to sf spatial object")
    }
    
    # Remove rows with missing coordinates
    clean_physician_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      missing_coordinate_count <- nrow(physician_geodata) - nrow(clean_physician_data)
      logger::log_info("Removed {format_with_commas(missing_coordinate_count)} rows with missing coordinates")
      logger::log_info("Final dataset contains {format_with_commas(nrow(clean_physician_data))} physicians with valid coordinates")
    }
    
    assertthat::assert_that(nrow(clean_physician_data) > 0,
                            msg = "No valid coordinate data found in physician_geodata")
    
    # Convert to sf object
    validated_spatial_data <- sf::st_as_sf(
      clean_physician_data,
      coords = c("longitude", "latitude"),
      crs = 4326  # WGS84
    )
  }
  
  # Ensure correct CRS
  validated_spatial_data <- sf::st_transform(validated_spatial_data, crs = 4326)
  
  if (verbose) {
    logger::log_info("Final spatial data CRS: {sf::st_crs(validated_spatial_data)$input}")
    logger::log_info("Spatial data validation completed successfully")
  }
  
  return(validated_spatial_data)
}

#' Validate Map Parameters
#' @noRd
validate_map_parameters <- function(output_file_path, include_heatmap, cluster_markers,
                                    hrr_boundaries_path, include_county_boundaries,
                                    acog_boundaries_path, include_compass_rose,
                                    include_scale_bar, verbose) {
  
  if (verbose) {
    logger::log_info("Validating map parameters")
  }
  
  # Validate logical parameters
  logical_parameters <- list(
    include_heatmap = include_heatmap,
    cluster_markers = cluster_markers,
    include_county_boundaries = include_county_boundaries,
    include_compass_rose = include_compass_rose,
    include_scale_bar = include_scale_bar,
    verbose = verbose
  )
  
  for (param_name in names(logical_parameters)) {
    assertthat::assert_that(is.logical(logical_parameters[[param_name]]),
                            msg = paste(param_name, "must be logical (TRUE/FALSE)"))
  }
  
  # Validate file paths
  if (!is.null(output_file_path)) {
    assertthat::assert_that(is.character(output_file_path),
                            msg = "output_file_path must be a character string")
  }
  
  if (!is.null(hrr_boundaries_path)) {
    assertthat::assert_that(is.character(hrr_boundaries_path),
                            msg = "hrr_boundaries_path must be a character string")
    assertthat::assert_that(file.exists(hrr_boundaries_path),
                            msg = "HRR boundaries file does not exist at specified path")
  }
  
  if (!is.null(acog_boundaries_path)) {
    assertthat::assert_that(is.character(acog_boundaries_path),
                            msg = "acog_boundaries_path must be a character string")
    assertthat::assert_that(file.exists(acog_boundaries_path),
                            msg = "ACOG boundaries file does not exist at specified path")
  }
  
  if (verbose) {
    logger::log_info("Map parameters validation completed successfully")
  }
}

#' Create Base Leaflet Map
#' @noRd
create_base_leaflet_map <- function(spatial_physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Creating base leaflet map")
  }
  
  # Calculate map center from physician locations
  physician_coordinates <- sf::st_coordinates(spatial_physician_data)
  map_center_latitude <- mean(physician_coordinates[, "Y"])
  map_center_longitude <- mean(physician_coordinates[, "X"])
  
  if (verbose) {
    logger::log_info("Map center calculated: latitude {round(map_center_latitude, 4)}, longitude {round(map_center_longitude, 4)}")
  }
  
  # Create base map
  base_leaflet_map <- leaflet::leaflet() %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::setView(lng = map_center_longitude, lat = map_center_latitude, zoom = 8)
  
  if (verbose) {
    logger::log_info("Base leaflet map created successfully")
  }
  
  return(base_leaflet_map)
}

#' Add Physician Markers to Map
#' @noRd
add_physician_markers <- function(map_object, spatial_physician_data, cluster_markers, verbose) {
  
  if (verbose) {
    logger::log_info("Adding physician markers to map")
    logger::log_info("Clustering enabled: {cluster_markers}")
  }
  
  # Create popup content
  popup_content <- create_physician_popup_content(spatial_physician_data, verbose)
  
  # Add markers with or without clustering
  if (cluster_markers) {
    updated_map_object <- map_object %>%
      leaflet::addCircleMarkers(
        data = spatial_physician_data,
        popup = popup_content,
        radius = 5,
        color = "blue",
        stroke = TRUE,
        fillOpacity = 0.7,
        group = "Physicians",
        clusterOptions = leaflet::markerClusterOptions()
      )
  } else {
    updated_map_object <- map_object %>%
      leaflet::addCircleMarkers(
        data = spatial_physician_data,
        popup = popup_content,
        radius = 5,
        color = "blue",
        stroke = TRUE,
        fillOpacity = 0.7,
        group = "Physicians"
      )
  }
  
  if (verbose) {
    logger::log_info("Physician markers added successfully")
  }
  
  return(updated_map_object)
}

#' Create Physician Popup Content
#' @noRd
create_physician_popup_content <- function(spatial_physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Creating popup content for physician markers")
  }
  
  # Extract data frame from sf object
  physician_attributes <- spatial_physician_data %>%
    sf::st_drop_geometry()
  
  # Create popup content with available information
  popup_content <- paste(
    "<b>Physician Information</b><br/>",
    if ("pfname" %in% names(physician_attributes) && "plname" %in% names(physician_attributes)) {
      paste("Name:", physician_attributes$pfname, physician_attributes$plname, "<br/>")
    } else "",
    if ("npi" %in% names(physician_attributes)) {
      paste("NPI:", physician_attributes$npi, "<br/>")
    } else "",
    if ("pcredential" %in% names(physician_attributes)) {
      paste("Credentials:", physician_attributes$pcredential, "<br/>")
    } else "",
    if ("pmailcityname" %in% names(physician_attributes) && "pmailstatename" %in% names(physician_attributes)) {
      paste("Location:", physician_attributes$pmailcityname, ",", physician_attributes$pmailstatename)
    } else "",
    sep = ""
  )
  
  if (verbose) {
    logger::log_info("Popup content created for {nrow(spatial_physician_data)} physicians")
  }
  
  return(popup_content)
}

#' Add Physician Heatmap Layer
#' @noRd
add_physician_heatmap <- function(map_object, spatial_physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Adding physician density heatmap layer")
  }
  
  # Check if leaflet.extras is available
  if (!requireNamespace("leaflet.extras", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("leaflet.extras package not available - skipping heatmap layer")
      logger::log_info("To enable heatmap functionality, install leaflet.extras: install.packages('leaflet.extras')")
    }
    return(map_object)
  }
  
  # Extract coordinates for heatmap
  physician_coordinates <- sf::st_coordinates(spatial_physician_data)
  heatmap_data <- data.frame(
    lat = physician_coordinates[, "Y"],
    lng = physician_coordinates[, "X"]
  )
  
  updated_map_object <- map_object %>%
    leaflet.extras::addHeatmap(
      data = heatmap_data,
      lng = ~lng,
      lat = ~lat,
      intensity = 1,
      blur = 20,
      max = 0.05,
      radius = 15,
      group = "Physician Density"
    )
  
  if (verbose) {
    logger::log_info("Heatmap layer added successfully with {nrow(heatmap_data)} data points")
  }
  
  return(updated_map_object)
}

#' Add Boundary Layers to Map
#' @noRd
add_boundary_layers <- function(map_object, include_county_boundaries, hrr_boundaries_path,
                                acog_boundaries_path, verbose) {
  
  updated_map_object <- map_object
  
  # Add county boundaries
  if (include_county_boundaries) {
    if (verbose) {
      logger::log_info("County boundaries requested")
      logger::log_warn("County boundaries feature requires additional spatial data setup - skipping for now")
      logger::log_info("To include county boundaries, provide county shapefile data or use a different mapping approach")
    }
    
    # Note: County boundaries implementation would require additional spatial data
    # This could be implemented by providing county boundary shapefiles similar to HRR/ACOG
    # For now, we skip this to avoid errors and continue with other map features
  }
  
  # Add HRR boundaries
  if (!is.null(hrr_boundaries_path)) {
    if (verbose) {
      logger::log_info("Loading HRR boundaries from: {hrr_boundaries_path}")
    }
    
    hrr_boundary_data <- sf::st_read(hrr_boundaries_path, quiet = !verbose)
    hrr_boundary_data <- sf::st_transform(hrr_boundary_data, crs = 4326)
    
    updated_map_object <- updated_map_object %>%
      leaflet::addPolygons(
        data = hrr_boundary_data,
        color = "red",
        weight = 2,
        opacity = 0.8,
        fillOpacity = 0.1,
        group = "HRR Boundaries"
      )
    
    if (verbose) {
      logger::log_info("HRR boundaries added successfully")
    }
  }
  
  # Add ACOG boundaries
  if (!is.null(acog_boundaries_path)) {
    if (verbose) {
      logger::log_info("Loading ACOG boundaries from: {acog_boundaries_path}")
    }
    
    acog_boundary_data <- sf::st_read(acog_boundaries_path, quiet = !verbose)
    acog_boundary_data <- sf::st_transform(acog_boundary_data, crs = 4326)
    
    updated_map_object <- updated_map_object %>%
      leaflet::addPolygons(
        data = acog_boundary_data,
        color = "green",
        weight = 2,
        opacity = 0.8,
        fillOpacity = 0.1,
        group = "ACOG Districts"
      )
    
    if (verbose) {
      logger::log_info("ACOG boundaries added successfully")
    }
  }
  
  return(updated_map_object)
}

#' Add Map Controls and Legend
#' @noRd
add_map_controls <- function(map_object, include_compass_rose, include_scale_bar,
                             include_heatmap, include_county_boundaries,
                             hrr_boundaries_path, acog_boundaries_path, verbose) {
  
  if (verbose) {
    logger::log_info("Adding map controls and legend")
  }
  
  updated_map_object <- map_object
  
  # Add scale bar
  if (include_scale_bar) {
    updated_map_object <- updated_map_object %>%
      leaflet::addScaleBar(position = "bottomleft", options = list(imperial = TRUE, metric = TRUE))
    
    if (verbose) {
      logger::log_info("Scale bar added to map")
    }
  }
  
  # Create layer control groups
  overlay_groups <- c("Physicians")
  if (include_heatmap) overlay_groups <- c(overlay_groups, "Physician Density")
  if (include_county_boundaries) overlay_groups <- c(overlay_groups, "County Boundaries")
  if (!is.null(hrr_boundaries_path)) overlay_groups <- c(overlay_groups, "HRR Boundaries")
  if (!is.null(acog_boundaries_path)) overlay_groups <- c(overlay_groups, "ACOG Districts")
  
  # Add layers control
  updated_map_object <- updated_map_object %>%
    leaflet::addLayersControl(
      baseGroups = c("OpenStreetMap"),
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    )
  
  if (verbose) {
    logger::log_info("Layer controls added with {length(overlay_groups)} overlay groups")
  }
  
  return(updated_map_object)
}

#' Save Physician Map to HTML File
#' @noRd
save_physician_map <- function(map_object, output_file_path, verbose) {
  
  if (verbose) {
    logger::log_info("Saving map to HTML file: {output_file_path}")
  }
  
  # Ensure output directory exists
  output_directory <- dirname(output_file_path)
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
    if (verbose) {
      logger::log_info("Created output directory: {output_directory}")
    }
  }
  
  # Save map
  htmlwidgets::saveWidget(
    widget = map_object,
    file = output_file_path,
    selfcontained = TRUE
  )
  
  if (verbose) {
    logger::log_info("Map saved successfully to: {normalizePath(output_file_path)}")
    logger::log_info("File size: {file.size(output_file_path)} bytes")
  }
}

# Install required packages for female demographics
# Remove and reinstall tidycensus
# remove.packages("tidycensus")
# install.packages("tidycensus")

library(tidycensus)
library(tigris)
library(leaflet.extras)

# Get free Census API key
# Visit: https://api.census.gov/data/key_signup.html
# tidycensus::census_api_key("485c6da8987af0b9829c25f899f2393b4bb1a4fb", install = TRUE, overwrite = TRUE)

# Then run your map
# Load your function and test the enhanced county demographics

full_results <- readr::read_csv("data/geocoded/obgyn_geocoded.csv")

create_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  verbose = TRUE
)


# Simple Physician Map with Basic County Boundaries -----
# Install required packages
source("R/01-setup.R")
#install.packages(c("tigris", "tidycensus"))
Sys.getenv("CENSUS_API_KEY")  # Should show your key

#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define female-specific demographic variables (updated variable codes)
    female_demographic_variables <- c(
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_18_to_64 = "B01001_030",       # Female 18 to 64
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Simplified education variables
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check if we have the key variables (fix column name format)
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      logger::log_info("Has total_female: {has_total_female}, Has age variables: {has_age_vars}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate female-specific percentages and metrics (fix column names)
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations with safe handling (correct column names)
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Calculate percentages with safe division (correct column names)
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education (simplified - just bachelor's degree) (correct column names)
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display (correct column names)
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          pct_female_under_18, pct_female_over_65, pct_female_bachelor_plus, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, female_bachelor_plus_raw, total_female_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show a sample of successful data
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME} - {good_county$total_female_raw} females, {good_county$pct_female_under_18}% under 18")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Demographic County Popups with Female Data
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 340px;'>",
    "<div style='background: linear-gradient(135deg, #d63384 0%, #6f42c1 100%); color: white; padding: 12px; margin: -10px -10px 12px -10px; border-radius: 8px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: bold;'>Female Demographics Focus</span>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #2196f3;'>",
    "<h4 style='margin: 0 0 4px 0; color: #1976d2; font-size: 13px;'>👩‍⚕️ Healthcare Providers</h4>",
    "<p style='margin: 0; font-size: 12px; font-weight: bold;'>", physician_counts, " physicians in your dataset</p>",
    "</div>",
    
    # Female population overview
    "<div style='background: #f8f9fa; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #d63384;'>",
    "<h4 style='margin: 0 0 6px 0; color: #d63384; font-size: 13px;'>👩 Female Population Overview</h4>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>Total Females:</strong> ", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "% of population")), ")</p>",
    "</div>",
    
    # Female age demographics
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>📊 Female Age Demographics</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Female under 18
    "<div style='background: #fff3cd; padding: 8px; border-radius: 6px; border-left: 3px solid #ffc107;'>",
    "<div style='font-weight: bold; color: #856404;'>🧒 Females Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # Female over 65
    "<div style='background: #d1ecf1; padding: 8px; border-radius: 6px; border-left: 3px solid #17a2b8;'>",
    "<div style='font-weight: bold; color: #0c5460;'>👵 Females Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female education
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>🎓 Female Education</h4>",
    "<div style='background: #d4edda; padding: 10px; border-radius: 6px; border-left: 3px solid #28a745;'>",
    "<div style='display: flex; justify-content: space-between; align-items: center;'>",
    "<div>",
    "<div style='font-weight: bold; color: #155724; font-size: 13px;'>👩‍🎓 Females with Bachelor's Degree+</div>",
    "<div style='font-size: 10px; color: #6c757d; margin-top: 2px;'>% of female population 25 years and older</div>",
    "</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #155724;'>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), "</div>",
    "</div>",
    "<div style='font-size: 11px; color: #6c757d; margin-top: 4px;'>", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0("(", format(counties$female_bachelor_plus_raw, big.mark = ","), " females with degrees)")), "</div>",
    "</div>",
    "</div>",
    
    # Healthcare relevance note
    "<div style='background: #f8d7da; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #dc3545;'>",
    "<div style='font-size: 11px; color: #721c24;'>",
    "<strong>🏥 Healthcare Relevance:</strong> Female demographics are key for OB/GYN, pediatric, and geriatric care planning.",
    "</div>",
    "</div>",
    
    # County administrative info
    "<div style='background: #e9ecef; padding: 6px; margin: 6px 0; border-radius: 4px;'>",
    "<div style='font-size: 10px; color: #6c757d;'>",
    "<strong>🏛️ County Code:</strong> ", counties$COUNTYFP, " | ",
    "<strong>🌍 FIPS:</strong> ", counties$GEOID, "<br/>",
    "<strong>📊 Land:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq mi | ",
    "<strong>💧 Water:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq mi",
    "</div>",
    "</div>",
    
    "<div style='font-size: 10px; color: #6c757d; text-align: center; margin-top: 12px; border-top: 1px solid #dee2e6; padding-top: 6px;'>",
    "📊 Data: 2022 American Community Survey 5-Year Estimates<br>",
    "All percentages calculated relative to female population in each age group",
    "</div>",
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# execute ----
full_results <- readr::read_csv("data/geocoded/obgyn_geocoded.csv")

# Skip the problematic .Renviron file and set it directly
Sys.setenv(CENSUS_API_KEY = "485c6da8987af0b9829c25f899f2393b4bb1a4fb")

# Test it works
Sys.getenv("CENSUS_API_KEY")

# Now run your map - it should work
# Run the improved function with better debugging
# The column names were wrong - this should work now!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)

# 1906 Function ----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define comprehensive female-specific demographic variables
    female_demographic_variables <- c(
      # Basic female population
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_18_to_64 = "B01001_030",       # Female 18 to 64
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Female employment & labor force
      female_labor_force = "B23001_029",    # Female in labor force
      female_employed = "B23001_030",       # Female employed
      
      # Female poverty
      female_poverty = "B17001_031",        # Female in poverty
      
      # Female marital status (15 years and over)
      female_never_married = "B12001_010",  # Female never married
      female_married = "B12001_014",        # Female married
      female_divorced = "B12001_020",       # Female divorced
      female_widowed = "B12001_019",        # Female widowed
      
      # Female health insurance
      female_no_insurance = "B27001_017",   # Female with no health insurance
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check if we have the key variables (fix column name format)
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_employment_vars <- all(c("female_labor_forceE", "female_employedE") %in% names(demographic_data))
      has_marital_vars <- all(c("female_never_marriedE", "female_marriedE") %in% names(demographic_data))
      logger::log_info("Has total_female: {has_total_female}, Has age variables: {has_age_vars}")
      logger::log_info("Has employment variables: {has_employment_vars}, Has marital variables: {has_marital_vars}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate comprehensive female-specific percentages and metrics
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages (% of total female population)
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Employment percentages
        pct_female_in_labor_force = ifelse(
          !is.na(female_labor_forceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_labor_forceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_employed = ifelse(
          !is.na(female_employedE) & !is.na(female_labor_forceE) & female_labor_forceE > 0,
          round((female_employedE / female_labor_forceE) * 100, 1),
          NA_real_
        ),
        
        # Poverty percentage
        pct_female_poverty = ifelse(
          !is.na(female_povertyE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_povertyE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Marital status percentages (of total female population)
        pct_female_never_married = ifelse(
          !is.na(female_never_marriedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_never_marriedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_married = ifelse(
          !is.na(female_marriedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_marriedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_divorced = ifelse(
          !is.na(female_divorcedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_divorcedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_widowed = ifelse(
          !is.na(female_widowedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_widowedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Health insurance percentage
        pct_female_no_insurance = ifelse(
          !is.na(female_no_insuranceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_no_insuranceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        female_labor_force_raw = female_labor_forceE,
        female_employed_raw = female_employedE,
        female_poverty_raw = female_povertyE,
        female_never_married_raw = female_never_marriedE,
        female_married_raw = female_marriedE,
        female_divorced_raw = female_divorcedE,
        female_widowed_raw = female_widowedE,
        female_no_insurance_raw = female_no_insuranceE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw,
          # Employment
          pct_female_in_labor_force, pct_female_employed,
          female_labor_force_raw, female_employed_raw,
          # Poverty
          pct_female_poverty, female_poverty_raw,
          # Marital status
          pct_female_never_married, pct_female_married, pct_female_divorced, pct_female_widowed,
          female_never_married_raw, female_married_raw, female_divorced_raw, female_widowed_raw,
          # Health insurance
          pct_female_no_insurance, female_no_insurance_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show a sample of successful data with more details
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Total females: {good_county$total_female_raw}, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Employment: {good_county$pct_female_in_labor_force}% in labor force, {good_county$pct_female_employed}% employment rate")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's+, Poverty: {good_county$pct_female_poverty}%")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Comprehensive Demographic County Popups with Female Data
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 360px;'>",
    "<div style='background: linear-gradient(135deg, #d63384 0%, #6f42c1 100%); color: white; padding: 12px; margin: -10px -10px 12px -10px; border-radius: 8px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #2196f3;'>",
    "<h4 style='margin: 0 0 4px 0; color: #1976d2; font-size: 13px;'>👩‍⚕️ Healthcare Providers</h4>",
    "<p style='margin: 0; font-size: 12px; font-weight: bold;'>", physician_counts, " physicians in your dataset</p>",
    "</div>",
    
    # Female population overview
    "<div style='background: #f8f9fa; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #d63384;'>",
    "<h4 style='margin: 0 0 6px 0; color: #d63384; font-size: 13px;'>👩 Female Population Overview</h4>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>Total Females:</strong> ", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "% of population")), ")</p>",
    "</div>",
    
    # Female age demographics
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>📊 Female Age Demographics</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Female under 18
    "<div style='background: #fff3cd; padding: 8px; border-radius: 6px; border-left: 3px solid #ffc107;'>",
    "<div style='font-weight: bold; color: #856404;'>🧒 Females Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # Female over 65
    "<div style='background: #d1ecf1; padding: 8px; border-radius: 6px; border-left: 3px solid #17a2b8;'>",
    "<div style='font-weight: bold; color: #0c5460;'>👵 Females Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female education
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>🎓 Female Education</h4>",
    "<div style='background: #d4edda; padding: 10px; border-radius: 6px; border-left: 3px solid #28a745;'>",
    "<div style='display: flex; justify-content: space-between; align-items: center;'>",
    "<div>",
    "<div style='font-weight: bold; color: #155724; font-size: 13px;'>👩‍🎓 Females with Bachelor's Degree+</div>",
    "<div style='font-size: 10px; color: #6c757d; margin-top: 2px;'>% of total female population</div>",
    "</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #155724;'>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), "</div>",
    "</div>",
    "<div style='font-size: 11px; color: #6c757d; margin-top: 4px;'>", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0("(", format(counties$female_bachelor_plus_raw, big.mark = ","), " females with degrees)")), "</div>",
    "</div>",
    "</div>",
    
    # Female employment
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>💼 Female Employment</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Labor force participation
    "<div style='background: #e1f5fe; padding: 8px; border-radius: 6px; border-left: 3px solid #0277bd;'>",
    "<div style='font-weight: bold; color: #01579b;'>👩‍💼 In Labor Force</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #01579b;'>", 
    ifelse(is.na(counties$pct_female_in_labor_force), "N/A", paste0(counties$pct_female_in_labor_force, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_labor_force_raw), "", paste0("(", format(counties$female_labor_force_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # Employment rate
    "<div style='background: #e8f5e8; padding: 8px; border-radius: 6px; border-left: 3px solid #2e7d32;'>",
    "<div style='font-weight: bold; color: #1b5e20;'>✅ Employment Rate</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #1b5e20;'>", 
    ifelse(is.na(counties$pct_female_employed), "N/A", paste0(counties$pct_female_employed, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>of females in labor force</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female marital status
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>💒 Female Marital Status</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 11px;'>",
    
    # Never married
    "<div style='background: #fff0e6; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-weight: bold; color: #bf5700;'>Single</div>",
    "<div style='font-size: 14px; font-weight: bold; color: #bf5700;'>", 
    ifelse(is.na(counties$pct_female_never_married), "N/A", paste0(counties$pct_female_never_married, "%")), "</div>",
    "</div>",
    
    # Married
    "<div style='background: #f3e5f5; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-weight: bold; color: #7b1fa2;'>Married</div>",
    "<div style='font-size: 14px; font-weight: bold; color: #7b1fa2;'>", 
    ifelse(is.na(counties$pct_female_married), "N/A", paste0(counties$pct_female_married, "%")), "</div>",
    "</div>",
    
    # Divorced
    "<div style='background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-weight: bold; color: #c62828;'>Divorced</div>",
    "<div style='font-size: 14px; font-weight: bold; color: #c62828;'>", 
    ifelse(is.na(counties$pct_female_divorced), "N/A", paste0(counties$pct_female_divorced, "%")), "</div>",
    "</div>",
    
    # Widowed
    "<div style='background: #f1f8e9; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-weight: bold; color: #558b2f;'>Widowed</div>",
    "<div style='font-size: 14px; font-weight: bold; color: #558b2f;'>", 
    ifelse(is.na(counties$pct_female_widowed), "N/A", paste0(counties$pct_female_widowed, "%")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female poverty and health insurance
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>💰 Economic & Health Status</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Poverty rate
    "<div style='background: #ffebee; padding: 8px; border-radius: 6px; border-left: 3px solid #d32f2f;'>",
    "<div style='font-weight: bold; color: #b71c1c;'>📉 In Poverty</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #b71c1c;'>", 
    ifelse(is.na(counties$pct_female_poverty), "N/A", paste0(counties$pct_female_poverty, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_poverty_raw), "", paste0("(", format(counties$female_poverty_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # No health insurance
    "<div style='background: #fff3e0; padding: 8px; border-radius: 6px; border-left: 3px solid #f57c00;'>",
    "<div style='font-weight: bold; color: #e65100;'>🏥 No Insurance</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #e65100;'>", 
    ifelse(is.na(counties$pct_female_no_insurance), "N/A", paste0(counties$pct_female_no_insurance, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_no_insurance_raw), "", paste0("(", format(counties$female_no_insurance_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    "<div style='font-size: 10px; color: #6c757d; text-align: center; margin-top: 12px; border-top: 1px solid #dee2e6; padding-top: 6px;'>",
    "📊 Data: 2022 American Community Survey 5-Year Estimates",
    "</div>",
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# This will now show comprehensive female demographics
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)

# Function 1912 ----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define reliable female-specific demographic variables (tested codes)
    female_demographic_variables <- c(
      # Basic female population (reliable)
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_18_to_64 = "B01001_030",       # Female 18 to 64
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education (reliable)
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Female employment (reliable codes)
      female_labor_force = "B23001_029",    # Female in labor force
      female_employed = "B23001_030",       # Female employed
      
      # Female poverty (reliable)
      female_poverty = "B17001_031",        # Female in poverty
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check if we have the key variables (fix column name format)
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_employment_vars <- all(c("female_labor_forceE", "female_employedE") %in% names(demographic_data))
      has_poverty_var <- "female_povertyE" %in% names(demographic_data)
      logger::log_info("Has total_female: {has_total_female}, Has age variables: {has_age_vars}")
      logger::log_info("Has employment variables: {has_employment_vars}, Has poverty variable: {has_poverty_var}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate reliable female-specific percentages and metrics
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages (% of total female population)
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Employment percentages
        pct_female_in_labor_force = ifelse(
          !is.na(female_labor_forceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_labor_forceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_employed = ifelse(
          !is.na(female_employedE) & !is.na(female_labor_forceE) & female_labor_forceE > 0,
          round((female_employedE / female_labor_forceE) * 100, 1),
          NA_real_
        ),
        
        # Poverty percentage
        pct_female_poverty = ifelse(
          !is.na(female_povertyE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_povertyE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        female_labor_force_raw = female_labor_forceE,
        female_employed_raw = female_employedE,
        female_poverty_raw = female_povertyE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw,
          # Employment
          pct_female_in_labor_force, pct_female_employed,
          female_labor_force_raw, female_employed_raw,
          # Poverty
          pct_female_poverty, female_poverty_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show a sample of successful data with more details
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Total females: {good_county$total_female_raw}, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Employment: {good_county$pct_female_in_labor_force}% in labor force, {good_county$pct_female_employed}% employment rate")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's+, Poverty: {good_county$pct_female_poverty}%")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Reliable Demographic County Popups with Female Data
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 360px;'>",
    "<div style='background: linear-gradient(135deg, #d63384 0%, #6f42c1 100%); color: white; padding: 12px; margin: -10px -10px 12px -10px; border-radius: 8px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #2196f3;'>",
    "<h4 style='margin: 0 0 4px 0; color: #1976d2; font-size: 13px;'>👩‍⚕️ Healthcare Providers</h4>",
    "<p style='margin: 0; font-size: 12px; font-weight: bold;'>", physician_counts, " physicians in your dataset</p>",
    "</div>",
    
    # Female population overview
    "<div style='background: #f8f9fa; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #d63384;'>",
    "<h4 style='margin: 0 0 6px 0; color: #d63384; font-size: 13px;'>👩 Female Population Overview</h4>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>Total Females:</strong> ", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "% of population")), ")</p>",
    "</div>",
    
    # Female age demographics
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>📊 Female Age Demographics</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Female under 18
    "<div style='background: #fff3cd; padding: 8px; border-radius: 6px; border-left: 3px solid #ffc107;'>",
    "<div style='font-weight: bold; color: #856404;'>🧒 Females Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # Female over 65
    "<div style='background: #d1ecf1; padding: 8px; border-radius: 6px; border-left: 3px solid #17a2b8;'>",
    "<div style='font-weight: bold; color: #0c5460;'>👵 Females Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female education
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>🎓 Female Education</h4>",
    "<div style='background: #d4edda; padding: 10px; border-radius: 6px; border-left: 3px solid #28a745;'>",
    "<div style='display: flex; justify-content: space-between; align-items: center;'>",
    "<div>",
    "<div style='font-weight: bold; color: #155724; font-size: 13px;'>👩‍🎓 Females with Bachelor's Degree+</div>",
    "<div style='font-size: 10px; color: #6c757d; margin-top: 2px;'>% of total female population</div>",
    "</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #155724;'>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), "</div>",
    "</div>",
    "<div style='font-size: 11px; color: #6c757d; margin-top: 4px;'>", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0("(", format(counties$female_bachelor_plus_raw, big.mark = ","), " females with degrees)")), "</div>",
    "</div>",
    "</div>",
    
    # Female employment
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>💼 Female Employment</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Labor force participation
    "<div style='background: #e1f5fe; padding: 8px; border-radius: 6px; border-left: 3px solid #0277bd;'>",
    "<div style='font-weight: bold; color: #01579b;'>👩‍💼 In Labor Force</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #01579b;'>", 
    ifelse(is.na(counties$pct_female_in_labor_force), "N/A", paste0(counties$pct_female_in_labor_force, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_labor_force_raw), "", paste0("(", format(counties$female_labor_force_raw, big.mark = ","), " females)")), "</div>",
    "</div>",
    
    # Employment rate
    "<div style='background: #e8f5e8; padding: 8px; border-radius: 6px; border-left: 3px solid #2e7d32;'>",
    "<div style='font-weight: bold; color: #1b5e20;'>✅ Employment Rate</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #1b5e20;'>", 
    ifelse(is.na(counties$pct_female_employed), "N/A", paste0(counties$pct_female_employed, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>of females in labor force</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female poverty
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>💰 Economic Status</h4>",
    "<div style='background: #ffebee; padding: 10px; border-radius: 6px; border-left: 3px solid #d32f2f;'>",
    "<div style='display: flex; justify-content: space-between; align-items: center;'>",
    "<div>",
    "<div style='font-weight: bold; color: #b71c1c; font-size: 13px;'>📉 Females Living in Poverty</div>",
    "<div style='font-size: 10px; color: #6c757d; margin-top: 2px;'>% of total female population</div>",
    "</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #b71c1c;'>", 
    ifelse(is.na(counties$pct_female_poverty), "N/A", paste0(counties$pct_female_poverty, "%")), "</div>",
    "</div>",
    "<div style='font-size: 11px; color: #6c757d; margin-top: 4px;'>", 
    ifelse(is.na(counties$female_poverty_raw), "", paste0("(", format(counties$female_poverty_raw, big.mark = ","), " females in poverty)")), "</div>",
    "</div>",
    "</div>",
    
    "<div style='font-size: 10px; color: #6c757d; text-align: center; margin-top: 12px; border-top: 1px solid #dee2e6; padding-top: 6px;'>",
    "📊 Data: 2022 American Community Survey 5-Year Estimates",
    "</div>",
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# execute ----
# Now with only reliable Census variables - should work!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)


# 1919 Funciton -----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define ONLY the most reliable female demographic variables
    female_demographic_variables <- c(
      # Basic female population (these are definitely reliable)
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education (reliable)
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check if we have the key variables (fix column name format)
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_education_var <- "female_bachelor_plusE" %in% names(demographic_data)
      logger::log_info("Has total_female: {has_total_female}, Has age variables: {has_age_vars}")
      logger::log_info("Has education variable: {has_education_var}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate ONLY reliable female demographics (no employment/poverty for now)
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages (% of total female population)
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage (% of total female population)
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show a sample of successful data (simplified)
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Total females: {good_county$total_female_raw}, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's degree or higher")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Reliable Demographic County Popups (Age + Education Only)
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 360px;'>",
    "<div style='background: linear-gradient(135deg, #d63384 0%, #6f42c1 100%); color: white; padding: 12px; margin: -10px -10px 12px -10px; border-radius: 8px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #2196f3;'>",
    "<h4 style='margin: 0 0 4px 0; color: #1976d2; font-size: 13px;'>👩‍⚕️ Healthcare Providers</h4>",
    "<p style='margin: 0; font-size: 12px; font-weight: bold;'>", physician_counts, " physicians in your dataset</p>",
    "</div>",
    
    # Female population overview
    "<div style='background: #f8f9fa; padding: 8px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #d63384;'>",
    "<h4 style='margin: 0 0 6px 0; color: #d63384; font-size: 13px;'>👩 Female Population Overview</h4>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>Total Females:</strong> ", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "% of population")), ")</p>",
    "</div>",
    
    # Female age demographics
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>📊 Female Age Demographics</h4>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 12px;'>",
    
    # Female under 18
    "<div style='background: #fff3cd; padding: 10px; border-radius: 6px; border-left: 3px solid #ffc107;'>",
    "<div style='font-weight: bold; color: #856404; font-size: 13px;'>🧒 Females Under 18</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #856404; margin: 4px 0;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), " females)")), "</div>",
    "<div style='font-size: 9px; color: #856404; margin-top: 2px;'>Pediatric population</div>",
    "</div>",
    
    # Female over 65
    "<div style='background: #d1ecf1; padding: 10px; border-radius: 6px; border-left: 3px solid #17a2b8;'>",
    "<div style='font-weight: bold; color: #0c5460; font-size: 13px;'>👵 Females Over 65</div>",
    "<div style='font-size: 18px; font-weight: bold; color: #0c5460; margin: 4px 0;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 10px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), " females)")), "</div>",
    "<div style='font-size: 9px; color: #0c5460; margin-top: 2px;'>Geriatric population</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Female education
    "<div style='padding: 8px 0;'>",
    "<h4 style='margin: 8px 0 6px 0; color: #6f42c1; font-size: 14px; border-bottom: 2px solid #e9ecef; padding-bottom: 4px;'>🎓 Female Education</h4>",
    "<div style='background: #d4edda; padding: 12px; border-radius: 6px; border-left: 3px solid #28a745;'>",
    "<div style='display: flex; justify-content: space-between; align-items: center;'>",
    "<div>",
    "<div style='font-weight: bold; color: #155724; font-size: 14px;'>👩‍🎓 Females with Bachelor's Degree+</div>",
    "<div style='font-size: 10px; color: #6c757d; margin-top: 2px;'>% of total female population</div>",
    "<div style='font-size: 9px; color: #155724; margin-top: 2px;'>Higher education indicator</div>",  
    "</div>",
    "<div style='font-size: 20px; font-weight: bold; color: #155724;'>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), "</div>",
    "</div>",
    "<div style='font-size: 11px; color: #6c757d; margin-top: 6px;'>", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0("(", format(counties$female_bachelor_plus_raw, big.mark = ","), " females with degrees)")), "</div>",
    "</div>",
    "</div>",
    
    # Healthcare planning note
    "<div style='background: #e8f4fd; padding: 10px; margin: 8px 0; border-radius: 6px; border-left: 4px solid #0288d1;'>",
    "<div style='font-size: 12px; color: #01579b;'>",
    "<strong>🏥 Healthcare Planning:</strong> Age demographics help plan pediatric, reproductive, and geriatric services. Education levels correlate with health literacy and preventive care engagement.",
    "</div>",
    "</div>",
    
    "<div style='font-size: 10px; color: #6c757d; text-align: center; margin-top: 12px; border-top: 1px solid #dee2e6; padding-top: 6px;'>",
    "📊 Data: 2022 American Community Survey 5-Year Estimates",
    "</div>",
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# This should now show realistic percentages!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)


# function 1923 ----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define ONLY the most reliable female demographic variables
    female_demographic_variables <- c(
      # Basic female population (these are definitely reliable)
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education (reliable)
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check if we have the key variables (fix column name format)
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_education_var <- "female_bachelor_plusE" %in% names(demographic_data)
      logger::log_info("Has total_female: {has_total_female}, Has age variables: {has_age_vars}")
      logger::log_info("Has education variable: {has_education_var}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate ONLY reliable female demographics (no employment/poverty for now)
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages (% of total female population)
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage (% of total female population)
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show a sample of successful data (simplified)
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Total females: {good_county$total_female_raw}, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's degree or higher")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Compact, Readable County Popups 
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px; font-size: 12px;'>",
    
    # Header - more compact
    "<div style='background: #6f42c1; color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count - compact
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_counts, 
    "</div>",
    
    # Female population - compact
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females:</strong><br/>", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "%")), " of population)",
    "</div>",
    
    # Age demographics - side by side, compact
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    
    # Under 18
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    
    # Over 65
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    
    "</div>",
    "</div>",
    
    # Education - compact
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+:</strong><br/>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0(" (", format(counties$female_bachelor_plus_raw, big.mark = ","), ")")),
    "</div>",
    
    # Healthcare note - very compact
    "<div style='background: #f0f8ff; padding: 4px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4fc3f7;'>",
    "<div style='font-size: 10px; color: #0277bd;'>",
    "<strong>💡 Healthcare Planning:</strong> Age groups help plan pediatric/geriatric services",
    "</div>",
    "</div>",
    
    # Data source - tiny
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 2022 American Community Survey",
    "</div>",
    
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# Now with compact, readable popups!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)

# Function 1951 ----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define comprehensive female-specific demographic variables
    female_demographic_variables <- c(
      # Basic female population
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Female marital status (15 years and over)
      female_never_married = "B12001_010",  # Female never married
      female_married = "B12001_014",        # Female married
      female_divorced = "B12001_018",       # Female divorced
      female_widowed = "B12001_019",        # Female widowed
      
      # Female health insurance coverage
      female_with_insurance = "B27001_014", # Female with health insurance
      female_no_insurance = "B27001_017",   # Female with no health insurance
      
      # Female fertility
      female_births_12mo = "B13016_010",    # Women 15-50 who had birth in past 12 months
      
      # Female disability status
      female_with_disability = "B18101_004", # Female with a disability
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check comprehensive variables
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_education_var <- "female_bachelor_plusE" %in% names(demographic_data)
      has_marital_vars <- all(c("female_never_marriedE", "female_marriedE") %in% names(demographic_data))
      has_insurance_vars <- all(c("female_with_insuranceE", "female_no_insuranceE") %in% names(demographic_data))
      has_fertility_var <- "female_births_12moE" %in% names(demographic_data)
      has_disability_var <- "female_with_disabilityE" %in% names(demographic_data)
      
      logger::log_info("Variable availability check:")
      logger::log_info("  - Total female: {has_total_female}")
      logger::log_info("  - Age variables: {has_age_vars}")
      logger::log_info("  - Education: {has_education_var}")
      logger::log_info("  - Marital status: {has_marital_vars}")
      logger::log_info("  - Health insurance: {has_insurance_vars}")
      logger::log_info("  - Recent births: {has_fertility_var}")
      logger::log_info("  - Disability status: {has_disability_var}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate comprehensive female demographics with safe error handling
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Marital status percentages (safe calculations)
        pct_female_never_married = ifelse(
          !is.na(female_never_marriedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_never_marriedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_married = ifelse(
          !is.na(female_marriedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_marriedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_divorced = ifelse(
          !is.na(female_divorcedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_divorcedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_widowed = ifelse(
          !is.na(female_widowedE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_widowedE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Health insurance percentages
        pct_female_with_insurance = ifelse(
          !is.na(female_with_insuranceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_with_insuranceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_no_insurance = ifelse(
          !is.na(female_no_insuranceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_no_insuranceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Fertility and disability percentages
        pct_female_births_12mo = ifelse(
          !is.na(female_births_12moE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_births_12moE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_with_disability = ifelse(
          !is.na(female_with_disabilityE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_with_disabilityE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        female_never_married_raw = female_never_marriedE,
        female_married_raw = female_marriedE,
        female_divorced_raw = female_divorcedE,
        female_widowed_raw = female_widowedE,
        female_with_insurance_raw = female_with_insuranceE,
        female_no_insurance_raw = female_no_insuranceE,
        female_births_12mo_raw = female_births_12moE,
        female_with_disability_raw = female_with_disabilityE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw,
          # Marital status
          pct_female_never_married, pct_female_married, pct_female_divorced, pct_female_widowed,
          female_never_married_raw, female_married_raw, female_divorced_raw, female_widowed_raw,
          # Health insurance
          pct_female_with_insurance, pct_female_no_insurance,
          female_with_insurance_raw, female_no_insurance_raw,
          # Fertility and disability
          pct_female_births_12mo, pct_female_with_disability,
          female_births_12mo_raw, female_with_disability_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show comprehensive sample of successful data
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Demographics: {good_county$total_female_raw} total females, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's degree+")
        logger::log_info("  - Marital: {good_county$pct_female_married}% married, {good_county$pct_female_never_married}% single")
        logger::log_info("  - Health: {good_county$pct_female_with_insurance}% insured, {good_county$pct_female_births_12mo}% had births in 12mo")
        logger::log_info("  - Disability: {good_county$pct_female_with_disability}% with disability")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create Comprehensive Female Demographics County Popups
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 290px; font-size: 12px;'>",
    
    # Header
    "<div style='background: #6f42c1; color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_counts, 
    "</div>",
    
    # Female population
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females:</strong><br/>", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "%")), " of population)",
    "</div>",
    
    # Age demographics
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Education
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+:</strong><br/>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0(" (", format(counties$female_bachelor_plus_raw, big.mark = ","), ")")),
    "</div>",
    
    # Marital status
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💒 Marital Status</strong>",
    "<div style='display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin: 4px 0; font-size: 11px;'>",
    "<div style='background: #fff0e6; padding: 4px; border-radius: 3px; text-align: center;'>",
    "<div style='font-weight: bold; color: #bf5700;'>Single</div>",
    "<div style='font-size: 13px; font-weight: bold; color: #bf5700;'>", 
    ifelse(is.na(counties$pct_female_never_married), "N/A", paste0(counties$pct_female_never_married, "%")), "</div>",
    "</div>",
    "<div style='background: #f3e5f5; padding: 4px; border-radius: 3px; text-align: center;'>",
    "<div style='font-weight: bold; color: #7b1fa2;'>Married</div>",
    "<div style='font-size: 13px; font-weight: bold; color: #7b1fa2;'>", 
    ifelse(is.na(counties$pct_female_married), "N/A", paste0(counties$pct_female_married, "%")), "</div>",
    "</div>",
    "<div style='background: #ffebee; padding: 4px; border-radius: 3px; text-align: center;'>",
    "<div style='font-weight: bold; color: #c62828;'>Divorced</div>",
    "<div style='font-size: 13px; font-weight: bold; color: #c62828;'>", 
    ifelse(is.na(counties$pct_female_divorced), "N/A", paste0(counties$pct_female_divorced, "%")), "</div>",
    "</div>",
    "<div style='background: #f1f8e9; padding: 4px; border-radius: 3px; text-align: center;'>",
    "<div style='font-weight: bold; color: #558b2f;'>Widowed</div>",
    "<div style='font-size: 13px; font-weight: bold; color: #558b2f;'>", 
    ifelse(is.na(counties$pct_female_widowed), "N/A", paste0(counties$pct_female_widowed, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Health insurance
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>🏥 Health Insurance</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Insured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties$pct_female_with_insurance), "N/A", paste0(counties$pct_female_with_insurance, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #d32f2f; font-weight: bold;'>❌ Uninsured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #d32f2f;'>", 
    ifelse(is.na(counties$pct_female_no_insurance), "N/A", paste0(counties$pct_female_no_insurance, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Recent births and disability
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>👶 Recent Births & Disability</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fce4ec; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #c2185b; font-weight: bold;'>👶 Births 12mo</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #c2185b;'>", 
    ifelse(is.na(counties$pct_female_births_12mo), "N/A", paste0(counties$pct_female_births_12mo, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #e3f2fd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #1976d2; font-weight: bold;'>♿ Disability</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #1976d2;'>", 
    ifelse(is.na(counties$pct_female_with_disability), "N/A", paste0(counties$pct_female_with_disability, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Data source
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 2022 American Community Survey",
    "</div>",
    
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# Now with comprehensive female demographics!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)

# Function 2000 -----
#' Simple Physician Map with Female Demographics in County Popups
#' 
#' Creates a physician map with clickable counties showing detailed female
#' demographic data including age distributions and education levels.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns
#' @param include_county_boundaries Logical to include counties with demographics
#' @param include_female_demographics Logical to include detailed female demographic data
#' @param verbose Logical for detailed logging
#' @return Interactive leaflet map
#' 
#' @examples
#' # Basic version without demographics
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   verbose = TRUE
#' )
#' 
#' # Enhanced version with female demographics (requires Census API key)
#' # Get free key at: https://api.census.gov/data/key_signup.html
#' # tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE)
#' simple_physician_map(
#'   physician_geodata = full_results,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn
#' @importFrom dplyr mutate case_when filter left_join select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @export
simple_physician_map <- function(physician_geodata,
                                 include_county_boundaries = TRUE,
                                 include_female_demographics = TRUE,
                                 verbose = TRUE) {
  
  if (verbose) {
    logger::log_info("Starting simple physician map creation")
    logger::log_info("Input data dimensions: {nrow(physician_geodata)} rows, {ncol(physician_geodata)} columns")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_geodata)) {
    assertthat::assert_that(all(c("latitude", "longitude") %in% names(physician_geodata)),
                            msg = "Data must contain 'latitude' and 'longitude' columns")
    
    clean_data <- physician_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      logger::log_info("Converting {nrow(clean_data)} rows to spatial data")
    }
    
    spatial_data <- sf::st_as_sf(
      clean_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_data <- physician_geodata
  }
  
  # Extract coordinates for mapping
  coords <- sf::st_coordinates(spatial_data)
  spatial_data$longitude <- coords[, "X"]
  spatial_data$latitude <- coords[, "Y"]
  
  # Create basic popups
  spatial_data <- spatial_data %>%
    dplyr::mutate(
      popup_content = paste0(
        "<b>", 
        ifelse("pfname" %in% names(.) & "plname" %in% names(.), 
               paste("Dr.", pfname, plname), 
               "Physician Details"), 
        "</b><br/>",
        ifelse("npi" %in% names(.), paste0("NPI: ", npi, "<br/>"), ""),
        ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
               paste0("Location: ", pmailcityname, ", ", pmailstatename), 
               "Location information available")
      )
    )
  
  # Create base map
  physician_map <- leaflet::leaflet(spatial_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~popup_content,
      group = "Physicians"
    )
  
  # Add simple county boundaries if requested
  if (include_county_boundaries) {
    physician_map <- add_simple_counties(physician_map, spatial_data, include_female_demographics, verbose)
  }
  
  # Add controls and fit bounds
  overlay_groups <- "Physicians"
  if (include_county_boundaries) {
    overlay_groups <- c(overlay_groups, "Counties")
  }
  
  physician_map <- physician_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_data$longitude) - 0.1,
      lat1 = min(spatial_data$latitude) - 0.1,
      lng2 = max(spatial_data$longitude) + 0.1,
      lat2 = max(spatial_data$latitude) + 0.1
    )
  
  if (verbose) {
    logger::log_info("Simple physician map created successfully")
  }
  
  return(physician_map)
}

#' Add Simple County Boundaries with Optional Female Demographics
#' @noRd
add_simple_counties <- function(physician_map, spatial_data, include_female_demographics, verbose) {
  
  if (verbose) {
    logger::log_info("Attempting to add county boundaries with female demographics: {include_female_demographics}")
  }
  
  # Check if tigris is available for automatic county download
  if (requireNamespace("tigris", quietly = TRUE)) {
    
    # Get state from physician data
    state_col <- NULL
    for (col in c("pmailstatename", "plocstatename", "state")) {
      if (col %in% names(spatial_data)) {
        state_col <- col
        break
      }
    }
    
    if (!is.null(state_col)) {
      physician_attributes <- sf::st_drop_geometry(spatial_data)
      primary_state <- names(sort(table(physician_attributes[[state_col]]), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info("Downloading county boundaries for: {primary_state}")
      }
      
      tryCatch({
        counties <- tigris::counties(state = primary_state, cb = TRUE)
        counties <- sf::st_transform(counties, crs = 4326)
        
        # Add female demographics if requested
        if (include_female_demographics) {
          counties <- add_female_demographics_to_counties(counties, primary_state, verbose)
        }
        
        # Count physicians in each county (simplified approximation)
        physician_locations <- sf::st_drop_geometry(spatial_data)
        physician_counts <- sapply(1:nrow(counties), function(i) {
          # Simple approximation - count by matching county names
          county_name <- counties$NAME[i]
          if ("geocoding_county" %in% names(physician_locations)) {
            sum(grepl(county_name, physician_locations$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
          } else {
            0
          }
        })
        
        # Create enhanced popup content with demographics
        if (include_female_demographics && "pct_female_under_18" %in% names(counties)) {
          county_popups <- create_demographic_county_popups(counties, physician_counts)
        } else {
          county_popups <- create_basic_county_popups(counties, physician_counts)
        }
        
        physician_map <- physician_map %>%
          leaflet::addPolygons(
            data = counties,
            weight = 1,
            color = "#888888",
            fillOpacity = 0.1,
            fillColor = "#lightblue",
            popup = county_popups,
            group = "Counties",
            highlightOptions = leaflet::highlightOptions(
              weight = 2,
              color = "#666666",
              fillOpacity = 0.3,
              bringToFront = FALSE
            )
          )
        
        if (verbose) {
          logger::log_info("Added {nrow(counties)} county boundaries with demographics: {include_female_demographics}")
        }
        
      }, error = function(e) {
        if (verbose) {
          logger::log_warn("Could not download counties: {e$message}")
        }
      })
    }
  } else {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
  }
  
  return(physician_map)
}

#' Add Female Demographics to County Data
#' @noRd
add_female_demographics_to_counties <- function(counties, primary_state, verbose) {
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(counties)
  }
  
  if (verbose) {
    logger::log_info("Downloading female demographic data for {primary_state} counties")
  }
  
  # First test the API connection with a simple variable
  tryCatch({
    test_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = c(total_pop = "B01003_001"),
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Census API connection successful - got {nrow(test_data)} counties for basic test")
    }
  }, error = function(e) {
    if (verbose) {
      logger::log_error("Census API connection failed: {e$message}")
    }
    return(counties)
  })
  
  tryCatch({
    # Define comprehensive female and economic demographic variables
    female_demographic_variables <- c(
      # Basic female population
      total_female = "B01001_026",          # Total female population
      female_under_5 = "B01001_027",        # Female under 5
      female_5_to_17 = "B01001_028",        # Female 5 to 17 years  
      female_65_plus = "B01001_044",        # Female 65 years and over
      
      # Female education
      female_bachelor_plus = "B15002_032",  # Female with bachelor's degree or higher
      
      # Female health insurance (better variables)
      female_with_insurance = "B27001_014", # Female with health insurance coverage
      female_total_insurance = "B27001_013", # Total female population for insurance calculation
      
      # Female fertility
      female_births_12mo = "B13016_010",    # Women 15-50 who had birth in past 12 months
      
      # Female disability status
      female_with_disability = "B18101_004", # Female with a disability
      
      # Economic variables (household level)
      median_household_income = "B19013_001", # Median household income
      per_capita_income = "B19301_001",       # Per capita income
      poverty_rate = "B17001_002",            # Population in poverty
      total_pop_poverty = "B17001_001",       # Total population for poverty calculation
      
      # Female employment
      female_labor_force = "B23001_029",      # Female in civilian labor force
      female_employed = "B23001_030",         # Female employed
      
      # Context
      total_pop = "B01003_001"              # Total population
    )
    
    # Get demographic data for the state
    demographic_data <- tidycensus::get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = 2022,
      survey = "acs5",
      output = "wide"
    )
    
    if (verbose) {
      logger::log_info("Downloaded demographic data with {nrow(demographic_data)} counties and {ncol(demographic_data)} variables")
      logger::log_info("Sample variables: {paste(names(demographic_data)[1:min(10, ncol(demographic_data))], collapse=', ')}")
      
      # Check comprehensive variables
      has_total_female <- "total_femaleE" %in% names(demographic_data)
      has_age_vars <- all(c("female_under_5E", "female_5_to_17E", "female_65_plusE") %in% names(demographic_data))
      has_education_var <- "female_bachelor_plusE" %in% names(demographic_data)
      has_insurance_vars <- all(c("female_with_insuranceE", "female_total_insuranceE") %in% names(demographic_data))
      has_economic_vars <- all(c("median_household_incomeE", "per_capita_incomeE", "poverty_rateE") %in% names(demographic_data))
      has_employment_vars <- all(c("female_labor_forceE", "female_employedE") %in% names(demographic_data))
      has_fertility_var <- "female_births_12moE" %in% names(demographic_data)
      has_disability_var <- "female_with_disabilityE" %in% names(demographic_data)
      
      logger::log_info("Variable availability check:")
      logger::log_info("  - Total female: {has_total_female}")
      logger::log_info("  - Age variables: {has_age_vars}")
      logger::log_info("  - Education: {has_education_var}")
      logger::log_info("  - Health insurance: {has_insurance_vars}")
      logger::log_info("  - Economic indicators: {has_economic_vars}")
      logger::log_info("  - Employment: {has_employment_vars}")
      logger::log_info("  - Recent births: {has_fertility_var}")
      logger::log_info("  - Disability status: {has_disability_var}")
      
      # Show sample data for first county
      if (nrow(demographic_data) > 0) {
        sample_county <- demographic_data[1, ]
        logger::log_info("Sample county: {sample_county$NAME}, Total females: {sample_county$total_femaleE}")
      }
    }
    
    # Calculate comprehensive female and economic demographics
    demographic_data <- demographic_data %>%
      dplyr::mutate(
        # Basic female age calculations
        female_under_18_count = ifelse(
          !is.na(female_under_5E) & !is.na(female_5_to_17E),
          female_under_5E + female_5_to_17E,
          0
        ),
        
        # Age percentages
        pct_female_under_18 = ifelse(
          !is.na(total_femaleE) & total_femaleE > 0,
          round((female_under_18_count / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_over_65 = ifelse(
          !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_65_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_of_total = ifelse(
          !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
          round((total_femaleE / total_popE) * 100, 1),
          NA_real_
        ),
        
        # Education percentage
        pct_female_bachelor_plus = ifelse(
          !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_bachelor_plusE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Health insurance (fixed calculation using proper denominator)
        pct_female_with_insurance = ifelse(
          !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
          round((female_with_insuranceE / female_total_insuranceE) * 100, 1),
          NA_real_
        ),
        pct_female_no_insurance = ifelse(
          !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
          round(((female_total_insuranceE - female_with_insuranceE) / female_total_insuranceE) * 100, 1),
          NA_real_
        ),
        
        # Employment percentages  
        pct_female_in_labor_force = ifelse(
          !is.na(female_labor_forceE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_labor_forceE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_employed = ifelse(
          !is.na(female_employedE) & !is.na(female_labor_forceE) & female_labor_forceE > 0,
          round((female_employedE / female_labor_forceE) * 100, 1),
          NA_real_
        ),
        
        # Economic indicators
        median_household_income_formatted = ifelse(
          !is.na(median_household_incomeE),
          paste0("$", format(median_household_incomeE, big.mark = ",")),
          "N/A"
        ),
        per_capita_income_formatted = ifelse(
          !is.na(per_capita_incomeE),
          paste0("$", format(per_capita_incomeE, big.mark = ",")),
          "N/A"
        ),
        pct_poverty = ifelse(
          !is.na(poverty_rateE) & !is.na(total_pop_povertyE) & total_pop_povertyE > 0,
          round((poverty_rateE / total_pop_povertyE) * 100, 1),
          NA_real_
        ),
        
        # Fertility and disability percentages
        pct_female_births_12mo = ifelse(
          !is.na(female_births_12moE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_births_12moE / total_femaleE) * 100, 1),
          NA_real_
        ),
        pct_female_with_disability = ifelse(
          !is.na(female_with_disabilityE) & !is.na(total_femaleE) & total_femaleE > 0,
          round((female_with_disabilityE / total_femaleE) * 100, 1),
          NA_real_
        ),
        
        # Raw counts for display
        female_under_18_raw = female_under_18_count,
        female_over_65_raw = female_65_plusE,
        female_bachelor_plus_raw = female_bachelor_plusE,
        female_with_insurance_raw = female_with_insuranceE,
        female_births_12mo_raw = female_births_12moE,
        female_with_disability_raw = female_with_disabilityE,
        female_labor_force_raw = female_labor_forceE,
        female_employed_raw = female_employedE,
        total_female_raw = total_femaleE
      )
    
    # Join demographic data to county boundaries
    if (verbose) {
      logger::log_info("Attempting to join demographic data to county boundaries")
      logger::log_info("Counties have GEOID, Demographics have GEOID: {all(c('GEOID') %in% names(counties)) && all(c('GEOID') %in% names(demographic_data))}")
    }
    
    counties_with_demo <- counties %>%
      dplyr::left_join(
        demographic_data %>% dplyr::select(
          GEOID, 
          # Age demographics
          pct_female_under_18, pct_female_over_65, pct_female_of_total,
          female_under_18_raw, female_over_65_raw, total_female_raw,
          # Education
          pct_female_bachelor_plus, female_bachelor_plus_raw,
          # Health insurance (fixed)
          pct_female_with_insurance, pct_female_no_insurance,
          female_with_insurance_raw,
          # Employment
          pct_female_in_labor_force, pct_female_employed,
          female_labor_force_raw, female_employed_raw,
          # Economic indicators
          median_household_income_formatted, per_capita_income_formatted, pct_poverty,
          # Fertility and disability
          pct_female_births_12mo, pct_female_with_disability,
          female_births_12mo_raw, female_with_disability_raw
        ),
        by = "GEOID"
      )
    
    # Check if join was successful and data looks good
    demo_counties_count <- sum(!is.na(counties_with_demo$total_female_raw))
    counties_with_good_data <- sum(!is.na(counties_with_demo$pct_female_under_18))
    
    if (verbose) {
      logger::log_info("Join results: {demo_counties_count} counties have raw female data, {counties_with_good_data} have calculated percentages")
      
      # Show comprehensive sample of successful data
      if (demo_counties_count > 0) {
        good_county <- counties_with_demo[!is.na(counties_with_demo$total_female_raw), ][1, ]
        logger::log_info("Sample successful county: {good_county$NAME}")
        logger::log_info("  - Demographics: {good_county$total_female_raw} total females, {good_county$pct_female_under_18}% under 18, {good_county$pct_female_over_65}% over 65")
        logger::log_info("  - Education: {good_county$pct_female_bachelor_plus}% with bachelor's degree+")
        logger::log_info("  - Economic: {good_county$median_household_income_formatted} median income, {good_county$pct_poverty}% poverty")
        logger::log_info("  - Employment: {good_county$pct_female_in_labor_force}% in labor force, {good_county$pct_female_employed}% employment rate")
        logger::log_info("  - Health: {good_county$pct_female_with_insurance}% insured, {good_county$pct_female_births_12mo}% had births in 12mo")
        logger::log_info("  - Disability: {good_county$pct_female_with_disability}% with disability")
      }
    }
    
    # Return counties with demo data if we got any good data
    if (counties_with_good_data > 0) {
      if (verbose) {
        logger::log_info("Successfully returning {nrow(counties_with_demo)} counties with demographic data")
      }
      return(counties_with_demo)
    } else {
      if (verbose) {
        logger::log_warn("No usable demographic data - returning basic counties")
      }
      return(counties)
    }
    
  }, error = function(e) {
    if (verbose) {
      logger::log_warn("Could not add female demographic data: {e$message}")
      logger::log_warn("Make sure you have a Census API key set up: tidycensus::census_api_key('YOUR_KEY', install = TRUE)")
    }
    return(counties)
  })
}

#' Create County Popups with Female Demographics and Economic Data
#' @noRd
create_demographic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 300px; font-size: 12px;'>",
    
    # Header
    "<div style='background: #6f42c1; color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties$NAME, " County, ", counties$STUSPS, "</h3>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_counts, 
    "</div>",
    
    # Female population
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females:</strong><br/>", 
    ifelse(is.na(counties$total_female_raw), "N/A", format(counties$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties$pct_female_of_total), "N/A", paste0(counties$pct_female_of_total, "%")), " of population)",
    "</div>",
    
    # Age demographics
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties$pct_female_under_18), "N/A", paste0(counties$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_under_18_raw), "", paste0("(", format(counties$female_under_18_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties$pct_female_over_65), "N/A", paste0(counties$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties$female_over_65_raw), "", paste0("(", format(counties$female_over_65_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Education
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+:</strong><br/>", 
    ifelse(is.na(counties$pct_female_bachelor_plus), "N/A", paste0(counties$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties$female_bachelor_plus_raw), "", paste0(" (", format(counties$female_bachelor_plus_raw, big.mark = ","), ")")),
    "</div>",
    
    # Economic indicators
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💰 Economic Indicators</strong>",
    "<div style='background: #f0f8ff; padding: 6px; margin: 4px 0; border-radius: 4px; border-left: 3px solid #1976d2;'>",
    "<div style='font-size: 11px;'>",
    "<strong>🏠 Median Household Income:</strong> ", counties$median_household_income_formatted, "<br/>",
    "<strong>👤 Per Capita Income:</strong> ", counties$per_capita_income_formatted, "<br/>",
    "<strong>📉 Poverty Rate:</strong> ", ifelse(is.na(counties$pct_poverty), "N/A", paste0(counties$pct_poverty, "%")),
    "</div>",
    "</div>",
    "</div>",
    
    # Female employment
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💼 Female Employment</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e1f5fe; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #01579b; font-weight: bold;'>👩‍💼 Labor Force</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #01579b;'>", 
    ifelse(is.na(counties$pct_female_in_labor_force), "N/A", paste0(counties$pct_female_in_labor_force, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Employed</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties$pct_female_employed), "N/A", paste0(counties$pct_female_employed, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Health insurance (fixed percentages)
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>🏥 Health Insurance</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Insured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties$pct_female_with_insurance), "N/A", paste0(counties$pct_female_with_insurance, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #d32f2f; font-weight: bold;'>❌ Uninsured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #d32f2f;'>", 
    ifelse(is.na(counties$pct_female_no_insurance), "N/A", paste0(counties$pct_female_no_insurance, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Recent births and disability
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>👶 Recent Births & Disability</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fce4ec; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #c2185b; font-weight: bold;'>👶 Births 12mo</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #c2185b;'>", 
    ifelse(is.na(counties$pct_female_births_12mo), "N/A", paste0(counties$pct_female_births_12mo, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #e3f2fd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #1976d2; font-weight: bold;'>♿ Disability</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #1976d2;'>", 
    ifelse(is.na(counties$pct_female_with_disability), "N/A", paste0(counties$pct_female_with_disability, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Data source
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 2022 American Community Survey",
    "</div>",
    
    "</div>"
  )
}

#' Create Basic County Popups (Fallback)
#' @noRd
create_basic_county_popups <- function(counties, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", counties$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", counties$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", counties$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", counties$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(counties$ALAND) / 2589988.11, 1), " sq miles</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>💧 Water Area:</strong> ", round(as.numeric(counties$AWATER) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 For female demographic data, install tidycensus and set up Census API key</p>",
    "</div>",
    "</div>"
  )
}

# Now with fixed health insurance + economic data!
simple_physician_map(
  physician_geodata = full_results,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  verbose = TRUE
)


# Multiple Years with County Data ----
#' Physician Map with Multi-Year Female Demographics and County Analysis
#' 
#' Creates an interactive physician map with clickable counties showing detailed female
#' demographic data from the American Community Survey. Users can select different
#' years (2010-2023) using radio button controls to see how demographics have changed
#' over time. The map includes comprehensive female demographics including age 
#' distributions, education levels, employment, health insurance, and economic indicators.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns and physician information
#' @param include_county_boundaries Logical to include counties with demographics (default: TRUE)
#' @param include_female_demographics Logical to include detailed female demographic data (default: TRUE)
#' @param acs_years Numeric vector of years to include (default: 2022, range: 2012-2023)
#' @param default_year Numeric year to display initially (default: 2022)
#' @param verbose Logical for detailed logging (default: TRUE)
#' 
#' @return Interactive leaflet map with year selector controls
#' 
#' @examples
#' # Example 1: Basic map with single year
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2022,
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 2: Multi-year comparison map (2018-2022)
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = c(2018, 2019, 2020, 2021, 2022),
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Full decade analysis (2012-2023)
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2012:2023,
#'   default_year = 2020,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr mutate case_when filter left_join select bind_rows
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions layersControlOptions
#' @importFrom tidycensus get_acs
#' @importFrom purrr map_dfr safely
#' @export
enhanced_physician_map_with_years <- function(physician_geodata,
                                              include_county_boundaries = TRUE,
                                              include_female_demographics = TRUE,
                                              acs_years = 2022,
                                              default_year = 2022,
                                              verbose = TRUE) {
  
  # Input validation and logging
  if (verbose) {
    logger::log_info("Starting enhanced physician map creation with multi-year ACS data")
    
    physician_data_rows <- nrow(physician_geodata)
    physician_data_cols <- ncol(physician_geodata)
    acs_years_text <- paste(acs_years, collapse = ", ")
    
    logger::log_info(paste0("Input physician data dimensions: ", physician_data_rows, " rows, ", physician_data_cols, " columns"))
    logger::log_info(paste0("ACS years requested: ", acs_years_text))
    logger::log_info(paste0("Default display year: ", default_year))
  }
  
  # Validate inputs using assertthat
  assertthat::assert_that(
    is.data.frame(physician_geodata),
    msg = "physician_geodata must be a data frame"
  )
  
  assertthat::assert_that(
    is.logical(include_county_boundaries),
    msg = "include_county_boundaries must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.logical(include_female_demographics),
    msg = "include_female_demographics must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.numeric(acs_years) && all(acs_years >= 2012 & acs_years <= 2023),
    msg = "acs_years must be numeric values between 2012 and 2023 (2010-2011 not supported)"
  )
  
  assertthat::assert_that(
    default_year %in% acs_years,
    msg = "default_year must be one of the specified acs_years"
  )
  
  # Process physician location data
  processed_physician_data <- process_physician_spatial_data(
    input_geodata = physician_geodata, 
    verbose = verbose
  )
  
  if (verbose) {
    processed_physician_count <- nrow(processed_physician_data)
    logger::log_info(paste0("Processed ", processed_physician_count, " physician locations"))
  }
  
  # Create base physician map
  base_physician_map <- create_base_physician_map(
    spatial_physician_data = processed_physician_data,
    verbose = verbose
  )
  
  # Add multi-year county boundaries if requested
  if (include_county_boundaries) {
    enhanced_map_with_counties <- add_multiyear_county_boundaries(
      leaflet_map = base_physician_map,
      spatial_physician_data = processed_physician_data,
      include_female_demographics = include_female_demographics,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    enhanced_map_with_counties <- base_physician_map
  }
  
  # Add final controls and formatting
  final_enhanced_map <- finalize_map_with_controls(
    leaflet_map = enhanced_map_with_counties,
    spatial_physician_data = processed_physician_data,
    include_county_boundaries = include_county_boundaries,
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (verbose) {
    final_enhanced_map_message <- "Enhanced physician map with multi-year demographics created successfully"
    logger::log_info(final_enhanced_map_message)
  }
  
  return(final_enhanced_map)
}

#' Process Physician Spatial Data
#' @noRd
process_physician_spatial_data <- function(input_geodata, verbose) {
  
  if (verbose) {
    logger::log_info("Processing physician spatial data")
  }
  
  # Convert to sf if needed
  if (!"sf" %in% class(input_geodata)) {
    assertthat::assert_that(
      all(c("latitude", "longitude") %in% names(input_geodata)),
      msg = "Data must contain 'latitude' and 'longitude' columns"
    )
    
    clean_physician_data <- input_geodata %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      clean_rows_count <- nrow(clean_physician_data)
      total_rows_count <- nrow(input_geodata)
      removed_rows_count <- total_rows_count - clean_rows_count
      
      logger::log_info(paste0("Converting ", clean_rows_count, " rows to spatial data"))
      logger::log_info(paste0("Removed ", removed_rows_count, " rows with missing coordinates"))
    }
    
    spatial_physician_data <- sf::st_as_sf(
      clean_physician_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_physician_data <- input_geodata
  }
  
  # Extract coordinates for mapping
  physician_coordinates <- sf::st_coordinates(spatial_physician_data)
  spatial_physician_data$longitude <- physician_coordinates[, "X"]
  spatial_physician_data$latitude <- physician_coordinates[, "Y"]
  
  # Create enhanced popups for physicians
  spatial_physician_data <- spatial_physician_data %>%
    dplyr::mutate(
      physician_popup_content = dplyr::case_when(
        "pfname" %in% names(.) & "plname" %in% names(.) & "npi" %in% names(.) ~ 
          paste0(
            "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
            "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
            "<h3 style='margin: 0; font-size: 16px;'>👩‍⚕️ Dr. ", pfname, " ", plname, "</h3>",
            "</div>",
            "<div style='padding: 4px 0;'>",
            "<p style='margin: 2px 0; font-size: 12px;'><strong>📋 NPI:</strong> ", npi, "</p>",
            ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📍 Location:</strong> ", pmailcityname, ", ", pmailstatename, "</p>"), ""),
            ifelse("pspec" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>🩺 Specialty:</strong> ", pspec, "</p>"), ""),
            "</div></div>"
          ),
        TRUE ~ paste0(
          "<div style='font-family: Arial, sans-serif;'>",
          "<h4 style='color: #2E86AB; margin: 0 0 8px 0;'>👩‍⚕️ Physician Details</h4>",
          "<p style='margin: 2px 0;'>Location information available</p>",
          "</div>"
        )
      )
    )
  
  if (verbose) {
    final_physician_count <- nrow(spatial_physician_data)
    logger::log_info(paste0("Created enhanced popups for ", final_physician_count, " physicians"))
  }
  
  return(spatial_physician_data)
}

#' Create Base Physician Map
#' @noRd
create_base_physician_map <- function(spatial_physician_data, verbose) {
  
  if (verbose) {
    base_physician_count <- nrow(spatial_physician_data)
    logger::log_info(paste0("Creating base physician map with ", base_physician_count, " physicians"))
  }
  
  base_map <- leaflet::leaflet(spatial_physician_data) %>%
    leaflet::addTiles(group = "OpenStreetMap") %>%
    leaflet::addCircleMarkers(
      lng = ~longitude,
      lat = ~latitude,
      radius = 6,
      fillColor = "#2E86AB",
      color = "white",
      weight = 2,
      opacity = 1,
      fillOpacity = 0.8,
      popup = ~physician_popup_content,
      group = "Physicians"
    )
  
  if (verbose) {
    logger::log_info("Base physician map created with circle markers")
  }
  
  return(base_map)
}

#' Add Multi-Year County Boundaries with Demographics
#' @noRd
add_multiyear_county_boundaries <- function(leaflet_map, spatial_physician_data, 
                                            include_female_demographics, acs_years, 
                                            default_year, verbose) {
  
  if (verbose) {
    acs_years_text <- paste(acs_years, collapse = ", ")
    logger::log_info(paste0("Adding multi-year county boundaries for years: ", acs_years_text))
  }
  
  # Check if tigris is available for county download
  if (!requireNamespace("tigris", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
    return(leaflet_map)
  }
  
  # Get primary state from physician data
  primary_state_info <- extract_primary_state(
    spatial_physician_data = spatial_physician_data,
    verbose = verbose
  )
  
  if (is.null(primary_state_info)) {
    if (verbose) {
      logger::log_warn("Cannot determine primary state from physician data")
    }
    return(leaflet_map)
  }
  
  # Download county boundaries
  county_boundary_data <- download_county_boundaries(
    primary_state = primary_state_info,
    verbose = verbose
  )
  
  if (is.null(county_boundary_data)) {
    return(leaflet_map)
  }
  
  # Calculate physician counts per county
  physician_county_counts <- calculate_physician_county_counts(
    county_boundaries = county_boundary_data,
    spatial_physician_data = spatial_physician_data,
    verbose = verbose
  )
  
  # Add demographics for each year if requested
  if (include_female_demographics) {
    enhanced_map_with_demographics <- add_demographic_layers_by_year(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_data,
      physician_counts = physician_county_counts,
      primary_state = primary_state_info,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    # Add basic county layer without demographics
    enhanced_map_with_demographics <- add_basic_county_layer(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_data,
      physician_counts = physician_county_counts,
      verbose = verbose
    )
  }
  
  return(enhanced_map_with_demographics)
}

#' Extract Primary State from Physician Data
#' @noRd
extract_primary_state <- function(spatial_physician_data, verbose) {
  
  state_column_name <- NULL
  for (column_candidate in c("pmailstatename", "plocstatename", "state")) {
    if (column_candidate %in% names(spatial_physician_data)) {
      state_column_name <- column_candidate
      break
    }
  }
  
  if (!is.null(state_column_name)) {
    physician_attributes_df <- sf::st_drop_geometry(spatial_physician_data)
    primary_state_name <- names(sort(table(physician_attributes_df[[state_column_name]]), decreasing = TRUE))[1]
    
    if (verbose) {
      logger::log_info(paste0("Primary state identified: ", primary_state_name, " from column: ", state_column_name))
    }
    
    return(primary_state_name)
  }
  
  return(NULL)
}

#' Download County Boundaries
#' @noRd
download_county_boundaries <- function(primary_state, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Downloading county boundaries for: ", primary_state))
  }
  
  tryCatch({
    county_boundaries <- tigris::counties(state = primary_state, cb = TRUE)
    county_boundaries <- sf::st_transform(county_boundaries, crs = 4326)
    
    if (verbose) {
      county_count <- nrow(county_boundaries)
      logger::log_info(paste0("Successfully downloaded ", county_count, " county boundaries"))
    }
    
    return(county_boundaries)
  }, error = function(error_msg) {
    if (verbose) {
      error_text <- error_msg$message
      logger::log_error(paste0("Could not download counties: ", error_text))
    }
    return(NULL)
  })
}

#' Calculate Physician Counts per County
#' @noRd
calculate_physician_county_counts <- function(county_boundaries, spatial_physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Calculating physician counts per county")
  }
  
  # Simple approximation - count by matching county names in geocoding data
  physician_attributes_df <- sf::st_drop_geometry(spatial_physician_data)
  
  physician_counts_per_county <- sapply(1:nrow(county_boundaries), function(county_index) {
    county_name <- county_boundaries$NAME[county_index]
    if ("geocoding_county" %in% names(physician_attributes_df)) {
      count_result <- sum(grepl(county_name, physician_attributes_df$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
    } else {
      count_result <- 0
    }
    return(count_result)
  })
  
  total_physician_count <- sum(physician_counts_per_county)
  if (verbose) {
    county_count <- length(physician_counts_per_county)
    logger::log_info(paste0("Calculated physician counts: ", total_physician_count, " total across ", county_count, " counties"))
  }
  
  return(physician_counts_per_county)
}

#' Add Demographic Layers by Year
#' @noRd
add_demographic_layers_by_year <- function(leaflet_map, county_boundaries, physician_counts,
                                           primary_state, acs_years, default_year, verbose) {
  
  if (verbose) {
    years_count <- length(acs_years)
    logger::log_info(paste0("Adding demographic layers for ", years_count, " years"))
  }
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(add_basic_county_layer(leaflet_map, county_boundaries, physician_counts, verbose))
  }
  
  # Download demographic data for all years
  multiyear_demographic_data <- download_multiyear_demographics(
    primary_state = primary_state,
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (length(multiyear_demographic_data) == 0) {
    if (verbose) {
      logger::log_warn("No demographic data available - falling back to basic counties")
    }
    return(add_basic_county_layer(leaflet_map, county_boundaries, physician_counts, verbose))
  }
  
  # Add county layers for each year
  enhanced_map_result <- leaflet_map
  
  for (data_year in names(multiyear_demographic_data)) {
    year_demographic_data <- multiyear_demographic_data[[data_year]]
    
    # Join demographics to county boundaries
    counties_with_year_demographics <- county_boundaries %>%
      dplyr::left_join(year_demographic_data, by = "GEOID")
    
    # Create popups for this year
    year_county_popups <- create_demographic_county_popups_with_year(
      counties_with_demographics = counties_with_year_demographics,
      physician_counts = physician_counts,
      data_year = as.numeric(data_year),
      verbose = verbose
    )
    
    # Determine initial visibility - only default year should be visible initially
    layer_initially_visible <- (as.numeric(data_year) == default_year)
    
    # Add polygon layer for this year
    if (layer_initially_visible) {
      # Add visible layer
      enhanced_map_result <- enhanced_map_result %>%
        leaflet::addPolygons(
          data = counties_with_year_demographics,
          weight = 1,
          color = "#888888",
          fillOpacity = 0.15,
          fillColor = "#lightblue",
          popup = year_county_popups,
          group = paste("Counties", data_year),
          highlightOptions = leaflet::highlightOptions(
            weight = 2,
            color = "#666666",
            fillOpacity = 0.3,
            bringToFront = FALSE
          )
        )
    } else {
      # Add hidden layer (will be available in layer control but not initially visible)
      enhanced_map_result <- enhanced_map_result %>%
        leaflet::addPolygons(
          data = counties_with_year_demographics,
          weight = 1,
          color = "#888888",
          fillOpacity = 0.1,
          fillColor = "#lightblue",
          popup = year_county_popups,
          group = paste("Counties", data_year),
          highlightOptions = leaflet::highlightOptions(
            weight = 2,
            color = "#666666",
            fillOpacity = 0.3,
            bringToFront = FALSE
          )
        ) %>%
        leaflet::hideGroup(paste("Counties", data_year))
    }
    
    if (verbose) {
      county_demo_count <- nrow(counties_with_year_demographics)
      logger::log_info(paste0("Added county layer for year ", data_year, " with ", county_demo_count, " counties"))
    }
  }
  
  return(enhanced_map_result)
}

#' Download Multi-Year Demographics
#' @noRd
download_multiyear_demographics <- function(primary_state, acs_years, verbose) {
  
  if (verbose) {
    years_count <- length(acs_years)
    logger::log_info(paste0("Downloading demographic data for ", years_count, " years"))
  }
  
  # Filter out problematic years (2010-2011) as ACS 5-year estimates may not be reliable
  valid_years <- acs_years[acs_years >= 2012]
  if (length(valid_years) < length(acs_years)) {
    if (verbose) {
      removed_years <- acs_years[acs_years < 2012]
      removed_years_text <- paste(removed_years, collapse = ", ")
      logger::log_warn(paste0("Removed years with unreliable ACS data: ", removed_years_text))
    }
  }
  
  # Define comprehensive female demographic variables
  female_demographic_variables <- c(
    # Basic female population
    total_female = "B01001_026",          
    female_under_5 = "B01001_027",        
    female_5_to_17 = "B01001_028",        
    female_65_plus = "B01001_044",        
    
    # Female education
    female_bachelor_plus = "B15002_032",  
    
    # Female health insurance
    female_with_insurance = "B27001_014", 
    female_total_insurance = "B27001_013", 
    
    # Economic variables
    median_household_income = "B19013_001", 
    per_capita_income = "B19301_001",       
    poverty_rate = "B17001_002",            
    total_pop_poverty = "B17001_001",       
    
    # Context
    total_pop = "B01003_001"              
  )
  
  # Create safe version of get_acs
  safe_get_acs <- purrr::safely(tidycensus::get_acs)
  
  # Download data for each valid year
  multiyear_results <- list()
  
  for (target_year in valid_years) {
    if (verbose) {
      logger::log_info(paste0("Downloading ACS data for year: ", target_year))
    }
    
    acs_result <- safe_get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = target_year,
      survey = "acs5",
      output = "wide"
    )
    
    if (!is.null(acs_result$result)) {
      # Process the demographic data
      processed_demographics <- process_demographic_calculations(
        raw_demographic_data = acs_result$result,
        data_year = target_year,
        verbose = verbose
      )
      
      multiyear_results[[as.character(target_year)]] <- processed_demographics
      
      if (verbose) {
        processed_county_count <- nrow(processed_demographics)
        logger::log_info(paste0("Successfully processed demographics for ", target_year, ": ", processed_county_count, " counties"))
      }
    } else {
      if (verbose) {
        error_message <- acs_result$error$message
        logger::log_warn(paste0("Failed to download data for year ", target_year, ": ", error_message))
      }
    }
  }
  
  if (verbose) {
    successful_years_count <- length(multiyear_results)
    logger::log_info(paste0("Downloaded demographic data for ", successful_years_count, " years successfully"))
  }
  
  return(multiyear_results)
}

#' Process Demographic Calculations
#' @noRd
process_demographic_calculations <- function(raw_demographic_data, data_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Processing demographic calculations for year ", data_year))
  }
  
  processed_data <- raw_demographic_data %>%
    dplyr::mutate(
      # Basic female age calculations
      female_under_18_count = ifelse(
        !is.na(female_under_5E) & !is.na(female_5_to_17E),
        female_under_5E + female_5_to_17E,
        0
      ),
      
      # Age percentages
      pct_female_under_18 = ifelse(
        !is.na(total_femaleE) & total_femaleE > 0,
        round((female_under_18_count / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_over_65 = ifelse(
        !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_65_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_of_total = ifelse(
        !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
        round((total_femaleE / total_popE) * 100, 1),
        NA_real_
      ),
      
      # Education percentage
      pct_female_bachelor_plus = ifelse(
        !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_bachelor_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      
      # Health insurance
      pct_female_with_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round((female_with_insuranceE / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      pct_female_no_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round(((female_total_insuranceE - female_with_insuranceE) / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      
      # Economic indicators
      median_household_income_formatted = ifelse(
        !is.na(median_household_incomeE),
        paste0("$", format(median_household_incomeE, big.mark = ",")),
        "N/A"
      ),
      per_capita_income_formatted = ifelse(
        !is.na(per_capita_incomeE),
        paste0("$", format(per_capita_incomeE, big.mark = ",")),
        "N/A"
      ),
      pct_poverty = ifelse(
        !is.na(poverty_rateE) & !is.na(total_pop_povertyE) & total_pop_povertyE > 0,
        round((poverty_rateE / total_pop_povertyE) * 100, 1),
        NA_real_
      ),
      
      # Raw counts for display
      female_under_18_raw = female_under_18_count,
      female_over_65_raw = female_65_plusE,
      female_bachelor_plus_raw = female_bachelor_plusE,
      female_with_insurance_raw = female_with_insuranceE,
      total_female_raw = total_femaleE,
      
      # Add year identifier
      acs_data_year = data_year
    )
  
  # Select key columns for joining
  processed_data <- processed_data %>%
    dplyr::select(
      GEOID, acs_data_year,
      # Age demographics
      pct_female_under_18, pct_female_over_65, pct_female_of_total,
      female_under_18_raw, female_over_65_raw, total_female_raw,
      # Education
      pct_female_bachelor_plus, female_bachelor_plus_raw,
      # Health insurance
      pct_female_with_insurance, pct_female_no_insurance,
      female_with_insurance_raw,
      # Economic indicators
      median_household_income_formatted, per_capita_income_formatted, pct_poverty
    )
  
  if (verbose) {
    valid_counties_count <- sum(!is.na(processed_data$total_female_raw))
    logger::log_info(paste0("Processed demographics for year ", data_year, ": ", valid_counties_count, " counties with valid data"))
  }
  
  return(processed_data)
}

#' Create Demographic County Popups with Year Information
#' @noRd
create_demographic_county_popups_with_year <- function(counties_with_demographics, physician_counts, 
                                                       data_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Creating demographic popups for year ", data_year))
  }
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 320px; font-size: 12px;'>",
    
    # Header with year
    "<div style='background: linear-gradient(135deg, #6f42c1, #28a745); color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties_with_demographics$NAME, " County, ", counties_with_demographics$STUSPS, "</h3>",
    "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
    "📊 ", data_year, " ACS Data</div>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_counts, 
    "</div>",
    
    # Female population
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females (", data_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$total_female_raw), "N/A", format(counties_with_demographics$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties_with_demographics$pct_female_of_total), "N/A", paste0(counties_with_demographics$pct_female_of_total, "%")), " of population)",
    "</div>",
    
    # Age demographics
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics (", data_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_under_18), "N/A", paste0(counties_with_demographics$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_under_18_raw), "", paste0("(", format(counties_with_demographics$female_under_18_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_over_65), "N/A", paste0(counties_with_demographics$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_over_65_raw), "", paste0("(", format(counties_with_demographics$female_over_65_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Education
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+ (", data_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$pct_female_bachelor_plus), "N/A", paste0(counties_with_demographics$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties_with_demographics$female_bachelor_plus_raw), "", paste0(" (", format(counties_with_demographics$female_bachelor_plus_raw, big.mark = ","), ")")),
    "</div>",
    
    # Economic indicators
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💰 Economic Indicators (", data_year, ")</strong>",
    "<div style='background: #f0f8ff; padding: 6px; margin: 4px 0; border-radius: 4px; border-left: 3px solid #1976d2;'>",
    "<div style='font-size: 11px;'>",
    "<strong>🏠 Median Household Income:</strong> ", counties_with_demographics$median_household_income_formatted, "<br/>",
    "<strong>👤 Per Capita Income:</strong> ", counties_with_demographics$per_capita_income_formatted, "<br/>",
    "<strong>📉 Poverty Rate:</strong> ", ifelse(is.na(counties_with_demographics$pct_poverty), "N/A", paste0(counties_with_demographics$pct_poverty, "%")),
    "</div>",
    "</div>",
    "</div>",
    
    # Health insurance
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>🏥 Female Health Insurance (", data_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Insured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_with_insurance), "N/A", paste0(counties_with_demographics$pct_female_with_insurance, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #d32f2f; font-weight: bold;'>❌ Uninsured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #d32f2f;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_no_insurance), "N/A", paste0(counties_with_demographics$pct_female_no_insurance, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Data source with year
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 ", data_year, " American Community Survey 5-Year Estimates",
    "</div>",
    
    "</div>"
  )
}

#' Add Basic County Layer (Fallback)
#' @noRd
add_basic_county_layer <- function(leaflet_map, county_boundaries, physician_counts, verbose) {
  
  if (verbose) {
    logger::log_info("Adding basic county layer without demographics")
  }
  
  basic_county_popups <- create_basic_county_popups_enhanced(
    county_boundaries = county_boundaries,
    physician_counts = physician_counts
  )
  
  enhanced_map <- leaflet_map %>%
    leaflet::addPolygons(
      data = county_boundaries,
      weight = 1,
      color = "#888888",
      fillOpacity = 0.1,
      fillColor = "#lightblue",
      popup = basic_county_popups,
      group = "Counties",
      highlightOptions = leaflet::highlightOptions(
        weight = 2,
        color = "#666666",
        fillOpacity = 0.3,
        bringToFront = FALSE
      )
    )
  
  if (verbose) {
    basic_county_count <- nrow(county_boundaries)
    logger::log_info(paste0("Added basic county layer with ", basic_county_count, " counties"))
  }
  
  return(enhanced_map)
}

#' Create Basic County Popups Enhanced
#' @noRd
create_basic_county_popups_enhanced <- function(county_boundaries, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", county_boundaries$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", county_boundaries$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", county_boundaries$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", county_boundaries$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(county_boundaries$ALAND) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 Enable demographics with tidycensus and Census API key</p>",
    "</div>",
    "</div>"
  )
}

#' Finalize Map with Controls
#' @noRd
finalize_map_with_controls <- function(leaflet_map, spatial_physician_data, 
                                       include_county_boundaries, acs_years, verbose) {
  
  if (verbose) {
    logger::log_info("Finalizing map with layer controls")
  }
  
  # Determine overlay groups
  overlay_groups_list <- "Physicians"
  
  if (include_county_boundaries) {
    if (length(acs_years) > 1) {
      # Multiple year county layers
      county_year_groups <- paste("Counties", acs_years)
      overlay_groups_list <- c(overlay_groups_list, county_year_groups)
    } else {
      # Single county layer
      overlay_groups_list <- c(overlay_groups_list, "Counties")
    }
  }
  
  # Add layer controls and final formatting
  finalized_map <- leaflet_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups_list,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft") %>%
    leaflet::fitBounds(
      lng1 = min(spatial_physician_data$longitude) - 0.1,
      lat1 = min(spatial_physician_data$latitude) - 0.1,
      lng2 = max(spatial_physician_data$longitude) + 0.1,
      lat2 = max(spatial_physician_data$latitude) + 0.1
    )
  
  if (verbose) {
    overlay_groups_count <- length(overlay_groups_list)
    overlay_groups_text <- paste(overlay_groups_list, collapse = ", ")
    
    logger::log_info(paste0("Map finalized with ", overlay_groups_count, " layer groups"))
    logger::log_info(paste0("Layer groups: ", overlay_groups_text))
    
    # Add guidance for multi-year usage
    if (include_county_boundaries && length(acs_years) > 1) {
      logger::log_info("TIP: For best demographic comparison, enable only one county year at a time")
    }
  }
  
  return(finalized_map)
}


physician_location_data <- readr::read_csv("data/geocoded/obgyn_geocoded.csv")

# execute multiple years with county data ----
enhanced_physician_map_with_years(
  physician_geodata = physician_location_data,
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  acs_years = 2012:2023,
  default_year = 2023,
  verbose = TRUE
)

# Physician and county year specific ----
physician_by_year <- read_csv("data/B-nber_nppes_combine_columns/nber_nppes_combine_columns_final_obgyn_provider_dataset.csv")


physician_by_year %>% dplyr::filter(plname == "MUFFLY")

# Get every physician, year combo with lat and long ----
join_physician_data_with_sf <- function(
    physician_by_year,
    full_results,
    verbose = TRUE,
    inner_join = FALSE
) {
  logger::log_info("Starting join between physician_by_year and full_results")
  
  # Assertions
  assertthat::assert_that("npi" %in% names(physician_by_year), msg = "Missing 'npi' in physician_by_year")
  assertthat::assert_that("data_year" %in% names(physician_by_year), msg = "Missing 'data_year' in physician_by_year")
  assertthat::assert_that("npi" %in% names(full_results), msg = "Missing 'npi' in full_results")
  assertthat::assert_that("data_year" %in% names(full_results), msg = "Missing 'data_year' in full_results")
  
  # Log input sizes
  logger::log_info(paste0("physician_by_year: ", scales::comma(nrow(physician_by_year)), " rows"))
  logger::log_info(paste0("full_results (sf): ", scales::comma(nrow(full_results)), " rows"))
  
  # ⬅️ NEW: Extract coordinates before dropping geometry
  full_results <- full_results %>%
    dplyr::mutate(
      longitude = sf::st_coordinates(.)[, "X"],
      latitude  = sf::st_coordinates(.)[, "Y"]
    )
  
  # Drop geometry
  full_results_df <- full_results %>%
    sf::st_drop_geometry()
  
  # Join function
  join_func <- if (inner_join) dplyr::inner_join else dplyr::left_join
  join_type <- if (inner_join) "inner_join" else "left_join"
  logger::log_info(paste0("Using ", join_type, " on npi and data_year"))
  
  # Join
  joined_physicians <- physician_by_year %>%
    join_func(
      full_results_df,
      by = c("npi", "data_year"),
      suffix = c("_nppes", "_sf")
    )
  
  # Logging
  matched_count <- joined_physicians %>%
    dplyr::filter(!is.na(address)) %>%
    nrow()
  
  logger::log_info(paste0("Completed join: ", scales::comma(matched_count), " matched rows"))
  logger::log_info(paste0("Resulting dataset has ", scales::comma(nrow(joined_physicians)), " rows"))
  
  return(joined_physicians)
}



join_physician_data_with_sf_output <- join_physician_data_with_sf(
  physician_by_year,
  full_results,
  inner_join = TRUE,
  verbose = TRUE
)


# Process Physician Spatial Data with Year-Specific Filtering ----
#' Process Physician Spatial Data with Year-Specific Filtering
#' @noRd
process_physician_spatial_data <- function(input_geodata, acs_years, verbose) {
  
  if (verbose) {
    logger::log_info("Processing physician spatial data with year-specific filtering")
  }
  
  # Check if data_year column exists for temporal filtering
  has_data_year <- "data_year" %in% names(input_geodata)
  
  if (has_data_year && length(acs_years) > 1) {
    # Create separate datasets for each year
    if (verbose) {
      available_years <- unique(input_geodata$data_year)
      available_years_text <- paste(sort(available_years), collapse = ", ")
      logger::log_info(paste0("Creating year-specific physician datasets. Available years: ", available_years_text))
    }
    
    yearly_physician_data <- list()
    
    for (target_year in acs_years) {
      # Filter physicians for this specific year
      year_physicians <- input_geodata %>%
        dplyr::filter(data_year == target_year)
      
      if (nrow(year_physicians) > 0) {
        # Process spatial data for this year
        processed_year_data <- process_single_year_physician_data(
          physician_data = year_physicians,
          analysis_year = target_year,
          verbose = verbose
        )
        
        yearly_physician_data[[as.character(target_year)]] <- processed_year_data
        
        if (verbose) {
          logger::log_info(paste0("Year ", target_year, ": ", nrow(processed_year_data), " physicians"))
        }
      } else {
        if (verbose) {
          logger::log_warn(paste0("No physicians found for year ", target_year))
        }
      }
    }
    
    return(yearly_physician_data)
    
  } else {
    # Single year or no data_year column - process all data together
    if (verbose) {
      if (!has_data_year) {
        logger::log_info("No data_year column found - processing all physicians together")
      } else {
        logger::log_info("Single year analysis - processing all physicians together")
      }
    }
    
    processed_data <- process_single_year_physician_data(
      physician_data = input_geodata,
      analysis_year = acs_years[1], # Use first year as identifier
      verbose = verbose
    )
    
    return(processed_data)
  }
}

#' Process Single Year Physician Data
#' @noRd
process_single_year_physician_data <- function(physician_data, analysis_year, verbose) {
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_data)) {
    assertthat::assert_that(
      all(c("latitude", "longitude") %in% names(physician_data)),
      msg = "Data must contain 'latitude' and 'longitude' columns"
    )
    
    clean_physician_data <- physician_data %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      clean_rows_count <- nrow(clean_physician_data)
      total_rows_count <- nrow(physician_data)
      removed_rows_count <- total_rows_count - clean_rows_count
      
      logger::log_info(paste0("Year ", analysis_year, ": Converting ", clean_rows_count, " rows to spatial data"))
      if (removed_rows_count > 0) {
        logger::log_info(paste0("Year ", analysis_year, ": Removed ", removed_rows_count, " rows with missing coordinates"))
      }
    }
    
    spatial_physician_data <- sf::st_as_sf(
      clean_physician_data,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_physician_data <- physician_data
  }
  
  # Extract coordinates for mapping
  physician_coordinates <- sf::st_coordinates(spatial_physician_data)
  spatial_physician_data$longitude <- physician_coordinates[, "X"]
  spatial_physician_data$latitude <- physician_coordinates[, "Y"]
  
  # Create enhanced popups for physicians with year information
  spatial_physician_data <- spatial_physician_data %>%
    dplyr::mutate(
      physician_popup_content = dplyr::case_when(
        "pfname" %in% names(.) & "plname" %in% names(.) & "npi" %in% names(.) ~ 
          paste0(
            "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
            "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
            "<h3 style='margin: 0; font-size: 16px;'>👩‍⚕️ Dr. ", pfname, " ", plname, "</h3>",
            "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
            "📅 ", analysis_year, " Data</div>",
            "</div>",
            "<div style='padding: 4px 0;'>",
            "<p style='margin: 2px 0; font-size: 12px;'><strong>📋 NPI:</strong> ", npi, "</p>",
            ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📍 Location:</strong> ", pmailcityname, ", ", pmailstatename, "</p>"), ""),
            ifelse("pspec" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>🩺 Specialty:</strong> ", pspec, "</p>"), ""),
            ifelse("data_year" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Data Year:</strong> ", data_year, "</p>"), ""),
            "</div></div>"
          ),
        TRUE ~ paste0(
          "<div style='font-family: Arial, sans-serif;'>",
          "<h4 style='color: #2E86AB; margin: 0 0 8px 0;'>👩‍⚕️ Physician Details (", analysis_year, ")</h4>",
          "<p style='margin: 2px 0;'>Location information available</p>",
          "</div>"
        )
      )
    )
  
  if (verbose) {
    final_physician_count <- nrow(spatial_physician_data)
    logger::log_info(paste0("Year ", analysis_year, ": Created enhanced popups for ", final_physician_count, " physicians"))
  }
  
  return(spatial_physician_data)
}

#' Physician Map with Multi-Year Female Demographics and County Analysis
#' 
#' Creates an interactive physician map with clickable counties showing detailed female
#' demographic data from the American Community Survey. Users can select different
#' years (2010-2023) using radio button controls to see how demographics have changed
#' over time. The map includes comprehensive female demographics including age 
#' distributions, education levels, employment, health insurance, and economic indicators.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns and physician information
#' @param include_county_boundaries Logical to include counties with demographics (default: TRUE)
#' @param include_female_demographics Logical to include detailed female demographic data (default: TRUE)
#' @param acs_years Numeric vector of years to include (default: 2022, range: 2012-2023)
#' @param default_year Numeric year to display initially (default: 2022)
#' @param verbose Logical for detailed logging (default: TRUE)
#' 
#' @return Interactive leaflet map with year selector controls
#' 
#' @examples
#' # Example 1: Basic map with single year
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2022,
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 2: Multi-year comparison map (2018-2022)
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = c(2018, 2019, 2020, 2021, 2022),
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Full decade analysis (2012-2023)
#' enhanced_physician_map_with_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2012:2023,
#'   default_year = 2020,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr mutate case_when filter left_join select bind_rows
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl addScaleBar fitBounds
#' @importFrom leaflet highlightOptions labelOptions layersControlOptions
#' @importFrom tidycensus get_acs
#' @importFrom purrr map_dfr safely
#' @export
enhanced_physician_map_with_years <- function(physician_geodata,
                                              include_county_boundaries = TRUE,
                                              include_female_demographics = TRUE,
                                              acs_years = 2022,
                                              default_year = 2022,
                                              verbose = TRUE) {
  
  # Input validation and logging
  if (verbose) {
    logger::log_info("Starting enhanced physician map creation with multi-year ACS data")
    
    physician_data_rows <- nrow(physician_geodata)
    physician_data_cols <- ncol(physician_geodata)
    acs_years_text <- paste(acs_years, collapse = ", ")
    
    logger::log_info(paste0("Input physician data dimensions: ", physician_data_rows, " rows, ", physician_data_cols, " columns"))
    logger::log_info(paste0("ACS years requested: ", acs_years_text))
    logger::log_info(paste0("Default display year: ", default_year))
  }
  
  # Validate inputs using assertthat
  assertthat::assert_that(
    is.data.frame(physician_geodata),
    msg = "physician_geodata must be a data frame"
  )
  
  assertthat::assert_that(
    is.logical(include_county_boundaries),
    msg = "include_county_boundaries must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.logical(include_female_demographics),
    msg = "include_female_demographics must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.numeric(acs_years) && all(acs_years >= 2012 & acs_years <= 2023),
    msg = "acs_years must be numeric values between 2012 and 2023 (2010-2011 not supported)"
  )
  
  assertthat::assert_that(
    default_year %in% acs_years,
    msg = "default_year must be one of the specified acs_years"
  )
  
  # Validate that physician data contains data_year column for temporal filtering
  if (!"data_year" %in% names(physician_geodata)) {
    if (verbose) {
      logger::log_warn("physician_geodata missing 'data_year' column - all physicians will be shown for all years")
    }
  }
  
  # Process physician location data with year-specific filtering
  processed_physician_data <- process_physician_spatial_data(
    input_geodata = physician_geodata, 
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (verbose) {
    if (is.list(processed_physician_data)) {
      total_physicians_across_years <- sum(sapply(processed_physician_data, nrow))
      years_with_data <- names(processed_physician_data)
      logger::log_info(paste0("Processed ", total_physicians_across_years, " physician locations across years: ", paste(years_with_data, collapse = ", ")))
    } else {
      processed_physician_count <- nrow(processed_physician_data)
      logger::log_info(paste0("Processed ", processed_physician_count, " physician locations"))
    }
  }
  
  # Create base physician map with year-specific layers
  base_physician_map <- create_base_physician_map(
    spatial_physician_data = processed_physician_data,
    acs_years = acs_years,
    default_year = default_year,
    verbose = verbose
  )
  
  # Add multi-year county boundaries if requested
  if (include_county_boundaries) {
    enhanced_map_with_counties <- add_multiyear_county_boundaries(
      leaflet_map = base_physician_map,
      spatial_physician_data = processed_physician_data,
      include_female_demographics = include_female_demographics,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    enhanced_map_with_counties <- base_physician_map
  }
  
  # Add final controls and formatting
  final_enhanced_map <- finalize_map_with_controls(
    leaflet_map = enhanced_map_with_counties,
    spatial_physician_data = processed_physician_data,
    include_county_boundaries = include_county_boundaries,
    acs_years = acs_years,
    default_year = default_year,
    verbose = verbose
  )
  
  if (verbose) {
    final_enhanced_map_message <- "Enhanced physician map with multi-year demographics created successfully"
    logger::log_info(final_enhanced_map_message)
  }
  
  return(final_enhanced_map)
}

#' #' Process Physician Spatial Data
#' #' @noRd
#' process_physician_spatial_data <- function(input_geodata, verbose) {
#'   
#'   if (verbose) {
#'     logger::log_info("Processing physician spatial data")
#'   }
#'   
#'   # Convert to sf if needed
#'   if (!"sf" %in% class(input_geodata)) {
#'     assertthat::assert_that(
#'       all(c("latitude", "longitude") %in% names(input_geodata)),
#'       msg = "Data must contain 'latitude' and 'longitude' columns"
#'     )
#'     
#'     clean_physician_data <- input_geodata %>%
#'       dplyr::filter(!is.na(latitude) & !is.na(longitude))
#'     
#'     if (verbose) {
#'       clean_rows_count <- nrow(clean_physician_data)
#'       total_rows_count <- nrow(input_geodata)
#'       removed_rows_count <- total_rows_count - clean_rows_count
#'       
#'       logger::log_info(paste0("Converting ", clean_rows_count, " rows to spatial data"))
#'       logger::log_info(paste0("Removed ", removed_rows_count, " rows with missing coordinates"))
#'     }
#'     
#'     spatial_physician_data <- sf::st_as_sf(
#'       clean_physician_data,
#'       coords = c("longitude", "latitude"),
#'       crs = 4326
#'     )
#'   } else {
#'     spatial_physician_data <- input_geodata
#'   }
#'   
#'   # Extract coordinates for mapping
#'   physician_coordinates <- sf::st_coordinates(spatial_physician_data)
#'   spatial_physician_data$longitude <- physician_coordinates[, "X"]
#'   spatial_physician_data$latitude <- physician_coordinates[, "Y"]
#'   
#'   # Create enhanced popups for physicians
#'   spatial_physician_data <- spatial_physician_data %>%
#'     dplyr::mutate(
#'       physician_popup_content = dplyr::case_when(
#'         "pfname" %in% names(.) & "plname" %in% names(.) & "npi" %in% names(.) ~ 
#'           paste0(
#'             "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
#'             "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
#'             "<h3 style='margin: 0; font-size: 16px;'>👩‍⚕️ Dr. ", pfname, " ", plname, "</h3>",
#'             "</div>",
#'             "<div style='padding: 4px 0;'>",
#'             "<p style='margin: 2px 0; font-size: 12px;'><strong>📋 NPI:</strong> ", npi, "</p>",
#'             ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
#'                    paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📍 Location:</strong> ", pmailcityname, ", ", pmailstatename, "</p>"), ""),
#'             ifelse("pspec" %in% names(.),
#'                    paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>🩺 Specialty:</strong> ", pspec, "</p>"), ""),
#'             "</div></div>"
#'           ),
#'         TRUE ~ paste0(
#'           "<div style='font-family: Arial, sans-serif;'>",
#'           "<h4 style='color: #2E86AB; margin: 0 0 8px 0;'>👩‍⚕️ Physician Details</h4>",
#'           "<p style='margin: 2px 0;'>Location information available</p>",
#'           "</div>"
#'         )
#'       )
#'     )
#'   
#'   if (verbose) {
#'     final_physician_count <- nrow(spatial_physician_data)
#'     logger::log_info(paste0("Created enhanced popups for ", final_physician_count, " physicians"))
#'   }
#'   
#'   return(spatial_physician_data)
#' }

#' Create Base Physician Map with Year-Specific Layers
#' @noRd
create_base_physician_map <- function(spatial_physician_data, acs_years, default_year, verbose) {
  
  if (verbose) {
    if (is.list(spatial_physician_data)) {
      logger::log_info(paste0("Creating base physician map with year-specific layers for ", length(spatial_physician_data), " years"))
    } else {
      base_physician_count <- nrow(spatial_physician_data)
      logger::log_info(paste0("Creating base physician map with ", base_physician_count, " physicians"))
    }
  }
  
  # Initialize base map
  base_map <- leaflet::leaflet() %>%
    leaflet::addTiles(group = "OpenStreetMap")
  
  if (is.list(spatial_physician_data)) {
    # Multiple years - create separate physician layers for each year
    for (year_key in names(spatial_physician_data)) {
      year_data <- spatial_physician_data[[year_key]]
      year_numeric <- as.numeric(year_key)
      
      # Determine initial visibility
      layer_initially_visible <- (year_numeric == default_year)
      
      if (nrow(year_data) > 0) {
        if (layer_initially_visible) {
          # Add visible physician layer
          base_map <- base_map %>%
            leaflet::addCircleMarkers(
              data = year_data,
              lng = ~longitude,
              lat = ~latitude,
              radius = 6,
              fillColor = "#2E86AB",
              color = "white",
              weight = 2,
              opacity = 1,
              fillOpacity = 0.8,
              popup = ~physician_popup_content,
              group = paste("Physicians", year_key)
            )
        } else {
          # Add hidden physician layer
          base_map <- base_map %>%
            leaflet::addCircleMarkers(
              data = year_data,
              lng = ~longitude,
              lat = ~latitude,
              radius = 6,
              fillColor = "#2E86AB",
              color = "white",
              weight = 2,
              opacity = 1,
              fillOpacity = 0.8,
              popup = ~physician_popup_content,
              group = paste("Physicians", year_key)
            ) %>%
            leaflet::hideGroup(paste("Physicians", year_key))
        }
        
        if (verbose) {
          physician_count <- nrow(year_data)
          visibility_status <- ifelse(layer_initially_visible, "visible", "hidden")
          logger::log_info(paste0("Added physician layer for year ", year_key, ": ", physician_count, " physicians (", visibility_status, ")"))
        }
      }
    }
  } else {
    # Single dataset - create single physician layer
    base_map <- base_map %>%
      leaflet::addCircleMarkers(
        data = spatial_physician_data,
        lng = ~longitude,
        lat = ~latitude,
        radius = 6,
        fillColor = "#2E86AB",
        color = "white",
        weight = 2,
        opacity = 1,
        fillOpacity = 0.8,
        popup = ~physician_popup_content,
        group = "Physicians"
      )
    
    if (verbose) {
      physician_count <- nrow(spatial_physician_data)
      logger::log_info(paste0("Added single physician layer with ", physician_count, " physicians"))
    }
  }
  
  return(base_map)
}

#' Add Multi-Year County Boundaries with Demographics
#' @noRd
add_multiyear_county_boundaries <- function(leaflet_map, spatial_physician_data, 
                                            include_female_demographics, acs_years, 
                                            default_year, verbose) {
  
  if (verbose) {
    acs_years_text <- paste(acs_years, collapse = ", ")
    logger::log_info(paste0("Adding multi-year county boundaries for years: ", acs_years_text))
  }
  
  # Check if tigris is available for county download
  if (!requireNamespace("tigris", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
    return(leaflet_map)
  }
  
  # Get primary state from physician data
  primary_state_info <- extract_primary_state(
    spatial_physician_data = spatial_physician_data,
    verbose = verbose
  )
  
  if (is.null(primary_state_info)) {
    if (verbose) {
      logger::log_warn("Cannot determine primary state from physician data")
    }
    return(leaflet_map)
  }
  
  # Download county boundaries
  county_boundary_data <- download_county_boundaries(
    primary_state = primary_state_info,
    verbose = verbose
  )
  
  if (is.null(county_boundary_data)) {
    return(leaflet_map)
  }
  
  # Calculate physician counts per county
  physician_county_counts <- calculate_physician_county_counts(
    county_boundaries = county_boundary_data,
    spatial_physician_data = spatial_physician_data,
    verbose = verbose
  )
  
  # Add demographics for each year if requested
  if (include_female_demographics) {
    enhanced_map_with_demographics <- add_demographic_layers_by_year(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_data,
      physician_counts = physician_county_counts,
      primary_state = primary_state_info,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    # Add basic county layer without demographics
    enhanced_map_with_demographics <- add_basic_county_layer(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_data,
      physician_counts = physician_county_counts,
      verbose = verbose
    )
  }
  
  return(enhanced_map_with_demographics)
}

#' Extract Primary State from Physician Data
#' @noRd
extract_primary_state <- function(spatial_physician_data, verbose) {
  
  state_column_name <- NULL
  for (column_candidate in c("pmailstatename", "plocstatename", "state")) {
    if (column_candidate %in% names(spatial_physician_data)) {
      state_column_name <- column_candidate
      break
    }
  }
  
  if (!is.null(state_column_name)) {
    physician_attributes_df <- sf::st_drop_geometry(spatial_physician_data)
    primary_state_name <- names(sort(table(physician_attributes_df[[state_column_name]]), decreasing = TRUE))[1]
    
    if (verbose) {
      logger::log_info(paste0("Primary state identified: ", primary_state_name, " from column: ", state_column_name))
    }
    
    return(primary_state_name)
  }
  
  return(NULL)
}

#' Download County Boundaries
#' @noRd
download_county_boundaries <- function(primary_state, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Downloading county boundaries for: ", primary_state))
  }
  
  tryCatch({
    county_boundaries <- tigris::counties(state = primary_state, cb = TRUE)
    county_boundaries <- sf::st_transform(county_boundaries, crs = 4326)
    
    if (verbose) {
      county_count <- nrow(county_boundaries)
      logger::log_info(paste0("Successfully downloaded ", county_count, " county boundaries"))
    }
    
    return(county_boundaries)
  }, error = function(error_msg) {
    if (verbose) {
      error_text <- error_msg$message
      logger::log_error(paste0("Could not download counties: ", error_text))
    }
    return(NULL)
  })
}

#' Calculate Physician Counts per County
#' @noRd
calculate_physician_county_counts <- function(county_boundaries, spatial_physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Calculating physician counts per county")
  }
  
  # Simple approximation - count by matching county names in geocoding data
  physician_attributes_df <- sf::st_drop_geometry(spatial_physician_data)
  
  physician_counts_per_county <- sapply(1:nrow(county_boundaries), function(county_index) {
    county_name <- county_boundaries$NAME[county_index]
    if ("geocoding_county" %in% names(physician_attributes_df)) {
      count_result <- sum(grepl(county_name, physician_attributes_df$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
    } else {
      count_result <- 0
    }
    return(count_result)
  })
  
  total_physician_count <- sum(physician_counts_per_county)
  if (verbose) {
    county_count <- length(physician_counts_per_county)
    logger::log_info(paste0("Calculated physician counts: ", total_physician_count, " total across ", county_count, " counties"))
  }
  
  return(physician_counts_per_county)
}

#' Add Demographic Layers by Year
#' @noRd
add_demographic_layers_by_year <- function(leaflet_map, county_boundaries, physician_counts,
                                           primary_state, acs_years, default_year, verbose) {
  
  if (verbose) {
    years_count <- length(acs_years)
    logger::log_info(paste0("Adding demographic layers for ", years_count, " years"))
  }
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(add_basic_county_layer(leaflet_map, county_boundaries, physician_counts, verbose))
  }
  
  # Download demographic data for all years
  multiyear_demographic_data <- download_multiyear_demographics(
    primary_state = primary_state,
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (length(multiyear_demographic_data) == 0) {
    if (verbose) {
      logger::log_warn("No demographic data available - falling back to basic counties")
    }
    return(add_basic_county_layer(leaflet_map, county_boundaries, physician_counts, verbose))
  }
  
  # Add county layers for each year
  enhanced_map_result <- leaflet_map
  
  for (data_year in names(multiyear_demographic_data)) {
    year_demographic_data <- multiyear_demographic_data[[data_year]]
    
    # Join demographics to county boundaries
    counties_with_year_demographics <- county_boundaries %>%
      dplyr::left_join(year_demographic_data, by = "GEOID")
    
    # Create popups for this year
    year_county_popups <- create_demographic_county_popups_with_year(
      counties_with_demographics = counties_with_year_demographics,
      physician_counts = physician_counts,
      data_year = as.numeric(data_year),
      verbose = verbose
    )
    
    # Determine initial visibility - only default year should be visible initially
    layer_initially_visible <- (as.numeric(data_year) == default_year)
    
    # Add polygon layer for this year
    if (layer_initially_visible) {
      # Add visible layer
      enhanced_map_result <- enhanced_map_result %>%
        leaflet::addPolygons(
          data = counties_with_year_demographics,
          weight = 1,
          color = "#888888",
          fillOpacity = 0.15,
          fillColor = "#lightblue",
          popup = year_county_popups,
          group = paste("Counties", data_year),
          highlightOptions = leaflet::highlightOptions(
            weight = 2,
            color = "#666666",
            fillOpacity = 0.3,
            bringToFront = FALSE
          )
        )
    } else {
      # Add hidden layer (will be available in layer control but not initially visible)
      enhanced_map_result <- enhanced_map_result %>%
        leaflet::addPolygons(
          data = counties_with_year_demographics,
          weight = 1,
          color = "#888888",
          fillOpacity = 0.1,
          fillColor = "#lightblue",
          popup = year_county_popups,
          group = paste("Counties", data_year),
          highlightOptions = leaflet::highlightOptions(
            weight = 2,
            color = "#666666",
            fillOpacity = 0.3,
            bringToFront = FALSE
          )
        ) %>%
        leaflet::hideGroup(paste("Counties", data_year))
    }
    
    if (verbose) {
      county_demo_count <- nrow(counties_with_year_demographics)
      logger::log_info(paste0("Added county layer for year ", data_year, " with ", county_demo_count, " counties"))
    }
  }
  
  return(enhanced_map_result)
}

#' Download Multi-Year Demographics
#' @noRd
download_multiyear_demographics <- function(primary_state, acs_years, verbose) {
  
  if (verbose) {
    years_count <- length(acs_years)
    logger::log_info(paste0("Downloading demographic data for ", years_count, " years"))
  }
  
  # Filter out problematic years (2010-2011) as ACS 5-year estimates may not be reliable
  valid_years <- acs_years[acs_years >= 2012]
  if (length(valid_years) < length(acs_years)) {
    if (verbose) {
      removed_years <- acs_years[acs_years < 2012]
      removed_years_text <- paste(removed_years, collapse = ", ")
      logger::log_warn(paste0("Removed years with unreliable ACS data: ", removed_years_text))
    }
  }
  
  # Define comprehensive female demographic variables
  female_demographic_variables <- c(
    # Basic female population
    total_female = "B01001_026",          
    female_under_5 = "B01001_027",        
    female_5_to_17 = "B01001_028",        
    female_65_plus = "B01001_044",        
    
    # Female education
    female_bachelor_plus = "B15002_032",  
    
    # Female health insurance
    female_with_insurance = "B27001_014", 
    female_total_insurance = "B27001_013", 
    
    # Economic variables
    median_household_income = "B19013_001", 
    per_capita_income = "B19301_001",       
    poverty_rate = "B17001_002",            
    total_pop_poverty = "B17001_001",       
    
    # Context
    total_pop = "B01003_001"              
  )
  
  # Create safe version of get_acs
  safe_get_acs <- purrr::safely(tidycensus::get_acs)
  
  # Download data for each valid year
  multiyear_results <- list()
  
  for (target_year in valid_years) {
    if (verbose) {
      logger::log_info(paste0("Downloading ACS data for year: ", target_year))
    }
    
    acs_result <- safe_get_acs(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variables,
      year = target_year,
      survey = "acs5",
      output = "wide"
    )
    
    if (!is.null(acs_result$result)) {
      # Process the demographic data
      processed_demographics <- process_demographic_calculations(
        raw_demographic_data = acs_result$result,
        data_year = target_year,
        verbose = verbose
      )
      
      multiyear_results[[as.character(target_year)]] <- processed_demographics
      
      if (verbose) {
        processed_county_count <- nrow(processed_demographics)
        logger::log_info(paste0("Successfully processed demographics for ", target_year, ": ", processed_county_count, " counties"))
      }
    } else {
      if (verbose) {
        error_message <- acs_result$error$message
        logger::log_warn(paste0("Failed to download data for year ", target_year, ": ", error_message))
      }
    }
  }
  
  if (verbose) {
    successful_years_count <- length(multiyear_results)
    logger::log_info(paste0("Downloaded demographic data for ", successful_years_count, " years successfully"))
  }
  
  return(multiyear_results)
}

#' Process Demographic Calculations
#' @noRd
process_demographic_calculations <- function(raw_demographic_data, data_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Processing demographic calculations for year ", data_year))
  }
  
  processed_data <- raw_demographic_data %>%
    dplyr::mutate(
      # Basic female age calculations
      female_under_18_count = ifelse(
        !is.na(female_under_5E) & !is.na(female_5_to_17E),
        female_under_5E + female_5_to_17E,
        0
      ),
      
      # Age percentages
      pct_female_under_18 = ifelse(
        !is.na(total_femaleE) & total_femaleE > 0,
        round((female_under_18_count / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_over_65 = ifelse(
        !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_65_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_of_total = ifelse(
        !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
        round((total_femaleE / total_popE) * 100, 1),
        NA_real_
      ),
      
      # Education percentage
      pct_female_bachelor_plus = ifelse(
        !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_bachelor_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      
      # Health insurance
      pct_female_with_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round((female_with_insuranceE / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      pct_female_no_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round(((female_total_insuranceE - female_with_insuranceE) / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      
      # Economic indicators
      median_household_income_formatted = ifelse(
        !is.na(median_household_incomeE),
        paste0("$", format(median_household_incomeE, big.mark = ",")),
        "N/A"
      ),
      per_capita_income_formatted = ifelse(
        !is.na(per_capita_incomeE),
        paste0("$", format(per_capita_incomeE, big.mark = ",")),
        "N/A"
      ),
      pct_poverty = ifelse(
        !is.na(poverty_rateE) & !is.na(total_pop_povertyE) & total_pop_povertyE > 0,
        round((poverty_rateE / total_pop_povertyE) * 100, 1),
        NA_real_
      ),
      
      # Raw counts for display
      female_under_18_raw = female_under_18_count,
      female_over_65_raw = female_65_plusE,
      female_bachelor_plus_raw = female_bachelor_plusE,
      female_with_insurance_raw = female_with_insuranceE,
      total_female_raw = total_femaleE,
      
      # Add year identifier
      acs_data_year = data_year
    )
  
  # Select key columns for joining
  processed_data <- processed_data %>%
    dplyr::select(
      GEOID, acs_data_year,
      # Age demographics
      pct_female_under_18, pct_female_over_65, pct_female_of_total,
      female_under_18_raw, female_over_65_raw, total_female_raw,
      # Education
      pct_female_bachelor_plus, female_bachelor_plus_raw,
      # Health insurance
      pct_female_with_insurance, pct_female_no_insurance,
      female_with_insurance_raw,
      # Economic indicators
      median_household_income_formatted, per_capita_income_formatted, pct_poverty
    )
  
  if (verbose) {
    valid_counties_count <- sum(!is.na(processed_data$total_female_raw))
    logger::log_info(paste0("Processed demographics for year ", data_year, ": ", valid_counties_count, " counties with valid data"))
  }
  
  return(processed_data)
}

#' Create Demographic County Popups with Year Information
#' @noRd
create_demographic_county_popups_with_year <- function(counties_with_demographics, physician_counts, 
                                                       data_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Creating demographic popups for year ", data_year))
  }
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 320px; font-size: 12px;'>",
    
    # Header with year
    "<div style='background: linear-gradient(135deg, #6f42c1, #28a745); color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties_with_demographics$NAME, " County, ", counties_with_demographics$STUSPS, "</h3>",
    "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
    "📊 ", data_year, " ACS Data</div>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_counts, 
    "</div>",
    
    # Female population
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females (", data_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$total_female_raw), "N/A", format(counties_with_demographics$total_female_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties_with_demographics$pct_female_of_total), "N/A", paste0(counties_with_demographics$pct_female_of_total, "%")), " of population)",
    "</div>",
    
    # Age demographics
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics (", data_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_under_18), "N/A", paste0(counties_with_demographics$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_under_18_raw), "", paste0("(", format(counties_with_demographics$female_under_18_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_over_65), "N/A", paste0(counties_with_demographics$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_over_65_raw), "", paste0("(", format(counties_with_demographics$female_over_65_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Education
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+ (", data_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$pct_female_bachelor_plus), "N/A", paste0(counties_with_demographics$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties_with_demographics$female_bachelor_plus_raw), "", paste0(" (", format(counties_with_demographics$female_bachelor_plus_raw, big.mark = ","), ")")),
    "</div>",
    
    # Economic indicators
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💰 Economic Indicators (", data_year, ")</strong>",
    "<div style='background: #f0f8ff; padding: 6px; margin: 4px 0; border-radius: 4px; border-left: 3px solid #1976d2;'>",
    "<div style='font-size: 11px;'>",
    "<strong>🏠 Median Household Income:</strong> ", counties_with_demographics$median_household_income_formatted, "<br/>",
    "<strong>👤 Per Capita Income:</strong> ", counties_with_demographics$per_capita_income_formatted, "<br/>",
    "<strong>📉 Poverty Rate:</strong> ", ifelse(is.na(counties_with_demographics$pct_poverty), "N/A", paste0(counties_with_demographics$pct_poverty, "%")),
    "</div>",
    "</div>",
    "</div>",
    
    # Health insurance
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>🏥 Female Health Insurance (", data_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Insured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_with_insurance), "N/A", paste0(counties_with_demographics$pct_female_with_insurance, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #d32f2f; font-weight: bold;'>❌ Uninsured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #d32f2f;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_no_insurance), "N/A", paste0(counties_with_demographics$pct_female_no_insurance, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Data source with year
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 ", data_year, " American Community Survey 5-Year Estimates",
    "</div>",
    
    "</div>"
  )
}

#' Add Basic County Layer (Fallback)
#' @noRd
add_basic_county_layer <- function(leaflet_map, county_boundaries, physician_counts, verbose) {
  
  if (verbose) {
    logger::log_info("Adding basic county layer without demographics")
  }
  
  basic_county_popups <- create_basic_county_popups_enhanced(
    county_boundaries = county_boundaries,
    physician_counts = physician_counts
  )
  
  enhanced_map <- leaflet_map %>%
    leaflet::addPolygons(
      data = county_boundaries,
      weight = 1,
      color = "#888888",
      fillOpacity = 0.1,
      fillColor = "#lightblue",
      popup = basic_county_popups,
      group = "Counties",
      highlightOptions = leaflet::highlightOptions(
        weight = 2,
        color = "#666666",
        fillOpacity = 0.3,
        bringToFront = FALSE
      )
    )
  
  if (verbose) {
    basic_county_count <- nrow(county_boundaries)
    logger::log_info(paste0("Added basic county layer with ", basic_county_count, " counties"))
  }
  
  return(enhanced_map)
}

#' Create Basic County Popups Enhanced
#' @noRd
create_basic_county_popups_enhanced <- function(county_boundaries, physician_counts) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", county_boundaries$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", county_boundaries$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", county_boundaries$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", county_boundaries$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(county_boundaries$ALAND) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 Enable demographics with tidycensus and Census API key</p>",
    "</div>",
    "</div>"
  )
}

#' Finalize Map with Controls
#' @noRd
finalize_map_with_controls <- function(leaflet_map, spatial_physician_data, 
                                       include_county_boundaries, acs_years, default_year, verbose) {
  
  if (verbose) {
    logger::log_info("Finalizing map with layer controls")
  }
  
  # Determine overlay groups based on data structure
  overlay_groups_list <- character(0)
  
  # Add physician groups
  if (is.list(spatial_physician_data)) {
    # Year-specific physician layers
    physician_year_groups <- paste("Physicians", names(spatial_physician_data))
    overlay_groups_list <- c(overlay_groups_list, physician_year_groups)
  } else {
    # Single physician layer
    overlay_groups_list <- c(overlay_groups_list, "Physicians")
  }
  
  # Add county groups
  if (include_county_boundaries) {
    if (length(acs_years) > 1) {
      # Multiple year county layers
      county_year_groups <- paste("Counties", acs_years)
      overlay_groups_list <- c(overlay_groups_list, county_year_groups)
    } else {
      # Single county layer
      overlay_groups_list <- c(overlay_groups_list, "Counties")
    }
  }
  
  # Calculate bounds for fitting the map
  if (is.list(spatial_physician_data)) {
    # Use default year data for bounds, or first available year
    bounds_data <- spatial_physician_data[[as.character(default_year)]]
    if (is.null(bounds_data) && length(spatial_physician_data) > 0) {
      bounds_data <- spatial_physician_data[[1]]
    }
  } else {
    bounds_data <- spatial_physician_data
  }
  
  # Add layer controls and final formatting
  finalized_map <- leaflet_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups_list,
      options = leaflet::layersControlOptions(collapsed = FALSE)
    ) %>%
    leaflet::addScaleBar(position = "bottomleft")
  
  # Fit bounds if we have data
  if (!is.null(bounds_data) && nrow(bounds_data) > 0) {
    finalized_map <- finalized_map %>%
      leaflet::fitBounds(
        lng1 = min(bounds_data$longitude) - 0.1,
        lat1 = min(bounds_data$latitude) - 0.1,
        lng2 = max(bounds_data$longitude) + 0.1,
        lat2 = max(bounds_data$latitude) + 0.1
      )
  }
  
  if (verbose) {
    overlay_groups_count <- length(overlay_groups_list)
    overlay_groups_text <- paste(overlay_groups_list, collapse = ", ")
    
    logger::log_info(paste0("Map finalized with ", overlay_groups_count, " layer groups"))
    logger::log_info(paste0("Layer groups: ", overlay_groups_text))
    
    # Add guidance for multi-year usage
    if (is.list(spatial_physician_data) || (include_county_boundaries && length(acs_years) > 1)) {
      logger::log_info("TIP: Enable matching year layers (e.g., 'Physicians 2020' + 'Counties 2020') for synchronized analysis")
    }
  }
  
  return(finalized_map)
}

# execute -----
join_physician_data_with_sf_output <- join_physician_data_with_sf_output %>%
  dplyr::mutate(
    pfname = pfname_sf,
    plname = plname_sf,
    npi = npi,
    pmailcityname = pmailcityname_sf,
    pmailstatename = pmailstatename_sf,
    pspec = NA_character_,  # or replace with actual if available
    data_year = data_year
  )


enhanced_physician_map_with_years(
  physician_geodata = join_physician_data_with_sf_output,  
  acs_years = 2013:2022,                 # Available years
  default_year = 2022,                   # Shows 2020 initially
  verbose = TRUE
)

# Function at 0722 ----
# Example Usage -----
# join_physician_data_with_sf_output <- join_physician_data_with_sf_output %>%
#   dplyr::mutate(
#     pfname = pfname_sf,
#     plname = plname_sf,
#     npi = npi,
#     pmailcityname = pmailcityname_sf,
#     pmailstatename = pmailstatename_sf,
#     pspec = NA_character_,  # or replace with actual if available
#     data_year = data_year
#   )
# 
# enhanced_physician_map_with_radio_years(
#   physician_geodata = join_physician_data_with_sf_output,  
#   include_county_boundaries = TRUE,
#   include_female_demographics = TRUE,
#   acs_years = 2013:2022,                 # Available years
#   default_year = 2022,                   # Counties and physicians visible initially
#   verbose = TRUE
# )#' Enhanced Physician Map with Radio Button Year Selection
#' 
#' Creates an interactive physician map with radio button year controls that 
#' simultaneously display physician locations and county demographics for the 
#' selected year. Counties are visible by default, and users can switch between 
#' years using radio buttons in the top-left corner. Both physician and county 
#' data update together when a new year is selected.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns and physician information
#' @param include_county_boundaries Logical to include counties with demographics (default: TRUE)
#' @param include_female_demographics Logical to include detailed female demographic data (default: TRUE)
#' @param acs_years Numeric vector of years to include (default: 2022, range: 2012-2023)
#' @param default_year Numeric year to display initially (default: 2022)
#' @param verbose Logical for detailed logging (default: TRUE)
#' 
#' @return Interactive leaflet map with radio button year selector controls
#' 
#' @examples
#' # Example 1: Single year map with counties visible by default
#' enhanced_physician_map_with_radio_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2022,
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 2: Multi-year map with radio button selection (2018-2022)
#' enhanced_physician_map_with_radio_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = c(2018, 2019, 2020, 2021, 2022),
#'   default_year = 2020,
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Decade analysis with radio button year switching (2013-2022)
#' enhanced_physician_map_with_radio_years(
#'   physician_geodata = physician_location_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = TRUE,
#'   acs_years = 2013:2022,
#'   default_year = 2020,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr mutate case_when filter left_join select bind_rows
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addControl hideGroup showGroup
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @importFrom purrr map_dfr safely
#' @importFrom htmltools HTML
#' @export
enhanced_physician_map_with_radio_years <- function(physician_geodata,
                                                    include_county_boundaries = TRUE,
                                                    include_female_demographics = TRUE,
                                                    acs_years = 2022,
                                                    default_year = 2022,
                                                    verbose = TRUE) {
  
  # Input validation and logging
  if (verbose) {
    logger::log_info("Starting enhanced physician map creation with radio button year controls")
    
    physician_input_rows <- nrow(physician_geodata)
    physician_input_cols <- ncol(physician_geodata)
    acs_years_requested <- paste(acs_years, collapse = ", ")
    
    logger::log_info(paste0("Input physician geodata dimensions: ", physician_input_rows, " rows, ", physician_input_cols, " columns"))
    logger::log_info(paste0("ACS years requested: ", acs_years_requested))
    logger::log_info(paste0("Default display year: ", default_year))
  }
  
  # Validate inputs using assertthat
  assertthat::assert_that(
    is.data.frame(physician_geodata),
    msg = "physician_geodata must be a data frame"
  )
  
  assertthat::assert_that(
    is.logical(include_county_boundaries),
    msg = "include_county_boundaries must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.logical(include_female_demographics),
    msg = "include_female_demographics must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.numeric(acs_years) && all(acs_years >= 2012 & acs_years <= 2023),
    msg = "acs_years must be numeric values between 2012 and 2023"
  )
  
  assertthat::assert_that(
    default_year %in% acs_years,
    msg = "default_year must be one of the specified acs_years"
  )
  
  # Validate that physician data contains required columns
  required_coordinate_columns <- c("latitude", "longitude")
  missing_coordinate_columns <- setdiff(required_coordinate_columns, names(physician_geodata))
  
  assertthat::assert_that(
    length(missing_coordinate_columns) == 0,
    msg = paste0("physician_geodata missing required columns: ", paste(missing_coordinate_columns, collapse = ", "))
  )
  
  if (verbose) {
    if (!"data_year" %in% names(physician_geodata)) {
      logger::log_warn("physician_geodata missing 'data_year' column - all physicians will be shown for all years")
    }
  }
  
  # Process physician location data with year-specific filtering
  processed_physician_collections <- process_physician_spatial_data_by_year(
    input_geodata = physician_geodata, 
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (verbose) {
    if (is.list(processed_physician_collections)) {
      total_physicians_all_years <- sum(sapply(processed_physician_collections, nrow))
      available_physician_years <- names(processed_physician_collections)
      logger::log_info(paste0("Processed ", total_physicians_all_years, " physician locations across years: ", paste(available_physician_years, collapse = ", ")))
    } else {
      processed_physician_count_single <- nrow(processed_physician_collections)
      logger::log_info(paste0("Processed ", processed_physician_count_single, " physician locations for single year"))
    }
  }
  
  # Create base map with all year layers (initially hidden except default)
  base_map_with_all_layers <- create_base_map_with_year_layers(
    spatial_physician_collections = processed_physician_collections,
    acs_years = acs_years,
    default_year = default_year,
    verbose = verbose
  )
  
  # Add county boundaries for all years if requested
  if (include_county_boundaries) {
    map_with_county_layers <- add_county_boundary_layers_by_year(
      leaflet_map = base_map_with_all_layers,
      spatial_physician_collections = processed_physician_collections,
      include_female_demographics = include_female_demographics,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    map_with_county_layers <- base_map_with_all_layers
  }
  
  # Add radio button controls for year selection
  final_map_with_radio_controls <- add_radio_button_year_controls(
    leaflet_map = map_with_county_layers,
    acs_years = acs_years,
    default_year = default_year,
    include_county_boundaries = include_county_boundaries,
    verbose = verbose
  )
  
  if (verbose) {
    radio_control_completion_message <- "Enhanced physician map with radio button year controls created successfully"
    logger::log_info(radio_control_completion_message)
    logger::log_info("Counties are visible by default - use radio buttons to switch between years")
  }
  
  return(final_map_with_radio_controls)
}

#' Process Physician Spatial Data by Year
#' @noRd
process_physician_spatial_data_by_year <- function(input_geodata, acs_years, verbose) {
  
  if (verbose) {
    logger::log_info("Processing physician spatial data with year-specific organization")
  }
  
  # Check if data_year column exists for temporal filtering
  has_data_year_column <- "data_year" %in% names(input_geodata)
  
  if (has_data_year_column && length(acs_years) > 1) {
    # Create separate datasets for each year
    if (verbose) {
      available_physician_years <- unique(input_geodata$data_year)
      available_years_formatted <- paste(sort(available_physician_years), collapse = ", ")
      logger::log_info(paste0("Creating year-specific physician datasets. Available years: ", available_years_formatted))
    }
    
    yearly_physician_collections <- list()
    
    for (target_analysis_year in acs_years) {
      # Filter physicians for this specific year
      year_specific_physicians <- input_geodata %>%
        dplyr::filter(data_year == target_analysis_year)
      
      if (nrow(year_specific_physicians) > 0) {
        # Process spatial data for this year
        processed_year_physician_data <- process_single_year_physician_spatial_data(
          physician_yearly_data = year_specific_physicians,
          analysis_year = target_analysis_year,
          verbose = verbose
        )
        
        yearly_physician_collections[[as.character(target_analysis_year)]] <- processed_year_physician_data
        
        if (verbose) {
          year_physician_count <- nrow(processed_year_physician_data)
          logger::log_info(paste0("Year ", target_analysis_year, ": ", year_physician_count, " physicians processed"))
        }
      } else {
        if (verbose) {
          logger::log_warn(paste0("No physicians found for year ", target_analysis_year))
        }
      }
    }
    
    return(yearly_physician_collections)
    
  } else {
    # Single year or no data_year column - process all data together
    if (verbose) {
      if (!has_data_year_column) {
        logger::log_info("No data_year column found - processing all physicians as single dataset")
      } else {
        logger::log_info("Single year analysis - processing all physicians together")
      }
    }
    
    processed_single_year_data <- process_single_year_physician_spatial_data(
      physician_yearly_data = input_geodata,
      analysis_year = acs_years[1], # Use first year as identifier
      verbose = verbose
    )
    
    return(processed_single_year_data)
  }
}

#' Process Single Year Physician Spatial Data
#' @noRd
process_single_year_physician_spatial_data <- function(physician_yearly_data, analysis_year, verbose) {
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_yearly_data)) {
    assertthat::assert_that(
      all(c("latitude", "longitude") %in% names(physician_yearly_data)),
      msg = "Physician data must contain 'latitude' and 'longitude' columns"
    )
    
    clean_physician_coordinates <- physician_yearly_data %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      clean_coordinate_rows <- nrow(clean_physician_coordinates)
      total_input_rows <- nrow(physician_yearly_data)
      removed_invalid_rows <- total_input_rows - clean_coordinate_rows
      
      logger::log_info(paste0("Year ", analysis_year, ": Converting ", clean_coordinate_rows, " rows to spatial data"))
      if (removed_invalid_rows > 0) {
        logger::log_info(paste0("Year ", analysis_year, ": Removed ", removed_invalid_rows, " rows with missing coordinates"))
      }
    }
    
    spatial_physician_year_data <- sf::st_as_sf(
      clean_physician_coordinates,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_physician_year_data <- physician_yearly_data
  }
  
  # Extract coordinates for mapping
  physician_coordinate_matrix <- sf::st_coordinates(spatial_physician_year_data)
  spatial_physician_year_data$longitude <- physician_coordinate_matrix[, "X"]
  spatial_physician_year_data$latitude <- physician_coordinate_matrix[, "Y"]
  
  # Create enhanced popups for physicians with year information
  spatial_physician_year_data <- spatial_physician_year_data %>%
    dplyr::mutate(
      physician_popup_content = dplyr::case_when(
        "pfname" %in% names(.) & "plname" %in% names(.) & "npi" %in% names(.) ~ 
          paste0(
            "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
            "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
            "<h3 style='margin: 0; font-size: 16px;'>👩‍⚕️ Dr. ", pfname, " ", plname, "</h3>",
            "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
            "📅 ", analysis_year, " Data</div>",
            "</div>",
            "<div style='padding: 4px 0;'>",
            "<p style='margin: 2px 0; font-size: 12px;'><strong>📋 NPI:</strong> ", npi, "</p>",
            ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📍 Location:</strong> ", pmailcityname, ", ", pmailstatename, "</p>"), ""),
            ifelse("pspec" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>🩺 Specialty:</strong> ", pspec, "</p>"), ""),
            ifelse("data_year" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Data Year:</strong> ", data_year, "</p>"), ""),
            "</div></div>"
          ),
        TRUE ~ paste0(
          "<div style='font-family: Arial, sans-serif;'>",
          "<h4 style='color: #2E86AB; margin: 0 0 8px 0;'>👩‍⚕️ Physician Details (", analysis_year, ")</h4>",
          "<p style='margin: 2px 0;'>Location information available</p>",
          "</div>"
        )
      )
    )
  
  if (verbose) {
    final_physician_popup_count <- nrow(spatial_physician_year_data)
    logger::log_info(paste0("Year ", analysis_year, ": Created enhanced popups for ", final_physician_popup_count, " physicians"))
  }
  
  return(spatial_physician_year_data)
}

#' Create Base Map with All Year Layers
#' @noRd
create_base_map_with_year_layers <- function(spatial_physician_collections, acs_years, default_year, verbose) {
  
  if (verbose) {
    if (is.list(spatial_physician_collections)) {
      year_layer_count <- length(spatial_physician_collections)
      logger::log_info(paste0("Creating base map with ", year_layer_count, " year-specific physician layers"))
    } else {
      single_layer_physician_count <- nrow(spatial_physician_collections)
      logger::log_info(paste0("Creating base map with single physician layer (", single_layer_physician_count, " physicians)"))
    }
  }
  
  # Initialize base map
  base_leaflet_map <- leaflet::leaflet() %>%
    leaflet::addTiles(group = "OpenStreetMap")
  
  if (is.list(spatial_physician_collections)) {
    # Multiple years - create separate physician layers for each year
    for (year_key in names(spatial_physician_collections)) {
      year_physician_data <- spatial_physician_collections[[year_key]]
      year_numeric_value <- as.numeric(year_key)
      
      # Determine initial visibility - only default year should be visible
      layer_is_default_year <- (year_numeric_value == default_year)
      
      if (nrow(year_physician_data) > 0) {
        # Add physician markers for this year with custom layer ID
        base_leaflet_map <- base_leaflet_map %>%
          leaflet::addCircleMarkers(
            data = year_physician_data,
            lng = ~longitude,
            lat = ~latitude,
            radius = 6,
            fillColor = "#2E86AB",
            color = "white",
            weight = 2,
            opacity = 1,
            fillOpacity = 0.8,
            popup = ~physician_popup_content,
            group = paste("Physicians", year_key),
            layerId = paste0("physician_", seq_len(nrow(year_physician_data)), "_", year_key)
          )
        
        # Hide layer if not default year
        if (!layer_is_default_year) {
          base_leaflet_map <- base_leaflet_map %>%
            leaflet::hideGroup(paste("Physicians", year_key))
        }
        
        if (verbose) {
          year_physician_marker_count <- nrow(year_physician_data)
          visibility_status_text <- ifelse(layer_is_default_year, "visible", "hidden")
          logger::log_info(paste0("Added physician layer for year ", year_key, ": ", year_physician_marker_count, " physicians (", visibility_status_text, ")"))
        }
      }
    }
  } else {
    # Single dataset - create single physician layer
    base_leaflet_map <- base_leaflet_map %>%
      leaflet::addCircleMarkers(
        data = spatial_physician_collections,
        lng = ~longitude,
        lat = ~latitude,
        radius = 6,
        fillColor = "#2E86AB",
        color = "white",
        weight = 2,
        opacity = 1,
        fillOpacity = 0.8,
        popup = ~physician_popup_content,
        group = paste("Physicians", acs_years[1])
      )
    
    if (verbose) {
      single_layer_physician_marker_count <- nrow(spatial_physician_collections)
      logger::log_info(paste0("Added single physician layer with ", single_layer_physician_marker_count, " physicians"))
    }
  }
  
  return(base_leaflet_map)
}

#' Add County Boundary Layers by Year
#' @noRd
add_county_boundary_layers_by_year <- function(leaflet_map, spatial_physician_collections, 
                                               include_female_demographics, acs_years, 
                                               default_year, verbose) {
  
  if (verbose) {
    acs_years_for_counties <- paste(acs_years, collapse = ", ")
    logger::log_info(paste0("Adding county boundary layers for years: ", acs_years_for_counties))
  }
  
  # Check if tigris is available for county download
  if (!requireNamespace("tigris", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
    return(leaflet_map)
  }
  
  # Get primary state from physician data
  primary_state_identification <- extract_primary_state_from_physicians(
    spatial_physician_collections = spatial_physician_collections,
    verbose = verbose
  )
  
  if (is.null(primary_state_identification)) {
    if (verbose) {
      logger::log_warn("Cannot determine primary state from physician data")
    }
    return(leaflet_map)
  }
  
  # Download county boundaries
  county_boundary_geometries <- download_state_county_boundaries(
    primary_state = primary_state_identification,
    verbose = verbose
  )
  
  if (is.null(county_boundary_geometries)) {
    return(leaflet_map)
  }
  
  # Calculate physician counts per county
  physician_counts_by_county <- calculate_physicians_per_county(
    county_boundaries = county_boundary_geometries,
    spatial_physician_collections = spatial_physician_collections,
    verbose = verbose
  )
  
  # Add demographics for each year if requested
  if (include_female_demographics) {
    map_with_demographic_county_layers <- add_demographic_county_layers_by_year(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_geometries,
      physician_county_counts = physician_counts_by_county,
      primary_state = primary_state_identification,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    # Add basic county layer without demographics
    map_with_demographic_county_layers <- add_basic_county_boundary_layer(
      leaflet_map = leaflet_map,
      county_boundaries = county_boundary_geometries,
      physician_county_counts = physician_counts_by_county,
      default_year = default_year,
      verbose = verbose
    )
  }
  
  return(map_with_demographic_county_layers)
}

#' Extract Primary State from Physician Collections
#' @noRd
extract_primary_state_from_physicians <- function(spatial_physician_collections, verbose) {
  
  state_column_candidates <- c("pmailstatename", "plocstatename", "state")
  
  # Handle both list and single data frame inputs
  if (is.list(spatial_physician_collections)) {
    # Use first available year's data to determine state
    first_year_data <- spatial_physician_collections[[1]]
    physician_attributes_table <- sf::st_drop_geometry(first_year_data)
  } else {
    physician_attributes_table <- sf::st_drop_geometry(spatial_physician_collections)
  }
  
  state_column_found <- NULL
  for (column_candidate in state_column_candidates) {
    if (column_candidate %in% names(physician_attributes_table)) {
      state_column_found <- column_candidate
      break
    }
  }
  
  if (!is.null(state_column_found)) {
    primary_state_name <- names(sort(table(physician_attributes_table[[state_column_found]]), decreasing = TRUE))[1]
    
    if (verbose) {
      logger::log_info(paste0("Primary state identified: ", primary_state_name, " from column: ", state_column_found))
    }
    
    return(primary_state_name)
  }
  
  return(NULL)
}

#' Download State County Boundaries
#' @noRd
download_state_county_boundaries <- function(primary_state, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Downloading county boundaries for state: ", primary_state))
  }
  
  tryCatch({
    county_shape_data <- tigris::counties(state = primary_state, cb = TRUE)
    county_shape_data <- sf::st_transform(county_shape_data, crs = 4326)
    
    if (verbose) {
      downloaded_county_count <- nrow(county_shape_data)
      logger::log_info(paste0("Successfully downloaded ", downloaded_county_count, " county boundaries"))
    }
    
    return(county_shape_data)
  }, error = function(error_message) {
    if (verbose) {
      error_details <- error_message$message
      logger::log_error(paste0("Could not download counties: ", error_details))
    }
    return(NULL)
  })
}

#' Calculate Physicians per County
#' @noRd
calculate_physicians_per_county <- function(county_boundaries, spatial_physician_collections, verbose) {
  
  if (verbose) {
    logger::log_info("Calculating physician counts per county using spatial join approximation")
  }
  
  # Handle both list and single data frame inputs
  if (is.list(spatial_physician_collections)) {
    # Use first available year's data for county matching
    first_year_physician_data <- spatial_physician_collections[[1]]
    physician_location_attributes <- sf::st_drop_geometry(first_year_physician_data)
  } else {
    physician_location_attributes <- sf::st_drop_geometry(spatial_physician_collections)
  }
  
  # Simple approximation - count by matching county names in geocoding data
  county_physician_counts <- sapply(1:nrow(county_boundaries), function(county_index) {
    county_name_target <- county_boundaries$NAME[county_index]
    if ("geocoding_county" %in% names(physician_location_attributes)) {
      county_match_count <- sum(grepl(county_name_target, physician_location_attributes$geocoding_county, ignore.case = TRUE), na.rm = TRUE)
    } else {
      county_match_count <- 0
    }
    return(county_match_count)
  })
  
  total_physician_county_matches <- sum(county_physician_counts)
  if (verbose) {
    counties_analyzed <- length(county_physician_counts)
    logger::log_info(paste0("Calculated physician counts: ", total_physician_county_matches, " total across ", counties_analyzed, " counties"))
  }
  
  return(county_physician_counts)
}

#' Add Demographic County Layers by Year
#' @noRd
add_demographic_county_layers_by_year <- function(leaflet_map, county_boundaries, physician_county_counts,
                                                  primary_state, acs_years, default_year, verbose) {
  
  if (verbose) {
    demographic_years_count <- length(acs_years)
    logger::log_info(paste0("Adding demographic county layers for ", demographic_years_count, " years"))
  }
  
  # Check if tidycensus is available
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tidycensus package not available - install with: install.packages('tidycensus')")
    }
    return(add_basic_county_boundary_layer(leaflet_map, county_boundaries, physician_county_counts, default_year, verbose))
  }
  
  # Download demographic data for all years
  multiyear_demographic_collections <- download_multiyear_acs_demographics(
    primary_state = primary_state,
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (length(multiyear_demographic_collections) == 0) {
    if (verbose) {
      logger::log_warn("No demographic data available - falling back to basic counties")
    }
    return(add_basic_county_boundary_layer(leaflet_map, county_boundaries, physician_county_counts, default_year, verbose))
  }
  
  # Add county layers for each year
  enhanced_map_with_demographics <- leaflet_map
  
  for (demographic_year in names(multiyear_demographic_collections)) {
    year_demographic_dataset <- multiyear_demographic_collections[[demographic_year]]
    
    # Join demographics to county boundaries
    counties_with_year_demographics <- county_boundaries %>%
      dplyr::left_join(year_demographic_dataset, by = "GEOID")
    
    # Create popups for this year
    year_county_popup_content <- create_year_demographic_county_popups(
      counties_with_demographics = counties_with_year_demographics,
      physician_county_counts = physician_county_counts,
      demographic_year = as.numeric(demographic_year),
      verbose = verbose
    )
    
    # Determine initial visibility - only default year should be visible initially
    layer_is_default_visible <- (as.numeric(demographic_year) == default_year)
    
    # Add polygon layer for this year
    enhanced_map_with_demographics <- enhanced_map_with_demographics %>%
      leaflet::addPolygons(
        data = counties_with_year_demographics,
        weight = 1,
        color = "#888888",
        fillOpacity = if(layer_is_default_visible) 0.15 else 0.1,
        fillColor = "#lightblue",
        popup = year_county_popup_content,
        group = paste0("counties_", demographic_year),
        highlightOptions = leaflet::highlightOptions(
          weight = 2,
          color = "#666666",
          fillOpacity = 0.3,
          bringToFront = FALSE
        )
      )
    
    # Hide layer if not default year
    if (!layer_is_default_visible) {
      enhanced_map_with_demographics <- enhanced_map_with_demographics %>%
        leaflet::hideGroup(paste0("counties_", demographic_year))
    }
    
    if (verbose) {
      county_demographic_layer_count <- nrow(counties_with_year_demographics)
      visibility_text <- ifelse(layer_is_default_visible, "visible", "hidden")
      logger::log_info(paste0("Added county demographic layer for year ", demographic_year, " with ", county_demographic_layer_count, " counties (", visibility_text, ")"))
    }
  }
  
  return(enhanced_map_with_demographics)
}

#' Download Multi-Year ACS Demographics
#' @noRd
download_multiyear_acs_demographics <- function(primary_state, acs_years, verbose) {
  
  if (verbose) {
    demographic_years_to_download <- length(acs_years)
    logger::log_info(paste0("Downloading ACS demographic data for ", demographic_years_to_download, " years"))
  }
  
  # Filter out problematic years (2010-2011) as ACS 5-year estimates may not be reliable
  valid_acs_years <- acs_years[acs_years >= 2012]
  if (length(valid_acs_years) < length(acs_years)) {
    if (verbose) {
      removed_unreliable_years <- acs_years[acs_years < 2012]
      removed_years_list <- paste(removed_unreliable_years, collapse = ", ")
      logger::log_warn(paste0("Removed years with unreliable ACS data: ", removed_years_list))
    }
  }
  
  # Define comprehensive female demographic variables
  female_demographic_variable_codes <- c(
    # Basic female population
    total_female = "B01001_026",          
    female_under_5 = "B01001_027",        
    female_5_to_17 = "B01001_028",        
    female_65_plus = "B01001_044",        
    
    # Female education
    female_bachelor_plus = "B15002_032",  
    
    # Female health insurance
    female_with_insurance = "B27001_014", 
    female_total_insurance = "B27001_013", 
    
    # Economic variables
    median_household_income = "B19013_001", 
    per_capita_income = "B19301_001",       
    poverty_rate = "B17001_002",            
    total_pop_poverty = "B17001_001",       
    
    # Context
    total_pop = "B01003_001"              
  )
  
  # Create safe version of get_acs
  safe_acs_download <- purrr::safely(tidycensus::get_acs)
  
  # Download data for each valid year
  multiyear_demographic_results <- list()
  
  for (target_demographic_year in valid_acs_years) {
    if (verbose) {
      logger::log_info(paste0("Downloading ACS data for year: ", target_demographic_year))
    }
    
    acs_download_result <- safe_acs_download(
      geography = "county",
      state = primary_state,
      variables = female_demographic_variable_codes,
      year = target_demographic_year,
      survey = "acs5",
      output = "wide"
    )
    
    if (!is.null(acs_download_result$result)) {
      # Process the demographic data
      processed_demographic_calculations <- process_demographic_variable_calculations(
        raw_acs_data = acs_download_result$result,
        demographic_year = target_demographic_year,
        verbose = verbose
      )
      
      multiyear_demographic_results[[as.character(target_demographic_year)]] <- processed_demographic_calculations
      
      if (verbose) {
        processed_demographic_county_count <- nrow(processed_demographic_calculations)
        logger::log_info(paste0("Successfully processed demographics for ", target_demographic_year, ": ", processed_demographic_county_count, " counties"))
      }
    } else {
      if (verbose) {
        acs_error_message <- acs_download_result$error$message
        logger::log_warn(paste0("Failed to download data for year ", target_demographic_year, ": ", acs_error_message))
      }
    }
  }
  
  if (verbose) {
    successful_demographic_years <- length(multiyear_demographic_results)
    logger::log_info(paste0("Downloaded demographic data for ", successful_demographic_years, " years successfully"))
  }
  
  return(multiyear_demographic_results)
}

#' Process Demographic Variable Calculations
#' @noRd
process_demographic_variable_calculations <- function(raw_acs_data, demographic_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Processing demographic variable calculations for year ", demographic_year))
  }
  
  processed_demographic_data <- raw_acs_data %>%
    dplyr::mutate(
      # Basic female age calculations
      female_under_18_total = ifelse(
        !is.na(female_under_5E) & !is.na(female_5_to_17E),
        female_under_5E + female_5_to_17E,
        0
      ),
      
      # Age percentages
      pct_female_under_18 = ifelse(
        !is.na(total_femaleE) & total_femaleE > 0,
        round((female_under_18_total / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_over_65 = ifelse(
        !is.na(female_65_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_65_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      pct_female_of_total_population = ifelse(
        !is.na(total_femaleE) & !is.na(total_popE) & total_popE > 0,
        round((total_femaleE / total_popE) * 100, 1),
        NA_real_
      ),
      
      # Education percentage
      pct_female_bachelor_plus = ifelse(
        !is.na(female_bachelor_plusE) & !is.na(total_femaleE) & total_femaleE > 0,
        round((female_bachelor_plusE / total_femaleE) * 100, 1),
        NA_real_
      ),
      
      # Health insurance
      pct_female_with_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round((female_with_insuranceE / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      pct_female_no_insurance = ifelse(
        !is.na(female_with_insuranceE) & !is.na(female_total_insuranceE) & female_total_insuranceE > 0,
        round(((female_total_insuranceE - female_with_insuranceE) / female_total_insuranceE) * 100, 1),
        NA_real_
      ),
      
      # Economic indicators
      median_household_income_display = ifelse(
        !is.na(median_household_incomeE),
        paste0("$", format(median_household_incomeE, big.mark = ",")),
        "N/A"
      ),
      per_capita_income_display = ifelse(
        !is.na(per_capita_incomeE),
        paste0("$", format(per_capita_incomeE, big.mark = ",")),
        "N/A"
      ),
      pct_poverty = ifelse(
        !is.na(poverty_rateE) & !is.na(total_pop_povertyE) & total_pop_povertyE > 0,
        round((poverty_rateE / total_pop_povertyE) * 100, 1),
        NA_real_
      ),
      
      # Raw counts for display
      female_under_18_count_raw = female_under_18_total,
      female_over_65_count_raw = female_65_plusE,
      female_bachelor_plus_count_raw = female_bachelor_plusE,
      female_with_insurance_count_raw = female_with_insuranceE,
      total_female_count_raw = total_femaleE,
      
      # Add year identifier
      acs_demographic_year = demographic_year
    )
  
  # Select key columns for joining
  processed_demographic_data <- processed_demographic_data %>%
    dplyr::select(
      GEOID, acs_demographic_year,
      # Age demographics
      pct_female_under_18, pct_female_over_65, pct_female_of_total_population,
      female_under_18_count_raw, female_over_65_count_raw, total_female_count_raw,
      # Education
      pct_female_bachelor_plus, female_bachelor_plus_count_raw,
      # Health insurance
      pct_female_with_insurance, pct_female_no_insurance,
      female_with_insurance_count_raw,
      # Economic indicators
      median_household_income_display, per_capita_income_display, pct_poverty
    )
  
  if (verbose) {
    valid_demographic_counties <- sum(!is.na(processed_demographic_data$total_female_count_raw))
    logger::log_info(paste0("Processed demographics for year ", demographic_year, ": ", valid_demographic_counties, " counties with valid data"))
  }
  
  return(processed_demographic_data)
}

#' Create Year Demographic County Popups
#' @noRd
create_year_demographic_county_popups <- function(counties_with_demographics, physician_county_counts, 
                                                  demographic_year, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Creating demographic county popups for year ", demographic_year))
  }
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 320px; font-size: 12px;'>",
    
    # Header with year
    "<div style='background: linear-gradient(135deg, #6f42c1, #28a745); color: white; padding: 8px; margin: -8px -8px 8px -8px; border-radius: 4px;'>",
    "<h3 style='margin: 0; font-size: 14px; font-weight: bold;'>", counties_with_demographics$NAME, " County, ", counties_with_demographics$STUSPS, "</h3>",
    "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
    "📊 ", demographic_year, " ACS Data</div>",
    "</div>",
    
    # Physician count
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<strong style='color: #1976d2;'>👩‍⚕️ Physicians:</strong> ", physician_county_counts, 
    "</div>",
    
    # Female population
    "<div style='background: #fce4ec; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #e91e63;'>",
    "<strong style='color: #c2185b;'>👩 Total Females (", demographic_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$total_female_count_raw), "N/A", format(counties_with_demographics$total_female_count_raw, big.mark = ",")), 
    " (", ifelse(is.na(counties_with_demographics$pct_female_of_total_population), "N/A", paste0(counties_with_demographics$pct_female_of_total_population, "%")), " of population)",
    "</div>",
    
    # Age demographics
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>📊 Age Demographics (", demographic_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #fff3cd; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #856404; font-weight: bold;'>🧒 Under 18</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #856404;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_under_18), "N/A", paste0(counties_with_demographics$pct_female_under_18, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_under_18_count_raw), "", paste0("(", format(counties_with_demographics$female_under_18_count_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #d1ecf1; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #0c5460; font-weight: bold;'>👵 Over 65</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #0c5460;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_over_65), "N/A", paste0(counties_with_demographics$pct_female_over_65, "%")), "</div>",
    "<div style='font-size: 9px; color: #6c757d;'>", 
    ifelse(is.na(counties_with_demographics$female_over_65_count_raw), "", paste0("(", format(counties_with_demographics$female_over_65_count_raw, big.mark = ","), ")")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Education
    "<div style='background: #e8f5e8; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #4caf50;'>",
    "<strong style='color: #2e7d32;'>🎓 Bachelor's Degree+ (", demographic_year, "):</strong><br/>", 
    ifelse(is.na(counties_with_demographics$pct_female_bachelor_plus), "N/A", paste0(counties_with_demographics$pct_female_bachelor_plus, "%")), 
    " of females", 
    ifelse(is.na(counties_with_demographics$female_bachelor_plus_count_raw), "", paste0(" (", format(counties_with_demographics$female_bachelor_plus_count_raw, big.mark = ","), ")")),
    "</div>",
    
    # Economic indicators
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>💰 Economic Indicators (", demographic_year, ")</strong>",
    "<div style='background: #f0f8ff; padding: 6px; margin: 4px 0; border-radius: 4px; border-left: 3px solid #1976d2;'>",
    "<div style='font-size: 11px;'>",
    "<strong>🏠 Median Household Income:</strong> ", counties_with_demographics$median_household_income_display, "<br/>",
    "<strong>👤 Per Capita Income:</strong> ", counties_with_demographics$per_capita_income_display, "<br/>",
    "<strong>📉 Poverty Rate:</strong> ", ifelse(is.na(counties_with_demographics$pct_poverty), "N/A", paste0(counties_with_demographics$pct_poverty, "%")),
    "</div>",
    "</div>",
    "</div>",
    
    # Health insurance
    "<div style='margin: 6px 0;'>",
    "<strong style='color: #6f42c1;'>🏥 Female Health Insurance (", demographic_year, ")</strong>",
    "<div style='display: flex; gap: 8px; margin: 4px 0;'>",
    "<div style='flex: 1; background: #e8f5e8; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #2e7d32; font-weight: bold;'>✅ Insured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #2e7d32;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_with_insurance), "N/A", paste0(counties_with_demographics$pct_female_with_insurance, "%")), "</div>",
    "</div>",
    "<div style='flex: 1; background: #ffebee; padding: 6px; border-radius: 4px; text-align: center;'>",
    "<div style='font-size: 10px; color: #d32f2f; font-weight: bold;'>❌ Uninsured</div>",
    "<div style='font-size: 16px; font-weight: bold; color: #d32f2f;'>", 
    ifelse(is.na(counties_with_demographics$pct_female_no_insurance), "N/A", paste0(counties_with_demographics$pct_female_no_insurance, "%")), "</div>",
    "</div>",
    "</div>",
    "</div>",
    
    # Data source with year
    "<div style='font-size: 9px; color: #999; text-align: center; margin-top: 8px; padding-top: 4px; border-top: 1px solid #eee;'>",
    "📊 ", demographic_year, " American Community Survey 5-Year Estimates",
    "</div>",
    
    "</div>"
  )
}

#' Add Basic County Boundary Layer (Fallback)
#' @noRd
add_basic_county_boundary_layer <- function(leaflet_map, county_boundaries, physician_county_counts, default_year, verbose) {
  
  if (verbose) {
    logger::log_info("Adding basic county boundary layer without demographics")
  }
  
  basic_county_popup_content <- create_basic_county_popup_content(
    county_boundaries = county_boundaries,
    physician_county_counts = physician_county_counts,
    default_year = default_year
  )
  
  enhanced_map_with_basic_counties <- leaflet_map %>%
    leaflet::addPolygons(
      data = county_boundaries,
      weight = 1,
      color = "#888888",
      fillOpacity = 0.15,  # Visible by default
      fillColor = "#lightblue",
      popup = basic_county_popup_content,
      group = paste0("counties_", default_year),
      highlightOptions = leaflet::highlightOptions(
        weight = 2,
        color = "#666666",
        fillOpacity = 0.3,
        bringToFront = FALSE
      )
    )
  
  if (verbose) {
    basic_county_boundary_count <- nrow(county_boundaries)
    logger::log_info(paste0("Added basic county boundary layer with ", basic_county_boundary_count, " counties (visible by default)"))
  }
  
  return(enhanced_map_with_basic_counties)
}

#' Create Basic County Popup Content
#' @noRd
create_basic_county_popup_content <- function(county_boundaries, physician_county_counts, default_year) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>📍 ", county_boundaries$NAME, " County</h3>",
    "<span style='background: rgba(255,255,255,0.25); padding: 2px 6px; border-radius: 8px; font-size: 11px; font-weight: bold;'>", county_boundaries$STUSPS, "</span>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ County Code:</strong> ", county_boundaries$COUNTYFP, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", county_boundaries$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(county_boundaries$ALAND) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #f8f9fa; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2E86AB;'>",
    "<p style='margin: 0; font-size: 11px; color: #666;'>👩‍⚕️ This county contains ", 
    "<strong>", physician_county_counts, " physicians</strong> from your dataset</p>",
    "</div>",
    "<div style='background: #fff3cd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #ffc107;'>",
    "<p style='margin: 0; font-size: 10px; color: #856404;'>💡 Enable demographics with tidycensus and Census API key</p>",
    "</div>",
    "</div>"
  )
}

#' Add Radio Button Year Controls
#' @noRd
add_radio_button_year_controls <- function(leaflet_map, acs_years, default_year, include_county_boundaries, verbose) {
  
  if (verbose) {
    radio_years_count <- length(acs_years)
    logger::log_info(paste0("Adding radio button controls for ", radio_years_count, " years"))
  }
  
  # Only add radio controls if there are multiple years
  if (length(acs_years) > 1) {
    
    # Create radio button HTML
    radio_button_html <- create_radio_button_html(
      acs_years = acs_years,
      default_year = default_year,
      include_county_boundaries = include_county_boundaries,
      verbose = verbose
    )
    
    # Add the radio button control to the map
    map_with_radio_controls <- leaflet_map %>%
      leaflet::addControl(
        html = radio_button_html,
        position = "topleft"
      )
    
    if (verbose) {
      logger::log_info("Radio button year selector controls added successfully")
    }
    
  } else {
    # Single year - no radio buttons needed
    map_with_radio_controls <- leaflet_map
    
    if (verbose) {
      logger::log_info("Single year detected - no radio button controls needed")
    }
  }
  
  return(map_with_radio_controls)
}

#' Create Radio Button HTML
#' @noRd
create_radio_button_html <- function(acs_years, default_year, include_county_boundaries, verbose) {
  
  if (verbose) {
    logger::log_info("Creating radio button HTML controls")
  }
  
  # Create radio button options
  radio_options <- paste(sapply(acs_years, function(year_option) {
    is_default_checked <- if(year_option == default_year) "checked" else ""
    paste0(
      "<label style='display: block; margin: 4px 0; cursor: pointer; padding: 2px;'>",
      "<input type='radio' name='yearSelector' value='", year_option, "' ", is_default_checked, " ",
      "style='margin-right: 6px;'/>",
      "<span style='font-size: 13px; font-weight: bold;'>", year_option, "</span>",
      "</label>"
    )
  }), collapse = "")
  
  # Create the complete HTML control
  radio_control_html <- paste0(
    "<div style='background: rgba(255,255,255,0.95); padding: 12px; border-radius: 8px; ",
    "box-shadow: 0 2px 10px rgba(0,0,0,0.2); border: 1px solid #ddd; min-width: 120px;'>",
    "<div style='font-size: 14px; font-weight: bold; margin-bottom: 8px; color: #333; text-align: center;'>",
    "📅 Select Year",
    "</div>",
    radio_options,
    "</div>",
    
    # JavaScript for handling radio button changes
    "<script>",
    "document.addEventListener('DOMContentLoaded', function() {",
    "  const radios = document.querySelectorAll('input[name=\"yearSelector\"]');",
    "  radios.forEach(function(radio) {",
    "    radio.addEventListener('change', function() {",
    "      if (this.checked) {",
    "        const selectedYear = this.value;",
    "        console.log('Year selected:', selectedYear);",
    "        ",
    "        // Hide all physician layers",
    paste(sapply(acs_years, function(y) {
      paste0("        window.map.layerManager.getLayer(null, null, 'physicians_", y, "').setStyle({opacity: 0, fillOpacity: 0});")
    }), collapse = "\n"),
    "        ",
    "        // Show selected physician layer",
    "        const physicianLayerName = 'physicians_' + selectedYear;",
    "        window.map.layerManager.getLayer(null, null, physicianLayerName).setStyle({opacity: 1, fillOpacity: 0.8});",
    "        ",
    if(include_county_boundaries) {
      paste0(
        "        // Hide all county layers",
        paste(sapply(acs_years, function(y) {
          paste0("        window.map.layerManager.getLayer(null, null, 'counties_", y, "').setStyle({opacity: 0, fillOpacity: 0});")
        }), collapse = "\n"),
        "        ",
        "        // Show selected county layer",
        "        const countyLayerName = 'counties_' + selectedYear;",
        "        window.map.layerManager.getLayer(null, null, countyLayerName).setStyle({opacity: 1, fillOpacity: 0.15});"
      )
    } else "",
    "      }",
    "    });",
    "  });",
    "});",
    "</script>"
  )
  
  return(htmltools::HTML(radio_control_html))
}

# execute -----
enhanced_physician_map_with_radio_years(
  physician_geodata = join_physician_data_with_sf_output,  
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  acs_years = 2013:2022,
  default_year = 2022,
  verbose = TRUE
)

# function 0740 with only Rstudio/leaflet controls ----
#' Enhanced Physician Map with Native Leaflet Year Controls
#' 
#' Creates an interactive physician map with Leaflet's native layer controls for 
#' year selection. Both physician locations and county demographics can be toggled 
#' by year using the built-in layer control panel. This approach is more reliable 
#' than custom JavaScript controls, especially in RStudio viewer.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns and 
#'   physician information. Must contain 'latitude' and 'longitude' columns.
#'   If 'data_year' column exists, physicians will be filtered by year.
#' @param include_county_boundaries Logical indicating whether to include 
#'   counties with demographics (default: TRUE)
#' @param include_female_demographics Logical indicating whether to include 
#'   detailed female demographic data from ACS (default: TRUE)
#' @param acs_years Numeric vector of years to include in analysis 
#'   (default: 2022, valid range: 2012-2023)
#' @param default_year Numeric year to display initially, must be one of 
#'   acs_years (default: 2022)
#' @param verbose Logical indicating whether to enable detailed console logging 
#'   (default: TRUE)
#' 
#' @return Interactive leaflet map object with native layer controls for 
#'   year selection and synchronized physician/county layers
#' 
#' @examples
#' # Example 1: Single year map with counties visible by default
#' physician_test_data <- data.frame(
#'   latitude = c(39.7392, 39.8561, 39.6403),
#'   longitude = c(-104.9903, -105.0178, -105.0178),
#'   pfname = c("John", "Jane", "Bob"),
#'   plname = c("Smith", "Doe", "Johnson"),
#'   npi = c("1234567890", "0987654321", "1122334455"),
#'   pmailcityname = c("Denver", "Boulder", "Lakewood"),
#'   pmailstatename = c("Colorado", "Colorado", "Colorado"),
#'   data_year = c(2022, 2022, 2022)
#' )
#' enhanced_physician_map_with_layer_controls(
#'   physician_geodata = physician_test_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   acs_years = 2022,
#'   default_year = 2022,
#'   verbose = TRUE
#' )
#' 
#' # Example 2: Multi-year map with layer controls (2020-2022)
#' physician_multiyear_data <- data.frame(
#'   latitude = rep(c(39.7392, 39.8561, 39.6403), 3),
#'   longitude = rep(c(-104.9903, -105.0178, -105.0178), 3),
#'   pfname = rep(c("John", "Jane", "Bob"), 3),
#'   plname = rep(c("Smith", "Doe", "Johnson"), 3),
#'   npi = rep(c("1234567890", "0987654321", "1122334455"), 3),
#'   pmailcityname = rep(c("Denver", "Boulder", "Lakewood"), 3),
#'   pmailstatename = rep(c("Colorado", "Colorado", "Colorado"), 3),
#'   data_year = c(rep(2020, 3), rep(2021, 3), rep(2022, 3))
#' )
#' enhanced_physician_map_with_layer_controls(
#'   physician_geodata = physician_multiyear_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   acs_years = c(2020, 2021, 2022),
#'   default_year = 2021,
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Decade analysis with layer controls (2018-2022)
#' physician_decade_data <- data.frame(
#'   latitude = rep(c(39.7392, 39.8561, 39.6403, 39.5501), 5),
#'   longitude = rep(c(-104.9903, -105.0178, -105.0178, -105.7821), 5),
#'   pfname = rep(c("John", "Jane", "Bob", "Alice"), 5),
#'   plname = rep(c("Smith", "Doe", "Johnson", "Wilson"), 5),
#'   npi = rep(c("1234567890", "0987654321", "1122334455", "9988776655"), 5),
#'   pmailcityname = rep(c("Denver", "Boulder", "Lakewood", "Arvada"), 5),
#'   pmailstatename = rep(c("Colorado", "Colorado", "Colorado", "Colorado"), 5),
#'   pspec = rep(c("Cardiology", "Pediatrics", "Internal Medicine", "Surgery"), 5),
#'   data_year = c(rep(2018, 4), rep(2019, 4), rep(2020, 4), 
#'                 rep(2021, 4), rep(2022, 4))
#' )
#' enhanced_physician_map_with_layer_controls(
#'   physician_geodata = physician_decade_data,
#'   include_county_boundaries = TRUE,
#'   include_female_demographics = FALSE,
#'   acs_years = 2018:2022,
#'   default_year = 2020,
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr mutate case_when filter left_join select bind_rows
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry st_transform
#' @importFrom leaflet leaflet addTiles addCircleMarkers addPolygons
#' @importFrom leaflet addLayersControl layersControlOptions hideGroup
#' @importFrom leaflet highlightOptions labelOptions
#' @importFrom tidycensus get_acs
#' @importFrom purrr map_dfr safely
#' @importFrom htmltools HTML
#' @export
enhanced_physician_map_with_layer_controls <- function(physician_geodata,
                                                       include_county_boundaries = TRUE,
                                                       include_female_demographics = TRUE,
                                                       acs_years = 2022,
                                                       default_year = 2022,
                                                       verbose = TRUE) {
  
  # Input validation and logging
  if (verbose) {
    logger::log_info("Starting enhanced physician map creation with native Leaflet layer controls")
    
    physician_input_rows <- nrow(physician_geodata)
    physician_input_cols <- ncol(physician_geodata)
    acs_years_requested <- paste(acs_years, collapse = ", ")
    
    logger::log_info(paste0("Input physician geodata dimensions: ", physician_input_rows, " rows, ", physician_input_cols, " columns"))
    logger::log_info(paste0("ACS years requested: ", acs_years_requested))
    logger::log_info(paste0("Default display year: ", default_year))
    logger::log_info(paste0("Include county boundaries: ", include_county_boundaries))
    logger::log_info(paste0("Include female demographics: ", include_female_demographics))
  }
  
  # Validate inputs using assertthat
  assertthat::assert_that(
    is.data.frame(physician_geodata),
    msg = "physician_geodata must be a data frame"
  )
  
  assertthat::assert_that(
    is.logical(include_county_boundaries),
    msg = "include_county_boundaries must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.logical(include_female_demographics),
    msg = "include_female_demographics must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.numeric(acs_years) && all(acs_years >= 2012 & acs_years <= 2023),
    msg = "acs_years must be numeric values between 2012 and 2023"
  )
  
  assertthat::assert_that(
    default_year %in% acs_years,
    msg = "default_year must be one of the specified acs_years"
  )
  
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be TRUE or FALSE"
  )
  
  # Validate that physician data contains required columns
  required_coordinate_columns <- c("latitude", "longitude")
  missing_coordinate_columns <- setdiff(required_coordinate_columns, names(physician_geodata))
  
  assertthat::assert_that(
    length(missing_coordinate_columns) == 0,
    msg = paste0("physician_geodata missing required columns: ", paste(missing_coordinate_columns, collapse = ", "))
  )
  
  if (verbose) {
    if (!"data_year" %in% names(physician_geodata)) {
      logger::log_warn("physician_geodata missing 'data_year' column - all physicians will be shown for all years")
    }
    logger::log_info("Input validation completed successfully")
  }
  
  # Process physician location data with year-specific filtering
  processed_physician_collections <- process_physician_spatial_data_by_year_native(
    input_geodata = physician_geodata, 
    acs_years = acs_years,
    verbose = verbose
  )
  
  if (verbose) {
    if (is.list(processed_physician_collections)) {
      total_physicians_all_years <- sum(sapply(processed_physician_collections, nrow))
      available_physician_years <- names(processed_physician_collections)
      logger::log_info(paste0("Processed ", total_physicians_all_years, " physician locations across years: ", paste(available_physician_years, collapse = ", ")))
    } else {
      processed_physician_count_single <- nrow(processed_physician_collections)
      logger::log_info(paste0("Processed ", processed_physician_count_single, " physician locations for single year"))
    }
  }
  
  # Create base map 
  base_leaflet_map <- leaflet::leaflet() %>%
    leaflet::addTiles(group = "OpenStreetMap")
  
  if (verbose) {
    logger::log_info("Created base leaflet map with OpenStreetMap tiles")
  }
  
  # Add physician layers for each year
  map_with_physician_layers <- add_physician_layers_by_year_native(
    leaflet_map = base_leaflet_map,
    spatial_physician_collections = processed_physician_collections,
    acs_years = acs_years,
    default_year = default_year,
    verbose = verbose
  )
  
  # Add county boundaries for each year if requested
  if (include_county_boundaries) {
    map_with_county_layers <- add_county_layers_by_year_native(
      leaflet_map = map_with_physician_layers,
      spatial_physician_collections = processed_physician_collections,
      acs_years = acs_years,
      default_year = default_year,
      verbose = verbose
    )
  } else {
    map_with_county_layers <- map_with_physician_layers
    if (verbose) {
      logger::log_info("County boundaries disabled - skipping county layer creation")
    }
  }
  
  # Hide all layers except default year, then add layer controls
  final_map_with_layer_controls <- hide_non_default_layers_and_add_controls(
    leaflet_map = map_with_county_layers,
    acs_years = acs_years,
    default_year = default_year,
    include_county_boundaries = include_county_boundaries,
    verbose = verbose
  )
  
  if (verbose) {
    layer_control_completion_message <- "Enhanced physician map with native Leaflet layer controls created successfully"
    logger::log_info(layer_control_completion_message)
    logger::log_info("Only the default year is visible initially - use the layer control panel (top-right) to toggle other years")
    logger::log_info("Uncheck current year layers and check different year layers to see the changes")
  }
  
  return(final_map_with_layer_controls)
}

#' Process Physician Spatial Data by Year for Native Controls
#' @noRd
process_physician_spatial_data_by_year_native <- function(input_geodata, acs_years, verbose) {
  
  if (verbose) {
    logger::log_info("Processing physician spatial data with year-specific organization for native controls")
  }
  
  # Check if data_year column exists for temporal filtering
  has_data_year_column <- "data_year" %in% names(input_geodata)
  
  if (has_data_year_column && length(acs_years) > 1) {
    # Create separate datasets for each year
    if (verbose) {
      available_physician_years <- unique(input_geodata$data_year)
      available_years_formatted <- paste(sort(available_physician_years), collapse = ", ")
      logger::log_info(paste0("Creating year-specific physician datasets. Available years: ", available_years_formatted))
    }
    
    yearly_physician_collections <- list()
    
    for (target_analysis_year in acs_years) {
      # Filter physicians for this specific year
      year_specific_physicians <- input_geodata %>%
        dplyr::filter(data_year == target_analysis_year)
      
      if (nrow(year_specific_physicians) > 0) {
        # Process spatial data for this year
        processed_year_physician_data <- process_single_year_physician_spatial_data_native(
          physician_yearly_data = year_specific_physicians,
          analysis_year = target_analysis_year,
          verbose = verbose
        )
        
        yearly_physician_collections[[as.character(target_analysis_year)]] <- processed_year_physician_data
        
        if (verbose) {
          year_physician_count <- nrow(processed_year_physician_data)
          logger::log_info(paste0("Year ", target_analysis_year, ": ", year_physician_count, " physicians processed"))
        }
      } else {
        if (verbose) {
          logger::log_warn(paste0("No physicians found for year ", target_analysis_year))
        }
        # Create empty dataset for this year to maintain consistency
        empty_year_data <- create_empty_physician_dataset(target_analysis_year)
        yearly_physician_collections[[as.character(target_analysis_year)]] <- empty_year_data
      }
    }
    
    return(yearly_physician_collections)
    
  } else {
    # Single year or no data_year column - process all data together
    if (verbose) {
      if (!has_data_year_column) {
        logger::log_info("No data_year column found - processing all physicians as single dataset")
      } else {
        logger::log_info("Single year analysis - processing all physicians together")
      }
    }
    
    processed_single_year_data <- process_single_year_physician_spatial_data_native(
      physician_yearly_data = input_geodata,
      analysis_year = acs_years[1], # Use first year as identifier
      verbose = verbose
    )
    
    # Return as list for consistency with multi-year processing
    single_year_list <- list()
    single_year_list[[as.character(acs_years[1])]] <- processed_single_year_data
    
    return(single_year_list)
  }
}

#' Process Single Year Physician Spatial Data for Native Controls
#' @noRd
process_single_year_physician_spatial_data_native <- function(physician_yearly_data, analysis_year, verbose) {
  
  # Convert to sf if needed
  if (!"sf" %in% class(physician_yearly_data)) {
    assertthat::assert_that(
      all(c("latitude", "longitude") %in% names(physician_yearly_data)),
      msg = "Physician data must contain 'latitude' and 'longitude' columns"
    )
    
    clean_physician_coordinates <- physician_yearly_data %>%
      dplyr::filter(!is.na(latitude) & !is.na(longitude))
    
    if (verbose) {
      clean_coordinate_rows <- nrow(clean_physician_coordinates)
      total_input_rows <- nrow(physician_yearly_data)
      removed_invalid_rows <- total_input_rows - clean_coordinate_rows
      
      logger::log_info(paste0("Year ", analysis_year, ": Converting ", clean_coordinate_rows, " rows to spatial data"))
      if (removed_invalid_rows > 0) {
        logger::log_warn(paste0("Year ", analysis_year, ": Removed ", removed_invalid_rows, " rows with missing coordinates"))
      }
    }
    
    spatial_physician_year_data <- sf::st_as_sf(
      clean_physician_coordinates,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  } else {
    spatial_physician_year_data <- physician_yearly_data
  }
  
  # Extract coordinates for mapping
  physician_coordinate_matrix <- sf::st_coordinates(spatial_physician_year_data)
  spatial_physician_year_data$longitude <- physician_coordinate_matrix[, "X"]
  spatial_physician_year_data$latitude <- physician_coordinate_matrix[, "Y"]
  
  # Create enhanced popups for physicians with year information
  spatial_physician_year_data <- spatial_physician_year_data %>%
    dplyr::mutate(
      physician_popup_content = dplyr::case_when(
        "pfname" %in% names(.) & "plname" %in% names(.) & "npi" %in% names(.) ~ 
          paste0(
            "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
            "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
            "<h3 style='margin: 0; font-size: 16px;'>👩‍⚕️ Dr. ", pfname, " ", plname, "</h3>",
            "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
            "📅 ", analysis_year, " Data</div>",
            "</div>",
            "<div style='padding: 4px 0;'>",
            "<p style='margin: 2px 0; font-size: 12px;'><strong>📋 NPI:</strong> ", npi, "</p>",
            ifelse("pmailcityname" %in% names(.) & "pmailstatename" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📍 Location:</strong> ", pmailcityname, ", ", pmailstatename, "</p>"), ""),
            ifelse("pspec" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>🩺 Specialty:</strong> ", pspec, "</p>"), ""),
            ifelse("data_year" %in% names(.),
                   paste0("<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Data Year:</strong> ", data_year, "</p>"), ""),
            "</div></div>"
          ),
        TRUE ~ paste0(
          "<div style='font-family: Arial, sans-serif;'>",
          "<h4 style='color: #2E86AB; margin: 0 0 8px 0;'>👩‍⚕️ Physician (", analysis_year, ")</h4>",
          "<p style='margin: 2px 0; font-size: 12px;'>Coordinates: ", round(latitude, 4), ", ", round(longitude, 4), "</p>",
          "</div>"
        )
      )
    )
  
  if (verbose) {
    final_physician_popup_count <- nrow(spatial_physician_year_data)
    logger::log_info(paste0("Year ", analysis_year, ": Created enhanced popups for ", final_physician_popup_count, " physicians"))
  }
  
  return(spatial_physician_year_data)
}

#' Create Empty Physician Dataset
#' @noRd
create_empty_physician_dataset <- function(analysis_year) {
  # Create empty sf object with same structure for consistency
  empty_sf <- sf::st_sf(
    physician_popup_content = character(0),
    longitude = numeric(0),
    latitude = numeric(0),
    geometry = sf::st_sfc(crs = 4326)
  )
  return(empty_sf)
}

#' Add Physician Layers by Year for Native Controls
#' @noRd
add_physician_layers_by_year_native <- function(leaflet_map, spatial_physician_collections, 
                                                acs_years, default_year, verbose) {
  
  if (verbose) {
    total_layers_to_create <- length(acs_years)
    logger::log_info(paste0("Adding ", total_layers_to_create, " physician layers for native layer control"))
  }
  
  enhanced_map_with_physicians <- leaflet_map
  
  # Add physician layers for each year
  for (analysis_year in acs_years) {
    year_key <- as.character(analysis_year)
    
    if (year_key %in% names(spatial_physician_collections)) {
      year_physician_data <- spatial_physician_collections[[year_key]]
      physician_group_name <- paste0("👩‍⚕️ Physicians ", analysis_year)
      
      if (nrow(year_physician_data) > 0) {
        # Add physician markers for this year
        enhanced_map_with_physicians <- enhanced_map_with_physicians %>%
          leaflet::addCircleMarkers(
            data = year_physician_data,
            lng = ~longitude,
            lat = ~latitude,
            radius = 7,
            fillColor = "#E91E63",  # Different color for each year could be added
            color = "white",
            weight = 2,
            opacity = 1,
            fillOpacity = 0.8,
            popup = ~physician_popup_content,
            group = physician_group_name,
            layerId = paste0("physician_", seq_len(nrow(year_physician_data)), "_", analysis_year)
          )
        
        if (verbose) {
          year_physician_count <- nrow(year_physician_data)
          logger::log_info(paste0("Added physician layer '", physician_group_name, "' with ", year_physician_count, " physicians"))
        }
      } else {
        if (verbose) {
          logger::log_info(paste0("Year ", analysis_year, " has no physicians - creating empty layer for consistency"))
        }
      }
    } else {
      if (verbose) {
        logger::log_warn(paste0("No physician data found for year ", analysis_year))
      }
    }
  }
  
  return(enhanced_map_with_physicians)
}

#' Add County Layers by Year for Native Controls
#' @noRd
add_county_layers_by_year_native <- function(leaflet_map, spatial_physician_collections, 
                                             acs_years, default_year, verbose) {
  
  if (verbose) {
    logger::log_info("Adding county boundary layers for native layer control")
  }
  
  # Check if tigris is available for county download
  if (!requireNamespace("tigris", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tigris package not available - install with: install.packages('tigris')")
    }
    return(leaflet_map)
  }
  
  # Get primary state from physician data
  primary_state_identification <- extract_primary_state_from_physicians_native(
    spatial_physician_collections = spatial_physician_collections,
    verbose = verbose
  )
  
  if (is.null(primary_state_identification)) {
    if (verbose) {
      logger::log_warn("Cannot determine primary state from physician data")
    }
    return(leaflet_map)
  }
  
  # Download county boundaries
  county_boundary_geometries <- download_state_county_boundaries_native(
    primary_state = primary_state_identification,
    verbose = verbose
  )
  
  if (is.null(county_boundary_geometries)) {
    return(leaflet_map)
  }
  
  # Add county layers for each year
  enhanced_map_with_counties <- leaflet_map
  
  for (analysis_year in acs_years) {
    county_group_name <- paste0("🏘️ Counties ", analysis_year)
    
    county_popup_content <- create_county_popup_content_native(
      county_boundaries = county_boundary_geometries,
      analysis_year = analysis_year
    )
    
    enhanced_map_with_counties <- enhanced_map_with_counties %>%
      leaflet::addPolygons(
        data = county_boundary_geometries,
        weight = 1,
        color = "#888888",
        fillOpacity = 0.1,
        fillColor = "#87CEEB",  # Different colors could be assigned per year
        popup = county_popup_content,
        group = county_group_name,
        highlightOptions = leaflet::highlightOptions(
          weight = 2,
          color = "#666666",
          fillOpacity = 0.3,
          bringToFront = FALSE
        )
      )
    
    if (verbose) {
      county_count_for_year <- nrow(county_boundary_geometries)
      logger::log_info(paste0("Added county layer '", county_group_name, "' with ", county_count_for_year, " counties"))
    }
  }
  
  return(enhanced_map_with_counties)
}

#' Extract Primary State from Physician Collections for Native Controls
#' @noRd
extract_primary_state_from_physicians_native <- function(spatial_physician_collections, verbose) {
  
  state_column_candidates <- c("pmailstatename", "plocstatename", "state")
  
  # Get first available year's data to determine state
  if (is.list(spatial_physician_collections) && length(spatial_physician_collections) > 0) {
    first_year_data <- spatial_physician_collections[[1]]
    if (nrow(first_year_data) > 0) {
      physician_attributes_table <- sf::st_drop_geometry(first_year_data)
    } else {
      # Try other years if first is empty
      for (year_data in spatial_physician_collections) {
        if (nrow(year_data) > 0) {
          physician_attributes_table <- sf::st_drop_geometry(year_data)
          break
        }
      }
      if (!exists("physician_attributes_table")) {
        return(NULL)
      }
    }
  } else {
    return(NULL)
  }
  
  state_column_found <- NULL
  for (column_candidate in state_column_candidates) {
    if (column_candidate %in% names(physician_attributes_table)) {
      state_column_found <- column_candidate
      break
    }
  }
  
  if (!is.null(state_column_found)) {
    state_values <- physician_attributes_table[[state_column_found]]
    state_values <- state_values[!is.na(state_values) & state_values != ""]
    
    if (length(state_values) > 0) {
      primary_state_name <- names(sort(table(state_values), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info(paste0("Primary state identified: ", primary_state_name, " from column: ", state_column_found))
      }
      
      return(primary_state_name)
    }
  }
  
  return(NULL)
}

#' Download State County Boundaries for Native Controls
#' @noRd
download_state_county_boundaries_native <- function(primary_state, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Downloading county boundaries for state: ", primary_state))
  }
  
  tryCatch({
    county_shape_data <- tigris::counties(state = primary_state, cb = TRUE)
    county_shape_data <- sf::st_transform(county_shape_data, crs = 4326)
    
    if (verbose) {
      downloaded_county_count <- nrow(county_shape_data)
      logger::log_info(paste0("Successfully downloaded ", downloaded_county_count, " county boundaries"))
    }
    
    return(county_shape_data)
  }, error = function(error_message) {
    if (verbose) {
      error_details <- error_message$message
      logger::log_error(paste0("Could not download counties: ", error_details))
    }
    return(NULL)
  })
}

#' Create County Popup Content for Native Controls
#' @noRd
create_county_popup_content_native <- function(county_boundaries, analysis_year) {
  
  paste0(
    "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
    "<div style='background: linear-gradient(135deg, #2E86AB, #A23B72); color: white; padding: 10px; margin: -8px -8px 8px -8px; border-radius: 6px;'>",
    "<h3 style='margin: 0; font-size: 16px;'>🏘️ ", county_boundaries$NAME, " County</h3>",
    "<div style='background: rgba(255,255,255,0.25); padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; margin-top: 4px;'>",
    "📅 ", analysis_year, " View</div>",
    "</div>",
    "<div style='padding: 4px 0;'>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🏛️ State:</strong> ", county_boundaries$STUSPS, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>🌍 FIPS Code:</strong> ", county_boundaries$GEOID, "</p>",
    "<p style='margin: 2px 0; font-size: 12px;'><strong>📊 Land Area:</strong> ", round(as.numeric(county_boundaries$ALAND) / 2589988.11, 1), " sq miles</p>",
    "</div>",
    "<div style='background: #e3f2fd; padding: 6px; margin: 6px 0; border-radius: 4px; border-left: 3px solid #2196f3;'>",
    "<p style='margin: 0; font-size: 10px; color: #1565c0;'>🗺️ County data for analysis year ", analysis_year, "</p>",
    "</div>",
    "</div>"
  )
}

#' Hide Non-Default Layers and Add Controls
#' @noRd
hide_non_default_layers_and_add_controls <- function(leaflet_map, acs_years, default_year, include_county_boundaries, verbose) {
  
  if (verbose) {
    logger::log_info(paste0("Hiding all layers except default year ", default_year, " and adding layer controls"))
  }
  
  # First, hide all layers except default year
  map_with_hidden_layers <- leaflet_map
  
  for (analysis_year in acs_years) {
    if (analysis_year != default_year) {
      # Hide non-default physician layers
      physician_group_name <- paste0("👩‍⚕️ Physicians ", analysis_year)
      map_with_hidden_layers <- map_with_hidden_layers %>%
        leaflet::hideGroup(physician_group_name)
      
      if (include_county_boundaries) {
        # Hide non-default county layers
        county_group_name <- paste0("🏘️ Counties ", analysis_year)
        map_with_hidden_layers <- map_with_hidden_layers %>%
          leaflet::hideGroup(county_group_name)
      }
      
      if (verbose) {
        logger::log_info(paste0("Hidden layers for year ", analysis_year))
      }
    } else {
      if (verbose) {
        logger::log_info(paste0("Keeping visible: default year ", analysis_year, " layers"))
      }
    }
  }
  
  # Now add layer controls
  final_map_with_controls <- add_native_layer_controls(
    leaflet_map = map_with_hidden_layers,
    acs_years = acs_years,
    include_county_boundaries = include_county_boundaries,
    verbose = verbose
  )
  
  return(final_map_with_controls)
}

#' Add Native Layer Controls
#' @noRd
add_native_layer_controls <- function(leaflet_map, acs_years, include_county_boundaries, verbose) {
  
  if (verbose) {
    layer_control_count <- length(acs_years)
    logger::log_info(paste0("Adding native Leaflet layer controls for ", layer_control_count, " years"))
  }
  
  # Create overlay groups list
  overlay_groups_list <- list()
  
  # Add physician layers to overlay groups
  for (analysis_year in acs_years) {
    physician_group_name <- paste0("👩‍⚕️ Physicians ", analysis_year)
    overlay_groups_list[[physician_group_name]] <- physician_group_name
    
    if (include_county_boundaries) {
      county_group_name <- paste0("🏘️ Counties ", analysis_year)
      overlay_groups_list[[county_group_name]] <- county_group_name
    }
  }
  
  # Convert to character vector for addLayersControl
  overlay_groups_vector <- unlist(overlay_groups_list)
  
  # Add layer controls
  map_with_native_controls <- leaflet_map %>%
    leaflet::addLayersControl(
      baseGroups = "OpenStreetMap",
      overlayGroups = overlay_groups_vector,
      options = leaflet::layersControlOptions(
        collapsed = FALSE,
        position = "topright"
      )
    )
  
  if (verbose) {
    total_overlay_groups <- length(overlay_groups_vector)
    overlay_group_names <- paste(names(overlay_groups_list), collapse = ", ")
    logger::log_info(paste0("Added native layer controls with ", total_overlay_groups, " overlay groups"))
    logger::log_info(paste0("Overlay groups created: ", overlay_group_names))
    logger::log_info("Layer controls positioned at top-right of map")
  }
  
  return(map_with_native_controls)
}

# Run the same command again with the fixed function
enhanced_physician_map_with_layer_controls(
  physician_geodata = join_physician_data_with_sf_output,  
  include_county_boundaries = TRUE,
  include_female_demographics = TRUE,
  acs_years = 2013:2022,
  default_year = 2022,
  verbose = TRUE
)


# Function at 0803 with mapview ----
library(mapview)
#' Enhanced Physician Map Using mapview with Year Controls
#' 
#' Creates an interactive physician map using mapview's native layer controls
#' for year selection. This approach leverages mapview's built-in functionality
#' for handling multiple datasets and provides reliable layer switching that
#' works consistently in RStudio viewer.
#' 
#' @param physician_geodata Data frame with latitude/longitude columns and 
#'   physician information. Must contain 'latitude' and 'longitude' columns.
#'   If 'data_year' column exists, physicians will be separated by year.
#' @param include_county_boundaries Logical indicating whether to include 
#'   counties with basic information (default: TRUE)
#' @param acs_years Numeric vector of years to include in analysis 
#'   (default: 2022, valid range: 2012-2023)
#' @param default_basemap Character string specifying the default basemap
#'   (default: "CartoDB.Positron")
#' @param verbose Logical indicating whether to enable detailed console logging 
#'   (default: TRUE)
#' 
#' @return Interactive mapview object with native layer controls for 
#'   year selection and physician data
#' 
#' @examples
#' # Example 1: Single year map with counties
#' physician_test_data <- data.frame(
#'   latitude = c(39.7392, 39.8561, 39.6403),
#'   longitude = c(-104.9903, -105.0178, -105.0178),
#'   pfname = c("John", "Jane", "Bob"),
#'   plname = c("Smith", "Doe", "Johnson"),
#'   npi = c("1234567890", "0987654321", "1122334455"),
#'   pmailcityname = c("Denver", "Boulder", "Lakewood"),
#'   pmailstatename = c("Colorado", "Colorado", "Colorado"),
#'   data_year = c(2022, 2022, 2022)
#' )
#' enhanced_physician_map_mapview(
#'   physician_geodata = physician_test_data,
#'   include_county_boundaries = TRUE,
#'   acs_years = 2022,
#'   default_basemap = "CartoDB.Positron",
#'   verbose = TRUE
#' )
#' 
#' # Example 2: Multi-year map with native layer controls (2020-2022)
#' physician_multiyear_data <- data.frame(
#'   latitude = rep(c(39.7392, 39.8561, 39.6403), 3),
#'   longitude = rep(c(-104.9903, -105.0178, -105.0178), 3),
#'   pfname = rep(c("John", "Jane", "Bob"), 3),
#'   plname = rep(c("Smith", "Doe", "Johnson"), 3),
#'   npi = rep(c("1234567890", "0987654321", "1122334455"), 3),
#'   pmailcityname = rep(c("Denver", "Boulder", "Lakewood"), 3),
#'   pmailstatename = rep(c("Colorado", "Colorado", "Colorado"), 3),
#'   data_year = c(rep(2020, 3), rep(2021, 3), rep(2022, 3))
#' )
#' enhanced_physician_map_mapview(
#'   physician_geodata = physician_multiyear_data,
#'   include_county_boundaries = TRUE,
#'   acs_years = c(2020, 2021, 2022),
#'   default_basemap = "OpenStreetMap",
#'   verbose = TRUE
#' )
#' 
#' # Example 3: Decade analysis with mapview controls (2018-2022)
#' physician_decade_data <- data.frame(
#'   latitude = rep(c(39.7392, 39.8561, 39.6403, 39.5501), 5),
#'   longitude = rep(c(-104.9903, -105.0178, -105.0178, -105.7821), 5),
#'   pfname = rep(c("John", "Jane", "Bob", "Alice"), 5),
#'   plname = rep(c("Smith", "Doe", "Johnson", "Wilson"), 5),
#'   npi = rep(c("1234567890", "0987654321", "1122334455", "9988776655"), 5),
#'   pmailcityname = rep(c("Denver", "Boulder", "Lakewood", "Arvada"), 5),
#'   pmailstatename = rep(c("Colorado", "Colorado", "Colorado", "Colorado"), 5),
#'   pspec = rep(c("Cardiology", "Pediatrics", "Internal Medicine", "Surgery"), 5),
#'   data_year = c(rep(2018, 4), rep(2019, 4), rep(2020, 4), 
#'                 rep(2021, 4), rep(2022, 4))
#' )
#' enhanced_physician_map_mapview(
#'   physician_geodata = physician_decade_data,
#'   include_county_boundaries = TRUE,
#'   acs_years = 2018:2022,
#'   default_basemap = "Esri.WorldImagery",
#'   verbose = TRUE
#' )
#' 
#' @importFrom assertthat assert_that
#' @importFrom logger log_info log_warn log_error
#' @importFrom dplyr mutate case_when filter select
#' @importFrom sf st_as_sf st_coordinates st_drop_geometry
#' @importFrom mapview mapview
#' @importFrom purrr map
#' @export
enhanced_physician_map_mapview <- function(physician_geodata,
                                           include_county_boundaries = TRUE,
                                           acs_years = 2022,
                                           default_basemap = "CartoDB.Positron",
                                           verbose = TRUE) {
  
  # Input validation and logging
  if (verbose) {
    logger::log_info("Starting enhanced physician map creation using mapview")
    
    physician_input_rows <- nrow(physician_geodata)
    physician_input_cols <- ncol(physician_geodata)
    acs_years_requested <- paste(acs_years, collapse = ", ")
    
    logger::log_info(paste0("Input physician geodata dimensions: ", physician_input_rows, " rows, ", physician_input_cols, " columns"))
    logger::log_info(paste0("ACS years requested: ", acs_years_requested))
    logger::log_info(paste0("Include county boundaries: ", include_county_boundaries))
    logger::log_info(paste0("Default basemap: ", default_basemap))
  }
  
  # Validate inputs using assertthat
  assertthat::assert_that(
    is.data.frame(physician_geodata),
    msg = "physician_geodata must be a data frame"
  )
  
  assertthat::assert_that(
    is.logical(include_county_boundaries),
    msg = "include_county_boundaries must be TRUE or FALSE"
  )
  
  assertthat::assert_that(
    is.numeric(acs_years) && all(acs_years >= 2012 & acs_years <= 2023),
    msg = "acs_years must be numeric values between 2012 and 2023"
  )
  
  assertthat::assert_that(
    is.character(default_basemap),
    msg = "default_basemap must be a character string"
  )
  
  assertthat::assert_that(
    is.logical(verbose),
    msg = "verbose must be TRUE or FALSE"
  )
  
  # Validate that physician data contains required columns
  required_coordinate_columns <- c("latitude", "longitude")
  missing_coordinate_columns <- setdiff(required_coordinate_columns, names(physician_geodata))
  
  assertthat::assert_that(
    length(missing_coordinate_columns) == 0,
    msg = paste0("physician_geodata missing required columns: ", paste(missing_coordinate_columns, collapse = ", "))
  )
  
  if (verbose) {
    if (!"data_year" %in% names(physician_geodata)) {
      logger::log_warn("physician_geodata missing 'data_year' column - all physicians will be shown together")
    }
    logger::log_info("Input validation completed successfully")
  }
  
  # Process physician data for mapview
  processed_physician_data <- prepare_physician_data_for_mapview(
    input_geodata = physician_geodata,
    acs_years = acs_years,
    verbose = verbose
  )
  
  # Create county boundaries if requested
  county_boundaries_data <- NULL
  if (include_county_boundaries) {
    county_boundaries_data <- prepare_county_boundaries_for_mapview(
      physician_data = processed_physician_data,
      verbose = verbose
    )
  }
  
  # Create the mapview visualization
  final_mapview_object <- create_mapview_with_year_layers(
    physician_data = processed_physician_data,
    county_data = county_boundaries_data,
    acs_years = acs_years,
    default_basemap = default_basemap,
    verbose = verbose
  )
  
  if (verbose) {
    mapview_completion_message <- "Enhanced physician map with mapview created successfully"
    logger::log_info(mapview_completion_message)
    logger::log_info("Use the layer control panel to toggle between years and data types")
  }
  
  return(final_mapview_object)
}

#' Prepare Physician Data for mapview
#' @noRd
prepare_physician_data_for_mapview <- function(input_geodata, acs_years, verbose) {
  
  if (verbose) {
    logger::log_info("Preparing physician data for mapview visualization")
  }
  
  # Clean and filter data
  clean_physician_data <- input_geodata %>%
    dplyr::filter(!is.na(latitude) & !is.na(longitude))
  
  if (verbose) {
    clean_coordinate_rows <- nrow(clean_physician_data)
    total_input_rows <- nrow(input_geodata)
    removed_invalid_rows <- total_input_rows - clean_coordinate_rows
    
    logger::log_info(paste0("Clean physician records: ", clean_coordinate_rows, " rows"))
    if (removed_invalid_rows > 0) {
      logger::log_warn(paste0("Removed ", removed_invalid_rows, " rows with missing coordinates"))
    }
  }
  
  # Add year labels and enhanced information
  if ("data_year" %in% names(clean_physician_data)) {
    # Filter to requested years only
    clean_physician_data <- clean_physician_data %>%
      dplyr::filter(data_year %in% acs_years)
    
    # Create year labels for mapview
    clean_physician_data <- clean_physician_data %>%
      dplyr::mutate(
        year_label = paste0("Year ", data_year),
        physician_display_name = dplyr::case_when(
          "pfname" %in% names(.) & "plname" %in% names(.) ~ 
            paste0("Dr. ", pfname, " ", plname),
          TRUE ~ "Physician"
        ),
        mapview_popup_info = create_physician_popup_text(., data_year)
      )
  } else {
    # No year column - treat as single year
    clean_physician_data <- clean_physician_data %>%
      dplyr::mutate(
        year_label = paste0("Year ", acs_years[1]),
        data_year = acs_years[1],
        physician_display_name = dplyr::case_when(
          "pfname" %in% names(.) & "plname" %in% names(.) ~ 
            paste0("Dr. ", pfname, " ", plname),
          TRUE ~ "Physician"
        ),
        mapview_popup_info = create_physician_popup_text(., acs_years[1])
      )
  }
  
  # Convert to sf for mapview
  spatial_physician_data <- sf::st_as_sf(
    clean_physician_data,
    coords = c("longitude", "latitude"),
    crs = 4326
  )
  
  # Add back coordinate columns for reference
  coordinate_matrix <- sf::st_coordinates(spatial_physician_data)
  spatial_physician_data$longitude <- coordinate_matrix[, "X"]
  spatial_physician_data$latitude <- coordinate_matrix[, "Y"]
  
  if (verbose) {
    final_physician_count <- nrow(spatial_physician_data)
    available_years <- unique(spatial_physician_data$data_year)
    logger::log_info(paste0("Prepared ", final_physician_count, " physicians for years: ", paste(available_years, collapse = ", ")))
  }
  
  return(spatial_physician_data)
}

#' Create Physician Popup Text
#' @noRd
create_physician_popup_text <- function(physician_row_data, analysis_year) {
  
  base_info <- paste0("Physician (", analysis_year, ")")
  
  if ("pfname" %in% names(physician_row_data) && "plname" %in% names(physician_row_data)) {
    physician_name <- paste0("Dr. ", physician_row_data$pfname, " ", physician_row_data$plname)
    
    additional_info <- c()
    
    if ("npi" %in% names(physician_row_data)) {
      additional_info <- c(additional_info, paste0("NPI: ", physician_row_data$npi))
    }
    
    if ("pmailcityname" %in% names(physician_row_data) && "pmailstatename" %in% names(physician_row_data)) {
      additional_info <- c(additional_info, paste0("Location: ", physician_row_data$pmailcityname, ", ", physician_row_data$pmailstatename))
    }
    
    if ("pspec" %in% names(physician_row_data)) {
      additional_info <- c(additional_info, paste0("Specialty: ", physician_row_data$pspec))
    }
    
    popup_text <- paste0(physician_name, " (", analysis_year, ")")
    if (length(additional_info) > 0) {
      popup_text <- paste0(popup_text, "<br>", paste(additional_info, collapse = "<br>"))
    }
    
    return(popup_text)
  } else {
    return(base_info)
  }
}

#' Prepare County Boundaries for mapview
#' @noRd
prepare_county_boundaries_for_mapview <- function(physician_data, verbose) {
  
  if (verbose) {
    logger::log_info("Preparing county boundaries for mapview")
  }
  
  # Check if tigris is available
  if (!requireNamespace("tigris", quietly = TRUE)) {
    if (verbose) {
      logger::log_warn("tigris package not available - skipping county boundaries")
    }
    return(NULL)
  }
  
  # Extract primary state from physician data
  primary_state_name <- extract_primary_state_for_mapview(physician_data, verbose)
  
  if (is.null(primary_state_name)) {
    if (verbose) {
      logger::log_warn("Cannot determine primary state - skipping county boundaries")
    }
    return(NULL)
  }
  
  # Download county boundaries
  tryCatch({
    county_boundaries_sf <- tigris::counties(state = primary_state_name, cb = TRUE)
    county_boundaries_sf <- sf::st_transform(county_boundaries_sf, crs = 4326)
    
    # Add display information
    county_boundaries_sf <- county_boundaries_sf %>%
      dplyr::mutate(
        county_display_name = paste0(NAME, " County, ", STUSPS),
        county_popup_info = paste0(NAME, " County<br>State: ", STUSPS, "<br>FIPS: ", GEOID)
      )
    
    if (verbose) {
      county_count <- nrow(county_boundaries_sf)
      logger::log_info(paste0("Successfully prepared ", county_count, " county boundaries"))
    }
    
    return(county_boundaries_sf)
    
  }, error = function(error_details) {
    if (verbose) {
      logger::log_error(paste0("Failed to download counties: ", error_details$message))
    }
    return(NULL)
  })
}

#' Extract Primary State for mapview
#' @noRd
extract_primary_state_for_mapview <- function(physician_data, verbose) {
  
  state_column_candidates <- c("pmailstatename", "plocstatename", "state")
  
  physician_attributes_df <- sf::st_drop_geometry(physician_data)
  
  state_column_found <- NULL
  for (column_candidate in state_column_candidates) {
    if (column_candidate %in% names(physician_attributes_df)) {
      state_column_found <- column_candidate
      break
    }
  }
  
  if (!is.null(state_column_found)) {
    state_values <- physician_attributes_df[[state_column_found]]
    state_values <- state_values[!is.na(state_values) & state_values != ""]
    
    if (length(state_values) > 0) {
      primary_state_name <- names(sort(table(state_values), decreasing = TRUE))[1]
      
      if (verbose) {
        logger::log_info(paste0("Primary state identified: ", primary_state_name))
      }
      
      return(primary_state_name)
    }
  }
  
  return(NULL)
}

#' Create mapview with Year Layers
#' @noRd
create_mapview_with_year_layers <- function(physician_data, county_data, acs_years, default_basemap, verbose) {
  
  if (verbose) {
    physician_years_available <- unique(physician_data$data_year)
    logger::log_info(paste0("Creating mapview with physician data for years: ", paste(physician_years_available, collapse = ", ")))
  }
  
  # Check if mapview is available
  if (!requireNamespace("mapview", quietly = TRUE)) {
    stop("mapview package is required. Install with: install.packages('mapview')")
  }
  
  # Set mapview options
  mapview::mapviewOptions(
    basemaps = c(default_basemap, "OpenStreetMap", "Esri.WorldImagery"),
    layers.control.pos = "topright",
    homebutton = TRUE,
    verbose = verbose
  )
  
  # Create physician maps by year
  if (length(acs_years) > 1 && "data_year" %in% names(physician_data)) {
    # Multiple years - create separate layers
    physician_map_list <- list()
    
    for (analysis_year in acs_years) {
      year_physician_subset <- physician_data %>%
        dplyr::filter(data_year == analysis_year)
      
      if (nrow(year_physician_subset) > 0) {
        year_mapview <- mapview::mapview(
          year_physician_subset,
          layer.name = paste0("Physicians ", analysis_year),
          col.regions = "#E91E63",
          alpha.regions = 0.8,
          cex = 7,
          popup = leafpop::popupTable(year_physician_subset, zcol = c("physician_display_name", "data_year")),
          homebutton = FALSE
        )
        
        physician_map_list[[paste0("physicians_", analysis_year)]] <- year_mapview
        
        if (verbose) {
          year_physician_count <- nrow(year_physician_subset)
          logger::log_info(paste0("Created mapview layer for year ", analysis_year, " with ", year_physician_count, " physicians"))
        }
      }
    }
    
    # Combine physician maps
    if (length(physician_map_list) > 0) {
      combined_physician_map <- physician_map_list[[1]]
      if (length(physician_map_list) > 1) {
        for (i in 2:length(physician_map_list)) {
          combined_physician_map <- combined_physician_map + physician_map_list[[i]]
        }
      }
    } else {
      stop("No physician data found for any of the requested years")
    }
    
  } else {
    # Single year or no year column - create single layer
    combined_physician_map <- mapview::mapview(
      physician_data,
      layer.name = paste0("Physicians ", acs_years[1]),
      col.regions = "#E91E63",
      alpha.regions = 0.8,
      cex = 7,
      popup = leafpop::popupTable(physician_data, zcol = c("physician_display_name", "data_year")),
      homebutton = TRUE
    )
    
    if (verbose) {
      single_year_physician_count <- nrow(physician_data)
      logger::log_info(paste0("Created single mapview layer with ", single_year_physician_count, " physicians"))
    }
  }
  
  # Add county boundaries if available
  if (!is.null(county_data)) {
    county_mapview <- mapview::mapview(
      county_data,
      layer.name = "County Boundaries",
      color = "#888888",
      col.regions = "#87CEEB",
      alpha = 0.3,
      alpha.regions = 0.1,
      popup = leafpop::popupTable(county_data, zcol = c("county_display_name", "GEOID")),
      homebutton = FALSE
    )
    
    final_mapview <- combined_physician_map + county_mapview
    
    if (verbose) {
      county_count <- nrow(county_data)
      logger::log_info(paste0("Added county boundaries layer with ", county_count, " counties"))
    }
  } else {
    final_mapview <- combined_physician_map
  }
  
  if (verbose) {
    logger::log_info("Final mapview object created successfully")
  }
  
  return(final_mapview)
}

# Try this new approach:
enhanced_physician_map_mapview(
  physician_geodata = join_physician_data_with_sf_output,  
  include_county_boundaries = TRUE,
  acs_years = 2013:2022,
  default_basemap = "CartoDB.Positron",
  verbose = TRUE
)

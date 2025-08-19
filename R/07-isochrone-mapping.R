
# Setup ----
source("~/Dropbox (Personal)/isochrones/R/01-setup.R")


# CONSTANTS - Configuration variables for reproducibility and maintainability
POSTAL_CODE_LENGTH <- 5
COORDINATE_REFERENCE_SYSTEM <- 4326  # WGS84 EPSG code
GEOMETRY_TOLERANCE_METERS <- 1000
POINT_SIZE_MAP <- 2
POINT_ALPHA_MAP <- 0.2
POLYGON_ALPHA_MAP <- 0.1
POLYGON_WEIGHT_MAP <- 0.5
POLYGON_SMOOTH_FACTOR <- 0.5
POLYGON_OPACITY_MAP <- 0.2
CIRCLE_MARKER_RADIUS <- 2
CIRCLE_MARKER_ALPHA <- 0.1
LEGEND_OPACITY <- 0.3
DEFAULT_MAP_LONGITUDE <- -105
DEFAULT_MAP_LATITUDE <- 39
DEFAULT_MAP_ZOOM <- 6
PROBLEMATIC_FEATURE_ROW <- 188562

# Base directory for isochrones project
ISOCHRONES_PROJECT_BASE <- "~/Dropbox (Personal)/isochrones"

# File path constants - all relative to isochrones project directory
INPUT_CLINICIAN_DATA_PATH <- file.path(ISOCHRONES_PROJECT_BASE, "data/04-geocode/end_completed_clinician_data_geocoded_addresses_12_8_2023.csv")
INPUT_ISOCHRONES_SHAPEFILE_PATH <- file.path(ISOCHRONES_PROJECT_BASE, "data/06-isochrones/end_isochrones_sf_clipped/isochrones.shp")
OUTPUT_RESULT_CSV_PATH <- file.path(ISOCHRONES_PROJECT_BASE, "data/result.csv")
OUTPUT_SHAPEFILE_DIRECTORY <- file.path(ISOCHRONES_PROJECT_BASE, "data/07-isochrone-mapping")
OUTPUT_SHAPEFILE_LAYER_NAME <- "results_simplified_validated_isochrones"
OUTPUT_FIGURES_DIRECTORY <- file.path(ISOCHRONES_PROJECT_BASE, "figures")

# Expand the tilde to full path for better compatibility
ISOCHRONES_PROJECT_BASE <- path.expand(ISOCHRONES_PROJECT_BASE)
INPUT_CLINICIAN_DATA_PATH <- path.expand(INPUT_CLINICIAN_DATA_PATH)
INPUT_ISOCHRONES_SHAPEFILE_PATH <- path.expand(INPUT_ISOCHRONES_SHAPEFILE_PATH)
OUTPUT_RESULT_CSV_PATH <- path.expand(OUTPUT_RESULT_CSV_PATH)
OUTPUT_SHAPEFILE_DIRECTORY <- path.expand(OUTPUT_SHAPEFILE_DIRECTORY)
OUTPUT_FIGURES_DIRECTORY <- path.expand(OUTPUT_FIGURES_DIRECTORY)

# Map styling constants
MAP_PROVIDER_TILES <- list(
  greyscale = "CartoDB.Positron",
  thunderforest = "CartoDB.Positron",  # Fixed: removed providers$ reference
  esri = "Esri.WorldStreetMap"
)

NORTH_ARROW_SVG <- "<svg width='30' height='30' viewBox='0 0 30 30'><polygon points='15,2 18,12 15,10 12,12' fill='black'/><text x='15' y='25' text-anchor='middle' font-size='10'>N</text></svg>"

# Color and styling constants
POINT_COLOR_HEX <- "#1f77b4"
STROKE_COLOR <- "black"
VIRIDIS_OPTION <- "magma"

# Pattern matching constants
POINT_PATTERN_START <- "^POINT \\("
POINT_PATTERN_END <- "\\)$"
COORDINATE_SEPARATOR <- "\\s+"

# Data filtering constants
VALID_COUNTRIES <- c("United States", "Puerto Rico")

# Log the file paths being used for verification
logger::log_info("📂 Using isochrones project directory: {ISOCHRONES_PROJECT_BASE}")
logger::log_info("📁 Input paths:")
logger::log_info("   📄 Clinician data: {INPUT_CLINICIAN_DATA_PATH}")
logger::log_info("   🗺️ Isochrones shapefile: {INPUT_ISOCHRONES_SHAPEFILE_PATH}")
logger::log_info("📁 Output paths:")
logger::log_info("   📄 Results CSV: {OUTPUT_RESULT_CSV_PATH}")
logger::log_info("   🗂️ Shapefile directory: {OUTPUT_SHAPEFILE_DIRECTORY}")
logger::log_info("   📊 Figures directory: {OUTPUT_FIGURES_DIRECTORY}")

# Verify that input files exist
if (!file.exists(INPUT_CLINICIAN_DATA_PATH)) {
  logger::log_error("❌ Clinician data file not found: {INPUT_CLINICIAN_DATA_PATH}")
  stop("Required input file missing: clinician data")
}

if (!file.exists(INPUT_ISOCHRONES_SHAPEFILE_PATH)) {
  logger::log_error("❌ Isochrones shapefile not found: {INPUT_ISOCHRONES_SHAPEFILE_PATH}")
  stop("Required input file missing: isochrones shapefile")
}

logger::log_info("✅ All required input files found")

# ACOG Districts data
ACOG_Districts <- tyler::ACOG_Districts

# Load and process subspecialists location data with constants and enhanced logging
logger::log_info("📂 Loading subspecialists location data from: {INPUT_CLINICIAN_DATA_PATH}")

subspecialists_lat_long <- readr::read_csv(INPUT_CLINICIAN_DATA_PATH, show_col_types = FALSE) %>%
  # Add row identifier for tracking
  dplyr::mutate(row_id = dplyr::row_number()) %>%
  
  # Standardize postal code length
  dplyr::mutate(postal_code = stringr::str_sub(postal_code, 1, POSTAL_CODE_LENGTH)) %>%
  
  # Clean coordinate data using pattern constants
  dplyr::mutate(
    access_clean = exploratory::str_remove(
      access, 
      regex(POINT_PATTERN_START, ignore_case = TRUE), 
      remove_extra_space = TRUE
    )
  ) %>%
  dplyr::mutate(
    access_clean = exploratory::str_remove(
      access_clean, 
      regex(POINT_PATTERN_END, ignore_case = TRUE), 
      remove_extra_space = TRUE
    )
  ) %>%
  
  # Extract coordinates
  tidyr::separate(
    access_clean, 
    into = c("long", "lat"), 
    sep = COORDINATE_SEPARATOR, 
    convert = TRUE,
    remove = FALSE  # Keep original for debugging
  ) %>%
  
  # Join with ACOG Districts data
  dplyr::left_join(
    ACOG_Districts, 
    by = dplyr::join_by(state_code == State_Abbreviations),
    suffix = c("", "_acog")
  ) %>%
  
  # Clean up unnecessary columns
  dplyr::select(-row_id, -rank, -type, -district, -state, -access_clean) %>%
  
  # Filter to valid countries using constant
  dplyr::filter(country %in% VALID_COUNTRIES) %>%
  
  # Standardize postal code again and rename
  dplyr::mutate(postal_code = stringr::str_sub(postal_code, 1, POSTAL_CODE_LENGTH)) %>%
  dplyr::rename(zip = postal_code) %>%
  
  # Ensure coordinates are numeric
  dplyr::mutate(dplyr::across(c(lat, long), readr::parse_number)) %>%
  
  # Filter out invalid coordinates
  dplyr::filter(!is.na(lat), !is.na(long)) %>%
  
  # Validate coordinate ranges (basic sanity check)
  dplyr::filter(
    lat >= -90 & lat <= 90,    # Valid latitude range
    long >= -180 & long <= 180  # Valid longitude range
  ) %>%
  
  # Convert ACOG District to factor for consistent plotting
  dplyr::mutate(ACOG_District = as.factor(ACOG_District)) %>%
  
  # Remove duplicate addresses (keep first occurrence)
  dplyr::distinct(address, .keep_all = TRUE) %>%
  
  # Sort for reproducible results
  dplyr::arrange(state_code, city, address)

# Log processing results
initial_records <- nrow(readr::read_csv(INPUT_CLINICIAN_DATA_PATH, show_col_types = FALSE))
final_records <- nrow(subspecialists_lat_long)
records_removed <- initial_records - final_records

logger::log_info("📊 Data processing completed:")
logger::log_info("   📥 Initial records: {formatC(initial_records, big.mark = ',', format = 'd')}")
logger::log_info("   📤 Final records: {formatC(final_records, big.mark = ',', format = 'd')}")
logger::log_info("   🗑️ Records removed: {formatC(records_removed, big.mark = ',', format = 'd')} ({round(records_removed/initial_records*100, 1)}%)")

# Data quality checks
missing_coordinates <- sum(is.na(subspecialists_lat_long$lat) | is.na(subspecialists_lat_long$long))
unique_states <- length(unique(subspecialists_lat_long$state_code))
unique_districts <- length(unique(subspecialists_lat_long$ACOG_District))

logger::log_info("📊 Data quality summary:")
logger::log_info("   🗺️ Records with valid coordinates: {formatC(final_records - missing_coordinates, big.mark = ',', format = 'd')}")
logger::log_info("   🏛️ States/territories represented: {unique_states}")
logger::log_info("   🏛️ ACOG Districts represented: {unique_districts}")
logger::log_info("   🌍 Countries: {paste(unique(subspecialists_lat_long$country), collapse = ', ')}")

# Coordinate range summary
if (nrow(subspecialists_lat_long) > 0) {
  lat_range <- range(subspecialists_lat_long$lat, na.rm = TRUE)
  long_range <- range(subspecialists_lat_long$long, na.rm = TRUE)
  
  logger::log_info("📍 Geographic coverage:")
  logger::log_info("   📐 Latitude range: {round(lat_range[1], 2)}° to {round(lat_range[2], 2)}°")
  logger::log_info("   📐 Longitude range: {round(long_range[1], 2)}° to {round(long_range[2], 2)}°")
}

logger::log_info("📊 Processed subspecialists data: {formatC(nrow(subspecialists_lat_long), big.mark = ',', format = 'd')} records")
logger::log_info("🗺️ Unique ACOG Districts: {length(unique(subspecialists_lat_long$ACOG_District))}")

#**********************************************
# SANITY CHECK ON THE POINTS
#**********************************************
# Create maps data for visualization
states_map_data <- ggplot2::map_data("state")
usa_map_data <- ggplot2::map_data("usa")

# Convert ACOG_District to factor for consistent plotting
subspecialists_lat_long$ACOG_District <- as.factor(subspecialists_lat_long$ACOG_District)

# Create enhanced subspecialist visualization with clean territory representation
logger::log_info("🗺️ Creating enhanced subspecialists location visualization...")

# Get all US states, territories, and international boundaries
states_map_data <- ggplot2::map_data("state")
usa_map_data <- ggplot2::map_data("usa")
world_map_data <- ggplot2::map_data("world", region = c("USA", "Puerto Rico"))

subspecialists_plot <- ggplot2::ggplot() +
  # Add world boundaries for context (includes Alaska, Hawaii outlines naturally)
  ggplot2::geom_polygon(
    data = world_map_data,
    ggplot2::aes(x = long, y = lat, group = group),
    color = STROKE_COLOR,
    fill = "white",
    linewidth = 0.5
  ) +
  # Add USA mainland outline
  ggplot2::geom_polygon(
    data = usa_map_data,
    ggplot2::aes(x = long, y = lat, group = group),
    color = STROKE_COLOR,
    fill = "white",
    linewidth = 0.8
  ) +
  # Add state boundaries
  ggplot2::geom_polygon(
    data = states_map_data,
    ggplot2::aes(x = long, y = lat, group = group),
    color = "grey60",
    fill = NA,
    linewidth = 0.3
  ) +
  # Add subspecialist points
  ggplot2::geom_point(
    data = subspecialists_lat_long,
    ggplot2::aes(x = long, y = lat, color = ACOG_District),
    size = 1.5,
    alpha = 0.7,
    stroke = 0
  ) +
  ggplot2::labs(
    title = "Gynecologic Oncology Subspecialists by ACOG District", 
    subtitle = paste("Geographic Distribution of", formatC(nrow(subspecialists_lat_long), big.mark = ',', format = 'd'), "Subspecialists across US States and Territories"),
    x = "Longitude", 
    y = "Latitude",
    color = "ACOG District",
    caption = "Includes Alaska, Hawaii, Puerto Rico | Data processed: 2025-08-12"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.title = ggplot2::element_text(hjust = 0.5, size = 16, face = "bold"),
    plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12, color = "grey40"),
    axis.title = ggplot2::element_text(size = 12),
    axis.text = ggplot2::element_text(size = 10),
    legend.title = ggplot2::element_text(size = 11, face = "bold"),
    legend.text = ggplot2::element_text(size = 9),
    panel.grid.major = ggplot2::element_line(color = "grey90", linewidth = 0.3),
    panel.grid.minor = ggplot2::element_blank(),
    plot.caption = ggplot2::element_text(size = 8, color = "grey50"),
    legend.position = "bottom"
  ) +
  ggplot2::coord_fixed(ratio = 1.3, xlim = c(-180, -60), ylim = c(15, 75)) +  # Show all territories
  ggplot2::scale_color_viridis_d(option = VIRIDIS_OPTION, name = "ACOG\nDistrict") +
  ggplot2::guides(color = ggplot2::guide_legend(override.aes = list(size = 3, alpha = 1), nrow = 2))

print(subspecialists_plot)

# Convert to spatial points with constants
subspecialists_lat_long_copy <- subspecialists_lat_long %>%
  sf::st_as_sf(coords = c("long", "lat")) %>%
  sf::st_set_crs(COORDINATE_REFERENCE_SYSTEM) %>%
  dplyr::distinct(address, .keep_all = TRUE)

logger::log_info("✅ Created spatial points object: {formatC(nrow(subspecialists_lat_long_copy), big.mark = ',', format = 'd')} unique addresses")

# Load isochrones shapefile with enhanced error handling
logger::log_info("📂 Loading isochrones shapefile from: {INPUT_ISOCHRONES_SHAPEFILE_PATH}")

# Check shapefile components exist
shapefile_components <- c(
  shp = INPUT_ISOCHRONES_SHAPEFILE_PATH,
  shx = gsub("\\.shp$", ".shx", INPUT_ISOCHRONES_SHAPEFILE_PATH),
  dbf = gsub("\\.shp$", ".dbf", INPUT_ISOCHRONES_SHAPEFILE_PATH),
  prj = gsub("\\.shp$", ".prj", INPUT_ISOCHRONES_SHAPEFILE_PATH)
)

missing_components <- shapefile_components[!file.exists(shapefile_components)]
if (length(missing_components) > 0) {
  logger::log_warn("⚠️ Missing shapefile components: {paste(names(missing_components), collapse = ', ')}")
}

# Attempt to load shapefile with error handling
tryCatch({
  end_isochrones_sf_clipped <- sf::st_read(
    dsn = INPUT_ISOCHRONES_SHAPEFILE_PATH,
    quiet = FALSE  # Show warnings/errors for debugging
  ) %>%
    dplyr::arrange(desc(rank))  # IMPORTANT for layering
  
  logger::log_info("📊 Initial load: {formatC(nrow(end_isochrones_sf_clipped), big.mark = ',', format = 'd')} features")
  
  # Check for and fix corrupted geometries
  logger::log_info("🔧 Checking for corrupted geometries...")
  
  # Identify corrupted/invalid geometries
  valid_geoms <- sf::st_is_valid(end_isochrones_sf_clipped)
  corrupted_count <- sum(!valid_geoms, na.rm = TRUE)
  
  if (corrupted_count > 0) {
    logger::log_warn("⚠️ Found {corrupted_count} corrupted geometries - attempting to fix...")
    
    # Try to fix corrupted geometries
    end_isochrones_sf_clipped <- sf::st_make_valid(end_isochrones_sf_clipped)
    
    # Re-check validity
    fixed_valid <- sf::st_is_valid(end_isochrones_sf_clipped)
    still_corrupted <- sum(!fixed_valid, na.rm = TRUE)
    
    if (still_corrupted > 0) {
      logger::log_warn("⚠️ Still have {still_corrupted} invalid geometries after repair attempt")
      # Remove remaining invalid geometries
      end_isochrones_sf_clipped <- end_isochrones_sf_clipped[fixed_valid, ]
      logger::log_info("🗑️ Removed {still_corrupted} invalid features")
    } else {
      logger::log_info("✅ Successfully repaired all corrupted geometries")
    }
  } else {
    logger::log_info("✅ All geometries are valid")
  }
  
}, error = function(e) {
  logger::log_error("❌ Failed to load shapefile: {e$message}")
  
  # Try alternative approach - read directory instead of specific file
  logger::log_info("🔄 Attempting alternative loading method...")
  
  shapefile_dir <- dirname(INPUT_ISOCHRONES_SHAPEFILE_PATH)
  shapefile_name <- tools::file_path_sans_ext(basename(INPUT_ISOCHRONES_SHAPEFILE_PATH))
  
  tryCatch({
    end_isochrones_sf_clipped <- sf::st_read(
      dsn = shapefile_dir,
      layer = shapefile_name,
      quiet = FALSE
    ) %>%
      dplyr::arrange(desc(rank))
    
    logger::log_info("✅ Alternative loading method successful")
    
  }, error = function(e2) {
    logger::log_error("❌ Alternative loading also failed: {e2$message}")
    stop("Cannot load isochrones shapefile with either method")
  })
})

logger::log_info("📊 Loaded isochrones: {formatC(nrow(end_isochrones_sf_clipped), big.mark = ',', format = 'd')} features")

logger::log_info("📊 Loaded isochrones: {formatC(nrow(end_isochrones_sf_clipped), big.mark = ',', format = 'd')} features")

# Additional shapefile diagnostics
if (exists("end_isochrones_sf_clipped") && nrow(end_isochrones_sf_clipped) > 0) {
  # Check coordinate system
  isochrones_crs <- sf::st_crs(end_isochrones_sf_clipped)
  logger::log_info("🗺️ Isochrones CRS: {isochrones_crs$input}")
  
  # Check for empty geometries
  empty_geoms <- sum(sf::st_is_empty(end_isochrones_sf_clipped))
  if (empty_geoms > 0) {
    logger::log_warn("⚠️ Found {empty_geoms} empty geometries")
  }
  
  # Get basic statistics
  if ("range" %in% names(end_isochrones_sf_clipped)) {
    range_summary <- table(end_isochrones_sf_clipped$range)
    logger::log_info("📊 Range distribution: {paste(names(range_summary), '=', range_summary, collapse = ', ')}")
  }
  
  # Get bounding box
  bbox <- sf::st_bbox(end_isochrones_sf_clipped)
  logger::log_info("📏 Bounding box: [{round(bbox[1], 2)}, {round(bbox[2], 2)}] to [{round(bbox[3], 2)}, {round(bbox[4], 2)}]")
}

# Filter out rows with empty geometries and provide detailed diagnostics
logger::log_info("🔍 Analyzing geometry status...")

initial_feature_count <- nrow(end_isochrones_sf_clipped)
empty_geometries_count <- sum(sf::st_is_empty(end_isochrones_sf_clipped))
valid_geometries_count <- initial_feature_count - empty_geometries_count

logger::log_info("📊 Geometry analysis:")
logger::log_info("   📋 Total features: {initial_feature_count}")
logger::log_info("   ❌ Empty geometries: {empty_geometries_count}")
logger::log_info("   ✅ Valid geometries: {valid_geometries_count}")

if (empty_geometries_count > 0) {
  logger::log_warn("⚠️ High number of empty geometries detected ({round(empty_geometries_count/initial_feature_count*100, 1)}%)")
  
  # Show which features have empty geometries
  if (empty_geometries_count < 10) {
    empty_indices <- which(sf::st_is_empty(end_isochrones_sf_clipped))
    logger::log_info("🔍 Empty geometry indices: {paste(empty_indices, collapse = ', ')}")
  }
  
  # Filter out empty geometries
  end_isochrones_sf_clipped <- end_isochrones_sf_clipped[!sf::st_is_empty(end_isochrones_sf_clipped), ]
  logger::log_info("🗑️ Filtered out {empty_geometries_count} empty geometries")
  logger::log_info("📊 Remaining features: {nrow(end_isochrones_sf_clipped)}")
  
  if (nrow(end_isochrones_sf_clipped) == 0) {
    logger::log_error("❌ No valid geometries remaining after filtering!")
    stop("All isochrone geometries are empty - cannot proceed with spatial analysis")
  }
} else {
  logger::log_info("✅ All geometries are non-empty")
}

# Convert range to factor and get summary
end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)

if (nrow(end_isochrones_sf_clipped) > 0) {
  # Show range distribution for valid geometries only
  if ("range" %in% names(end_isochrones_sf_clipped)) {
    range_summary <- table(end_isochrones_sf_clipped$range)
    logger::log_info("📊 Valid geometry range distribution:")
    for (i in seq_along(range_summary)) {
      logger::log_info("   🕒 {names(range_summary)[i]} minutes: {range_summary[i]} features")
    }
  }
  
  # Get updated bounding box for valid geometries only
  if (nrow(end_isochrones_sf_clipped) > 0) {
    bbox_valid <- sf::st_bbox(end_isochrones_sf_clipped)
    logger::log_info("📏 Valid geometries bounding box: [{round(bbox_valid[1], 2)}, {round(bbox_valid[2], 2)}] to [{round(bbox_valid[3], 2)}, {round(bbox_valid[4], 2)}]")
    
    # Calculate total area covered
    if (valid_geometries_count > 0) {
      # Transform to projected CRS for area calculation
      isochrones_projected <- sf::st_transform(end_isochrones_sf_clipped, crs = 3857)  # Web Mercator
      total_area_sq_meters <- sum(sf::st_area(isochrones_projected), na.rm = TRUE)
      total_area_sq_km <- as.numeric(total_area_sq_meters) / 1000000
      logger::log_info("📐 Total coverage area: {round(total_area_sq_km, 1)} square kilometers")
    }
  }
}

# Create enhanced isochrones visualization with empty geometry handling
logger::log_info("🗺️ Creating enhanced isochrones visualization...")

if (nrow(end_isochrones_sf_clipped) > 0) {
  # Get range levels and create better labels
  range_levels <- sort(unique(end_isochrones_sf_clipped$range))
  range_labels <- paste(range_levels, "minutes")
  
  logger::log_info("🎨 Creating visualization with {length(range_levels)} range levels: {paste(range_levels, collapse = ', ')}")
  
  isochrones_plot <- ggplot2::ggplot() +
    ggplot2::geom_sf(
      data = end_isochrones_sf_clipped, 
      ggplot2::aes(fill = range),
      color = "white",
      linewidth = 0.2,
      alpha = 0.8
    ) +
    ggplot2::scale_fill_viridis_d(
      option = VIRIDIS_OPTION,
      name = "Drive Time\n(minutes)",
      labels = range_labels,
      drop = FALSE  # Keep all factor levels even if not present
    ) +
    ggplot2::labs(
      title = "Healthcare Accessibility Isochrones",
      subtitle = paste("Drive Time Coverage Areas -", length(range_levels), "Time Zones"),
      caption = paste("Valid geometries:", nrow(end_isochrones_sf_clipped), "| Generated:", Sys.Date())
    ) +
    ggplot2::theme_void() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(hjust = 0.5, size = 16, face = "bold"),
      plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12, color = "grey40"),
      plot.caption = ggplot2::element_text(size = 8, color = "grey50"),
      legend.title = ggplot2::element_text(size = 11, face = "bold"),
      legend.text = ggplot2::element_text(size = 9),
      legend.position = "right",
      panel.background = ggplot2::element_rect(fill = "white", color = NA),
      plot.background = ggplot2::element_rect(fill = "white", color = NA)
    ) +
    ggplot2::guides(fill = ggplot2::guide_legend(
      title.position = "top",
      title.hjust = 0.5,
      reverse = TRUE  # Show longest times at top
    ))
  
  print(isochrones_plot)
  
} else {
  logger::log_warn("⚠️ Cannot create isochrones visualization - no valid geometries")
  
  # Create improved placeholder plot with helpful information
  placeholder_plot <- ggplot2::ggplot() +
    ggplot2::geom_rect(
      xmin = -1, xmax = 1, ymin = -1, ymax = 1,
      fill = "lightgrey", color = "darkgrey", alpha = 0.3
    ) +
    ggplot2::annotate("text", x = 0, y = 0.3, 
                      label = "⚠️ NO VALID ISOCHRONE GEOMETRIES", 
                      size = 8, color = "red", fontface = "bold") +
    ggplot2::annotate("text", x = 0, y = 0, 
                      label = paste("Total features loaded:", initial_feature_count), 
                      size = 6, color = "darkred") +
    ggplot2::annotate("text", x = 0, y = -0.2, 
                      label = paste("Empty geometries:", empty_geometries_count), 
                      size = 6, color = "darkred") +
    ggplot2::annotate("text", x = 0, y = -0.4, 
                      label = "💡 Check isochrone generation process", 
                      size = 5, color = "blue") +
    ggplot2::annotate("text", x = 0, y = -0.6, 
                      label = paste("Data source:", basename(INPUT_ISOCHRONES_SHAPEFILE_PATH)), 
                      size = 4, color = "grey40") +
    ggplot2::labs(
      title = "Isochrones Visualization - Data Quality Issue",
      subtitle = "Most isochrone geometries are empty - regeneration may be needed",
      caption = paste("Analysis attempted:", Sys.Date())
    ) +
    ggplot2::theme_void() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(hjust = 0.5, size = 14, face = "bold", color = "darkred"),
      plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 11, color = "red"),
      plot.caption = ggplot2::element_text(size = 8, color = "grey50"),
      panel.background = ggplot2::element_rect(fill = "white", color = "darkgrey", linewidth = 1),
      plot.background = ggplot2::element_rect(fill = "white", color = NA)
    ) +
    ggplot2::xlim(-1.2, 1.2) +
    ggplot2::ylim(-1, 1)
  
  print(placeholder_plot)
}

##########################################################################
# SPATIAL JOIN OPERATIONS WITH CONSTANTS
##########################################################################
logger::log_info("🔗 Performing spatial join operations...")

# Ensure both data frames have the same CRS
sf::st_crs(subspecialists_lat_long_copy) <- sf::st_crs(end_isochrones_sf_clipped)

# Debug spatial join issues
logger::log_info("🔍 Debugging spatial join setup...")

# Check coordinate systems
subspecialists_crs <- sf::st_crs(subspecialists_lat_long_copy)
isochrones_crs <- sf::st_crs(end_isochrones_sf_clipped)

logger::log_info("🗺️ Subspecialists CRS: {subspecialists_crs$input}")
logger::log_info("🗺️ Isochrones CRS: {isochrones_crs$input}")

# Check bounding boxes before spatial join
subspecialists_bbox <- sf::st_bbox(subspecialists_lat_long_copy)
isochrones_bbox <- sf::st_bbox(end_isochrones_sf_clipped)

logger::log_info("📊 Subspecialists bbox: [{round(subspecialists_bbox[1], 2)}, {round(subspecialists_bbox[2], 2)}] to [{round(subspecialists_bbox[3], 2)}, {round(subspecialists_bbox[4], 2)}]")
logger::log_info("📊 Isochrones bbox: [{round(isochrones_bbox[1], 2)}, {round(isochrones_bbox[2], 2)}] to [{round(isochrones_bbox[3], 2)}, {round(isochrones_bbox[4], 2)}]")

# Check for overlap
bbox_overlap <- (subspecialists_bbox[1] <= isochrones_bbox[3]) && 
  (subspecialists_bbox[3] >= isochrones_bbox[1]) &&
  (subspecialists_bbox[2] <= isochrones_bbox[4]) && 
  (subspecialists_bbox[4] >= isochrones_bbox[2])

logger::log_info("🎯 Bounding boxes overlap: {bbox_overlap}")

if (!bbox_overlap) {
  logger::log_warn("⚠️ No bounding box overlap detected - spatial join may fail!")
}

# Ensure both have the same CRS before join
logger::log_info("🛠️ Ensuring consistent coordinate reference systems...")
sf::st_crs(subspecialists_lat_long_copy) <- COORDINATE_REFERENCE_SYSTEM
sf::st_crs(end_isochrones_sf_clipped) <- COORDINATE_REFERENCE_SYSTEM

logger::log_info("🛠️ Making geometries valid and simplifying with tolerance: {GEOMETRY_TOLERANCE_METERS}m...")

# Make geometries valid and simplify with constants
subspecialists_lat_long_copy <- sf::st_make_valid(subspecialists_lat_long_copy)
end_isochrones_sf_clipped <- sf::st_make_valid(end_isochrones_sf_clipped)

# Apply simplification more carefully
subspecialists_lat_long_copy <- sf::st_simplify(
  subspecialists_lat_long_copy, 
  preserveTopology = TRUE, 
  dTolerance = GEOMETRY_TOLERANCE_METERS
)

end_isochrones_sf_clipped <- sf::st_simplify(
  end_isochrones_sf_clipped, 
  preserveTopology = TRUE, 
  dTolerance = GEOMETRY_TOLERANCE_METERS
)

# Final CRS check
subspecialists_lat_long_copy <- sf::st_set_crs(subspecialists_lat_long_copy, COORDINATE_REFERENCE_SYSTEM)
end_isochrones_sf_clipped <- sf::st_set_crs(end_isochrones_sf_clipped, COORDINATE_REFERENCE_SYSTEM)

logger::log_info("🎯 Executing spatial join with enhanced parameters...")

# Perform spatial join with better error handling
tryCatch({
  spatial_join_result <- sf::st_join(
    end_isochrones_sf_clipped, 
    subspecialists_lat_long_copy, 
    join = sf::st_intersects,  # More explicit join type
    left = FALSE, 
    suffix = c("_isochrones", "_subspecialists")
  ) %>%
    dplyr::arrange(desc(rank))  # IMPORTANT for layering
  
  logger::log_info("✅ Spatial join completed successfully")
  
}, error = function(e) {
  logger::log_error("❌ Spatial join failed: {e$message}")
  
  # Create empty result as fallback
  spatial_join_result <- end_isochrones_sf_clipped[0, ]
  logger::log_warn("⚠️ Created empty result as fallback")
})

logger::log_info("✅ Spatial join completed: {formatC(nrow(spatial_join_result), big.mark = ',', format = 'd')} result features")

# Save results with constants
logger::log_info("💾 Saving spatial join results...")

readr::write_csv(spatial_join_result, OUTPUT_RESULT_CSV_PATH)
logger::log_info("📄 Saved result table to: {OUTPUT_RESULT_CSV_PATH}")

# Filter out problematic feature if it exists
filtered_spatial_result <- spatial_join_result %>%
  dplyr::filter(dplyr::row_number() != PROBLEMATIC_FEATURE_ROW)

logger::log_info("🔧 Filtered problematic feature {PROBLEMATIC_FEATURE_ROW}: {formatC(nrow(filtered_spatial_result), big.mark = ',', format = 'd')} features remaining")

# Validate spatial join results before saving
logger::log_info("🔍 Validating spatial join results...")

if (nrow(spatial_join_result) == 0) {
  logger::log_warn("⚠️ Spatial join resulted in 0 features - no intersections found!")
  logger::log_info("💡 This suggests the point and polygon geometries don't overlap")
  logger::log_info("🔧 Check coordinate systems and geographic extents")
} else {
  logger::log_info("✅ Spatial join successful: {formatC(nrow(spatial_join_result), big.mark = ',', format = 'd')} features")
  
  # Check for valid geometries
  valid_geoms <- sum(!sf::st_is_empty(spatial_join_result))
  logger::log_info("📐 Valid geometries: {valid_geoms} of {nrow(spatial_join_result)}")
  
  # Get bounding box info
  bbox_info <- sf::st_bbox(spatial_join_result)
  logger::log_info("🗺️ Bounding box: [{round(bbox_info[1], 2)}, {round(bbox_info[2], 2)}] to [{round(bbox_info[3], 2)}, {round(bbox_info[4], 2)}]")
}

# Save shapefile with better error handling
if (nrow(spatial_join_result) > 0) {
  
  # Create output directory
  if (!dir.exists(OUTPUT_SHAPEFILE_DIRECTORY)) {
    dir.create(OUTPUT_SHAPEFILE_DIRECTORY, recursive = TRUE, showWarnings = FALSE)
  }
  
  # Create full shapefile path (ESRI Shapefile needs .shp extension)
  shapefile_full_path <- file.path(OUTPUT_SHAPEFILE_DIRECTORY, paste0(OUTPUT_SHAPEFILE_LAYER_NAME, ".shp"))
  
  # Remove existing shapefile if it exists to avoid conflicts
  if (file.exists(shapefile_full_path)) {
    file.remove(list.files(
      path = OUTPUT_SHAPEFILE_DIRECTORY, 
      pattern = paste0("^", OUTPUT_SHAPEFILE_LAYER_NAME, "\\.(shp|shx|dbf|prj|cpg)$"), 
      full.names = TRUE
    ))
    logger::log_info("🗑️ Removed existing shapefile components")
  }
  
  tryCatch({
    sf::st_write(
      spatial_join_result,
      dsn = shapefile_full_path,
      driver = "ESRI Shapefile",
      quiet = TRUE,
      append = FALSE
    )
    logger::log_info("🗂️ Saved shapefile to: {shapefile_full_path}")
  }, error = function(e) {
    logger::log_error("❌ Failed to save shapefile: {e$message}")
    logger::log_info("💾 Attempting to save as GeoPackage instead...")
    
    # Fallback to GeoPackage format
    gpkg_path <- file.path(OUTPUT_SHAPEFILE_DIRECTORY, paste0(OUTPUT_SHAPEFILE_LAYER_NAME, ".gpkg"))
    sf::st_write(
      spatial_join_result,
      dsn = gpkg_path,
      driver = "GPKG",
      quiet = TRUE,
      append = FALSE
    )
    logger::log_info("✅ Saved as GeoPackage: {gpkg_path}")
  })
  
  # Create diagnostic plot with error handling
  if (nrow(spatial_join_result) > 0 && !all(sf::st_is_empty(spatial_join_result))) {
    tryCatch({
      # Plot with better error handling
      plot(sf::st_geometry(spatial_join_result), main = "Spatial Join Results", col = "lightblue", border = "darkblue")
      logger::log_info("📊 Created diagnostic plot successfully")
    }, error = function(e) {
      logger::log_warn("⚠️ Could not create diagnostic plot: {e$message}")
      logger::log_info("💡 Try plotting individual components or checking geometry validity")
    })
  } else {
    logger::log_warn("⚠️ Cannot create diagnostic plot - no valid geometries")
  }
  
} else {
  logger::log_warn("⚠️ Skipping shapefile save - no features to save")
}

# Clean up memory
rm(filtered_spatial_result)
invisible(gc())

#*******************************
# INTERACTIVE MAP CREATION WITH CONSTANTS
#*******************************
logger::log_info("🗺️ Creating interactive leaflet map...")

# Prepare color palette and styling
end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)
color_palette <- viridis::magma(length(unique(end_isochrones_sf_clipped$range)))

# Enhanced map bounds for full USA coverage including territories
USA_BOUNDS <- list(
  lng_min = -180,   # Include Alaska
  lng_max = -65,    # Include Puerto Rico  
  lat_min = 15,     # Include Puerto Rico
  lat_max = 72      # Include Alaska
)

# Calculate center point for USA including territories
USA_CENTER_LNG <- mean(c(USA_BOUNDS$lng_min, USA_BOUNDS$lng_max))
USA_CENTER_LAT <- mean(c(USA_BOUNDS$lat_min, USA_BOUNDS$lat_max))

# Create enhanced interactive map with comprehensive data popups
logger::log_info("🗺️ Creating enhanced interactive leaflet map with detailed popups...")

# Prepare enhanced color palette and styling
if (nrow(end_isochrones_sf_clipped) > 0) {
  end_isochrones_sf_clipped$range <- as.factor(end_isochrones_sf_clipped$range)
  color_palette <- viridis::magma(length(unique(end_isochrones_sf_clipped$range)))
} else {
  color_palette <- c("#440154")  # Default purple for empty case
}

# Log the file paths being used for verification
logger::log_info("📂 Using isochrones project directory: {ISOCHRONES_PROJECT_BASE}")
logger::log_info("📁 Input paths:")
logger::log_info("   📄 Clinician data: {INPUT_CLINICIAN_DATA_PATH}")
logger::log_info("   🗺️ Isochrones shapefile: {INPUT_ISOCHRONES_SHAPEFILE_PATH}")
logger::log_info("📁 Output paths:")
logger::log_info("   📄 Results CSV: {OUTPUT_RESULT_CSV_PATH}")
logger::log_info("   🗂️ Shapefile directory: {OUTPUT_SHAPEFILE_DIRECTORY}")
logger::log_info("   📊 Figures directory: {OUTPUT_FIGURES_DIRECTORY}")

# Verify that input files exist
if (!file.exists(INPUT_CLINICIAN_DATA_PATH)) {
  logger::log_error("❌ Clinician data file not found: {INPUT_CLINICIAN_DATA_PATH}")
  stop("Required input file missing: clinician data")
}

if (!file.exists(INPUT_ISOCHRONES_SHAPEFILE_PATH)) {
  logger::log_error("❌ Isochrones shapefile not found: {INPUT_ISOCHRONES_SHAPEFILE_PATH}")
  stop("Required input file missing: isochrones shapefile")
}

logger::log_info("✅ All required input files found")

# ACOG Districts data
ACOG_Districts <- tyler::ACOG_Districts

# Create the enhanced interactive map
isochrone_interactive_map <- leaflet::leaflet() %>%
  # Add multiple tile layer options
  leaflet::addProviderTiles(MAP_PROVIDER_TILES$greyscale, group = "Greyscale") %>%
  leaflet::addProviderTiles(MAP_PROVIDER_TILES$thunderforest, group = "Standard") %>%
  leaflet::addProviderTiles(MAP_PROVIDER_TILES$esri, group = "ESRI World") %>%
  leaflet::addProviderTiles("OpenStreetMap", group = "OpenStreetMap") %>%
  
  # Add scale bar and north arrow
  leaflet::addScaleBar(position = "bottomleft") %>%
  leaflet::addControl(html = NORTH_ARROW_SVG, position = "topright") %>%
  
  # Add isochrones layer (if available)
  {if (nrow(end_isochrones_sf_clipped) > 0) {
    leaflet::addPolygons(
      ., # pipe to continue leaflet chain
      data = end_isochrones_sf_clipped,
      fillColor = ~color_palette[match(range, unique(end_isochrones_sf_clipped$range))],
      fillOpacity = POLYGON_ALPHA_MAP,
      weight = POLYGON_WEIGHT_MAP,
      smoothFactor = POLYGON_SMOOTH_FACTOR,
      stroke = TRUE,
      color = STROKE_COLOR,
      opacity = POLYGON_OPACITY_MAP,
      popup = ~paste0(
        "<div style='font-family: Arial, sans-serif;'>",
        "<h4 style='color: #8e44ad;'>🕒 Healthcare Accessibility Zone</h4>",
        "<p><strong>Drive Time:</strong> ", range, " minutes</p>",
        "<p><strong>Coverage Area:</strong> ", round(as.numeric(sf::st_area(geometry))/1000000, 2), " km²</p>",
        ifelse(!is.na(rank), paste0("<p><strong>Priority Rank:</strong> ", rank, "</p>"), ""),
        "<p><em>Areas within this drive time can access gynecologic oncology care</em></p>",
        "</div>"
      ),
      group = "Isochrones"
    )
  } else {
    . # return unchanged if no isochrones
  }} %>%
  
  # Add subspecialist markers with dataframe-based popups
  leaflet::addCircleMarkers(
    data = subspecialists_lat_long_copy,
    radius = CIRCLE_MARKER_RADIUS * 1.5,  # Slightly larger for visibility
    fill = TRUE,
    fillOpacity = 0.8,
    weight = 1,
    color = "white",
    fillColor = POINT_COLOR_HEX,
    popup = ~paste0(
      "<div style='font-family: Arial, sans-serif; max-width: 280px;'>",
      "<h4 style='margin: 5px 0; color: #2c3e50;'>🏥 Gynecologic Oncology Subspecialist</h4>",
      "<p style='margin: 3px 0;'><strong>📍 Address:</strong> ", address, "</p>",
      "<p style='margin: 3px 0;'><strong>🏙️ City:</strong> ", city, "</p>",
      "<p style='margin: 3px 0;'><strong>🏛️ State:</strong> ", State, "</p>",
      "<p style='margin: 3px 0;'><strong>📮 ZIP:</strong> ", zip, "</p>",
      "<p style='margin: 3px 0;'><strong>🏞️ County:</strong> ", county, "</p>",
      "<p style='margin: 3px 0;'><strong>🌍 Country:</strong> ", country, "</p>",
      "<p style='margin: 3px 0;'><strong>🏛️ ACOG District:</strong> ", ACOG_District, "</p>",
      "<p style='margin: 3px 0;'><strong>🗺️ Subregion:</strong> ", Subregion, "</p>",
      "<p style='margin: 3px 0;'><strong>📊 Geocoding Score:</strong> ", round(score, 2), "</p>",
      "</div>"
    ),
    popupOptions = leaflet::popupOptions(
      maxWidth = 300,
      closeOnClick = TRUE,
      autoClose = TRUE
    ),
    clusterOptions = leaflet::markerClusterOptions(
      showCoverageOnHover = TRUE,
      zoomToBoundsOnClick = TRUE,
      spiderfyOnMaxZoom = TRUE,
      removeOutsideVisibleBounds = TRUE,
      maxClusterRadius = 50
    ),
    group = "Subspecialists"
  ) %>%
  
  # Add layer control
  leaflet::addLayersControl(
    baseGroups = c("Greyscale", "Standard", "ESRI World", "OpenStreetMap"),
    overlayGroups = c("Isochrones", "Subspecialists"),
    options = leaflet::layersControlOptions(collapsed = FALSE)
  ) %>%
  
  # Add legend for isochrones (if available)
  {if (nrow(end_isochrones_sf_clipped) > 0) {
    leaflet::addLegend(
      .,
      data = end_isochrones_sf_clipped,
      position = "bottomright",
      colors = color_palette,
      labels = paste(unique(end_isochrones_sf_clipped$range), "min"),
      title = "Drive Time to Care",
      opacity = LEGEND_OPACITY
    )
  } else {
    leaflet::addControl(
      .,
      html = "<div style='background: white; padding: 10px; border-radius: 5px; border: 2px solid red;'>
                <strong style='color: red;'>⚠️ No Isochrone Data</strong><br/>
                <small>Only subspecialist locations shown</small>
              </div>",
      position = "bottomright"
    )
  }} %>%
  
  # Set view to cover entire USA including territories
  leaflet::setView(
    lng = USA_CENTER_LNG, 
    lat = USA_CENTER_LAT, 
    zoom = 4  # Zoom level 4 shows entire USA including Alaska, Hawaii, Puerto Rico
  ) %>%
  
  # Set maximum bounds to prevent excessive panning
  leaflet::setMaxBounds(
    lng1 = USA_BOUNDS$lng_min, 
    lat1 = USA_BOUNDS$lat_min,
    lng2 = USA_BOUNDS$lng_max, 
    lat2 = USA_BOUNDS$lat_max
  )

print(isochrone_interactive_map)

# Save interactive map with timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

if (!dir.exists(OUTPUT_FIGURES_DIRECTORY)) {
  dir.create(OUTPUT_FIGURES_DIRECTORY, recursive = TRUE, showWarnings = FALSE)
}

html_output_file <- file.path(OUTPUT_FIGURES_DIRECTORY, paste0("isochrone_map_", timestamp, ".html"))
png_output_file <- file.path(OUTPUT_FIGURES_DIRECTORY, paste0("isochrone_map_", timestamp, ".png"))

htmlwidgets::saveWidget(
  widget = isochrone_interactive_map, 
  file = html_output_file,
  selfcontained = TRUE
)

logger::log_info("📄 Leaflet map saved as HTML: {html_output_file}")

# Take screenshot if webshot is available
if (requireNamespace("webshot", quietly = TRUE)) {
  webshot::webshot(html_output_file, file = png_output_file)
  logger::log_info("📸 Screenshot saved as PNG: {png_output_file}")
} else {
  logger::log_warn("📸 webshot package not available - PNG screenshot not created")
}

# Final summary
logger::log_info("🎉 Isochrone mapping analysis completed successfully!")
logger::log_info("📊 Final results summary:")
logger::log_info("   📍 Subspecialists processed: {formatC(nrow(subspecialists_lat_long), big.mark = ',', format = 'd')}")
logger::log_info("   🕒 Isochrones processed: {formatC(nrow(end_isochrones_sf_clipped), big.mark = ',', format = 'd')}")
logger::log_info("   🔗 Spatial join results: {formatC(nrow(spatial_join_result), big.mark = ',', format = 'd')}")
logger::log_info("   💾 Output files created:")
logger::log_info("      • CSV: {OUTPUT_RESULT_CSV_PATH}")
logger::log_info("      • Shapefile: {OUTPUT_SHAPEFILE_DIRECTORY}")
logger::log_info("      • Interactive map: {html_output_file}")
if (file.exists(png_output_file)) {
  logger::log_info("      • Map screenshot: {png_output_file}")
}


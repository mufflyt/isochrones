# Create a simple diverse isochrones map that should work
library(dplyr)
library(sf)
library(leaflet)
library(logger)

log_info("Creating simple diverse map...")

# Load the diverse test isochrones
diverse_isochrones <- sf::st_read("data/04-geocode/output/diverse_physician_isochrones_here_api.gpkg", quiet = TRUE)

log_info("Loaded {nrow(diverse_isochrones)} total isochrones")

# Color palettes
region_pal <- leaflet::colorFactor("Set3", diverse_isochrones$region)
drive_time_colors <- c("30" = "#FFD700", "60" = "#FF8C00", "120" = "#FF4500", "180" = "#DC143C")
drive_time_pal <- leaflet::colorFactor(drive_time_colors, diverse_isochrones$drive_time_minutes)

# Get unique locations for markers
location_markers <- diverse_isochrones %>%
  sf::st_drop_geometry() %>%
  dplyr::distinct(location_id, sample_name, sample_address, region, center_lat, center_lon)

# Create comprehensive map
simple_diverse_map <- leaflet() %>%
  addTiles() %>%
  
  # Add isochrones by drive time (largest first)
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 180),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.2,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.6,
    group = "180 minutes",
    popup = ~paste0("<b>", sample_name, "</b> (", region, ")<br/>180 min: ", round(area_km2, 1), " km²")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 120),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.3,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.7,
    group = "120 minutes",
    popup = ~paste0("<b>", sample_name, "</b> (", region, ")<br/>120 min: ", round(area_km2, 1), " km²")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 60),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.4,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.8,
    group = "60 minutes",
    popup = ~paste0("<b>", sample_name, "</b> (", region, ")<br/>60 min: ", round(area_km2, 1), " km²")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 30),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.5,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.9,
    group = "30 minutes",
    popup = ~paste0("<b>", sample_name, "</b> (", region, ")<br/>30 min: ", round(area_km2, 1), " km²")
  ) %>%
  
  # Add physician markers
  addCircleMarkers(
    data = location_markers,
    lng = ~center_lon,
    lat = ~center_lat,
    radius = 6,
    color = "white",
    fillColor = ~region_pal(region),
    fillOpacity = 1,
    weight = 2,
    group = "Physicians",
    popup = ~paste0("🏥 ", sample_name, " (", region, ")")
  ) %>%
  
  # Add layer controls
  addLayersControl(
    overlayGroups = c("Physicians", "30 minutes", "60 minutes", "120 minutes", "180 minutes"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  
  # Add legends
  addLegend(
    pal = drive_time_pal,
    values = diverse_isochrones$drive_time_minutes,
    title = "Drive Time (min)",
    position = "topright"
  ) %>%
  
  addLegend(
    pal = region_pal,
    values = location_markers$region,
    title = "Region",
    position = "bottomright"
  )

# Display and save
print(simple_diverse_map)
htmlwidgets::saveWidget(simple_diverse_map, "plots/simple_diverse_map.html", selfcontained = TRUE)
log_info("Simple diverse map saved to plots/simple_diverse_map.html")

# Show coordinate ranges to verify diversity
log_info("Coordinate ranges:")
log_info("Longitude: {min(diverse_isochrones$center_lon)} to {max(diverse_isochrones$center_lon)}")
log_info("Latitude: {min(diverse_isochrones$center_lat)} to {max(diverse_isochrones$center_lat)}")
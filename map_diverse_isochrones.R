# Map the geographically diverse test isochrones
library(dplyr)
library(sf)
library(leaflet)
library(RColorBrewer)
library(logger)

log_info("Loading and mapping diverse test isochrones...")

# Load the diverse test isochrones
diverse_isochrones <- sf::st_read("data/04-geocode/output/diverse_physician_isochrones_here_api.gpkg", quiet = TRUE)

log_info("Loaded {nrow(diverse_isochrones)} diverse isochrones")
log_info("Drive times: {paste(sort(unique(diverse_isochrones$drive_time_minutes)), collapse = ', ')} minutes")
log_info("Locations: {length(unique(diverse_isochrones$location_id))}")
log_info("Regions: {paste(sort(unique(diverse_isochrones$region)), collapse = ', ')}")

# Define colors for different drive times
drive_time_colors <- c(
  "30" = "#FFD700",   # Gold - shortest time
  "60" = "#FF8C00",   # Dark orange  
  "120" = "#FF4500",  # Orange red
  "180" = "#DC143C"   # Crimson - longest time
)

# Define colors for regions
region_colors <- c(
  "Northeast" = "#2E86AB",  # Blue
  "Northwest" = "#A23B72",  # Purple  
  "Southeast" = "#F18F01",  # Orange
  "Southwest" = "#C73E1D"   # Red
)

# Create color palette function
drive_time_pal <- leaflet::colorFactor(
  palette = drive_time_colors,
  domain = diverse_isochrones$drive_time_minutes
)

region_pal <- leaflet::colorFactor(
  palette = region_colors,
  domain = diverse_isochrones$region
)

# Get unique locations for markers
location_markers <- diverse_isochrones %>%
  sf::st_drop_geometry() %>%
  dplyr::distinct(location_id, sample_name, sample_address, region, center_lat, center_lon)

# Create the map
diverse_map <- leaflet() %>%
  # Add base map layers
  addProviderTiles(providers$CartoDB.Positron, group = "Light Map") %>%
  addProviderTiles(providers$OpenStreetMap, group = "OpenStreetMap") %>%
  
  # Add isochrones by drive time (in reverse order so shorter times appear on top)
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 180),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.2,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.6,
    group = "180 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b> (", region, ")<br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 120),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.3,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.7,
    group = "120 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b> (", region, ")<br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 60),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.4,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 1,
    opacity = 0.8,
    group = "60 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b> (", region, ")<br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = diverse_isochrones %>% dplyr::filter(drive_time_minutes == 30),
    fillColor = ~drive_time_pal(drive_time_minutes),
    fillOpacity = 0.5,
    color = ~drive_time_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.9,
    group = "30 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b> (", region, ")<br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  # Add physician location markers colored by region
  addCircleMarkers(
    data = location_markers,
    lng = ~center_lon,
    lat = ~center_lat,
    radius = 8,
    color = "white",
    fillColor = ~region_pal(region),
    fillOpacity = 1,
    weight = 2,
    opacity = 1,
    group = "Physician Locations",
    popup = ~paste0(
      "<b>🏥 ", sample_name, "</b><br/>",
      "📍 Region: ", region, "<br/>",
      "📍 Address: ", sample_address
    ),
    label = ~paste0("🏥 ", sample_name, " (", region, ")")
  ) %>%
  
  # Add layer controls
  addLayersControl(
    baseGroups = c("Light Map", "OpenStreetMap"),
    overlayGroups = c("Physician Locations", "30 minutes", "60 minutes", "120 minutes", "180 minutes"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  
  # Add legends
  addLegend(
    pal = drive_time_pal,
    values = diverse_isochrones$drive_time_minutes,
    title = "Drive Time<br/>(minutes)",
    position = "topright"
  ) %>%
  
  addLegend(
    pal = region_pal,
    values = location_markers$region,
    title = "US Region",
    position = "bottomright"
  ) %>%
  
  # Add title
  addControl(
    html = "<div style='background: white; padding: 10px; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2);'>
              <h3 style='margin: 0; color: #333;'>🚗 OB/GYN Physician Drive Time Isochrones</h3>
              <p style='margin: 5px 0 0 0; color: #666; font-size: 14px;'>Geographically Diverse Sample: 12 Locations Across 4 US Regions</p>
            </div>",
    position = "topleft"
  ) %>%
  
  # Set initial view to show all isochrones
  fitBounds(
    lng1 = sf::st_bbox(diverse_isochrones)[1],
    lat1 = sf::st_bbox(diverse_isochrones)[2], 
    lng2 = sf::st_bbox(diverse_isochrones)[3],
    lat2 = sf::st_bbox(diverse_isochrones)[4]
  )

# Display the map
log_info("Displaying diverse isochrone map...")
print(diverse_map)

# Save the map
map_file <- "plots/diverse_isochrones_map.html"
dir.create("plots", showWarnings = FALSE)
htmlwidgets::saveWidget(diverse_map, map_file, selfcontained = TRUE)
log_info("Saved diverse isochrones map to: {map_file}")

# Create summary by region and drive time
log_info("Regional summary statistics:")
regional_summary <- diverse_isochrones %>%
  sf::st_drop_geometry() %>%
  dplyr::group_by(region, drive_time_minutes) %>%
  dplyr::summarise(
    locations = n(),
    avg_area_km2 = round(mean(area_km2), 0),
    min_area_km2 = round(min(area_km2), 0),
    max_area_km2 = round(max(area_km2), 0),
    .groups = "drop"
  ) %>%
  dplyr::arrange(region, drive_time_minutes)

print(regional_summary)

log_info("Diverse isochrone mapping complete!")
log_info("Open {map_file} in your browser to view the interactive map")
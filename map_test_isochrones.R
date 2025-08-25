# Map the test isochrones we just generated
library(dplyr)
library(sf)
library(leaflet)
library(leaflet.extras)
library(RColorBrewer)
library(logger)

log_info("Loading and mapping test isochrones...")

# Load the test isochrones
test_isochrones <- sf::st_read("data/04-geocode/output/physician_isochrones_here_api.gpkg", quiet = TRUE)

log_info("Loaded {nrow(test_isochrones)} test isochrones")
log_info("Drive times: {paste(sort(unique(test_isochrones$drive_time_minutes)), collapse = ', ')} minutes")
log_info("Locations: {length(unique(test_isochrones$location_id))}")

# Define colors for different drive times
drive_time_colors <- c(
  "30" = "#FFD700",   # Gold - shortest time
  "60" = "#FF8C00",   # Dark orange  
  "120" = "#FF4500",  # Orange red
  "180" = "#DC143C"   # Crimson - longest time
)

# Create color palette function
color_pal <- leaflet::colorFactor(
  palette = drive_time_colors,
  domain = test_isochrones$drive_time_minutes
)

# Create the map
isochrone_map <- leaflet() %>%
  # Add base map layers
  addProviderTiles(providers$CartoDB.Positron, group = "Light Map") %>%
  addProviderTiles(providers$OpenStreetMap, group = "OpenStreetMap") %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%
  
  # Add isochrones by drive time (in reverse order so shorter times appear on top)
  addPolygons(
    data = test_isochrones %>% dplyr::filter(drive_time_minutes == 180),
    fillColor = ~color_pal(drive_time_minutes),
    fillOpacity = 0.3,
    color = ~color_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.7,
    group = "180 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b><br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = test_isochrones %>% dplyr::filter(drive_time_minutes == 120),
    fillColor = ~color_pal(drive_time_minutes),
    fillOpacity = 0.4,
    color = ~color_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.8,
    group = "120 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b><br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = test_isochrones %>% dplyr::filter(drive_time_minutes == 60),
    fillColor = ~color_pal(drive_time_minutes),
    fillOpacity = 0.5,
    color = ~color_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.8,
    group = "60 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b><br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  addPolygons(
    data = test_isochrones %>% dplyr::filter(drive_time_minutes == 30),
    fillColor = ~color_pal(drive_time_minutes),
    fillOpacity = 0.6,
    color = ~color_pal(drive_time_minutes),
    weight = 2,
    opacity = 0.9,
    group = "30 minutes",
    popup = ~paste0(
      "<b>", sample_name, "</b><br/>",
      "Drive Time: ", drive_time_minutes, " minutes<br/>",
      "Area: ", round(area_km2, 1), " km²<br/>",
      "Address: ", sample_address
    ),
    label = ~paste0(sample_name, " - ", drive_time_minutes, " min")
  ) %>%
  
  # Add physician location markers
  addCircleMarkers(
    data = test_isochrones %>% 
      dplyr::distinct(location_id, .keep_all = TRUE) %>%
      dplyr::mutate(
        longitude = center_lon,
        latitude = center_lat
      ),
    lng = ~longitude,
    lat = ~latitude,
    radius = 8,
    color = "white",
    fillColor = "red",
    fillOpacity = 1,
    weight = 2,
    opacity = 1,
    group = "Physician Locations",
    popup = ~paste0(
      "<b>🏥 ", sample_name, "</b><br/>",
      "📍 ", sample_address, "<br/>",
      "📊 Coverage Areas:<br/>",
      "• 30 min: ", round(area_km2[drive_time_minutes == 30], 1), " km²<br/>",
      "• 60 min: ", round(area_km2[drive_time_minutes == 60], 1), " km²<br/>", 
      "• 120 min: ", round(area_km2[drive_time_minutes == 120], 1), " km²<br/>",
      "• 180 min: ", round(area_km2[drive_time_minutes == 180], 1), " km²"
    ),
    label = ~paste0("🏥 ", sample_name)
  ) %>%
  
  # Add layer controls
  addLayersControl(
    baseGroups = c("Light Map", "OpenStreetMap", "Satellite"),
    overlayGroups = c("Physician Locations", "30 minutes", "60 minutes", "120 minutes", "180 minutes"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  
  # Add legend
  addLegend(
    pal = color_pal,
    values = test_isochrones$drive_time_minutes,
    title = "Drive Time<br/>(minutes)",
    position = "topright"
  ) %>%
  
  # Add title
  addControl(
    html = "<div style='background: white; padding: 10px; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2);'>
              <h3 style='margin: 0; color: #333;'>🚗 OB/GYN Physician Drive Time Isochrones</h3>
              <p style='margin: 5px 0 0 0; color: #666; font-size: 14px;'>Test Sample: 10 Locations Across US</p>
            </div>",
    position = "topleft"
  ) %>%
  
  # Set initial view to show all isochrones
  fitBounds(
    lng1 = sf::st_bbox(test_isochrones)[1],
    lat1 = sf::st_bbox(test_isochrones)[2], 
    lng2 = sf::st_bbox(test_isochrones)[3],
    lat2 = sf::st_bbox(test_isochrones)[4]
  )

# Display the map
log_info("Displaying interactive isochrone map...")
print(isochrone_map)

# Save the map
map_file <- "plots/test_isochrones_map.html"
dir.create("plots", showWarnings = FALSE)
htmlwidgets::saveWidget(isochrone_map, map_file, selfcontained = TRUE)
log_info("Saved interactive map to: {map_file}")

# Create summary statistics
log_info("Test isochrone summary statistics:")

summary_stats <- test_isochrones %>%
  sf::st_drop_geometry() %>%
  dplyr::group_by(drive_time_minutes) %>%
  dplyr::summarise(
    locations = n(),
    min_area_km2 = round(min(area_km2), 1),
    max_area_km2 = round(max(area_km2), 1),
    avg_area_km2 = round(mean(area_km2), 1),
    total_coverage_km2 = round(sum(area_km2), 1),
    .groups = "drop"
  )

print(summary_stats)

log_info("Interactive map created successfully!")
log_info("Click on isochrones and physician markers to see details")
log_info("Use layer controls to show/hide different drive times")
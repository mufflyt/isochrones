# Create a simple test map that should work in RStudio
library(dplyr)
library(sf)
library(leaflet)
library(logger)

log_info("Creating simple test map...")

# Load the test isochrones
test_isochrones <- sf::st_read("data/04-geocode/output/physician_isochrones_here_api.gpkg", quiet = TRUE)

# Just show 60-minute isochrones for simplicity
isochrones_60min <- test_isochrones %>%
  dplyr::filter(drive_time_minutes == 60)

# Simple color palette
pal <- leaflet::colorNumeric("viridis", isochrones_60min$area_km2)

# Create simple map
simple_map <- leaflet(isochrones_60min) %>%
  addTiles() %>%
  addPolygons(
    fillColor = ~pal(area_km2),
    fillOpacity = 0.7,
    color = "white",
    weight = 2,
    popup = ~paste0(
      "<b>", sample_name, "</b><br/>",
      "60-minute drive time<br/>",
      "Area: ", round(area_km2, 1), " km²"
    )
  ) %>%
  addCircleMarkers(
    lng = ~center_lon,
    lat = ~center_lat,
    radius = 5,
    color = "red",
    fillColor = "red",
    fillOpacity = 1,
    popup = ~sample_name
  ) %>%
  addLegend(
    pal = pal,
    values = ~area_km2,
    title = "Area (km²)"
  )

# Display
print(simple_map)

# Also save as HTML
htmlwidgets::saveWidget(simple_map, "plots/simple_test_map.html")
log_info("Simple map saved to plots/simple_test_map.html")

# Try opening in browser
browseURL("plots/simple_test_map.html")
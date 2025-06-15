#######################
source("R/01-setup.R")
#######################

library(tidyverse)
library(sf)
library(tigris)

# Let's bring in the isochrones
# Let's bring in the isochrones
isos <- sf::st_read("/Users/tylermuffly/Dropbox (Personal)/walker_maps/data/20241013161700.shp") %>%

#isos <- st_read("data/HERE_isochrone_results (1)/20241013161700.shp") %>%
  mutate(year = str_sub(departure, 1, 4))

# Let's make a list of isochrone maps, named by year. Styling can be modified as needed.
years <- 2013:2023
names(years) <- paste0("y", years)

library(tigris)
states_data <- tigris::states(cb = TRUE, resolution = "20m", year = 2020)


state_borders <- tigris::states(cb = TRUE, resolution = "20m") %>% tigris::shift_geometry()

# Iterate over the years and create a map for each year
yearly_maps <- map(years, function(yr) {
  isos1 <- isos %>%
    filter(year == yr) %>%
    shift_geometry() %>%
    group_by(range) %>%
    summarize() %>%
    arrange(desc(range)) %>%
    mutate(range = range / 60)

  ggplot() +
    geom_sf(data = state_borders, color = "darkgrey", fill = "white") +
    geom_sf(data = isos1, aes(fill = as.factor(range)), color = NA, alpha = 0.8) +
    theme_void() +
    scale_fill_viridis_d() +
    labs(fill = "Drive Time (minutes)",
         title = paste0("Access to Oncologic Gynecologists in ", yr))

})

# Examine a map:
yearly_maps$y2016

# We can combine the maps with patchwork and some modifications
# to make the styling better when in a grid of plots
library(patchwork)

yearly_maps_min <- map(years, function(yr) {
  isos1 <- isos %>%
    filter(year == yr) %>%
    shift_geometry() %>%
    group_by(range) %>%
    summarize() %>%
    arrange(desc(range)) %>%
    mutate(range = range / 60)

  ggplot() +
    geom_sf(data = state_borders, color = "darkgrey", fill = "white") +
    geom_sf(data = isos1, aes(fill = as.factor(range)), color = NA, alpha = 0.8) +
    theme_void() +
    scale_fill_viridis_d() +
    labs(title = yr,
         fill = "")

})

# wrap_plots() takes a list of plots and arranges them in a grid
# guide_area() places the legend in the empty space; modify styling as needed
# guides = "collect" collects the legends into one
wrap_plots(yearly_maps_min) +
  guide_area() +
  plot_layout(guides = "collect")








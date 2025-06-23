

# Let's bring in the isochrones
# Let's bring in the isochrones
isos <- sf::st_read("/Users/tylermuffly/Dropbox (Personal)/walker_maps/data/20241013161700.shp") %>%

#isos <- sf::st_read("data/HERE_isochrone_results (1)/20241013161700.shp") %>%
  dplyr::mutate(year = stringr::str_sub(departure, 1, 4))

# Let's make a list of isochrone maps, named by year. Styling can be modified as needed.
years <- 2013:2023
names(years) <- paste0("y", years)
states_data <- tigris::states(cb = TRUE, resolution = "20m", year = 2020)


state_borders <- tigris::states(cb = TRUE, resolution = "20m") %>% tigris::shift_geometry()

# Iterate over the years and create a map for each year
yearly_maps <- purrr::map(years, function(yr) {
  isos1 <- isos %>%
    dplyr::filter(year == yr) %>%
    tigris::shift_geometry() %>%
    dplyr::group_by(range) %>%
    dplyr::summarize() %>%
    dplyr::arrange(dplyr::desc(range)) %>%
    dplyr::mutate(range = range / 60)

  ggplot2::ggplot() +
    ggplot2::geom_sf(data = state_borders, color = "darkgrey", fill = "white") +
    ggplot2::geom_sf(data = isos1, ggplot2::aes(fill = as.factor(range)), color = NA, alpha = 0.8) +
    ggplot2::theme_void() +
    viridis::scale_fill_viridis_d() +
    ggplot2::labs(fill = "Drive Time (minutes)",
         title = paste0("Access to Oncologic Gynecologists in ", yr))

})

# Examine a map:
yearly_maps$y2016

# We can combine the maps with patchwork and some modifications
# to make the styling better when in a grid of plots

yearly_maps_min <- purrr::map(years, function(yr) {
  isos1 <- isos %>%
    dplyr::filter(year == yr) %>%
    tigris::shift_geometry() %>%
    dplyr::group_by(range) %>%
    dplyr::summarize() %>%
    dplyr::arrange(dplyr::desc(range)) %>%
    dplyr::mutate(range = range / 60)

  ggplot2::ggplot() +
    ggplot2::geom_sf(data = state_borders, color = "darkgrey", fill = "white") +
    ggplot2::geom_sf(data = isos1, ggplot2::aes(fill = as.factor(range)), color = NA, alpha = 0.8) +
    ggplot2::theme_void() +
    viridis::scale_fill_viridis_d() +
    ggplot2::labs(title = yr,
         fill = "")

})

# wrap_plots() takes a list of plots and arranges them in a grid
# guide_area() places the legend in the empty space; modify styling as needed
# guides = "collect" collects the legends into one
patchwork::wrap_plots(yearly_maps_min) +
  patchwork::guide_area() +
  patchwork::plot_layout(guides = "collect")








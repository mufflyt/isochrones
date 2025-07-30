#' Setup Output Structure
#'
#' Create structured directories for intermediate and visualization outputs.
#'
#' @param base_path Base path for output directories. Defaults to "output".
#' @return Named list with created directory paths.
#' @export
create_output_structure <- function(base_path = "output") {
  dirs <- list(
    base = base_path,
    intermediate = file.path(base_path, "intermediate"),
    visualization = file.path(base_path, "visualization")
  )

  for (d in dirs) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE, showWarnings = FALSE)
    }
  }
  return(dirs)
}

#' Save Intermediate Result
#'
#' Save an R object as an RDS file inside the intermediate directory.
#'
#' @param object R object to save.
#' @param name File name without extension.
#' @param paths List returned by `create_output_structure`.
#' @return File path of the saved object.
#' @export
save_intermediate_result <- function(object, name, paths) {
  stopifnot("intermediate" %in% names(paths))
  file_path <- file.path(paths$intermediate, paste0(name, ".rds"))
  saveRDS(object, file_path)
  return(file_path)
}

#' Prepare Visualization Ready Data
#'
#' Write an `sf` object to GeoJSON for easy visualization.
#'
#' @param sf_object An `sf` object.
#' @param paths List returned by `create_output_structure`.
#' @param file_name Name of the GeoJSON file. Defaults to "visualization_data.geojson".
#' @return File path of the saved GeoJSON.
#' @export
prepare_visualization_data <- function(sf_object, paths, file_name = "visualization_data.geojson") {
  stopifnot("visualization" %in% names(paths))
  file_path <- file.path(paths$visualization, file_name)
  sf::st_write(sf_object, file_path, driver = "GeoJSON", append = FALSE, quiet = TRUE)
  return(file_path)
}

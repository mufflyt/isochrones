testthat::local_edition(3)
source(file.path("..", "..", "R", "output_structure.R"))


testthat::test_that("create_output_structure creates directories", {
  tmp <- tempfile()
  paths <- create_output_structure(tmp)
  testthat::expect_true(dir.exists(paths$intermediate))
  testthat::expect_true(dir.exists(paths$visualization))
})


testthat::test_that("save_intermediate_result writes file", {
  tmp <- tempfile()
  paths <- create_output_structure(tmp)
  f <- save_intermediate_result(mtcars, "mt", paths)
  testthat::expect_true(file.exists(f))
})


testthat::test_that("prepare_visualization_data writes geojson", {
  testthat::skip_if_not_installed("sf")
  library(sf)
  tmp <- tempfile()
  paths <- create_output_structure(tmp)
  sf_obj <- st_as_sf(data.frame(id = 1, geometry = st_sfc(st_point(c(0,0)))), crs = 4326)
  out <- prepare_visualization_data(sf_obj, paths, "file.geojson")
  testthat::expect_true(file.exists(out))
})

library(testthat)
source(file.path("..", "..", "R", "formatting.R"))

test_that("format_pct rounds and formats numbers", {
  expect_equal(format_pct(0.1234, my_digits = 2), "0.12")
})

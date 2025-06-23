source(file.path("..", "..", "R", "print_ascii_art.R"))

testthat::test_that("print_ascii_art outputs ascii", {
  out <- capture.output(print_ascii_art(file.path("..", "..", "docs", "ascii_art.txt")))
  testthat::expect_true(any(grepl("___", out[1], fixed = TRUE)))
})

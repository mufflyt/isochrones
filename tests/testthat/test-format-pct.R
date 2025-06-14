format_pct <- function(x, my_digits = 0) {
  format(x, digits = my_digits, nsmall = my_digits)
}

# testthat
library(testthat)

# or we could source the function from R/01-setup.R
# source("R/01-setup.R")

test_that("format_pct rounds and formats numbers", {
  expect_equal(format_pct(0.1234, my_digits = 2), "0.12")
})

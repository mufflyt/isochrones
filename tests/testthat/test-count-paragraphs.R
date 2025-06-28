testthat::local_edition(3)
library(testthat)
source(file.path("..", "..", "R", "abstract_statistics.R"))

test_that("count_paragraphs counts paragraphs", {
  text <- "One.\n\nTwo."
  expect_equal(count_paragraphs(text), 2)
})

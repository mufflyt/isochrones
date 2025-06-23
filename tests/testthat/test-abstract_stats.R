library(testthat)
source(file.path("..", "..", "R", "abstract_statistics.R"))


test_that("summarize_abstract counts correctly", {
  text <- "Hello world. This is a test!"
  res <- summarize_abstract(text)
  expect_equal(res$words, 6)
  expect_equal(res$sentences, 2)
  expect_equal(res$characters, nchar(text))
})

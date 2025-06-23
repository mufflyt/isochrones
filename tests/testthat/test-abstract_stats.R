source(file.path("..", "..", "R", "abstract_statistics.R"))


testthat::test_that("summarize_abstract counts correctly", {
  text <- "Hello world. This is a test!"
  res <- summarize_abstract(text)
  testthat::expect_equal(res$words, 6)
  testthat::expect_equal(res$sentences, 2)
  testthat::expect_equal(res$characters, nchar(text))
})

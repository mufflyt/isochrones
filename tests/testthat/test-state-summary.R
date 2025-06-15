library(testthat)

source("R/state_summary.R")


test_that("summarize_state_counts computes correct statistics", {
  summary <- summarize_state_counts("state_data.csv")
  expect_equal(summary$total_count, 21712)
  expect_equal(round(summary$average_count, 2), 417.54)
  expect_equal(summary$median_count, 248.5)
})

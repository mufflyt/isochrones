library(testthat)

skip_if_not_installed("RSQLite")
skip_if_not_installed("assertthat")

con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbCreateTable(con, "NPPES_Data_Dissemination2019", data.frame(NPI=1, `Healthcare Provider Taxonomy Code_1`="207L00000X", `Entity Type Code`=1, `Provider Business Practice Location Address Postal Code`="80220"))
DBI::dbCreateTable(con, "NPPES_Data_Dissemination2020", data.frame(NPI=2, `Healthcare Provider Taxonomy Code_1`="207L00000X", `Entity Type Code`=1, `Provider Business Practice Location Address Postal Code`="80230"))

mapping <- create_nppes_table_mapping(con)

test_that("mapping extracts years", {
  expect_equal(sort(mapping$year), c(2019, 2020))
})

test_that("find_physicians_across_years returns expected rows", {
  res <- find_physicians_across_years(con, mapping, "207L00000X")
  expect_equal(nrow(res), 2)
  expect_true(all(c("NPI", "Zip", "Year") %in% names(res)))
})

DBI::dbDisconnect(con)

# This file is part of the standard setup for testthat.
# It is recommended that you do not modify it.
#
# Where should you do additional test configuration?
# Learn more about the roles of various files in:
# * https://r-pkgs.org/testing-design.html#sec-tests-files-overview
# * https://testthat.r-lib.org/articles/special-files.html

library(testthat)

# Source package R code so tests can run without installation
source(file.path("R", "formatting.R"))
source(file.path("R", "nppes.R"))
testthat::test_dir("tests/testthat")

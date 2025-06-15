# Common mistakes and fixes

This repository contained a few patterns that often trip up new R coders.  The recent changes address the following issues:

1. **Hard coded API keys** – replaced with `Sys.getenv()` so secrets are read from the environment.
2. **Package installation inside scripts** – commented out install commands and clarified that packages should be installed beforehand.
3. **Duplicated helper functions** – moved `format_pct()` to `R/format_pct.R` and updated tests to source it.
4. **Absolute paths** – database path now comes from `NPPES_DB_PATH` environment variable.
5. **Unnecessary `setwd()` and duplicate `library()` calls** – removed redundant
   working directory changes and extra `library(ggplot2)` line.

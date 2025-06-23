# Common Setup Mistakes

- **NUCC_PATH not set**: Some scripts expect the `NUCC_PATH` environment variable to point to the directory containing `nucc_taxonomy_201.csv`. Add a line like `NUCC_PATH=/path/to/nucc` to your `.Renviron` or shell profile before running the deprecated `zzC-Extracting_and_Processing_NPPES.R` script.
- **Incorrect FIPS loop**: Earlier versions of `get_census_data` looped over an
  undefined object `us_fips`, causing errors. The function now correctly
  iterates over the provided `us_fips_list` argument.

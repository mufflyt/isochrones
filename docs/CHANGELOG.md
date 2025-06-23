# Changelog
## 2025-06-23
- Documented supporting files in the README and introduced the `count_paragraphs()` helper.
- Added tests for the new helper function.


## 2025-06-22
- Verified recent updates to `01-setup.R`, correcting indentation and spacing to match the style guide.

## 2025-06-21
- Added a new "About Me" page that introduces the project author and explains the motivation behind the work.

## 2025-06-20
- Removed a duplicate definition of `format_pct` from the helpers to avoid ambiguous references and simplify maintenance.

## 2025-06-19
- Fixed the variable order bug in the geocoding script which previously caused incorrect location lookups.

## 2025-06-18
- Parameterized the setup helper functions so they can be reused across different scripts with minimal configuration.

## 2025-06-17
- Introduced a `verbose` flag to utility functions, allowing optional logging for longer-running operations.

## 2025-06-16
- Cleaned up `R/01-setup.R` by removing an extra blank line before the `# fin` comment for a tidier script.

## 2025-06-15
- Enhanced file save functions to print the path of the written file, making it easier to confirm the output location.

## 2025-06-14
- Added an unlimited NPI search helper that iterates over paginated API results so no data is missed.

## 2025-06-13
- Supported a `NUCC_PATH` environment variable, letting users override the default taxonomy data location.

## 2025-06-12
- Documented several database optimization suggestions to speed up provider lookups and reduce memory use.

## 2025-06-11
- Outlined guidelines for consistent function naming to help new contributors follow the project style.

## 2025-06-10
- Updated the README to mention deprecated scripts and explain when to migrate to the new pipelines.

## 2025-06-09
- Inserted a compass rose and scale bar into interactive maps for better orientation and scale awareness.

## 2025-06-08
- Documented the option to visualize results with either mapview or leaflet, highlighting the pros and cons of each.

## 2025-06-07
- Centralized HERE API key retrieval through helper functions to minimize duplicated code.

## 2025-06-06
- Replaced hard-coded secrets with environment variables, improving security and configurability.

## 2025-06-05
- Removed an obsolete demographic processing script that was superseded by the new pipeline.

## 2025-06-04
- Added a unit test for `format_pct` to verify correct formatting of percentage outputs.

## 2025-06-03
- Implemented memory-efficient provider processing with DuckDB so large datasets can be handled on modest hardware.

## 2025-06-02
- Added numbered pipeline files and restored critical analysis scripts from backup to ensure the full workflow is versioned.

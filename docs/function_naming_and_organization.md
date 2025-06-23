# Function Naming and Organization Guidelines

To make it easier to navigate the large number of functions in this project,
consider the following suggestions:

## Naming Conventions

- Use **snake_case** for all function names (e.g. `load_data()`).
- Use a **verb_noun** pattern so the function name describes an action (e.g.
  `calculate_isochrones()`).
- When functions belong to a category, use a prefix such as `db_` for database
  utilities or `plot_` for visualization helpers.
- Avoid overly long names, but be descriptive enough to convey purpose.

## File Organization

- Group related functions into separate R scripts under the `R/` directory.
  For example, place database helper functions in `R/db_helpers.R` and plotting
  utilities in `R/plot_helpers.R`.
- Keep each file focused on a single theme or data workflow. This reduces
  cognitive load when browsing the project.

## Documentation

- Add `roxygen2` comments to each function. Document what the function does,
  its inputs, and outputs. This makes it easier to understand and search for
  functions.
- Create an index in `docs/` listing all functions with brief descriptions and
  links to their source files. This acts as a quick reference.

## Example

Here is a simple template for a function with `roxygen2` comments:

```r
#' Retrieve physician data from the database
#'
#' @param con A database connection
#' @param npi Vector of NPIs to query
#' @return A tibble with provider information
#' @export
get_physician_data <- function(con, npi) {
  # function code here
}
```

Following a consistent style will help future contributors quickly locate and
understand the many functions in this repository.

#' Print the project ASCII art
#'
#' Reads the ASCII art stored in `docs/ascii_art.txt` and prints it to the
#' console. This helper is mainly for fun and quick project identification.
#'
#' @param path Path to the ASCII art text file. Defaults to
#'   `"docs/ascii_art.txt"`.
#'
#' @return Invisibly returns the lines of the ASCII art.
#' @export
print_ascii_art <- function(path = file.path("docs", "ascii_art.txt")) {
  if (!file.exists(path)) {
    stop(sprintf("ASCII art file '%s' not found", path))
  }

  art <- readLines(path, warn = FALSE)
  cat(art, sep = "\n")
  invisible(art)
}

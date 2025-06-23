#' @title Summarize an abstract
#' @description Tokenizes text using `stringr::boundary` to count words and
#'   sentences. The counts along with character length are returned in a tibble
#'   for easy downstream analysis or unit testing.
#'
#' @param text Character string containing the abstract text.
#'
#' @return A tibble with three columns: `words`, `sentences` and `characters`.
#'
#' @family text-utilities
#'
#' @importFrom stringr str_count boundary
#' @importFrom tibble tibble
#'
#' @examples
#' abstract_text <- "This is a test."
#' summarize_abstract(abstract_text)
#' @export
summarize_abstract <- function(text) {
  tibble::tibble(
    words = stringr::str_count(text, stringr::boundary("word")),
    sentences = stringr::str_count(text, stringr::boundary("sentence")),
    characters = nchar(text)
  )
}

#' @title Count paragraphs in text
#' @description Splits the input string on blank lines and returns the number
#'   of non-empty elements.
#'
#' @param text Character string containing paragraphs separated by blank lines
#'
#' @return Integer count of paragraphs
#'
#' @family text-utilities
#'
#' @examples
#' count_paragraphs("One.\n\nTwo.")
#'
#' @export
count_paragraphs <- function(text) {
  if (length(text) > 1) {
    text <- paste(text, collapse = "\n")
  }
  parts <- unlist(strsplit(text, "\n\s*\n"))
  sum(nzchar(trimws(parts)))
}

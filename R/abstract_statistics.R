#' Summarize an abstract by counting words, sentences and characters
#'
#' The function tokenizes the input text using `stringr::boundary` to determine
#' word and sentence counts.  It then returns these counts along with the
#' character length of the input.  The result is returned as a small tibble that
#' is convenient for downstream analysis or unit testing.
#'
#' @param text Character string containing the abstract text.
#'
#' @return A tibble with three columns: `words`, `sentences` and `characters`.
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

#' Count the number of paragraphs in text
#'
#' @param text Character string containing paragraphs separated by blank lines
#' @return Integer count of paragraphs
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

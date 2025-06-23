#' Calculate basic text statistics for an abstract
#'
#' @param text Character string containing the abstract text
#' @return A tibble with word, sentence and character counts
#' @examples
#' abstract_text <- "This is a test."
#' summarize_abstract(abstract_text)
#'
#' @export
summarize_abstract <- function(text) {
  tibble::tibble(
    words = stringr::str_count(text, stringr::boundary("word")),
    sentences = stringr::str_count(text, stringr::boundary("sentence")),
    characters = nchar(text)
  )
}

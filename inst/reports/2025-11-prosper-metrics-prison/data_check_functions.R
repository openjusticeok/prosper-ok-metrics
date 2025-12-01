
#' Summarise Missing Values in a Data Frame Column
#'
#' Calculates the number of missing (NA) values and the total number of rows for a specified column in a data frame.
#'
#' @param data A data frame containing the data to be summarised.
#' @param field The column to check for missing values (unquoted, tidy evaluation).
#' @return A data frame with two columns: \code{missing} (number of missing values) and \code{total} (total number of rows).
summarise_missing <- function(data, field) {
  data |>
    summarise(
      missing = sum(is.na({{ field }})),
      total = n()
    )
}


summarise_date_range <- function(data, date_field) {
  data |>
    summarise(
      min_date = min({{ date_field }}, na.rm = TRUE),
      max_date = max({{ date_field }}, na.rm = TRUE),
      total = n(),
      # Should be the same after nesting offenses and multiple sentences
      n_people = n_distinct(doc_num)
    )
}



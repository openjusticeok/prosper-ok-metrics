# TODO: All of this is scaffold code and needs to be implemented properly.
summarise_missing <- function(data, field) {
  data |>
    dplyr::summarise(
      missing = sum(is.na({{ field }})),
      total = dplyr::n()
    )
}

summarise_date_range <- function(data, date_field) {
  out <- data |>
    dplyr::summarise(
      min_date = min({{ date_field }}, na.rm = TRUE),
      max_date = max({{ date_field }}, na.rm = TRUE),
      total = dplyr::n(),
      n_people = dplyr::n_distinct(.data$doc_num)
    )

  out |>
    dplyr::mutate(
      min_date = dplyr::if_else(is.infinite(as.numeric(.data$min_date)), as.Date(NA), .data$min_date),
      max_date = dplyr::if_else(is.infinite(as.numeric(.data$max_date)), as.Date(NA), .data$max_date)
    )
}

check_prison_processed <- function(processed_data = prison_processed_data) {
  people_with_sentence_info <- processed_data$people_with_sentence_info

  missing_county <- summarise_missing(people_with_sentence_info, .data$most_recent_sentencing_county)
  missing_date <- summarise_missing(people_with_sentence_info, .data$most_recent_sentencing_date)
  date_range <- summarise_date_range(people_with_sentence_info, .data$most_recent_sentencing_date)

  tibble::tibble(
    check = c(
      "most_recent_sentencing_county_missing",
      "most_recent_sentencing_date_missing",
      "most_recent_sentencing_date_range"
    ),
    status = c(
      ifelse(missing_county$missing == 0, "pass", "warning"),
      ifelse(missing_date$missing == 0, "pass", "warning"),
      "info"
    ),
    details = list(missing_county, missing_date, date_range)
  )
}

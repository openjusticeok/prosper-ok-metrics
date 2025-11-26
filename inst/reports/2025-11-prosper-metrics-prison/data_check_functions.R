
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



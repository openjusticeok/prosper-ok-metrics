## Process Vera Incarceration Trends Data
process_ingested_vera_data <- function(vera_data) {
  vera_data |>
    dplyr::mutate(
      source = "Vera: Incarceration Trends",
      year_date = lubridate::make_date(.data$year, 1, 1)
    )
}
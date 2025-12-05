## Process Vera Incarceration Trends Data
process_ingested_vera_data <- function(vera_data) {
  vera_data |>
    dplyr::mutate(
      source = "Vera: Incarceration Trends",
      quarter_date = lubridate::ymd(paste0(.data$year, "-", .data$quarter * 3, "-01"))
    )
}
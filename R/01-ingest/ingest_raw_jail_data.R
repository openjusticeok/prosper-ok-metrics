ingest_okpolicy_scraped_data <- function() {
  scraped_jail_data <- tulsaCountyJailScraper::scrape_data()

  list(
    bookings = scraped_jail_data$bookings_tibble |>
      dplyr::mutate(
        created_at = lubridate::today()
      ),
    charges = scraped_jail_data$charges_tibble |>
      dplyr::mutate(
        created_at = lubridate::today()
      )
  )
}

ingest_asemio_scraped_data <- function(schema = "iic") {
  list(
    bookings = ojodb::ojo_tbl("arrest", schema = schema) |>
      dplyr::collect(),
    charges = ojodb::ojo_tbl("offense", schema = schema) |>
      dplyr::collect(),
    inmates = ojodb::ojo_tbl("inmate", schema = schema) |>
      dplyr::collect()
  )
}

read_brek_jail_report_data <- function(path = here::here("data", "input", "brek_jail_report_data.csv")) {
  if (!file.exists(path)) {
    stop(
      sprintf("Static Brek reference file not found at %s. Run the one-time ingest script.", path),
      call. = FALSE
    )
  }

  brek_jail_data <- readr::read_csv(path, show_col_types = FALSE)

  return(brek_jail_data)
}

ingest_jail_raw_data <- function(schema = "iic",
                                 brek_data_path = here::here(
                                   "data",
                                   "input",
                                   "brek_jail_report_data.csv"
                                 )) {
  list(
    okpolicy = ingest_okpolicy_scraped_data(),
    asemio = ingest_asemio_scraped_data(schema = schema),
    brek = read_brek_jail_report_data(brek_data_path)
  )
}

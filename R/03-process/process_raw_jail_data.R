process_jail_dataset <- function(input_checks = jail_input_checks) {
  raw <- input_checks$raw_data

  scraped_bookings <- raw$scraped$bookings |>
    dplyr::mutate(
      # TODO: Come back an reevaluate use of tz and quiet parameters
      birth_date = lubridate::mdy(.data$birth_date),
      booking_datetime = lubridate::ymd_hms(
        .data$booking_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      booking_date = as.Date(.data$booking_datetime),
      source = "scraper",
      gender = stringr::str_to_title(.data$gender),
      race = stringr::str_to_title(.data$race)
    )

  scraped_charges <- raw$scraped$charges |>
    dplyr::mutate(
      bail_set_datetime = lubridate::ymd_hms(
        .data$bail_set_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      bail_set_date = as.Date(.data$bail_set_datetime)
    )

  db_bookings <- raw$ojodb$bookings |>
    dplyr::mutate(source = "ojodb")

  combined_bookings <- dplyr::bind_rows(scraped_bookings, db_bookings) |>
    dplyr::filter(!is.na(booking_date)) |>
    dplyr::mutate(
      booking_month = lubridate::floor_date(booking_date, "month"),
      booking_year = lubridate::year(booking_date)
    )

  booking_trends <- combined_bookings |>
    dplyr::count(source, booking_month, name = "bookings") |>
    dplyr::arrange(booking_month)

  booking_demographics_gender <- combined_bookings |>
    tidyr::drop_na(gender) |>
    dplyr::count(source, gender, name = "bookings")

  booking_demographics_race <- combined_bookings |>
    tidyr::drop_na(race) |>
    dplyr::count(source, race, name = "bookings")

  list(
    booking_records = combined_bookings,
    booking_trends = booking_trends,
    booking_demographics = list(
      gender = booking_demographics_gender,
      race = booking_demographics_race
    ),
    brek_report = raw$brek,
    release_counts = raw$brek |>
      dplyr::filter(metric_family == "release_counts"),
    release_shares = raw$brek |>
      dplyr::filter(metric_family == "release_share"),
    adp_summary = raw$brek |>
      dplyr::filter(metric_family == "adp"),
    brek_bookings_summary = raw$brek |>
      dplyr::filter(metric_family == "bookings")
  )
}

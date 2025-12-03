# TODO: This is all a placeholder for now until we have real checks to implement
# Function to check the integrity of raw jail data inputs using pointblank
check_jail_ingested <- function(ingested_data = jail_ingested_data) {
  pb_levels <- pointblank::action_levels(warn_at = 0.02, stop_at = 0.1)
  warning_only_levels <- pointblank::action_levels(warn_at = 0.02, stop_at = Inf)

  # Checks for Jail Data Initiative data
  # There should never be both a dlm and jacket_number
  # !is.na(.data$dlm) & !is.na(.data$jacket_number)
  #
  # The jacket number should have an old and new version that are not the same
  # janitor::clean_names() |>
  # dplyr::rename(
  #   birth_date = "dob",
  #   jacket_number_old = "dlm",
  # ) |>
  # dplyr::mutate(
  #   jacket_number_type = dplyr::case_when(
  #     is.na(.data$jacket_number) & !is.na(.data$jacket_number_old) ~ "old",
  #     !is.na(.data$jacket_number) ~ "new",
  #     TRUE ~ NA_character_
  #   )
  # ) |>
  # dplyr::summarize(
  #   average_scrape_date = mean(.data$meta_scrape_date, na.rm = TRUE),
  #   average_booking_date = mean(.data$booking_date, na.rm = TRUE),
  #   n = n(),
  #   .by = jacket_number_type
  # )

  # OK Policy Tulsa County jail bookings data
  okpolicy_bookings_agent <- pointblank::create_agent(
    tbl = ingested_data$okpolicy$bookings,
    label = "Scraped Tulsa County jail bookings",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(booking_date, gender, race)) |>
    pointblank::col_is_date(pointblank::vars(booking_date), actions = warning_only_levels) |>
    pointblank::col_vals_not_null(pointblank::vars(gender, race)) |>
    pointblank::interrogate()

  asemio_bookings_agent <- pointblank::create_agent(
    tbl = ingested_data$asemio$bookings,
    label = "asemio jail bookings",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(booking_date)) |>
    pointblank::col_is_date(pointblank::vars(booking_date)) |>
    pointblank::interrogate()

  brek_agent <- pointblank::create_agent(
    tbl = ingested_data$brek,
    label = "Brek report data aggregated metrics",
    actions = pb_levels
  ) |>
    pointblank::col_vals_in_set(
      columns = pointblank::vars(metric_family),
      set = c("adp", "bookings", "release_counts", "release_share", "avg_length_of_stay", "rebooking_rate")
    ) |>
    pointblank::col_vals_between(pointblank::vars(year), 2000, 2030) |>
    pointblank::interrogate()

  summarize_agent <- function(agent, name) {
    status <- if (pointblank::all_passed(agent)) "pass" else "warning"
    tibble::tibble(
      check = name,
      status = status,
      failed = sum(rlang::`%||%`(agent$validation_set$n_failed, 0))
    )
  }

  tibble::lst(
    ingested_data = ingested_data,
    checks = dplyr::bind_rows(
      summarize_agent(okpolicy_bookings_agent, "okpolicy_bookings"),
      summarize_agent(asemio_bookings_agent, "asemio_bookings"),
      summarize_agent(brek_agent, "brek_reference")
    )
  )
}

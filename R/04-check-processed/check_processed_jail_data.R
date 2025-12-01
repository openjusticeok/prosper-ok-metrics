# TODO: All of this is boilerplate. Replace with something actually meaninful
check_jail_processed <- function(processed_data = jail_processed_data) {
  pb_levels <- pointblank::action_levels(warn_at = 0.02, stop_at = 0.1)

  trend_agent <- pointblank::create_agent(
    tbl = processed_data$booking_trends,
    label = "Booking trend summaries",
    actions = pb_levels
  ) |>
    pointblank::col_is_date(pointblank::vars(booking_month)) |>
    pointblank::col_vals_gte(pointblank::vars(bookings), 0) |>
    pointblank::interrogate()

  release_agent <- pointblank::create_agent(
    tbl = processed_data$release_counts,
    label = "Release counts",
    actions = pb_levels
  ) |>
    pointblank::col_vals_gte(pointblank::vars(value), 0) |>
    pointblank::interrogate()

  summarize_agent <- function(agent, name) {
    status <- if (pointblank::all_passed(agent)) "pass" else "warning"
    tibble::tibble(
      check = name,
      status = status,
      failed = sum(rlang::`%||%`(agent$validation_set$n_failed, 0))
    )
  }

  dplyr::bind_rows(
    summarize_agent(trend_agent, "booking_trends"),
    summarize_agent(release_agent, "release_counts")
  )
}

analyze_jail_metrics <- function(processed_data = jail_processed_data) {
  booking_totals <- processed_data$booking_totals
  booking_records <- processed_data$booking_records
  brek_report <- processed_data$brek_report
  vera <- processed_data$vera

  latest_month_bookings <- booking_totals |>
    dplyr::group_by(source) |>
    dplyr::slice_max(order_by = booking_month, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()

  booking_month_totals <- booking_totals |>
    dplyr::distinct(source, booking_month, .keep_all = TRUE)

  booking_year_totals <- booking_records |>
    dplyr::summarise(
      total_bookings = dplyr::n(),
      .by = c(source, booking_year)
    )

  tulsa_brek_overall <- brek_report |>
    dplyr::filter(
      metric_family %in% c("adp", "bookings"),
      dimension == "Overall",
      category == "Total",
      county %in% c("Tulsa County", "Oklahoma County")
    )

  key_metrics <- dplyr::bind_rows(
    tulsa_brek_overall |>
      dplyr::mutate(
        metric = dplyr::case_when(
          metric_family == "adp" ~ "Average Daily Population",
          metric_family == "bookings" ~ "Annual Bookings",
          TRUE ~ metric_family
        ),
        display = glue::glue("{metric} ({county})")
      ) |>
      dplyr::transmute(
        metric = display,
        value,
        yoy_change
      ),
    latest_month_bookings |>
      dplyr::mutate(
        metric = glue::glue("Latest month bookings ({source})"),
        value = bookings,
        yoy_change = NA_real_
      ) |>
      dplyr::select(metric, value, yoy_change)
  )

  list(
    booking_records = booking_records,
    booking_month_totals = booking_month_totals,
    booking_year_totals = booking_year_totals,
    latest_month = latest_month_bookings,
    release_counts = processed_data$release_counts,
    release_shares = processed_data$release_shares,
    brek_report = brek_report,
    adp_summary = processed_data$adp_summary,
    key_metrics = key_metrics
  )
}

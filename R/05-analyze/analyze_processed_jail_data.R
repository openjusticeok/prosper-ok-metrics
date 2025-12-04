analyze_processed_jail_data <- function(processed_data = jail_processed_data) {
  booking_totals <- processed_data$booking_totals
  booking_records <- processed_data$booking_records
  brek_report <- processed_data$brek_report
  vera <- processed_data$vera

  ### Jail Bookings
  ## Public Jail Bookings Metrics
  # TODO: fix(analyze): Consider removing as it's not used in the report
  latest_month_bookings <- booking_totals |>
    dplyr::group_by(source) |>
    dplyr::slice_max(order_by = booking_month, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()

  bookings_multiproducer_mixmethod_month_total <- booking_totals |>
    dplyr::distinct(source, booking_month, .keep_all = TRUE)

  booking_year_totals <- booking_records |>
    dplyr::summarise(
      total_bookings = dplyr::n(),
      .by = c(source, booking_year)
    )

  # TODO: fix(analyze): Replace with actual 2025 data we want to report
  tulsa_brek_overall <- brek_report |>
    dplyr::filter(
      metric_family %in% c("adp", "bookings"),
      dimension == "Overall",
      category == "Total",
      county %in% c("Tulsa County", "Oklahoma County")
    )

  metrics_executive_summary <- dplyr::bind_rows(
    # TODO: Same as TODO above
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


  ## External Jail Bookings Metrics




  ### Jail Releases




  ### Jail Average Daily Population (ADP)





  ### Return list as the targets object
  list(
    # Executive Summary
    metrics_executive_summary = metrics_executive_summary,
    # Jail Bookings
    bookings_multiproducer_mixmethod_month_total = bookings_multiproducer_mixmethod_month_total,
    booking_year_totals = booking_year_totals,
    latest_month = latest_month_bookings,
    # Jail Releases
    release_counts = processed_data$release_counts,
    release_shares = processed_data$release_shares,
    # Jail Average Daily Population (ADP)
    adp_summary = processed_data$adp_summary,
    # Other
    brek_report = brek_report
  )
}

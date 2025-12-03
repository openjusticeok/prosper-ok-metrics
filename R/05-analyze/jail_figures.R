generate_jail_figures <- function(analysis_results = jail_analysis_results) {
  fmt_pct <- function(x) scales::percent(x, accuracy = 0.1)
  fmt_num <- function(x) scales::comma(x, accuracy = 1)
  has_rows_and_not_null <- function(x) !is.null(x) && nrow(x) > 0

  # TODO: feat(jail-figures): Add dashed lines for scraped data.
  # TODO: feat(jail-processing): Add data source type (scraped, administrative, etc.)
  booking_month_totals <- analysis_results$booking_month_totals
  if (has_rows_and_not_null(booking_month_totals)) {
    booking_trend_plot <- ggplot2::ggplot(
      booking_month_totals,
      ggplot2::aes(x = booking_month, y = bookings, color = source)
    ) +
      ggplot2::geom_line(linewidth = 1) +
      ggplot2::geom_point(size = 1) +
      ggplot2::labs(
        title = "Tulsa monthly jail bookings by data source",
        x = "Month",
        y = "Bookings",
        color = "Source"
      ) +
      ggplot2::scale_color_brewer(palette = "Dark2") +
      ojothemes::theme_okpi() +
      ggplot2::guides(color = ggplot2::guide_legend(ncol = 3))
  } else {
    booking_trend_plot <- ggplot2::ggplot() +
      ggplot2::theme_void() +
      ggplot2::labs(title = "No booking data available")
  }

  adp_overall <- analysis_results$adp_summary |>
    dplyr::filter(dimension == "Overall", category == "Total")

  if (has_rows_and_not_null(adp_overall)) {
    adp_plot <- ggplot2::ggplot(
      adp_overall,
      ggplot2::aes(x = county, y = value, fill = county)
    ) +
      ggplot2::geom_col(width = 0.6) +
      ggplot2::geom_text(
        ggplot2::aes(label = scales::comma(value), fontface = "bold"),
        vjust = -0.25,
        size = 8,
      ) +
      ggplot2::labs(
        title = "Average daily population (Starling Analytics, 2024)",
        x = NULL,
        y = "People"
      ) +
      ggplot2::scale_fill_brewer(palette = "Set2") +
      ojothemes::theme_okpi() +
      ggplot2::theme(legend.position = "none")
  } else {
    adp_plot <- ggplot2::ggplot() +
      ggplot2::theme_void() +
      ggplot2::labs(title = "No ADP data available")
  }

  release_counts_plot <- NULL
  release_share_plot <- NULL

  release_counts_data <- analysis_results$release_counts
  if (has_rows_and_not_null(release_counts_data) &&
    all(c("group", "category", "value") %in% colnames(release_counts_data))) {
    release_counts_plot <- release_counts_data |>
      dplyr::filter(.data$group %in% c("All", "Male", "Female")) |>
      ggplot2::ggplot(
        ggplot2::aes(
          x = category,
          y = value,
          fill = group
        )
      ) +
      ggplot2::geom_col(position = "dodge", width = 0.7) +
      ggplot2::labs(
        title = "Release counts by disposition and gender",
        x = NULL,
        y = "People",
        fill = "Group"
      ) +
      ggplot2::scale_fill_brewer(palette = "Dark2") +
      ojothemes::theme_okpi() +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 20, hjust = 1))
  }

  release_share_data <- analysis_results$release_shares
  if (has_rows_and_not_null(release_share_data) &&
    all(c("group", "category", "value") %in% colnames(release_share_data))) {
    release_share_plot <- release_share_data |>
      dplyr::filter(.data$group %in% c("All", "Male", "Female")) |>
      ggplot2::ggplot(
        ggplot2::aes(
          x = group,
          y = value,
          fill = category
        )
      ) +
      ggplot2::geom_col(position = "fill", width = 0.7) +
      ggplot2::scale_y_continuous(labels = scales::percent) +
      ggplot2::labs(
        title = "Share of releases by disposition",
        x = "Group",
        y = "Share of releases",
        fill = "Disposition"
      ) +
      ggplot2::scale_fill_brewer(palette = "Set2") +
      ojothemes::theme_okpi()
  }

  overview_metrics_table <- NULL
  if (has_rows_and_not_null(analysis_results$key_metrics)) {
    overview_metrics_table <- analysis_results$key_metrics |>
      dplyr::mutate(
        value = fmt_num(value),
        yoy_change = dplyr::case_when(
          is.na(yoy_change) ~ "—",
          TRUE ~ fmt_pct(yoy_change)
        )
      ) |>
      dplyr::rename(`Metric` = metric, `Value` = value, `Yoy Change` = yoy_change) |>
      ojothemes::gt_ojo()
  }

  booking_latest_table <- NULL
  if (has_rows_and_not_null(analysis_results$latest_month)) {
    booking_latest_table <- analysis_results$latest_month |>
      dplyr::mutate(
        `Latest Month` = format(booking_month, "%B %Y"),
        Bookings = fmt_num(bookings)
      ) |>
      dplyr::select(Source = source, `Latest Month`, Bookings) |>
      ojothemes::gt_ojo()
  }

  booking_annual_table <- NULL
  if (has_rows_and_not_null(analysis_results$booking_year_totals)) {
    booking_annual_table <- analysis_results$booking_year_totals |>
      dplyr::arrange(dplyr::desc(booking_year)) |>
      dplyr::mutate(
        Year = booking_year,
        Bookings = fmt_num(total_bookings)
      ) |>
      dplyr::select(Source = source, Year, Bookings) |>
      ojothemes::gt_ojo()
  }

  release_counts_table <- NULL
  if (has_rows_and_not_null(analysis_results$release_counts)) {
    release_counts_table <- analysis_results$release_counts |>
      dplyr::filter(group %in% c("All", "Male", "Female")) |>
      dplyr::mutate(Value = fmt_num(value)) |>
      dplyr::select(Disposition = category, Group = group, Value) |>
      ojothemes::gt_ojo()
  }

  release_shares_table <- NULL
  if (has_rows_and_not_null(analysis_results$release_shares)) {
    release_shares_table <- analysis_results$release_shares |>
      dplyr::mutate(Value = fmt_pct(value)) |>
      dplyr::select(Disposition = category, Group = group, Value) |>
      ojothemes::gt_ojo()
  }

  county_comparison_table <- NULL
  if (has_rows_and_not_null(analysis_results$brek_report)) {
    county_comparison_table <- analysis_results$brek_report |>
      dplyr::filter(
        metric_family %in% c("adp", "bookings"),
        dimension == "Overall",
        category == "Total",
        county %in% c("Tulsa County", "Oklahoma County")
      ) |>
      dplyr::mutate(
        Metric = dplyr::case_when(
          metric_family == "adp" ~ "Average Daily Population",
          TRUE ~ "Annual Bookings"
        ),
        Value = fmt_num(value),
        `YoY Change` = fmt_pct(as.numeric(yoy_change))
      ) |>
      dplyr::select(Metric, County = county, Value, `YoY Change`) |>
      ojothemes::gt_ojo()
  }

  list(
    booking_trend = booking_trend_plot,
    adp_comparison = adp_plot,
    release_counts = release_counts_plot,
    release_share = release_share_plot,
    overview_metrics_table = overview_metrics_table,
    booking_latest_table = booking_latest_table,
    booking_annual_table = booking_annual_table,
    release_counts_table = release_counts_table,
    release_shares_table = release_shares_table,
    county_comparison_table = county_comparison_table
  )
}

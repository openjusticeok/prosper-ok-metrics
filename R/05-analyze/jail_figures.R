generate_jail_figures <- function(analysis_results = jail_analysis_results) {
  booking_month_totals <- analysis_results$booking_month_totals
  if (nrow(booking_month_totals) > 0) {
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

  if (nrow(adp_overall) > 0) {
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

  list(
    booking_trend = booking_trend_plot,
    adp_comparison = adp_plot
  )
}

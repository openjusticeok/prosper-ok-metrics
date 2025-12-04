visualize_jail_figures <- function(analysis_results = jail_analysis_results) {
  ### Helper functions
  fmt_pct <- function(x) scales::percent(x, accuracy = 0.1)
  fmt_num <- function(x) scales::comma(x, accuracy = 1)




  ### Executive Summary
  ## Overview Metrics Table
  table_metrics_executive_summary <- analysis_results$metrics_executive_summary |>
    dplyr::mutate(
      value = fmt_num(value),
      yoy_change = dplyr::case_when(
        is.na(yoy_change) ~ "—",
        TRUE ~ fmt_pct(yoy_change)
      )
    ) |>
    dplyr::rename(`Metric` = metric, `Value` = value, `Yoy Change` = yoy_change) |>
    ojothemes::gt_ojo()




  ### Jail Bookings
  ## Public Jail Bookings Figures
  table_bookings_multiproducer_estimate_year_last_5_years_all <- placeholder_gt()

  plot_bookings_multiproducer_estimate_month_total <- placeholder_ggplot()
  plot_bookings_multiproducer_estimate_year_total <- placeholder_ggplot()
  plot_bookings_multiproducer_estimate_year_gender <- placeholder_ggplot()
  table_bookings_multiproducer_estimate_month_race <- placeholder_gt()
  plot_bookings_rate_multiproducer_estimate_month_race <- placeholder_ggplot()
  plot_bookings_multiproducer_estimate_yoy_all_county <- placeholder_ggplot()

  ## External Jail Bookings Figures
  # TODO: feat(jail-figures): Add dashed lines for scraped data.
  # TODO: feat(jail-processing): Add data source type (scraped, administrative, etc.)
  bookings_multiproducer_mixmethod_month_total <-
    analysis_results$bookings_multiproducer_mixmethod_month_total
  bookings_multiproducer_mixmethod_month_total <- placeholder_tibble()

  plot_bookings_multiproducer_mixmethod_month_total <- ggplot2::ggplot(
    bookings_multiproducer_mixmethod_month_total,
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






  ### Jail Releases Figures
  ## Public Jail Releases Figures
  plot_releases_vera_admin_quarter_total <- placeholder_ggplot()
  plot_releases_vera_admin_quarter_gender <- placeholder_ggplot()
  plot_releases_vera_admin_quarter_race <- placeholder_ggplot()
  plot_releases_vera_admin_quarter_yoy_county <- placeholder_ggplot()

  ## External Jail Releases Figures
  table_releases_vera_admin_year_last_5_years_all <- placeholder_gt()




  ### Jail Average Daily Population (ADP) Figures
  ## Public Jail Average Daily Population Figures
  plot_adp_multiproducer_estimate_year_total_county_gender <- placeholder_ggplot()
  plot_adp_multiproducer_estimate_year_total_county_race <- placeholder_ggplot()
  plot_adp_multiproducer_estimate_yoy_all_county <- placeholder_ggplot()

  ## External Jail Average Daily Population Figures
  plot_adp_multiproducer_mixmethod_year_total_county_gender <- placeholder_ggplot()




  ### Return list as the targets object
  named_list(
    ## Executive Summary
    # Public
    table_metrics_executive_summary,
    ## Jail Bookings
    # External
    plot_bookings_multiproducer_mixmethod_month_total,
    plot_bookings_multiproducer_estimate_month_total,
    # Public
    plot_bookings_multiproducer_estimate_year_total,
    plot_bookings_multiproducer_estimate_year_gender,
    table_bookings_multiproducer_estimate_month_race,
    plot_bookings_rate_multiproducer_estimate_month_race,
    plot_bookings_multiproducer_estimate_yoy_all_county,
    ## Jail Releases
    plot_releases_vera_admin_quarter_total,
    plot_releases_vera_admin_quarter_gender,
    plot_releases_vera_admin_quarter_race,
    plot_releases_vera_admin_quarter_yoy_county,
    ## Jail Average Daily Population (ADP)
    # External
    plot_adp_multiproducer_mixmethod_year_total_county_gender, # Only Tulsa & Oklahoma County
    # Public
    plot_adp_multiproducer_estimate_year_total_county_gender,
    plot_adp_multiproducer_estimate_year_total_county_race,
    plot_adp_multiproducer_estimate_yoy_all_county,
    ## Appendix Tables
    table_bookings_multiproducer_estimate_year_last_5_years_all,
    table_releases_vera_admin_year_last_5_years_all
  )
}

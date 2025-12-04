visualize_prison_figures <- function(analysis_results = prison_analysis_results) {

  plot_sentences_doc_admin_year_total_by_county <- placeholder_ggplot()
  plot_releases_vera_admin_year_total_by_county <- placeholder_ggplot()
  plot_population_doc_admin_yoy_by_gender_county <- placeholder_ggplot()
  plot_population_doc_admin_year_by_gender_county <- placeholder_ggplot()

  named_list(
    plot_sentences_doc_admin_year_total_by_county,
    plot_releases_vera_admin_year_total_by_county,
    plot_population_doc_admin_yoy_by_gender_county,
    plot_population_doc_admin_year_by_gender_county
  )
}

### Main analysis function
analyze_prison_metrics <- function(processed_data = prison_processed_data) {
  people_with_sentence_info <- processed_data$people_with_sentence_info

  sentences_doc_admin_year_total_by_county <- placeholder_tibble()
  releases_vera_admin_year_total_by_county <- placeholder_tibble()
  population_doc_admin_yoy_by_gender_county <- placeholder_tibble()
  population_doc_admin_year_by_gender_county <- placeholder_tibble()

  # Return named list as the targets object
  named_list(
    sentences_doc_admin_year_total_by_county,
    releases_vera_admin_year_total_by_county,
    population_doc_admin_yoy_by_gender_county,
    population_doc_admin_year_by_gender_county
  )
}

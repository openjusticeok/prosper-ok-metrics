filter_doc_population <- function(data,
                                  sentencing_county = NA_character_,
                                  physical_custody_only = TRUE,
                                  status_active_only = TRUE,
                                  exclude_interstate = TRUE,
                                  sentence_date = NA) {
  data |>
    dplyr::filter(
      is.na(sentencing_county) | .data$most_recent_sentencing_county == sentencing_county,
      !physical_custody_only | .data$physical_custody,
      !status_active_only | .data$status == "ACTIVE",
      !exclude_interstate | .data$facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
      is.na(sentence_date) |
        (!is.na(.data$most_recent_sentencing_date) & .data$most_recent_sentencing_date >= sentence_date)
    )
}

summarise_population_count <- function(data, group_vars = NULL) {
  if (is.null(group_vars)) {
    data |>
      dplyr::summarise(n_people = dplyr::n_distinct(.data$doc_num), .groups = "drop")
  } else {
    data |>
      dplyr::group_by(dplyr::across(dplyr::all_of(group_vars))) |>
      dplyr::summarise(n_people = dplyr::n_distinct(.data$doc_num), .groups = "drop")
  }
}

analyze_prison_metrics <- function(processed_data) {
  doc_population <- processed_data$doc_population
  baseline_date <- as.Date("2024-10-16")

  current_population <- dplyr::bind_rows(
    filter_doc_population(
      doc_population,
      physical_custody_only = TRUE,
      status_active_only = TRUE,
      exclude_interstate = TRUE
    ) |>
      summarise_population_count() |>
      dplyr::mutate(metric = "current_physical_active_excluding_interstate"),
    filter_doc_population(
      doc_population,
      physical_custody_only = TRUE,
      status_active_only = TRUE,
      exclude_interstate = FALSE
    ) |>
      summarise_population_count() |>
      dplyr::mutate(metric = "current_physical_active_including_interstate")
  ) |>
    dplyr::select("metric", "n_people")

  counties <- c("Tulsa", "Oklahoma")

  county_current <- dplyr::bind_rows(
    c(
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = TRUE,
            status_active_only = TRUE,
            exclude_interstate = TRUE
          ) |>
            summarise_population_count() |>
            dplyr::mutate(
              county = county,
              physical_custody_only = TRUE,
              since_date = as.Date(NA)
            )
        }
      ),
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = FALSE,
            status_active_only = TRUE,
            exclude_interstate = TRUE
          ) |>
            summarise_population_count() |>
            dplyr::mutate(
              county = county,
              physical_custody_only = FALSE,
              since_date = as.Date(NA)
            )
        }
      )
    )
  )

  county_since_baseline <- dplyr::bind_rows(
    c(
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = TRUE,
            status_active_only = TRUE,
            exclude_interstate = TRUE,
            sentence_date = baseline_date
          ) |>
            summarise_population_count() |>
            dplyr::mutate(
              county = county,
              physical_custody_only = TRUE,
              since_date = baseline_date
            )
        }
      ),
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = FALSE,
            status_active_only = TRUE,
            exclude_interstate = TRUE,
            sentence_date = baseline_date
          ) |>
            summarise_population_count() |>
            dplyr::mutate(
              county = county,
              physical_custody_only = FALSE,
              since_date = baseline_date
            )
        }
      )
    )
  )

  county_since_baseline_by_sex <- dplyr::bind_rows(
    c(
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = TRUE,
            status_active_only = TRUE,
            exclude_interstate = TRUE,
            sentence_date = baseline_date
          ) |>
            summarise_population_count("sex") |>
            dplyr::mutate(
              county = county,
              physical_custody_only = TRUE,
              since_date = baseline_date
            )
        }
      ),
      lapply(
        counties,
        function(county) {
          filter_doc_population(
            doc_population,
            sentencing_county = county,
            physical_custody_only = FALSE,
            status_active_only = TRUE,
            exclude_interstate = TRUE,
            sentence_date = baseline_date
          ) |>
            summarise_population_count("sex") |>
            dplyr::mutate(
              county = county,
              physical_custody_only = FALSE,
              since_date = baseline_date
            )
        }
      )
    )
  )

  list(
    doc_population = doc_population,
    current_population = current_population,
    county_current = county_current,
    county_since_2024_10_16 = county_since_baseline,
    county_since_2024_10_16_by_sex = county_since_baseline_by_sex,
    reference_date = baseline_date
  )
}

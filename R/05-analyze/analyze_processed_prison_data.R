### Main analysis function

analyze_processed_prison_data <- function(processed_data = prison_processed_data) {
  people_with_sentence_info <- processed_data$people_with_sentence_info |>
    as.data.frame()

  sentences_doc_admin_year_total_by_county <-
    people_with_sentence_info |>
    filter_doc_population(physical_custody_only = TRUE,
                          status_active_only = TRUE,
                          exclude_interstate = TRUE,
                          sentence_date = as.Date("2024-10-16")) |>
    summarise_population_count("Tulsa", "Oklahoma")

  releases_vera_admin_year_total_by_county <- placeholder_tibble()

  population_doc_admin_year_by_gender_county <-
    people_with_sentence_info |>
    filter_doc_population(physical_custody_only = TRUE,
                          status_active_only = TRUE,
                          exclude_interstate = TRUE) |>
    summarise_population_count("Tulsa", "Oklahoma", "sex")

  population_doc_admin_yoy_by_gender_county <- placeholder_tibble()


  # Return named list as the targets object
  named_list(
    sentences_doc_admin_year_total_by_county,
    releases_vera_admin_year_total_by_county,
    population_doc_admin_year_by_gender_county,
    population_doc_admin_yoy_by_gender_county
  )
}


#' This function subsets DOC-level records using a set of optional filters.
#'
#' @param data A dataframe containing DOC person-level records.
#' @param sentencing_county If supplied, limits results to individuals
#'   whose most recent sentencing occurred in this county.
#'   Defaults to `NA_character_` meaning no county filtering.
#' @param physical_custody_only If `TRUE` (default), only individuals
#'   currently in physical custody are included.
#'   If `FALSE`, all individuals are retained regardless of custody status.
#' @param status_active_only If `TRUE` (default), filters to individuals
#'  with `status == "ACTIVE"`.
#'  If `FALSE`, include all individuals regardless of status value.
#' @param exclude_interstate  If `TRUE` (default), excludes individuals
#'   assigned to the "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT".
#'   Setting this to `FALSE` retains all facilities.
#'   # IMPORTANT NOTE:
#'   When we don't filter out interstate compact, the total count balloons
#'   however, the DOC weekly count appears to include "ICC Out-of-State"...
#' @param sentence_date If supplied, limits results to individuals whose most
#' recent sentencing date is on or after this value.
#' Defaults to `NA_character_`, meaning no date-based filtering.
#'
#' @return A filtered tibble containing only rows meeting the specified criteria.
#' @examples
#' # Individuals sentenced in Tulsa since Oct 2024, all forms of custody:
#' data |>
#' filter_doc_population(
#'   sentencing_county = "Tulsa",
#'   physical_custody_only = FALSE,
#'   sentence_date = "2024-10-16")
#'

filter_doc_population <- function(data,
                                  sentencing_county = NA_character_,
                                  physical_custody_only = TRUE,
                                  status_active_only = TRUE,
                                  exclude_interstate = TRUE,
                                  sentence_date = NA_character_) {
  data |>
    filter(
      is.na(sentencing_county) |
        most_recent_sentencing_county == sentencing_county,

      !physical_custody_only | physical_custody == "TRUE",

      !status_active_only | status == "ACTIVE",

      !exclude_interstate |
        facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",

      is.na(sentence_date) |
        most_recent_sentencing_date >= sentence_date
    )
}


#' This function returns a count of unique individuals based on `doc_num`.
#' You can supply one or more grouping variables to break the
#' counts into sub-populations (e.g., by sex, race, county).
#'
#' @param data A dataframe containing DOC person-level records.
#' @param group_vars Optional parameters indicating variables to
#'   group results by (e.g., `sex`, `c(sex, race)`, or `repeat_offender`).
#'   Defaults to `NULL`, returning a single population count with no grouping.
#'
#' @return A tibble containing the count of unique individuals (`n_people`),
#'   optionally grouped by the supplied variables.
#'
#' @examples
#' # Count by sex and race:
#' summarise_population_count(data, c("sex", "race"))
#'
summarise_population_count <- function(filtered_data,
                                       county_1,
                                       county_2,
                                       group_vars = NULL) {

  if (is.null(group_vars)) {

    summary_data <- filtered_data |>
      summarise(
        county_1_tmp = n_distinct(doc_num[most_recent_sentencing_county == county_1]),
        county_2_tmp = n_distinct(doc_num[most_recent_sentencing_county == county_2]),
        total = n_distinct(doc_num),
        .groups = "drop"
      )

  } else {

    summary_data <- filtered_data |>
      group_by(across(all_of(group_vars))) |>
      summarise(
        county_1_tmp = n_distinct(doc_num[most_recent_sentencing_county == county_1]),
        county_2_tmp = n_distinct(doc_num[most_recent_sentencing_county == county_2]),
        total = n_distinct(doc_num),
        .groups = "drop"
      )
  }

  summary_data <- summary_data |>
    rename_with(~ county_1, "county_1_tmp") |>
    rename_with(~ county_2, "county_2_tmp")

  return(summary_data)
}


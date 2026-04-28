check_prison_ingested <- function(ingested_data = prison_ingested_data) {
  doc_ingested_data <- ingested_data$doc

  # WARNING: These are just placeholder checks. Real checks needed.
  pb_levels <- pointblank::action_levels(warn_at = 0.02, stop_at = 0.1)
  warn_only <- pointblank::action_levels(warn_at = 0.02, stop_at = Inf)

  profile_agent <- pointblank::create_agent(
    tbl = doc_ingested_data$profile_data,
    label = "DOC profile data",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(doc_num, facility, status, birth_date)) |>
    pointblank::col_vals_not_null(pointblank::vars(doc_num)) |>
    # Check that all doc_num's in the profile data appear in the sentence data
    # I.e. profile doc_num's ⊆ sentence doc_num's
    pointblank::col_vals_in_set(
      pointblank::vars(doc_num),
      set = unique(doc_ingested_data$sentence_data$doc_num)
    ) |>
    pointblank::interrogate()

  sentence_agent <- pointblank::create_agent(
    tbl = doc_ingested_data$sentence_data,
    label = "DOC sentence data",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(sentence_id, doc_num, sentencing_county, js_date)) |>
    pointblank::col_is_date(pointblank::vars(js_date), actions = warn_only) |>
    pointblank::col_vals_not_null(pointblank::vars(sentence_id)) |>
    pointblank::col_vals_not_null(pointblank::vars(doc_num)) |>
    pointblank::col_vals_not_null(pointblank::vars(sentencing_county)) |>
    # Check that all doc_num's in the sentence data appear in the profile data
    # I.e. sentence doc_num's ⊆ profile doc_num's
    pointblank::col_vals_in_set(
      pointblank::vars(doc_num),
      set = unique(doc_ingested_data$profile_data$doc_num)
    ) |>
    pointblank::interrogate()

  offense_agent <- pointblank::create_agent(
    tbl = doc_ingested_data$offense_data,
    label = "DOC offense data",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(statute_code, description)) |>
    pointblank::interrogate()

  summarize_agent <- function(agent, name) {
    status <- if (pointblank::all_passed(agent)) "pass" else "warning"
    tibble::tibble(
      check = name,
      status = status,
      failed = sum(rlang::`%||%`(agent$validation_set$n_failed, 0))
    )
  }

  tibble::lst(
    checks = dplyr::bind_rows(
      summarize_agent(profile_agent, "profile_data"),
      summarize_agent(sentence_agent, "sentences_data"),
      summarize_agent(offense_agent, "offense_data")
    )
  )
}

#' Check ingested GKFF prison data from GCS
#'
#' Runs pointblank validation agents on each table from the GKFF prison data
#' request: sentences, receptions, releases, profile, offense, and alias.
#'
#' @param ingested_data List of ingested GKFF data tables as returned by
#'   [ingest_gkff_prison_data()].
#' @param snapshot_date Date string for bound checks on date columns.
#'
#' @return A named list containing:
#'   * `data_scans`: Tibble of OVMS data scan summaries for each table
#'   * `agents`: List of pointblank agent objects for detailed inspection
#' @export
check_gkff_prison_ingested <- function(ingested_data, snapshot_date = "2026-03-13") {
  snapshot_date <- lubridate::as_date(snapshot_date)

  sentences_data <- ingested_data$sentences_data |>
    dplyr::mutate(
      js_date = lubridate::ymd(js_date),
      sentence_start_date = lubridate::ymd(sentence_start_date),
      sentence_end_date = lubridate::ymd(sentence_end_date)
    )

  receptions_data <- ingested_data$receptions_data |>
    dplyr::mutate(
      movement_date = lubridate::ymd(movement_date)
    )

  releases_data <- ingested_data$releases_data |>
    dplyr::mutate(
      movement_date = lubridate::ymd(movement_date)
    )

  profile_data <- ingested_data$profile_data |>
    dplyr::mutate(
      birth_date = lubridate::ymd(birth_date),
      admit_date = lubridate::ymd(admit_date),
      release_date = lubridate::ymd(release_date)
    )
  offense_data <- ingested_data$offense_data
  alias_data <- ingested_data$alias_data

  wf <- pointblank::warn_on_fail()

  sentences_agent <- pointblank::create_agent(
    tbl = sentences_data,
    tbl_name = "ODOC Sentences",
    label = "Sentences Data"
  ) |>
    pointblank::col_exists(
      pointblank::vars(sentence_id, doc_num, sentencing_county, js_date),
      actions = wf
    ) |>
    pointblank::col_is_date(pointblank::vars(js_date)) |>
    pointblank::col_vals_not_null(pointblank::vars(sentence_id)) |>
    pointblank::col_vals_not_null(pointblank::vars(doc_num)) |>
    pointblank::col_vals_not_null(pointblank::vars(sentencing_county)) |>
    pointblank::col_vals_in_set(
      pointblank::vars(doc_num),
      set = unique(profile_data$doc_num)
    ) |>
    pointblank::col_vals_not_null(js_date) |>
    pointblank::col_vals_not_null(sentence_start_date) |>
    pointblank::col_vals_not_null(sentence_end_date) |>
    pointblank::col_vals_lte(js_date, snapshot_date, na_pass = TRUE) |>
    pointblank::col_vals_lte(sentence_start_date, snapshot_date, na_pass = TRUE) |>
    pointblank::col_vals_lte(sentence_end_date, snapshot_date, na_pass = TRUE) |>
    pointblank::interrogate()

  receptions_agent <- receptions_data |>
    dplyr::mutate(
      movement_date_year = lubridate::year(movement_date)
    ) |>
    pointblank::create_agent(
      tbl_name = "ODOC Receptions",
      label = "Receptions Data"
    ) |>
    pointblank::col_exists(doc_num, actions = wf) |>
    pointblank::col_exists(movement_date, actions = wf) |>
    pointblank::col_exists(reason, actions = wf) |>
    pointblank::col_exists(facility, actions = wf) |>
    pointblank::col_vals_expr(
      expr = ~ movement_date_year %in% c(2024, 2025),
      label = "All movement data is in the specified years",
      actions = wf
    ) |>
    pointblank::col_vals_equal(
      columns = movement_date_year,
      value = 2024,
      actions = pointblank::action_levels(warn_at = 1.0)
    ) |>
    pointblank::col_vals_equal(
      columns = movement_date_year,
      value = 2025,
      actions = pointblank::action_levels(warn_at = 1.0)
    ) |>
    pointblank::col_vals_in_set(
      columns = facility,
      label = "Receptions are all to standard reception facilities",
      set = c(
        "Lexington Assessment And Reception Center",
        "Mabel Bassett Assessment & Reception Center"
      )
    ) |>
    pointblank::col_vals_not_null(doc_num) |>
    pointblank::col_vals_not_null(movement_date) |>
    pointblank::col_vals_not_null(reason) |>
    pointblank::col_vals_not_null(facility) |>
    pointblank::interrogate()

  releases_agent <- releases_data |>
    dplyr::mutate(
      movement_date_year = lubridate::year(movement_date)
    ) |>
    pointblank::create_agent(
      tbl_name = "ODOC Releases",
      label = "Releases Data"
    ) |>
    pointblank::col_exists(doc_num, actions = wf) |>
    pointblank::col_exists(movement_date, actions = wf) |>
    pointblank::col_exists(reason, actions = wf) |>
    pointblank::col_exists(facility, actions = wf) |>
    pointblank::col_vals_expr(
      expr = ~ movement_date_year %in% c(2024, 2025),
      label = "All movement data is in the specified years",
      actions = wf
    ) |>
    pointblank::col_vals_equal(
      columns = movement_date_year,
      value = 2024,
      actions = pointblank::action_levels(warn_at = 1.0)
    ) |>
    pointblank::col_vals_equal(
      columns = movement_date_year,
      value = 2025,
      actions = pointblank::action_levels(warn_at = 1.0)
    ) |>
    pointblank::col_vals_not_null(doc_num) |>
    pointblank::col_vals_not_null(movement_date) |>
    pointblank::col_vals_not_null(reason) |>
    pointblank::col_vals_not_null(facility) |>
    pointblank::interrogate()

  profile_agent <- profile_data |>
    pointblank::create_agent(
      tbl_name = "ODOC Inmate Profile",
      label = "Profile Data"
    ) |>
    pointblank::col_vals_not_null(doc_num) |>
    pointblank::col_vals_not_null(first_name) |>
    pointblank::col_vals_not_null(last_name) |>
    pointblank::col_vals_not_null(middle_name) |>
    pointblank::col_vals_not_null(suffix) |>
    pointblank::col_vals_not_null(birth_date) |>
    pointblank::col_vals_not_null(sex) |>
    pointblank::col_vals_not_null(race) |>
    pointblank::col_vals_not_null(facility) |>
    pointblank::col_vals_not_null(status) |>
    pointblank::col_vals_not_null(admit_date) |>
    pointblank::col_vals_not_null(release_date) |>
    pointblank::col_vals_not_equal(status, "ACTIVE") |>
    pointblank::col_vals_not_equal(race, "Not Reported") |>
    pointblank::col_vals_not_equal(sex, "Not Reported") |>
    pointblank::col_vals_lte(birth_date, snapshot_date) |>
    pointblank::interrogate()

  summarize_agent <- function(agent, name) {
    status <- if (pointblank::all_passed(agent)) "pass" else "warning"
    tibble::tibble(
      check = name,
      status = status,
      failed = sum(rlang::`%||%`(agent$validation_set$n_failed, 0))
    )
  }

  tibble::lst(
    checks = dplyr::bind_rows(
      summarize_agent(sentences_agent, "sentences_data"),
      summarize_agent(receptions_agent, "receptions_data"),
      summarize_agent(releases_agent, "releases_data"),
      summarize_agent(profile_agent, "profile_data")
    ),
    agents = list(
      sentences = sentences_agent,
      receptions = receptions_agent,
      releases = releases_agent,
      profile = profile_agent
    )
  )
}

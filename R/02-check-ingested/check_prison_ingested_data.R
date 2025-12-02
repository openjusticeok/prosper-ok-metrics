check_prison_ingested <- function(ingested_data = prison_ingested_data) {
  # WARNING: These are just placeholder checks. Real checks needed.
  pb_levels <- pointblank::action_levels(warn_at = 0.02, stop_at = 0.1)
  warn_only <- pointblank::action_levels(warn_at = 0.02, stop_at = Inf)

  profile_agent <- pointblank::create_agent(
    tbl = ingested_data$profile_data,
    label = "DOC profile data",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(doc_num, facility, status, birth_date)) |>
    pointblank::col_vals_not_null(pointblank::vars(doc_num)) |>
    pointblank::interrogate()

  sentence_agent <- pointblank::create_agent(
    tbl = ingested_data$sentence_data,
    label = "DOC sentence data",
    actions = pb_levels
  ) |>
    pointblank::col_exists(pointblank::vars(sentence_id, doc_num, sentencing_county, js_date)) |>
    pointblank::col_is_date(pointblank::vars(js_date), actions = warn_only) |>
    pointblank::interrogate()

  offense_agent <- pointblank::create_agent(
    tbl = ingested_data$offense_data,
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
    ingested_data = ingested_data,
    checks = dplyr::bind_rows(
      summarize_agent(profile_agent, "profile_data"),
      summarize_agent(sentence_agent, "sentence_data"),
      summarize_agent(offense_agent, "offense_data")
    )
  )
}

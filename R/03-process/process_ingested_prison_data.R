process_ingested_prison_data <- function(ingested_data = prison_ingested_data) {
  ingested_data <- ingested_data$prison_ingested_data

  profile_data <- ingested_data$profile_data
  offense_data <- ingested_data$offense_data
  sentence_data <- ingested_data$sentence_data
  consecutive_data <- ingested_data$consecutive_data

  assessment_and_reception <- c(
    "LEXINGTON ASSESSMENT AND RECEPTION CENTER",
    "MABEL BASSETT ASSESSMENT & RECEPTION CENTER"
  )

  state_institutions <- c(
    "ALLEN GAMBLE CORRECTIONAL CENTER",
    "CHARLES E. (BILL) JOHNSON CORRECTIONAL CENTER",
    "DICK CONNER CORRECTIONAL CENTER",
    "DR. EDDIE WARRIOR CORRECTIONAL CENTER (MIN)",
    "GREAT PLAINS CORRECTIONAL CENTER",
    "HOWARD MCLEOD CORRECTIONAL CENTER",
    "JACKIE BRANNON CORRECTIONAL CENTER",
    "JAMES CRABTREE CORRECTIONAL CENTER",
    "JESS DUNN CORRECTIONAL CENTER",
    "JIM E. HAMILTON CORRECTIONAL CENTER",
    "JOHN H. LILLEY CORRECTIONAL CENTER",
    "JOSEPH HARP CORRECTIONAL CENTER",
    "LEXINGTON CORRECTIONAL CENTER",
    "MABEL BASSETT CORRECTIONAL CENTER",
    "MACK ALFORD CORRECTIONAL CENTER",
    "OKLAHOMA STATE PENITENTIARY",
    "OKLAHOMA STATE REFORMATORY",
    "RED ROCK CORRECTIONAL CENTER" # previously LAWTON CORRECTIONAL AND REHABILITATION FACILITY
  )

  private_facilities <- c("LAWTON CORRECTIONAL AND REHABILITATION FACILITY")

  community_centers <- c(
    "CLARA WATERS COMMUNITY CORRECTIONS CENTER",
    "DR. EDDIE WARRIOR CORRECTIONAL CENTER (CCC)",
    "ENID COMMUNITY CORRECTIONS CENTER",
    "LAWTON COMMUNITY CORRECTIONS CENTER",
    "NORTHEAST OKLAHOMA COMMUNITY CORRECTIONS CENTER",
    "OKLAHOMA CITY COMMUNITY CORRECTIONS CENTER",
    "UNION CITY COMMUNITY CORRECTIONS CENTER"
  )

  halfway_house <- c("BRIDGEWAY HALFWAY HOUSE")

  interstate <- c("INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT")

  clean_profile_data <- profile_data |>
    dplyr::mutate(
      # TODO: Reconsider name formatting for consistency with other datasets
      name = stringr::str_c(
        dplyr::if_else(!is.na(.data$last_name), stringr::str_c(" ", .data$last_name), ""),
        dplyr::if_else(!is.na(.data$suffix), stringr::str_c(" ", .data$suffix), ""),
        ", ",
        dplyr::if_else(!is.na(.data$first_name), stringr::str_c(" ", .data$first_name), ""),
        " ",
        dplyr::if_else(!is.na(.data$middle_name), stringr::str_c(" ", .data$middle_name), ""),
        "."
      ),
      # TODO: Should be from the time of the snapshot, not the current date.
      # TODO: I need to add a snapshot date to the ingested data.
      age = floor(lubridate::time_length(lubridate::interval(.data$birth_date, Sys.Date()), "years")),
      age_bucket = dplyr::case_when(
        .data$age < 18 ~ "Under 18",
        .data$age >= 18 & .data$age < 25 ~ "18-24",
        .data$age >= 25 & .data$age < 35 ~ "25-34",
        .data$age >= 35 & .data$age < 45 ~ "35-44",
        .data$age >= 45 & .data$age < 55 ~ "45-54",
        .data$age >= 55 & .data$age < 65 ~ "55-64",
        .data$age >= 65 ~ "65 and older",
        TRUE ~ "Unknown"
      ),
      facility_upper = stringr::str_to_upper(.data$facility),
      physical_custody = .data$facility_upper %in% c(
        assessment_and_reception,
        state_institutions,
        community_centers,
        halfway_house,
        private_facilities,
        interstate
      ) |
        stringr::str_detect(.data$facility_upper, "(?i)jail|SHERIFFS OFFICE")
    ) |>
    dplyr::select(-c("facility_upper"))

  sentence_data <- sentence_data |>
    dplyr::mutate(js_date = lubridate::as_date(.data$js_date))

  doc_repeat <- sentence_data |>
    dplyr::group_by(.data$doc_num) |>
    dplyr::summarise(
      num_sentencing_dates = dplyr::n_distinct(.data$js_date, na.rm = TRUE),
      repeat_offender = .data$num_sentencing_dates > 1,
      .groups = "drop"
    )

  people_with_sentence_info <- sentence_data |>
    dplyr::left_join(consecutive_data, by = "sentence_id") |>
    dplyr::left_join(doc_repeat, by = "doc_num") |>
    dplyr::left_join(
      offense_data |>
        dplyr::distinct(.data$statute_code, .keep_all = TRUE),
      by = "statute_code"
    ) |>
    dplyr::rename(
      offense_description = "description",
      sentencing_date = "js_date"
    ) |>
    dplyr::mutate(
      life_sentence = .data$incarcerated_term_in_years == "7777",
      lwop = .data$incarcerated_term_in_years == "8888",
      dp = .data$incarcerated_term_in_years == "9999"
    ) |>
    dplyr::group_by(.data$doc_num) |>
    dplyr::arrange(.data$doc_num, .data$sentencing_date, .data$sentence_id, .data$statute_code, .by_group = TRUE) |>
    dplyr::mutate(
      most_recent_sentencing_date = dplyr::if_else(
        all(is.na(.data$sentencing_date)),
        as.Date(NA),
        max(.data$sentencing_date, na.rm = TRUE)
      ),
      most_recent_sentencing_county = .data$sentencing_county[.data$sentencing_date == .data$most_recent_sentencing_date][1]
    ) |>
    # Nested offenses per doc_num, so each row is one individual
    tidyr::nest(
      offenses = c(
        "statute_code",
        "offense_description",
        "sentence_id",
        "consecutive_to_id",
        "sentencing_county",
        "sentencing_date",
        "crf_number",
        "incarcerated_term_in_years",
        "probation_term_in_years",
        "violent",
        "num_sentencing_dates",
        "life_sentence",
        "lwop",
        "dp"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(clean_profile_data, by = "doc_num")

  list(
    people_with_sentence_info = people_with_sentence_info
  )
}

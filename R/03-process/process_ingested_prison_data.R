process_ingested_prison_data <- function(ingested_data = prison_ingested_data) {
  doc_ingested_data <- ingested_data$doc

  available_extracts <- doc_ingested_data$available_extracts
  profile_data <- doc_ingested_data$profile_data
  offense_data <- doc_ingested_data$offense_data
  sentence_data <- doc_ingested_data$sentence_data
  consecutive_data <- doc_ingested_data$consecutive_data

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

  profile_data <- profile_data |>
    # Construct full name variable
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
      # Construct snapshot_age, current_age, and age_bucket variables based on birth_date
      snapshot_age = floor(lubridate::time_length(lubridate::interval(.data$birth_date, as.Date(.data$snapshot_date)), "years")),
      current_age = floor(lubridate::time_length(lubridate::interval(.data$birth_date, Sys.Date()), "years")),
      current_age_date = today(), # Day the current age was calculated, which can be useful for tracking when age-based analyses were conducted
      snapshot_age_bucket = dplyr::case_when(
        .data$snapshot_age < 18 ~ "Under 18",
        .data$snapshot_age >= 18 & .data$snapshot_age < 25 ~ "18-24",
        .data$snapshot_age >= 25 & .data$snapshot_age < 35 ~ "25-34",
        .data$snapshot_age >= 35 & .data$snapshot_age < 45 ~ "35-44",
        .data$snapshot_age >= 45 & .data$snapshot_age < 55 ~ "45-54",
        .data$snapshot_age >= 55 & .data$snapshot_age < 65 ~ "55-64",
        .data$snapshot_age >= 65 ~ "65 and older",
        TRUE ~ "Unknown"
      ),
      current_age_bucket = dplyr::case_when(
        .data$current_age < 18 ~ "Under 18",
        .data$current_age >= 18 & .data$current_age < 25 ~ "18-24",
        .data$current_age >= 25 & .data$current_age < 35 ~ "25-34",
        .data$current_age >= 35 & .data$current_age < 45 ~ "35-44",
        .data$current_age >= 45 & .data$current_age < 55 ~ "45-54",
        .data$current_age >= 55 & .data$current_age < 65 ~ "55-64",
        .data$current_age >= 65 ~ "65 and older",
        TRUE ~ "Unknown"
      ),
      # Determine physical custody based on facility, and whehter it's of a
      # certain type
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

  offense_data <- offense_data |>
    dplyr::rename(
      offense_description = "description"
    )

  sentence_data <- sentence_data |>
    dplyr::rename(
      sentencing_date = "js_date"
    ) |>
    dplyr::mutate(
      sentencing_date = lubridate::ymd(.data$sentencing_date),
      incarcerated_term_in_years = as.numeric(.data$incarcerated_term_in_years),
      probation_term_in_years = as.numeric(.data$probation_term_in_years)
    )

  # Identify repeat offenders based on number of distinct sentencing dates
  # Determine if a person has multiple distinct sentencing dates, which is a proxy for repeat offenders.
  # This is not a perfect measure, as some people may have multiple sentences on
  # aa similar day, and some repeat offenders may only have one sentencing date
  # in the data. However, it provides a useful starting point for identifying
  # potential repeat offenders.
  doc_repeat <- sentence_data |>
    dplyr::summarise(
      n_sentences = dplyr::n(),
      n_sentencing_dates = dplyr::n_distinct(.data$sentencing_date, na.rm = TRUE),
      repeat_offender = .data$n_sentencing_dates > 1,
      .by = c("doc_num")
    )


  ### Joins
  profile_data <- profile_data |>
    dplyr::left_join(doc_repeat, by = c("doc_num")) |>
    dplyr::relocate("snapshot_date", .after = -1)

  # TODO: Join data where a unique sentence is the unit of analysis
  sentence_with_profile_offense_data <- sentence_data |> # Each row is a unique sentence
    dplyr::left_join(
      offense_data |>
        # the changes in offense data itself.
        # We usually want the latest offense data unless we are analyzing
        filter(snapshot_date == max(snapshot_date)) |>
        # There are ~2 offense which are not unique. They have a row where
        # violent is "N" for not violent and "C" for circumstancial. See
        # documentation for more details.
        # For now keep the first row, which is "C".
        distinct(statute_code, .keep_all = TRUE) |>
        select(-c("snapshot_date")),
      by = c("statute_code")
    ) |>
    # Add booleans for life sentence, life without parole, and death penalty
    dplyr::mutate(
      life_sentence = .data$incarcerated_term_in_years == "7777",
      life_without_parole = .data$incarcerated_term_in_years == "8888",
      death_penalty = .data$incarcerated_term_in_years == "9999"
    ) |>
    # Arrange for easier visual skimming
    dplyr::arrange(
      .data$doc_num,
      .data$sentencing_date,
      .data$sentence_id,
      .data$statute_code
    ) |>
    # Join profile data to sentence data to get demographic and other profile information for each sentence
    # This is a many-to-one join because there are multiple sentences per
    # profile (via doc_num), and doc_num is unique per snapshot_date in the profile data.
    dplyr::left_join(
      profile_data,
      by = c("doc_num", "snapshot_date")
    )

  list(
    profile_data = profile_data,
    sentence_with_profile_offense_data = sentence_with_profile_offense_data
  )
}

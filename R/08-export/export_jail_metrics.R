export_jail_metrics <- function(processed_data = jail_processed_data) {
  # Create output directory if needed
  output_dir <- here::here("data", "output", "tulsa-county-jail")
  fs::dir_create(output_dir, recurse = TRUE)

  # Get the Tulsa County Jail bookings data
  tcj_bookings <- processed_data$tulsa_county_jail$bookings

  # Get the Tulsa County Jail charges data
  tcj_charges <- processed_data$tulsa_county_jail$charges


  ### ADP Calculations
  # Calculate daily population using count_interval
  tcj_daily_pop <- ojoutils::count_interval(
    data = tcj_bookings,
    start = "booking_date",
    end = "release_date",
    period = "day"
  )

  # Annual ADP - overall
  adp_by_year <- tcj_daily_pop |>
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::summarise(
      avg_daily_population = mean(n, na.rm = TRUE),
      .by = year
    ) |>
    dplyr::arrange(year) |>
    dplyr::mutate(
      yoy_change = (avg_daily_population - dplyr::lag(avg_daily_population)) / dplyr::lag(avg_daily_population)
    )

  readr::write_csv(adp_by_year, fs::path(output_dir, "adp_by_year.csv"))

  # Annual ADP by gender
  tcj_daily_pop_gender <- ojoutils::count_interval(
    data = tcj_bookings,
    start = "booking_date",
    end = "release_date",
    period = "day",
    .by = "sex_gender_standardized"
  )

  adp_by_year_gender <- tcj_daily_pop_gender |>
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::summarise(
      avg_daily_population = mean(n, na.rm = TRUE),
      .by = c(year, sex_gender_standardized)
    ) |>
    dplyr::arrange(year, sex_gender_standardized) |>
    dplyr::group_by(sex_gender_standardized) |>
    dplyr::mutate(
      yoy_change = (avg_daily_population - dplyr::lag(avg_daily_population)) / dplyr::lag(avg_daily_population)
    ) |>
    dplyr::ungroup()

  readr::write_csv(adp_by_year_gender, fs::path(output_dir, "adp_by_year_gender.csv"))

  # Annual ADP by race/ethnicity
  tcj_daily_pop_race <- ojoutils::count_interval(
    data = tcj_bookings,
    start = "booking_date",
    end = "release_date",
    period = "day",
    .by = "race_ethnicity_standardized"
  )

  adp_by_year_race <- tcj_daily_pop_race |>
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::summarise(
      avg_daily_population = mean(n, na.rm = TRUE),
      .by = c(year, race_ethnicity_standardized)
    ) |>
    dplyr::arrange(year, race_ethnicity_standardized) |>
    dplyr::group_by(race_ethnicity_standardized) |>
    dplyr::mutate(
      yoy_change = (avg_daily_population - dplyr::lag(avg_daily_population)) / dplyr::lag(avg_daily_population)
    ) |>
    dplyr::ungroup()

  readr::write_csv(adp_by_year_race, fs::path(output_dir, "adp_by_year_race.csv"))


  ### Bookings
  bookings_by_year <- tcj_bookings |>
    dplyr::mutate(year = lubridate::year(booking_date)) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::count(year, name = "bookings") |>
    dplyr::arrange(year) |>
    dplyr::mutate(
      yoy_change = (bookings - dplyr::lag(bookings)) / dplyr::lag(bookings)
    )

  readr::write_csv(bookings_by_year, fs::path(output_dir, "bookings_by_year.csv"))


  ### Releases
  releases_by_year <- tcj_bookings |>
    dplyr::filter(!is.na(release_date)) |>
    dplyr::mutate(year = lubridate::year(release_date)) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::count(year, name = "releases") |>
    dplyr::arrange(year) |>
    dplyr::mutate(
      yoy_change = (releases - dplyr::lag(releases)) / dplyr::lag(releases)
    )

  readr::write_csv(releases_by_year, fs::path(output_dir, "releases_by_year.csv"))


  ### Release Dispositions
  # Categorize disposition types into logical groups
  # Include ALL charges, even those without disposition dates (pending)
  tcj_charges_categorized <- tcj_charges |>
    dplyr::mutate(
      # Use disposition_date year if available, otherwise booking_date year
      year = dplyr::coalesce(
        lubridate::year(charge_disposition_date),
        lubridate::year(booking_date)
      )
    ) |>
    dplyr::filter(year %in% 2023:2025) |>
    dplyr::mutate(
      disposition_category = dplyr::case_when(
        # Pending Resolution - no disposition date yet (case ongoing)
        is.na(charge_disposition_date) ~ "Pending Resolution",
        # Not Specified - has disposition date but type is "N/A" string
        disposition_type == "N/A" ~ "Not Specified",
        # Bond Release
        stringr::str_detect(disposition_type, "Bond|115 - Bond") ~ "Bond Release",
        # Released on Recognizance
        stringr::str_detect(disposition_type, "Recognizance|906 - Own|004 - Electronic") ~ "Released on Recognizance",
        # Dismissed/Declined
        stringr::str_detect(disposition_type, "Dismissed|Declined|Not Filed|No PC|Acquitted|918 - Dismissed") ~ "Dismissed/Declined",
        # DOC/Prison Transfer
        stringr::str_detect(disposition_type, "610 - DOC|Chain Pull|Return to DOC|DOC Sentenced|915 - Can Not Sign") ~ "DOC/Prison Transfer",
        # Sentence Served (including Sentenced to Jail)
        stringr::str_detect(disposition_type, "Sentence Served|Weekend Served|Sanction Served|SNT - Sentenced") ~ "Sentence Served",
        # Sentence Suspended/Deferred
        stringr::str_detect(disposition_type, "Suspended|Deferred|Time Pay|Fine") ~ "Sentence Suspended/Deferred",
        # Transfer to Other Jurisdiction
        stringr::str_detect(disposition_type, "County|Agency|ICE|USM|OJA|JBDC|State Agency|Out of State") ~ "Transfer to Other Jurisdiction",
        # Pending/Continued
        stringr::str_detect(disposition_type, "PreTrial|Pre-|Hold|Review|Pre-Signed") ~ "Pending/Continued",
        # Administrative/Cleared
        stringr::str_detect(disposition_type, "Wrong|Duplicated|Admin|Waiver|Warrant Recalled|Court Order|Signed") ~ "Administrative/Cleared",
        # Medical/Death
        stringr::str_detect(disposition_type, "Medical|Deceased") ~ "Medical/Death",
        # Other - truly miscellaneous dispositions
        TRUE ~ "Other"
      )
    )

  release_dispositions_by_year <- tcj_charges_categorized |>
    dplyr::count(year, disposition_category, name = "count") |>
    dplyr::arrange(year, disposition_category) |>
    dplyr::group_by(disposition_category) |>
    dplyr::mutate(
      yoy_change = (count - dplyr::lag(count)) / dplyr::lag(count)
    ) |>
    dplyr::ungroup()

  readr::write_csv(release_dispositions_by_year, fs::path(output_dir, "release_dispositions_by_year.csv"))


  ### Return file paths
  list(
    adp_by_year = fs::path(output_dir, "adp_by_year.csv"),
    adp_by_year_gender = fs::path(output_dir, "adp_by_year_gender.csv"),
    adp_by_year_race = fs::path(output_dir, "adp_by_year_race.csv"),
    bookings_by_year = fs::path(output_dir, "bookings_by_year.csv"),
    releases_by_year = fs::path(output_dir, "releases_by_year.csv"),
    release_dispositions_by_year = fs::path(output_dir, "release_dispositions_by_year.csv")
  )
}

process_ingested_jail_data <- function(ingested_checks = jail_ingested_checks) {
  ingested_data <- ingested_checks$ingested_data

  # TODO: Pull out some helper functions for repeated processing tasks
  # E.g., parsing date times, normalizing names, etc.
  # Only do this if it improves readability and confidence in uniform processing

  #' Normalize Jail Data Initiative inputs before merging with other sources
  #'
  #' The helper functions below enforce consistent schemas, fill in missing
  #' columns, and coerce date/time fields from the Jail Data Initiative CSVs.
  #'
  #' We clean here instead of downstream so that future joins can treat JDI rows
  #' the same as Asemio or OK Policy scrapes.

  #' Parsing defensively (e.g., tolerating missing columns, parsing timestamps
  #' with quiet lubridate calls) prevents the entire pipeline from crashing when
  #' the shared Drive exports tweak column sets or formats. That lets analysts
  #' drop in the latest CSVs without code edits while keeping downstream
  #' pointblank checks meaningful.
  jail_data_initiative_people <- ingested_data$jail_data_initiative$people |>
    janitor::clean_names() |>
    dplyr::mutate(
      source = "Jail Data Initiative",
      age = as.integer(.data$age)
    )
  jail_data_initiative_charges <- ingested_data$jail_data_initiative$charges |>
    janitor::clean_names() |>
    dplyr::mutate(
      source = "Jail Data Initiative"
    )

  # Asemio Scraper Data Processing
  asemio_scraped_bookings <- ingested_data$asemio$bookings |>
    dplyr::rename(
      jacket_number = dlm,
      arresting_agency = arrested_by,
      zip_code = postal_code
    ) |>
    dplyr::mutate(
      source = "Asemio Scraper",
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at),
      jacket_number = as.character(.data$jacket_number)
    )

  asemio_scraped_inmates <- ingested_data$asemio$inmates |>
    dplyr::mutate(
      source = "Asemio Scraper",
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at)
    ) |>
    dplyr::rename(
      jacket_number = dlm,
      given_name = first_name,
      surname = last_name,
    ) |>
    # TODO: Decide how to normalize asemio & okpolicy name columns
    # asemio data has no middle name
    # Create full name column
    dplyr::mutate(
      jacket_number = as.character(.data$jacket_number),
      full_name = paste(.data$given_name, .data$surname)
    )


  asemio_scraped_charges <- ingested_data$asemio$charges |>
    dplyr::mutate(
      source = "Asemio Scraper",
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at)
    ) |>
    dplyr::rename(
      jacket_number = dlm,
      charge_description = description
    ) |>
    dplyr::mutate(
      jacket_number = as.character(.data$jacket_number),
    )


  # OK Policy Scraper Data Processing
  okpolicy_scraped_bookings <- ingested_data$okpolicy$bookings |>
    dplyr::mutate(
      full_name = paste(.data$given_name, .data$middle_name, .data$surname),
      birth_date = lubridate::mdy(.data$birth_date),
      # Booking date is really booking datetime
      booking_datetime = lubridate::ymd_hms(
        .data$booking_date,
        # TODO: We actually need to confirm the timezone of the booking date
        tz = "America/Chicago",
        quiet = TRUE
      ),
      booking_date = as.Date(.data$booking_datetime),
      source = "OK Policy Scraper",
      gender = stringr::str_to_title(.data$gender),
      race = stringr::str_to_title(.data$race),
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at)
    )

  okpolicy_scraped_charges <- ingested_data$okpolicy$charges |>
    dplyr::mutate(
      bail_set_datetime = lubridate::ymd_hms(
        # TODO: We actually need to confirm the timezone of the booking date
        .data$bail_set_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      bail_set_date = as.Date(.data$bail_set_datetime),
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at)
    ) |>
    dplyr::rename(
      charge = statute_title,
      charge_description = statute_description,
      arresting_agency_abbreviation = charging_agency_abbreviation
    )


  ## Joining data

  # Join inmate info to bookings for Asemio data
  asemio_scraped_bookings_with_inmate_info <- asemio_scraped_bookings |>
    dplyr::left_join(
      asemio_scraped_inmates,
      by = "jacket_number",
      suffix = c("", "_inmate")
    )

  # Join booking info to charges for OK Policy data
  # This is needed to filter bookings with certain hold charges
  okpolicy_scraped_charges_with_bookings_info <- okpolicy_scraped_bookings |>
    dplyr::left_join(
      okpolicy_scraped_charges,
      by = c("id" = "booking_id"),
      suffix = c("", "_charge")
    )


  # Filter out hold charges unless for City of Tulsa or Electronic Monitoring Required
  # TODO: Finer replacement with what the charge actually is rather than
  # the hold category. I.e., Merging typos rather than "Tribal Hold"
  # Hold Categories
  okpolicy_scraped_charges_with_bookings_info <- okpolicy_scraped_charges_with_bookings_info |>
    dplyr::mutate(
      hold_category = dplyr::case_when(
        # Conditional Release Hold,
        # Reason for Hold: Specific conditions (e.g., electronic monitoring) must be met before release.
        # Release Condition: Compliance with set conditions required for release.
        stringr::str_detect(charge_description, "ELECTRONIC MONITOR") ~ "Conditional Release Hold",

        # Tribal Hold
        # Reason for Hold: A tribal authority has placed a hold.
        # Release Condition: Release controlled by tribal authority, not jail.
        stringr::str_detect(charge_description, "MUSCOGEE|MUSKOGEE|CHEROKEE|TRIBAL") ~ "Tribal Hold",

        # Federal Hold (Non-Immigration)
        # Reason for Hold: Federal (non-immigration) authority has placed a hold.
        # Release Condition: Federal (non-immigration) authority dictates release/transfer.
        stringr::str_detect(charge_description, "U\\.S\\. MARSHAL|US MARSHAL|US MARSHALL|FORM-41|DETainer") ~ "Federal Hold (Non-Immigration)",
        stringr::str_detect(charge_description, "\\bATF\\b|\\bFBI\\b|\\bDEA\\b|FEDERAL AGENCY") ~ "Federal Hold (Non-Immigration)",

        # Immigration Hold
        # Reason for Hold: Immigration authorities have placed a hold.
        # Release Condition: Federal authority dictates release/transfer.
        stringr::str_detect(charge_description, "HOLD/ICE") ~ "Immigration Hold",

        # Oklahoma Municipal Hold
        # Reason for Hold: An Oklahoma municipal authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the municipal authority.
        stringr::str_detect(charge_description, "CITY OF TULSA") ~ "Oklahoma Municipal Hold",

        # Hold for Another Oklahoma County
        # Reason for Hold: Another Oklahoma county authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the other county.
        stringr::str_detect(charge_description, "COUNTY|CO\\.") & stringr::str_detect(charge_description, "OK\\b|OKLAHOMA|\\(OK\\)") ~ "Hold for Another Oklahoma County",
        stringr::str_detect(charge_description, "ANOTHER OK COUNTY|OTHER COUNTY, OK") ~ "Hold for Another Oklahoma County",

        # State of Oklahoma Hold
        # Reason for Hold: An Oklahoma state authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the state authority.
        stringr::str_detect(charge_description, "RETURN TO DOC|ALLEN GAMBLE") ~ "State of Oklahoma Hold",
        stringr::str_detect(charge_description, "^HOLD/DOC\\b") ~ "State of Oklahoma Hold",

        # Hold for Another State's Jurisdiction/Agency
        # Reason for Hold: Another state's authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the other state.
        stringr::str_detect(charge_description, "ANOTHER STATE") ~ "Hold for Another State's Jurisdiction/Agency",
        stringr::str_detect(charge_description, "COUNTY|CO\\.") & stringr::str_detect(charge_description, "TX|TEXAS|MO|MISSOURI|OHIO|AR\\b|ARKANSAS|UTAH") ~ "Hold for Another State's Jurisdiction/Agency",
        stringr::str_detect(charge_description, "DEPARTMENT OF CORRECTIONS P&P") ~ "Hold for Another State's Jurisdiction/Agency",

        # Probation/Parole Hold
        # Reason for Hold: Supervising authority has placed a hold.
        # Release Condition: Release controlled by supervising officer/agency, not jail.
        stringr::str_detect(charge_description, "PROBATION AND PAROLE|PAROLE BOARD") ~ "Probation/Parole Hold",

        # Court-Ordered Hold
        # Reason for Hold: The court has explicitly ordered that the individual not be released.
        # Release Condition: Tied to judicial action; cannot release until court is satisfied.
        stringr::str_detect(charge_description, "FUGITIVE FROM JUSTICE") ~ "Court-Ordered Hold",

        # Administrative/Operational Hold
        # Reason for Hold: Internal jail policies or procedures prevent immediate release.
        # Release Condition: Internal review or process must be completed before release.
        stringr::str_detect(charge_description, "OTHER AGENCY") ~ "Administrative/Operational Hold",

        # Financial Hold
        # Reason for Hold: Release is contingent on payment of financial obligations.
        # Release Condition: Payment or resolution of financial obligations needed for release.

        # Protective/Safety Hold
        # Reason for Hold: Concerns about the individual's safety or well-being.
        # Release Condition: Safety measures or evaluations must be addressed before release.

        # Medical Hold
        # Reason for Hold: Medical issues prevent safe release.
        # Release Condition: Medical clearance required prior to release.

        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      charge_description_clean = dplyr::if_else(
        !is.na(hold_category),
        hold_category,
        charge_description
      )
    )

  # TODO: Broader Hold Categories
  # Oklahoma Jurisdiction/Agency Hold
  # Reason for Hold: Another local Oklahoma agency is preventing release until they take custody.
  # Release Condition: Release depends on transfer or action by the other agency.

  # Combine bookings data from both sources
  booking_sources <- list(
    okpolicy_scraped_bookings,
    asemio_scraped_bookings_with_inmate_info
  )

  # if (!is.null(jail_data_initiative_people)) {
  #   booking_sources <- append(booking_sources, list(jail_data_initiative_people))
  # }

  combined_bookings <- dplyr::bind_rows(booking_sources) |>
    dplyr::mutate(
      booking_month = lubridate::floor_date(booking_date, "month"),
      booking_year = lubridate::year(booking_date),
      release_month = lubridate::floor_date(release_date, "month"),
      release_year = lubridate::year(release_date),
      created_at_month = lubridate::floor_date(created_at, "month"),
      created_at_year = lubridate::year(created_at),
      created_at_inmate_month = lubridate::floor_date(created_at_inmate, "month"),
      created_at_inmate_year = lubridate::year(created_at_inmate)
    )

  # A note on number of bookings per day with scraped data:
  # Booking date in scraped data cannot be used to determine the number of
  # bookings per day. People booked before the first day of scraping
  # will appear on the first day of scraping, but not all of them. So the
  # number of bookings per day will slowly increase as it approaches the first day of scraping.
  # If scraping occurs daily, then the number of bookings will be accurate going
  # forward, but still not accurate for past dates. However, for any days that
  # scraping did not occur, the number of bookings will be undercounted since
  # they may have been released before the next scraping date.

  # We should utilize the "created_at" field from the scraped data to
  # determine when the data was actually scraped. This will help identify
  # the dates scraping occurred to determine which date ranges have accurate
  # booking counts. Number of daily, monthly, and yearly bookings can only be
  # accurately determined for dates where scraping occurred daily without gaps
  # throughout the entire date range.

  # The new scrapes alleviates this partially since it shows recent releases as
  # well, but it still introduces some uncertainty since the behavior of what
  # appears on the recent releases is not fully known.

  # A note on jail population per day with scraped data:
  # The jail population is tricky because people can be booked and released
  # within the same day. So just counting the number of bookings per day
  # does not give an accurate picture of the average population at a given point
  # in time. Accouting for the booking/release times would need to be done to
  # get a more accurate picture of the jail population over time.

  # TODO:: I want to count the number of people booked per day, month, and year by
  # source. Not the number of rows in the bookings data since people may be in
  # the data multiple times per month.
  # Each source needs to be counted by source, booking_date, and booking_id
  booking_counts_by_date <- combined_bookings |>
    # dplyr::distinct(jacket_number, booking_date, source, .keep_all = TRUE) |>
    # The accurate counts are only for dates where scraping occurred
    dplyr::mutate(
      scraped_on_day_of_booking = dplyr::if_else(
        booking_date == as.Date(created_at),
        TRUE,
        FALSE
      )
    ) |>
    dplyr::count(
      source, booking_date, booking_month, booking_year, scraped_on_day_of_booking,
      name = "daily_bookings"
    ) |>
    dplyr::mutate(
      monthly_bookings = sum(.data$daily_bookings[.data$scraped_on_day_of_booking], na.rm = TRUE),
      monthly_bookings_scraped_different_day = sum(.data$daily_bookings, na.rm = TRUE),
      .by = c(source, booking_month)
    ) |>
    dplyr::mutate(
      yearly_bookings = sum(.data$daily_bookings[.data$scraped_on_day_of_booking], na.rm = TRUE),
      yearly_bookings_scraped_different_day = sum(.data$daily_bookings, na.rm = TRUE),
      .by = c(source, booking_year)
    ) |>
    dplyr::arrange(booking_date)

  # TODO: For the same unique booking, is there more than one row, and if so
  # what differs between the rows? Is it just charges or other info as well?
  # TODO: Under what circumstances does the updated_at date change?
  # ID is unique
  asemio_scraped_bookings_with_inmate_info |>
    dplyr::filter(dplyr::n() > 1, .by = c("id"))
  # Jacket number + booking date has 133k duplicates
  asemio_scraped_bookings_with_inmate_info |>
    dplyr::filter(dplyr::n() > 1, .by = c("jacket_number", "booking_date"))
  # Jacket number + booking date + created at date has 105k duplicates
  asemio_scraped_bookings_with_inmate_info |>
    dplyr::filter(dplyr::n() > 1, .by = c("jacket_number", "booking_date", "created_at_date"))
  # Jacket number + booking date + created at (date time) has 3,667 duplicates
  asemio_scraped_bookings_with_inmate_info |>
    dplyr::filter(dplyr::n() > 1, .by = c("jacket_number", "booking_date", "created_at"))


  # WARNING: Tests, not part of processing
  test <- asemio_scraped_bookings_with_inmate_info |>
    dplyr::filter(
      dplyr::n() > 1,
      .by = c("jacket_number", "booking_date", "created_at")
    ) |>
    dplyr::select(source, booking_date)
  asemio_scraped_bookings_with_inmate_info |>
    dplyr::select(!tidyselect::where(~ lubridate::is.POSIXct(.x) || lubridate::is.Date(.x))) |>
    dplyr::select(source, id)


  scraping_trends <- combined_bookings |>
    dplyr::count(source, created_at_date, updated_at_date, name = "n_bookings_updated") |>
    dplyr::mutate(
      n_bookings_created = sum(.data$n_bookings_updated[.data$created_at_date == .data$updated_at_date], na.rm = TRUE),
      .by = c(source, created_at_date)
    ) |>
    dplyr::arrange(created_at_date)

  booking_trends <- combined_bookings |>
    dplyr::count(source, booking_month, name = "bookings") |>
    dplyr::arrange(booking_month)

  booking_demographics_gender <- combined_bookings |>
    tidyr::drop_na(gender) |>
    dplyr::count(source, gender, name = "bookings")

  booking_demographics_race <- combined_bookings |>
    tidyr::drop_na(race) |>
    dplyr::count(source, race, name = "bookings")

  # TODO: Analysis of top arresting_officers

  list(
    booking_records = combined_bookings,
    booking_trends = booking_trends,
    booking_demographics = list(
      gender = booking_demographics_gender,
      race = booking_demographics_race
    ),
    jail_data_initiative = list(
      people = jail_data_initiative_people,
      charges = jail_data_initiative_charges
    ),
    brek_report = ingested_data$brek,
    release_counts = ingested_data$brek |>
      dplyr::filter(metric_family == "release_counts"),
    release_shares = ingested_data$brek |>
      dplyr::filter(metric_family == "release_share"),
    adp_summary = ingested_data$brek |>
      dplyr::filter(metric_family == "adp"),
    brek_bookings_summary = ingested_data$brek |>
      dplyr::filter(metric_family == "bookings")
  )
}

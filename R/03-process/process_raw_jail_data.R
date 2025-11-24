process_jail_dataset <- function(input_checks = jail_input_checks) {
  raw <- input_checks$raw_data

  # TODO: Standardize Asemio scraper columns to match scraped data as much as possible
  asemio_scraped_bookings <- raw$asemio$bookings |>
    dplyr::mutate(source = "Asemio Scraper") |>
    dplyr::rename(
      jacket_number = dlm,
      arresting_agency = arrested_by,
      zip_code = postal_code
    )

  asemio_scraped_bookings <- raw$asemio$inmates |>
    dplyr::mutate(source = "Asemio Scraper") |>
    dplyr::rename(
      jacket_number = dlm,
      arresting_agency = arrested_by,
      zip_code = postal_code
    )


  asemio_scraped_charges <- raw$asemio$charges |>
    dplyr::mutate(source = "Asemio Scraper") |>
    dplyr::rename(
      jacket_number = dlm,
      charge_description = description
    )

  okpolicy_scraped_bookings <- raw$okpolicy$bookings |>
    dplyr::mutate(
      # TODO: Come back an reevaluate use of tz and quiet parameters
      birth_date = lubridate::mdy(.data$birth_date),
      booking_datetime = lubridate::ymd_hms(
        .data$booking_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      booking_date = as.Date(.data$booking_datetime),
      source = "OK Policy Scraper",
      gender = stringr::str_to_title(.data$gender),
      race = stringr::str_to_title(.data$race)
    )

  okpolicy_scraped_charges <- raw$okpolicy$charges |>
    dplyr::mutate(
      bail_set_datetime = lubridate::ymd_hms(
        .data$bail_set_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      bail_set_date = as.Date(.data$bail_set_datetime)
    ) |>
    dplyr::rename(
      jacket_number = dlm,
      charge_description = description,
      arresting_agency_abbreviation = charging_agency_abbreviation
    )



  # Join bookings and charges to add
  scraped_charges_with_bookings_info <- okpolicy_scraped_bookings |>
    dplyr::left_join(
      scraped_charges,
      by = c("id" = "booking_id"),
      suffix = c("", "_charge")
    )

  # Filter out hold charges unless for City of Tulsa or Electronic Monitoring Required

  # TODO: Finer replacement with what the charge actually is rather than
  # the hold category. I.e., Merging typos rather than "Tribal Hold"
  # Hold Categories
  scraped_charges_with_bookings_info <- scraped_charges_with_bookings_info |>
    dplyr::mutate(
      hold_category = dplyr::case_when(
        # Conditional Release Hold,
        # Reason for Hold: Specific conditions (e.g., electronic monitoring) must be met before release.
        # Release Condition: Compliance with set conditions required for release.
        stringr::str_detect(statute_description, "ELECTRONIC MONITOR") ~ "Conditional Release Hold",

        # Tribal Hold
        # Reason for Hold: A tribal authority has placed a hold.
        # Release Condition: Release controlled by tribal authority, not jail.
        stringr::str_detect(statute_description, "MUSCOGEE|MUSKOGEE|CHEROKEE|TRIBAL") ~ "Tribal Hold",

        # Federal Hold (Non-Immigration)
        # Reason for Hold: Federal (non-immigration) authority has placed a hold.
        # Release Condition: Federal (non-immigration) authority dictates release/transfer.
        stringr::str_detect(statute_description, "U\\.S\\. MARSHAL|US MARSHAL|US MARSHALL|FORM-41|DETainer") ~ "Federal Hold (Non-Immigration)",
        stringr::str_detect(statute_description, "\\bATF\\b|\\bFBI\\b|\\bDEA\\b|FEDERAL AGENCY") ~ "Federal Hold (Non-Immigration)",

        # Immigration Hold
        # Reason for Hold: Immigration authorities have placed a hold.
        # Release Condition: Federal authority dictates release/transfer.
        stringr::str_detect(statute_description, "HOLD/ICE") ~ "Immigration Hold",

        # Oklahoma Municipal Hold
        # Reason for Hold: An Oklahoma municipal authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the municipal authority.
        stringr::str_detect(statute_description, "CITY OF TULSA") ~ "Oklahoma Municipal Hold",

        # Hold for Another Oklahoma County
        # Reason for Hold: Another Oklahoma county authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the other county.
        stringr::str_detect(statute_description, "COUNTY|CO\\.") & stringr::str_detect(statute_description, "OK\\b|OKLAHOMA|\\(OK\\)") ~ "Hold for Another Oklahoma County",
        stringr::str_detect(statute_description, "ANOTHER OK COUNTY|OTHER COUNTY, OK") ~ "Hold for Another Oklahoma County",

        # State of Oklahoma Hold
        # Reason for Hold: An Oklahoma state authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the state authority.
        stringr::str_detect(statute_description, "RETURN TO DOC|ALLEN GAMBLE") ~ "State of Oklahoma Hold",
        stringr::str_detect(statute_description, "^HOLD/DOC\\b") ~ "State of Oklahoma Hold",

        # Hold for Another State's Jurisdiction/Agency
        # Reason for Hold: Another state's authority has placed a hold.
        # Release Condition: Release depends on transfer or action by the other state.
        stringr::str_detect(statute_description, "ANOTHER STATE") ~ "Hold for Another State's Jurisdiction/Agency",
        stringr::str_detect(statute_description, "COUNTY|CO\\.") & stringr::str_detect(statute_description, "TX|TEXAS|MO|MISSOURI|OHIO|AR\\b|ARKANSAS|UTAH") ~ "Hold for Another State's Jurisdiction/Agency",
        stringr::str_detect(statute_description, "DEPARTMENT OF CORRECTIONS P&P") ~ "Hold for Another State's Jurisdiction/Agency",

        # Probation/Parole Hold
        # Reason for Hold: Supervising authority has placed a hold.
        # Release Condition: Release controlled by supervising officer/agency, not jail.
        stringr::str_detect(statute_description, "PROBATION AND PAROLE|PAROLE BOARD") ~ "Probation/Parole Hold",

        # Court-Ordered Hold
        # Reason for Hold: The court has explicitly ordered that the individual not be released.
        # Release Condition: Tied to judicial action; cannot release until court is satisfied.
        stringr::str_detect(statute_description, "FUGITIVE FROM JUSTICE") ~ "Court-Ordered Hold",

        # Administrative/Operational Hold
        # Reason for Hold: Internal jail policies or procedures prevent immediate release.
        # Release Condition: Internal review or process must be completed before release.
        stringr::str_detect(statute_description, "OTHER AGENCY") ~ "Administrative/Operational Hold",

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
        statute_description
      )
    )

  # TODO: Broader Hold Categories
  # Oklahoma Jurisdiction/Agency Hold
  # Reason for Hold: Another local Oklahoma agency is preventing release until they take custody.
  # Release Condition: Release depends on transfer or action by the other agency.


  okpolicy_scraped_bookings
  raw$okpolicy$charges |>
    count(booking_id)


  combined_bookings <- dplyr::bind_rows(okpolicy_scraped_bookings, asemio_scraped_bookings) |>
    dplyr::mutate(
      booking_month = lubridate::floor_date(booking_date, "month"),
      booking_year = lubridate::year(booking_date)
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
  # in time. Accoutnign for the booking/release times would need to be done to
  # get a more accurate picture of the jail population over time.

  # TODO:: I want to count the number of people booked per day, month, and year by
  # source. Not the number of rows in the bookings data since people may be in
  # the data multiple times per month.
  # Each source needs to be counted by source, booking_date, and booking_id
  booking_counts_by_date <- combined_bookings |>
    dplyr::distinct(source, booking_date, id) |>
    dplyr::count(source, booking_date, name = "daily_bookings") |>
    mutate(
      booking_month = lubridate::floor_date(booking_date, "month"),
      booking_year = lubridate::year(booking_date)
    ) |>
    mutate(
      monthly_bookings = sum(.data$daily_bookings, na.rm = TRUE),
      .by = c(source, booking_month)
    )


  # TODO: Understand if the Asemio scraper booking_date is really the booking date
  # or the date the data was scraped. Do so by comparing to OCDC and other sources.
  # Also look for a pattern in the bookin_dates. Also look for a release data, and
  # use that to determine the jail population over time.

  scraping_trends <- combined_bookings |>
    dplyr::count(source, booking_month, name = "bookings") |>
    dplyr::arrange(booking_month)

  booking_trends <- combined_bookings |>
    dplyr::count(source, booking_month, name = "bookings") |>
    dplyr::arrange(booking_month)

  booking_demographics_gender <- combined_bookings |>
    tidyr::drop_na(gender) |>
    dplyr::count(source, gender, name = "bookings")

  booking_demographics_race <- combined_bookings |>
    tidyr::drop_na(race) |>
    dplyr::count(source, race, name = "bookings")

  list(
    booking_records = combined_bookings,
    booking_trends = booking_trends,
    booking_demographics = list(
      gender = booking_demographics_gender,
      race = booking_demographics_race
    ),
    brek_report = raw$brek,
    release_counts = raw$brek |>
      dplyr::filter(metric_family == "release_counts"),
    release_shares = raw$brek |>
      dplyr::filter(metric_family == "release_share"),
    adp_summary = raw$brek |>
      dplyr::filter(metric_family == "adp"),
    brek_bookings_summary = raw$brek |>
      dplyr::filter(metric_family == "bookings")
  )
}

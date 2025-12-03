### Helper functions
# The helper functions below enforce consistent schemas, fill in missing
# columns, and coerce date/time fields from the Jail Data Initiative CSVs.
# E.g., parsing date times, normalizing names, etc.
# Only do this if it improves readability and confidence in uniform processing
standardize_sex_gender <- function(data, sex_gender_col = "gender", clean_col_name = "sex_gender_standardized") {
  data |>
    dplyr::mutate(
      {{ clean_col_name }} := dplyr::case_when(
        stringr::str_detect(.data[[sex_gender_col]], "(?i)^f(emale)?$") ~ "Female",
        stringr::str_detect(.data[[sex_gender_col]], "(?i)^m(ale)?$") ~ "Male",
        TRUE ~ "Other/Unknown Gender"
      )
    )
}

standardize_race_ethnicity <- function(data, race_ethnicity_col = "race", clean_col_name = "race_ethnicity_standardized") {
  data |>
    dplyr::mutate(
      {{ clean_col_name }} := dplyr::case_when(
        # WARNING: Recodes for A, H, I, N, O, P, and U are guessed based on
        # other data sources. They have not been corroborated by jail staff.
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^i$|^n$") ~ "American Indian or Alaska Native",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^a$|^p$") ~ "Asian American and Pacific Islander",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^b$") ~ "Black",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^h$") ~ "Hispanic/Latino",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^w$") ~ "White",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^o$") ~ "Other Race/Ethnicity",
        stringr::str_detect(.data[[race_ethnicity_col]], "(?i)^u$") ~ "Unknown Race/Ethnicity",
        TRUE ~ "Unknown Race/Ethnicity"
      )
    )
}

categorize_hold_charge <- function(charge_description) {
  dplyr::case_when(
    stringr::str_detect(charge_description, "ELECTRONIC MONITOR") ~ "Conditional Release Hold",
    stringr::str_detect(charge_description, "MUSCOGEE|MUSKOGEE|CHEROKEE|TRIBAL") ~ "Tribal Hold",
    stringr::str_detect(charge_description, "U\\.S\\. MARSHAL|US MARSHAL|US MARSHALL|FORM-41|DETainer") ~ "Federal Hold (Non-Immigration)",
    stringr::str_detect(charge_description, "\\bATF\\b|\\bFBI\\b|\\bDEA\\b|FEDERAL AGENCY") ~ "Federal Hold (Non-Immigration)",
    stringr::str_detect(charge_description, "HOLD/ICE") ~ "Immigration Hold",
    stringr::str_detect(charge_description, "CITY OF TULSA") ~ "Oklahoma Municipal Hold",
    stringr::str_detect(charge_description, "COUNTY|CO\\.") & stringr::str_detect(charge_description, "OK\\b|OKLAHOMA|\\(OK\\)") ~ "Hold for Another Oklahoma County",
    stringr::str_detect(charge_description, "ANOTHER OK COUNTY|OTHER COUNTY, OK") ~ "Hold for Another Oklahoma County",
    stringr::str_detect(charge_description, "RETURN TO DOC|ALLEN GAMBLE") ~ "State of Oklahoma Hold",
    stringr::str_detect(charge_description, "^HOLD/DOC\\b") ~ "State of Oklahoma Hold",
    stringr::str_detect(charge_description, "ANOTHER STATE") ~ "Hold for Another State's Jurisdiction/Agency",
    stringr::str_detect(charge_description, "COUNTY|CO\\.") & stringr::str_detect(charge_description, "TX|TEXAS|MO|MISSOURI|OHIO|AR\\b|ARKANSAS|UTAH") ~ "Hold for Another State's Jurisdiction/Agency",
    stringr::str_detect(charge_description, "DEPARTMENT OF CORRECTIONS P&P") ~ "Hold for Another State's Jurisdiction/Agency",
    stringr::str_detect(charge_description, "PROBATION AND PAROLE|PAROLE BOARD") ~ "Probation/Parole Hold",
    stringr::str_detect(charge_description, "FUGITIVE FROM JUSTICE") ~ "Court-Ordered Hold",
    stringr::str_detect(charge_description, "OTHER AGENCY") ~ "Administrative/Operational Hold",
    TRUE ~ NA_character_
  )
}

clean_hold_charge_description <- function(charge_description, hold_category) {
  dplyr::if_else(
    !is.na(hold_category),
    hold_category,
    charge_description
  )
}


### Main processing function
process_ingested_jail_data <- function(ingested_checks = jail_ingested_checks) {
  ingested_data <- ingested_checks$ingested_data

  ### Normalize, clean, and parse ingested data
  # We clean here instead of downstream so that future joins can treat JDI rows
  # the same as Asemio or OK Policy scrapes.

  # For after the report is done:
  # TODO: chore(process): Move code to explore if interesting but not needed in processing
  # TODO: feat(process): Determine timezone of each data source
  # TODO: Pull out some helper functions for repeated processing tasks
  # TODO: feat(process): Coalesce columns when multiple columns exist for same data

  ## Process Vera Incarceration Trends Data
  vera <- ingested_data$vera |>
    dplyr::mutate(
      source = "Vera: Incarceration Trends",
      quarter_date = lubridate::ymd(paste0(.data$year, "-", .data$quarter * 3, "-01"))
    )


  ## Process Starling Analytics (JailNet) Data
  brek <- ingested_data$brek


  ## Process Jail Data Initiative (JDI) Scraped Data
  # JDI Bookings with Inmate Info
  jail_data_initiative_bookings_with_inmate_info <- ingested_data$jail_data_initiative$people |>
    janitor::clean_names() |>
    dplyr::rename(
      full_name = "name",
      birth_date = "dob",
      jacket_number_old = "dlm",
    ) |>
    dplyr::mutate(sex_gender = dplyr::coalesce(.data$gender, .data$sex)) |>
    standardize_sex_gender(sex_gender_col = "sex_gender") |>
    standardize_race_ethnicity(race_ethnicity_col = "race") |>
    dplyr::mutate(
      source = "Jail Data Initiative Scraper",
      # Direct PII
      # Indirect PII
      age = as.integer(.data$age),
      age_standardized = as.double(.data$age_standardized),
      # Dates
      arrest_booking_date = lubridate::mdy(.data$arrest_booking_date),
      # TODO: Confirm time zone by comparing to other known timezone
      arrest_booking_time = hms::as_hms(lubridate::parse_date_time(.data$arrest_booking_time, orders = c("H:M"))),
      arrest_booking_datetime = as.POSIXct(
        .data$arrest_booking_date + .data$arrest_booking_time,
        tz = "America/Chicago"
      ),
      arrest_date = lubridate::ymd(.data$arrest_date),
      arrest_time = hms::as_hms(lubridate::parse_date_time(.data$arrest_time, orders = c("H:M"))),
      arrest_datetime = as.POSIXct(
        .data$arrest_date + .data$arrest_time,
        tz = "America/Chicago"
      ),
      birth_date = lubridate::ymd(.data$birth_date),
      booking_date = lubridate::ymd(.data$booking_date),
      booking_time = hms::as_hms(lubridate::parse_date_time(.data$booking_time, orders = c("H:M"))),
      released_date = lubridate::ymd(.data$released_date),
      released_time = hms::as_hms(lubridate::parse_date_time(.data$released_time, orders = c("H:M"))),
      released_datetime = as.POSIXct(
        .data$released_date + .data$released_time,
        tz = "America/Chicago"
      ),
      release_date = lubridate::ymd(.data$release_date),
      release_time = hms::as_hms(lubridate::parse_date_time(.data$release_time, orders = c("H:M"))),
      release_datetime = as.POSIXct(
        .data$release_date + .data$release_time,
        tz = "America/Chicago"
      ),
      meta_first_seen = lubridate::ymd_hms(.data$meta_first_seen, tz = "America/Chicago"),
      meta_last_seen = lubridate::ymd_hms(.data$meta_last_seen, tz = "America/Chicago"),
      meta_scrape_date = lubridate::ymd(.data$meta_scrape_date, quiet = TRUE) # Loud because of NAs
    )

  # JDI Charges
  jail_data_initiative_charges <- ingested_data$jail_data_initiative$charges |>
    janitor::clean_names() |>
    dplyr::mutate(
      source = "Jail Data Initiative",
      case_type = stringr::str_extract(.data$case_number, "^([A-Z]+)-\\d+-\\d+", group = 1),
      # TODO: case_year is very inconsistent and contains likely errors
      case_year = stringr::str_extract(.data$case_number, "^[A-Z]+-(\\d+)-\\d+", group = 1),
      # TODO: If case_year is unreliable, case_number_id is likely unreliable as
      # well, but potentially even harder to notice errors (needs matching with OSCN data)
      case_index = stringr::str_extract(.data$case_number, "^[A-Z]+-\\d+-(\\d+)", group = 1),
      court_date = lubridate::ymd(.data$court_date),
      bond_amount = as.double(.data$bond_amount),
      bail_amount = as.double(.data$bail_amount),
      bail_set_date = lubridate::ymd(.data$bail_set_date),
      bail_set_time = hms::as_hms(lubridate::parse_date_time(.data$bail_set_time, orders = c("H:M"))),
      # TODO: Confirm time zone by comparing to other known timezone
      bail_set_datetime = as.POSIXct(
        .data$bail_set_date + .data$bail_set_time,
        tz = "America/Chicago"
      )
    )


  ## Process Asemio Scraper Data
  # Asemio Bookings
  asemio_scraped_bookings <- ingested_data$asemio$bookings |>
    dplyr::rename(
      jacket_number = dlm,
      arresting_agency = arrested_by,
      zip_code = postal_code
    ) |>
    dplyr::mutate(
      source = "Asemio Scraper",
      # WARNING: Very little verification, needs comprehensive checks and reasoning
      # booking_id excluded because it is not unique it seems
      custom_booking_id = paste(
        .data$jacket_number, .data$booking_date,
        sep = "-"
      ),
      time = hms::as_hms(lubridate::parse_date_time(time, orders = c("IM p"))),
      booking_time = hms::as_hms(lubridate::parse_date_time(booking_time, orders = c("IM p"))),
      release_time = hms::as_hms(lubridate::parse_date_time(release_time, orders = c("IM p"))),
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at),
      created_at_time = hms::parse_hms(stringr::str_sub(created_at, 12, 19)),
      updated_at_time = hms::parse_hms(stringr::str_sub(updated_at, 12, 19)),
      jacket_number = as.character(.data$jacket_number)
    )

  # Asemio Inmate Information
  asemio_scraped_inmates <- ingested_data$asemio$inmates |>
    dplyr::rename(
      jacket_number = dlm,
      given_name = first_name,
      surname = last_name,
    ) |>
    standardize_sex_gender(sex_gender_col = "gender") |>
    standardize_race_ethnicity(race_ethnicity_col = "race") |>
    dplyr::mutate(
      source = "Asemio Scraper",
      # Direct PII
      jacket_number = as.character(.data$jacket_number),
      # TODO: Decide how to normalize asemio & okpolicy full name columns
      # Asemio data has no middle name, so just concatenate given_name and surname
      full_name = paste(.data$given_name, .data$surname),
      # Other Dates
      created_at_date = as.Date(created_at),
      updated_at_date = as.Date(updated_at)
    )

  # Asemio Charges
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


  ## Process OK Policy Scraper Data
  # OK Policy Bookings with Inmate Info
  okpolicy_scraped_bookings_with_inmate_info <- ingested_data$okpolicy$bookings |>
    dplyr::rename(
      created_at_date = created_at,
      updated_at_datetime = updated_at
    ) |>
    standardize_sex_gender(sex_gender_col = "gender") |>
    standardize_race_ethnicity(race_ethnicity_col = "race") |>
    dplyr::mutate(
      source = "OK Policy Scraper",
      # Direct PII
      full_name = paste(.data$given_name, .data$middle_name, .data$surname),
      # Indirect PII
      birth_date = lubridate::mdy(.data$birth_date),
      # Other Dates
      booking_datetime = lubridate::ymd_hms(
        .data$booking_date,
        # TODO: We actually need to confirm the timezone of the booking date
        tz = "America/Chicago",
        quiet = TRUE
      ),
      booking_date = as.Date(.data$booking_datetime), # Booking date is really booking datetime
      booking_time = hms::as_hms(.data$booking_datetime),
      updated_at_date = as.Date(updated_at_datetime),
      updated_at_time = hms::as_hms(updated_at_datetime)
    )

  # OK Policy Charges
  okpolicy_scraped_charges <- ingested_data$okpolicy$charges |>
    dplyr::rename(
      charge = statute_title,
      charge_description = statute_description,
      arresting_agency = charging_agency_abbreviation,
      updated_at_datetime = updated_at,
      # WARNING: This will break if I change created_at to a datetime later.
      created_at_date = created_at
    ) |>
    dplyr::mutate(
      source = "OK Policy Scraper",
      bail_set_datetime = lubridate::ymd_hms(
        # TODO: We actually need to confirm the timezone of the booking date
        .data$bail_set_date,
        tz = "America/Chicago",
        quiet = TRUE
      ),
      bail_set_date = as.Date(.data$bail_set_datetime),
      created_at_date = as.Date(created_at_date),
      updated_at_date = as.Date(updated_at_datetime),
      updated_at_time = hms::as_hms(updated_at_datetime),
    )


  ## TODO: Remaining data cleaning and processing tasks
  # Move above where it belongs when done.

  # TODO: feat(jail-process): Filter out hold charges unless for City of Tulsa or Electronic Monitoring Required
  # TODO: fix(jail-process): Finer replacement with what the charge actually is rather than
  # the hold category. I.e., Merging typos rather than "Tribal Hold"
  # Hold Categories
  okpolicy_scraped_charges <- okpolicy_scraped_charges |>
    dplyr::mutate(
      hold_category = categorize_hold_charge(charge_description),
      charge_description_clean = clean_hold_charge_description(charge_description, hold_category)
    )

  # TODO: feat(jail-process): Add broader Hold Categories
  # Oklahoma Jurisdiction/Agency Hold
  # Reason for Hold: Another local Oklahoma agency is preventing release until they take custody.
  # Release Condition: Release depends on transfer or action by the other agency.


  ### Joining data
  # Join inmate info to bookings for Asemio data
  asemio_scraped_bookings_with_inmate_info <- asemio_scraped_bookings |>
    # NOTE: I've decided to keep this here, though it procludes scraping trends
    # because it's not necessary for this report.
    dplyr::arrange(desc(updated_at)) |> # Want the most recent update to be kept
    dplyr::distinct(custom_booking_id, .keep_all = TRUE) |>
    dplyr::left_join(
      asemio_scraped_inmates,
      by = "jacket_number",
      suffix = c("", "_inmate")
    )
  # Join booking info to charges for OK Policy data
  # This is needed to filter bookings with certain hold charges
  okpolicy_scraped_charges_with_bookings_info <- okpolicy_scraped_bookings_with_inmate_info |>
    dplyr::left_join(
      okpolicy_scraped_charges,
      by = c("id" = "booking_id"),
      suffix = c("", "_charge")
    )


  ### Combine individual processed bookings records from each data source
  # Each should include inmate info already where available
  # NOTE: Right now they are all scraped data but could include other sources later
  booking_sources <- list(
    jail_data_initiative_bookings_with_inmate_info,
    okpolicy_scraped_charges_with_bookings_info,
    asemio_scraped_bookings_with_inmate_info
  )


  combined_processed_bookings <- dplyr::bind_rows(booking_sources) |>
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


  ### Generate derived metrics: Jail Bookings, Jail Releases, Jail Average Daily Population (ADP)

  ## Jail Bookings
  # Methodology notes for bookings:
  # NOTE: Number of bookings per day with scraped data:
  # Booking date in scraped data cannot be used to determine the number of
  # bookings per day. People booked before the first day of scraping
  # will appear on the first day of scraping, but not all of them. So the
  # number of bookings per day will slowly increase as it approaches the first day of scraping.
  # If scraping occurs daily, then the number of bookings will be accurate going
  # forward, but still not accurate for past dates. However, for any days that
  # scraping did not occur, the number of bookings will be undercounted since
  # they may have been released before the next scraping date.
  #
  # We should utilize the "created_at" field from the scraped data to
  # determine when the data was actually scraped. This will help identify
  # the dates scraping occurred to determine which date ranges have accurate
  # booking counts. Number of daily, monthly, and yearly bookings can only be
  # accurately determined for dates where scraping occurred daily without gaps
  # throughout the entire date range.
  #
  # The new scrapes alleviates this partially since it shows recent releases as
  # well, but it still introduces some uncertainty since the behavior of what
  # appears on the recent releases is not fully known.

  # NOTE: Jail population per day with scraped data:
  # The jail population is tricky because people can be booked and released
  # within the same day. So just counting the number of bookings per day
  # does not give an accurate picture of the average population at a given point
  # in time. Accouting for the booking/release times would need to be done to
  # get a more accurate picture of the jail population over time.

  # Main total bookings calculation
  # Goal is to get as accurate an estimate as possible.
  # For that reason we differentiate using the methodology above between
  # bookings counted on days where scraping occurred and those where it did not.
  # TODO: feat(process): Add booking_totals vs scraped_booking_totals to account for scraping gaps
  # TODO: feat(process): Add gender, race, age demographics to booking totals
  # TODO: feat(process): Add est_booking_totals to account for undercounting due to scraping gaps
  # TODO: Later, feat(process): Add hold charge categories to booking totals
  booking_totals <- combined_processed_bookings |>
    dplyr::count(source, booking_month, name = "bookings") |>
    # Combining data from sources which are already aggregated
    dplyr::bind_rows(
      vera |>
        dplyr::filter(!is.na(total_jail_adm)) |>
        dplyr::filter(county_code == "US_OK_TULSA") |>
        dplyr::filter(year >= 1999) |>
        dplyr::mutate(
          source,
          booking_month = quarter_date,
          # total_jail_adm is an estimate of total admissions in the past year
          # for a county.
          # Source: pg.10 of Vera Incarceration Trends codebook
          bookings_past_year = total_jail_adm,
          bookings = total_jail_adm / 12,
          .keep = "none"
        ),
      brek |>
        dplyr::filter(
          metric == "Annual Bookings",
          category == "Total",
          county == "Tulsa County"
        ) |>
        dplyr::mutate(
          source = "Starling Analytics (JailNet)",
          bookings = value / 12,
          booking_month = year_date,
          .keep = "none"
        )
    ) |>
    dplyr::arrange(desc(booking_month))

  # NOTE: This logic needs to be merged into the above as part of one of the TODOs
  booking_counts_by_date <- combined_processed_bookings |>
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


  ## Jail Releases


  ## Jail Average Daily Population (ADP)


  ## Other derived metrics (not used in report)
  # TODO: feat(process): Scraping trends analysis
  scraping_trends <- combined_processed_bookings |>
    dplyr::count(source, created_at_date, updated_at_date, name = "n_bookings_updated") |>
    dplyr::mutate(
      n_bookings_created = sum(.data$n_bookings_updated[.data$created_at_date == .data$updated_at_date], na.rm = TRUE),
      .by = c(source, created_at_date)
    ) |>
    dplyr::arrange(created_at_date)
  # TODO: feat(process): Analysis of top arresting_officers




  ### Return processed data
  list(
    booking_records = combined_processed_bookings,
    booking_totals = booking_totals,
    booking_demographics = list(
      gender = booking_demographics_gender,
      race = booking_demographics_race
    ),
    vera = vera,
    jail_data_initiative = list(
      people = jail_data_initiative_bookings_with_inmate_info,
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

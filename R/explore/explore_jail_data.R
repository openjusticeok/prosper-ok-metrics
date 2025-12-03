library(tulsaCountyJailScraper)
library(ojodb)
library(dplyr)
library(readr)
library(lubridate)
library(tidyr)
library(ggplot2)


# TODO: Grab the import data from the processing step

# Explore hold charges in scraped data
scraped_bookings_and_charges <- scraped_bookings |>
  dplyr::left_join(
    scraped_charges,
    by = c("id" = "booking_id"),
    suffix = c("", "_charge")
  )

# NOTE: This is not necessarily the latest version of the processing code.
# It is copied here for exploration purposes.
scraped_charges_with_bookings_info <- scraped_charges_with_bookings_info |>
  mutate(
    hold_category = case_when(
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
  mutate(
    charge_description_clean = if_else(
      !is.na(hold_category),
      hold_category,
      statute_description
    )
  )


# Move to exploration script later
scraped_charges_with_bookings_info |>
  filter(
    stringr::str_detect(
      statute_description,
      "(?i)hold"
    )
  ) |>
  count(statute_description, sort = TRUE) |>
  print(n = Inf)

scraped_charges_with_bookings_info |>
  filter(
    stringr::str_detect(
      statute_title,
      "(?i)hold"
    )
  ) |>
  count(statute_title, sort = TRUE) |>
  print(n = Inf)

scraped_charges_with_bookings_info |>
  count(hold_category, sort = TRUE) |>
  print(n = Inf)

### Explore Asemio Data
# TODO: There has to be a better way...
tar_load(jail_input_checks)
raw <- jail_input_checks$ingested_data

# Asemio Scraper Data Processing
asemio_scraped_bookings <- raw$asemio$bookings |>
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

# TODO: Plot scraping date-times (created_at) to see the frequency of scrapes
# TODO: Analyze the average frequency (days) by month
# TODO: Plot jail population over time on each scraping date
# TODO: Plot days where the number of bookings is accurate by the scraping data
# TODO: Create a function to easily do the accurate booking numbers for any
# scraped data using a default created_at variable.
# TODO: Incorporate the analysis into a report for monitoring scraping accuracy over time

## Scraping Trends
# How often does create_at match updated_at?:
# On the same day: 70.7%
# On the same minute: 68.0%
# Conclusion: Most scraped bookings are not updated.
asemio_scraped_bookings |>
  dplyr::summarise(
    total_scrapes = dplyr::n(),
    matching_dates = sum(.data$created_at_date == .data$updated_at_date),
    matching_minute = sum(
      lubridate::floor_date(.data$created_at, unit = "minute") ==
        lubridate::floor_date(.data$updated_at, unit = "minute")
    ),
    pct_matching_date = matching_dates / total_scrapes,
    pct_matching_minute = matching_minute / total_scrapes
  )

# What are statistics on the time difference between created_at and updated_at?
# Heavily right-skewed by few scraped booking that were updates 4 years after creation
# As shown above, most are the same day. 75pp was 4 days.
# Conclusion: Most updates happen quickly, but a few take a long time.
asemio_scraped_bookings |>
  dplyr::mutate(
    days_to_update = as.numeric(difftime(updated_at_date, created_at_date, units = "days"))
  ) |>
  dplyr::pull(days_to_update) |>
  (\(.) {
    print(summary(.))
    hist(.)
  })()

# Just among those that were updated, how long did it take?: 7 median, max 4
# years, 75pp 30 days. Slightly less right-skewed but still a long tail.
# Most updates happen within a month when they do happen.
asemio_scraped_bookings |>
  dplyr::filter(
    created_at_date != updated_at_date
  ) |>
  dplyr::mutate(
    days_to_update = as.numeric(difftime(updated_at_date, created_at_date, units = "days"))
  ) |>
  dplyr::pull(days_to_update) |>
  (\(.) {
    print(summary(.))
    hist(.)
  })()

# What were those that took a long time to update?
asemio_scraped_bookings |>
  dplyr::mutate(
    days_to_update = as.numeric(difftime(updated_at_date, created_at_date, units = "days"))
  ) |>
  dplyr::filter(
    days_to_update > 180
  )

asemio_scraped_bookings |>
  dplyr::mutate(
    days_days_to_scrape = as.numeric(difftime(created_at_date, booking_date, units = "days"))
  ) |>
  dplyr::pull(days_days_to_scrape) |>
  (\(.) {
    print(summary(.))
    hist(.)
  })()

## Explore asemio scrape duplicates to construct custom booking id
# WARNING: Not part of processing. Exploration of duplicates only.
asemio_scraped_bookings_with_inmate_info |>
  dplyr::mutate(
    duplicate_count = dplyr::n(),
    .by = c("jacket_number", "booking_date")
  ) |>
  dplyr::filter(
    duplicate_count > 1
  ) |>
  dplyr::select(duplicate_count, booking_id, jacket_number, full_name, booking_date, created_at, updated_at) |>
  dplyr::arrange(desc(duplicate_count), booking_date, jacket_number, created_at)
asemio_scraped_bookings_with_inmate_info |>
  dplyr::mutate(
    duplicate_count = dplyr::n(),
    .by = c("jacket_number", "booking_date", "created_at")
  ) |>
  dplyr::filter(
    duplicate_count > 1
  ) |>
  dplyr::select(duplicate_count, booking_id, jacket_number, full_name, booking_date, created_at, updated_at) |>
  dplyr::arrange(desc(duplicate_count), booking_date, jacket_number, created_at)

asemio_scraped_bookings_with_inmate_info |>
  dplyr::mutate(
    duplicate_count = dplyr::n(),
    .by = c("booking_id", "jacket_number", "booking_date", "created_at")
  ) |>
  dplyr::filter(
    duplicate_count > 1
  ) |>
  dplyr::select(duplicate_count, booking_id, jacket_number, full_name, booking_date, created_at, updated_at) |>
  dplyr::arrange(desc(duplicate_count), booking_date, jacket_number, created_at)

# For these unique bookings, lets only keep them when booking_date == created_at_date
# Cuts 17k rows.
asemio_scraped_bookings_with_inmate_info |>
  dplyr::distinct(custom_booking_id, .keep_all = TRUE) |>
  dplyr::select(booking_id, jacket_number, full_name, booking_date, created_at, updated_at, created_at_date) |>
  dplyr::arrange(booking_date, jacket_number, created_at) |>
  dplyr::filter(
    booking_date == created_at_date
  )

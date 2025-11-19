library(tulsaCountyJailScraper)
library(ojodb)
library(dplyr)
library(readr)
library(lubridate)
library(tidyr)
library(ggplot2)


scraped_jail_data <- tulsaCountyJailScraper::scrape_data()
scraped_jail_bookings <- scraped_jail_data$bookings_tibble |>
  mutate(
    birth_date = mdy(birth_date),
    booking_date = ymd_hms(booking_date)
  )
scraped_jail_charges <- scraped_jail_data$charges_tibble |>
  mutate(
    bail_set_date = ymd_hms(bail_set_date)
  )

ojodb_jail_bookings <- ojo_tbl("arrest", schema = "iic") |>
  collect()

ojodb_jail_charges <- ojo_tbl("offense", schema = "iic") |>
  collect()

brek_adp_2024 <- tribble(
  ~metric_type, ~category,        ~oklahoma_county, ~tulsa_county, ~yoy_change_ok, ~yoy_change_tulsa,
  "Overall",    "Total",          1404,             1333,          -0.053,         -0.016,
  "Gender",     "Male",           1200,             1121,          -0.071,         -0.017,
  "Gender",     "Female",         204,              212,            0.068,         -0.006,
  "Race",       "White",          542,              603,           -0.047,         -0.055,
  "Race",       "Black",          608,              466,           -0.051,         -0.001,
  "Race",       "Native American", 51,              72,            -0.030,          0.034,
  "Race",       "Hispanic",       189,              174,           -0.087,          0.056
)

brek_bookings_2024 <- tribble(
  ~metric_type, ~category,        ~oklahoma_county, ~tulsa_county, ~yoy_change_ok, ~yoy_change_tulsa,
  "Overall",    "Total",          20890,            15900,         -0.073,          0.046,
  "Gender",     "Male",           15748,            11975,         -0.062,          0.054,
  "Gender",     "Female",         5142,             3925,          -0.107,          0.024,
  "Race",       "White",          9020,             8730,          -0.071,          0.016,
  "Race",       "Black",          8007,             4491,          -0.070,          0.080,
  "Race",       "Native American", 745,             850,           -0.038,         -0.037,
  "Race",       "Hispanic",       2566,             1597,          -0.105,          0.182
)

brek_tulsa_release_counts_2024 <- tribble(
  ~disposition_type,             ~total, ~male, ~female,
  "Bond (Surety/Cash)",          6455,   4829,  1626,
  "Dismissed/Withdrawn",         2425,   1785,  640,
  "Sentence Served",             1385,   1082,  302,
  "Atty/Personal Recognizance",  873,    590,   283
)

brek_tulsa_release_pct_race_2024 <- tribble(
  ~disposition_type,            ~white, ~black, ~native_american, ~hispanic, ~other,
  "Bond (Surety/Cash)",         0.408,  0.446,  0.062,            0.440,     0.453,
  "Dismissed/Withdrawn",        0.129,  0.123,  0.668,            0.096,     0.163,
  "Sentence Served",            0.086,  0.092,  0.025,            0.113,     0.058,
  "Atty/Personal Recognizance", 0.067,  0.051,  0.016,            0.025,     0.016
)

brek_tulsa_avg_los_2024 <- tribble(
  ~metric_type, ~category,        ~avg_days, ~yoy_change,
  "Overall",    "Total",          31.5,      0.069,
  "Gender",     "Male",           35.6,      0.095,
  "Gender",     "Female",         19.0,     -0.034,
  "Race",       "White",          26.8,      0.024,
  "Race",       "Black",          35.3,     -0.039,
  "Race",       "Native American", 35.2,      0.692, # Notable increase
  "Race",       "Hispanic",       45.3,      0.157
)

brek_tulsa_rebooking_rates_2024 <- tribble(
  ~metric_type, ~category,        ~rebooking_rate, ~yoy_change,
  "Overall",    "Total",          0.247,           0.004,
  "Gender",     "Male",           0.251,           0.004,
  "Gender",     "Female",         0.236,           0.003,
  "Race",       "White",          0.240,          -0.007, # Text body says 23.9%, chart says 24.0%. Used chart.
  "Race",       "Black",          0.286,           0.025,
  "Race",       "Native American", 0.238,           0.042,
  "Race",       "Hispanic",       0.189,          -0.003
)

tribble(
  ~ source, ~ column, ~ min_date, ~ max_date,
  "scraped", "booking_date", min(scraped_jail_bookings$booking_date), max(scraped_jail_bookings$booking_date),
  "db", "booking_date", min(ojodb_jail_bookings$booking_date), max(ojodb_jail_bookings$booking_date)
)

ojodb_jail_bookings |>
  mutate(
    year = floor_date(date, "year")
  ) |>
  summarise(
    .by = year,
    n = n()
  ) |>
  filter(
    year >= "2020-01-01",
    year <= "2025-01-01"
  ) |>
  arrange(desc(year))

scraped_jail_bookings |>
  summarise(
    n = n()
  )

scraped_jail_bookings |>
  summarise(
    .by = gender,
    n = n()
  )

scraped_jail_bookings |>
  summarise(
    .by = race,
    n = n()
  )

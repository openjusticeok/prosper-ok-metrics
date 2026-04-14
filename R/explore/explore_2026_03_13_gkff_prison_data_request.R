library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(readr)
library(googledrive)
library(here)
library(readxl)
library(pointblank)
library(janitor)

# Ingest
single_drive_folder_url <- "https://drive.google.com/drive/u/0/folders/1AcwuaOHYifDDYHCHQaykAzLba1yDkHcZ"
folder_id <- googledrive::as_id(single_drive_folder_url)
folder_meta <- googledrive::drive_get(folder_id)
drive_files <- googledrive::drive_ls(folder_id)
snapshot_date <- lubridate::ymd("2026-03-13")
input_dir <- here::here("data/input/doc/2026-03-13-gkff/")

fs::dir_create(input_dir)

purrr::pwalk(
  .l = drive_files,
  .f = function(id, name, ...) {
    googledrive::drive_download(
      file = id,
      path = here::here(input_dir, name),
      overwrite = TRUE
    )
  }
)

sentences_data <- readxl::read_xlsx(here(input_dir, "Sentences.xlsx")) |>
  janitor::clean_names()
receptions_data <- readxl::read_xlsx(here(input_dir, "OffenderReception.xlsx")) |>
  janitor::clean_names()
releases_data <- readxl::read_xlsx(here(input_dir, "OffenderExit.xlsx")) |>
  janitor::clean_names()
profile_data <- readxl::read_xlsx(here(input_dir, "Profile.xlsx")) |>
  janitor::clean_names()
offense_data <- readxl::read_xlsx(here(input_dir, "Offense.xlsx")) |>
  janitor::clean_names()
alias_data <- readxl::read_xlsx(here(input_dir, "Alias.xlsx")) |>
  janitor::clean_names()

# Check Ingested
sentences_data |>
  pointblank::scan_data(sections = "OVMS")
receptions_data |>
  pointblank::scan_data(sections = "OVMS")
releases_data |>
  pointblank::scan_data(sections = "OVMS")
profile_data |>
  pointblank::scan_data(sections = "OVMS")
offense_data |>
  pointblank::scan_data(sections = "OVMS")
alias_data |>
  pointblank::scan_data(sections = "OVMS")

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
  # Check that all doc_num's in the sentence data appear in the profile data
  # I.e. sentence doc_num's ⊆ profile doc_num's
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

# TODO: Add offense and alias checks
# TODO: Incorporate the checks Polina created

# Process

# Analysis
profile_data <- profile_data |>
  mutate(
    birth_date = ymd(birth_date),
    admit_date = ymd(admit_date),
    release_date = ymd(release_date)
  ) |>
  mutate(
    physical_stay = lubridate::interval(
      start = admit_date,
      end = if_else(is.na(release_date), snapshot_date, release_date)
    )
  )

dates <- tibble::tibble(
  day = seq(ymd("2024-01-01"), ymd("2025-12-31"), by = "1 day")
) |>
  dplyr::mutate(day = lubridate::floor_date(day, "hour"))

compute_daily_population <- function(date, intervals) {
  return(
    # This is equivalent to sum(date %within% intervals):
    sum(lubridate::`%within%`(date, intervals), na.rm = TRUE)
  )
}

dates |>
  dplyr::mutate(
    total_pop = sapply(dates, function(x) {
      compute_daily_population(x, profile_data$physical_stay)
    })
  )

start_date <- ymd("2024-01-01")
end_date   <- ymd("2025-12-31")

daily_population <- profile_data |>
  transmute(
    admit_date   = ymd(admit_date),
    exit_date    = coalesce(ymd(release_date), snapshot_date)
  ) |>
  filter(!is.na(admit_date), !is.na(exit_date), admit_date <= exit_date) |>
  transmute(
    start = admit_date,
    end   = exit_date + days(1)
  ) |>
  bind_rows(
    transmute(., day = start, delta =  1),
    transmute(., day = end,   delta = -1)
  ) |>
  summarise(delta = sum(delta), .by = day) |>
  right_join(tibble(day = seq(start_date, end_date, by = "1 day")), by = "day") |>
  arrange(day) |>
  mutate(
    delta = replace_na(delta, 0L),
    total_pop = cumsum(delta)
  ) |>
  select(day, total_pop)


### Takeaways
## sentences data
# INCARCERATED_TERM_IN_YEARS is missing __, __, __. Shows only 999.
# What is the reason for lack of SENTENCE_START_DATE?
# Why do some have such high SENTENCE_END_DATE?
#

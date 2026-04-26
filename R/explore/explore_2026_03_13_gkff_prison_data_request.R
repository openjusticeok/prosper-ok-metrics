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
library(benchCalculatePopulation)
library(ojoutils)
library(googleCloudStorageR)
library(gargle)
library(DBI)
library(duckdb)
library(fastexcel)

parquet_raw <- function(x, ...) {
  sink <- arrow::BufferOutputStream$create()

  arrow::write_parquet(x, sink, ...)

  sink$finish()$data()
}

gcs_upload_raw <- function(raw, bucket, name, token,
                           content_type = "application/vnd.apache.parquet") {
  token$refresh()

  resp <- httr2::request(
    glue::glue("https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o")
  ) |>
    httr2::req_url_query(
      uploadType = "media",
      name = name
    ) |>
    httr2::req_headers(
      Authorization = paste("Bearer", token$credentials$access_token),
      `Content-Type` = content_type
    ) |>
    httr2::req_body_raw(raw) |>
    httr2::req_perform()

  httr2::resp_body_json(resp)
}


gcs_read_parquet <- function(
  object,
  bucket = googleCloudStorageR::gcs_get_global_bucket(),
  clean_names = TRUE
) {
  data <- googleCloudStorageR::gcs_get_object(
    # TODO: pass other arugments
    object_name = object,
    bucket = bucket
  ) |>
    arrow::read_parquet(
      # TODO: pass other arguments
    )
  if (clean_names) data <- janitor::clean_names(data)

  return(data)
}


# Ingest
## Auth
scope <- "https://www.googleapis.com/auth/cloud-platform"
bucket <- "odoc-adhoc-data-requests"

token <- gargle::token_fetch(scopes = scope)
googleCloudStorageR::gcs_global_bucket(bucket = bucket)
googleCloudStorageR::gcs_auth(token = token)

objects <- googleCloudStorageR::gcs_list_objects(
  prefix = "2026-03-13-gkff/landing/EXTID-136 ODOC"
) |>
  filter(stringr::str_detect(name, ".xlsx")) |>
  pull(name)

purrr::walk(
  .x = objects,
  .f = function(object) {
    new_object <- object |>
      stringr::str_replace(".xlsx", ".parquet") |>
      stringr::str_replace("landing/EXTID-136 ODOC", "formatted")

    googleCloudStorageR::gcs_get_object(object) |>
      fastexcel::read_excel() |>
      janitor::clean_names() |>
      parquet_raw() |>
      gcs_upload_raw(
        name = new_object,
        bucket = bucket,
        token = token
      )
  }
)

object_dir <- "2026-03-13-gkff/formatted/"
sentences_data <- gcs_read_parquet(paste0(object_dir, "Sentences.parquet"))
receptions_data <- gcs_read_parquet(paste0(object_dir, "OffenderReception.parquet"))
releases_data <- gcs_read_parquet(paste0(object_dir, "OffenderExit.parquet"))
profile_data <- gcs_read_parquet(paste0(object_dir, "Profile.parquet"))
offense_data <- gcs_read_parquet(paste0(object_dir, "Offense.parquet"))
alias_data <- gcs_read_parquet(paste0(object_dir, "Alias.parquet"))


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
  # TODO: Checking uniqueness
  # It is unique by doc_num and admit date which is most improtant
  # Not unique with doc_num and each of: name (especially suffix), race,
  # facility, maybe others. Need to check this more but may not be an issue.
  # It appears the inconsitency with race which it notable is only present when
  # the jail backlog has an issue.
  # Checking missingness
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
  dplyr::mutate(snapshot_date = snapshot_date) |>
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
  ) |>
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
    current_age = floor(lubridate::time_length(lubridate::interval(.data$birth_date, lubridate::today("America/Chicago")), "years")),
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


# Analysis
dates <- tibble::tibble(
  day = seq(ymd("2024-01-01"), ymd("2025-12-31"), by = "1 day")
) |>
  dplyr::mutate(day = lubridate::floor_date(day, "hour"))

start_date <- ymd("2024-01-01")
end_date <- ymd("2025-12-31")

profile_data |>
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

# NOTE: All of this track Annual + YOY for CY24/25
# Overall population trends
profile_data |>
  filter(stringr::str_detect(facility, "(?i)jail"))
profile_data |>
  mutate(
    admit_date = if_else(
      is.na(admit_date),
      ymd("2024-01-01"),
      admit_date
    )
  ) |>
  distinct(doc_num, .keep_all = TRUE) |>
  benchCalculatePopulation::calculate_population(
    start = "admit_date",
    end = "release_date",
    groups = c("sex")
  ) |>
  mutate(n_total = sum(n), .by = "date") |>
  as_tibble()

population_data <- profile_data |>
  distinct(doc_num, admit_date, release_date, .keep_all = TRUE) |>
  benchCalculatePopulation::calculate_population(
    start = "admit_date",
    end = "release_date",
    groups = c("sex", "race")
  ) |>
  as_tibble()

# Population trends, breakdown by gender (male vs. female)
population_month_data <- profile_data |>
  distinct(doc_num, admit_date, release_date, .keep_all = TRUE) |>
  mutate(
    admit_month = lubridate::floor_date(admit_date, "month"),
    release_month = lubridate::floor_date(release_date, "month")
  ) |>
  benchCalculatePopulation::calculate_population(
    start = "admit_month",
    end = "release_month",
    groups = c("sex", "race")
  ) |>
  rename(month = date) |>
  as_tibble()

population_year_data <- profile_data |>
  distinct(doc_num, admit_date, release_date, .keep_all = TRUE) |>
  mutate(
    admit_year = lubridate::floor_date(admit_date, "month"),
    release_year = lubridate::floor_date(release_date, "month")
  ) |>
  benchCalculatePopulation::calculate_population(
    start = "admit_year",
    end = "release_year",
    groups = c("sex", "race")
  ) |>
  rename(year = date) |>
  as_tibble()

population_data <- population_data |>
  mutate(
    year = lubridate::year(date),
    month = lubridate::floor_date(date, "month")
  )

population_data <- population_data |>
  mutate(
    n_month = sum(n),
    .by = month
  ) |>
  mutate(
    n_year = sum(n),
    .by = year
  )


# Average Daily Prison Population by Year
population_data |>
  summarize(
    n = sum(n),
    .by = c("date")
  ) |>
  mutate(
    year = year(date)
  ) |>
  summarize(
    population_year_avg = mean(n),
    .by = c("year")
  )
population_data |>
  summarize(
    population_year_avg = mean(n),
    .by = c("year", "sex", "race")
  )
# Population trends, breakdown by race/ethnicity
# Population trends, breakdown by county

# Prison admission - Overall
profile_data |>
  filter(n() > 1, .by = c("doc_num", "admit_date"))

profile_data |>
  distinct(doc_num, admit_date) |>
  mutate(
    admit_month = lubridate::floor_date(admit_date, unit = "month"),
    admit_year = lubridate::year(admit_date),
  ) |>
  summarise(
    n_admissions = n(),
    .by = admit_year
  )

# Prison releases - Overall
profile_data |>
  mutate(
    release_month = lubridate::floor_date(release_date, unit = "month"),
    release_year = lubridate::year(release_date),
  ) |>
  filter(release_year < 2024)
summarise(
  n_releases = n(),
  .by = release_year
)

profile_data |>
  filter(!is.na(admit_date)) |>
  filter(n() > 1, .by = c("doc_num", "admit_date"))

# Prison sentences - Overall
sentences_data |>
  distinct(doc_num, js_date) |>
  count(
    year(js_date)
  )

sentences_data |>
  distinct(doc_num, sentence_start_date) |>
  count(
    year(sentence_start_date)
  )
# Prison sentences - breakdown by county

# Prison recidivism rate - overall
# Prison recidivism rate by gender
# Prison recidivism rate by race/ethnicity
# Prison recidivism rate by county

### Takeaways
## sentences data
# INCARCERATED_TERM_IN_YEARS is missing __, __, __. Shows only 999.
# What is the reason for lack of SENTENCE_START_DATE?
# Why do some have such high SENTENCE_END_DATE?
#

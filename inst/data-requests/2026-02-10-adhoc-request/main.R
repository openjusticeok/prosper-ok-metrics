library(googledrive)
library(dplyr)
library(fs)
library(here)
library(purrr)
library(readxl)
library(janitor)
library(lubridate)
library(ggplot2)
library(ojodb)


# Download

drive_folder_id <- googledrive::as_id("1cHigAME8-K0x1ZWE2dBydf9mRduZbf57")
drive_files <- googledrive::drive_ls(drive_folder_id) |>
  select(name, id)

data_dir <- here::here("data/input/2026-02-10-adhoc-request")
fs::dir_create(data_dir)

purrr::pwalk(
  drive_files,
  \(name, id) {
    file_path <- fs::path(data_dir, name)
    googledrive::drive_download(
      file = id,
      path = file_path,
      overwrite = TRUE
    )
  }
)


# Load

profiles <- read_xlsx(
  path = fs::path(data_dir, "profile.xlsx")
) |>
  janitor::clean_names() |>
  mutate(
    admit_date = ymd(admit_date),
    release_date = ymd(release_date)
  )

sentences <- read_xlsx(
  path = fs::path(data_dir, "sentence.xlsx")
) |>
  janitor::clean_names() |>
  mutate(
    js_date = ymd(js_date)
  )

aliases <- read_xlsx(
  path = fs::path(data_dir, "alias.xlsx")
) |>
  janitor::clean_names()


# Report Config

start_date <- ymd("2023-07-01")
end_date <- ymd("2025-06-30")


# Checks

## Profile

profiles |>
  count(fiscal_year)

sentences |>
  count(fiscal_year)

aliases |>
  count(fiscal_year)

profiles |>
  summarise(
    min_admit_date = min(admit_date, na.rm = TRUE),
    max_admit_date = max(admit_date, na.rm = TRUE),
    min_release_date = min(release_date, na.rm = TRUE),
    max_release_date = max(release_date, na.rm = TRUE)
  )

profiles |>
  count(
    missing_release_date = is.na(release_date)
  )

profiles |>
  count(
    missing_admit_date = is.na(admit_date)
  )

profiles |>
  mutate(
    release_fiscal_year = ojo_fiscal_year(release_date)
  ) |>
  count(
    fiscal_years_match = fiscal_year == release_fiscal_year
  )

profiles |>
  summarise(
    n_rows = n(),
    distinct_doc_nums = n_distinct(doc_num)
  )

profiles |>
  filter(
    .by = doc_num,
    n() > 1
  ) |>
  arrange(doc_num)

profiles |>
  filter(
    .by = doc_num,
    n() > 1
  ) |>
  arrange(doc_num) |>
  filter(
    .by = c(doc_num, admit_date),
    n() == 1
  )

### Find rows for duplicate `doc_num` and overlapping admit_date to release_date
profiles |>
  filter(
    .by = doc_num,
    n() > 1
  ) |>
  inner_join(
    profiles |>
      filter(
        .by = doc_num,
        n() > 1
      ),
    by = "doc_num",
    relationship = "many-to-many"
  ) |>
  filter(
    admit_date.x <= release_date.y,
    admit_date.y <= release_date.x,
    !(admit_date.x == admit_date.y & release_date.x == release_date.y)
  ) |>
  arrange(doc_num)

### Some duplicate release dates are in different months
profiles |>
  filter(
    .by = doc_num,
    n() > 1
  ) |>
  inner_join(
    profiles |>
      filter(
        .by = doc_num,
        n() > 1
      ),
    by = "doc_num",
    relationship = "many-to-many"
  ) |>
  filter(
    admit_date.x <= release_date.y,
    admit_date.y <= release_date.x,
    !(admit_date.x == admit_date.y & release_date.x == release_date.y)
  ) |>
  arrange(doc_num) |>
  count(
    same_month_released = floor_date(release_date.x, "month") == floor_date(release_date.y, "month")
  )

### All duplicate release dates are in same fiscal year
profiles |>
  filter(
    .by = doc_num,
    n() > 1
  ) |>
  inner_join(
    profiles |>
      filter(
        .by = doc_num,
        n() > 1
      ),
    by = "doc_num",
    relationship = "many-to-many"
  ) |>
  filter(
    admit_date.x <= release_date.y,
    admit_date.y <= release_date.x,
    !(admit_date.x == admit_date.y & release_date.x == release_date.y)
  ) |>
  arrange(doc_num) |>
  count(
    same_fy_released = ojo_fiscal_year(release_date.x) == ojo_fiscal_year(release_date.y)
  )

## Alias

semi_join(
  aliases,
  profiles,
  by = c("doc_num")
)

semi_join(
  profiles,
  aliases,
  by = c("doc_num")
)


## Sentence

### Has many-to-many due to duplicate `doc_num` values in profiles
sentences |>
  left_join(
    profiles,
    by = c("doc_num")
  )

### Take the first row of those duplicates as workaround
sentences |>
  left_join(
    profiles |>
      slice_head(
        by = doc_num,
        n = 1
      ),
    by = c("doc_num"),
    suffix = c(".sentence", ".profile")
  )

sentences |>
  left_join(
    profiles |>
      slice_head(
        by = doc_num,
        n = 1
      ),
    by = c("doc_num"),
    suffix = c(".sentence", ".profile")
  ) |>
  count(
    fiscal_years_match = fiscal_year.sentence == fiscal_year.profile
  )


# Profiles

profiles |>
  filter(
    admit_date >= start_date,
    admit_date <= end_date
  ) |>
  mutate(
    month_admitted = floor_date(admit_date, "month")
  ) |>
  count(month_admitted) |>
  ggplot(aes(x = month_admitted, y = n)) +
    geom_line() +
    scale_y_continuous(limits = \(y) range(y, 0))

profiles |>
  mutate(
    month_released = floor_date(release_date, "month")
  ) |>
  count(month_released) |>
  ggplot(aes(x = month_released, y = n)) +
    geom_line() +
    scale_y_continuous(limits = \(y) range(y, 0))

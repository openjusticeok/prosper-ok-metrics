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
library(readr)

source(here::here("R/functions/write_group_count.R"))


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


# Analysis

## Prison Releases

profiles |>
  slice_head(
    by = doc_num,
    n = 1
  ) |>
  count(fiscal_year)

profiles |>
  slice_head(
    by = doc_num,
    n = 1
  ) |>
  mutate(
    month_released = floor_date(release_date, "month")
  ) |>
  count(month_released) |>
  ggplot(aes(x = month_released, y = n)) +
    geom_line() +
    scale_y_continuous(limits = \(y) range(y, 0))


## Date Comparison: js_date vs admit_date

### Summary statistics by fiscal year alignment
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
  filter(
    !is.na(js_date),
    !is.na(admit_date)
  ) |>
  mutate(
    js_fiscal_year = ojo_fiscal_year(js_date),
    admit_fiscal_year = ojo_fiscal_year(admit_date),
    same_fiscal_year = js_fiscal_year == admit_fiscal_year
  ) |>
  count(same_fiscal_year)

### Distribution of date differences
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
  filter(
    !is.na(js_date),
    !is.na(admit_date)
  ) |>
  mutate(
    days_diff = as.numeric(js_date - admit_date),
    diff_category = factor(
      case_when(
        days_diff == 0 ~ "Same Day",
        abs(days_diff) <= 30 ~ "Within 30 days",
        abs(days_diff) <= 90 ~ "Within 90 days",
        abs(days_diff) <= 365 ~ "Within 1 year",
        TRUE ~ "> 1 year"
      ),
      levels = c("Same Day", "Within 30 days", "Within 90 days", "Within 1 year", "> 1 year")
    )
  ) |>
  count(diff_category) |>
  ggplot(aes(x = diff_category, y = n)) +
    geom_col() +
    labs(
      x = "Date Difference (js_date - admit_date)",
      y = "Count",
      title = "Distribution of Days Between Sentencing and Admission"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))


# Export

output_dir <- here::here("data/output/2026-02-10-adhoc-request/releases")
fs::dir_create(output_dir)

## Releases by month, sex, race, sentencing county
releases_summary <- profiles |>
  # Add sentence information, meaning there should not be
  # More rows after joining than before.
  left_join(
    sentences |>
      slice_head(by = doc_num, n = 1),
    by = "doc_num",
    suffix = c(".profile", ".sentence")
  ) |>
  mutate(
    month_released = floor_date(release_date, "month"),
    fiscal_year = ojo_fiscal_year(release_date),
    sentencing_county = case_when(
      is.na(sentencing_county) ~ "Unknown",
      !(sentencing_county %in% c("Tulsa", "Oklahoma")) ~ "All Other Counties",
      TRUE ~ sentencing_county
    )
  )

## Releases by fiscal year, then add sex, race, sentence, county
write_group_count(
  data = releases_summary,
  base_group_vars = c("fiscal_year"),
  other_group_vars = c("sex", "race", "sentencing_county"),
  output_dir = output_dir,
  prefix = "releases",
  value_name = "releases",
  cumulative = TRUE
)

## Releases by fiscal year and month, then add sex, race, sentence, county
write_group_count(
  data = releases_summary,
  base_group_vars = c("fiscal_year", "month_released"),
  other_group_vars = c("sex", "race", "sentencing_county"),
  output_dir = output_dir,
  prefix = "releases",
  value_name = "releases",
  cumulative = TRUE
)

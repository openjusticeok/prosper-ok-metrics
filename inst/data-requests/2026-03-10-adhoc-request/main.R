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
library(tidyr)

source(here::here("R/functions/write_group_count.R"))


# Download 2026-03-02

drive_folder_id <- googledrive::as_id("19SupcSGVm0dKBUP5rjft4kCevhEpNNiy")
drive_files <- googledrive::drive_ls(drive_folder_id) |>
  select(name, id)

data_dir_02 <- here::here("data/input/2026-03-02-adhoc-request")
fs::dir_create(data_dir_02)

purrr::pwalk(
  drive_files,
  \(name, id) {
    file_path <- fs::path(data_dir_02, name)
    googledrive::drive_download(
      file = id,
      path = file_path,
      overwrite = TRUE
    )
  }
)

# Download 2026-03-09

drive_folder_id <- googledrive::as_id("15hy_gQdIqP3OukAYsBZxOaj6XYt_KrmZ")
drive_files <- googledrive::drive_ls(drive_folder_id) |>
  select(name, id)

data_dir_09 <- here::here("data/input/2026-03-10-adhoc-request")
fs::dir_create(data_dir_09)

purrr::pwalk(
  drive_files,
  \(name, id) {
    file_path <- fs::path(data_dir_09, name)
    googledrive::drive_download(
      file = id,
      path = file_path,
      overwrite = TRUE
    )
  }
)


# Load 2026-03-02

profiles_02 <- read_xlsx(
  path = fs::path(data_dir_02, "profile.xlsx")
) |>
  janitor::clean_names()

sentences_02 <- read_xlsx(
  path = fs::path(data_dir_02, "sentence.xlsx")
) |>
  janitor::clean_names() |>
  mutate(
    js_date = ymd(js_date)
  )

aliases_02 <- read_xlsx(
  path = fs::path(data_dir_02, "alias.xlsx")
) |>
  janitor::clean_names()

receptions_02 <- read_xlsx(
  path = fs::path(data_dir_02, "reception.xlsx")
) |>
  janitor::clean_names()

releases_02 <- read_xlsx(
  path = fs::path(data_dir_02, "release.xlsx")
) |>
  janitor::clean_names()

# Load 2026-03-09

profiles_09 <- read_xlsx(
  path = fs::path(data_dir_09, "profile.xlsx")
) |>
  janitor::clean_names()

sentences_09 <- read_xlsx(
  path = fs::path(data_dir_09, "sentence.xlsx")
) |>
  janitor::clean_names() |>
  mutate(
    js_date = ymd(js_date)
  )

aliases_09 <- read_xlsx(
  path = fs::path(data_dir_09, "alias.xlsx")
) |>
  janitor::clean_names()

receptions_09 <- read_xlsx(
  path = fs::path(data_dir_09, "reception.xlsx")
) |>
  janitor::clean_names()

releases_09 <- read_xlsx(
  path = fs::path(data_dir_09, "release.xlsx")
) |>
  janitor::clean_names()


# Report Config

start_date <- ymd("2024-01-01")
end_date <- ymd("2025-12-31")


# Checks

## Profile

profiles_02 |>
  count(status)

receptions_02 |>
  count(movement_year = year(movement_date))

releases_02 |>
  count(movement_year = year(exit_date))

profiles_09 |>
  count(status)

receptions_09 |>
  count(movement_year = year(movement_date))

releases_09 |>
  count(movement_year = year(movement_date))

profiles_02 |>
  select(doc_num) |>
  distinct() |>
  mutate(in_02 = TRUE) |>
  full_join(
    profiles_09 |>
      select(doc_num) |>
      distinct() |>
      mutate(in_09 = TRUE),
    by = "doc_num"
  ) |>
  mutate(
    across(starts_with("in_"), \(x) !is.na(x))
  ) |>
  count(
    in_02,
    in_09
  )

receptions_02 |>
  summarise(
    min_reception_date = min(movement_date, na.rm = TRUE),
    max_reception_date = max(movement_date, na.rm = TRUE),
  )

receptions_09 |>
  summarise(
    min_reception_date = min(movement_date, na.rm = TRUE),
    max_reception_date = max(movement_date, na.rm = TRUE),
  )

releases_02 |>
  summarise(
    min_release_date = min(exit_date, na.rm = TRUE),
    max_release_date = max(exit_date, na.rm = TRUE),
  )

releases_09 |>
  summarise(
    min_release_date = min(movement_date, na.rm = TRUE),
    max_release_date = max(movement_date, na.rm = TRUE),
  )

only_in_09 <- profiles_09 |>
  select(doc_num) |>
  distinct() |>
  anti_join(profiles_02, by = "doc_num")

only_in_09 |>
  left_join(
    receptions_09 |> 
      select(doc_num) |> 
      distinct() |> 
      mutate(has_reception = TRUE),
    by = "doc_num"
  ) |>
  left_join(
    releases_09 |> 
      select(doc_num) |> 
      distinct() |> 
      mutate(has_release = TRUE),
    by = "doc_num"
  ) |>
  mutate(
    has_reception = coalesce(has_reception, FALSE),
    has_release = coalesce(has_release, FALSE)
  ) |>
  count(has_reception, has_release)

new_doc_nums_09 <- profiles_02 |>
  select(doc_num) |>
  distinct() |>
  mutate(in_02 = TRUE) |>
  full_join(
    profiles_09 |>
      select(doc_num) |>
      distinct() |>
      mutate(in_09 = TRUE),
    by = "doc_num"
  ) |>
  filter(is.na(in_02) & in_09 == TRUE)
  select(doc_num)

new_doc_nums_09 |>
  left_join(releases_09, by = "doc_num") |>
  mutate(
    has_release_record = !is.na(movement_date)
  ) |>
  count(has_release_record)

releases_09 |>
  filter(year(movement_date) == 2024) |>
  anti_join(
    releases_02,
    by = "doc_num"
  )

releases_02 |>
  filter(year(exit_date) == 2024) |>
  anti_join(
    releases_09 ,
    by = "doc_num"
  )

releases_2025_patched <- releases_02 |>
  rename(movement_date = exit_date) |>
  mutate(movement_date = as.Date(movement_date)) |>
  filter(year(movement_date) == 2025) |>
  distinct()

releases_09_clean <- releases_09 |>
  mutate(movement_date = as.Date(movement_date)) |>
  distinct()

master_releases <- bind_rows(
  releases_09_clean,
  releases_2025_patched
)

master_releases |>
  summarise(
    total_records = n(),
    min_release_date = min(movement_date, na.rm = TRUE),
    max_release_date = max(movement_date, na.rm = TRUE)
  ) |>
  bind_cols(
    master_releases |> count(movement_year = year(movement_date)) |> pivot_wider(names_from = movement_year, values_from = n, names_prefix = "releases_")
  )

master_receptions <- bind_rows(
  receptions_09 |> select(doc_num, movement_date),
  receptions_02 |> select(doc_num, movement_date)
) |>
  distinct(doc_num) |>
  mutate(admitted_after_jan1 = TRUE)

all_known_doc_nums <- bind_rows(
  profiles_09 |> select(doc_num),
  master_releases |> select(doc_num)
) |>
  distinct(doc_num)

baseline_pop_df <- all_known_doc_nums |>
  left_join(master_receptions, by = "doc_num") |>
  filter(is.na(admitted_after_jan1))

jan_1_baseline <- nrow(baseline_pop_df)

monthly_receptions <- receptions_09 |>
  mutate(month_year = floor_date(as.Date(movement_date), "month")) |>
  count(month_year, name = "total_receptions")

monthly_releases <- master_releases |>
  mutate(month_year = floor_date(as.Date(movement_date), "month")) |>
  count(month_year, name = "total_releases")

population_timeline <- full_join(monthly_receptions, monthly_releases, by = "month_year") |>
  arrange(month_year) |>
  mutate(
    total_receptions = coalesce(total_receptions, 0),
    total_releases = coalesce(total_releases, 0),
    net_change = total_receptions - total_releases,
    cumulative_change = cumsum(net_change),
    absolute_population = jan_1_baseline + cumulative_change
  )

monthly_receptions |>
  ggplot(aes(x = month_year, y = total_receptions)) +
    geom_line() +
    expand_limits(y = 0) +
    labs(
      title = "Total Receptions Over Time",
      subtitle = "Monthly admissions 2024-2025",
      x = NULL,
      y = "Total Receptions"
    )

monthly_releases |>
  ggplot(aes(x = month_year, y = total_releases)) +
  geom_line() +
  expand_limits(y = 0) +
  labs(
    title = "Total Releases Over Time",
    subtitle = "Monthly releases 2024-2025",
    x = NULL,
    y = "Total Releases"
  )

population_timeline |>
  ggplot(aes(x = month_year, y = absolute_population)) +
    geom_line() +
    expand_limits(y = 0) +
    labs(
      title = "Total Incarcerated Population Over Time",
      subtitle = "Monthly snapshot 2024-2025",
      x = NULL,
      y = "Total Population"
    )

# # Export

# output_dir <- here::here("data/output/2026-03-10-adhoc-request/releases")
# fs::dir_create(output_dir)

# ## Releases by month, sex, race, sentencing county
# releases_summary <- profiles |>
#   # Add sentence information, meaning there should not be
#   # More rows after joining than before.
#   left_join(
#     sentences |>
#       slice_head(by = doc_num, n = 1),
#     by = "doc_num",
#     suffix = c(".profile", ".sentence")
#   ) |>
#   mutate(
#     month_released = floor_date(release_date, "month"),
#     fiscal_year = ojodb::ojo_fiscal_year(release_date),
#     sentencing_county = case_when(
#       is.na(sentencing_county) ~ "Unknown",
#       !(sentencing_county %in% c("Tulsa", "Oklahoma")) ~ "All Other Counties",
#       TRUE ~ sentencing_county
#     )
#   )

# ## Releases by fiscal year, then add sex, race, sentence, county
# write_group_count(
#   data = releases_summary,
#   base_group_vars = c("fiscal_year"),
#   other_group_vars = c("sex", "race", "sentencing_county"),
#   output_dir = output_dir,
#   prefix = "releases",
#   value_name = "releases",
#   cumulative = TRUE
# )

# ## Releases by fiscal year and month, then add sex, race, sentence, county
# write_group_count(
#   data = releases_summary,
#   base_group_vars = c("fiscal_year", "month_released"),
#   other_group_vars = c("sex", "race", "sentencing_county"),
#   output_dir = output_dir,
#   prefix = "releases",
#   value_name = "releases",
#   cumulative = TRUE
# )


# # Sentences, Population, and Admissions
# ## Import the processed public media release data
# tar_load(prison_processed_data)

# sentence_with_profile_offense_data <- prison_processed_data$sentence_with_profile_offense_data
# profile_data <- prison_processed_data$profile_data


# ### Number of sentences
# active_sentence_with_profile_offense_data <- sentence_with_profile_offense_data |>
#   # TODO: Do I not want those who aren't in physical custody? I.e. those in community supervision?
#   # TODO: Do I want to exclude those in the interstate compact unit?
#   filter(
#     physical_custody == "TRUE",
#     status == "ACTIVE",
#     facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT"
#   )

# # Number of active sentences by snapshot date
# active_sentence_with_profile_offense_data |>
#   count(snapshot_date, name = "n_active_sentences")
# active_sentence_with_profile_offense_data |>
#   count(snapshot_date, race, name = "n_active_sentences")
# active_sentence_with_profile_offense_data |>
#   count(snapshot_date, sex, name = "n_active_sentences")

# sentence_summary <- sentence_with_profile_offense_data |>
#   filter(
#     snapshot_date == max(.data$snapshot_date)
#   ) |>
#   mutate(
#     sentencing_county = case_when(
#       is.na(sentencing_county) ~ "Unknown",
#       sentencing_county == "Other State" ~ "Other State",
#       !(sentencing_county %in% c("Tulsa", "Oklahoma")) ~ "All Other Counties",
#       TRUE ~ sentencing_county
#     ),
#     sentencing_year = sentencing_date |> lubridate::year(),
#     sentencing_fiscal_year = ojodb::ojo_fiscal_year(sentencing_date)
#   )

# active_sentence_summary <- active_sentence_with_profile_offense_data |>
#   mutate(
#     sentencing_county = case_when(
#       is.na(sentencing_county) ~ "Unknown",
#       sentencing_county == "Other State" ~ "Other State",
#       !(sentencing_county %in% c("Tulsa", "Oklahoma")) ~ "All Other Counties",
#       TRUE ~ sentencing_county
#     ),
#     sentencing_year = sentencing_date |> lubridate::year(),
#     sentencing_fiscal_year = ojodb::ojo_fiscal_year(sentencing_date)
#   )


# output_dir <- here::here("data/output/2026-02-10-adhoc-request/sentences")
# fs::dir_create(output_dir)

# write_group_count(
#   data = sentence_summary,
#   base_group_vars = c("sentencing_fiscal_year"),
#   other_group_vars = c("sex", "race", "sentencing_county"),
#   output_dir = output_dir,
#   prefix = "sentences",
#   value_name = "sentences",
#   cumulative = TRUE
# )

# write_group_count(
#   data = active_sentence_summary,
#   base_group_vars = c("sentencing_fiscal_year", "snapshot_date"),
#   other_group_vars = c("sex", "race", "sentencing_county"),
#   output_dir = output_dir,
#   prefix = "active_sentences",
#   value_name = "active_sentences",
#   cumulative = TRUE
# )


# # Population
# # TODO: Add this to processing and remove the join below
# latest_sentence_info <- sentence_with_profile_offense_data |>
#   summarise(
#     latest_sentencing_date = max(sentencing_date, na.rm = TRUE),
#     latest_sentencing_county = sentencing_county[which.max(sentencing_date)],
#     .by = c("doc_num", "snapshot_date")
#   )

# profile_summary <- profile_data |>
#   # For population we want those actively in DOC custody, and in physical
#   # custody so that we are not including those in community supervision. We also want to
#   # Exclude those in the interstate compact unit, as they are not in custody in Oklahoma.
#   # Even though they are technically in DOC custody.
#   filter(
#     physical_custody == "TRUE",
#     status == "ACTIVE",
#     facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT"
#   ) |>
#   left_join(
#     latest_sentence_info,
#     by = c("doc_num", "snapshot_date")
#   ) |>
#   mutate(
#     latest_sentencing_county = case_when(
#       is.na(latest_sentencing_county) ~ "Unknown",
#       latest_sentencing_county == "Other State" ~ "Other State",
#       !(latest_sentencing_county %in% c("Tulsa", "Oklahoma")) ~ "All Other Counties",
#       TRUE ~ latest_sentencing_county
#     )
#   )

# output_dir <- here::here("data/output/2026-02-10-adhoc-request/population")
# fs::dir_create(output_dir)
# write_group_count(
#   data = profile_summary,
#   base_group_vars = c("snapshot_date"),
#   other_group_vars = c("sex", "race", "latest_sentencing_county"),
#   output_dir = output_dir,
#   prefix = "population",
#   value_name = "population",
#   cumulative = TRUE
# )

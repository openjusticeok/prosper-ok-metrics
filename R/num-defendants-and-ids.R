library(readr)
library(stringr)
library(here)
library(janitor)
library(glue)
library(ojoverse)
library(lubridate)
library(ojodb)
library(gt)
library(tidyr)
library(purrr)

# Number of defendants and number of unique individuals for 2022 CM + CF
ojodb <- ojo_connect()

start_date <- ymd("2022-01-01")
end_date <- ymd("2023-01-01")

data_cases <- ojo_tbl("case", .con = ojodb) |>
  select(
    id,
    case_number,
    district,
    case_type,
    date_filed,
    date_closed,
    created_at,
    updated_at,
    status
  ) |>
  filter(
    case_type %in% c("CM", "CF"),
    date_filed >= start_date,
    date_filed < end_date
  ) |>
  left_join(
    ojo_tbl("count", .con = ojodb),
    by = c("id" = "case_id"),
    suffix = c("", "_count")
  ) |>
  ojo_collect()

unique_case_id_list <- data_cases |>
  distinct(id)

# number of unique id's for 2022 CM + CF
unique_case_id_list |>
  count()


parties <- data_cases |>
  left_join(
    ojo_tbl("party", .con = ojodb) |>
      filter(
        role == "Defendant",
        case_id %in% !!data_cases$id
      ) |>
      distinct() |>
      ojo_collect(),
    by = c("party" = "id")
  )

# test <- data_cases |>
#   distinct(id, .keep_all = TRUE)

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

# Number of unique oscn ids
parties |>
  distinct(oscn_id) |>
  count()

# Number of defendants
parties |>
  distinct(party) |>
  count()

# number of unique case id's for 2022 CM + CF
data_cases |>
  distinct(id) |>
  count()

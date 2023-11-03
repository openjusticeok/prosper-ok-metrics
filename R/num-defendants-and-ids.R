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
theme_set(theme_bw(base_family = "Roboto") %+replace% ojo_theme())

# Number of defendants and number of unique individuals for 2022 CM + CF
ojo_connect()

start_date <- ymd("2022-01-01")
end_date <- ymd("2023-01-01")

data_cases <- ojo_tbl("case") |>
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
  ojo_collect()
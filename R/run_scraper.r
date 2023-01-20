library(tidyverse)
library(lubridate)
devtools::load_all()

params <- ojo_tbl("case") |>
  filter(
    case_type == "CM",
    year == 2022
  ) |>
  select(
    district, case_number, date_filed
  ) |>
  arrange(desc(date_filed)) |>
  group_by(district) |>
  collect() |>
  nest() |>
  mutate(
    data = map(data, ~.x |> slice_head(n = 1))
  ) |>
  unnest(cols = data) |>
  select(district, case_number, date_filed) |>
  mutate(
    param_num = map_int(case_number, ~str_extract(., "\\d*$") |> as.integer()),
  ) |>
  ungroup() |>
  arrange(date_filed)

params


queue <- case |>
  arrange(desc(date_filed)) |>
  group_by(district) |>
  slice_head(n = 1) |>
  ungroup() |>
  select(district, case_number, date_filed) |>
  mutate(
    param_num = map_int(case_number, ~str_extract(., "\\d*$") |> as.integer()),
  ) |>
  mutate(
    district = map_chr(district, str_flatten)
  ) |>
  rbind(params) |>
  group_by(district) |>
  arrange(desc(date_filed)) |>
  slice_head(n = 1) |>
  ungroup() |>
  arrange(date_filed) |>
  mutate(next_code = glue::glue('ojo_scrape("{district}", "CM", 2022, {param_num + 1}:{param_num * 1.15})'))

queue |>
  filter(!district %in% c("ROGER MILLS", "CREEK (BRISTOW)", "MCINTOSH", "TEXAS", "JACKSON", "MCCLAIN", "BRYAN")) |>
  slice_head(n = 1) |>
  pull(next_code)

case |>
  arrange(desc(date_filed)) |>
  group_by(district) |>
  slice_head(n = 1) |>
  ungroup() |>
  select(district, case_number, date_filed) |>
  mutate(
    param_num = map_int(case_number, ~str_extract(., "\\d*$") |> as.integer()),
  ) |>
  mutate(
    district = map_chr(district, str_flatten)
  ) |>
  rbind(params) |>
  group_by(district) |>
  arrange(desc(date_filed)) |>
  slice_head(n = 1) |>
  ungroup() |>
  arrange(date_filed) |>
  filter(
    date_filed < "2022-10-01"
  )

ojo_scrape("CREEKBRISTOW", "CM", 2022, 142:150)


save.image()

ls() |>
  map(~.x |> readr::write_rds(path = "data/"))

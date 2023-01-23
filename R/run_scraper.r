library(tidyverse)
library(lubridate)
library(ojodb)

# Get a tibble with values we can pass to ojo_scrape
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

# params

# Get a tibble that has commands to download the next 15% of potential cases
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

# Pull the next command from the queue
queue |>
  filter(!district %in% c("ROGER MILLS", "CREEK (BRISTOW)", "MCINTOSH", "TEXAS", "JACKSON", "MCCLAIN", "BRYAN")) |>
  slice_head(n = 1) |>
  pull(next_code)

# Check coverage of db data + scraped data
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

# ojo_scrape("CREEKBRISTOW", "CM", 2022, 142:150)

# save.image()

# ls() |>
#   map(~.x |> readr::write_rds(path = "data/"))


view_updated_completeness <- function() {
    while (TRUE) {
      cases_in_db <- ojo_tbl("case") |>
        filter(
          case_type == "CM",
          year == 2022
        ) |>
        select(district, case_number, date_filed) |>
        collect() |>
        mutate(casenum = paste0(str_sub(case_number, 1, 8),
                                str_pad(str_extract(case_number, "\\d{1,5}$"),
                                        5, "left", 0))) |>
        select(district, casenum, date_filed) |>
        distinct()

      completeness <- cases_in_db |>
        mutate(case_seq = str_extract(casenum, "\\d{1,5}$") |>
                as.numeric(),
              file_year = str_sub(casenum, 4, 7),
              case_type = str_sub(casenum, 1, 2)) |>
        group_by(district, case_type, file_year) |>
        summarize(n_cases = n_distinct(casenum),
                  seq_max = ifelse(!all(is.na(case_seq)), max(case_seq, na.rm = T), as.double(NA)),
                  latest_case = max(date_filed, na.rm = TRUE)) |>
        drop_na(seq_max) |>
        mutate(perc_complete = n_cases/seq_max) |>
        arrange(desc(latest_case))


      print(paste0("Updated at: ", Sys.time()))
      View(completeness)
      Sys.sleep(120)
  }
}
view_updated_completeness()

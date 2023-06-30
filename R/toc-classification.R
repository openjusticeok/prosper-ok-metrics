library(ojoverse)
library(readxl)


criminal_counts_filed <- ojo_tbl("count") |>
  distinct(count_as_filed) |>
  ojo_collect() |>
  pull()

criminal_counts_disposed <- ojo_tbl("count") |>
  distinct(count_as_disposed) |>
  ojo_collect() |>
  pull()

prepare <- function(x) {
  x |>
    str_to_upper() |>
    str_remove_all("^[A-Z0-9]{0,10},") |>
    str_remove_all("(?i)IN VIOLATION.*") |>
    str_remove_all(r"{\s*\([^\)]+\)}") |>
    str_replace_all("[[:punct:][:blank:]]+", " ") |>
    str_squish()
}

criminal_dispositions <- c(
  criminal_counts_filed,
  criminal_counts_disposed
) |>
  as_tibble() |>
  rename(
    disposition = value
  ) |>
  mutate(
    disposition = prepare(disposition)
  ) |>
  distinct() |>
  drop_na() |>
  filter(disposition != "")

write_csv(criminal_dispositions, here("data/criminal_dispositions.csv"))

results <- read_xlsx(
  here("data/toc-results/82f3de5bac1c4baeb0b4736996cdf9da.xlsx"),
  sheet = 2
) |>
  select(
    disposition,
    uccs_code,
    probability
  )

write_csv(results, here("data/toc-results/disposition_predictions.csv"))

uccs <- read_xlsx(
  here("data/toc-results/82f3de5bac1c4baeb0b4736996cdf9da.xlsx"),
  sheet = 3
)

write_csv(uccs, here("data/toc-results/uccs-schema.csv"))




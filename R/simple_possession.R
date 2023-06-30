library(ojodb)
library(here)
library(readr)
library(stringr)
library(ggplot2)
library(gt)
library(lubridate)
library(tidyr)

theme_set(ojo_theme())

data <- read_csv(here("data/cm_cf_2001_2022.csv"))
lookup_table <- read_csv(here("data/toc-results/charge-lookup-table.csv"))
lookup_table_clean <- read_csv(here("data/toc-results/charge-lookup-table-cleaned.csv"))
uccs <- read_csv(here("data/toc-results/uccs-schema.csv"))

result_clean <- data |>
  # Cleaning data to match w/ cleaned lookup table
  mutate(
    # Removing code at the beginning, e.g. "[ABDOM,] Domestic Assault and Battery"
    count_as_filed = gsub("^[A-Z0-9]*,", "", toupper(count_as_filed)),
    # Removing "in violation of..."
    count_as_filed = gsub("IN VIOLATION.*", "", count_as_filed),
    # Removing (...)
    count_as_filed = gsub(r"{\s*\([^\)]+\)}","", count_as_filed),
    # Removing punctuation
    count_as_filed = gsub("[[:punct:][:blank:]]+", " ", count_as_filed),
    # all uppercase, removing ws
    count_as_filed = toupper(trimws(count_as_filed)),
  ) |>
  left_join(lookup_table_clean, by = "count_as_filed") |>
  left_join(uccs, by = "uccs_code")

d <- result_clean |>
  select(
    id,
    case_type,
    date_filed,
    count_as_filed,
    disposition,
    charge_desc,
    offense_category_desc,
    offense_type_desc,
    probability,
    disposition,
    uccs_code
  ) |>
  mutate(
    poss = str_detect(count_as_filed, "(?i)poss"),
    dist = str_detect(count_as_filed, "(?i)\\sdist"),
    intent = str_detect(count_as_filed, "(?i)intent|\\bint\\b"),
    paraphernalia = str_detect(count_as_filed, "(?i)paraph.*\\b"),
    weapon = str_detect(count_as_filed, "(?i)weapon"),
    trafficking = str_detect(count_as_filed, "(?i)traffick"),
    endeavor = str_detect(count_as_filed, "(?i)endeavor|\\bend\\b"),
    wildlife = str_detect(count_as_filed, "(?i)wildlife"),
    manufacture = str_detect(count_as_filed, "(?i)\\bmanufac"),
    commercial = str_detect(count_as_filed, "(?i)COMM FAC"),
    school_park_church = str_detect(count_as_filed, "(?i)school|education|park|church|day care|\\spk\\s"),
    conspiracy = str_detect(count_as_filed, "(?i)\\bconsp"),
    maintaining = str_detect(count_as_filed, "(?i)\\bmaint.*(place|dwelling)"),
    tax = str_detect(count_as_filed, "(?i)\\btax\\b"),
    minor = str_detect(count_as_filed, "(?i)minor|child|under.*21"),
    inmate = str_detect(count_as_filed, "(?i)inmate|jail|contrab|penal|\\bfac"),
    theft = str_detect(count_as_filed, "(?i)larceny|theft|\\bstol(e|en)\\b"),
    vehicle = str_detect(count_as_filed, "(?i)(?<!\\snon|not\\s)(?:\\sMV\\b|motor vehicle)"),
    proceeds = str_detect(count_as_filed, "(?i)ac.*\\bp(r|o){0,2}|proceed"),
    drug = str_detect(count_as_filed, "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230
  ) |>
  arrange(count_as_filed) |>
  filter(
    (uccs_drug_plus & !proceeds & !maintaining & !conspiracy & !school_park_church) |
      (poss & drug & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds)
  )

yearly_filings <- d |>
  count(
    year = floor_date(date_filed, "year")
  ) |>
  filter(
    year >= "2012-01-01",
    year < "2023-01-01"
  )

# write_csv(yearly_filings, here("data/simple_possession_filings_2012_2022.csv"))

yearly_filings |>
  ggplot(aes(x = year, y = n)) +
    geom_line() +
    expand_limits(
      y = 0
    ) +
    labs(
      title = "Simple Possession Filings by Year",
      subtitle = "Statewide, 2012 - 2022",
      x = NULL,
      y = NULL
    )



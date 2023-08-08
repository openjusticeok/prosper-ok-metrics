library(readr)
library(stringr)
library(here)
library(janitor)
library(glue)
library(ojoverse)
library(lubridate)
library(ojodb)
library(ggplot2)
library(gt)
library(lubridate)
library(tidyr)
library(purrr)
library(extrafont)
theme_set(theme_bw(base_family = "Roboto") %+replace% ojo_theme())

# Source code in prosper-ok-metrics/reports/hb2153/hb2153.qmd
parties_df <- read_csv(here("data/parties_convictions_2001_2022.csv"))

# * Number of simply possession cases charged in 2014, 2015, 2016
# * Number of simple possessions in 2021 and 2022

parties_2014_2022 <- parties_df |>
  filter(
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01",
  ) |>
  mutate(year = lubridate::year(date_filed)) |>
  distinct(party, case_type, date_filed, year) |>
  group_by(year, case_type) |>
  count()

parties_2014_2022 |>
  pivot_wider(names_from = case_type, values_from = n) |>
  mutate(
    total = CM + CF,
    statewide = round(total * 2.3778)
  ) |>
  ungroup() |>
  gt() |>
  cols_label(contains("CM") ~ "Misdemeanor",
             contains("CF") ~ "Felony",
             contains("y") ~ "Year",
             total ~ "Total (OSCN)",
             statewide ~ "Statewide Estimate"
            ) |>
  tab_header(
    title = "Annual Simple Possession Drug Convictions",
    subtitle = "Before and After SQ780 Passing"
  )

parties_2014_2022 |>
  ggplot(aes(x = year, y = n, fill = case_type)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = 2014:2022) +
  theme(axis.text.x = element_text(angle = 45, vjust=0.5)) +
  scale_y_continuous(labels = scales::comma) +
  labs(title = "Simple Possession Drug Convictions, OSCN Counties",
       subtitle = "Before and After SQ780 Passing",
       x = "Year",
       y = "Number of Convictions",
       caption = "This data is based on the total number\nof parties convicted, not the number of cases\nfiled.")

parties_2014_2022 |>
  pivot_wider(names_from = case_type, values_from = n) |>
  mutate(
    total = CM + CF,
    statewide = round(total * 2.3778)
  ) |>
  write_csv(here("data/parties_convictions_2014_2022.csv"))


# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
ojodb <- ojo_connect()

# data <- ojo_tbl("case", .con = ojodb) |>
#   select(
#     id,
#     district,
#     case_type,
#     date_filed,
#     date_closed,
#     created_at,
#     updated_at
#   ) |>
#   filter(
#     case_type %in% c("CM", "CF"),
#     date_filed >= "2001-01-01",
#     date_filed < "2023-01-01"
#   ) |>
#   left_join(
#     ojo_tbl("count", .con = ojodb),
#     by = c("id" = "case_id"),
#     suffix = c("", "_count")
#   ) |>
#   ojo_collect()
#
# write_csv(data, here("data/cm_cf_2001_2022.csv"))
data <- read_csv(here("data/cm_cf_2001_2022.csv"))

# UCCS look-up-- check for charges categorized as "property"
# will need to filter for "charge_desc" auto related crimes
charge_predictions <- read_csv(
  here("data/charge_predictions.csv")) |>
  distinct()

uccs <- read_csv(here("data/uccs-schema.csv"))

# We are limited to the 13 official OSCN counties since
# the other counties do not report the charges in each case.
oscn_county_list <- c("TULSA", "OKLAHOMA", "CLEVELAND", "ROGERS", "PAYNE",
                      "COMANCHE", "GARFIELD", "CANADIAN", "LOGAN", "ADAIR",
                      "PUSHMATAHA", "ROGER MILLS", "ELLIS")

data_oscn <- data |>
  filter(district %in% oscn_county_list)

# Clean data to join with UCCS codes
prepare <- function(x) {
  x |>
    str_to_upper() |>
    str_remove_all("^[A-Z0-9]{0,10},") |>
    str_remove_all("(?i)IN VIOLATION.*") |>
    str_remove_all(r"{\s*\([^\)]+\)}") |>
    str_replace_all("[[:punct:][:blank:]]+", " ") |>
    str_squish()
}

result_clean <- data |>
  # prepare function cleans data to match w/ cleaned lookup table
  mutate(
    count_as_filed = prepare(count_as_filed),
    # If count_as_disposed is missing, fill it with count_as_filed
    # This will retain
    count_as_disposed = if_else(
      is.na(count_as_disposed),
      count_as_filed,
      prepare(count_as_disposed)
    )
  ) |>
  # Filtering to only include OSCN counties
  filter(district %in% oscn_county_list) |>
  left_join(
    charge_predictions,
    by = c("count_as_disposed" = "charge"),
    suffix = c("", "_toc"),
    relationship = "many-to-many"
  ) |>
  left_join(uccs, by = "uccs_code")

## Gathering relevant charges
# SQ780 charges include: simple possession of controlled substances
# other drugs include ephedrine, salts, optical isomers

# larceny -- stolen property, buying or receiving stolen property,
#             concealing or withholding stolen property,
#             aiding in concealing such property, taking domesticated fish or game,
#             theft of crude oil or gas or any machinery necessary for the
#             drilling or production of oil wells, larceny of merchandise or edible meat

# property crimes --
#
# pawnbroker stuff -- failure to repay pawnbroker,
#                   operating a pawnshop without appropriate license,

# forgery/counterfeiting -- selling, exchanging, delivering a forged or counterfeited money

drug_toc <- result_clean |>
  filter(offense_type_desc=="Drug") # num of rows - 301680

property_toc <- result_clean |>
  filter(offense_type_desc=="Property") # num of rows - 329911

# find appropriate property uccs codes
property_codes <- uccs |>
  filter(offense_type_desc=="Property") |>
  filter(
    !str_detect(charge_desc, "(?i)arson|(?i)hit and run|(?i)vehicle|(?i)auto|(?i)trespass")
  ) |>
  select(uccs_code)

# 63 O.S. 2011 Section 2-402 -- simple possession
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b|(?i)salt|ephedrine"),
    !str_detect(count_as_disposed, "(?i)DOMESTIC|(?i)ASSAULT|(?i)FIREARM|(?i)DISTRIBUTE|(?i)INTENT|(?i)MANUFACTURE|(?i)DISPENSE|TUO|alcohol|acohol|alchol")
  ) |>
  View()
# include uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230

# 21 O.S. 2011 Section 1704 and 1705 -- grand and petit larceny
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)larceny|theft|\\bstol(e|en)\\b"),
    !str_detect(count_as_disposed, "(?i)gas|automobile|vehicle|LMFR|(?i)concealing")
  ) |>
  View()

# 21 O.S. 2011 Section 1713 -- buying/receiving/concealing stolen property
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)RCSP|(?i)stolen property|(?i)embezzled property|(?i)stoeln|concealing stolen")
  ) |>
  View()

# 21 O.S. 2011 Section 1719.1 -- taking domesticated fish and game
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game")
  ) |>
  View()

# 21 O.S. 2011 Section 1722 -- unlawfully taking crude oil/gas/related
result_clean |>
  filter(
    str_detect(count_as_disposed, "oil|(?i)drilling|(?i)gas")
  ) |>
  View()

# 21 O.S. 2011 Section 1731
# -- Larceny of merchandise (edible meat or other physical property)
# held for sale in retail or wholesale establishments
result_clean |>
  filter(
    str_detect(count_as_disposed, "LMFR|larceny of merchandise|meat")
  ) |>
  View()

# 21 O.S. 2011, Sections 1451 - embezzlement
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement")
  ) |>
  View()

# 21 O.S. 2011, Sections 1503 - defrauding hospitality/lodging
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)")
  ) |>
  View()

# 21 O.S. 2011, Sections 1521 - vehicle embezzlement
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)vehicle with bogus|(?i)embezzlement of a motor")
  ) |>
  View()

# 21 O.S. 2011, Sections 1541.1, 1541.2, 1541.3
result_clean |>
  filter(
    str_detect(count_as_disposed, "cheat|defraud")
  ) |>
  View()

# 59 O.S. 2011, Section 1512 -- pawn
result_clean |>
  filter(
    str_detect(count_as_disposed, "(?i)repay pawn|(?i)pawnbroker|pawn shop")
  ) |>
  View()

# 21 O.S. 2011, Section 1579 and Section 1621
result_clean |>
  filter(
    str_detect(count_as_disposed, "forgery|forged|(?i)counterfeit")
  ) |>
  View()

data_clean <- result_clean |>
  select(
    id,
    case_type,
    date_filed,
    count_as_disposed,
    disposition_date,
    disposition,
    charge_desc,
    offense_category_desc,
    offense_type_desc,
    probability,
    uccs_code,
    party
  ) |>
  mutate(
    poss = str_detect(count_as_disposed, "(?i)poss|(?i)poass of cds|(?i)poissession"), # added typos I found in `n`
    dist = str_detect(count_as_disposed, "(?i)\\bdist"),
    intent = str_detect(count_as_disposed, "(?i)intent|\\bint\\b"),
    paraphernalia = str_detect(count_as_disposed, "(?i)paraph.*\\b|(?i)\\bpara\\b|(?i)p.*nalia"), # added typos
    weapon = str_detect(count_as_disposed, "(?i)weapon"),
    trafficking = str_detect(count_as_disposed, "(?i)traffick"),
    endeavor = str_detect(count_as_disposed, "(?i)endeavor|\\bend\\b"),
    wildlife = str_detect(count_as_disposed, "(?i)wildlife"),
    manufacture = str_detect(count_as_disposed, "(?i)\\bmanufac"),
    commercial = str_detect(count_as_disposed, "(?i)COMM FAC"),
    school_park_church = str_detect(count_as_disposed, "(?i)school|education|park|church|day care|\\spk\\s"),
    conspiracy = str_detect(count_as_disposed, "(?i)\\bconsp"),
    maintaining = str_detect(count_as_disposed, "(?i)\\bmaint.*(place|dwelling)"),
    tax = str_detect(count_as_disposed, "(?i)\\btax\\b"),
    minor = str_detect(count_as_disposed, "(?i)minor|child|under.*21"),
    inmate = str_detect(count_as_disposed, "(?i)inmate|jail|contrab|penal|\\bfac"),
    vehicle = str_detect(count_as_disposed, "(?i)(?<!\\snon|not\\s)(?:\\sMV\\b|motor vehicle)"),
    proceeds = str_detect(count_as_disposed, "(?i)ac.*\\bp(r|o){0,2}|proceed"),
    larceny = str_detect(count_as_disposed, "(?i)larceny|theft|LMFR|larceny of merchandise|meat|corporeal property|\\bstol(e|en)\\b"),
    receive_stolen = str_detect(count_as_disposed, "(?i)RCSP|(?i)stolen property|(?i)stoeln|concealing stolen"),
    embezzle = str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement"),
    misc = str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game|oil|(?i)drilling|(?i)gas"),
    pawn = str_detect(count_as_disposed, "(?i)repay pawn|(?i)pawnbroker|pawn shop"),
    hospitality = str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)"),
    forgery = str_detect(count_as_disposed, "forgery|forged|(?i)counterfeit"),
    drug = str_detect(count_as_disposed, "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230,
    uccs_property = probability >= 0.8 & uccs_code %in% property_codes$uccs_code
  ) |>
  arrange(count_as_disposed)

# 407675
clean_df <- data_clean |>
  filter(
    (uccs_drug_plus & !intent & !proceeds & !maintaining & !conspiracy & !school_park_church) |
      (poss & drug & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (uccs_property & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (larceny & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds)|
      (receive_stolen & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds)|
      (embezzle & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (misc & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (pawn & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (hospitality & !dist & !intent & !paraphernalia & !weapon & !trafficking &
        !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
        !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds) |
      (forgery & !dist & !intent & !paraphernalia & !weapon & !trafficking &
         !endeavor & !wildlife & !manufacture & !commercial & !school_park_church &
         !conspiracy & !maintaining & !minor & !inmate & !vehicle & !proceeds)
    )

# check uccs codes for  property charges



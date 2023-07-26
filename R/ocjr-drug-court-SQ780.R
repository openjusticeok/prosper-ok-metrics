library(readr)
library(stringr)
library(tidyverse)
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

data <- ojo_tbl("case", .con = ojodb) |>
  select(
    id,
    district,
    case_type,
    date_filed,
    date_closed,
    created_at,
    updated_at
  ) |>
  filter(
    case_type %in% c("CM", "CF"),
    date_filed >= "2001-01-01",
    date_filed < "2023-01-01"
  ) |>
  left_join(
    ojo_tbl("count", .con = ojodb),
    by = c("id" = "case_id"),
    suffix = c("", "_count")
  ) |>
  ojo_collect()

write_csv(data, here("data/cm_cf_2001_2022.csv"))

# We are limited to the 13 official OSCN counties since 
# the other counties do not report the charges in each case.
oscn_county_list <- c("TULSA", "OKLAHOMA", "CLEVELAND", "ROGERS", "PAYNE",
                      "COMANCHE", "GARFIELD", "CANADIAN", "LOGAN", "ADAIR",
                      "PUSHMATAHA", "ROGER MILLS", "ELLIS")

# 63 O.S. 2011 Section 2-402 -- simple posession
data |> 
  filter(
    str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b|(?i)salt|ephedrine"), 
    !str_detect(count_as_disposed, "(?i)DOMESTIC|(?i)ASSAULT|(?i)FIREARM|(?i)DISTRIBUTE|(?i)INTENT|(?i)MANUFACTURE|(?i)DISPENSE|TUO|alcohol|acohol|alchol")
  ) |> 
  view()

# 21 O.S. 2011 Section 1704 and 1705 -- grand and petit larceny
data |> 
  filter(
    str_detect(count_as_disposed, "(?i)larceny|theft|\\bstol(e|en)\\b"), 
    !str_detect(count_as_disposed, "(?i)gas|automobile|vehicle|LMFR|(?i)concealing")
  ) |> 
  view()

# 21 O.S. 2011 Section 1713 -- buying/receiving/concealing stolen property
data |> 
  filter(
    str_detect(count_as_disposed, "(?i)RCSP|(?i)stolen property|(?i)embezzled property|(?i)stoeln|concealing stolen")
  ) |> 
  view()

# 21 O.S. 2011 Section 1719.1 -- taking domesticated fish and game
data |> 
  filter(
    str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game")
  ) |> 
  view()

# 21 O.S. 2011 Section 1722 -- unlawfully taking crude oil/gas/related
data |> 
  filter(
    str_detect(count_as_disposed, "oil|(?i)drilling|(?i)gas")
  ) |> 
  view()

# 21 O.S. 2011 Section 1731 
# -- Larceny of merchandise (edible meat or other physical property)
# held for sale in retail or wholesale establishments 
data |> 
  filter(
    str_detect(count_as_disposed, "LMFR|larceny of merchandise|meat|corporeal property")
  ) |> 
  view()

# 59 O.S. 2011, Section 1512 -- pawn
data |> 
  filter(
    str_detect(count_as_disposed, "(?i)repay pawn|(?i)pawnbroker|pawn shop")
  ) |> 
  view()

# 21 O.S. 2011, Section 1579 and Section 1621
data |> 
  filter(
    str_detect(count_as_disposed, "forgery|forged|(?i)counterfeit")
  ) |> 
  view()

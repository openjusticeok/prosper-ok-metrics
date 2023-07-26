library(ojodb)
library(here)
library(readr)
library(stringr)
library(ggplot2)
library(gt)
library(lubridate)
library(tidyr)
library(purrr)
library(extrafont)

ojodb <- ojo_connect()

# Pulling the number of suspended sentences for CMs / CFs in the 13 OSCN counties
oscn_county_list <- c(
  "TULSA",
  "OKLAHOMA",
  "CLEVELAND",
  "ROGERS",
  "PAYNE",
  "COMANCHE",
  "GARFIELD",
  "CANADIAN",
  "LOGAN",
  "ADAIR",
  "PUSHMATAHA",
  "ROGER MILLS",
  "ELLIS"
)

data_case <- ojo_tbl("case", .con = ojodb) |>
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

oscn_cases <- data |>
  filter(district %in% oscn_county_list)

# Pulling relevant minute tables
data_minutes <- ojo_tbl("minute") |>    
  filter(date >= cumulative_period_start,
          date < reporting_period_end) |>
   select(id,
          case_id,
          date,
          code,
          description,
          count,
          amount) |>
  filter(str_detect(description, "(?i)suspended")
# !str_detect(description, min_desc_exclude)
) |>
  ojo_collect()
#
# minute_ids <- minute_old_method |>
#   distinct(case_id)
#
# # None of the case id's for minutes related to drivers license suspension among
# # appear among misdemeanors filtered for drugs charges.
# # 'case_misdemeanor_drug' filters for strings in "drugs" and excludes strings in "drug exclude"
# # drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
# # drug_exclude <- "FIREARM|DISTRIBUTE|INTENT|MANUFACTURE|DISPENSE|TUO"
# case_misdemeanor_drug |>
#    filter(!id %in% minute_ids)
#
# # A little over 200 case_ids in "case_misdemeanor_drug" are in minutes pulled
# # using the "old" dl suspension method.
# # ~9000 of the case_ids in "case_misdemeanor_drug" are not in minutes pulled using the "old" method
# join_misd_min <- minute_ids |>
#   anti_join(
#     case_misdemeanor_drug,
#     by = c("case_id" = "id"))
#
# minute_old_method |>
#   inner_join(
#     case_misdemeanor_drug,
#     by = c("case_id" = "id"))
# Extrapolate to state-wide


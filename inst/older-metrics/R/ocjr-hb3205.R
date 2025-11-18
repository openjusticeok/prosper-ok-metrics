library(readr)
library(stringr)
library(readxl)
library(tidycensus)
library(tidyverse)
library(here)
library(janitor)
library(glue)
library(ojoverse)
library(lubridate)

# ProsperOK
# requested estimates of the number of Oklahoman families impacted by 3205
# Using OJA data and ACS survey estimates we pull counts on a quarterly basis
# and multiply the counts by the avg. household size and avg. family size.

# Retrieve ACS data on avg. household size by county

v21 <- load_variables(2021, "acs5/subject", cache = TRUE) |>
  filter(str_detect(concept, "HOUSEHOLD"))

# variable for avg. household size is S1101_C01_002
# variable for avg. family size is S1101_C01_004
# ACS notes:
# Average family size is derived by dividing the number of related people in
# households by the number of family households.

hh_estimate <- get_acs(geography = "county",
                       survey = "acs5",
                       variable = "S1101_C01_002",
                       state = "OK",
                       year = 2021,
                       cache = TRUE) |>
  mutate(
    NAME = str_extract(NAME, "^(.*)(?=\\sCounty,\\sOklahoma$)") |>
      str_to_title()
  ) |>
  rename(
    county = NAME
  )

hh_estimate_state <- get_acs(geography = "state",
                       survey = "acs5",
                       variable = "S1101_C01_002",
                       state = "OK",
                       year = 2021,
                       cache = TRUE)

family_estimate <- get_acs(geography = "county",
                       survey = "acs5",
                       variable = "S1101_C01_004",
                       state = "OK",
                       year = 2021,
                       cache = TRUE) |>
  mutate(
    NAME = str_extract(NAME, "^(.*)(?=\\sCounty,\\sOklahoma$)") |>
      str_to_title()
  ) |>
  rename(
    county = NAME
  )

family_estimate_state <- get_acs(geography = "state",
                           survey = "acs5",
                           variable = "S1101_C01_004",
                           state = "OK",
                           year = 2021,
                           cache = TRUE)

# data shared in 2023: OJA data with monthly referrals 2018-2022
referrals <- read_excel(here("data", "YoungOffenders.xlsx"), sheet = "Referrals") |>
  janitor::clean_names() |>
  rename(
    age = new_age,
    class = arrest_class
  ) |>
  mutate(
    fiscal_year = ojo_fiscal_year(
      lubridate::as_date(
        year,
        format = "%Y"
      )
    ),
    quarter = quarter(
      lubridate::as_date(
        month,
        format = "%Y/%m"
      )
    )
  ) |>
  mutate(
    fiscal_quarter = case_when(
      quarter == 1 ~ 3,
      quarter == 2 ~ 4,
      quarter == 3 ~ 1,
      quarter == 4 ~ 2,
      TRUE ~ NA_real_
    )
  )

# aggregate for quarterly reporting
county_referrals_quarter <- referrals |>
  group_by(
    county,
    fiscal_year,
    fiscal_quarter
  ) |>
  count() |>
  rename(
    num_referred = n
  )

state_referrals_quarter <- referrals |>
  group_by(
    fiscal_year,
    fiscal_quarter
  ) |>
  count() |>
  rename(
    num_referred = n
  )

# Reporting periods look at data starting from 2022
# will need to access OJA data on a quarterly basis

total_state <- state_referrals_quarter |>
  filter(
    fiscal_year %in% c(2022, 2023)
  )

report_hh <- (sum(total_state$num_referred) * (hh_estimate_state$estimate)) |>
  round()

report_family <- (sum(total_state$num_referred) * (family_estimate_state$estimate)) |>
  round()

county_referrals_quarter |>
  ungroup() |>
  left_join(
    hh_estimate,
    by = "county"
  ) |>
  mutate(
    impact_estimate = num_referred * estimate
  ) |>
  filter(
    fiscal_year %in% c(2022, 2023)
  ) |>
  summarise(
    total_household_impact_estimate = sum(impact_estimate, na.rm = TRUE) |>
      round()
  )

county_referrals_quarter |>
  ungroup() |>
  left_join(
    family_estimate,
    by = "county"
  ) |>
  mutate(
    impact_estimate = num_referred * estimate
  ) |>
  filter(
    fiscal_year %in% c(2022, 2023)
  ) |>
  summarise(
    total_family_impact_estimate = sum(impact_estimate, na.rm = TRUE) |>
      round()
  )

# ______________________________________________________________________________
# old code from qmd report - 2023
county_referrals_quarter <- referrals |>
  group_by(
    county,
    month,
    quarter,
    fiscal_year,
    fiscal_quarter
  ) |>
  count() |>
  rename(
    num_referred = n
  ) |>
  ungroup() |>
  left_join(
    hh_estimate |>
      select(
        county,
        household_size = estimate
      ),
    by = "county"
  ) |>
  left_join(
    family_estimate |>
      select(
        county,
        family_size = estimate
      ),
    by = "county"
  ) |>
  mutate(
    household_impact_estimate = num_referred * household_size,
    family_impact_estimate = num_referred * family_size
  )
#
# gt(county_referrals_quarter) |>
#   cols_label(
#     county = "County",
#     month = "Month",
#     quarter = "Quarter Start",
#     fiscal_year = "Fiscal Year",
#     fiscal_quarter = "Fiscal Quarter",
#     num_referred = "Referrals",
#     household_size = "Average Household Size",
#     household_impact_estimate = "Impacted Oklahomans (Low)",
#     family_size = "Average Family Size",
#     family_impact_estimate = "Impacted Oklahomans (High)"
#   ) |>
#   fmt_number(
#     columns = ends_with("_estimate"),
#     decimals = 0
#   ) |>
#   tab_footnote(
#     footnote = "American Community Survey 5-Year Data (2009-2021), Households and Families (S1101)",
#     locations = cells_column_labels(
#       columns = c(household_size, family_size)
#     )
#   ) |>
#   tab_source_note(
#     county_referrals_quarter |>
#       download_this(
#         output_name = "oja_referrals_county_quarterly",
#         output_extension = ".csv"
#       )
#   ) |>
#   tab_options(
#     ihtml.active = TRUE,
#     ihtml.use_filters = TRUE,
#     ihtml.use_highlight = TRUE,
#     ihtml.use_text_wrapping = TRUE,
#     quarto.use_bootstrap = TRUE
#   )

county_referrals_quarter |>
  filter(
    quarter >= "2022-10-01"
  ) |>
  group_by(
    fiscal_year,
    fiscal_quarter,
    quarter
  ) |>
  summarise(
    total_referred = sum(num_referred, na.rm = TRUE),
    household_impact_estimate = sum(household_impact_estimate, na.rm = TRUE),
    family_impact_estimate = sum(family_impact_estimate, na.rm = TRUE)
  ) |>
  ungroup() |>
  gt() |>
  grand_summary_rows(
    columns = c(
      total_referred,
      household_impact_estimate,
      family_impact_estimate
    ),
    fns = list(
      label = "Total",
      id = "total",
      fn = "sum"
    ),
    fmt = ~ fmt_number(., decimals = 0)
  ) |>
  cols_label(
    quarter = "Quarter Start",
    fiscal_year = "Fiscal Year",
    fiscal_quarter = "Fiscal Quarter",
    total_referred = "Referrals",
    household_impact_estimate = "Household Impact",
    family_impact_estimate = "Family Impact"
  ) |>
  fmt_number(
    columns = c("total_referred", "household_impact_estimate", "family_impact_estimate"),
    decimals = 0
  )

#_______________________________________________________________________________
## Correcting overestimate and assuming that each referral id represents one person (this is not always true).
# The previous code treated each row of data as a unique individual not taking into account that each
#referral id represents a "case" and each row is an individual charge.

unique_county_referrals_quarter <- referrals |>
  distinct(referral_id, .keep_all = TRUE) |>
  group_by(
    county,
    month,
    quarter,
    fiscal_year,
    fiscal_quarter
  ) |>
  count() |>
  rename(
    num_referred = n
  ) |>
  ungroup() |>
  left_join(
    hh_estimate |>
      select(
        county,
        household_size = estimate
      ),
    by = "county"
  ) |>
  left_join(
    family_estimate |>
      select(
        county,
        family_size = estimate
      ),
    by = "county"
  ) |>
  mutate(
    household_impact_estimate = num_referred * household_size,
    family_impact_estimate = num_referred * family_size
  )

# gt(unique_county_referrals_quarter) |>
#   cols_label(
#     county = "County",
#     month = "Month",
#     quarter = "Quarter Start",
#     fiscal_year = "Fiscal Year",
#     fiscal_quarter = "Fiscal Quarter",
#     num_referred = "Referrals",
#     household_size = "Average Household Size",
#     household_impact_estimate = "Impacted Oklahomans (Low)",
#     family_size = "Average Family Size",
#     family_impact_estimate = "Impacted Oklahomans (High)"
#   ) |>
#   fmt_number(
#     columns = ends_with("_estimate"),
#     decimals = 0
#   ) |>
#   tab_footnote(
#     footnote = "American Community Survey 5-Year Data (2009-2021), Households and Families (S1101)",
#     locations = cells_column_labels(
#       columns = c(household_size, family_size)
#     )
#   ) |>
#   tab_source_note(
#     county_referrals_quarter |>
#       download_this(
#         output_name = "oja_referrals_county_quarterly",
#         output_extension = ".csv"
#       )
#   ) |>
#   tab_options(
#     ihtml.active = TRUE,
#     ihtml.use_filters = TRUE,
#     ihtml.use_highlight = TRUE,
#     ihtml.use_text_wrapping = TRUE,
#     quarto.use_bootstrap = TRUE
#   )

## Testing unique referral counts
unique_county_referrals_quarter |>
  filter(
    quarter >= "2022-10-01"
  ) |>
  group_by(
    fiscal_year,
    fiscal_quarter,
    quarter
  ) |>
  summarise(
    total_referred = sum(num_referred, na.rm = TRUE),
    household_impact_estimate = sum(household_impact_estimate, na.rm = TRUE),
    family_impact_estimate = sum(family_impact_estimate, na.rm = TRUE)
  ) |>
  ungroup() |>
  gt() |>
  grand_summary_rows(
    columns = c(
      total_referred,
      household_impact_estimate,
      family_impact_estimate
    ),
    fns = list(
      label = "Total",
      id = "total",
      fn = "sum"
    ),
    fmt = ~ fmt_number(., decimals = 0)
  ) |>
  cols_label(
    quarter = "Quarter Start",
    fiscal_year = "Fiscal Year",
    fiscal_quarter = "Fiscal Quarter",
    total_referred = "Referrals",
    household_impact_estimate = "Household Impact",
    family_impact_estimate = "Family Impact"
  ) |>
  fmt_number(
    columns = c("total_referred", "household_impact_estimate", "family_impact_estimate"),
    decimals = 0
  )

#_______________________________________________________________________________



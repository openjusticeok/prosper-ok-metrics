library(ojodb)
library(tidyverse)
library(lubridate)
library(here)
library(glue)
library(ojoverse)
library(lubridate)

drugs <- "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|MARIJUANA|MARIHUANA|MJ"
# PARAPHERNALIA|
drug_exclude <- "(?i)DOMESTIC|ASSAULT|FIREARM|DISTRIBUTE|INTENT|MANUFACTURE|DISPENSE|TUO|alcohol|acohol|alchol"
# REVOCATION|SUSPENSION|
vehicle_related <- "VEHICLE"

# DPS violation codes saved in "docs" along with bill
dps_codes <- "(?i)DI1|DI1M|DI2D|DI2M|DI2DM|DRI|DR3|DU2II|DU2IV|DU4|DU4II|DU8I|DU8II|DU9|DU9I|DU9II|DU9IV|dui drugs misdemeanor"
dps_exclude <- "(?i)alcohol|acohol|alchol|under 21|suspended|revoked|valid|twenty-one"
dps_related <- "(DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"

min_desc_dl <- "(SUSPENSION|SUSPEND)"
min_desc_exclude <- "(WITHDRAW|ERROR|RETURN|UNABLE|REVOKE|LIFT|RECALL|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"

# Set to reporting period
# Replace with fiscal quarter using ojo_fiscal_year
cumulative_period_start <- ymd("2022-01-01")
reporting_period_start <- ymd("2023-01-01")
reporting_period_end <- ymd("2023-06-01")

# All misdemeanors
all_cases <- ojo_crim_cases(case_types = "CM",
                            file_years = 2022:2023) |>
  filter(
    date_filed >= cumulative_period_start,
    date_filed < reporting_period_end
  ) |>
  ojo_collect()

# Defendants for all cases
ojo_tbl("party") |>
  right_join(
    all_cases |>
      distinct(id),
    by = c("case_id" = "id"),
    copy = TRUE
  ) |>
  filter(role == "Defendant") |>
  count()

# Pulling every case with at least one misdemeanor drug charge
# (excluding distribution/intent to dispense/etc.)
case_misdemeanor_drug <- all_cases |>
  filter(
    str_detect(count_as_filed, drugs)
  ) |>
  filter(
    !str_detect(count_as_filed, drug_exclude)
  )

# Defendants in cases above:
ojo_tbl("party") |>
  right_join(
    case_misdemeanor_drug |>
      distinct(id),
    by = c("case_id" = "id"),
    copy = TRUE
  ) |>
  filter(role == "Defendant") |>
  count()

# Check what unique filing charges are being excluded when
# filtering misdemeanor for drug charges.
all_cases |>
  anti_join(
    case_misdemeanor_drug,
    by = "id"
  ) |>
  count(
    count_as_filed,
    sort = T
  ) |>
  view()

# A conservative estimate of the number of impacted individuals can be made by
# filtering misdemeanors by DPS violation code and related codes.
# For the most part, this excludes drug offenses occurring outside of motor vehicle
case_dps_violation <- all_cases |>
  filter(str_detect(count_as_filed, dps_codes),
         !str_detect(count_as_filed, dps_exclude))

## Above you are counting number of charges. 
# This calculates number of defendants:
ojo_tbl("party") |>
  right_join(
    case_dps_violation |>
      distinct(id),
    by = c("case_id" = "id"),
    copy = TRUE
  ) |>
  filter(role == "Defendant") |>
  count()


all_cases |>
  anti_join(
    case_dps_violation,
    by = "id"
  ) |>
  count(
    count_as_filed,
    sort = T
  ) |>
  view()

# manual check
list_all_counts <- all_cases |>
  count(count_as_filed)

list_drug_counts <- case_misdemeanor_drug |>
  count(count_as_filed)

list_dps <- case_dps_violation |>
  count(count_as_filed)

# unique ids
cm_ids <- case_misdemeanor_drug |>
  pull(id) |>
  unique()

dps_ids <- case_dps_violation |>
  pull(id) |>
  unique()

# Pulling relevant minute tables
minute_old_method <- ojo_tbl("minute") |>
  filter(date >= cumulative_period_start,
         date < reporting_period_end) |>
  select(id,
         case_id,
         date,
         code,
         description,
         count,
         amount) |>
  filter(code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT", "CNOTE",
                     "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
         str_detect(description, min_desc_dl),
         !str_detect(description, min_desc_exclude)) |>
  ojo_collect()

minute_ids <- minute_old_method |>
  distinct(case_id)

# None of the case id's for minutes related to drivers license suspension among
# appear among misdemeanors filtered for drugs charges.
# 'case_misdemeanor_drug' filters for strings in "drugs" and excludes strings in "drug exclude"
# drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
# drug_exclude <- "FIREARM|DISTRIBUTE|INTENT|MANUFACTURE|DISPENSE|TUO"
case_misdemeanor_drug |>
   filter(!id %in% minute_ids)

# A little over 200 case_ids in "case_misdemeanor_drug" are in minutes pulled
# using the "old" dl suspension method. 
# ~9000 of the case_ids in "case_misdemeanor_drug" are not in minutes pulled using the "old" method
join_misd_min <- minute_ids |>
  anti_join(
    case_misdemeanor_drug,
    by = c("case_id" = "id"))

minute_old_method |>
  inner_join(
    case_misdemeanor_drug,
    by = c("case_id" = "id"))

# Following the same process for cases filtered using dps_violation codes
# None of the cases in minute_ids using "old" method
case_dps_violation |>
  filter(!id %in% minute_ids)

join_dps_min <- minute_ids |>
  anti_join(
    case_dps_violation,
    by = c("case_id" = "id"))

minute_old_method |>
  inner_join(
    case_dps_violation,
    by = c("case_id" = "id"))

# Other relevant minutes?
minute_method_two <- ojo_tbl("minute") |>
  filter(date >= cumulative_period_start,
         date < reporting_period_end) |>
  select(id,
         case_id,
         date,
         code,
         description,
         count,
         amount) |>
  filter(code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT", "CNOTE",
                     "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
         str_detect(description, dps_codes),
         !str_detect(description, dps_exclude)) |>
  ojo_collect()

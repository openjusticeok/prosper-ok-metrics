library(ojodb)
library(tidyverse)
library(lubridate)
library(here)
library(glue)
library(ojoverse)
library(lubridate)

drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
drug_exclude <- "FIREARM|DISTRIBUTE|INTENT|MANUFACTURE|DISPENSE|REVOCATION|SUSPENSION|TUO"
vehicle_related <- "VEHICLE"

# DPS violation codes saved in docs along with bill 
dps_codes <- "(?i) DI1|DI1M|DI2D|DI2M|DI2DM|DRI|DR3|DU2II|DU2IV|DU4|DU4II|DU8I|DU8II|DU9|DU9I|DU9II|DU9IV"
dps_exclude <- "(?i)alcohol"

dps_related <- "(DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"


min_desc_dl <- "(SUSPENSION|SUSPEND)"
min_desc_exclude <- "(WITHDRAW|ERROR|RETURN|UNABLE|REVOKE|LIFT|RECALL|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"

# Set to reporting period 
# Replace with fiscal quarter using ojo_fiscal_year
reporting_start_date <- ymd("2022-01-01")
reporting_end_date <- ymd("2023-04-01")

# All misdemeanors 
all_cases <- ojo_crim_cases(case_types = "CM",
                            file_years = 2022:2023) |>
  filter(
    date_filed >= reporting_start_date,
    date_filed < reporting_end_date
  ) |>
  ojo_collect()

# Pulling every case with at least one misdemeanor drug charge
# (excluding distribution/intent to dispense/etc.)
# ~17000
case_misdemeanor_drug <- ojo_crim_cases(case_types = "CM",
                     file_years = 2022:2023) |>
  filter(date_filed >= reporting_start_date,
         date_filed < reporting_end_date,
         str_detect(count_as_filed, drugs),
         !str_detect(count_as_filed, drug_exclude)
         ) |>
  ojo_collect()

# Check what unique filing charges are being excluded when 
# filtering misdemeanor for drug charges
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
# ~14600
case_dps_violation <- ojo_crim_cases(case_types = "CM",
                         file_years = 2022:2023) |>
  filter(date_filed >= reporting_start_date,
         date_filed < reporting_end_date,
         str_detect(count_as_filed, dps_codes), 
         !str_detect(count_as_filed, dps_exclude)) |>
  ojo_collect()

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

# Look for cases that have vehicle mentioned in count_as_filed:
list_all_counts <- all_cases |>
  group_by(count_as_filed) |>
  count()

list_drug_counts <- case_misdemeanor_drug |>
  group_by(count_as_filed) |>
  count()

list_dps <- case_dps_violation |>
  group_by(count_as_filed) |>
  count()

# unique ids
cm_ids <- case_misdemeanor_drug |>
  pull(id) |>
  unique()

dps_ids <- case_dps_violation |>
  pull(id) |>
  unique()

# 116067
minute_old_method <- ojo_tbl("minute") |>
  filter(date >= reporting_start_date,
         date < reporting_end_date) |>
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
         !str_detect(description, min_desc_exclude))

minute_ids <- minute_old_method |> 
  distinct(case_id) |> 
  ojo_collect()

  

minute_dps_violation <- ojo_tbl("minute") |>
  filter(case_id %in% dps_ids,
         date >= reporting_start_date,
         date < reporting_end_date) |>
  select(id,
         case_id,
         date,
         code,
         description,
         count,
         amount) |>
  collect()

#min_ids <- unique(min$case_id)
min_misd_ids <- min |>
  pull(case_id) |>
  unique()

join_misd_min <- min |>
  anti_join(
    case_misdemeanor_drug,
    by = c("case_id" = "id"))

min |>
  filter(str_detect(description, drugs),
         !str_detect(description, count_exclude)
  )

# Shortcut to use ojo_add_minutes ?
# Question: why do rows repeat?
case_minute_dps_short <- ojo_crim_cases(case_types = "CM",
                     file_years = 2022:2023) |>
  ojo_add_minutes(case_id %in% dps_ids) |>
  ojo_collect() #|>
  #distinct(case_id)


# Then filter the "universe of minutes" to include only case_ids that are
# in the relevant cases of interest.

# -> Exclude cases where individual is driving with a suspended license
# -> Don't include separate condition for cases mentioning drivers license
# suspension in the minute description
drug_dl_cm <- local_cm_data |>
  filter(str_detect(count_as_filed, drugs),
         !str_detect(count_as_filed, count_exclude),
         #!str_detect(count_as_filed, "SUSPEND|SUSPENDED|LICENSE"),
         str_detect(description, dps_related),
         #!str_detect(description, exclude)
         )

# of these cases how many resulted in a conviction, dismissal, or are ongoing?
drug_dl_cm |>
  group_by(disposition) |>
  count()



# regex from Brancen's "Tulsa County Driver's License Suspensions" report
drug_charge = if_else(grepl(drugs, count_as_filed), TRUE, FALSE)

filter(min_code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT",
                       "CNOTE", "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
       str_detect(min_desc, "SUSPENSION"),
       !str_detect(min_desc, "(WITHDRAW|ERROR|RETURN|REVOKE|UNABLE|LIFT|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"))


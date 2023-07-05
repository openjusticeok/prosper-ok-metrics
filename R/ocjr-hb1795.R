library(ojodb)
library(tidyverse)
library(lubridate)
library(here)
library(glue)  
library(ojoverse)
library(lubridate)

# notes from call
# cases_of_interest <- ojo_tbl("case") |> filters to limit to misd drug charges
# ojo_tbl("minute") |> 
# all our filters for dl susp |> filter(case_id %in% !!cases_of_interest$id)

# DPS violation codes: https://oklahoma.gov/content/dam/ok/en/dps/VCB%20February%202022%20w%20corrections.pdf
drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
dps_codes <- "(?i) DRIVING|DI0|DI1|DI1M|DI2|DI2D|DI2DM|DI3|DI5|DI6|DI7|DI8|DL8|DR6|DR3|DR3MV"
drug_exclude <- "DISTRIBUTE|INTENT|MANUFACTURE|DISPENSE"
vehicle_related <- "VEHICLE"
# 47 O.S. 2011|
dps_related <- "(DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"
exclude <- "(WITHDRAW|ERROR|RETURN|UNABLE|REVOKE|LIFT|RECALL|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"
reporting_start_date <- ymd("2022-01-01")
reporting_end_date <- ymd("2023-04-01")

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
  collect()
  
# Pulling misdemeanor drug related cases by DPS violation code
# ~11100
# around 9000 of these are unique ids
case_dps_violation <- ojo_crim_cases(case_types = "CM",
                         file_years = 2022:2023)|> 
  filter(date_filed >= reporting_start_date, 
         date_filed < reporting_end_date,
         str_detect(count_as_filed, dps_codes)) |> 
  collect()

# look for cases that have vehicle mentioned in count_as_filed ?
list_counts <- case_misdemeanor_drug |> 
  group_by(count_as_filed) |> 
  count()

list_dps <- case_dps_violation |> 
  group_by(count_as_filed) |> 
  count()


# Style question: is it better to use pull() in place of $?
#cm_ids <- unique(case_m$id) 
# 11693
cm_ids <- case_misdemeanor_drug |> 
  pull(id) |> 
  unique() 

# 8967
dps_ids <- case_dps_violation |> 
  pull(id) |> 
  unique()

#116067
minute_drug_misd <- ojo_tbl("minute") |> 
  filter(case_id %in% cm_ids,     
         date >= reporting_start_date,
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
         #str_detect(description, "SUSPENSION"),
         !str_detect(description, "(WITHDRAW|ERROR|RETURN|UNABLE|LIFT|NOT PROCESSED)")
         ) |> 
  collect()

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
  left_join(
    case_misdemeanor_drug, 
    by = c("case_id" = "id")
    # , 
    # relationship = "many-to-many"
  )

min |> 
  filter(str_detect(description, drugs),
         !str_detect(description, count_exclude)
  )

# Shortcut?
# Question: why do rows repeat?
case_minute_dps_short <- ojo_crim_cases(case_types = "CM", 
                     file_years = 2022:2023) |> 
  ojo_add_minutes(case_id %in% dps_ids) |> 
  collect() #|> 
  #distinct(case_id)


# readr::write_csv(cm, here("data/misdemeanor_local.csv"))
# local_cm_data <- read_csv(here("data", "misdemeanor_local.csv")) |>
#   janitor::clean_names()

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
  count() |> 
  print(n = 60)


test <- drug_cm |> filter(
  str_detect(description, "SUSPEND"))

# code from existing repos below 
################ 

# regex from Brancen's "Tulsa County Driver's License Suspensions" report
drug_charge = if_else(grepl(drugs, count_as_filed), TRUE, FALSE)

filter(min_code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT",
                       "CNOTE", "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
       str_detect(min_desc, "SUSPENSION"),
       !str_detect(min_desc, "(WITHDRAW|ERROR|RETURN|REVOKE|UNABLE|LIFT|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"))

# from Andrew's dl-revocation code
# Grouping / summarizing the charges by case
cm_cases <- cm_charges |>
  group_by(case_number, district) |>
  summarize(
    file_date = unique(date_filed),
    file_year = unique(file_year),
    quarter_fy = unique(quarter_fy),
    quarter_fy_start_date = unique(quarter_fy_start_date),
    pre_november = if_else(sum(pre_november, na.rm = T) > 0, TRUE, FALSE),
    n_charges = n(),
    drug_charge_present = if_else(sum(drug_charge, na.rm = T) > 0, TRUE, FALSE),
    all_drugs = if_else(sum(drug_charge, na.rm = T) == n_charges, TRUE, FALSE),
    charges_list = paste(count_as_filed, collapse = "; ")
  ) |>
  ungroup() |>
  mutate(
    file_month = floor_date(file_date, "months")
  )



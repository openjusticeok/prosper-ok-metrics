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

#relevant strings
drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
count_exclude <- "DISTRIBUTE|INTENT"
dps_related <- "(DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"
exclude <- "(WITHDRAW|ERROR|RETURN|UNABLE|REVOKE|LIFT|RECALL|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"

# Pulling every case with at least one misdemeanor charge
reporting_start_date <- ymd("2022-01-01")
reporting_end_date <- ymd("2023-04-01")

case_m <- ojo_crim_cases(case_types = "CM",
                     file_years = 2022:2023) |> 
  collect()

cm_ids <- case_m |>
  pull(id)

min <- ojo_tbl("minute") |> 
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
  filter(
    str_detect(description, "SUSPENSION|SUSPENDED|SUSPEND"))|> 
  collect()

join_misd_min <- min |> 
  left_join(
    case_m, 
    by = c("case_id" = "id"), 
    relationship = "many-to-many"
  )

min |> 
  filter(str_detect(description, drugs),
         !str_detect(description, count_exclude)
  )

# Shortcut?
cm <- ojo_crim_cases(case_types = "CM", 
                     file_years = 2022:2023) |> 
  ojo_add_minutes() |> 
  collect()

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

# useful regex
drug_charge = if_else(grepl(drugs, count_as_filed), TRUE, FALSE)

filter(min_code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT",
                       "CNOTE", "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
       str_detect(min_desc, "SUSPENSION"),
       !str_detect(min_desc, "(WITHDRAW|ERROR|RETURN|REVOKE|UNABLE|LIFT|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)"))





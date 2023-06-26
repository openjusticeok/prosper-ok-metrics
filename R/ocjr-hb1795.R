library(ojodb)
library(tidyverse)
library(lubridate)
library(here)

# notes from call
# cases_of_interest <- ojo_tbl("case") |> filters to limit to misd drug charges
# ojo_tbl("minute") |> 
# all our filters for dl susp |> filter(case_id %in% !!cases_of_interest$id)

#relevant strings
drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"
dl_related <- "(SUSPEND|SUSPENSION|DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"
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
  collect()

min |> 
  filter(str_detect(description, dl_related),
         !str_detect(description, exclude)
  )

# Shortcut?
cm <- ojo_crim_cases(case_types = "CM", 
                     file_years = 2022:2023) |> 
  ojo_add_minutes() |> 
  collect() 

# Then filter the "universe of minutes" to include only case_ids that are
# in the relevant cases of interest.
drug_dl_cm <- cm |> 
  filter(str_detect(count_as_filed, drugs), 
         str_detect(description, dl_related),
         !str_detect(description, exclude)
         )


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


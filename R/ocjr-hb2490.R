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

# Pulling the number of suspended sentences for CMs / CFs 

start_date <- ymd("2001-01-01")
end_date <- ymd("2023-01-01")

data_case <- ojo_tbl("case", .con = ojodb) |>
  select(
    id,
    district,
    case_type,
    date_filed,
    date_closed,
    created_at, 
    updated_at,
    status
    ) |>
  filter(
    case_type %in% c("CM", "CF"),
    date_filed >= start_date,
    date_filed < end_date
  ) |>
  left_join(
    ojo_tbl("count", .con = ojodb),
    by = c("id" = "case_id"),
    suffix = c("", "_count")
  ) |>
  ojo_collect()

# write_csv(data_case, here("data/cm_cf_2001_2022.csv"))
# df_case <- read_csv(here("data/cm_cf_2001_2022.csv"))
df_case <- data_case

#check
df_ids <- df_case |>
  distinct(id)

############ Pulling relevant minute tables
# We first look for any mention of suspended sentences in the description column. 
# It appears that revocation or application for revocation of a suspended sentence is not uncommon and might be worth noting. 
# If we want to include cases that result in a revocation of the suspended sentence or want to do further analysis with that on what might happen.
data_minutes <- ojo_tbl("minute") |>
  filter(date >= start_date,
         date < end_date) |>
   select(id,
          case_id,
          date,
          code,
          description,
          count,
          amount) |>
  filter(str_detect(description, "(?i)^\\ssuspended sentence")) |> 
  filter(!str_detect(description, "(?i)\\brevok(e|ed|ing)\\b")
         ) |>
  ojo_collect()

# write_csv(data_minutes, here("data/min_2001_2022.csv"))
# df_min <- read_csv(here("data/min_2001_2022.csv"))
df_min <- data_minutes

#111058
df_min <- df_min |> 
  filter(
    !str_detect(description, "(?i)dismiss|(?i)\\brevok(e|ed|ing)\\b|(?i)\\brevoc(atio|aton|ation)\\b|(?i)torevoke|revocke")
         )

#check
minute_ids <- df_min |>
  distinct(case_id)

# df_min("description") |>
#   inner_join(
#     oscn_cases,
#     by = c("case_id" = "id"))

join_case_min <- df_min |>
  inner_join(
    df_case |>
      distinct(),
      by = c("case_id" = "id"),
    copy = TRUE
    ) |>
  mutate(year = lubridate::year(date)) 

join_case_min$description |> 
  view()

# 84 cases filtered out
#check for cases being filtered out with minute regex
# For some reason it filters these out (these rows contain suspended sentence in the string) and when I checked for spelling/spacing issues I was unable to resolve the problem. 
test <- df_min |>
  anti_join(
    df_case,
    by = c("case_id" = "id")) |> 
  #distinct(id) |> 
  view()

annual_suspended <- join_case_min |>
  distinct(id, year) |>
  group_by(year) |>
  count()

# Check if we should be splitting OSCN counties from the rest
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

oscn_cases <- df_case |>
  filter(district %in% oscn_county_list)

# check that we only have 13 OSCN counties
# join_case_min |> 
#   distinct(district) |> 
#   count()
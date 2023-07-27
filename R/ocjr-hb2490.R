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
    date_filed >= "2001-01-01",
    date_filed < "2023-01-01"
  ) |>
  left_join(
    ojo_tbl("count", .con = ojodb),
    by = c("id" = "case_id"),
    suffix = c("", "_count")
  ) |>
  ojo_collect()

oscn_cases <- data_case |>
  filter(district %in% oscn_county_list)

# Pulling relevant minute tables
# We first look for any mention of suspended sentences in the description column. 
# It appears that revocation or application for revocation of a suspended sentence is not uncommon and might be worth noting. 
# If we want to include cases that result in a revocation of the suspended sentence or want to do further analysis with that on what might happen.
data_minutes <- ojo_tbl("minute") |>    
<<<<<<< Updated upstream
  filter(date >= "2001-01-01",
          date < "2023-01-01") |>
=======
  filter(date >= start_date,
          date < end_date) |>
>>>>>>> Stashed changes
   select(id,
          case_id,
          date,
          code,
          description,
          count,
          amount) |>
<<<<<<< Updated upstream
  filter(str_detect(description, "(?i)suspended sentence"), 
         !str_detect(description, "(?i)\\brevok(e|ed|ing)\\b")
         ) |>
=======
#   filter(str_detect(description, "(?i)suspended")
# !str_detect(description, min_desc_exclude)
# ) |>
>>>>>>> Stashed changes
  ojo_collect()



oscn_cases |>
  inner_join(
    oscn_cases |>
      distinct(id),
    by = c("case_id" = "id"),
    copy = TRUE
  )

# check for case_id's that are in 
join_case_min <- minute_ids |>
   anti_join(
     oscn_cases,
     by = c("case_id" = "id"))


# Extrapolate to state-wide


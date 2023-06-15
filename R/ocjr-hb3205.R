library(readr)
library(stringr)
library(readxl)
library(tidycensus)
library(tidyverse)
library(here)
library(janitor)
library(glue)  

# ProsperOK 
# requested estimates of the number of Oklahoman families impacted by 3205
# Using OJA data and ACS survey estimates we pull counts on a quarterly basis
# and multiply the counts by the avg. household size and avg. family size. 

# Retrieve ACS data on avg. household size by county
# api_key <- 'dea99d9b375b4594c415dd0c5089eb407b70f447'
# census_api_key(api_key, install = TRUE)
v21 <- load_variables(2021, "acs5/subject", cache = TRUE) |>
  filter(str_detect(concept, "HOUSEHOLD"))
# v21_state<- load_variables(2021, "acs5/subject", cache = TRUE) |>
#   filter(str_detect(concept, "HOUSEHOLD"))

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
                       cache = TRUE)

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
                       cache = TRUE)

family_estimate_state <- get_acs(geography = "state",
                           survey = "acs5",
                           variable = "S1101_C01_004",
                           state = "OK", 
                           year = 2021,
                           cache = TRUE)

# OJA data with monthly referrals 2018-2022 
referrals <- read_excel(here("data", "YoungOffenders.xlsx"), sheet = "Referrals") |>
  janitor::clean_names() |>
  rename(
    age = new_age,
    class = arrest_class
  ) |> 
  mutate(
    month_new = as.numeric(
      str_sub(referrals$month,
              nchar(referrals$month)-1,
              nchar(referrals$month)
              )
      )
    ) |> 
  mutate(
    quarters = case_when(month_new <= 3 ~ 'q1',
                         month_new >= 4 & month_new < 7 ~ 'q2',
                         month_new >= 7 & month_new < 9 ~ 'q3',
                         month_new >= 9 ~ 'q4')
  )

# aggregate for quarterly reporting
referrals_quarter <- referrals |>
  group_by(county, year, quarters) |>
  count() |> 
  rename(num_referred = n) 

state_referrals_quarter <- referrals |> 
  group_by(year, quarters) |>
  count() |> 
  rename(num_referred = n)

# Reporting periods look at data starting from 2022
# will need to access OJA data on a quarterly basis 
total_state <- state_referrals_quarter |>
  filter(year == "2022" | year == "2023") 

report_hh <- sum(total_state$num_referred)*(hh_estimate_state$estimate)

report_family <- sum(total_state$num_referred)*(family_estimate_state$estimate)


  




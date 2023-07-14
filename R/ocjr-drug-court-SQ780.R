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
library(ojodb)
library(ggplot2)
library(gt)
library(lubridate)
library(tidyr)
library(purrr)
library(extrafont)
#theme_set(theme_bw(base_family = "Roboto") %+replace% ojo_theme())

# * Number of simply possession cases charged in 2014, 2015, 2016
# 
# * Number of simple possessions in 2021 and 2022
# 
# * Total number of SQ780 offenses charged in 2014, 2015, and 2016

ojodb <- ojo_connect()

data <- ojo_tbl("case", .con = ojodb) |>
  select(
    id,
    district,
    case_type,
    date_filed,
    date_closed,
    created_at,
    updated_at
  ) |>
  filter(
    case_type %in% c("CM", "CF"),
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01"
  ) |>
  left_join(
    ojo_tbl("count", .con = ojodb),
    by = c("id" = "case_id"),
    suffix = c("", "_count")
  ) |>
  ojo_collect()

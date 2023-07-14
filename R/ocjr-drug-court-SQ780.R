library(readr)
library(stringr)
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
theme_set(theme_bw(base_family = "Roboto") %+replace% ojo_theme())

# Source code in prosper-ok-metrics/reports/hb2153/hb2153.qmd
parties_df <- read_csv(here("data/parties_convictions_2001_2022.csv"))

# * Number of simply possession cases charged in 2014, 2015, 2016
# * Number of simple possessions in 2021 and 2022

parties_2014_2022 <- parties_df |>
  filter(
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01"
  ) |> 
  mutate(year = lubridate::year(date_filed)) |>
  group_by(year, case_type) |>
  #group_by(year) |>
  count()

parties_2014_2022 |> 
  pivot_wider(names_from = case_type, values_from = n) |> 
  ungroup() |> 
  gt() |> 
  cols_label(contains("CM") ~ "Misdemeanor", 
             contains("CF") ~ "Felony", 
             contains("y") ~ "Year",
            ) |>
  tab_header(
    title = "Annual Simple Possession Drug Charges", 
    subtitle = "Before and After SQ780 Passing"
  )

parties_2014_2022 |>
  ggplot(aes(x = year, y = n, fill = case_type)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_x_continuous(breaks = c(2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022)) +
  theme(axis.text.x = element_text(angle = 45, vjust=0.5)) + 
  scale_y_continuous(labels = scales::comma) +
  labs(title = "Simple Possession Drug Charges",
       subtitle = "Before and After SQ780 Passing",
       x = "Year",
       y = "Number of Charges",
       caption = "This data is based on the total number\nof charges in our dataset, not cases")


# * Total number of SQ780 offenses charged in 2014, 2015, and 2016

# ojodb <- ojo_connect()
# 
# data <- ojo_tbl("case", .con = ojodb) |>
#   select(
#     id,
#     district,
#     case_type,
#     date_filed,
#     date_closed,
#     created_at,
#     updated_at
#   ) |>
#   filter(
#     case_type %in% c("CM", "CF"),
#     date_filed >= "2014-01-01",
#     date_filed < "2023-01-01"
#   ) |>
#   left_join(
#     ojo_tbl("count", .con = ojodb),
#     by = c("id" = "case_id"),
#     suffix = c("", "_count")
#   ) |>
#   ojo_collect()

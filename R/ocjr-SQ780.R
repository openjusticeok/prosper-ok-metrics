library(readr)
library(stringr)
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

# * Number of simply possession cases charged in 2013, 2014, 2015, 2016
# * Number of simple possessions in 2021 and 2022

parties_2013_2022 <- parties_df |>
  filter(
    date_filed >= "2013-01-01",
    date_filed < "2023-01-01",
  ) |>
  mutate(year = lubridate::year(date_filed)) |>
  distinct(party, case_type, date_filed, year) |>
  group_by(year, case_type) |>
  count()

simple_poss_old <- parties_2013_2022 |>
  pivot_wider(names_from = case_type, values_from = n) |>
  mutate(
    total = CM + CF,
    statewide = round(total * 2.3778)
  ) |>
  ungroup() |>
  gt() |>
  cols_label(contains("CM") ~ "Misdemeanor",
             contains("CF") ~ "Felony",
             contains("y") ~ "Year",
             total ~ "Total (OSCN)",
             statewide ~ "Statewide Estimate"
            ) |>
  tab_header(
    title = "Annual Simple Possession Drug Convictions",
    subtitle = "Before and After SQ780 Passing"
  ) |>
  tab_options(
    column_labels.font.size = "small",
    table.font.size = "small",
    heading.title.font.size = "medium",
    heading.subtitle.font.size = "small")

simple_poss_old

parties_2013_2022 |>
  ggplot(aes(x = year, y = n, fill = case_type)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = 2013:2022) +
  theme(axis.text.x = element_text(angle = 45, vjust=0.5),
        text=element_text(size=12)) +
  scale_y_continuous(labels = scales::comma) +
  labs(title = "Simple Possession Drug Convictions, OSCN Counties",
       color = "Case Type",
       subtitle = "Impact of SQ780 Passing",
       x = "Year",
       y = "Number of Convictions",
       caption = "Data represents the total number of parties convicted, \nnot the number of cases filed.") +
  scale_fill_manual(values=c("#999999", "#E69F00"),
                    name="Case Type",
                    labels=c("Felony", "Misdemeanor")) +
  annotate("text", x = 2018.2, y = 8200, label = "SQ780 in Effect",
           family = "serif", fontface = "italic", colour = "darkred",
           size = 3)

parties_2013_2022 |>
  pivot_wider(names_from = case_type, values_from = n) |>
  mutate(
    total = CM + CF,
    statewide = round(total * 2.3778)
  ) |>
  write_csv(here("data/parties_convictions_2013_2022.csv"))

# * Total number of SQ780 offenses charged in 2013-2022 statewide

## Brancen was able to gather a more robust estimate of 780 cases using some of the methodology below + methodology used in the Loft report to extrapolate OSCN estimates to a state-wide estimate.
clean_df <- read_csv(here("data/clean_df.csv"))
parties <- read_csv(here("data/parties_2001_2023.csv"))

# One with the requirement that all charges in the case be 780 charges
all_780_csv <- read_csv(here("data/all_780_summary.csv"))

# At least one charge in the case being a 780 charge
any_780_csv <- read_csv(here("data/any_780_summary.csv"))

# Will proceed to work with "at least one" SQ780 charge in the case

clean_df |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    .by = c("fiscal_year", "case_type")
  ) |>
  arrange(
    fiscal_year,
    case_type
  ) |>
  mutate(
    est_state_cases = n_cases * 2.3778
  )

any_780_df <- clean_df |>
  group_by(id) |>
  mutate(
    any_780 = any(sq780_drugs | sq780_props)
  ) |>
  add_count(any_780) |>
  ungroup() |>
  filter(any_780)

any_780_df_out <- any_780_df |>
  filter(fiscal_year > 2012,
         fiscal_year < 2023) |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    .by = c("fiscal_year", "case_type")
  ) |>
  arrange(fiscal_year)

convicted_parties_any_780 <- any_780_df |>
  mutate(
    conviction = case_when(
      str_detect(disposition, "CONV") ~ T,
      str_detect(disposition, "Guilty Plea") ~ T,
      str_detect(disposition_detail, "(?i)Guilty Plea") ~ T,
      str_detect(disposition, "DISMIS") ~ F,
      str_detect(disposition, "(?i)DEFER") ~ F,
      str_detect(disposition, "ACQUIT") ~ F,
      str_detect(disposition, "Pending") ~ F,
      str_detect(disposition, "(?i)TRANSFER") ~ F,
      str_detect(disposition_detail, "Dismiss") ~ F,
      TRUE ~ F
    )
  ) |>
  #filter(conviction) |>
  left_join(
    parties,
    by = c("party" = "id")
  )

convicted_any_780 <- convicted_parties_any_780 |>
  filter(fiscal_year > 2012,
         fiscal_year < 2023) |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    n_people = n_distinct(oscn_id),
    .by = c("fiscal_year", "case_type")
  ) |>
  arrange(fiscal_year)

sq780_charge_table <- convicted_any_780  |>
  select(fiscal_year, case_type, n_counts, n_people) |>
  pivot_wider(names_from = case_type, values_from = c(n_counts, n_people)) |>
  gt() |>
  tab_spanner(label = md("Number of Charges"),
              columns = contains("counts")) |>
  tab_spanner(label = md("Number of Individuals Convicted"),
              columns = contains("people")) |>
  cols_label(contains("counts_CM") ~ "Misdemeanor",
             contains("counts_CF") ~ "Felony",
             contains("people_CF") ~ "Felony",
             contains("people_CM") ~ "Misdemeanor",
             contains("fiscal") ~ "Fiscal Year"
  ) |>
  tab_header(
    title = "Statewide Estimate of SQ780 Charges",
    subtitle = "SQ780 Property Crimes and Simple Possession Drug Charges"
  ) |>
  tab_options(
    column_labels.font.size = "small",
    table.font.size = "small",
    heading.title.font.size = "medium",
    heading.subtitle.font.size = "small")

sq780_charge_table

# Estimates for sq780 simple possession drug charges using loft numbers
any_780_drug <- clean_df |>
  group_by(id) |>
  mutate(
    any_780_drug = any(sq780_drugs)
  ) |>
  add_count(any_780_drug) |>
  ungroup() |>
  filter(any_780_drug)

any_780_drug_out <- any_780_drug |>
  filter(fiscal_year > 2012,
         fiscal_year < 2023) |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    .by = c("fiscal_year", "case_type")
  ) |>
  arrange(fiscal_year)

convicted_parties_any_drug_780 <- any_780_drug |>
  mutate(
    conviction = case_when(
      str_detect(disposition, "CONV") ~ T,
      str_detect(disposition, "Guilty Plea") ~ T,
      str_detect(disposition_detail, "(?i)Guilty Plea") ~ T,
      str_detect(disposition, "DISMIS") ~ F,
      str_detect(disposition, "(?i)DEFER") ~ F,
      str_detect(disposition, "ACQUIT") ~ F,
      str_detect(disposition, "Pending") ~ F,
      str_detect(disposition, "(?i)TRANSFER") ~ F,
      str_detect(disposition_detail, "Dismiss") ~ F,
      TRUE ~ F
    )
  ) |>
  #filter(conviction) |>
  left_join(
    parties,
    by = c("party" = "id")
  )

convicted_any_780_drug <- convicted_parties_any_drug_780 |>
  filter(fiscal_year > 2012,
         fiscal_year < 2023) |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    n_people = n_distinct(oscn_id),
    .by = c("fiscal_year", "case_type")
  ) |>
  arrange(fiscal_year)

statewide_simple_poss <- convicted_any_780_drug |>
  ggplot(aes(x = fiscal_year, y = n_people, fill = case_type)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = 2013:2022) +
  theme(axis.text.x = element_text(angle = 45, vjust=0.5),
        text=element_text(size=12)) +
  scale_y_continuous(labels = scales::comma) +
  labs(title = "Simple Possession Drug Convictions",
       color = "Case Type",
       subtitle = "Annual Statewide Estimates",
       x = "Fiscal Year",
       y = "Number of Convictions",
       caption = "Data represents the number of people convicted, with at least one SQ780 drug charge.") +
  theme(plot.caption.position = "plot",
        plot.caption = element_text(size = 8, color = "grey20", hjust = 0.025)) +
  scale_fill_manual(values=c("#999999", "#E69F00"), name="Case Type",
                    labels=c("Felony", "Misdemeanor")) +
  annotate("text", x = 2018.8, y = 8200, label = "SQ780 in Effect", colour = "darkred", size = 3) +
  geom_text(aes(label = n_people), colour = "white", size = 2.5, position = position_stack(vjust = 0.5))

statewide_simple_poss
ggsave(here("plots", "statewide_simple_poss_2013-2022.png"))

drug_charge_table <- convicted_any_780_drug |>
  select(fiscal_year, case_type, n_counts, n_people) |>
  pivot_wider(names_from = case_type, values_from = c(n_counts, n_people)) |>
  gt() |>
  tab_spanner(label = md("Number of Charges"),
              columns = contains("counts")) |>
  tab_spanner(label = md("Number of Individuals Convicted"),
              columns = contains("people")) |>
  cols_label(contains("counts_CM") ~ "Misdemeanor",
             contains("counts_CF") ~ "Felony",
             contains("people_CF") ~ "Felony",
             contains("people_CM") ~ "Misdemeanor",
             contains("fiscal") ~ "Fiscal Year"
  ) |>
  tab_header(
    title = "Statewide Estimate of SQ780 Drug Charges",
    subtitle = "Simple Possession Charges"
  ) |>
  tab_options(
    column_labels.font.size = "small",
    table.font.size = "small",
    heading.title.font.size = "medium",
    heading.subtitle.font.size = "small")

drug_charge_table

write_csv(convicted_any_780, here("output/convicted_any_780.csv"))
write_csv(convicted_any_780_drug, here("output/convicted_any_780_drug.csv"))

# compare table above to the simple possession numbers we shared with ocjr in July
simple_poss_old

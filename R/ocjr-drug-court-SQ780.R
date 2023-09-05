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

parties_2013_2022 |>
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


# ________________________Methodology________________________
ojodb <- ojo_connect()

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
#     date_filed >= "2001-01-01",
#     date_filed < "2023-01-01"
#   ) |>
#   left_join(
#     ojo_tbl("count", .con = ojodb),
#     by = c("id" = "case_id"),
#     suffix = c("", "_count")
#   ) |>
#   ojo_collect()
#
# write_csv(data, here("data/cm_cf_2001_2022.csv"))
data <- read_csv(here("data/cm_cf_2001_2022.csv"))

# UCCS look-up-- check for charges categorized as "property"
# will need to filter for "charge_desc" auto related crimes
charge_predictions <- read_csv(
  here("data/charge_predictions.csv")) |>
  distinct()

uccs <- read_csv(here("data/uccs-schema.csv"))

# We are limited to the 13 official OSCN counties since
# the other counties do not report the charges in each case.
oscn_county_list <- c("TULSA", "OKLAHOMA", "CLEVELAND", "ROGERS", "PAYNE",
                      "COMANCHE", "GARFIELD", "CANADIAN", "LOGAN", "ADAIR",
                      "PUSHMATAHA", "ROGER MILLS", "ELLIS")

data_oscn <- data |>
  filter(district %in% oscn_county_list)

# Clean data to join with UCCS codes
prepare <- function(x) {
  x |>
    str_to_upper() |>
    str_remove_all("^[A-Z0-9]{0,10},") |>
    str_remove_all("(?i)IN VIOLATION.*") |>
    str_remove_all(r"{\s*\([^\)]+\)}") |>
    str_replace_all("[[:punct:][:blank:]]+", " ") |>
    str_squish()
}

result_clean <- data |>
  # prepare function cleans data to match w/ cleaned lookup table
  mutate(
    count_as_filed = prepare(count_as_filed),
    # If count_as_disposed is missing, fill it with count_as_filed
    # This will retain
    count_as_disposed = if_else(
      is.na(count_as_disposed),
      count_as_filed,
      prepare(count_as_disposed)
    )
  ) |>
  # Filtering to only include OSCN counties
  filter(district %in% oscn_county_list) |>
  left_join(
    charge_predictions,
    by = c("count_as_disposed" = "charge"),
    suffix = c("", "_toc"),
    relationship = "many-to-many"
  ) |>
  left_join(uccs, by = "uccs_code")


drug_toc <- result_clean |>
  filter(offense_type_desc=="Drug") # num of rows - 301680

property_toc <- result_clean |>
  filter(offense_type_desc=="Property") # num of rows - 329911

# find appropriate property uccs codes
property_codes <- uccs |>
  filter(offense_type_desc=="Property") |>
  filter(
    !str_detect(charge_desc, "(?i)arson|(?i)hit and run|(?i)vehicle|(?i)auto|(?i)trespass")
  ) |>
  select(uccs_code)

columns <- c(
  "id", "case_type", "date_filed", "count_as_disposed",
  "disposition_date", "disposition", "charge_desc",
  "offense_category_desc", "offense_type_desc",
  "probability", "uccs_code", "party"
)

data_clean <- result_clean |>
  select(all_of(columns)
  ) |>
  mutate(
    poss = str_detect(count_as_disposed, "(?i)poss|(?i)poass of cds|(?i)poissession"), # added typos I found in `n`
    dist = str_detect(count_as_disposed, "(?i)\\bdist"),
    intent = str_detect(count_as_disposed, "(?i)intent|\\bint\\b"),
    paraphernalia = str_detect(count_as_disposed, "(?i)paraph.*\\b|(?i)\\bpara\\b|(?i)p.*nalia"), # added typos
    weapon = str_detect(count_as_disposed, "(?i)weapon"),
    trafficking = str_detect(count_as_disposed, "(?i)traffick"),
    endeavor = str_detect(count_as_disposed, "(?i)endeavor|\\bend\\b"),
    wildlife = str_detect(count_as_disposed, "(?i)wildlife"),
    manufacture = str_detect(count_as_disposed, "(?i)\\bmanufac"),
    commercial = str_detect(count_as_disposed, "(?i)COMM FAC"),
    school_park_church = str_detect(count_as_disposed, "(?i)school|education|park|church|day care|\\spk\\s"),
    conspiracy = str_detect(count_as_disposed, "(?i)\\bconsp"),
    maintaining = str_detect(count_as_disposed, "(?i)\\bmaint.*(place|dwelling)"),
    tax = str_detect(count_as_disposed, "(?i)\\btax\\b"),
    minor = str_detect(count_as_disposed, "(?i)minor|child|under.*21"),
    inmate = str_detect(count_as_disposed, "(?i)inmate|jail|contrab|penal|\\bfac"),
    vehicle = str_detect(count_as_disposed, "(?i)(?<!\\snon|not\\s)(?:\\sMV\\b|motor vehicle)"),
    proceeds = str_detect(count_as_disposed, "(?i)ac.*\\bp(r|o){0,2}|proceed"),
    larceny = str_detect(count_as_disposed, "(?i)larceny|theft|LMFR|larceny of merchandise|meat|corporeal property|\\bstol(e|en)\\b"),
    receive_stolen = str_detect(count_as_disposed, "(?i)RCSP|(?i)stolen property|(?i)stoeln|concealing stolen"),
    embezzle = str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement"),
    misc = str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game|oil|(?i)drilling|(?i)gas"),
    pawn = str_detect(count_as_disposed, "(?i)repay pawn|(?i)pawnbroker|pawn shop"),
    hospitality = str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)"),
    forgery = str_detect(count_as_disposed, "forgery|forged|(?i)counterfeit"),
    drug = str_detect(count_as_disposed, "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230,
    uccs_property = probability >= 0.8 & uccs_code %in% property_codes$uccs_code
  ) |>
  arrange(count_as_disposed)

# 406777 rows
clean_df <- data_clean |>
  filter(
    (uccs_drug_plus & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (poss & drug & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (uccs_property & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (larceny & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (receive_stolen & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (embezzle & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (misc & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (pawn & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (hospitality & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
    (forgery & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))
  ) |>
  select(id, case_type, date_filed, count_as_disposed, disposition_date,
         disposition, charge_desc, offense_category_desc, offense_type_desc,
         probability, uccs_code, party, uccs_drug_plus, poss, drug,
         uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
         hospitality, forgery)

# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
sq780_counts <- clean_df |>
  filter(
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01",
  ) |>
  mutate(year = lubridate::year(date_filed)) |>
  distinct(id, case_type, date_filed, year) |>
  group_by(year, case_type) |>
  count()

# expanding to non-OSCN counties
# using statewide formula from simple possession analysis
sq780_counts |>
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
    title = "Simple Possession Drug and Property Crime Convictions",
    subtitle = "Before and After SQ780 Passing"
  ) |>
  tab_options(
    column_labels.font.size = "small",
    table.font.size = "small",
    heading.title.font.size = "medium",
    heading.subtitle.font.size = "small")


#
# ggplot(sq780_counts, aes(x=year, y=n, group=case_type)) +
#   geom_line(aes(color=case_type), linewidth=0.5, alpha=0.9) +
#   scale_color_manual(values=c("#999999", "#E69F00", "#56B4E9")) +
#   scale_y_continuous(labels = scales::comma) +
#   theme(legend.position="bottom",
#         legend.title = element_text(size = 10),
#         legend.text = element_text(size = 10)) +
#   theme(
#     plot.title = element_text(size = 14),
#     axis.title = element_text(size = 12),
#     plot.subtitle = element_text(size = 8),
#     plot.caption = element_text(size = 8),
#     axis.text = element_text(size = 10)) +
#   scale_x_continuous(breaks = 2014:2022) +
#   labs(title = "SQ780 Offenses 2014-2022",
#        subtitle = "Simple possession drug and property crime charges\nfiled before and after the passing of SQ780",
#        x = "Year",
#        y = "Number of Charges",
#        color = "Case Type",
#        caption = "This data is based on the total number of charges in \nthe court records, not cases among OSCN counties only."
#   )


########################### SQ780 offenses:
# 63 O.S. 2011 Section 2-402 -- simple possession
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b|(?i)salt|ephedrine"),
#     !str_detect(count_as_disposed, "(?i)DOMESTIC|(?i)ASSAULT|(?i)FIREARM|(?i)DISTRIBUTE|(?i)INTENT|(?i)MANUFACTURE|(?i)DISPENSE|TUO|alcohol|acohol|alchol")
#   ) |>
#   View()
#
# # 21 O.S. 2011 Section 1704 and 1705 -- grand and petit larceny
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)larceny|theft|\\bstol(e|en)\\b"),
#     !str_detect(count_as_disposed, "(?i)gas|automobile|vehicle|LMFR|(?i)concealing")
#   ) |>
#   View()
#
# # 21 O.S. 2011 Section 1713 -- buying/receiving/concealing stolen property
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)RCSP|(?i)stolen property|(?i)embezzled property|(?i)stoeln|concealing stolen")
#   ) |>
#   View()
#
# # 21 O.S. 2011 Section 1719.1 -- taking domesticated fish and game
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game")
#   ) |>
#   View()
#
# # 21 O.S. 2011 Section 1722 -- unlawfully taking crude oil/gas/related
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "oil|(?i)drilling|(?i)gas")
#   ) |>
#   View()
#
# # 21 O.S. 2011 Section 1731
# # -- Larceny of merchandise (edible meat or other physical property)
# # held for sale in retail or wholesale establishments
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "LMFR|larceny of merchandise|meat")
#   ) |>
#   View()
#
# # 21 O.S. 2011, Sections 1451 - embezzlement
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement")
#   ) |>
#   View()
#
# # 21 O.S. 2011, Sections 1503 - defrauding hospitality/lodging
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)")
#   ) |>
#   View()
#
# # 21 O.S. 2011, Sections 1521 - vehicle embezzlement
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)vehicle with bogus|(?i)embezzlement of a motor")
#   ) |>
#   View()
#
# # 21 O.S. 2011, Sections 1541.1, 1541.2, 1541.3
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "cheat|defraud")
#   ) |>
#   View()
#
# # 59 O.S. 2011, Section 1512 -- pawn
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "(?i)repay pawn|(?i)pawnbroker|pawn shop")
#   ) |>
#   View()
#
# # 21 O.S. 2011, Section 1579 and Section 1621
# result_clean |>
#   filter(
#     str_detect(count_as_disposed, "forgery|forged|(?i)counterfeit")
#   ) |>
#   View()


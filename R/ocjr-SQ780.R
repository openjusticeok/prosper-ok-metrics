library(ojoverse)
library(tidyverse)
library(here)
library(fs)
library(janitor)
library(gt)
library(purrr)
library(extrafont)

theme_set(theme_bw(base_family = "Roboto") %+replace% ojo_theme())

if (!dir_exists(here("local"))) {
  dir_create(here("local"))
}

if (!file_exists(here("local/cm_cf_2001_2023.csv"))) {
  data <- ojo_tbl("case") |>
    filter(
      case_type %in% c("CM", "CF"),
      date_filed >= "2001-01-01"
    ) |>
    select(
      id,
      district,
      case_type,
      year,
      case_number,
      date_filed,
      date_closed,
      open_counts
    ) |>
    left_join(
      ojo_tbl("count"),
      by = c("id" = "case_id"),
      suffix = c("", "_count")
    ) |>
    ojo_collect() |>
    bind_rows(
      ojo_tbl("case", schema = "oscn") |>
        filter(
          case_type %in% c("CM", "CF"),
          date_filed >= "2001-01-01"
        ) |>
        select(
          id,
          district,
          case_type,
          year,
          case_number,
          date_filed,
          date_closed,
          open_counts
        ) |>
        left_join(
          ojo_tbl("count", schema = "oscn"),
          by = c("id" = "case_id"),
          suffix = c("", "_count")
        ) |>
        ojo_collect()
    ) |>
    mutate(
      fiscal_year = ojo_fiscal_year(date_filed)
    )

  write_csv(data, here("local/cm_cf_2001_2023.csv"))
}

data <- read_csv(here("local/cm_cf_2001_2023.csv"))

if (!file_exists(here("local/parties_2001_2023.csv"))) {
  parties <- ojo_tbl("party") |>
    filter(
      role == "Defendant",
      case_id %in% !!data$id
    ) |>
    ojo_collect() |>
    bind_rows(
      ojo_tbl("party", schema = "oscn") |>
        filter(
          role == "Defendant",
          case_id %in% !!data$id
        ) |>
        ojo_collect()
    )
  write_csv(parties, here("local/parties_2001_2023.csv"))
}

parties <- read_csv(here("local/parties_2001_2023.csv"))


# CJARS TOC Tooling

charge_predictions <- read_csv(
  here("data/charge_predictions.csv")) |>
  distinct()

uccs <- read_csv(here("data/uccs-schema.csv"))

# We are limited to the 13 official OSCN counties since
# the other counties do not report the charges in each case.
oscn_county_list <- c("TULSA", "OKLAHOMA", "CLEVELAND", "ROGERS", "PAYNE",
                      "COMANCHE", "GARFIELD", "CANADIAN", "LOGAN", "ADAIR",
                      "PUSHMATAHA", "ROGER MILLS", "ELLIS")

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

data <- data |>
  # prepare function cleans data to match w/ cleaned lookup table
  mutate(
    count_as_filed = prepare(count_as_filed),
    count_as_disposed = if_else(
      is.na(count_as_disposed),
      count_as_filed,
      prepare(count_as_disposed)
    )
  ) |>
  left_join(
    charge_predictions,
    by = c("count_as_disposed" = "charge"),
    suffix = c("", "_toc"),
    relationship = "many-to-many"
  ) |>
  left_join(uccs, by = "uccs_code")

property_codes <- uccs |>
  filter(offense_type_desc == "Property") |>
  filter(
    !str_detect(charge_desc, "(?i)arson|(?i)hit and run|(?i)vehicle|(?i)auto|(?i)trespass")
  ) |>
  select(uccs_code)

columns <- c(
  "id", "case_type", "district",
  "date_filed", "date_closed", "fiscal_year",
  "case_number",
  "open_counts", "id_count", "count_as_filed", "count_as_disposed",
  "disposition_date", "disposition", "disposition_detail", "charge_desc",
  "offense_category_desc", "offense_type_desc",
  "probability", "uccs_code", "party"
)

data <- data |>
  select(
    all_of(columns)
  ) |>
  mutate(
    # 780 drug regex -----------------------------------------------------------
    poss = str_detect(count_as_disposed, "(?i)poss|poass of cds|poissession"),
    drug = str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),

    # 780 property regex -------------------------------------------------------
    larceny = str_detect(count_as_disposed, "(?i)larceny|theft|LMFR|larceny of merchandise|meat|corporeal property|\\bstol(e|en)\\b|\\blarc\\b"),
    receive_stolen = str_detect(count_as_disposed, "(?i)RCSP|stolen property|stoeln|concealing stolen"),
    embezzle = str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement"),
    misc = str_detect(count_as_disposed, "(?i)fish|domesticated game|\\boil\\b|\\bdrilling\\b|\\bgas\\b"),
    pawn = str_detect(count_as_disposed, "(?i)repay pawn|pawnbroker|pawn shop|in pawn"),
    hospitality = str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)"),
    false_pretense = str_detect(count_as_disposed, "(?i)false pretense|con game|confidence game|false representation|trick or deception"), # Andrew: added
    bogus_check = str_detect(count_as_disposed, "(?i)bogus check"),
    false_ownership = str_detect(count_as_disposed, "(?i)decla") & str_detect(count_as_disposed, "(?i)false"),

    # Disqualifying regex ------------------------------------------------------
    dist = str_detect(count_as_disposed, "(?i)\\bdist"),
    intent = str_detect(count_as_disposed, "(?i)intent|\\bint\\b"),
    paraphernalia = str_detect(count_as_disposed, "(?i)paraph.*\\b|\\bpara\\b|p.*nalia"), # added typos
    weapon = str_detect(count_as_disposed, "(?i)weapon|firearm"), # Andrew: added "firearm"
    trafficking = str_detect(count_as_disposed, "(?i)traffick"),
    endeavor = str_detect(count_as_disposed, "(?i)endeavor|\\bend\\b"),
    wildlife = str_detect(count_as_disposed, "(?i)wildlife"),
    manufacture = str_detect(count_as_disposed, "(?i)\\bmanufac"),
    commercial = str_detect(count_as_disposed, "(?i)COMM FAC"),
    school_park_church = str_detect(count_as_disposed, "(?i)school|education|park|church|day care|\\spk\\s"),
    conspiracy = str_detect(count_as_disposed, "(?i)\\bconsp"),
    maintaining = str_detect(count_as_disposed, "(?i)\\bmaint.*(place|dwelling)"),
    minor = str_detect(count_as_disposed, "(?i)minor|child|under.*21"),
    inmate = str_detect(count_as_disposed, "(?i)inmate|jail|contrab|penal|\\bfac"),
    vehicle = str_detect(count_as_disposed, "(?i)(?<!\\snon|not\\s)(?:\\sMV\\b|motor vehicle)"),
    proceeds = str_detect(count_as_disposed, "(?i)ac.*\\bp(r|o){0,2}|proceed"),
    impersonate_officer = str_detect(count_as_disposed, "(?i)personating police|personating an offic"), # Andrew: added
    tax_stamp = str_detect(count_as_disposed, "(?i)\\btax\\b"), # Andrew: added
    a_b_upon = str_detect(count_as_disposed, "(?i)a b "), # This was getting picked up by UCCS property
    elderly = str_detect(count_as_disposed, "(?i)\\belder"), # "Elder abuse" stuff was getting picked up by UCCS property
    larceny_cds = larceny & drug, # Larceny of CDS; not reclassified by 780
    burglary_bne = str_detect(count_as_disposed, "(?i)burgl|break and|breaking"),
    computers = str_detect(count_as_disposed, "(?i)computer"), # Excluded on advice from Gateley
    malicious = str_detect(count_as_disposed, "(?i)mali"), # Malicious injury to property; excluded on advice from Gateley
    false_personate = str_detect(count_as_disposed, "(?i)false") & str_detect(count_as_disposed, "(?i)person"), # "False personation" crimes
    credit_card = str_detect(count_as_disposed, "(?i)credit card|debit card"),
    medicaid = str_detect(count_as_disposed, "(?i)medicaid|insurance"),
    forgery = str_detect(count_as_disposed, "(?i)forgery|forged|counterfeit|forg inst"), # Gateley says this should be excluded
    fraud = str_detect(count_as_disposed, "(?i)\\bfraud\\b") & !hospitality & !pawn,
    id_theft = str_detect(count_as_disposed, "(?i)identity theft"),
    tampering_w_vehicle = str_detect(count_as_disposed, "(?i)tampering|altering") & str_detect(count_as_disposed, "(?i)vehicle|plate"),
    fake_id = str_detect(count_as_disposed, "(?i)false id|unauthorized id"),
    misc_exclude = str_detect(count_as_disposed, "(?i)racketeering|exploitation|railroad|corporate records|securities act|drug money laundering|unlawful communication by use|tampering with utilities|alter license plate decal|fictitious state driver|obtain unemployment|dentistry without a license|false (claims|claim) against state|pay toll|public building|money laundering|road signs"),

    # UCCS ---------------------------------------------------------------
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230,
    uccs_property = probability >= 0.8 & uccs_code %in% property_codes$uccs_code,

    # All regex disqualifying a charge from SQ780; this will make the below chunks more readable.
    disqualifying_regex = (
      dist | intent | paraphernalia | weapon | trafficking |
        endeavor | wildlife | manufacture | commercial | school_park_church |
        conspiracy | maintaining | minor | inmate | vehicle |
        proceeds | impersonate_officer | tax_stamp | a_b_upon | elderly |
        larceny_cds | burglary_bne | computers | malicious | false_personate |
        credit_card | medicaid | forgery | fraud | id_theft |
        tampering_w_vehicle | fake_id | misc_exclude
    ),
    # Any drug possession regex? Not worried about uccs codes with this variable yet
    regex_drugs = poss & drug,
    # Any property crime regex? Not worried about uccs codes with this variable
    regex_props = (
      larceny | receive_stolen | embezzle |
        misc | pawn | hospitality |
        false_pretense | bogus_check | false_ownership
    )
  ) |>
  arrange(count_as_disposed)

data <- data |>
  mutate(
    # Is the charge a drug possession charge covered by SQ780? -----------------
    # UCCS code or regex indicating drug possession, and no disqualifying regex
    sq780_drugs = (uccs_drug_plus | regex_drugs) & !disqualifying_regex,
    # Is the charge a property crime covered by SQ780? -------------------------
    # UCCS code or regex indicating 780 property crime, and no disqualifying regex
    sq780_props = (uccs_property | regex_props) & !disqualifying_regex,
    # Classify by specific statute ---------------------------------------------
    # Simple possession
    sq780_statute = case_when(
      sq780_drugs ~
        "63 O.S. Section 2-402 -- Simple Drug Possession",
      # Grand / petit larceny
      larceny & !receive_stolen ~
        "21 O.S. Section 1704, 1705 -- Grand/Petit Larceny",
      # Buying / receiving stolen property
      larceny & receive_stolen ~
        "21 O.S. Section 1713 -- Buying/Receiving/Concealing Stolen Property",
      # Domesticated fish / game -- using new regex since it's split up above
      str_detect(count_as_disposed, "(?i)fish|(?i)domesticated game") ~
        "21 O.S. Section 1719.1 -- Taking Domesticated Fish/Game",
      # Taking crude oil / gas / related
      str_detect(count_as_disposed, "(?i)\\boil\\b|\\bdrilling\\b|\\bgas\\b") ~
        "21 O.S. Section 1722 -- Unlawfully Taking Oil/Gas",
      # Larceny of merchandise in retail or wholesale establishment
      larceny & str_detect(count_as_disposed, "(?i)meat|edible|retail|wholesale") ~
        "21 O.S. Section 1731 -- Larceny of Merchandise from Retail or Wholesale",
      # Embezzlement
      embezzle ~
        "21 O.S. Sections 1451 -- Embezzlement",
      # Defrauding hotel / lodging
      hospitality ~
        "21 O.S. Sections 1503 -- Defrauding Hospitality/Lodging",
      # Fraud
      false_pretense | str_detect(count_as_disposed, "(?i)cheat|defraud|personat|bogus") ~
        "21 O.S. Sections 1541.1, 1541.2, 1541.3 -- Fraud",
      # Pawn Shop Fraud
      pawn | false_ownership ~
        "59 O.S. Section 1512 -- Defrauding Pawn Shop",
      # Forgery
      forgery ~
        "21 O.S. Section 1579, Section 1621 -- Forgery",
      # Default
      TRUE ~ "Unknown / Other"
    )
  ) |>
  select(
    id, case_type, district, date_filed, date_closed, fiscal_year,
    case_number, open_counts,
    id_count, count_as_filed, count_as_disposed, disposition_date,
    disposition, disposition_detail, charge_desc, offense_category_desc, offense_type_desc,
    probability, uccs_code, party, uccs_drug_plus, poss, drug,
    uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
    hospitality, forgery, sq780_drugs, sq780_props, sq780_statute
  )

# Add conviction classification
data <- data |>
  mutate(
    conviction = case_when(
      str_detect(disposition, "(?i)\\bCONV") ~ T,
      str_detect(disposition, "(?i)Guilty Plea") ~ T,
      str_detect(disposition_detail, "(?i)Guilty Plea") ~ T,
      str_detect(disposition, "(?i)\\bNOLO\\b") ~ T,
      str_detect(disposition_detail, "(?i)\\bNOLO\\b") ~ T,
      str_detect(disposition, "(?i)\\bFORFEIT") ~ F,
      str_detect(disposition, "(?i)\\bDISMIS") ~ F,
      str_detect(disposition, "(?i)\\bDEFER") ~ F,
      str_detect(disposition, "(?i)\\bACQUIT") ~ F,
      str_detect(disposition, "(?i)Pending") ~ F,
      str_detect(disposition, "(?i)TRANSFER") ~ F,
      str_detect(disposition_detail, "(?i)\\bDismiss") ~ F,
      str_detect(disposition, "(?i)\\bClosed") ~ F,
      str_detect(disposition, "(?i)\\bDEMURRER") ~ F,
      str_detect (disposition, "(?i)\\bSUCCESS") ~ F,
      TRUE ~ F
    )
  )

# Add identifier for whether the district is an OSCN one or not
data <- data |>
  mutate(
    oscn = district %in% oscn_county_list
  )

data |>
  left_join(
    parties,
    by = c("party" = "id")
  ) |>
  summarise(
    n_cases = n_distinct(id),
    n_counts = n_distinct(id_count),
    n_convictions = sum(conviction),
    n_parties = n_distinct(party),
    n_people = n_distinct(oscn_id),
    .by = c("fiscal_year", "case_type", "oscn", "sq780_drugs")
  ) |>
  arrange(fiscal_year, case_type, oscn) |>
  mutate(
    ratio_counts = case_when(
      oscn ~ n_counts / n_cases,
      TRUE ~ NA_real_
    ),
    ratio_convictions = case_when(
      oscn ~ n_convictions / n_cases,
      TRUE ~ NA_real_
    )
  ) |>
  group_by(fiscal_year, case_type, sq780_drugs) |>
  fill(starts_with("ratio_"), .direction = "downup") |>
  ungroup() |>
  mutate(
    n_counts = case_when(
      oscn ~ n_counts,
      TRUE ~ floor(n_cases * ratio_counts)
    ),
    n_convictions = case_when(
      oscn ~ n_convictions,
      TRUE ~ floor(n_cases * ratio_convictions)
    )
  ) |>
  # summarise(
  #   n_cases = sum(n_cases),
  #   n_counts = sum(n_counts),
  #   n_convictions = sum(n_convictions),
  #   .by = c("fiscal_year", "case_type")
  # ) |>
  filter(
    fiscal_year >= 2002,
    fiscal_year <= 2023
  )

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
  scale_fill_manual(values = c("#999999", "#E69F00"),
                    name = "Case Type",
                    labels = c("Felony", "Misdemeanor")) +
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

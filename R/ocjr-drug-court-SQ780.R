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

# * Number of simply possession cases charged in 2014, 2015, 2016
# * Number of simple possessions in 2021 and 2022

# parties_2014_2022 <- parties_df |>
#   filter(
#     date_filed >= "2014-01-01",
#     date_filed < "2023-01-01",
#   ) |>
#   mutate(year = lubridate::year(date_filed)) |>
#   distinct(party, case_type, date_filed, year) |>
#   group_by(year, case_type) |>
#   count()
#
# parties_2014_2022 |>
#   pivot_wider(names_from = case_type, values_from = n) |>
#   mutate(
#     total = CM + CF,
#     statewide = round(total * 2.3778)
#   ) |>
#   ungroup() |>
#   gt() |>
#   cols_label(contains("CM") ~ "Misdemeanor",
#              contains("CF") ~ "Felony",
#              contains("y") ~ "Year",
#              total ~ "Total (OSCN)",
#              statewide ~ "Statewide Estimate"
#             ) |>
#   tab_header(
#     title = "Annual Simple Possession Drug Convictions",
#     subtitle = "Before and After SQ780 Passing"
#   )
#
# parties_2014_2022 |>
#   ggplot(aes(x = year, y = n, fill = case_type)) +
#   geom_bar(stat = "identity") +
#   scale_x_continuous(breaks = 2014:2022) +
#   theme(axis.text.x = element_text(angle = 45, vjust=0.5)) +
#   scale_y_continuous(labels = scales::comma) +
#   labs(title = "Simple Possession Drug Convictions, OSCN Counties",
#        subtitle = "Before and After SQ780 Passing",
#        x = "Year",
#        y = "Number of Convictions",
#        caption = "This data is based on the total number\nof parties convicted, not the number of cases\nfiled.")
#
# parties_2014_2022 |>
#   pivot_wider(names_from = case_type, values_from = n) |>
#   mutate(
#     total = CM + CF,
#     statewide = round(total * 2.3778)
#   ) |>
#   write_csv(here("data/parties_convictions_2014_2022.csv"))


# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
# ojodb <- ojo_connect()

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

# Cleaning data ================================================================

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
  filter(offense_type_desc == "Property") |>
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

# Classifying data by SQ780 relevance ==========================================

data_clean <- result_clean |>
  select(all_of(columns)
  ) |>
  mutate(
    poss = str_detect(count_as_disposed, "(?i)poss|poass of cds|poissession"), # added typos I found in `n`
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
    tax = str_detect(count_as_disposed, "(?i)\\btax\\b"),
    minor = str_detect(count_as_disposed, "(?i)minor|child|under.*21"),
    inmate = str_detect(count_as_disposed, "(?i)inmate|jail|contrab|penal|\\bfac"),
    vehicle = str_detect(count_as_disposed, "(?i)(?<!\\snon|not\\s)(?:\\sMV\\b|motor vehicle)"),
    proceeds = str_detect(count_as_disposed, "(?i)ac.*\\bp(r|o){0,2}|proceed"),
    larceny = str_detect(count_as_disposed, "(?i)larceny|theft|LMFR|larceny of merchandise|meat|corporeal property|\\bstol(e|en)\\b"),
    receive_stolen = str_detect(count_as_disposed, "(?i)RCSP|stolen property|stoeln|concealing stolen"),
    embezzle = str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement"),
    misc = str_detect(count_as_disposed, "(?i)fish|domesticated game|oil|drilling|gas"),
    pawn = str_detect(count_as_disposed, "(?i)repay pawn|pawnbroker|pawn shop|in pawn"),
    hospitality = str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)"),
    forgery = str_detect(count_as_disposed, "(?i)forgery|forged|counterfeit|bogus check"), # Andrew: added "bogus check"
    false_pretense = str_detect(count_as_disposed, "(?i)false pretense|con game|confidence game|false representation"), # Andrew: added
    drug = str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),
    impersonate_officer = str_detect(count_as_disposed, "(?i)personating police|personating an offic"), # Andrew: added
    tax_stamp = str_detect(count_as_disposed, "(?i)tax stamp"), # Andrew: added
    a_b_upon = str_detect(count_as_disposed, "(?i)a b "), # This was getting picked up by UCCS property
    elderly = str_detect(count_as_disposed, "(?i)\\belder"), # "Elder abuse" stuff was getting picked up by UCCS property
    larceny_cds = larceny & drug, # Larceny of CDS; not reclassified by 780
    burglary_bne = str_detect(count_as_disposed, "(?i)burgl|break and|breaking"),
    computers = str_detect(count_as_disposed, "(?i)computer"), # Excluded on advice from Gateley
    malicious = str_detect(count_as_disposed, "(?i)mali"), # Malicious injury to property; excluded on advice from Gateley
    false_personate = str_detect(count_as_disposed, "(?i)false") &
      str_detect(count_as_disposed, "(?i)person"),
    # UCCS stuff
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230,
    uccs_property = probability >= 0.8 & uccs_code %in% property_codes$uccs_code,
    # All regex disqualifying a charge from SQ780; this will make the below chunks more readable.
    disqualifying_regex = if_else(
      dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife |
        manufacture | commercial | school_park_church | conspiracy | maintaining |
        minor | inmate | vehicle | proceeds | impersonate_officer | tax_stamp |
        a_b_upon | elderly | larceny_cds | burglary_bne | computers | malicious,
      TRUE, FALSE
    ),
    # Any drug regex? Not worried about uccs codes with this variable yet
    regex_drugs = if_else(poss & drug, TRUE, FALSE),
    # Any property crime regex? Not worried about uccs codes with this variable
    regex_props = if_else(larceny | receive_stolen | embezzle | misc | pawn | hospitality | forgery | false_pretense, TRUE, FALSE)
  ) |>
  arrange(count_as_disposed)

# Andrew note: Gonna try to remove repeated code and make this easier to read
# Basically just replacing the !(dist | intent | paraphernalia...) bit with the `disqualifying_regex` var above
# 406777 rows
# clean_df <- data_clean |>
#   filter(
#     (uccs_drug_plus & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (poss & drug & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (uccs_property & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (larceny & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (receive_stolen & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (embezzle & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (misc & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (pawn & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (hospitality & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (forgery & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))
#   ) |>
#   select(id, case_type, date_filed, count_as_disposed, disposition_date,
#          disposition, charge_desc, offense_category_desc, offense_type_desc,
#          probability, uccs_code, party, uccs_drug_plus, poss, drug,
#          uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
#          hospitality, forgery)

# This should be equivalent to the above, minus the `sq780_statute = case_when`
# and `filter(count_as_disposed == "")` bits, but is hopefully more readable
clean_df <- data_clean |>
  mutate(
    # Is the charge a drug possession charge covered by SQ780? -----------------
    sq780_drugs = if_else(
      # UCCS code or regex indicating drug possession, and no disqualifying regex
      (uccs_drug_plus | regex_drugs) & !disqualifying_regex, TRUE, FALSE),
    # Is the charge a property crime covered by SQ780? -------------------------
    sq780_props = if_else(
      # UCCS code or regex indicating 780 property crime, and no disqualifying regex
      (uccs_property | regex_props) & !disqualifying_regex, TRUE, FALSE),
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
        "21 O.S. Section 1719.1 -- Taking Domesticated Gish/Game",
      # Taking crude oil / gas / related
      str_detect(count_as_disposed, "(?i)oil|drilling|gas") ~
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
      # Vehicle Embezzlement
      embezzle & str_detect(count_as_disposed, "(?i)vehicle|motor") ~
        "21 O.S. Sections 1521 -- Vehicle Embezzlement",
      # Fraud
      false_pretense | str_detect(count_as_disposed, "cheat|defraud|personat") ~
        "21 O.S. Sections 1541.1, 1541.2, 1541.3 -- Fraud",
      # Pawn Shop Fraud
      pawn ~
        "59 O.S. Section 1512 -- Defrauding Pawn Shop",
      # Forgery
      forgery ~
        "21 O.S. Section 1579, Section 1621 -- Forgery",
      # Default
      TRUE ~ "Unknown / Other"
    )
  ) |>
  # I'm also gonna remove the filter for now so we can see what got classified as non-780
  # filter(sq780_drugs | sq780_props) |>
  select(id, case_type, date_filed, count_as_disposed, disposition_date,
         disposition, charge_desc, offense_category_desc, offense_type_desc,
         probability, uccs_code, party, uccs_drug_plus, poss, drug,
         uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
         hospitality, forgery,
         # keeping my new vars as well
         sq780_drugs, sq780_props, sq780_statute) |>
  # There are a few of these cluttering things up
  filter(count_as_disposed != "")

# Looking at the cleaned data a bit closer =====================================
# Drugs first... ---------------------------------------------------------------
clean_df |>
  filter(sq780_drugs) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# - Tax stamp shouldn't be counted here because it's still a felony I believe -- added to regex
# - is "Larceny of a CDS" type stuff covered under 780? That's currently included as a property crime
# - is "Attempting to obtain CDS" stuff covered?

# Property crimes next... ------------------------------------------------------
clean_df |>
  filter(sq780_props) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# I'd like to have someone who's more familiar look over the top ~100 though just to be sure
# - "IMPERSONATING POLICE OFFICER"? -- Added to regex above.
# - "FALSE PERSONATION"? -- not added to regex above
# - Currently, "CREATING A LIABILITY BY FALSE PERSONATION" is not counted because it's a public order
# crime in UCCS. However, "FALSE PERSONATION" is included, because it's fraud / property crime in UCCS
# - Is "Burglary" covered / different from larceny? "Breaking and Entering"?
# - Is "Malicious injury to property" covered?
# - Is credit card fraud stuff covered?

# Looking at the excluded charges ----------------------------------------------
clean_df |>
  filter(!sq780_drugs, !sq780_props) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# - "BOGUS CHECK" is on here a few times, that's covered under Section 1541 -- Added to regex above.

# Looking by sq780_statute var -------------------------------------------------
clean_df |>
  filter(sq780_drugs | sq780_props) |>
  select(!c(id, case_type, date_filed, disposition_date, party, uccs_code)) |>
  View()

clean_df |>
  select(!c(id, case_type, date_filed, disposition_date, party, uccs_code)) |>
  filter(sq780_drugs | sq780_props) |>
  group_by(sq780_statute) |>
  summarize(
    n_charges = n()
  ) |>
  arrange(desc(n_charges))

# "Unknown / other"s only -- these are currently counted as SQ780 charges, but don't have a
# specific statute attached yet.
clean_df |>
  filter(sq780_drugs | sq780_props) |>
  # filter(sq780_statute == "Unknown / Other") |>
  group_by(sq780_statute) |>
  count(count_as_disposed, sort = T) |>
  filter(n >= 100) |>
  slice_max(n = 50,
            order_by = n) |>
  View()

# Summarizing for export =======================================================

# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
sq780_counts <- clean_df |>
  filter(sq780_drugs | sq780_props) |> # Filter out non-780 stuff
  filter(
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01",
  ) |>
  mutate(year = lubridate::year(date_filed)) |>
  distinct(id, case_type, date_filed, year) |>
  count(year, case_type) # You can do this in one step w/out group_by()!

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
  )

ggplot(sq780_counts, aes(x=year, y=n, group=case_type)) +
  geom_line(aes(color=case_type), linewidth=0.5, alpha=0.9) +
  scale_color_manual(values=c("#999999", "#E69F00", "#56B4E9")) +
  scale_y_continuous(labels = scales::comma,
                     limits = c(0, NA)) +
  theme(legend.position="bottom",
        legend.title = element_text(size = 10),
        legend.text = element_text(size = 10)) +
  theme(
    plot.title = element_text(size = 14),
    axis.title = element_text(size = 12),
    plot.subtitle = element_text(size = 8),
    plot.caption = element_text(size = 8),
    axis.text = element_text(size = 10)) +
  # scale_x_continuous(breaks = 2014:2022) +
  labs(title = "SQ780 Offenses 2014-2022",
       subtitle = "Simple possession drug and property crime charges\nfiled before and after the passing of SQ780",
       x = "Year",
       y = "Number of Charges",
       color = "Case Type",
       caption = "This data is based on the total number of charges in \nthe court records, not cases among OSCN counties only."
  )


########################### SQ780 offenses:
# 63 O.S. 2011 Section 2-402 -- simlibrary(readr)
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

# * Number of simply possession cases charged in 2014, 2015, 2016
# * Number of simple possessions in 2021 and 2022

# parties_2014_2022 <- parties_df |>
#   filter(
#     date_filed >= "2014-01-01",
#     date_filed < "2023-01-01",
#   ) |>
#   mutate(year = lubridate::year(date_filed)) |>
#   distinct(party, case_type, date_filed, year) |>
#   group_by(year, case_type) |>
#   count()
#
# parties_2014_2022 |>
#   pivot_wider(names_from = case_type, values_from = n) |>
#   mutate(
#     total = CM + CF,
#     statewide = round(total * 2.3778)
#   ) |>
#   ungroup() |>
#   gt() |>
#   cols_label(contains("CM") ~ "Misdemeanor",
#              contains("CF") ~ "Felony",
#              contains("y") ~ "Year",
#              total ~ "Total (OSCN)",
#              statewide ~ "Statewide Estimate"
#             ) |>
#   tab_header(
#     title = "Annual Simple Possession Drug Convictions",
#     subtitle = "Before and After SQ780 Passing"
#   )
#
# parties_2014_2022 |>
#   ggplot(aes(x = year, y = n, fill = case_type)) +
#   geom_bar(stat = "identity") +
#   scale_x_continuous(breaks = 2014:2022) +
#   theme(axis.text.x = element_text(angle = 45, vjust=0.5)) +
#   scale_y_continuous(labels = scales::comma) +
#   labs(title = "Simple Possession Drug Convictions, OSCN Counties",
#        subtitle = "Before and After SQ780 Passing",
#        x = "Year",
#        y = "Number of Convictions",
#        caption = "This data is based on the total number\nof parties convicted, not the number of cases\nfiled.")
#
# parties_2014_2022 |>
#   pivot_wider(names_from = case_type, values_from = n) |>
#   mutate(
#     total = CM + CF,
#     statewide = round(total * 2.3778)
#   ) |>
#   write_csv(here("data/parties_convictions_2014_2022.csv"))


# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
# ojodb <- ojo_connect()

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

# Cleaning data ================================================================

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
  filter(offense_type_desc == "Property") |>
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

# Classifying data by SQ780 relevance ==========================================

data_clean <- result_clean |>
  select(all_of(columns)
  ) |>
  mutate(
    # 780 drug regex -----------------------------------------------------------
    poss = str_detect(count_as_disposed, "(?i)poss|poass of cds|poissession"), # added typos I found in `n`
    drug = str_detect(count_as_disposed, "(?i)CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |\\bPRESCRIP|\\bNARC|\\bMETH|\\bC\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|\\bPARAPH\\b|\\bMA.*NA\\b|\\bMJ\\b|\\bMARI\\b"),

    # 780 property regex -------------------------------------------------------
    larceny = str_detect(count_as_disposed, "(?i)larceny|theft|LMFR|larceny of merchandise|meat|corporeal property|\\bstol(e|en)\\b|\\blarc\\b"),
    receive_stolen = str_detect(count_as_disposed, "(?i)RCSP|stolen property|stoeln|concealing stolen"),
    embezzle = str_detect(count_as_disposed, "(?i)embezzlement|embezlement|emblezzlement|embzzlement"),
    misc = str_detect(count_as_disposed, "(?i)fish|domesticated game|\\boil\\b|\\bdrilling\\b|\\bgas\\b"),
    pawn = str_detect(count_as_disposed, "(?i)repay pawn|pawnbroker|pawn shop|in pawn"),
    hospitality = str_detect(count_as_disposed, "(?i)(hotel|inn|restaurant|boarding house|rooming house|motel|auto camp|trailer camp|apartment|rental unit|rental house)"),
    false_pretense = str_detect(count_as_disposed, "(?i)false pretense|con game|confidence game|false representation|trick or deception"), # Andrew: added
    bogus_check = str_detect(count_as_disposed, "(?i)bogus check"), # Andrew: added "bogus check"
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

    # UCCS stuff ---------------------------------------------------------------
    uccs_drug = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3162,
    uccs_drug_plus = probability >= 0.8 & uccs_code >= 3090 & uccs_code <= 3230,
    uccs_property = probability >= 0.8 & uccs_code %in% property_codes$uccs_code,

    # All regex disqualifying a charge from SQ780; this will make the below chunks more readable.
    disqualifying_regex = if_else(
      dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife |
        manufacture | commercial | school_park_church | conspiracy | maintaining |
        minor | inmate | vehicle | proceeds | impersonate_officer | tax_stamp |
        a_b_upon | elderly | larceny_cds | burglary_bne | computers | malicious |
        false_personate | credit_card | medicaid | forgery | fraud | id_theft |
        tampering_w_vehicle | fake_id | misc_exclude,
      TRUE, FALSE
    ),

    # Any drug regex? Not worried about uccs codes with this variable yet
    regex_drugs = if_else(poss & drug, TRUE, FALSE),
    # Any property crime regex? Not worried about uccs codes with this variable
    regex_props = if_else(larceny | receive_stolen | embezzle | misc |
                            pawn | hospitality | false_pretense | bogus_check |
                            false_ownership, TRUE, FALSE)
  ) |>
  arrange(count_as_disposed)

# Andrew note: Gonna try to remove repeated code and make this easier to read
# Basically just replacing the !(dist | intent | paraphernalia...) bit with the `disqualifying_regex` var above
# 406777 rows
# clean_df <- data_clean |>
#   filter(
#     (uccs_drug_plus & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (poss & drug & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (uccs_property & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (larceny & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (receive_stolen & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (embezzle & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (misc & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (pawn & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (hospitality & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))|
#     (forgery & !(dist | intent | paraphernalia | weapon | trafficking | endeavor | wildlife | manufacture | commercial | school_park_church | conspiracy | maintaining | minor | inmate | vehicle | proceeds))
#   ) |>
#   select(id, case_type, date_filed, count_as_disposed, disposition_date,
#          disposition, charge_desc, offense_category_desc, offense_type_desc,
#          probability, uccs_code, party, uccs_drug_plus, poss, drug,
#          uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
#          hospitality, forgery)

# This should be equivalent to the above, minus the `sq780_statute = case_when`
# and `filter(count_as_disposed == "")` bits, but is hopefully more readable
clean_df <- data_clean |>
  mutate(
    # Is the charge a drug possession charge covered by SQ780? -----------------
    sq780_drugs = if_else(
      # UCCS code or regex indicating drug possession, and no disqualifying regex
      (uccs_drug_plus | regex_drugs) & !disqualifying_regex, TRUE, FALSE),
    # Is the charge a property crime covered by SQ780? -------------------------
    sq780_props = if_else(
      # UCCS code or regex indicating 780 property crime, and no disqualifying regex
      (uccs_property | regex_props) & !disqualifying_regex, TRUE, FALSE),
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
  # I'm also gonna remove the filter for now so we can see what got classified as non-780
  # filter(sq780_drugs | sq780_props) |>
  select(id, case_type, date_filed, count_as_disposed, disposition_date,
         disposition, charge_desc, offense_category_desc, offense_type_desc,
         probability, uccs_code, party, uccs_drug_plus, poss, drug,
         uccs_property, larceny, receive_stolen, embezzle, misc, pawn,
         hospitality, forgery,
         # keeping my new vars as well
         sq780_drugs, sq780_props, sq780_statute) |>
  # There are a few of these cluttering things up
  filter(count_as_disposed != "")

# Looking at the cleaned data a bit closer =====================================
# Drugs first... ---------------------------------------------------------------
clean_df |>
  filter(sq780_drugs) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# - Tax stamp shouldn't be counted here because it's still a felony I believe -- added to regex
# - is "Larceny of a CDS" type stuff covered under 780? That's currently included as a property crime
# - is "Attempting to obtain CDS" stuff covered?

# Property crimes next... ------------------------------------------------------
clean_df |>
  filter(sq780_props) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# I'd like to have someone who's more familiar look over the top ~100 though just to be sure
# - "IMPERSONATING POLICE OFFICER"? -- Added to regex above.
# - "FALSE PERSONATION"? -- not added to regex above
# - Currently, "CREATING A LIABILITY BY FALSE PERSONATION" is not counted because it's a public order
# crime in UCCS. However, "FALSE PERSONATION" is included, because it's fraud / property crime in UCCS
# - Is "Burglary" covered / different from larceny? "Breaking and Entering"?
# - Is "Malicious injury to property" covered?
# - Is credit card fraud stuff covered?

# Looking at the excluded charges ----------------------------------------------
clean_df |>
  filter(!sq780_drugs, !sq780_props) |>
  count(count_as_disposed, sort = T) |>
  print(n = 200)

# Looked at the top 200 most common
# - "BOGUS CHECK" is on here a few times, that's covered under Section 1541 -- Added to regex above.

# Looking by sq780_statute var -------------------------------------------------
clean_df |>
  filter(sq780_drugs | sq780_props) |>
  select(!c(id, case_type, date_filed, disposition_date, party, uccs_code)) |>
  View()

clean_df |>
  select(!c(id, case_type, date_filed, disposition_date, party, uccs_code)) |>
  filter(sq780_drugs | sq780_props) |>
  group_by(sq780_statute) |>
  summarize(
    n_charges = n()
  ) |>
  arrange(desc(n_charges))

# "Unknown / other"s only -- these are currently counted as SQ780 charges, but don't have a
# specific statute attached yet.
clean_df |>
  filter(sq780_drugs | sq780_props) |>
  # filter(sq780_statute == "Unknown / Other") |>
  group_by(sq780_statute) |>
  count(count_as_disposed, sort = T) |>
  # filter(n >= 100)
  slice_max(n = 50,
            order_by = n) |>
  View()

# Summarizing for export =======================================================

# * Total number of SQ780 offenses charged in 2014, 2015, and 2016
sq780_counts <- clean_df |>
  filter(sq780_drugs | sq780_props) |> # Filter out non-780 stuff
  filter(
    date_filed >= "2014-01-01",
    date_filed < "2023-01-01",
  ) |>
  mutate(year = lubridate::year(date_filed)) |>
  distinct(id, case_type, date_filed, year) |>
  count(year, case_type) # You can do this in one step w/out group_by()!

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
  )

ggplot(sq780_counts, aes(x=year, y=n, group=case_type)) +
  geom_line(aes(color=case_type), linewidth=0.5, alpha=0.9) +
  scale_color_manual(values=c("#999999", "#E69F00", "#56B4E9")) +
  scale_y_continuous(labels = scales::comma,
                     limits = c(0, NA)) +
  theme(legend.position="bottom",
        legend.title = element_text(size = 10),
        legend.text = element_text(size = 10)) +
  theme(
    plot.title = element_text(size = 14),
    axis.title = element_text(size = 12),
    plot.subtitle = element_text(size = 8),
    plot.caption = element_text(size = 8),
    axis.text = element_text(size = 10)) +
  # scale_x_continuous(breaks = 2014:2022) +
  labs(title = "SQ780 Offenses 2014-2022",
       subtitle = "Simple possession drug and property crime charges\nfiled before and after the passing of SQ780",
       x = "Year",
       y = "Number of Charges",
       color = "Case Type",
       caption = "This data is based on the total number of charges in \nthe court records, not cases among OSCN counties only."
  )


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

ple possession
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


library(dplyr)
library(janitor)
library(stringr)
library(readr)
library(tidyr)
library(purrr)
library(here)
library(lubridate)

profile_data <- read_csv(here("data/input/doc/profile_data.csv"))
offense_data <- read_csv(here("data/input/doc/offense_data.csv"))
sentence_data <- read_csv(here("data/input/doc/sentence_data.csv"))
consecutive_data <- read_csv(here("data/input/doc/consecutive_data.csv"))

#_________________________ Facility categories__________________________________

# Physical Custody according to weekly count reports
assessment_and_reception <- c(
  "LEXINGTON ASSESSMENT AND RECEPTION CENTER",
  "MABEL BASSETT ASSESSMENT & RECEPTION CENTER"
)

state_institutions <- c(
  "ALLEN GAMBLE CORRECTIONAL CENTER",
  "CHARLES E. (BILL) JOHNSON CORRECTIONAL CENTER",
  "DICK CONNER CORRECTIONAL CENTER",
  "DR. EDDIE WARRIOR CORRECTIONAL CENTER (MIN)",
  "GREAT PLAINS CORRECTIONAL CENTER",
  "HOWARD MCLEOD CORRECTIONAL CENTER",
  "JACKIE BRANNON CORRECTIONAL CENTER",
  "JAMES CRABTREE CORRECTIONAL CENTER",
  "JESS DUNN CORRECTIONAL CENTER",
  "JIM E. HAMILTON CORRECTIONAL CENTER",
  "JOHN H. LILLEY CORRECTIONAL CENTER",
  "JOSEPH HARP CORRECTIONAL CENTER",
  "LEXINGTON CORRECTIONAL CENTER",
  "MABEL BASSETT CORRECTIONAL CENTER",
  "MACK ALFORD CORRECTIONAL CENTER",
  "OKLAHOMA STATE PENITENTIARY",
  "OKLAHOMA STATE REFORMATORY",
  "RED ROCK CORRECTIONAL CENTER" # previously private lawton prison
  )

# This is now the Red Rock
# uncomment this if working with data from before 09/2025
#private_facilities <- c("LAWTON CORRECTIONAL AND REHABILITATION FACILITY")

community_centers <- c(
  "CLARA WATERS COMMUNITY CORRECTIONS CENTER",
  "DR. EDDIE WARRIOR CORRECTIONAL CENTER (CCC)",
  "ENID COMMUNITY CORRECTIONS CENTER",
  "LAWTON COMMUNITY CORRECTIONS CENTER",
  "NORTHEAST OKLAHOMA COMMUNITY CORRECTIONS CENTER",
  "OKLAHOMA CITY COMMUNITY CORRECTIONS CENTER", # added this
  "UNION CITY COMMUNITY CORRECTIONS CENTER"
)

halfway_house <- c("BRIDGEWAY HALFWAY HOUSE")

interstate <- c("INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT")

clean_profile_data <- profile_data |>
  mutate(
    name = str_c(if_else(!is.na(last_name), str_c(" ", last_name), ""),
                 if_else(!is.na(suffix), str_c(" ", suffix), ""),
                 ", ",
                 if_else(!is.na(first_name), str_c(" ", first_name), ""),
                 " ",
                 if_else(!is.na(middle_name), str_c(" ", middle_name), ""),
                 "."),
    age = floor(time_length(interval(birth_date, Sys.Date()), "years")),
    age_bucket = case_when(
      age < 18 ~ "Under 18",
      age >= 18 & age < 25 ~ "18-24",
      age >= 25 & age < 35 ~ "25-34",
      age >= 35 & age < 45 ~ "35-44",
      age >= 45 & age < 55 ~ "45-54",
      age >= 55 & age < 65 ~ "55-64",
      age >= 65 ~ "65 and older",
      TRUE ~ "Unknown"
    ),
    physical_custody = facility %in% toupper(c(assessment_and_reception,
                                               state_institutions,
                                               community_centers,
                                               halfway_house,
                                               interstate)) |
      str_detect(facility, "(?i)jail|SHERIFFS OFFICE")
    )

doc_repeat <- sentence_data |>
  mutate(js_date = lubridate::as_date(js_date)) |>
  group_by(doc_num) |>
  summarise(
    num_sentencing_dates = n_distinct(js_date, na.rm = TRUE),
    repeat_offender = num_sentencing_dates > 1)

# We join all the datasets but the fields relevant to the metrics will be in
# sentence data and profile data.
# The nesting and generated variables are to identify the most recent
# sentence for those who may have been in DOC custody more than once.
doc_data_join_all <- sentence_data |>
  left_join(consecutive_data,
            by = "sentence_id") |>
  inner_join(doc_repeat,
             by = "doc_num") |>
  inner_join(
    offense_data |>
      distinct(statute_code, .keep_all = TRUE),
    by = "statute_code"
  ) |>
  rename(offense_description = description,
         sentencing_date = js_date) |>
  mutate(life_sentence = incarcerated_term_in_years == "7777",
         lwop = incarcerated_term_in_years == "8888",
         dp = incarcerated_term_in_years == "9999")|>
  group_by(doc_num) |>
  arrange(doc_num, sentencing_date, sentence_id, statute_code) |>
  # We want the most recent sentence
  mutate(
    most_recent_sentencing_date = max(sentencing_date, na.rm = TRUE),
    most_recent_sentencing_county =
      sentencing_county[sentencing_date == most_recent_sentencing_date][1]
    ) |>
  nest(offenses = c(statute_code,
                    offense_description,
                    sentence_id,
                    consecutive_to_id,
                    sentencing_county,
                    sentencing_date,
                    crf_number,
                    incarcerated_term_in_years,
                    probation_term_in_years,
                    violent,
                    num_sentencing_dates,
                    life_sentence,
                    lwop,
                    dp)) |>
  ungroup() |>
  inner_join(clean_profile_data,
             by = c("doc_num"))


write_csv(doc_data_join_all, here("data", "output", "doc_data_join_all.csv"))

  # Sanity checks that the nest matches the counts
  # mutate(
  #   n_rows_in_nest = map_int(offenses, nrow),
  #   n_dates_in_nest = map_int(offenses, ~ n_distinct(.x$sentencing_date)))


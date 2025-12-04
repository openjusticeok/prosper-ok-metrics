library(dplyr)
library(stringr)
library(here)

# Get helper functions and join DOC datasets
source_files <- list.files(
  here("inst/reports/2025-11-prosper-metrics-prison"),
  pattern = "\\.R$",
  full.names = TRUE
)

for (f in source_files) source(f)

# Load data:
# Currently using the most recent release and this "joined" data is created in
# the 2025_11_processing_doc_data.R script.
doc_data_20251022 <- read_csv(here("data/output/people_with_sentence_info.csv"))

## _________________________Run checks__________________________________________
# Check that we have the sentencing county for each person:
doc_data_20251022 |>
  summarise_missing(most_recent_sentencing_county)

doc_data_20251022 |>
  filter(status == "ACTIVE",
         physical_custody == "TRUE",
         most_recent_sentencing_county == "Not Reported / Not Applicable"
  ) |>
  summarise(total = n())

# Check that we have the sentencing date (called `js_date` in raw data)
# for each person:
doc_data_20251022 |>
  summarise_missing(most_recent_sentencing_date)

# Some nonsense in the date column
# Two dates stand out and may have some meaning "1000-01-01" and "1753-01-01"
doc_data_20251022 |>
  filter(most_recent_sentencing_date <= "1920-01-01") |>
  count(most_recent_sentencing_date)

# These are most likely typos
doc_data_20251022 |>
  filter(most_recent_sentencing_date >= "2025-10-22") |>
  count(most_recent_sentencing_date)

## _______________________ Check total counts __________________________________
# From DOC's weekly count report for 10.20.2025 the total including
# court, hospital, etc. is 23,275.
# Excluding the people in transit the total count is 22,875

# Our count comes up to 22,758.
doc_data_20251022 |>
  filter_people_with_sentence_info(
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE) |>
  summarise_population_count()

# Unless you exclude interstate compacts and then it goes up to 24,261 ://
doc_data_20251022 |>
  filter_people_with_sentence_info(
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = FALSE) |>
  summarise_population_count()

# * Total people currently physically incarcerated who were sentenced in Tulsa:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE) |>
  summarise_population_count()

# * Total number of people in DOC custody who were sentenced in Tulsa:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE) |>
  summarise_population_count()

# * Total people currently physically incarcerated sentenced in Oklahoma County:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE) |>
  summarise_population_count()

# * Total number of people in DOC custody who were sentenced in Oklahoma County:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE) |>
  summarise_population_count()

## Calculate metrics requested in report using the metric_helper_functions.R

## ___________________ Metric 1: "Prison Sentences"_____________________________
# Counting the number of people sentenced in Tulsa county

# * Physically incarcerated and sentenced in Tulsa SINCE LAST YEAR:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    # Using the date of the data released a year ago
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count()

# * Total number of people in DOC custody sentenced in Tulsa SINCE LAST YEAR
# (This gives us a number that's closer to last year's GKFF report)
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    # Using the date of the data released a year ago
    sentence_date = as.Date("2024-10-16")
  )|>
  summarise_population_count()

## __________________ Metric 2: "Population by Gender"_________________________
# The same counts as above now dis-aggregated by sex

# * Physically incarcerated sentenced in Tulsa by sex SINCE LAST YEAR
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count("sex")


# * Total number of people in DOC custody sentenced in Tulsa by sex SINCE LAST YEAR
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Tulsa",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count("sex")

# Metric 3: "Oklahoma County comparison to Tulsa County vs all other counties"
# * Physically incarcerated and sentenced in Oklahoma County SINCE LAST YEAR:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count()

# * Total people in DOC custody who were sentenced in Oklahoma County SINCE LAST YEAR:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count()

# * Physically incarcerated and sentenced in Oklahoma county by sex SINCE LAST YEAR:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = TRUE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count("sex")

# * TOTAL number of people in DOC custody sentenced in Oklahoma county by sex SINCE LAST YEAR:
doc_data_20251022 |>
  filter_people_with_sentence_info(
    sentencing_county = "Oklahoma",
    physical_custody_only = FALSE,
    status_active_only = TRUE,
    exclude_interstate = TRUE,
    sentence_date = as.Date("2024-10-16")
  ) |>
  summarise_population_count(c("sex"))

library(dplyr)
library(stringr)

doc_data_20251022 <- read_csv(here("data/output/doc_data_join_all.csv"))

# The main fields we are concerned with for these metrics
# are the sentencing county and date.

# Check that we have the sentencing county for each person:
doc_data_20251022 |>
  summarise(
    missing = sum(is.na(most_recent_sentencing_county)),
    total = n()
  )

# Check that we have the sentencing date (called `js_date` in raw data)
# for each person:
doc_data_20251022 |>
  summarise(
    min_date = min(most_recent_sentencing_date, na.rm = TRUE),
    max_date = max(most_recent_sentencing_date, na.rm = TRUE),
    missing = sum(is.na(most_recent_sentencing_date)),
    total = n()
  )

doc_data_20251022 |>
  filter(most_recent_sentencing_county == "Not Reported / Not Applicable") |>
  summarise(
    min_date = min(most_recent_sentencing_date, na.rm = TRUE),
    max_date = max(most_recent_sentencing_date, na.rm = TRUE),
    total = n()
  )

# There's some nonsense in the date column, let's see how bad it is:
doc_data_20251022 |>
  filter(most_recent_sentencing_date >= "2025-10-22")

doc_data_20251022 |>
  filter(most_recent_sentencing_date <= "1900-01-01")

## _______________________ Check total counts __________________________________
# From weekly count report for 10.20.2025 the total including
# court, hospital, etc. is 23,275
# Excluding the people in transit the total count is 22,875
# Our count is 22,758

# * Total number of people currently physically incarcerated:
doc_data_20251022 |>
  filter(
    physical_custody == "TRUE",
    status == "ACTIVE",
    # When we include interstate compact this number balloons
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

# * Total people currently physically incarcerated sentenced in Tulsa County:
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    physical_custody == "TRUE",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

# * Total number of people in DOC custody who were sentenced in Tulsa:
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

## ___________________ Metric 1: "Prison Sentences"_____________________________
# Counting the number of people sentenced in Tulsa county

# * Physically incarcerated sentenced in Tulsa  SINCE LAST YEAR:
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    physical_custody == "TRUE",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    # Using the date of the data released a year ago but not positive this is
    # what we should use:
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

# * Total number of people in DOC custody sentenced in Tulsa SINCE LAST YEAR
# (This gives us a number that's closer to last year's GKFF report)
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    # Using the date of the data released a year ago but not positive this is
    # what we should use:
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

## __________________ Metric 2: "Population by Gender"_________________________
# The same counts as above now dis-aggregated by sex

# * Physically incarcerated sentenced in Tulsa by sex SINCE LAST YEAR
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    physical_custody == "TRUE",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  group_by(sex) |>
  summarize(
    n_people = n_distinct(doc_num)
  )


# * Total number of people in DOC custody sentenced in Tulsa SINCE LAST YEAR
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Tulsa",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  group_by(sex) |>
  summarize(
    n_people = n_distinct(doc_num)
  )


# "Oklahoma County comparison to Tulsa County vs all other counties"
# * Same metrics for people sentenced in Oklahoma County
doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Oklahoma",
    physical_custody == "TRUE",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  summarize(
    n_people = n_distinct(doc_num)
  )

doc_data_20251022 |>
  filter(
    most_recent_sentencing_county == "Oklahoma",
    physical_custody == "TRUE",
    status == "ACTIVE",
    facility != "INTERSTATE COMPACT (OUT TO OTHER STATE) UNIT",
    most_recent_sentencing_date >= "2024-10-16"
  ) |>
  group_by(sex) |>
  summarize(
    n_people = n_distinct(doc_num)
  )


# * Same metrics for all counties excluding Tulsa and Oklahoma counties

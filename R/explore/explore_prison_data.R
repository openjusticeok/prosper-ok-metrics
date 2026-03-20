# TODO: Is it possible that some people with complex consecutive sentence
# schemes are actually eligible for parole or release?

# Check for duplicate sentence and consecutive sentence IDs within the same snapshot
# In order to understand the meaning of the data and check data quality issues
# Findings:
# 1) There are no duplicate sentence_id/snapshot_date combinations in the sentence_data.
# but there are 12k rows in the consecutive_data.
# If the "consecutive_to_id" variable indicates that the sentence in sentence_id
# is consecutive to the sentence in consecutive_to_id, then we would expect
# that each sentence_id in the sentence_data would appear at most once
# in the consecutive_data. Since we see around 12k rows, this suggests either
# that some sentences are marked as consecutive to multiple other sentences, or
# some data error. It may be possible for a sentence to be consecutive
# to the greater of two other concurrent sentences.
# Example:
  # https://www.okcca.net/cases/2020/OK-CR-14/
  # https://oscn.net/dockets/GetCaseInformation.aspx?db=tulsa&number=CF-2017-3086
# In Newman v. State (2020 OK CR 14), Count 1 is the “anchor” sentence
# nothing is stated as running before it.
# The opinion describes the structure like this:
# Count 2 consecutive to Count 1; Count 3 concurrent with Count 2;
# Count 4 consecutive to Counts 1, 2, and 3; Count 5 concurrent with Count 4.
# So operationally:
# Count 1 runs first.
# Count 2 starts after Count 1 ends.
# Count 3 runs at the same time as Count 2 (so it also starts after Count 1 ends).
# Count 4 starts after Count 1 and after the Count 2/3 block is completed.
# Count 5 runs at the same time as Count 4.
# 2) 435k rows have duplicate consecutive_to_id/snapshot_date combinations
# Implying 2 or more sentences are marked as consecutive to the same sentence.
# In the above exampe, this occurs when Counrt 2 is consequtive to Count 1,
# and Count 4 is also consecutive to Count 1.
# 3) 11k rows have sentence_id that matches a consecutive_to_id within the
# same snapshot_date. Need exploration for why this occurs. Check individual
# case information to understand if this is a data error or if it reflects some complex
# sentencing structure.
# The sentence data for the individual in case above.
sentence_data |>
  dplyr::filter(doc_num == "0000718254", snapshot_date == "2024-10-16")
# The great irony is these consecutive sentences (nor the other sentences for
# this individual) don't appear in the consecutive_data, which suggests some data
# entry issues may be pervasive.
consecutive_data |>
  dplyr::filter(stringr::str_detect(sentence_id, "00718254"))


# There are no duplicate sentence_id's in sentence table.
sentence_data |>
  dplyr::filter(n() > 1, .by = c("sentence_id",  "snapshot_date"))

# Duplicate sentence IDs in consecutive_sentence table
consecutive_data |>
  filter(sentence_id != consecutive_to_id) |>
  dplyr::filter(
    n() > 1,
    .by = c("sentence_id", "snapshot_date")
  ) |>
  arrange(snapshot_date, sentence_id, consecutive_to_id)
# Duplicate consecutive_to_id
consecutive_data |>
  filter(sentence_id != consecutive_to_id) |>
  filter(
    n() > 1,
    .by = c("consecutive_to_id",  "snapshot_date")
  ) |>
  arrange(snapshot_date, consecutive_to_id, sentence_id)
# Matching sentence_id and consecutive_to_id
consecutive_data |>
  filter(consecutive_to_id == sentence_id)


# TODO:  Explore the use of "C" for circumstancial violence in the offense data,
# which is described in our ODOC documentation but requires further exploration
# of how it's used and how it affects our analyses, and how to best use it
# in our processed datasets.
  offense_data |>
    # the changes in offense data itself.
    # We usually want the latest offense data unless we are analyzing
    filter(snapshot_date == max(snapshot_date)) |>
    # There are ~2 offense which are not unique. They have a row where
    # violent is "N" for not violent and "C" for circumstancial. See
    # documentation for more details.
    # For now keep the first row, which is "C".
    distinct(statute_code, .keep_all = TRUE)

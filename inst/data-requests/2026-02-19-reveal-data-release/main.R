library(googledrive)
library(dplyr)
library(fs)
library(here)
library(purrr)
library(readr)
library(stringr)
library(janitor)
library(lubridate)

source(here::here("inst/data-requests/2026-02-19-reveal-data-release/preprocess.R"))

# Data originally obtained 2025-02-19
# Article: https://revealnews.org/article/before-you-dive-into-oklahomas-prison-data-read-reveals-tips/
# Download: https://s3-us-west-2.amazonaws.com/oklahoma-offender-data/oklahoma_offender_data.zip

# Fetch

drive_folder_id <- googledrive::as_id("1zQFDJXs0z5ADlOtysulI8zERCxrCXvSv")
drive_files <- googledrive::drive_ls(
  drive_folder_id,
  type = "csv"
) |>
  select(name, id)

input_dir <- here::here("data/input/2026-02-19-reveal-data-release")
output_dir <- here::here("data/output/2026-02-19-reveal-data-release")
preprocess_dir <- here::here(output_dir, "preprocess")

fs::dir_create(input_dir)
fs::dir_create(output_dir)
fs::dir_create(preprocess_dir)

data_files <- purrr::pmap_chr(
  drive_files,
  \(name, id) {
    file_path <- fs::path(input_dir, name)
    googledrive::drive_download(
      file = id,
      path = file_path,
      overwrite = TRUE
    )
    file_path
  }
)

# Preprocess

preprocess_offender(
  input_file = fs::path(input_dir, "Offender.csv"),
  output_file = fs::path(preprocess_dir, "offender.csv")
)

preprocess_offender_alias(
  input_file = fs::path(input_dir, "OffenderAlias.csv"),
  output_file = fs::path(preprocess_dir, "offender_alias.csv")
)

preprocess_offender_sentence(
  input_file = fs::path(input_dir, "OffenderSentence.csv"),
  output_file = fs::path(preprocess_dir, "offender_sentence.csv")
)

preprocess_offender_reception(
  input_file = fs::path(input_dir, "OffenderReception.csv"),
  output_file = fs::path(preprocess_dir, "offender_reception.csv")
)

preprocess_offender_exit(
  input_file = fs::path(input_dir, "OffenderExit.csv"),
  output_file = fs::path(preprocess_dir, "offender_exit.csv")
)

# Upload Preprocessed
drive_preprocessed_folder_id <- drive_mkdir(
  name = "preprocessed",
  path = drive_folder_id,
  overwrite = TRUE
) |>
  pull(id)

preprocessed_data_files <- fs::dir_ls(preprocess_dir)

walk(
  preprocessed_data_files,
  \(x) {
    drive_upload(x, drive_preprocessed_folder_id, overwrite = TRUE)
  }
)

# Load

walk(
  preprocessed_data_files,
  \(x) {
    obj_name <- fs::path_file(x) |>
      fs::path_ext_remove()

    data <- readr::read_csv(
      x,
      col_types = cols(.default = col_character())
    ) |>
      janitor::clean_names()

    assign(obj_name, data, envir = .GlobalEnv)
  }
)

# Explore

offender |>
  select(doc_num, race, gender, dob, reception_date, current_facility, status)

offender_sentence |>
  count(offence_description, sort = TRUE)

offender_sentence |>
  count(offence_comment, sort = TRUE)

offender_sentence |>
  count(sentence_term_code, sort = TRUE)

offender_sentence |>
  count(sentence_term, sort = TRUE)

offender_sentence |>
  count(order_code, sort = TRUE)

offender_sentence |>
  count(charge_status, sort = TRUE)

offender_reception |>
  count(
    facility,
    sort = TRUE
  )

offender_reception |>
  count(
    doc_num,
    name = "n_receptions"
  ) |>
  count(n_receptions, sort = TRUE)

offender_exit |>
  count(
    exit_reason,
    sort = TRUE
  )

offender_exit |>
  count(
    doc_num,
    name = "n_exits"
  ) |>
  count(n_exits, sort = TRUE)

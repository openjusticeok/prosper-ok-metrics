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

# Load

## Remaining 2 warnings are expected
offender <- readr::read_csv(
  fs::path(preprocess_dir, "offender.csv"),
  col_types = cols(.default = col_character())
) |>
  janitor::clean_names()

offender_alias <- readr::read_csv(
  fs::path(preprocess_dir, "offender_alias.csv"),
  show_col_types = FALSE
) |>
  janitor::clean_names()

offender_sentence <- readr::read_csv(
  fs::path(preprocess_dir, "offender_sentence.csv"),
  show_col_types = FALSE
) |>
  janitor::clean_names()

offender_reception <- readr::read_csv(
  fs::path(preprocess_dir, "offender_reception.csv"),
  show_col_types = FALSE
) |>
  janitor::clean_names()

offender_exit <- readr::read_csv(
  fs::path(preprocess_dir, "offender_exit.csv"),
  show_col_types = FALSE
) |>
  janitor::clean_names()


# # Clean
#
# offender |>
#   select(
#     -c(
#       "hair_color",
#       "eye_color",
#       "height",
#       "weight"
#     )
#   ) |>
#   mutate(
#     across(
#       c("dob", ends_with("_date")),
#       \(x) lubridate::ymd_hms(x)
#     )
#   ) |>
#   glimpse()
#
# ## Some `doc_num` are not actual numbers and can break joins
# offender |>
#   filter(
#     is.na(as.integer(doc_num))
#   ) |>
#   distinct(doc_num)
#
# offender_alias
# offender_sentence
# offender_reception
# offender_exit
#

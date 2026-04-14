library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(readr)
library(googledrive)
library(here)
library(readxl)
library(pointblank)

# Ingest
single_drive_folder_url <- "https://drive.google.com/drive/u/0/folders/1AcwuaOHYifDDYHCHQaykAzLba1yDkHcZ"
folder_id <- googledrive::as_id(single_drive_folder_url)
folder_meta <- googledrive::drive_get(folder_id)
drive_files <- googledrive::drive_ls(folder_id)

input_dir <- here::here("data/input/doc/2026-03-13-gkff/")
fs::dir_create(input_dir)

purrr::pwalk(
  .l = drive_files,
  .f = function(id, name, ...) {
    googledrive::drive_download(
      file = id,
      path = here::here(input_dir, name),
      overwrite = TRUE
    )
  }
)

sentences_data <- readxl::read_xlsx(here(input_dir, "Sentences.xlsx"))
receptions_data <- readxl::read_xlsx(here(input_dir, "OffenderReception.xlsx"))
releases_data <- readxl::read_xlsx(here(input_dir, "OffenderExit.xlsx"))
profile_data <- readxl::read_xlsx(here(input_dir, "Profile.xlsx"))
offense_data <- readxl::read_xlsx(here(input_dir, "Offense.xlsx"))
alias_data <- readxl::read_xlsx(here(input_dir, "Alias.xlsx"))

# Check Ingested
sentences_data
receptions_data
releases_data
profile_data
offense_data
alias_data


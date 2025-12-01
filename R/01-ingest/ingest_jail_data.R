# Function to injest Tulsa jail data using the OK Policy's scraper package
ingest_okpolicy_scraped_data <- function() {
  scraped_jail_data <- tulsaCountyJailScraper::scrape_data()

  list(
    bookings = scraped_jail_data$bookings_tibble |>
      dplyr::mutate(
        created_at = lubridate::today()
      ),
    charges = scraped_jail_data$charges_tibble |>
      dplyr::mutate(
        created_at = lubridate::today()
      )
  )
}

# Function to ingest Asemio-scraped jail data from an OJO database
ingest_asemio_scraped_data <- function() {
  list(
    bookings = ojodb::ojo_tbl("arrest", schema = "iic") |>
      dplyr::collect(),
    charges = ojodb::ojo_tbl("offense", schema = "iic") |>
      dplyr::collect(),
    inmates = ojodb::ojo_tbl("inmate", schema = "iic") |>
      dplyr::collect()
  )
}

# Function to read static Brek Jail Report data from a CSV file
read_brek_jail_report_data <- function(path = here::here("data", "input", "brek_jail_report_data.csv")) {
  if (!file.exists(path)) {
    stop(
      sprintf("Static Brek reference file not found at %s. Run the one-time ingest script.", path),
      call. = FALSE
    )
  }

  brek_jail_data <- readr::read_csv(path, show_col_types = FALSE)

  return(brek_jail_data)
}

# Function to read Jail Data Initiative scraped data from a Google Drive folder
read_jail_data_initiative_scraped_data <- function(drive_folder_url = "https://drive.google.com/drive/folders/1fsv2pAkRd6DoDgG77SoXA3-0wK5tUnmh",
                                                   charges_filename = "charges.csv",
                                                   people_filename = "people.csv",
                                                   download_dir = tempdir()) {
  handle_drive_error <- function(err) {
    message <- conditionMessage(err)
    is_auth_issue <- grepl("unauthor|forbidden|auth", message, ignore.case = TRUE)

    if (is_auth_issue) {
      stop(
        paste(
          "Google Drive authentication error:",
          "Please ensure you have authenticated with Google Drive using the googledrive package.",
          "Run googledrive::drive_auth() with valid credentials before ingesting.",
          "See https://googledrive.tidyverse.org/articles/googledrive.html#authentication for more information.",
          sprintf("Error details: %s", message),
          sep = "\n"
        ),
        call. = FALSE
      )
    }

    stop(err)
  }

  drive_folder <- googledrive::as_id(drive_folder_url)

  drive_files <- tryCatch(
    googledrive::drive_ls(drive_folder),
    error = handle_drive_error
  )

  read_remote_csv <- function(target_name) {
    target_file <- drive_files |>
      dplyr::filter(name == target_name)

    if (nrow(target_file) == 0) {
      stop(sprintf("File '%s' was not found in the provided Google Drive folder.", target_name), call. = FALSE)
    }

    # Create download directory if it doesn't exist
    if (!dir.exists(download_dir)) {
      dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)
    }
    # Define local path for download
    local_path <- file.path(download_dir, target_name)
    # Ensure temporary file is deleted after reading
    on.exit({
      if (file.exists(local_path)) {
        unlink(local_path)
      }
    }, add = TRUE)

    # Download the file, handling authentication errors
    tryCatch(
      googledrive::drive_download(
        file = target_file,
        path = local_path,
        overwrite = TRUE
      ),
      error = handle_drive_error
    )

    # Read the CSV with all columns as character to avoid type issues
    readr::read_csv(
      local_path,
      col_types = readr::cols(.default = readr::col_character())
    )
  }

  # Read both people and charges data
  list(
    people = read_remote_csv(people_filename),
    charges = read_remote_csv(charges_filename)
  )
}

# Target function to ingest all raw jail data sources
ingest_jail_data <- function(
  read_jail_data_initiative = read_jail_data_initiative_scraped_data,
  ingest_okpolicy = ingest_okpolicy_scraped_data,
  ingest_asemio = ingest_asemio_scraped_data,
  read_brek_jail_report = read_brek_jail_report_data
) {
  total_sources <- 4

  jail_data_initiative <- read_jail_data_initiative()
  message(sprintf("(1/%d) jail_data_initiative ingestion complete", total_sources))

  okpolicy <- ingest_okpolicy()
  message(sprintf("(2/%d) okpolicy ingestion complete", total_sources))

  asemio <- ingest_asemio()
  message(sprintf("(3/%d) asemio ingestion complete", total_sources))

  brek <- read_brek_jail_report()
  message(sprintf("(4/%d) brek ingestion complete", total_sources))

  list(
    jail_data_initiative = jail_data_initiative,
    okpolicy = okpolicy,
    asemio = asemio,
    brek = brek
  )
}

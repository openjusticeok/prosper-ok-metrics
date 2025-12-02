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
ingest_brek_jail_report_data <- function(path = here::here("data", "input", "brek_jail_report_data.csv")) {
  if (!file.exists(path)) {
    message(
      sprintf(
        "Static Brek reference file not found at %s. Running the one-time ingest script.",
        path
      )
    )

    brek_year <- 2024L

    # Data manually transcribed from Brek Jail Report PDF for 2024
    # See inst/reports/old-brek-reports-for-reference
    # From section 1: Average Daily Population (ADP)
    brek_adp_2024 <- dplyr::tribble(
      ~metric_type, ~category, ~oklahoma_county, ~tulsa_county, ~yoy_change_ok, ~yoy_change_tulsa,
      "Overall", "Total", 1404, 1333, -0.053, -0.016,
      "Gender", "Male", 1200, 1121, -0.071, -0.017,
      "Gender", "Female", 204, 212, 0.068, -0.006,
      "Race", "White", 542, 603, -0.047, -0.055,
      "Race", "Black", 608, 466, -0.051, -0.001,
      "Race", "Native American", 51, 72, -0.030, 0.034,
      "Race", "Hispanic", 189, 174, -0.087, 0.056
    )

    # From section 2: Jail Bookings and Releases
    brek_bookings_2024 <- dplyr::tribble(
      ~metric_type, ~category, ~oklahoma_county, ~tulsa_county, ~yoy_change_ok, ~yoy_change_tulsa,
      "Overall", "Total", 20890, 15900, -0.073, 0.046,
      "Gender", "Male", 15748, 11975, -0.062, 0.054,
      "Gender", "Female", 5142, 3925, -0.107, 0.024,
      "Race", "White", 9020, 8730, -0.071, 0.016,
      "Race", "Black", 8007, 4491, -0.070, 0.080,
      "Race", "Native American", 745, 850, -0.038, -0.037,
      "Race", "Hispanic", 2566, 1597, -0.105, 0.182
    )

    # From section 3: Tulsa County Release Dispositions
    # The release disposition counts
    brek_tulsa_release_disposition_counts_2024 <- dplyr::tribble(
      ~disposition_type,             ~total, ~male, ~female,
      "Bond (Surety/Cash)",          6455,   4829,  1626,
      "Dismissed/Withdrawn",         2425,   1785,  640,
      "Sentence Served",             1385,   1082,  302,
      "Atty/Personal Recognizance",  873,    590,   283
    )

    # The percentage a disposition makes up all dispositions by race
    brek_tulsa_release_disposition_pct_race_2024 <- dplyr::tribble(
      ~disposition_type,            ~white, ~black, ~native_american, ~hispanic, ~other,
      "Bond (Surety/Cash)",         0.408,  0.446,  0.062,            0.440,     0.453,
      "Dismissed/Withdrawn",        0.129,  0.123,  0.668,            0.096,     0.163,
      "Sentence Served",            0.086,  0.092,  0.025,            0.113,     0.058,
      "Atty/Personal Recognizance", 0.067,  0.051,  0.016,            0.025,     0.016
    )

    # From section 4: Average Length of Stay
    brek_tulsa_avg_length_of_stay_2024 <- dplyr::tribble(
      ~metric_type, ~category, ~avg_days, ~yoy_change,
      "Overall", "Total", 31.5, 0.069,
      "Gender", "Male", 35.6, 0.095,
      "Gender", "Female", 19.0, -0.034,
      "Race", "White", 26.8, 0.024,
      "Race", "Black", 35.3, -0.039,
      "Race", "Native American", 35.2, 0.692, # Notable increase
      "Race", "Hispanic", 45.3, 0.157
    )

    # From section 5: Rebooking Rates
    brek_tulsa_rebooking_rates_2024 <- dplyr::tribble(
      ~metric_type, ~category, ~rebooking_rate, ~yoy_change,
      "Overall", "Total", 0.247, 0.004,
      "Gender", "Male", 0.251, 0.004,
      "Gender", "Female", 0.236, 0.003,
      "Race", "White", 0.240, -0.007, # Text body says 23.9%, chart says 24.0%. Used chart.
      "Race", "Black", 0.286, 0.025,
      "Race", "Native American", 0.238, 0.042,
      "Race", "Hispanic", 0.189, -0.003
    )

    brek_columns <- c(
      "year", "metric_family", "metric", "dimension", "category", "group", "county", "value", "yoy_change"
    )

    format_county_metric <- function(df, metric_label, metric_family_label) {
      df |>
        tidyr::pivot_longer(
          cols = c(oklahoma_county, tulsa_county),
          names_to = "county",
          values_to = "value"
        ) |>
        dplyr::mutate(
          yoy_change = dplyr::case_when(
            county == "oklahoma_county" ~ yoy_change_ok,
            county == "tulsa_county" ~ yoy_change_tulsa,
            TRUE ~ NA_real_
          ),
          county = dplyr::case_when(
            county == "oklahoma_county" ~ "Oklahoma County",
            county == "tulsa_county" ~ "Tulsa County",
            TRUE ~ county
          ),
          year = brek_year,
          metric_family = metric_family_label,
          metric = metric_label,
          dimension = metric_type,
          group = NA_character_
        ) |>
        dplyr::select(dplyr::all_of(brek_columns))
    }

    adp_summary <- format_county_metric(
      brek_adp_2024,
      metric_label = "Average Daily Population",
      metric_family_label = "adp"
    )

    bookings_summary <- format_county_metric(
      brek_bookings_2024,
      metric_label = "Annual Bookings",
      metric_family_label = "bookings"
    )

    release_disposition_counts <- brek_tulsa_release_disposition_counts_2024 |>
      dplyr::rename(category = disposition_type) |>
      tidyr::pivot_longer(
        cols = c(total, male, female),
        names_to = "group",
        values_to = "value"
      ) |>
      dplyr::mutate(
        group = dplyr::case_when(
          group == "total" ~ "All",
          group == "male" ~ "Male",
          group == "female" ~ "Female",
          TRUE ~ group
        ),
        year = brek_year,
        metric_family = "release_disposition_counts",
        metric = "Release Counts",
        dimension = "Disposition",
        county = "Tulsa County",
        yoy_change = NA_real_
      ) |>
      dplyr::select(dplyr::all_of(brek_columns))

    release_disposition_shares <- brek_tulsa_release_disposition_pct_race_2024 |>
      dplyr::rename(category = disposition_type) |>
      tidyr::pivot_longer(
        cols = c(white, black, native_american, hispanic, other),
        names_to = "group",
        values_to = "value"
      ) |>
      dplyr::mutate(
        group = dplyr::case_when(
          group == "white" ~ "White",
          group == "black" ~ "Black",
          group == "native_american" ~ "Native American",
          group == "hispanic" ~ "Hispanic",
          group == "other" ~ "Other",
          TRUE ~ group
        ),
        year = brek_year,
        metric_family = "release_disposition_share",
        metric = "Release Disposition Share by Race",
        dimension = "Disposition",
        county = "Tulsa County",
        yoy_change = NA_real_
      ) |>
      dplyr::select(dplyr::all_of(brek_columns))

    avg_length_of_stay <- brek_tulsa_avg_length_of_stay_2024 |>
      dplyr::mutate(
        year = brek_year,
        metric_family = "avg_length_of_stay",
        metric = "Average Length of Stay",
        dimension = metric_type,
        group = NA_character_,
        county = "Tulsa County",
        value = avg_days
      ) |>
      dplyr::select(dplyr::all_of(brek_columns))

    rebooking_rates <- brek_tulsa_rebooking_rates_2024 |>
      dplyr::mutate(
        year = brek_year,
        metric_family = "rebooking_rate",
        metric = "Rebooking Rate (<1 year)",
        dimension = metric_type,
        group = NA_character_,
        county = "Tulsa County",
        value = rebooking_rate
      ) |>
      dplyr::select(dplyr::all_of(brek_columns))

    brek_reference <- dplyr::bind_rows(
      adp_summary,
      bookings_summary,
      release_disposition_counts,
      release_disposition_shares,
      avg_length_of_stay,
      rebooking_rates
    )

    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    readr::write_csv(brek_reference, path)
  }

  brek_jail_data <- readr::read_csv(path, show_col_types = FALSE)

  return(brek_jail_data)
}

# Function to read Jail Data Initiative scraped data from a Google Drive folder
ingest_jail_data_initiative_scraped_data <- function(
  drive_folder_url = "https://drive.google.com/drive/folders/1fsv2pAkRd6DoDgG77SoXA3-0wK5tUnmh",
  charges_filename = "charges.csv",
  people_filename = "people.csv",
  download_dir = tempdir()
) {
  # Helper function to handle Google Drive errors, particularly authentication issues
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
    on.exit(
      {
        if (file.exists(local_path)) {
          unlink(local_path)
        }
      },
      add = TRUE
    )

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

# Function to ingest Vera Institute incarceration trends data
# Source: https://github.com/vera-institute/incarceration-trends
ingest_vera_incerceration_trends_data <- function() {
  commit_info <- httr2::request("https://api.github.com/repos/vera-institute/incarceration-trends/commits") |>
    httr2::req_url_query(path = "incarceration_trends_county.csv", sha = "main") |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  readr::read_csv(
    "https://raw.githubusercontent.com/vera-institute/incarceration-trends/main/incarceration_trends_county.csv",
    show_col_types = FALSE
  ) |>
    filter(state_code == "US_OK") |>
    dplyr::mutate(
      updated_at      = commit_info[[1]]$commit$committer$date |> lubridate::ymd_hms(),
      updated_at_date = as.Date(updated_at),
      updated_commit  = commit_info[[1]]$sha
    )
}

# Target function to ingest all raw jail data sources
ingest_jail_data <- function() {
  total_sources <- 5

  jail_data_initiative <- ingest_jail_data_initiative_scraped_data()
  message(sprintf("(1/%d) jail_data_initiative ingestion complete", total_sources))

  okpolicy <- ingest_okpolicy_scraped_data()
  message(sprintf("(2/%d) okpolicy ingestion complete", total_sources))

  asemio <- ingest_asemio_scraped_data()
  message(sprintf("(3/%d) asemio ingestion complete", total_sources))

  brek <- ingest_brek_jail_report_data()
  message(sprintf("(4/%d) brek ingestion complete", total_sources))

  vera <- ingest_vera_incerceration_trends_data()
  message(sprintf("(5/%d) vera ingestion complete", total_sources))

  list(
    jail_data_initiative = jail_data_initiative,
    okpolicy = okpolicy,
    asemio = asemio,
    brek = brek,
    vera = vera
  )
}

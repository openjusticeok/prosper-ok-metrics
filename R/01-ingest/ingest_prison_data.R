ingest_prison_data <- function(
  drive_folder_url = "https://drive.google.com/drive/u/0/folders/1-yeHWh-DxC6NYT6nOP63Gs7cZC5uuFmR",
  extract_dates = NULL,
  download_dir = tempdir()
) {
  # Large DOC vendor extracts; skip Drive downloads if cached CSVs already exist.
  # Cached files must be stamped with the extract date:
  # data/input/doc/YYYY-MM-DD_profile_data.csv, etc.

  # read fixed-width file with specified column widths and names
  read_fwf_file <- function(file_path, col_widths, col_names, date_cols = NULL) {
    col_types <- paste(rep("c", length(col_names)), collapse = "")

    data <- readr::read_fwf(
      file = file_path,
      col_positions = readr::fwf_widths(col_widths, col_names),
      col_types = col_types
    )

    if (!is.null(date_cols)) {
      data <- data |>
        dplyr::mutate(dplyr::across(dplyr::all_of(date_cols), ~ as.Date(.x, format = "%Y%m%d")))
    }

    data
  }

  # Discover DOC extract folders in the specified Drive folder
  discover_extract_folders <- function(parent_id) {
    drive_folders <- googledrive::drive_ls(parent_id, type = "folder")

    folder_path <- if ("path" %in% names(drive_folders)) {
      drive_folders$path
    } else {
      rep(drive_folder_url, nrow(drive_folders))
    }

    drive_folders |>
      dplyr::transmute(
        folder_name = name,
        folder_id = id,
        folder_path = folder_path,
        # Extract dates from folder names using regex pattern matching
        extract_date = lubridate::mdy(stringr::str_extract(name, "\\d{1,2}[./-]\\d{1,2}[./-]\\d{4}"))
      ) |>
      dplyr::filter(!is.na(.data$extract_date))
  }

  # Handle Google Drive authentication errors
  handle_drive_error <- function(err) {
    message <- conditionMessage(err)
    is_auth_issue <- grepl("unauthor|forbidden|auth", message, ignore.case = TRUE)

    if (is_auth_issue) {
      stop(
        paste(
          "Google Drive authentication error:",
          "Authenticate with googledrive::drive_auth() before ingesting.",
          sprintf("Error details: %s", message),
          sep = "\n"
        ),
        call. = FALSE
      )
    }

    stop(err)
  }

  parent_id <- googledrive::as_id(drive_folder_url) # Convert URL to Drive folder ID

  # Discover available extract folders
  extract_map <- tryCatch(
    discover_extract_folders(parent_id),
    error = handle_drive_error
  )

  # Ensure at least one extract folder was found
  if (nrow(extract_map) == 0) {
    stop("No dated vendor extract folders were found in the provided Drive folder.", call. = FALSE)
  }

  # Determine which extract dates to download
  # By default, use the most recent available extract date if none specified
  if (is.null(extract_dates)) {
    extract_dates <- max(extract_map$extract_date, na.rm = TRUE)
  }
  extract_dates <- lubridate::as_date(extract_dates)

  # Function to download and cache extracts for a specific extract date
  # Returns paths to cached CSV files
  download_extract <- function(target_date) {
    # Create input directory if it doesn't exist
    date_label <- format(target_date, "%Y_%m_%d")
    input_dir <- here::here("data", "input", "doc")
    fs::dir_create(input_dir)

    # Define cached file paths
    cached_paths <- list(
      profile = file.path(input_dir, sprintf("%s_profile_data.csv", date_label)),
      offense = file.path(input_dir, sprintf("%s_offense_data.csv", date_label)),
      sentence = file.path(input_dir, sprintf("%s_sentence_data.csv", date_label)),
      consecutive = file.path(input_dir, sprintf("%s_consecutive_data.csv", date_label))
    )

    # Return cached files if they already exist
    if (all(vapply(cached_paths, file.exists, logical(1)))) {
      return(cached_paths)
    }

    # Find the Drive folder for the target extract date
    folder_row <- extract_map |>
      dplyr::filter(.data$extract_date == target_date) |>
      dplyr::slice_head(n = 1)

    if (nrow(folder_row) == 0) {
      stop(sprintf("No Drive folder matched extract date %s.", date_label), call. = FALSE)
    }

    drive_files <- tryCatch(
      googledrive::drive_ls(folder_row$folder_id),
      error = handle_drive_error
    )

    # Function to download a specific file, read it, and cache as CSV
    # We have reader function as an argument to handle different file structures
    # We also add a snapshot_date column to each dataset
    download_file <- function(file_name, reader, cache_path, snapshot_date_value) {
      target_file <- drive_files |>
       dplyr::filter(stringr::str_detect(tolower(.data$name), tolower(file_name)))

      if (nrow(target_file) == 0) {
        stop(sprintf("File '%s' not found in Drive folder for %s.", file_name, date_label), call. = FALSE)
      }

      # Download to a temporary local path
      local_path <- file.path(download_dir, file_name)
      # Ensure temporary file is deleted after use
      on.exit(
        {
          if (file.exists(local_path)) unlink(local_path)
        },
        add = TRUE
      )

      # Download the file from Google Drive
      tryCatch(
        googledrive::drive_download(
          file = target_file,
          path = local_path,
          overwrite = TRUE
        ),
        error = handle_drive_error
      )

      # Use the user provided reader function to read the file
      data <- reader(local_path) |>
        dplyr::mutate(snapshot_date = snapshot_date_value)

      # Cache as CSV for future use to reduce Drive downloads
      readr::write_csv(data, cache_path)

      return(cache_path)
    }

    reader_jobs <- list(
      list(
        file_name = "Vendor_Profile_Extract_Text.dat",
        cache_path = cached_paths$profile,
        reader = function(path) {
          read_fwf_file(
            file_path = path,
            col_widths = c(10, 30, 30, 30, 4, 8, 50, 8, 1, 60, 60, 1, 2, 3, 60, 10),
            col_names = c(
              "doc_num", "last_name", "first_name", "middle_name", "suffix",
              "last_move_date", "facility", "birth_date", "sex", "race",
              "hair", "height_ft", "height_in", "weight", "eye", "status"
            ),
            date_cols = c("last_move_date", "birth_date")
          )
        }
      ),
      list(
        file_name = "Vendor_Sentence_Extract_Text.dat",
        cache_path = cached_paths$sentence,
        reader = function(path) {
          read_fwf_file(
            file_path = path,
            col_widths = c(10, 13, 30, 60, 8, 32, 20, 20),
            col_names = c(
              "doc_num", "sentence_id", "statute_code", "sentencing_county",
              "js_date", "crf_number", "incarcerated_term_in_years",
              "probation_term_in_years"
            ),
            date_cols = c("js_date")
          )
        }
      ),
      list(
        file_name = "Vendor_Offense_Extract_Text.dat",
        cache_path = cached_paths$offense,
        reader = function(path) {
          read_fwf_file(
            file_path = path,
            col_widths = c(30, 60, 1),
            col_names = c("statute_code", "description", "violent")
          )
        }
      ),
      list(
        file_name = "Vendor_Consecutive_Extract_Text.dat",
        cache_path = cached_paths$consecutive,
        reader = function(path) {
          read_fwf_file(
            file_path = path,
            col_widths = c(13, 13),
            col_names = c("sentence_id", "consecutive_to_id")
          )
        }
      )
    )

    lapply(reader_jobs, function(job) {
      download_file(
        file_name = job$file_name,
        reader = job$reader,
        cache_path = job$cache_path,
        snapshot_date_value = target_date
      )
    })

    cached_paths
  }

  # Download and cache extracts for all requested dates
  cached_paths_by_date <- rlang::set_names(
    lapply(extract_dates, download_extract),
    format(extract_dates, "%Y-%m-%d")
  )

  # Return data for the most recent requested date
  target_date <- max(extract_dates, na.rm = TRUE)
  cached_paths <- cached_paths_by_date[[format(target_date, "%Y-%m-%d")]]

  read_doc_csv <- function(path) {
    readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()))
  }

  list(
    available_extracts = extract_map,
    profile_data = read_doc_csv(cached_paths$profile),
    offense_data = read_doc_csv(cached_paths$offense),
    sentence_data = read_doc_csv(cached_paths$sentence),
    consecutive_data = read_doc_csv(cached_paths$consecutive)
  )
}

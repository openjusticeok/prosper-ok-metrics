#' Ingest DOC vendor extracts from Google Drive folders
#'
#' Each provided Drive folder URL should point directly to a specific vendor
#' extract (containing the fixed-width `.dat` files). The function downloads,
#' caches, and returns the latest profile, offense, sentence, and consecutive
#' datasets, adding a `snapshot_date` column to each. Caching happens under
#' `data/input/doc/`, grouped by folder label and extract date (if available).
#'
#' @param drive_folder_url Character vector of Google Drive folder URLs. Each
#'   folder is treated as a single extract.
#' @param extract_dates Optional vector or list of dates matching
#'   `drive_folder_url`. Used for cache labels and the `snapshot_date` column.
#'   If `NULL`, the date is inferred from the folder name when possible; if not
#'   found, an `undated_extract` label is used and `snapshot_date` is `NA`.
#' @param download_dir Directory for temporary downloads (defaults to
#'   `tempdir()`).
#'
#' @return A list containing:
#'   * `available_extracts`: Tibble of folder metadata and inferred dates.
#'   * `profile_data`, `offense_data`, `sentence_data`, `consecutive_data`:
#'     Tibbles of ingest results with character columns and `snapshot_date`.
#' @seealso [ingest_prison_data()]
ingest_doc_data <- function(
  drive_folder_url = c(
    # 2024-10-16 extract
    "https://drive.google.com/drive/folders/1rYwFUKikr8iJys3StLppcxjy9Zs8EdAa",
    # 2025-10-22 extract
    "https://drive.google.com/drive/folders/1mnaw1XjSAMTrzXWdatDS7m597sZk51xM"
  ),
  extract_dates = NULL,
  download_dir = tempdir()
) {
  drive_folder_urls <- as.character(drive_folder_url)

  if (length(drive_folder_urls) == 0) {
    stop("Provide at least one Drive folder URL for DOC data.", call. = FALSE)
  }

  sanitize_drive_label <- function(folder_url) {
    label <- gsub("[^A-Za-z0-9]+", "_", folder_url)
    label <- stringr::str_sub(label, 1L, 40L)
    if (identical(label, "")) "doc_drive" else label
  }

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

  extract_dates_list <- if (is.list(extract_dates)) {
    if (!rlang::is_empty(extract_dates) && length(extract_dates) != length(drive_folder_urls)) {
      stop("When supplying extract_dates as a list, its length must match drive_folder_url.", call. = FALSE)
    }
    extract_dates
  } else {
    rep(list(extract_dates), length(drive_folder_urls))
  }

  use_folder_specific_cache <- length(drive_folder_urls) > 1
  cache_labels <- vapply(drive_folder_urls, sanitize_drive_label, character(1))

  ingest_single_folder <- function(single_drive_folder_url, extract_date_single, cache_label) {
    folder_id <- googledrive::as_id(single_drive_folder_url)

    folder_meta <- tryCatch(
      googledrive::drive_get(folder_id),
      error = handle_drive_error
    )

    inferred_date <- lubridate::mdy(stringr::str_extract(folder_meta$name, "\\d{1,2}[./-]\\d{1,2}[./-]\\d{4}"))
    target_date <- if (!is.null(extract_date_single)) lubridate::as_date(extract_date_single) else inferred_date
    date_label <- if (is.na(target_date)) "undated_extract" else format(target_date, "%Y_%m_%d")

    drive_files <- tryCatch(
      googledrive::drive_ls(folder_id),
      error = handle_drive_error
    )

    input_dir <- if (use_folder_specific_cache) {
      here::here("data", "input", "doc", cache_label)
    } else {
      here::here("data", "input", "doc")
    }
    fs::dir_create(input_dir)

    cached_paths <- list(
      profile = file.path(input_dir, sprintf("%s_profile_data.csv", date_label)),
      offense = file.path(input_dir, sprintf("%s_offense_data.csv", date_label)),
      sentence = file.path(input_dir, sprintf("%s_sentence_data.csv", date_label)),
      consecutive = file.path(input_dir, sprintf("%s_consecutive_data.csv", date_label))
    )

    if (!all(vapply(cached_paths, file.exists, logical(1)))) {
      download_file <- function(file_name, reader, cache_path, snapshot_date_value) {
        target_file <- drive_files |>
          dplyr::filter(stringr::str_detect(tolower(.data$name), tolower(file_name)))

        if (nrow(target_file) == 0) {
          stop(sprintf("File '%s' not found in Drive folder %s.", file_name, folder_meta$name), call. = FALSE)
        }

        local_path <- file.path(download_dir, file_name)
        on.exit(
          {
            if (file.exists(local_path)) unlink(local_path)
          },
          add = TRUE
        )

        tryCatch(
          googledrive::drive_download(
            file = target_file,
            path = local_path,
            overwrite = TRUE
          ),
          error = handle_drive_error
        )

        data <- reader(local_path) |>
          dplyr::mutate(snapshot_date = snapshot_date_value)

        readr::write_csv(data, cache_path)

        cache_path
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
    }

    read_doc_csv <- function(path) {
      readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()))
    }

    list(
      available_extracts = tibble::tibble(
        folder_name = folder_meta$name,
        folder_id = folder_meta$id,
        folder_path = if ("path" %in% names(folder_meta)) folder_meta$path else single_drive_folder_url,
        source_drive_folder = single_drive_folder_url,
        extract_date = target_date
      ),
      profile_data = read_doc_csv(cached_paths$profile),
      offense_data = read_doc_csv(cached_paths$offense),
      sentence_data = read_doc_csv(cached_paths$sentence),
      consecutive_data = read_doc_csv(cached_paths$consecutive)
    )
  }

  results <- Map(
    f = ingest_single_folder,
    drive_folder_urls,
    extract_dates_list,
    cache_labels
  )

  combine_results <- function(component) {
    pieces <- lapply(results, function(res) res[[component]])
    if (length(pieces) == 1) {
      return(pieces[[1]])
    }

    dplyr::bind_rows(pieces)
  }

  list(
    available_extracts = combine_results("available_extracts"),
    profile_data = combine_results("profile_data"),
    offense_data = combine_results("offense_data"),
    sentence_data = combine_results("sentence_data"),
    consecutive_data = combine_results("consecutive_data")
  )
}

#' Wrapper to ingest all prison data sources
#'
#' Currently delegates to [ingest_doc_data()] to pull DOC vendor extracts.
#'
#' @inheritParams ingest_doc_data
#' @return List of DOC ingest outputs identical to [ingest_doc_data()].
ingest_prison_data <- function() {
  list(
    doc = ingest_doc_data()
  )
}

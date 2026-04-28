#' Upload Tulsa County Jail metrics to Google Drive
#'
#' Uploads all exported CSV files from data/output/tulsa-county-jail/ to the
#' tulsa-county-jail folder in Google Drive, organized into subfolders.
#' Creates the folder and subfolders if they don't exist.
#'
#' @param exported_files List of file paths from export_jail_metrics()
#' @param drive_parent_folder_url Google Drive URL where tulsa-county-jail folder should be created
#' @return Invisibly returns NULL, called for side effects (file upload)
upload_jail_metrics_to_drive <- function(
  exported_files = jail_exported_metrics,
  drive_parent_folder_url = "https://drive.google.com/drive/u/0/folders/1Q3GfR--AanGwFGa_sjrRKXbdAZeBj_BD"
) {
  # Error handler (same pattern as ingest_jail_data_initiative_scraped_data)
  handle_drive_error <- function(err) {
    message <- conditionMessage(err)
    is_auth_issue <- grepl("unauthor|forbidden|auth", message, ignore.case = TRUE)

    if (is_auth_issue) {
      stop(
        paste(
          "Google Drive authentication error:",
          "Please ensure you have authenticated with Google Drive using the googledrive package.",
          "Run googledrive::drive_auth() with valid credentials before uploading.",
          "See https://googledrive.tidyverse.org/articles/googledrive.html#authentication for more information.",
          sprintf("Error details: %s", message),
          sep = "\n"
        ),
        call. = FALSE
      )
    }
    stop(err)
  }

  # Convert URL to folder ID
  parent_folder_id <- googledrive::as_id(drive_parent_folder_url)

  # Create tulsa-county-jail folder (or get existing)
  tcj_folder_id <- tryCatch(
    googledrive::drive_mkdir(
      name = "tulsa-county-jail",
      path = parent_folder_id,
      overwrite = TRUE
    ) |>
      dplyr::pull(id),
    error = handle_drive_error
  )

  # Create subfolders
  subfolders <- list(
    population = "population",
    bookings = "bookings",
    releases = "releases",
    dispositions = "dispositions"
  )

  subfolder_ids <- purrr::map(
    subfolders,
    \(folder_name) {
      tryCatch(
        googledrive::drive_mkdir(
          name = folder_name,
          path = tcj_folder_id,
          overwrite = TRUE
        ) |>
          dplyr::pull(id),
        error = handle_drive_error
      )
    }
  )

  # Map files to subfolders based on filename
  get_subfolder_for_file <- function(filename) {
    if (stringr::str_detect(filename, "^adp_")) {
      return(subfolder_ids$population)
    } else if (stringr::str_detect(filename, "^bookings_")) {
      return(subfolder_ids$bookings)
    } else if (filename == "releases_by_year.csv") {
      return(subfolder_ids$releases)
    } else if (stringr::str_detect(filename, "disposition")) {
      return(subfolder_ids$dispositions)
    } else {
      return(tcj_folder_id)  # Default to root if no match
    }
  }

  # Upload all exported files to appropriate subfolders
  purrr::walk(
    exported_files,
    \(file_path) {
      filename <- fs::path_file(file_path)
      target_folder_id <- get_subfolder_for_file(filename)

      tryCatch(
        googledrive::drive_upload(
          file_path,
          path = target_folder_id,
          overwrite = TRUE
        ),
        error = handle_drive_error
      )
    }
  )

  invisible(NULL)
}

# Shared helper functions for the jail and prison target pipelines.


# GCS Utilities ===============================================================

#' Convert a data frame to in-memory Parquet bytes
#'
#' @param x A data frame or tibble
#' @param ... Additional arguments passed to [arrow::write_parquet()]
#' @return Raw Parquet bytes
#' @export
parquet_raw <- function(x, ...) {
  sink <- arrow::BufferOutputStream$create()

  arrow::write_parquet(x, sink, ...)

  sink$finish()$data()
}

#' Upload raw bytes to a GCS bucket
#'
#' @param raw Raw bytes to upload
#' @param bucket GCS bucket name
#' @param name Object name in the bucket
#' @param token A gargle OAuth token
#' @param content_type MIME type of the upload
#' @return JSON response from the GCS API
#' @export
gcs_upload_raw <- function(raw, bucket, name, token,
                           content_type = "application/vnd.apache.parquet") {
  token$refresh()

  resp <- httr2::request(
    glue::glue("https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o")
  ) |>
    httr2::req_url_query(
      uploadType = "media",
      name = name
    ) |>
    httr2::req_headers(
      Authorization = paste("Bearer", token$credentials$access_token),
      `Content-Type` = content_type
    ) |>
    httr2::req_body_raw(raw) |>
    httr2::req_perform()

  httr2::resp_body_json(resp)
}

#' Read a Parquet file from GCS into a data frame
#'
#' @param object Object name in the GCS bucket
#' @param bucket GCS bucket name (defaults to the global bucket)
#' @param clean_names If TRUE, run [janitor::clean_names()] on the result
#' @return A data frame
#' @export
gcs_read_parquet <- function(
  object,
  bucket = googleCloudStorageR::gcs_get_global_bucket(),
  clean_names = TRUE
) {
  data <- googleCloudStorageR::gcs_get_object(
    object_name = object,
    bucket = bucket
  ) |>
    arrow::read_parquet()

  if (clean_names) data <- janitor::clean_names(data)

  data
}


# Report Rendering ============================================================

## Report Paths ---------------------------------------------------------------

.pipeline_report_paths <- list(
  jail = here::here("inst", "reports", "2025-11-prosper-metrics-jail", "2025-11-prosper-metrics-jail.qmd"),
  prison = here::here("inst", "reports", "2025-11-prosper-metrics-prison", "2025-11-prosper-metrics-prison.qmd")
)

pipeline_report_path <- function(report) {
  if (!report %in% names(.pipeline_report_paths)) {
    stop(sprintf("Unknown report key: %s", report), call. = FALSE)
  }
  .pipeline_report_paths[[report]]
}

## Render Reports -------------------------------------------------------------

render_report <- function(
  report,
  analysis_results,
  figure_outputs,
  execute = TRUE,
  draft = TRUE,
  input = pipeline_report_path(report),
  audiences = "internal"
) {
  audiences <- unique(match.arg(audiences, c("internal", "external", "public"), several.ok = TRUE))

  data_dir <- here::here("data", "output")
  fs::dir_create(data_dir, recurse = TRUE)

  analysis_path <- file.path(data_dir, sprintf("%s_analysis_results.rds", report))
  figures_path <- file.path(data_dir, sprintf("%s_figures.rds", report))

  saveRDS(analysis_results, analysis_path)
  saveRDS(figure_outputs, figures_path)

  env_vars <- stats::setNames(
    c(analysis_path, figures_path),
    c(
      sprintf("PROSPER_%s_ANALYSIS_RDS", toupper(report)),
      sprintf("PROSPER_%s_FIGURES_RDS", toupper(report))
    )
  )

  file_stem <- sub("\\.qmd$", "", basename(input))
  output_dir <- dirname(input)
  # Default to internal-only rendering for faster local iteration; request
  # "external" explicitly when needed.
  variant_specs <- lapply(
    audiences,
    function(audience) {
      output_file <- switch(
        audience,
        internal = paste0(file_stem, "-internal.html"),
        external = paste0(file_stem, "-external.html"),
        public = paste0(file_stem, "-public.html")
      )

      list(audience = audience, output_file = output_file)
    }
  )

  outputs <- withr::with_envvar(
    env_vars,
    vapply(variant_specs, function(spec) {
      quarto::quarto_render(
        input = input,
        execute = execute,
        execute_params = list(
          audience = spec$audience,
          draft = draft
        ),
        output_file = spec$output_file,
        quiet = FALSE # Always set to FALSE for easier debugging
      )

      output_path <- file.path(output_dir, spec$output_file)

      if (!fs::file_exists(output_path)) {
        stop(sprintf(
          "Quarto did not produce an HTML output for '%s' (audience = %s). Expected file at '%s'.",
          report,
          spec$audience,
          output_path
        ), call. = FALSE)
      }

      normalizePath(output_path, winslash = "/", mustWork = FALSE)
    }, character(1))
  )

  unname(outputs)
}


# General Utilities ===========================================================

#' Build a named list from bare object names
#'
#' Creates a list where each element is named after the variable passed to it.
#' This avoids repetition when returning lists of objects where the list name
#' should match all of the object names.
#'
#' This is particularly useful in target pipelines where functions return a
#' lists of objects to be used in downstream targets, and you want the list
#' names to match the object names without having to type them out twice.
#'
#' Without this, renaming or refactoring variables would require updating both the
#' variable names and the list construction, increasing the chance of errors.
#'
#' @param ... Bare object names (not strings or expressions).
#' @return A named list with names derived from the argument symbols.
#'
#' @examples
#' foo <- 1
#' bar <- 2
#' named_list(foo, bar)
#' # list(foo = 1, bar = 2)
#'
#' @export
named_list <- function(...) {
  values <- list(...)
  names(values) <- as.character(substitute(list(...))[-1])

  if (any(names(values) == "")) {
    stop("All arguments must be bare object names.", call. = FALSE)
  }

  values
}


# Placeholder Helpers =========================================================
# Shared placeholder helpers for targets scaffolding.

## Internal Placeholder Data --------------------------------------------------

.placeholder_time_series <- function() {
  tibble::tibble(
    time = seq(as.Date("2025-01-01"), by = "1 month", length.out = 7),
    good_stuff = c(10, 14, 12, 18, 16, 22, 27)
  )
}

.placeholder_ok_counties <- function() {
  counties <- withr::with_options(
    list(tigris_use_cache = TRUE, tigris_class = "sf"),
    tigris::counties(state = "OK", cb = TRUE, year = 2022)
  )

  counties <- dplyr::arrange(counties, .data$GEOID)

  withr::with_seed(7, {
    counties$good <- stats::runif(nrow(counties))
  })

  sf::st_transform(counties, 4326)
}

## Placeholder Functions ------------------------------------------------------

placeholder_ggplot <- function() {
  data <- .placeholder_time_series()

  ggplot2::ggplot(data, ggplot2::aes(x = time, y = good_stuff)) +
    ggplot2::geom_line(color = "#972421") +
    ggplot2::geom_point(color = "#972421", size = 2) +
    ggplot2::labs(
      title = "Placeholder",
      subtitle = "A very scientific graph.",
      x = "Time",
      y = "Good Stuff"
    ) +
    ojothemes::theme_okpi()
}

placeholder_list <- function() {
  list()
}

placeholder_tibble <- function() {
  tibble::tibble()
}

placeholder_gt <- function() {
  data <- .placeholder_time_series()
  gt::gt(data)
}

placeholder_highchart <- function() {
  data <- .placeholder_time_series()

  highcharter::highchart() |>
    highcharter::hc_add_series(
      data = data,
      type = "line",
      highcharter::hcaes(x = time, y = good_stuff),
      name = "Good Stuff"
    ) |>
    highcharter::hc_title(text = "Placeholder") |>
    highcharter::hc_subtitle(text = "A very scientific graph.") |>
    highcharter::hc_xAxis(title = list(text = "Time")) |>
    highcharter::hc_yAxis(title = list(text = "Good Stuff"))
}

placeholder_highchart_map <- function() {
  counties <- .placeholder_ok_counties()
  county_values <- sf::st_drop_geometry(counties)

  geojson_path <- tempfile(fileext = ".geojson")
  on.exit(unlink(geojson_path), add = TRUE)

  sf::st_write(
    counties[, c("GEOID", "NAME", "good")],
    geojson_path,
    driver = "GeoJSON",
    quiet = TRUE
  )

  geojson <- jsonlite::fromJSON(geojson_path, simplifyVector = FALSE)

  highcharter::highchart(type = "map") |>
    highcharter::hc_add_series_map(
      map = geojson,
      df = county_values,
      value = "good",
      joinBy = c("GEOID", "GEOID"),
      name = "Good"
    ) |>
    highcharter::hc_title(text = "Placeholder") |>
    highcharter::hc_subtitle(text = "A very scientific map.")
}

placeholder_number <- function() {
  999999999
}

placeholder_string <- function() {
  "[PLACEHOLDER]"
}


# Google Drive Upload Utilities ================================================

#' Upload a data frame as CSV to Google Drive entirely in memory
#'
#' Formats the data frame as CSV, converts to raw bytes, and uploads via the
#' Drive API without touching disk. Supports both multipart and resumable
#' upload methods.
#'
#' @param data A data frame to upload
#' @param folder_id Google Drive folder ID (string)
#' @param name Filename for the uploaded CSV (`.csv` extension added automatically)
#' @param overwrite If `TRUE`, finds and overwrites an existing file with the
#'   same name in the folder
#' @param upload_type One of `"auto"`, `"multipart"`, or `"resumable"`.
#'   `"auto"` chooses based on `resumable_threshold`.
#' @param resumable_threshold Byte threshold for switching to resumable upload
#'   when `upload_type = "auto"` (default: 5 MB)
#' @param scopes OAuth scope for authentication
#' @return JSON response from the Drive API
#' @export
drive_upload_csv_raw <- function(data,
                                 folder_id,
                                 name,
                                 overwrite = TRUE,
                                 upload_type = c("auto", "multipart", "resumable"),
                                 resumable_threshold = 5 * 1024^2,
                                 bearer = NULL,
                                 scopes = "https://www.googleapis.com/auth/drive") {
  upload_type <- rlang::arg_match(upload_type)

  if (!is.data.frame(data)) {
    rlang::abort("`data` must be a data frame.")
  }

  if (!rlang::is_string(folder_id) || !nzchar(folder_id)) {
    rlang::abort("`folder_id` must be a non-empty string.")
  }

  if (!rlang::is_string(name) || !nzchar(name)) {
    rlang::abort("`name` must be a non-empty string.")
  }

  if (!is.logical(overwrite) || length(overwrite) != 1 || is.na(overwrite)) {
    rlang::abort("`overwrite` must be `TRUE` or `FALSE`.")
  }

  if (!is.numeric(resumable_threshold) || length(resumable_threshold) != 1 || resumable_threshold < 0) {
    rlang::abort("`resumable_threshold` must be a non-negative number of bytes.")
  }

  file_name <- ensure_csv_extension(name)

  csv_raw <- data |>
    readr::format_csv() |>
    charToRaw()

  if (upload_type == "auto") {
    upload_type <- if (length(csv_raw) >= resumable_threshold) {
      "resumable"
    } else {
      "multipart"
    }
  }

  if (is.null(bearer)) {
    bearer <- drive_bearer_token(scopes)
  }

  existing_id <- NULL

  if (overwrite) {
    existing_id <- drive_find_existing_file_id(
      bearer = bearer,
      folder_id = folder_id,
      file_name = file_name
    )
  }

  metadata <- list(
    name = file_name,
    mimeType = "text/csv"
  )

  if (is.null(existing_id)) {
    metadata$parents <- list(folder_id)
  }

  switch(
    upload_type,
    multipart = drive_upload_multipart(
      bearer = bearer,
      csv_raw = csv_raw,
      metadata = metadata,
      existing_id = existing_id
    ),
    resumable = drive_upload_resumable(
      bearer = bearer,
      csv_raw = csv_raw,
      metadata = metadata,
      existing_id = existing_id
    )
  )
}

ensure_csv_extension <- function(name) {
  if (grepl("\\.csv$", name, ignore.case = TRUE)) {
    name
  } else {
    paste0(name, ".csv")
  }
}

drive_bearer_token <- function(scopes) {
  token <- gargle::token_fetch(scopes = scopes)

  access_token <- token$credentials$access_token

  if (is.null(access_token) || !nzchar(access_token)) {
    rlang::abort("Could not fetch a Google Drive access token.")
  }

  access_token
}

drive_escape_query_string <- function(x) {
  x |>
    gsub("\\\\", "\\\\\\\\", x = _) |>
    gsub("'", "\\\\'", x = _)
}

drive_find_existing_file_id <- function(bearer, folder_id, file_name) {
  q <- sprintf(
    "name = '%s' and '%s' in parents and trashed = false",
    drive_escape_query_string(file_name),
    drive_escape_query_string(folder_id)
  )

  resp <- httr2::request("https://www.googleapis.com/drive/v3/files") |>
    httr2::req_url_query(
      q = q,
      fields = "files(id,name,modifiedTime)",
      spaces = "drive",
      supportsAllDrives = "true",
      includeItemsFromAllDrives = "true",
      orderBy = "modifiedTime desc"
    ) |>
    httr2::req_auth_bearer_token(bearer) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  if (length(resp$files) == 0) {
    return(NULL)
  }

  resp$files[[1]]$id
}

drive_upload_endpoint <- function(existing_id = NULL) {
  if (is.null(existing_id)) {
    "https://www.googleapis.com/upload/drive/v3/files"
  } else {
    paste0("https://www.googleapis.com/upload/drive/v3/files/", existing_id)
  }
}

drive_upload_method <- function(existing_id = NULL) {
  if (is.null(existing_id)) "POST" else "PATCH"
}

drive_response_fields <- function() {
  "id,name,mimeType,webViewLink"
}

drive_upload_multipart <- function(bearer, csv_raw, metadata, existing_id = NULL) {
  boundary <- paste0(
    "-------",
    paste(sample(c(letters, LETTERS, 0:9), 32, replace = TRUE), collapse = "")
  )

  body <- c(
    charToRaw(paste0(
      "--", boundary, "\r\n",
      "Content-Type: application/json; charset=UTF-8\r\n\r\n",
      jsonlite::toJSON(metadata, auto_unbox = TRUE), "\r\n",
      "--", boundary, "\r\n",
      "Content-Type: text/csv; charset=UTF-8\r\n\r\n"
    )),
    csv_raw,
    charToRaw(paste0("\r\n--", boundary, "--\r\n"))
  )

  httr2::request(drive_upload_endpoint(existing_id)) |>
    httr2::req_method(drive_upload_method(existing_id)) |>
    httr2::req_url_query(
      uploadType = "multipart",
      supportsAllDrives = "true",
      fields = drive_response_fields()
    ) |>
    httr2::req_auth_bearer_token(bearer) |>
    httr2::req_headers(
      `Content-Type` = paste0("multipart/related; boundary=", boundary)
    ) |>
    httr2::req_body_raw(body) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform() |>
    httr2::resp_body_json()
}

drive_upload_resumable <- function(bearer, csv_raw, metadata, existing_id = NULL) {
  init_resp <- httr2::request(drive_upload_endpoint(existing_id)) |>
    httr2::req_method(drive_upload_method(existing_id)) |>
    httr2::req_url_query(
      uploadType = "resumable",
      supportsAllDrives = "true",
      fields = drive_response_fields()
    ) |>
    httr2::req_auth_bearer_token(bearer) |>
    httr2::req_headers(
      `Content-Type` = "application/json; charset=UTF-8",
      `X-Upload-Content-Type` = "text/csv; charset=UTF-8",
      `X-Upload-Content-Length` = as.character(length(csv_raw))
    ) |>
    httr2::req_body_json(metadata, auto_unbox = TRUE) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform()

  upload_url <- httr2::resp_header(init_resp, "location")

  if (is.null(upload_url) || !nzchar(upload_url)) {
    rlang::abort("Drive did not return a resumable upload URL.")
  }

  httr2::request(upload_url) |>
    httr2::req_method("PUT") |>
    httr2::req_headers(
      `Content-Type` = "text/csv; charset=UTF-8",
      `Content-Length` = as.character(length(csv_raw))
    ) |>
    httr2::req_body_raw(csv_raw) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform() |>
    httr2::resp_body_json()
}

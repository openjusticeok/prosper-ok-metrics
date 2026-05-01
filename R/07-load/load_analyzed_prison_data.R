load_gkff_prison_analysis_results <- function(
  results = gkff_prison_analysis_results,
  snapshot_date = "2026-03-13",
  bucket = "odoc-adhoc-data-requests",
  drive_folder_url = "https://drive.google.com/drive/u/0/folders/1UskeGYzE5r8K2v5SBE13MxkptpZGuAZB"
) {
  date_slug <- lubridate::as_date(snapshot_date) |> format("%Y-%m-%d")
  gcs_prefix <- paste0(date_slug, "-gkff/derived/")

  token <- gargle::token_fetch(scopes = "https://www.googleapis.com/auth/cloud-platform")
  googleCloudStorageR::gcs_global_bucket(bucket = bucket)
  googleCloudStorageR::gcs_auth(token = token)

  drive_folder_id <- googledrive::as_id(drive_folder_url)

  googledrive::drive_auth()

  names(results) |>
    purrr::walk(function(name) {
      tibble <- results[[name]]

      parquet_raw(tibble) |>
        gcs_upload_raw(
          name = paste0(gcs_prefix, name, ".parquet"),
          bucket = bucket,
          token = token
        )

      temp_csv <- tempfile(fileext = ".csv")
      on.exit(unlink(temp_csv), add = TRUE)

      readr::write_csv(tibble, temp_csv)

      googledrive::drive_upload(
        media = temp_csv,
        path = drive_folder_id,
        name = paste0(name, ".csv"),
        overwrite = TRUE
      )
    })

  results
}

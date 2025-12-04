#' Ingest Vera Institute incarceration trends data (OK counties only)
#'
#' Fetches the latest incarceration trends CSV from GitHub, stamps it with the
#' current commit hash and commit date, and caches the result under
#' `data/input/` using the commit SHA in the filename. Subsequent runs reuse the
#' cached `.qs2` file when present.
#'
#' Data source: <https://github.com/vera-institute/incarceration-trends>  
#' Codebook: <https://github.com/vera-institute/incarceration-trends/blob/main/Incarceration%20Trends%20Codebook%2005-2025.pdf>  
#' License: <https://github.com/vera-institute/incarceration-trends/blob/main/License.pdf>
#'
#' @return A tibble filtered to Oklahoma counties with commit metadata columns.
ingest_vera_data <- function() {
  # Get current commit info from GitHub API
  commit_info <- httr2::request("https://api.github.com/repos/vera-institute/incarceration-trends/commits") |>
    httr2::req_url_query(path = "incarceration_trends_county.csv", sha = "main") |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  current_commit <- commit_info[[1]]$sha
  commit_date <- commit_info[[1]]$commit$committer$date |> lubridate::ymd_hms()
  commit_date_formatted <- commit_date |> format("%Y_%m_%d")

  # Define cache path with commit stamp
  cache_path <- here::here("data", "input", paste0(commit_date_formatted, "_vera_incerceration_trends_", current_commit, ".qs2"))

  # Return cached data if it exists
  if (file.exists(cache_path)) {
    message(sprintf("Cached Vera Institute incarceration trends data found at %s.\nReading it from disk! 💽", cache_path))
    cached_data <- qs2::qs_read(cache_path)

    return(cached_data)
  }

  # Download and process new data since cached data not found
  message(sprintf("Cached Vera Institute incarceration trends data not found at %s.\nDownloading from GitHub! 📥", cache_path))

  vera_data <- readr::read_csv(
    "https://raw.githubusercontent.com/vera-institute/incarceration-trends/main/incarceration_trends_county.csv",
    show_col_types = FALSE
  ) |>
    dplyr::filter(state_code == "US_OK") |>
    dplyr::mutate(
      updated_at      = commit_date,
      updated_at_date = as.Date(commit_date),
      updated_commit  = current_commit
    )

  # Save to cache
  qs2::qs_save(vera_data, cache_path)

  return(vera_data)
}
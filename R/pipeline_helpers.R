# Shared helper functions for the jail and prison target pipelines.


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

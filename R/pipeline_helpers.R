# Shared helper functions for the jail and prison target pipelines.

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

render_report <- function(
  report,
  analysis_results,
  figure_outputs,
  execute = TRUE,
  input = pipeline_report_path(report),
  audiences = "internal"
) {
  audiences <- unique(match.arg(audiences, c("internal", "external"), several.ok = TRUE))

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
      output_file <- if (identical(audience, "internal")) {
        paste0(file_stem, ".html")
      } else {
        paste0(file_stem, "-external.html")
      }

      list(audience = audience, output_file = output_file)
    }
  )

  outputs <- withr::with_envvar(
    env_vars,
    vapply(variant_specs, function(spec) {
      quarto::quarto_render(
        input = input,
        execute = execute,
        execute_params = list(audience = spec$audience),
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

# Shared placeholder helpers for targets scaffolding.
.placeholder_time_series <- function() {
  tibble::tibble(
    Time = seq(as.Date("2025-01-01"), by = "1 month", length.out = 7),
    `Good Stuff` = c(10, 14, 12, 18, 16, 22, 27)
  )
}

placeholder_ggplot <- function() {
  data <- .placeholder_time_series()

  ggplot2::ggplot(data, ggplot2::aes(x = Time, y = `Good Stuff`)) +
    ggplot2::geom_line(color = "#972421", linewidth = 1) +
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
      highcharter::hcaes(x = Time, y = `Good Stuff`),
      name = "Good Stuff"
    ) |>
    highcharter::hc_title(text = "Placeholder") |>
    highcharter::hc_subtitle(text = "A very scientific graph.") |>
    highcharter::hc_xAxis(title = list(text = "Time")) |>
    highcharter::hc_yAxis(title = list(text = "Good Stuff"))
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

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

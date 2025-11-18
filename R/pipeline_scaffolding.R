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

render_report <- function(report, analysis_results, figure_outputs, execute = FALSE) {
  input <- pipeline_report_path(report)
  analysis_results
  figure_outputs

  quarto::quarto_render(
    input = input,
    execute = execute,
    quiet = TRUE
  )

  output_file <- sub("\\.qmd$", ".html", input)
  normalizePath(output_file, winslash = "/", mustWork = FALSE)
}

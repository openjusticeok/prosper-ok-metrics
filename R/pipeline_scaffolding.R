# Shared helper functions for the jail and prison target pipelines.

.pipeline_report_paths <- list(
  jail = "reports/2025-11-prosper-metrics-jail/2025-11-prosper-metrics-jail.qmd",
  prison = "reports/2025-11-prosper-metrics-prison/2025-11-prosper-metrics-prison.qmd"
)

pipeline_report_path <- function(report) {
  if (!report %in% names(.pipeline_report_paths)) {
    stop(sprintf("Unknown report key: %s", report), call. = FALSE)
  }
  .pipeline_report_paths[[report]]
}

check_input_data <- function(report) {
  pipeline_report_path(report)
  tibble::tibble(
    report = report,
    check = character(),
    status = "pending"
  )
}

process_report_data <- function(report, input_checks) {
  pipeline_report_path(report)
  input_checks
  tibble::tibble(
    report = report,
    todo = character()
  )
}

check_output_data <- function(report, processed_data) {
  pipeline_report_path(report)
  processed_data
  tibble::tibble(
    report = report,
    check = character(),
    status = "pending"
  )
}

analyze_output_data <- function(report, processed_data) {
  pipeline_report_path(report)
  processed_data
  tibble::tibble(
    report = report,
    metric = character(),
    value = numeric()
  )
}

produce_output_figures <- function(report, analysis_results) {
  pipeline_report_path(report)
  analysis_results
  tibble::tibble(
    report = report,
    figure = character(),
    path = character()
  )
}

render_report <- function(report, analysis_results, figure_outputs, execute = FALSE) {
  pipeline_report_path(report)
  analysis_results
  figure_outputs

  quarto::quarto_render(
    input = pipeline_report_path(report),
    execute = execute,
    quiet = TRUE
  )
}

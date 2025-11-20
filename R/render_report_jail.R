render_jail_report <- function(analysis_results, figure_outputs, report_source = pipeline_report_path("jail"), execute = TRUE) {
  render_report(
    "jail",
    analysis_results,
    figure_outputs,
    execute = execute,
    input = report_source
  )
}

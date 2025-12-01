render_prison_report <- function(
  analysis_results,
  figure_outputs,
  report_source = pipeline_report_path("prison"),
  execute = TRUE
) {
  render_report(
    "prison",
    analysis_results,
    figure_outputs,
    execute = execute,
    input = report_source
  )
}

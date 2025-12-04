render_jail_report <- function(
  analysis_results,
  figure_outputs,
  report_source = pipeline_report_path("jail"),
  execute = TRUE,
  draft = TRUE,
  audiences = "internal"
) {
  render_report(
    "jail",
    analysis_results,
    figure_outputs,
    execute = execute,
    draft = draft,
    input = report_source,
    audiences = audiences
  )
}

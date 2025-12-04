render_prison_report <- function(
  analysis_results,
  figure_outputs,
  report_source = pipeline_report_path("prison"),
  execute = TRUE,
  draft = TRUE,
  audiences = "internal"
) {
  render_report(
    "prison",
    analysis_results,
    figure_outputs,
    execute = execute,
    draft = draft,
    input = report_source,
    audiences = audiences
  )
}

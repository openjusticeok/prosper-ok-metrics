# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c(
    "tibble",
    "dplyr",
    "tidyr",
    "stringr",
    "lubridate",
    "readr",
    "pointblank",
    "ggplot2",
    "scales",
    "ojodb",
    "ojothemes",
    "tulsaCountyJailScraper",
    "quarto",
    "here",
    "withr",
    "fs",
    "qs",
    "qs2",
    "glue",
    "rlang"
  ),
  format = "qs"
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source("R/pipeline_helpers.R")
tar_source("R/01-ingest/ingest_jail_data.R")
tar_source("R/01-ingest/ingest_prison_data.R")
tar_source("R/02-check-ingested/check_jail_ingested_data.R")
tar_source("R/02-check-ingested/check_prison_ingested_data.R")
tar_source("R/03-process/process_ingested_jail_data.R")
tar_source("R/03-process/process_ingested_prison_data.R")
tar_source("R/04-check-processed/check_processed_jail_data.R")
tar_source("R/04-check-processed/check_processed_prison_data.R")
tar_source("R/05-analyze/analyze_processed_jail_data.R")
tar_source("R/05-analyze/analyze_processed_prison_data.R")
tar_source("R/06-visualize/visualize_jail_figures.R")
tar_source("R/06-visualize/visualize_prison_figures.R")
tar_source("R/render_report_jail.R")
tar_source("R/render_report_prison.R")

# Targets pipeline definition:
list(
  # Jail pipeline
  tar_target(
    name = jail_ingested_data,
    command = ingest_jail_data()
  ),
  # TODO: feat(check): Implement real input checks for jail data
  # Can wait until after first draft of report.
  tar_target(
    name = jail_ingested_checks,
    command = check_jail_ingested(jail_ingested_data)
  ),
  tar_target(
    name = jail_processed_data,
    command = process_ingested_jail_data(jail_ingested_checks)
  ),
  # TODO: feat(check): Implement real output checks for jail data
  # Can wait until after today.
  tar_target(
    name = jail_processed_checks,
    command = check_jail_processed(jail_processed_data)
  ),
  tar_target(
    name = jail_analysis_results,
    command = analyze_processed_jail_data(jail_processed_data)
  ),
  tar_target(
    name = jail_figures,
    command = visualize_jail_figures(jail_analysis_results)
  ),
  tar_target(
    name = jail_report_source,
    command = pipeline_report_path("jail"),
    format = "file"
  ),
  tar_target(
    name = jail_report,
    command = render_jail_report(jail_analysis_results, jail_figures, report_source = jail_report_source),
    format = "file"
  ),
  # Prison pipeline
  tar_target(
    name = prison_ingested_data,
    command = ingest_prison_data()
  ),
  # Can wait until after today.
  tar_target(
    name = prison_ingested_checks,
    command = check_prison_ingested(prison_ingested_data)
  ),
  tar_target(
    name = prison_processed_data,
    command = process_ingested_prison_data(prison_ingested_data)
  ),
  # TODO: feat(check): Implement real output checks for prison data
  # Can wait until after report.
  tar_target(
    name = prison_processed_checks,
    command = check_prison_processed(prison_processed_data)
  ),
  tar_target(
    name = prison_analysis_results,
    command = analyze_processed_prison_data(prison_processed_data)
  ),
  tar_target(
    name = prison_figures,
    command = visualize_prison_figures(prison_analysis_results)
  ),
  tar_target(
    name = prison_report_source,
    command = pipeline_report_path("prison"),
    format = "file"
  ),
  tar_target(
    name = prison_report,
    command = render_prison_report(prison_analysis_results, prison_figures, report_source = prison_report_source),
    format = "file"
  )
  # Wrap-up
  # TODO: chore: Update the README with description of this pipeline
)

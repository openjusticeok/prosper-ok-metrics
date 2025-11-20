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
    "tulsaCountyJailScraper",
    "quarto",
    "here",
    "withr",
    "fs",
    "qs",
    "qs2",
    "glue",
    "ojothemes",
    "rlang"
  ),
  format = "qs"
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source("R/pipeline_helpers.R")
tar_source("R/01-ingest/ingest_raw_jail_data.R")
tar_source("R/02-check/input/check_raw_jail_data.R")
tar_source("R/02-check/input/check_raw_prison_data.R")
tar_source("R/02-check/output/check_processed_jail_data.R")
tar_source("R/02-check/output/check_processed_prison_data.R")
tar_source("R/03-process/process_raw_jail_data.R")
tar_source("R/03-process/process_raw_prison_data.R")
tar_source("R/04-analyze/jail_metrics.R")
tar_source("R/04-analyze/prison_metrics.R")
tar_source("R/04-analyze/jail_figures.R")
tar_source("R/04-analyze/prison_figures.R")
tar_source("R/render_report_jail.R")
tar_source("R/render_report_prison.R")

# Replace the target list below with your own:
# Estimated total implementation time: ~12 hours
list(
  # Jail pipeline
  tar_target(
    name = jail_raw_data,
    command = ingest_jail_raw_data()
  ),
  tar_target(
    name = jail_input_checks,
    command = check_jail_inputs(jail_raw_data)
  ),
  tar_target(
    name = jail_processed_data,
    command = process_jail_dataset(jail_input_checks)
  ),
  tar_target(
    name = jail_output_checks,
    command = verify_jail_outputs(jail_processed_data)
  ),
  tar_target(
    name = jail_analysis_results,
    command = analyze_jail_metrics(jail_processed_data)
  ),
  tar_target(
    name = jail_figures,
    command = generate_jail_figures(jail_analysis_results)
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
  # Estimate: 1.0 hour to implement prison input checks
  tar_target(
    name = prison_input_checks,
    command = check_prison_inputs()
  ),
  # Estimate: 1.5 hours to implement prison data processing
  tar_target(
    name = prison_processed_data,
    command = process_prison_dataset(prison_input_checks)
  ),
  # Estimate: 1.0 hour to implement prison output checks
  tar_target(
    name = prison_output_checks,
    command = verify_prison_outputs(prison_processed_data)
  ),
  # Estimate: 1.5 hours to implement prison analysis routines
  tar_target(
    name = prison_analysis_results,
    command = analyze_prison_metrics(prison_processed_data)
  ),
  # Estimate: 1.0 hour to implement prison figure generation
  tar_target(
    name = prison_figures,
    command = generate_prison_figures(prison_analysis_results)
  ),
  tar_target(
    name = prison_report_source,
    command = pipeline_report_path("prison"),
    format = "file"
  ),
  # Estimate: 1.0 hour to wire up prison report rendering
  tar_target(
    name = prison_report,
    command = render_prison_report(prison_analysis_results, prison_figures, report_source = prison_report_source),
    format = "file"
  )
)

# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble", "quarto", "here"), # Packages that your targets need for their tasks.
  format = "qs", # Optionally set the default storage format. qs is fast.
  #   controller = crew::crew_controller_local(workers = 2, seconds_idle = 60)
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source("R/pipeline_scaffolding.R")
tar_source("R/checks/input/jail.R")
tar_source("R/checks/input/prison.R")
tar_source("R/checks/output/jail.R")
tar_source("R/checks/output/prison.R")
tar_source("R/processing/jail.R")
tar_source("R/processing/prison.R")
tar_source("R/analysis/jail_metrics.R")
tar_source("R/analysis/prison_metrics.R")
tar_source("R/analysis/jail_figures.R")
tar_source("R/analysis/prison_figures.R")
tar_source("R/report_jail.R")
tar_source("R/report_prison.R")

# Replace the target list below with your own:
# Estimated total implementation time: ~12 hours
list(
  # Jail pipeline
  # Estimate: 1.0 hour to implement jail input checks
  tar_target(
    name = jail_input_checks,
    command = check_jail_inputs()
  ),
  # Estimate: 1.5 hours to implement jail data processing
  tar_target(
    name = jail_processed_data,
    command = process_jail_dataset(jail_input_checks)
  ),
  # Estimate: 1.0 hour to implement jail output checks
  tar_target(
    name = jail_output_checks,
    command = verify_jail_outputs(jail_processed_data)
  ),
  # Estimate: 1.5 hours to implement jail analysis routines
  tar_target(
    name = jail_analysis_results,
    command = analyze_jail_metrics(jail_processed_data)
  ),
  # Estimate: 1.0 hour to implement jail figure generation
  tar_target(
    name = jail_figures,
    command = generate_jail_figures(jail_analysis_results)
  ),
  # Estimate: 1.0 hour to wire up jail report rendering
  tar_target(
    name = jail_report,
    command = render_jail_report(jail_analysis_results, jail_figures),
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
  # Estimate: 1.0 hour to wire up prison report rendering
  tar_target(
    name = prison_report,
    command = render_prison_report(prison_analysis_results, prison_figures),
    format = "file"
  )
)

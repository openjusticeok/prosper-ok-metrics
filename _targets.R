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
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
# Estimated total implementation time: ~12 hours
list(
  # Jail pipeline
  # Estimate: 1.0 hour to implement jail input checks
  tar_target(
    name = jail_input_checks,
    command = check_input_data("jail")
  ),
  # Estimate: 1.5 hours to implement jail data processing
  tar_target(
    name = jail_processed_data,
    command = process_report_data("jail", jail_input_checks)
  ),
  # Estimate: 1.0 hour to implement jail output checks
  tar_target(
    name = jail_output_checks,
    command = check_output_data("jail", jail_processed_data)
  ),
  # Estimate: 1.5 hours to implement jail analysis routines
  tar_target(
    name = jail_analysis_results,
    command = analyze_output_data("jail", jail_processed_data)
  ),
  # Estimate: 1.0 hour to implement jail figure generation
  tar_target(
    name = jail_figures,
    command = produce_output_figures("jail", jail_analysis_results)
  ),
  # Estimate: 1.0 hour to wire up jail report rendering
  tar_target(
    name = jail_report,
    command = render_report("jail", jail_analysis_results, jail_figures),
    format = "file"
  ),
  # Prison pipeline
  # Estimate: 1.0 hour to implement prison input checks
  tar_target(
    name = prison_input_checks,
    command = check_input_data("prison")
  ),
  # Estimate: 1.5 hours to implement prison data processing
  tar_target(
    name = prison_processed_data,
    command = process_report_data("prison", prison_input_checks)
  ),
  # Estimate: 1.0 hour to implement prison output checks
  tar_target(
    name = prison_output_checks,
    command = check_output_data("prison", prison_processed_data)
  ),
  # Estimate: 1.5 hours to implement prison analysis routines
  tar_target(
    name = prison_analysis_results,
    command = analyze_output_data("prison", prison_processed_data)
  ),
  # Estimate: 1.0 hour to implement prison figure generation
  tar_target(
    name = prison_figures,
    command = produce_output_figures("prison", prison_analysis_results)
  ),
  # Estimate: 1.0 hour to wire up prison report rendering
  tar_target(
    name = prison_report,
    command = render_report("prison", prison_analysis_results, prison_figures),
    format = "file"
  )
)

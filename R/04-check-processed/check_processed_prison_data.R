check_prison_processed <- function(prison_processed_data) {
  sentence_with_profile_offense_data <- prison_processed_data$sentence_with_profile_offense_data

  # Create a pointblank agent
  agent <- pointblank::create_agent(
    tbl = sentence_with_profile_offense_data,
    label = "Prison Processed Data Checks"
  ) |>
    # Check 1: Sentencing county missing
    pointblank::col_vals_not_null(columns = sentencing_county) |>
    # Check 2: Active & In Custody but County "Not Reported / Not Applicable"
    # Ensure no rows have status == "ACTIVE" AND physical_custody == "TRUE" AND most_recent_sentencing_county == "Not Reported / Not Applicable"
    pointblank::col_vals_expr(
      expr = ~ !((status == "ACTIVE" & physical_custody == "TRUE") & most_recent_sentencing_county == "Not Reported / Not Applicable"),
      label = "Active & Custody must have reported sentencing county"
    ) |>
    # Check 3: Sentencing date missing
    pointblank::col_vals_null(columns = sentencing_date) |>
    # Check 4: Date lower bound (nonsensical dates like 1000-01-01)
    pointblank::col_vals_gt(columns = sentencing_date, value = "1920-01-01") |>
    # Check 5: Date upper bound (future dates / typos)
    # Using 2025-10-22 as per instructions/snippet context
    pointblank::col_vals_lt(columns = sentencing_date, value = "2025-10-22") |>
    pointblank::interrogate()

  # TODO: Add this as a check
  # # Identify multiple people with same sentence_id on the same extract date in
  #   # different counties
  # sentence_data |>
  #     dplyr::left_join(consecutive_data, by = c("sentence_id", "snapshot_date")) |>
  #     dplyr::filter(
  #       n() > 1,
  #       .by = c("sentence_id", "consecutive_to_id", "snapshot_date")
  #     )

  agent
}

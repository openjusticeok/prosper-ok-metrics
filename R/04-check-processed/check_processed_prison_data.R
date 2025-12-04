check_prison_processed <- function(processed_data) {
  people_with_sentence_info <- processed_data$people_with_sentence_info

  # Create a pointblank agent
  agent <- pointblank::create_agent(
    tbl = people_with_sentence_info,
    label = "Prison Processed Data Checks"
  ) |>
    # Check 1: Sentencing county missing
    pointblank::col_vals_not_null(columns = most_recent_sentencing_county) |>
    # Check 2: Active & In Custody but County "Not Reported / Not Applicable"
    # Ensure no rows have status == "ACTIVE" AND physical_custody == "TRUE" AND most_recent_sentencing_county == "Not Reported / Not Applicable"
    pointblank::col_vals_expr(
      expr = ~ !((status == "ACTIVE" & physical_custody == "TRUE") & most_recent_sentencing_county == "Not Reported / Not Applicable"),
      label = "Active & Custody must have reported sentencing county"
    ) |>
    # Check 3: Sentencing date missing
    pointblank::col_vals_null(columns = most_recent_sentencing_date) |>
    # Check 4: Date lower bound (nonsensical dates like 1000-01-01)
    pointblank::col_vals_gt(columns = most_recent_sentencing_date, value = "1920-01-01") |>
    # Check 5: Date upper bound (future dates / typos)
    # Using 2025-10-22 as per instructions/snippet context
    pointblank::col_vals_lt(columns = most_recent_sentencing_date, value = "2025-10-22") |>
    pointblank::interrogate()

  agent
}

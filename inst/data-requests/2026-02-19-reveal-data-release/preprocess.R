library(readr)
library(cli)
library(stringr)
library(fs)
library(purrr)

#' Preprocess Offender.csv - fixes unquoted commas in CurrentFacility column
#'
#' Strategy: CurrentFacility (column 14) contains commas like "ESCAPE, SAYRE"
#' Merge overflow columns back into CurrentFacility and quote it properly.
#'
#' @param input_file Path to input CSV file
#' @param output_file Path to output processed CSV file
#'
preprocess_offender <- function(input_file, output_file) {
  lines <- read_lines(
    input_file,
    locale = locale(encoding = "UTF-8"),
    progress = FALSE
  )

  header <- lines[1]
  expected_cols <- 15
  target_commas <- 14

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar)

  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts > target_commas)
  fixed_count <- length(bad_idx)

  # Fix only the bad rows
  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)
      n_extra <- n_fields - expected_cols

      # CurrentFacility is column 14, but we need to account for extra commas
      # The extra commas are IN the CurrentFacility field
      merge_start <- 14
      merge_end <- merge_start + n_extra

      if (merge_end <= n_fields) {
        # Merge fields from merge_start to merge_end into CurrentFacility
        merged <- str_flatten(fields[merge_start:merge_end], collapse = ",")
        
        # Remove leading comma if present (happens when merge_start field is empty)
        merged <- str_remove(merged, "^,")

        # Reconstruct the line with proper quoting
        if (merge_end < n_fields) {
          # There are more fields after CurrentFacility (Status column)
          fields <- c(
            fields[1:13],
            str_c('"', merged, '"'),
            fields[(merge_end + 1):n_fields]
          )
        } else {
          # CurrentFacility is the last field
          fields <- c(
            fields[1:13],
            str_c('"', merged, '"')
          )
        }

        return(str_flatten(fields, collapse = ","))
      }

      return(line)
    })
  }

  # Handle rows with standalone quote characters used as field values
  # Pattern: ,", (comma, quote, comma) - the quote is the entire field value
  # Replace with empty field: ,", -> ,,
  data_lines <- data_lines |>
    str_replace_all(',",', ',,')

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines, fixed {fixed_count}")

  invisible()
}

#' Preprocess OffenderAlias.csv - handles encoding issues
#'
#' This file has minimal comma issues (only 9 lines) but has encoding problems
#' Just clean up line endings and encoding.
#'
#' @param input_file Path to input CSV file
#' @param output_file Path to output processed CSV file
#'
preprocess_offender_alias <- function(input_file, output_file) {
  lines <- read_lines(
    input_file,
    locale = locale(encoding = "UTF-8"),
    progress = FALSE
  )

  if (length(lines) <= 1) {
    cli::cli_alert_warning("Empty file: {input_file}")
    return(invisible())
  }

  header <- lines[1]
  expected_cols <- 7

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar)

  # Count commas to find rows with wrong number of columns
  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts != (expected_cols - 1))
  fixed_count <- length(bad_idx)

  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)

      if (n_fields > expected_cols) {
        # Some rows have 8 columns - the issue is extra empty fields
        # Expected: Id, DOCNum, LastName, FirstName, MiddleInit, Suffix, DOB
        # Problem: Id, DOCNum, LastName, FirstName, MiddleInit, Suffix, (empty), DOB
        # OR: Id, DOCNum, LastName, FirstName_part1, FirstName_part2, MiddleInit, Suffix, DOB
        # Strategy: merge fields 4-5 (FirstName) and remove the extra empty field before DOB
        n_extra <- n_fields - expected_cols
        
        # Merge fields 4 and 5 into FirstName
        merged_firstname <- str_flatten(fields[4:5], collapse = ",")
        
        # Remove one empty field from position 7 (before DOB)
        # Result: Id, DOCNum, LastName, FirstName, MiddleInit, Suffix, DOB
        fields <- c(
          fields[1:3],
          merged_firstname,
          fields[6],
          fields[8]
        )

        return(str_flatten(fields, collapse = ","))
      } else if (n_fields < expected_cols) {
        # Pad with empty fields to reach expected column count
        fields <- c(fields, rep("", expected_cols - n_fields))
        return(str_flatten(fields, collapse = ","))
      }

      return(line)
    })
  }
  
  # Handle rows with standalone quote characters used as field values
  # Pattern: ,", (comma, quote, comma) - the quote is the entire field value
  # Replace with empty field: ,", -> ,,
  # Also handle quote at beginning of field followed by comma: ,"word -> ,word
  data_lines <- data_lines |>
    str_replace_all(',",', ',,') |>
    str_replace_all(',"([a-zA-Z])', ',\\1')

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines, fixed {fixed_count}")

  invisible()
}

#' Preprocess OffenderSentence.csv - fixes unquoted commas in OffenseDescription
#'
#' @param input_file Path to input CSV file
#' @param output_file Path to output processed CSV file
#'
preprocess_offender_sentence <- function(input_file, output_file) {
  lines <- read_lines(
    input_file,
    locale = locale(encoding = "UTF-8"),
    progress = FALSE
  )

  if (length(lines) <= 1) return(invisible())

  header <- lines[1]

  # OffenderSentence.csv is properly quoted CSV but has unescaped quotes inside fields
  # We need to escape unescaped quotes (e.g., "BURGLARY":ATTEMPTED" -> "BURGLARY""":ATTEMPTED")
  # Strategy: Replace any single quote that appears between two non-comma characters
  # with two quotes (CSV escaping convention)

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar) |>
    # Replace unescaped quotes inside fields with escaped quotes (doubled)
    # Pattern: match a quote that is preceded by a non-comma/non-quote and followed by non-comma/non-quote
    str_replace_all('(?<=[^,])"(?=[^,])', '""')

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines (escaped internal quotes)")

  invisible()
}

#' Preprocess OffenderReception.csv - fixes unquoted commas in Reason column
#'
#' Strategy: Reason (column 3) contains commas like "ACCELERATED, DEFERRED TO INCARCERATED"
#' Merge overflow columns back into Reason and quote it properly.
#'
#' @param input_file Path to input CSV file
#' @param output_file Path to output processed CSV file
#'
preprocess_offender_reception <- function(input_file, output_file) {
  lines <- read_lines(
    input_file,
    locale = locale(encoding = "UTF-8"),
    progress = FALSE
  )

  if (length(lines) <= 1) {
    cli::cli_alert_warning("Empty file: {input_file}")
    return(invisible())
  }

  header <- lines[1]
  expected_cols <- 4
  target_commas <- 3

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar)

  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts > target_commas)
  fixed_count <- length(bad_idx)

  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)
      n_extra <- n_fields - expected_cols

      merge_start <- 3
      merge_end <- merge_start + n_extra

      if (merge_end <= n_fields) {
        merged <- str_flatten(fields[merge_start:merge_end], collapse = ",")

        if (merge_end < n_fields) {
          fields <- c(
            fields[1:2],
            str_c('"', merged, '"'),
            fields[(merge_end + 1):n_fields]
          )
        } else {
          fields <- c(
            fields[1:2],
            str_c('"', merged, '"')
          )
        }
        return(str_flatten(fields, collapse = ","))
      }
      return(line)
    })
  }

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines, fixed {fixed_count}")

  invisible()
}

#' Preprocess OffenderExit.csv - fixes unquoted commas in ExitReason column
#'
#' Strategy: ExitReason (column 3) contains commas like "DISCHARGED, COURT ORDERED"
#' Merge overflow columns back into ExitReason and quote it properly.
#'
#' @param input_file Path to input CSV file
#' @param output_file Path to output processed CSV file
#'
preprocess_offender_exit <- function(input_file, output_file) {
  lines <- read_lines(
    input_file,
    locale = locale(encoding = "UTF-8"),
    progress = FALSE
  )

  if (length(lines) <= 1) {
    cli::cli_alert_warning("Empty file: {input_file}")
    return(invisible())
  }

  header <- lines[1]
  expected_cols <- 3
  target_commas <- 2

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar)

  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts > target_commas)
  fixed_count <- length(bad_idx)

  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)
      n_extra <- n_fields - expected_cols

      merge_start <- 3
      merge_end <- merge_start + n_extra

      if (merge_end <= n_fields) {
        merged <- str_flatten(fields[merge_start:merge_end], collapse = ",")

        if (merge_end < n_fields) {
          fields <- c(
            fields[1:2],
            str_c('"', merged, '"'),
            fields[(merge_end + 1):n_fields]
          )
        } else {
          fields <- c(
            fields[1:2],
            str_c('"', merged, '"')
          )
        }
        return(str_flatten(fields, collapse = ","))
      }
      return(line)
    })
  }

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines, fixed {fixed_count}")

  invisible()
}

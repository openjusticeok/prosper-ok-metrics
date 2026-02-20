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
    keep(nzchar) |>
    str_replace_all(c(',"' = ',', '",' = ','))

  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts > target_commas)
  fixed_count <- length(bad_idx)

  # Fix only the bad rows
  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)
      n_extra <- n_fields - expected_cols

      merge_start <- 14
      merge_end <- merge_start + n_extra

      if (merge_end <= n_fields) {
        merged <- str_flatten(fields[merge_start:merge_end], collapse = ",")

        if (merge_end < n_fields) {
          fields <- c(
            fields[1:13],
            str_c('"', merged, '"'),
            fields[(merge_end + 1):n_fields]
          )
        } else {
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

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar)

  readr::write_lines(c(header, data_lines), output_file)
  cli::cli_alert_success("Processed {length(data_lines)} lines (encoding cleanup only)")

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
  expected_cols <- 21
  target_commas <- 20

  data_lines <- lines[-1] |>
    str_remove_all("\\r$") |>
    keep(nzchar) |>
    str_replace_all(c(',"' = ',', '",' = ','))

  comma_counts <- str_count(data_lines, ",")
  bad_idx <- which(comma_counts > target_commas)
  fixed_count <- length(bad_idx)

  if (fixed_count > 0) {
    data_lines[bad_idx] <- map_chr(data_lines[bad_idx], function(line) {
      fields <- str_split_1(line, ",")
      n_fields <- length(fields)
      n_extra <- n_fields - expected_cols

      merge_start <- 9 # OffenseDescription column
      merge_end <- merge_start + n_extra

      if (merge_end <= n_fields) {
        merged <- str_flatten(fields[merge_start:merge_end], collapse = ",")

        if (merge_end < n_fields) {
          fields <- c(
            fields[1:8],
            str_c('"', merged, '"'),
            fields[(merge_end + 1):n_fields]
          )
        } else {
          fields <- c(fields[1:8], str_c('"', merged, '"'))
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

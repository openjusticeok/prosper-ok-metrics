##' Write grouped count exports
##'
##' Writes grouped count CSVs using `base_group_vars` and `other_group_vars`.
##' Always writes:
##' 1) base groups only
##' 2) base groups + each `other_group_vars` value individually
##'
##' When `cumulative = TRUE`, also writes cumulative groupings by order of
##' `other_group_vars`:
##' 1) base + other[1]
##' 2) base + other[1] + other[2]
##' 3) ... and so on
##'
##' @param data A data frame to aggregate.
##' @param base_group_vars Character vector of base grouping variables.
##' @param other_group_vars Character vector of additional grouping variables.
##' @param output_dir Directory where CSV files are written.
##' @param prefix Filename prefix used before `_by_<group vars>.csv`.
##' @param value_name Name of the count column in outputs.
##' @param cumulative Logical; when `TRUE`, writes cumulative groupings.
##'
##' @return Invisibly returns `NULL`. Called for side effects (file output).
##' @export
write_group_count <- function(
  data,
  base_group_vars,
  other_group_vars = character(0),
  output_dir,
  prefix,
  value_name = "n",
  cumulative = FALSE
) {
  all_group_vars <- c(base_group_vars, other_group_vars)
  missing_group_vars <- setdiff(all_group_vars, names(data))
  if (length(missing_group_vars) > 0) {
    stop(
      "Missing export variables: ",
      paste(missing_group_vars, collapse = ", ")
    )
  }

  if (length(base_group_vars) == 0) {
    stop("`base_group_vars` must include at least one variable.")
  }

  group_sets <- list(base_group_vars)

  if (length(other_group_vars) > 0) {
    pairwise_sets <- purrr::map(
      other_group_vars,
      \(group_var) c(base_group_vars, group_var)
    )
    group_sets <- c(group_sets, pairwise_sets)
  }

  if (isTRUE(cumulative) && length(other_group_vars) > 0) {
    cumulative_sets <- purrr::map(
      seq_along(other_group_vars),
      \(i) c(base_group_vars, other_group_vars[seq_len(i)])
    )
    group_sets <- c(group_sets, cumulative_sets)
  }

  group_set_keys <- purrr::map_chr(
    group_sets,
    \(vars) paste(vars, collapse = "||")
  )
  group_sets <- group_sets[!duplicated(group_set_keys)]

  purrr::walk(
    group_sets,
    \(current_group_vars) {
      file_name <- paste0(
        prefix,
        "_by_",
        paste(current_group_vars, collapse = "_"),
        ".csv"
      )

      dplyr::count(
        data,
        !!!rlang::syms(current_group_vars),
        name = value_name
      ) |>
        readr::write_csv(
          fs::path(output_dir, file_name)
        )
    }
  )
}

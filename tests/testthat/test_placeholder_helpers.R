# WARNING: These need more comprehensive tests once we have more complex logic.
# TODO: feat(helper): Add more comprehensive tests for placeholder helpers
source(testthat::test_path("..", "..", "R", "pipeline_helpers.R"))

# named_list tests ============================================================

test_that("named_list creates a named list from bare object names", {
  foo <- 1
  bar <- "hello"
  baz <- list(a = 1, b = 2)

  result <- named_list(foo, bar, baz)

  expect_type(result, "list")
  expect_named(result, c("foo", "bar", "baz"))
  expect_identical(result$foo, 1)
  expect_identical(result$bar, "hello")
  expect_identical(result$baz, list(a = 1, b = 2))
})

test_that("named_list works with a single argument", {
  single <- 42

  result <- named_list(single)

  expect_named(result, "single")
  expect_identical(result$single, 42)
})

test_that("named_list returns empty list with no arguments", {
  result <- named_list()
  expect_identical(result, stats::setNames(list(), character(0)))
})

test_that("named_list preserves object types", {
  df <- data.frame(x = 1:3)
  vec <- c(1, 2, 3)
  fn <- function(x) x + 1

  result <- named_list(df, vec, fn)

  expect_s3_class(result$df, "data.frame")
  expect_type(result$vec, "double")
  expect_type(result$fn, "closure")
})

test_that("placeholder time series is deterministic and well-formed", {
  ts <- .placeholder_time_series()

  expect_true(tibble::is_tibble(ts))
  expect_identical(names(ts), c("time", "good_stuff"))
  expect_equal(nrow(ts), 7) # 7 time points
  expect_equal(sum(diff(ts$good_stuff) < 0), 2) # 2 kinks downward
})

test_that("placeholder_ggplot returns configured plot", {
  plot <- placeholder_ggplot()

  expect_s3_class(plot, "ggplot") # Check it's a ggplot object
  expect_equal(plot$labels$title, "Placeholder")
  expect_equal(plot$labels$subtitle, "A very scientific graph.")
  expect_equal(plot$labels$x, "Time")
  expect_equal(plot$labels$y, "Good Stuff")

  built <- ggplot2::ggplot_build(plot) # Build the plot to inspect data layers
  expect_equal(nrow(built$data[[1]]), 7)
})

test_that("placeholder_list returns an empty list", {
  expect_identical(placeholder_list(), list())
  # Expected behavior of empty list
  expect_equal(length(placeholder_list()), 0)
  expect_true(is.list(placeholder_list()))
  expect_false(is.data.frame(placeholder_list()))
  expect_identical(is.null(placeholder_list()), FALSE)
  expect_identical(names(placeholder_list()), NULL)
  expect_identical(nrow(placeholder_list()), NULL)
})

test_that("placeholder_tibble is an empty tibble", {
  tbl <- placeholder_tibble()
  expect_true(tibble::is_tibble(tbl))
  expect_equal(nrow(tbl), 0)
  expect_equal(ncol(tbl), 0)
})

test_that("placeholder_gt returns a gt table with placeholder data", {

  tbl <- placeholder_gt()

  expect_s3_class(tbl, "gt_tbl")
  data <- tbl[["_data"]]
  expect_identical(names(data), c("time", "good_stuff"))
  expect_equal(nrow(data), 7)
})

test_that("placeholder_highchart returns a line chart", {
  testthat::skip_if_not_installed("highcharter")

  chart <- placeholder_highchart()

  expect_s3_class(chart, "highchart")
  expect_true("htmlwidget" %in% class(chart))

  series <- chart$x$hc_opts$series[[1]]
  expect_equal(series$type, "line")
  expect_length(series$data, 7)

  first_point <- series$data[[1]]
  expect_equal(first_point$y, .placeholder_time_series()$good_stuff[1])
})

test_that("placeholder_highchart_map returns a county choropleth", {
  testthat::skip_if_not_installed("highcharter")
  testthat::skip_if_not_installed("tigris")

  map <- placeholder_highchart_map()
  counties <- .placeholder_ok_counties()

  expect_s3_class(map, "highchart")
  expect_length(map$x$hc_opts$series, 1)

  series <- map$x$hc_opts$series[[1]]
  expect_equal(series$name, "Good")
  expect_equal(length(series$data), nrow(counties))
  expect_true(all(vapply(series$data, function(x) !is.null(x$value), logical(1))))
})

test_that("placeholder_number returns the sentinel numeric", {
  expect_identical(placeholder_number(), 999999999)
})

test_that("placeholder_string returns the sentinel string", {
  expect_identical(placeholder_string(), "[PLACEHOLDER]")
})

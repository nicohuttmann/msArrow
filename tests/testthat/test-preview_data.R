test_that("preview_data() reads only the first n rows of every type", {

  dir <- withr::local_tempdir()
  d <- test_table(50)

  for (type in c("parquet", "rds", "tsv", "csv", "txt")) {
    path <- write_data(d, paste0("p_", type), dir, type = type, silent = T)
    out <- preview_data(path, n = 5, silent = T)
    expect_s3_class(out, "tbl_df")
    expect_equal(nrow(out), 5, info = type)
    expect_equal(out$Protein.Group, d$Protein.Group[1:5], info = type)
  }
})


test_that("preview_data() attaches the total row count", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(50), "tot", dir, silent = T)

  out <- preview_data(path, n = 5, silent = T)

  expect_equal(attr(out, "total_rows"), 50)
  expect_equal(nrow(out), 5)
})


test_that("preview_data() reports what it is showing unless silenced", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(50), "msg", dir, silent = T)

  expect_output(preview_data(path, n = 5), "Showing 5 of 50 rows")
  expect_silent(preview_data(path, n = 5, silent = T))
})


test_that("preview_data() handles n larger than the file", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(3), "small", dir, silent = T)

  out <- preview_data(path, n = 999, silent = T)

  expect_equal(nrow(out), 3)
  expect_equal(attr(out, "total_rows"), 3)
})


test_that("preview_data() works on a partitioned dataset directory", {

  dir <- withr::local_tempdir()
  path <- write_data(test_long(), "part", dir, partitioning = "observations",
                     silent = T)

  out <- preview_data(path, n = 4, silent = T)

  expect_s3_class(out, "tbl_df")
  expect_equal(nrow(out), 4)
})


test_that("a .parquetlist shows only the first dataset by default", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = test_table(5),
                          second = test_table(7),
                          third  = test_table(9)),
                     "lst", dir, silent = T)

  out <- preview_data(path, n = 3, silent = T)

  expect_type(out, "list")
  expect_named(out, "first")
  expect_equal(nrow(out$first), 3)
  expect_equal(attr(out$first, "total_rows"), 5)
  expect_equal(attr(out, "total_sets"), 3)
})


test_that("the second number of <n> says how many datasets to show", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = test_table(5),
                          second = test_table(7),
                          third  = test_table(9)),
                     "lst", dir, silent = T)

  two <- preview_data(path, n = c(3, 2), silent = T)
  expect_named(two, c("first", "second"))
  expect_equal(nrow(two$second), 3)
  expect_equal(attr(two$second, "total_rows"), 7)

  all <- preview_data(path, n = c(3, 3), silent = T)
  expect_named(all, c("first", "second", "third"))

  # asking for more than there are is not an error
  expect_named(preview_data(path, n = c(3, 99), silent = T),
               c("first", "second", "third"))
})


test_that("the message says how to see the datasets that were left out", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = test_table(5),
                          second = test_table(7),
                          third  = test_table(9)),
                     "lst", dir, silent = T)

  expect_output(preview_data(path, n = 4), "2 more datasets not shown")
  expect_output(preview_data(path, n = 4), "n = c(4, 3) to show all", fixed = TRUE)

  # singular when only one is hidden
  expect_output(preview_data(path, n = c(4, 2)), "1 more dataset not shown")

  # and no hint once everything is shown
  out <- capture.output(preview_data(path, n = c(4, 3)))
  expect_false(any(grepl("not shown", out)))
})


test_that("a .parquetlist of different schemas is not flattened", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = tibble::tibble(a = 1:3, b = letters[1:3]),
                          second = tibble::tibble(x = 1:2, y = 3:4, z = 5:6)),
                     "mixed", dir, silent = T)

  out <- preview_data(path, n = c(10, 2), silent = T)

  # both elements survive, with their own columns
  expect_named(out$first, c("a", "b"))
  expect_named(out$second, c("x", "y", "z"))
})


test_that("preview_data() accepts data that is already in the session", {

  d <- test_table(20)

  out <- preview_data(d, n = 4, silent = T)

  expect_equal(nrow(out), 4)
  expect_equal(attr(out, "total_rows"), 20)
})


test_that("preview_data() keeps open_data()'s message for a missing input", {

  expect_error(preview_data(NULL), "<file> or <fallback>", fixed = TRUE)
})


test_that("preview_data() uses <fallback> when <file> is NULL", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(9), "fb", dir, silent = T)

  out <- preview_data(NULL, n = 2, fallback = path, silent = T)

  expect_equal(nrow(out), 2)
  expect_equal(attr(out, "total_rows"), 9)
})


test_that("view_data() returns the preview invisibly", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(12), "v", dir, silent = T)

  expect_invisible(view_data(path, n = 3, silent = T))

  out <- view_data(path, n = 3, silent = T)
  expect_s3_class(out, "tbl_df")
  expect_equal(nrow(out), 3)
  expect_equal(attr(out, "total_rows"), 12)
})


test_that("view_data() accepts data rather than a path", {

  # the old version died here building the viewer title
  expect_no_error(view_data(test_table(5), n = 2, silent = T))
})


test_that("view_data() accepts an explicit title", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(5), "t", dir, silent = T)

  expect_no_error(view_data(path, n = 2, title = "my tab", silent = T))
})


test_that("view_data() returns a list for a .parquetlist", {

  dir <- withr::local_tempdir()
  path <- write_data(list(a = test_table(4)), "vl", dir, silent = T)

  out <- view_data(path, n = 2, silent = T)

  expect_type(out, "list")
  expect_named(out, "a")
})


test_that("open_data() opens a .parquetlist as a list of connections", {

  dir <- withr::local_tempdir()
  path <- write_data(list(one = test_table(4), two = test_table(6)),
                     "ol", dir, silent = T)

  out <- open_data(path)

  expect_type(out, "list")
  expect_named(out, c("one", "two"), ignore.order = TRUE)
  expect_s3_class(out$one, "Dataset", exact = FALSE)
  expect_equal(nrow(out$two), 6)
})


test_that("view_data() uses the View() on the search path, not utils::View()", {

  # RStudio masks View() this way; if view_data() called utils::View()
  # directly the mask would be bypassed and the RStudio viewer never used.
  seen <- new.env()
  masking <- new.env(parent = globalenv())
  assign("View", function(x, title) {
    seen$x <- x
    seen$title <- title
    invisible(NULL)
  }, envir = masking)

  attach(masking, name = "tools:rstudio_test", warn.conflicts = FALSE)
  withr::defer(detach("tools:rstudio_test"))

  # interactive() is FALSE under testthat, so call the lookup the same way
  view_fun <- get("View", envir = globalenv())
  expect_false(identical(view_fun, utils::View))

  dir <- withr::local_tempdir()
  path <- write_data(test_table(6), "masked", dir, silent = T)
  view_fun(preview_data(path, n = 2, silent = T), basename(path))

  expect_equal(nrow(seen$x), 2)
  expect_equal(seen$title, "masked.parquet")
})



test_that("view_data() reports after the viewer, and honours silent", {

  dir <- withr::local_tempdir()
  path <- write_data(test_table(12), "v2", dir, silent = T)

  expect_output(view_data(path, n = 3), "Showing 3 of 12 rows")
  expect_silent(view_data(path, n = 3, silent = T))
})


test_that("view_data() passes the dataset count through to the hint", {

  dir <- withr::local_tempdir()
  path <- write_data(list(a = test_table(4),
                          b = test_table(6)),
                     "vl2", dir, silent = T)

  out <- view_data(path, n = 2)
  expect_named(out, "a")
  expect_equal(attr(out, "total_sets"), 2)
})

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


test_that("preview_data() previews each element of a .parquetlist separately", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = test_table(5),
                          second = test_table(7)),
                     "lst", dir, silent = T)

  out <- preview_data(path, n = 3, silent = T)

  expect_type(out, "list")
  expect_named(out, c("first", "second"), ignore.order = TRUE)
  expect_equal(nrow(out$first), 3)
  expect_equal(attr(out$first, "total_rows"), 5)
  expect_equal(attr(out$second, "total_rows"), 7)
})


test_that("a .parquetlist of different schemas is not flattened", {

  dir <- withr::local_tempdir()

  path <- write_data(list(first  = tibble::tibble(a = 1:3, b = letters[1:3]),
                          second = tibble::tibble(x = 1:2, y = 3:4, z = 5:6)),
                     "mixed", dir, silent = T)

  out <- preview_data(path, n = 10, silent = T)

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

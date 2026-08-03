test_that("tempdir_list() finds files written by write_data()", {

  path <- write_data(test_table(), type = "tsv", silent = T)

  expect_true(normalizePath(path) %in% normalizePath(tempdir_list()))
})


test_that("tempdir_list() covers every writable type", {

  dir <- withr::local_tempdir(tmpdir = tempdir())

  for (type in c("parquet", "rds", "tsv", "csv", "txt")) {
    write_data(test_table(), paste0("tl_", type), dir, type = type, silent = T)
  }

  found <- tempdir_list(dir)
  expect_length(found, 5)
})


test_that("tempdir_size() returns one human readable size per file", {

  dir <- withr::local_tempdir(tmpdir = tempdir())
  write_data(test_table(), "s1", dir, type = "tsv", silent = T)
  write_data(test_table(), "s2", dir, type = "parquet", silent = T)

  sizes <- tempdir_size(dir)

  expect_length(sizes, 2)
  expect_type(sizes, "character")
})


test_that("tempdir_remove() deletes the files it lists", {

  dir <- withr::local_tempdir(tmpdir = tempdir())
  write_data(test_table(), "r1", dir, type = "tsv", silent = T)
  write_data(test_table(), "r2", dir, type = "parquet", silent = T)

  expect_length(tempdir_list(dir), 2)
  expect_true(all(tempdir_remove(dir)))
  expect_length(tempdir_list(dir), 0)
})


test_that("tempdir_list() accepts a custom pattern", {

  dir <- withr::local_tempdir(tmpdir = tempdir())
  write_data(test_table(), "p1", dir, type = "tsv", silent = T)
  write_data(test_table(), "p2", dir, type = "parquet", silent = T)

  expect_length(tempdir_list(dir, pattern = "\\.tsv$"), 1)
})

test_that(".save_objects_recursively() mirrors a nested list as folders", {

  dir <- withr::local_tempdir()

  .save_objects_recursively(object = list(a = test_table(),
                                          inner = list(b = test_table())),
                            name = "tree",
                            dir = dir,
                            silent = T)

  expect_true(dir.exists(file.path(dir, "tree", "inner")))
  expect_true(file.exists(file.path(dir, "tree", "a.parquet")))
  expect_true(file.exists(file.path(dir, "tree", "inner", "b.parquet")))
})


test_that(".read_objects_recursively() restores the nesting", {

  dir <- withr::local_tempdir()

  .save_objects_recursively(object = list(a = test_table(),
                                          inner = list(b = test_table())),
                            name = "tree",
                            dir = dir,
                            silent = T)

  out <- .read_objects_recursively(name = "tree", dir = dir, silent = T)

  expect_named(out, c("a", "inner"), ignore.order = TRUE)
  expect_equal(out$a, test_table())
  expect_equal(out$inner$b, test_table())
})


test_that("<type> propagates into the folder tree", {

  dir <- withr::local_tempdir()

  .save_objects_recursively(object = list(a = test_table()),
                            name = "tsvtree",
                            dir = dir,
                            type = "tsv",
                            silent = T)

  expect_true(file.exists(file.path(dir, "tsvtree", "a.tsv")))
  expect_false(file.exists(file.path(dir, "tsvtree", "a.parquet")))
})


test_that("a .parquetlist written as tsv reads back correctly", {

  dir <- withr::local_tempdir()

  path <- write_data(list(a = test_table(), b = test_table()), "lst", dir,
                     type = "tsv", silent = T)

  out <- get_data(path)

  expect_named(out, c("a", "b"), ignore.order = TRUE)
  expect_equal(out$a, test_table())
})


test_that("load_objects() reads a store tree without assigning", {

  dir <- withr::local_tempdir()

  .save_objects_recursively(object = list(a = test_table()),
                            name = "Datasets",
                            dir = dir,
                            silent = T)

  out <- load_objects(dir = dir, objects = "Datasets", assign = FALSE,
                      silent = TRUE)

  expect_equal(out$Datasets$a, test_table())
})


test_that("load_objects() errors on a missing directory", {

  expect_error(load_objects(dir = file.path(tempdir(), "does_not_exist_xyz")),
               "not found")
})

test_that(".add_dataset() creates the folder layout and both registries", {

  dir <- local_store()

  expect_true(dir.exists(file.path(dir, "Precursors", "Variables")))
  expect_true(dir.exists(file.path(dir, "Precursors", "Observations")))
  expect_true(dir.exists(file.path(dir, "Precursors", "Data_frames")))
  expect_true(exists("Datasets", envir = globalenv()))
  expect_true(exists(".Datasets", envir = globalenv()))
})


test_that("get_dataset_names() lists registered datasets", {

  dir <- local_store()

  expect_equal(get_dataset_names(), "Precursors")

  .add_dataset("Peptides", save_dir = dir)
  expect_setequal(get_dataset_names(), c("Precursors", "Peptides"))
})


test_that("get_dataset() resolves a single dataset without an argument", {

  local_store()

  expect_equal(get_dataset(), "Precursors")
  expect_equal(get_dataset("Precursors"), "Precursors")
})


test_that("get_dataset() requires a name when several datasets exist", {

  dir <- local_store()
  .add_dataset("Peptides", save_dir = dir)

  expect_error(get_dataset(), "specify a dataset")
})


test_that("get_dataset() rejects an unknown dataset", {

  local_store()

  expect_error(get_dataset("nope"), "could not be found")
})


test_that("get_dataset() errors when the registries are missing", {

  # no local_store() here on purpose
  suppressWarnings(rm(list = intersect(c("Datasets", ".Datasets"),
                                       ls(globalenv(), all.names = TRUE)),
                      envir = globalenv()))

  expect_error(get_dataset("x"), "list named Datasets")
})


test_that(".add_dataset() warns when a name is reused", {

  dir <- local_store()

  expect_warning(.add_dataset("Precursors", save_dir = dir), "already exists")
  expect_silent(.add_dataset("Precursors", save_dir = dir, replace = T))
})


test_that("retag_datasets() refuses the placeholder tag", {

  local_full_store()

  expect_message(out <- retag_datasets(), "change the tag")
  expect_false(out)
})


test_that("retag_datasets() renames the stored files and updates the registry", {

  local_full_store()

  before <- .Datasets$Precursors$Variables
  expect_true(file.exists(before))

  suppressMessages(retag_datasets(tag = "_v2"))

  after <- .Datasets$Precursors$Variables
  expect_match(after, "variables_v2")
  expect_true(file.exists(after))
  expect_false(file.exists(before))
})

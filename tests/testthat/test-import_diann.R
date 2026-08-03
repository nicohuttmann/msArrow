test_that(".add_defaults() populates Info$defaults", {

  withr::defer(suppressWarnings(rm(list = intersect("Info",
                                                    ls(globalenv(),
                                                       all.names = TRUE)),
                                   envir = globalenv())))

  .add_defaults()

  expect_true(exists("Info", envir = globalenv()))
  expect_true(all(c("variables_data", "data_frames") %in%
                    names(get("Info", envir = globalenv())$defaults)))
})


test_that(".get_defaults() returns the requested default set", {

  withr::defer(suppressWarnings(rm(list = intersect("Info",
                                                    ls(globalenv(),
                                                       all.names = TRUE)),
                                   envir = globalenv())))

  expect_true("default" %in% names(.get_defaults("variables_data")))
  expect_true("Precursor.Id" %in% .get_defaults("variables_data", "default"))
  expect_equal(.get_defaults("data_frames", "default"),
               c("Precursor.Normalised", "Ms1.Normalised"))
  expect_true(is.list(.get_defaults()))
})


test_that("check_report() summarises a DIA-NN report", {

  dir <- withr::local_tempdir()
  file <- write_test_report(file.path(dir, "report.parquet"))

  out <- suppressMessages(check_report(file))

  expect_true(is.list(out))
  expect_true("schema" %in% names(out))
})


test_that("import_diann() builds a dataset store from a report", {

  dir <- withr::local_tempdir()
  file <- write_test_report(file.path(dir, "report.parquet"))

  withr::defer(suppressWarnings(rm(list = intersect(c("Datasets", ".Datasets",
                                                      "Info"),
                                                    ls(globalenv(),
                                                       all.names = TRUE)),
                                   envir = globalenv())))

  suppressMessages(import_diann(file = file,
                                name = "Precursors",
                                save_dir = dir,
                                silent = TRUE))

  expect_true("Precursors" %in% get_dataset_names())
  expect_length(get_observations(dataset = "Precursors"), 2)
  expect_length(get_variables(dataset = "Precursors"), 6)
  expect_true(any(c("Precursor.Normalised", "Ms1.Normalised") %in%
                    get_data_frame_names("Precursors")))
})


test_that("import_diann() stores the requested variables columns", {

  dir <- withr::local_tempdir()
  file <- write_test_report(file.path(dir, "report.parquet"))

  withr::defer(suppressWarnings(rm(list = intersect(c("Datasets", ".Datasets",
                                                      "Info"),
                                                    ls(globalenv(),
                                                       all.names = TRUE)),
                                   envir = globalenv())))

  suppressMessages(import_diann(file = file, name = "Precursors",
                                save_dir = dir, silent = TRUE))

  expect_true(all(c("Genes", "Protein.Group") %in%
                    get_variables_data_names("Precursors")))
})


test_that("import_diann_channel() exists and takes a channel argument", {

  expect_true(is.function(import_diann_channel))
  expect_true("channel" %in% names(formals(import_diann_channel)))
})

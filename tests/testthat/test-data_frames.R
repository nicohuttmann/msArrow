test_that("save_data_frame() registers the frame and writes it to disk", {

  local_full_store()

  expect_true("Intensity" %in% get_data_frame_names("Precursors"))
  expect_true(file.exists(.Datasets$Precursors$Data_frames$Intensity))
})


test_that("get_data_frame_names() lists every stored frame", {

  dir <- local_full_store()

  suppressMessages(save_data_frame(test_long(), dataset = "Precursors",
                                   name = "Second", save_dir = dir))

  expect_setequal(get_data_frame_names("Precursors"), c("Intensity", "Second"))
})


test_that("get_data_frame() returns the stored data", {

  local_full_store()

  out <- get_data_frame("Intensity", dataset = "Precursors")

  expect_gt(nrow(out), 0)
  expect_true(all(c("observations", "variables") %in% names(out)) ||
                ncol(out) > 1)
})


test_that("get_data_frame() can be subset by observations and variables", {

  local_full_store()

  out <- get_data_frame("Intensity",
                        observations = c("run1", "run2"),
                        dataset = "Precursors")

  expect_gt(nrow(out), 0)
  expect_lte(nrow(out), nrow(get_data_frame("Intensity",
                                            dataset = "Precursors")))
})


test_that("open_data_frame() returns a lazy Arrow object", {

  local_full_store()

  expect_s3_class(open_data_frame("Intensity", dataset = "Precursors"),
                  c("Dataset", "ArrowTabular", "arrow_dplyr_query"),
                  exact = FALSE)
})


test_that("save_data_frame() supports partitioning", {

  dir <- local_full_store()

  suppressMessages(save_data_frame(test_long(), dataset = "Precursors",
                                   name = "Part", save_dir = dir,
                                   partitioning = "observations"))

  expect_true(dir.exists(.Datasets$Precursors$Data_frames$Part))
  expect_gt(nrow(get_data_frame("Part", dataset = "Precursors")), 0)
})


test_that("save_data_frame() errors without data", {

  local_store()

  expect_error(save_data_frame(dataset = "Precursors"), "No data given")
})


test_that(".save_data_frame() stores a frame and returns its location", {

  dir <- local_full_store()

  out <- suppressMessages(.save_data_frame(test_long(),
                                           dataset = "Precursors",
                                           name = "Direct",
                                           save_dir = dir))

  expect_true(file.exists(out$location) || dir.exists(out$location))
})

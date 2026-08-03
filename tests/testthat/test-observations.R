test_that("save_observations_data() writes a parquet file and a preview", {

  local_full_store()

  expect_match(.Datasets$Precursors$Observations, "[.]parquet$")
  expect_true(file.exists(.Datasets$Precursors$Observations))
  expect_s3_class(Datasets$Precursors$Observations, "tbl_df")
})


test_that("save_observations_data() refuses duplicate observations", {

  dir <- local_store()

  dup <- tibble::tibble(observations = c("run1", "run1"),
                        Condition = c("a", "b"))

  expect_warning(save_observations_data(dup, dataset = "Precursors",
                                        save_dir = dir))
})


test_that("get_observations() returns the observation identifiers", {

  local_full_store()

  expect_length(get_observations(dataset = "Precursors"), 4)
  expect_true(all(paste0("run", 1:4) %in%
                    get_observations(dataset = "Precursors")))
})


test_that("get_observations_data() returns a tibble or an Arrow connection", {

  local_full_store()

  expect_s3_class(get_observations_data(dataset = "Precursors"), "tbl_df")
  expect_s3_class(get_observations_data(dataset = "Precursors",
                                        as_arrow_table = T),
                  c("Dataset", "ArrowTabular", "arrow_dplyr_query"),
                  exact = FALSE)
})


test_that("get_observations_data_names() lists the stored columns", {

  local_full_store()

  expect_true("Condition" %in% get_observations_data_names("Precursors"))
})


test_that("add_observations_data() joins stored columns onto a frame", {

  local_full_store()

  out <- add_observations_data(tibble::tibble(observations = c("run1", "run3")),
                               which = "Condition",
                               dataset = "Precursors")

  expect_true("Condition" %in% names(out))
  expect_equal(out$Condition, c("ctrl", "t8"))
})


test_that("add_observations_data() returns the data unchanged without <which>", {

  local_full_store()

  d <- tibble::tibble(observations = "run1")
  expect_equal(add_observations_data(d, dataset = "Precursors"), d)
})


test_that(".save_observations_data() stores a frame and returns its location", {

  dir <- local_store()

  out <- suppressMessages(
    .save_observations_data(tibble::tibble(observations = paste0("run", 1:3),
                                           Condition = letters[1:3]),
                            dataset = "Precursors",
                            save_dir = dir))

  expect_true(file.exists(out$location))
  expect_s3_class(out$preview, "tbl_df")
})

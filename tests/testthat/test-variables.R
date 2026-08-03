test_that("save_variables_data() writes a parquet file and a preview", {

  local_full_store()

  expect_match(.Datasets$Precursors$Variables, "[.]parquet$")
  expect_true(file.exists(.Datasets$Precursors$Variables))
  expect_s3_class(Datasets$Precursors$Variables, "tbl_df")
})


test_that("save_variables_data() refuses duplicate variables", {

  dir <- local_store()

  dup <- tibble::tibble(variables = c("P1", "P1"), Genes = c("a", "b"))

  expect_warning(save_variables_data(dup, dataset = "Precursors",
                                     save_dir = dir))
})


test_that("get_variables() returns the variable identifiers", {

  local_full_store()

  expect_length(get_variables(dataset = "Precursors"), 6)
  expect_true(all(paste0("P", 1:6) %in% get_variables(dataset = "Precursors")))
})


test_that("get_variables_data() returns a tibble or an Arrow connection", {

  local_full_store()

  expect_s3_class(get_variables_data(dataset = "Precursors"), "tbl_df")
  expect_s3_class(get_variables_data(dataset = "Precursors",
                                     as_arrow_table = T),
                  c("Dataset", "ArrowTabular", "arrow_dplyr_query"),
                  exact = FALSE)
})


test_that("get_variables_data() can select single columns", {

  local_full_store()

  out <- get_variables_data("Genes", dataset = "Precursors")

  expect_true("Genes" %in% names(out) || is.atomic(out))
})


test_that("get_variables_data_names() lists the stored columns", {

  local_full_store()

  expect_true(all(c("Genes", "Protein.Group") %in%
                    get_variables_data_names("Precursors")))
})


test_that("add_variables_data() joins stored columns onto a frame", {

  local_full_store()

  out <- add_variables_data(tibble::tibble(variables = c("P1", "P2")),
                            which = "Genes",
                            dataset = "Precursors")

  expect_true("Genes" %in% names(out))
  expect_equal(nrow(out), 2)
})


test_that("add_variables_data() returns the data unchanged without <which>", {

  local_full_store()

  d <- tibble::tibble(variables = c("P1", "P2"))
  expect_equal(add_variables_data(d, dataset = "Precursors"), d)
})


test_that(".save_variables_data() stores a frame and returns its location", {

  dir <- local_store()

  out <- suppressMessages(
    .save_variables_data(tibble::tibble(variables = paste0("P", 1:3),
                                        Genes = letters[1:3]),
                         dataset = "Precursors",
                         save_dir = dir))

  expect_true(file.exists(out$location))
  expect_s3_class(out$preview, "tbl_df")
})

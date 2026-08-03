test_that("get_data() round-trips every writable type", {

  dir <- withr::local_tempdir()
  d <- test_table()

  for (type in c("parquet", "rds", "tsv", "csv", "txt")) {
    path <- write_data(d, paste0("rt_", type), dir, type = type, silent = T)
    expect_equal(get_data(path), d, info = type)
  }
})


test_that("get_data() returns a materialised tibble by default", {

  path <- write_data(test_table(), type = "tsv", silent = T)

  expect_s3_class(get_data(path), "tbl_df")
})


test_that("get_data(as_arrow_table = T) returns a lazy Dataset", {

  dir <- withr::local_tempdir()

  for (type in c("parquet", "tsv", "csv", "txt")) {
    path <- write_data(test_table(), paste0("lazy_", type), dir, type = type,
                       silent = T)
    expect_s3_class(get_data(path, as_arrow_table = T), "Dataset", exact = FALSE)
  }
})


test_that("open_data() opens every delimited type lazily", {

  dir <- withr::local_tempdir()

  for (type in c("parquet", "tsv", "csv", "txt")) {
    path <- write_data(test_table(), paste0("op_", type), dir, type = type,
                       silent = T)
    con <- open_data(path)
    expect_s3_class(con, "Dataset", exact = FALSE)
    expect_equal(nrow(dplyr::collect(con)), 3, info = type)
  }
})


test_that("open_data() supports filtering before collecting", {

  path <- write_data(test_table(), type = "tsv", silent = T)

  out <- open_data(path) %>%
    dplyr::filter(Genes == "b") %>%
    dplyr::collect()

  expect_equal(nrow(out), 1)
  expect_equal(out$Protein.Group, "P2")
})


test_that("embedded delimiters survive the round trip", {

  dir <- withr::local_tempdir()
  d <- tibble::tibble(Genes = c("A,B", "C\tD", "E"), n = 1:3)

  for (type in c("tsv", "csv")) {
    path <- write_data(d, paste0("q_", type), dir, type = type, silent = T)
    expect_equal(get_data(path), d, info = type)
  }
})


test_that("get_data() passes non-paths straight through", {

  expect_equal(get_data(test_table()), test_table())
  expect_equal(get_data(c("a", "b", "c")), c("a", "b", "c"))
})


test_that("get_data() resolves .Rds files holding another path recursively", {

  dir <- withr::local_tempdir()

  inner <- write_data(test_table(), "inner", dir, silent = T)
  outer <- file.path(dir, "pointer.Rds")
  saveRDS(inner, outer)

  expect_equal(get_data(outer), test_table())
  expect_equal(get_data(outer, recursive = F), inner)
})


test_that("get_data() and open_data() use <fallback> when <file> is NULL", {

  path <- write_data(test_table(), type = "parquet", silent = T)

  expect_equal(get_data(NULL, fallback = path), test_table())
  expect_s3_class(open_data(NULL, fallback = path), "Dataset", exact = FALSE)
})


test_that("get_data() and open_data() error without file or fallback", {

  expect_error(get_data(NULL), "<file> or <fallback>", fixed = TRUE)
  expect_error(open_data(NULL), "<file> or <fallback>", fixed = TRUE)
})


test_that("get_data_m() maps and binds several files", {

  dir <- withr::local_tempdir()

  files <- c(write_data(test_table(), "m1", dir, silent = T),
             write_data(test_table(), "m2", dir, silent = T))

  expect_equal(nrow(get_data_m(files)), 6)
  expect_true("id" %in% names(get_data_m(stats::setNames(files, c("a", "b")),
                                         .id = "id")))
})

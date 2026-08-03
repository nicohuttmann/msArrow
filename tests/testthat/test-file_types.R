# Internal helpers behind write_data()'s <type> argument

test_that(".check_data_type() lowercases, de-duplicates and validates", {

  expect_equal(msArrow:::.check_data_type("TSV"), "tsv")
  expect_equal(msArrow:::.check_data_type(c("tsv", "tsv", "csv")),
               c("tsv", "csv"))
  expect_equal(msArrow:::.check_data_type(c("parquet", "rds", "tsv", "csv",
                                            "txt")),
               c("parquet", "rds", "tsv", "csv", "txt"))
})


test_that(".check_data_type() rejects unknown and empty input", {

  expect_error(msArrow:::.check_data_type("xlsx"), "Unknown file <type>",
               fixed = TRUE)
  expect_error(msArrow:::.check_data_type(character(0)),
               "at least one file <type>", fixed = TRUE)
})


test_that(".data_type_delim() maps types to delimiters", {

  expect_equal(msArrow:::.data_type_delim("tsv"), "\t")
  expect_equal(msArrow:::.data_type_delim("txt"), "\t")
  expect_equal(msArrow:::.data_type_delim("csv"), ",")
  expect_true(is.na(msArrow:::.data_type_delim("parquet")))
  expect_null(names(msArrow:::.data_type_delim("tsv")))
})


test_that(".trim_data_type() strips only known extensions", {

  expect_equal(msArrow:::.trim_data_type("a/b.parquet"), "a/b")
  expect_equal(msArrow:::.trim_data_type("a/b.tsv"), "a/b")
  expect_equal(msArrow:::.trim_data_type("a/b.Rds"), "a/b")
  expect_equal(msArrow:::.trim_data_type("a/b"), "a/b")
  # not a known data type, so left alone
  expect_equal(msArrow:::.trim_data_type("data_2024.01"), "data_2024.01")
})


test_that(".add_data_type() builds one path per type and keeps .Rds casing", {

  expect_equal(msArrow:::.add_data_type("x", c("parquet", "tsv")),
               c("x.parquet", "x.tsv"))
  expect_equal(msArrow:::.add_data_type("x", "rds"), "x.Rds")
  expect_equal(msArrow:::.add_data_type("x.tsv", "parquet"), "x.parquet")
})


test_that(".write_data_file() returns the path invisibly", {

  dir <- withr::local_tempdir()
  path <- file.path(dir, "one.tsv")

  expect_invisible(msArrow:::.write_data_file(test_table(), path, "tsv",
                                              silent = T))
  expect_true(file.exists(path))
})


test_that(".write_data_file() notes that partitioning is ignored for delimited", {

  dir <- withr::local_tempdir()

  expect_output(msArrow:::.write_data_file(test_table(),
                                           file.path(dir, "p.tsv"), "tsv",
                                           partitioning = "Genes"),
                "<partitioning> ignored", fixed = TRUE)
})

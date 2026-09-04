test_that("write_data() chooses the format automatically when <type> is absent", {

  dir <- withr::local_tempdir()
  d <- test_table()

  expect_match(write_data(d, "auto", dir, silent = T), "[.]parquet$")
  expect_match(write_data(list(a = d), "auto_list", dir, silent = T),
               "[.]parquetlist$")
  expect_match(write_data(function(x) x, "auto_fun", dir, silent = T), "[.]Rds$")
})


test_that("write_data() writes every given type and returns the first", {

  dir <- withr::local_tempdir()

  path <- write_data(test_table(), "both", dir, type = c("parquet", "tsv"),
                     silent = T)

  expect_equal(path, file.path(dir, "both.parquet"))
  expect_true(file.exists(file.path(dir, "both.parquet")))
  expect_true(file.exists(file.path(dir, "both.tsv")))
})


test_that("write_data() honours the order of <type>", {

  dir <- withr::local_tempdir()

  expect_equal(write_data(test_table(), "x", dir, type = c("tsv", "parquet"),
                          silent = T),
               file.path(dir, "x.tsv"))
})


test_that("a single type writes only that file", {

  dir <- withr::local_tempdir()

  path <- write_data(test_table(), "only", dir, type = "tsv", silent = T)

  expect_equal(path, file.path(dir, "only.tsv"))
  expect_false(file.exists(file.path(dir, "only.parquet")))
})


test_that("type = 'rds' forces an .Rds file for a table", {

  dir <- withr::local_tempdir()

  expect_equal(write_data(test_table(), "forced", dir, type = "rds", silent = T),
               file.path(dir, "forced.Rds"))
})


test_that("delimiters are correct per type", {

  dir <- withr::local_tempdir()
  write_data(test_table(), "d", dir, type = c("tsv", "csv", "txt"), silent = T)

  expect_match(readLines(file.path(dir, "d.tsv"))[1], "\t")
  expect_match(readLines(file.path(dir, "d.csv"))[1], ",")
  expect_match(readLines(file.path(dir, "d.txt"))[1], "\t")
  expect_false(grepl(",", readLines(file.path(dir, "d.txt"))[1]))
})


test_that("<type> is case-insensitive and de-duplicated", {

  dir <- withr::local_tempdir()

  expect_equal(write_data(test_table(), "case", dir, type = c("TSV", "tsv"),
                          silent = T),
               file.path(dir, "case.tsv"))
})


test_that("an existing known extension is not doubled", {

  dir <- withr::local_tempdir()

  expect_equal(write_data(test_table(), "ext.parquet", dir,
                          type = c("parquet", "tsv"), silent = T),
               file.path(dir, "ext.parquet"))
  expect_true(file.exists(file.path(dir, "ext.tsv")))
})


test_that("redo = F skips only when every requested type exists", {

  dir <- withr::local_tempdir()
  d <- test_table()

  write_data(d, "r", dir, type = c("parquet", "tsv"), silent = T)

  # both present -> skipped, first path returned
  expect_equal(write_data(d, "r", dir, type = c("parquet", "tsv"), redo = F,
                          silent = T),
               file.path(dir, "r.parquet"))

  # one missing -> recomputed
  file.remove(file.path(dir, "r.tsv"))
  write_data(d, "r", dir, type = c("parquet", "tsv"), redo = F, silent = T)
  expect_true(file.exists(file.path(dir, "r.tsv")))
})


test_that("redo = F still works without <type>", {

  dir <- withr::local_tempdir()
  write_data(test_table(), "n", dir, silent = T)

  expect_equal(write_data(test_table(), "n", dir, redo = F, silent = T),
               file.path(dir, "n.parquet"))
})


test_that("return_path = F returns the data instead of the path", {

  dir <- withr::local_tempdir()
  d <- test_table()

  out <- write_data(d, "keep", dir, type = "tsv", return_path = F, silent = T)

  expect_equal(out, d)
  expect_true(file.exists(file.path(dir, "keep.tsv")))
})


test_that("return_path = T (the default) returns the path", {

  dir <- withr::local_tempdir()

  expect_equal(write_data(test_table(), "p", dir, type = "tsv", silent = T),
               file.path(dir, "p.tsv"))
})


test_that("return_path = F still writes every requested type", {

  dir <- withr::local_tempdir()

  out <- write_data(test_table(), "multi", dir, type = c("parquet", "tsv"),
                    return_path = F, silent = T)

  expect_equal(out, test_table())
  expect_true(file.exists(file.path(dir, "multi.parquet")))
  expect_true(file.exists(file.path(dir, "multi.tsv")))
})


test_that("return_path = F passes an Arrow object straight back", {

  dir <- withr::local_tempdir()
  tbl <- arrow::arrow_table(test_table())

  out <- write_data(tbl, "arrow", dir, return_path = F, silent = T)

  expect_s3_class(out, "ArrowTabular", exact = FALSE)
  expect_true(file.exists(file.path(dir, "arrow.parquet")))
})


test_that("write_data() writes to a temporary file when file and dir are absent", {

  path <- write_data(test_table(), type = "tsv", silent = T)

  expect_match(path, "[.]tsv$")
  expect_true(file.exists(path))
})


test_that("unknown types are rejected with a helpful message", {

  dir <- withr::local_tempdir()

  expect_error(write_data(test_table(), "bad", dir, type = "xlsx"),
               "Unknown file <type>", fixed = TRUE)
  expect_error(write_data(test_table(), "bad", dir, type = "xlsx"), "parquet")
})


test_that("a list with several types is rejected rather than silently colliding", {

  dir <- withr::local_tempdir()

  expect_error(write_data(list(a = test_table()), "l", dir,
                          type = c("parquet", "tsv"), silent = T),
               "Only one <type>", fixed = TRUE)
})


test_that("a list with one type writes that type into the .parquetlist folder", {

  dir <- withr::local_tempdir()

  path <- write_data(list(a = test_table(), b = test_table()), "tl", dir,
                     type = "tsv", silent = T)

  expect_match(path, "[.]parquetlist$")
  expect_true(all(grepl("[.]tsv$", list.files(path))))
})


test_that("delimited output rejects columns it cannot represent", {

  dir <- withr::local_tempdir()
  bad <- tibble::tibble(a = 1:2, b = list(1:3, 1:4))

  expect_error(write_data(bad, "b", dir, type = "tsv", silent = T),
               "Delimited files store flat tables")
})


test_that("write_data() errors when no data is given", {

  dir <- withr::local_tempdir()
  expect_error(write_data(file = "x", dir = dir), "No data <x> given", fixed = TRUE)
})


test_that("write_data() requires a file name when a directory is given", {

  expect_error(write_data(test_table(), dir = tempdir()), "<file> name")
})


test_that("partitioning produces a parquet dataset directory", {

  dir <- withr::local_tempdir()

  path <- write_data(test_long(), "part", dir, partitioning = "observations",
                     silent = T)

  expect_true(dir.exists(path))
  expect_gt(nrow(get_data(path, as_arrow_table = T) %>% dplyr::collect()), 0)
})


test_that("silent = T produces no output", {

  dir <- withr::local_tempdir()

  expect_silent(write_data(test_table(), "q", dir, type = c("parquet", "tsv"),
                           silent = T))
})


test_that("write_data() reports each file it writes when not silent", {

  dir <- withr::local_tempdir()

  expect_output(write_data(test_table(), "loud", dir, type = c("parquet", "tsv")),
                "loud\\.parquet")
  expect_output(write_data(test_table(), "loud2", dir, type = "tsv"), "Done!")
})


test_that("a name that already carries a type is not given a second one", {

  dir <- withr::local_tempdir()
  d <- test_table()

  for (ty in c("parquet", "tsv", "csv", "txt", "Rds")) {
    nm <- paste0("keep.", ty)
    expect_equal(basename(write_data(d, nm, dir, silent = T)), nm, info = ty)
  }
})


test_that("the ending in the name decides the format written", {

  dir <- withr::local_tempdir()
  d <- test_table()

  # a .tsv named file really is tab-separated, not parquet under a tsv name
  write_data(d, "fmt.tsv", dir, silent = T)
  expect_match(readLines(file.path(dir, "fmt.tsv"), n = 1), "\t")

  write_data(d, "fmt.csv", dir, silent = T)
  expect_match(readLines(file.path(dir, "fmt.csv"), n = 1), ",")

  # and each round-trips
  for (nm in c("fmt.tsv", "fmt.csv")) {
    expect_equal(get_data(file.path(dir, nm)), d, info = nm)
  }
})


test_that("an explicit <type> still overrides the ending in the name", {

  dir <- withr::local_tempdir()

  expect_equal(basename(write_data(test_table(), "over.parquet", dir,
                                   type = "tsv", silent = T)),
               "over.tsv")
})


test_that("the ending is kept exactly as written", {

  dir <- withr::local_tempdir()

  expect_equal(basename(write_data(test_table(), "case.TSV", dir, silent = T)),
               "case.TSV")
  expect_equal(basename(write_data(test_table(), "case.PARQUET", dir,
                                   silent = T)),
               "case.PARQUET")
})


test_that("a .parquetlist name is not doubled", {

  dir <- withr::local_tempdir()

  path <- write_data(list(a = test_table()), "pl.parquetlist", dir, silent = T)

  expect_equal(basename(path), "pl.parquetlist")
  expect_named(get_data(path), "a")
})


test_that("a dot that is not a file type is left alone", {

  dir <- withr::local_tempdir()
  d <- test_table()

  expect_equal(basename(write_data(d, "data_2024.01", dir, silent = T)),
               "data_2024.01.parquet")
  expect_equal(basename(write_data(d, "v1.5", dir, silent = T)),
               "v1.5.parquet")
  expect_equal(basename(write_data(d, "report.final", dir, silent = T)),
               "report.final.parquet")
})

# Load every namespace the tests touch before the first test runs.
#
# testthat compares global state around each test. If a namespace is loaded
# part way through a test (which also registers its S3 methods) that comparison
# trips, so the loading is done up front instead.

for (pkg in c("arrow", "nanoparquet", "tidyr", "dplyr", "tibble", "withr")) {
  requireNamespace(pkg, quietly = TRUE)
}

# Touch the arrow entry points the tests use, so any lazy initialisation
# happens here rather than inside a test.
invisible(arrow::infer_type(tibble::tibble(a = 1L)))

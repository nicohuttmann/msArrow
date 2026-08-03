test_that("cc() names a vector by its own elements", {

  expect_equal(cc("a", "b"), c(a = "a", b = "b"))
  expect_equal(cc("x"), c(x = "x"))
  expect_named(cc("Genes", "Protein.Group"), c("Genes", "Protein.Group"))
})


test_that("fu() keeps the order of first appearance", {

  expect_equal(levels(fu(c("b", "a", "b", "c"))), c("b", "a", "c"))
  expect_s3_class(fu(c("b", "a")), "factor")
})


test_that("fus() sorts the levels", {

  expect_equal(levels(fus(c("b", "a", "c"))), c("a", "b", "c"))
  expect_equal(levels(fus(c("b", "a", "c"), decreasing = T)), c("c", "b", "a"))
})


test_that("cleanMem() runs the requested number of collections", {

  expect_no_error(cleanMem(1))
  expect_no_error(cleanMem(2))
})


# cleanup() empties the global environment by design, so these two tests
# snapshot it first and put everything back afterwards.
local_globalenv <- function(env = parent.frame()) {
  before <- mget(ls(globalenv(), all.names = TRUE), envir = globalenv())
  withr::defer({
    rm(list = ls(globalenv(), all.names = TRUE), envir = globalenv())
    list2env(before, envir = globalenv())
  }, envir = env)
}


test_that("cleanup() removes objects from the global environment", {

  local_globalenv()

  assign("tmp_object_for_test", 1, envir = globalenv())
  expect_true(exists("tmp_object_for_test", envir = globalenv()))

  expect_equal(cleanup(), "Good job.")

  expect_false(exists("tmp_object_for_test", envir = globalenv()))
})


test_that("cleanup() keeps excluded objects", {

  local_globalenv()

  assign("keep_me_for_test", 1, envir = globalenv())
  assign("drop_me_for_test", 1, envir = globalenv())

  cleanup(exclude = "keep_me_for_test")

  expect_true(exists("keep_me_for_test", envir = globalenv()))
  expect_false(exists("drop_me_for_test", envir = globalenv()))
})


test_that("cleanup() keeps the dataset registry objects", {

  local_globalenv()

  assign("Datasets", list(a = 1), envir = globalenv())
  assign("Info", list(b = 2), envir = globalenv())

  cleanup()

  expect_true(exists("Datasets", envir = globalenv()))
  expect_true(exists("Info", envir = globalenv()))
})

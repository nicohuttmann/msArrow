test_that("strsplit_() splits and unlists", {

  expect_equal(strsplit_("a_b_c", "_"), c("a", "b", "c"))
  expect_equal(strsplit_("a", "_"), "a")
})


test_that("strsplit_keep_first() and _last() take the outer pieces", {

  expect_equal(strsplit_keep_first("a_b_c", "_"), "a")
  expect_equal(strsplit_keep_last("a_b_c", "_"), "c")
  expect_equal(strsplit_keep_first("nosplit", "_"), "nosplit")
})


test_that("strsplit_keep_firstn() and _lastn() keep n pieces and rejoin", {

  expect_equal(strsplit_keep_firstn("a_b_c_d", "_", 2), "a_b")
  expect_equal(strsplit_keep_lastn("a_b_c_d", "_", 2), "c_d")
  expect_equal(strsplit_keep_firstn("a_b_c_d", "_", 1), "a")
})


test_that("the strsplit helpers are vectorised", {

  x <- c("a_b", "c_d", "e_f")

  expect_equal(strsplit_keep_first(x, "_"), c("a", "c", "e"))
  expect_equal(strsplit_keep_last(x, "_"), c("b", "d", "f"))
})


test_that("strsplit_keep() keeps the requested element", {

  expect_equal(strsplit_keep("a_b_c", "_", 2), "b")
})


test_that("str_rev() reverses each string", {

  expect_equal(str_rev("abc"), "cba")
  expect_equal(str_rev(c("abc", "de")), c("cba", "ed"))
  expect_equal(str_rev(""), "")
})


test_that("str_locate_last() returns the last match position", {

  # a_b_c -> a=1 _=2 b=3 _=4 c=5, so the last underscore is at 4.
  # The result carries regexpr() attributes, hence as.integer().
  expect_equal(as.integer(str_locate_last("a_b_c", "_")), 4L)
  expect_equal(as.integer(str_locate_last("P12345_SEQ_1_20", "_")), 13L)
})


test_that("str_locate_last() treats a dot literally", {

  expect_equal(as.integer(str_locate_last("a.b.c", ".")), 4L)
})


test_that("str_locate_last() returns -1 when there is no match", {

  expect_equal(as.integer(str_locate_last("abc", "_")), -1L)
})


test_that("str_locate_last() is vectorised", {

  expect_length(str_locate_last(c("a_b", "c_d_e"), "_"), 2)
})

test_that("cleanMem() runs the requested number of collections", {

  expect_no_error(cleanMem(1))
  expect_no_error(cleanMem(2))
})

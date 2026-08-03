test_that(".cat_character() prints a pasteable character vector", {

  out <- capture.output(.cat_character(c("a", "b")))

  expect_match(paste(out, collapse = ""), 'c\\("a"')
  expect_match(paste(out, collapse = ""), '"b"\\)')
})


test_that(".cat_numeric() prints a pasteable numeric vector", {

  out <- paste(capture.output(.cat_numeric(c(1, 2, 3))), collapse = "")

  expect_match(out, "^c\\(1")
  expect_match(out, "3\\)$")
})


test_that(".cat_character_named() prints names alongside values", {

  out <- paste(capture.output(.cat_character_named(c(a = "x", b = "y"))),
               collapse = "")

  expect_match(out, "a")
  expect_match(out, "x")
})


test_that(".cat_gsub_n() substitutes once per replacement", {

  out <- capture.output(.cat_gsub_n("value = X", "X", c("1", "2")))
  joined <- paste(out, collapse = " ")

  expect_match(joined, "value = 1")
  expect_match(joined, "value = 2")
})


test_that(".cat_function() prints a call snippet for a function", {

  out <- paste(capture.output(.cat_function(cc)), collapse = "")

  expect_true(nchar(out) > 0)
})


test_that(".cat_function() returns FALSE for a non-function", {

  expect_false(.cat_function("not a function"))
  expect_false(.cat_function())
})


test_that("the interactive .cat_get_* writers exist and take the documented args", {

  # These call utils::select.list() and only do something useful in an
  # interactive session, so only their interface is checked here.
  for (f in c(".cat_get_variables_data", ".cat_get_observations_data",
              ".cat_get_data_frame", ".cat_get_data_frame_m")) {
    fun <- get(f, envir = asNamespace("msArrow"))
    expect_true(is.function(fun), info = f)
    expect_true("copy2clipboard" %in% names(formals(fun)), info = f)
  }
})

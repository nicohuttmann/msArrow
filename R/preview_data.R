#' Collects the first rows of one opened object
#'
#' @param x an opened connection or a data frame
#' @param n number of rows to read
#'
#' @returns a tibble of the first <n> rows carrying a `total_rows` attribute
#' @keywords internal
.preview_one <- function(x, n) {

  # nrow() reads the metadata only, so this stays free even for a huge dataset
  total_rows <- tryCatch(nrow(x), error = function(e) NA_integer_)

  if (is.null(total_rows) || length(total_rows) != 1) total_rows <- NA_integer_

  preview <- x %>%
    dplyr::slice_head(n = n) %>%
    dplyr::collect()

  attr(preview, "total_rows") <- total_rows

  return(preview)

}


#' Splits <n> into a row count and a number of datasets
#'
#' @param n one number, or two as c(rows, datasets)
#'
#' @returns list with the elements `rows` and `sets`
#' @keywords internal
.preview_n <- function(n) {
  list(rows = n[1], 
       sets = if (length(n) > 1) n[2] else 1)
}


#' Reports what a preview is showing
#'
#' Kept separate from building the preview so that `view_data()` can open the
#' viewer first and report afterwards.
#'
#' @param previews a preview tibble, or a named list of them
#' @param n_rows number of rows that was asked for
#' @param total_sets number of datasets the file holds, used for the hint
#'
#' @returns nothing; the counts are printed
#' @keywords internal
.preview_message <- function(previews, 
                             n_rows, 
                             total_sets = NULL) {

  fmt <- function(x) if (is.na(x)) "?" else format(x, big.mark = ",")

  line <- function(preview, label = NULL)
    cat(paste0(if (is.null(label)) "" else paste0(label, ": "), 
               "Showing ", 
               fmt(nrow(preview)), 
               " of ", 
               fmt(attr(preview, "total_rows")), 
               " rows\n"))

  if (inherits(previews, "data.frame")) {

    line(previews)

  } else {

    purrr::iwalk(previews, \(x, i) line(x, label = i))

    # a .parquetlist holds several datasets and only the first are shown
    n_hidden <- if (is.null(total_sets)) 0 else total_sets - length(previews)

    if (n_hidden > 0)
      cat(paste0("  ", 
                 n_hidden, 
                 " more dataset", 
                 if (n_hidden == 1) "" else "s", 
                 " not shown - use n = c(", 
                 n_rows, 
                 ", ", 
                 total_sets, 
                 ") to show all\n"))

  }

  return(invisible(NULL))

}


#' Reads the first rows of a file without loading the rest
#'
#' `preview_data()` opens <file> lazily with [open_data()] and collects only the
#' first <n> rows, so a multi-million-row file can be inspected without reading
#' it. It takes everything `open_data()` takes: .parquet, .tsv, .csv, .txt, 
#' .Rds, a partitioned dataset directory, or data that is already in the
#' session.
#'
#' The total number of rows comes from the file metadata, which costs nothing, 
#' and is attached to the result as the attribute `total_rows`; unless <silent>
#' is TRUE a line reporting `Showing <n> of <total_rows> rows` is printed. The
#' result is otherwise a plain tibble.
#'
#' A `.parquetlist` folder holds several datasets rather than a single table.
#' Only the first is shown by default; give <n> a second number to ask for
#' more, so `n = c(10, 5)` takes ten rows of each of the first five datasets.
#' The message then says how many were left out and what to pass to see them.
#'
#' @inheritParams open_data
#' @param n number of rows to read, or `c(rows, datasets)` for a
#' `.parquetlist`, where the second number defaults to 1
#' @param silent Should the message reporting the row counts be suppressed?
#'
#' @returns a tibble of the first <n> rows carrying a `total_rows` attribute, 
#' or a named list of such tibbles for a `.parquetlist`
#' @export
#'
#' @examples
#'   data_small <- tibble::tibble(a = 1:100, b = rnorm(100)) %>%
#'     write_data(file = "data_small", 
#'                dir = tempdir(), 
#'                type = "parquet")
#'
#'   preview_data(data_small, n = 5)
preview_data <- function(file, 
                         n = 10, 
                         fallback, 
                         recursive = T, 
                         credit = 10, 
                         silent = F, 
                         ...) {

  n <- .preview_n(n)

  # <fallback> is forwarded only when given, so that open_data() can still
  # report a missing input itself instead of failing on an empty promise
  if (hasArg(fallback))
    data_object <- open_data(file = file, 
                             fallback = fallback, 
                             recursive = recursive, 
                             credit = credit, 
                             ...)

  else
    data_object <- open_data(file = file, 
                             recursive = recursive, 
                             credit = credit, 
                             ...)

  # a .parquetlist opens as a named list of connections
  if (class(data_object)[1] == "list") {

    total_sets <- length(data_object)

    previews <- utils::head(data_object, n$sets) %>%
      purrr::map(\(x) .preview_one(x, n = n$rows))

    attr(previews, "total_sets") <- total_sets

    if (!silent) .preview_message(previews, 
                                  n_rows = n$rows, 
                                  total_sets = total_sets)

    return(previews)

  }

  preview <- .preview_one(data_object, n = n$rows)

  if (!silent) .preview_message(preview, n_rows = n$rows)

  return(preview)

}


#' Opens the first rows of a file in the data viewer
#'
#' `view_data()` is [preview_data()] followed by `View()`: the first <n> rows
#' are collected without reading the rest of the file and handed to the viewer.
#' The viewer is only called in an interactive session, and the preview is
#' returned invisibly either way, so the same call is safe inside a script.
#'
#' `View()` is looked up on the search path rather than called as
#' `utils::View()`, so an RStudio session gets RStudio's data viewer instead of
#' base R's separate window.
#'
#' The viewer is opened before the row counts are reported, so the message is
#' the last thing in the console once the data is up. For a `.parquetlist`, 
#' <n> takes a second number giving how many datasets to open, one by default.
#'
#' @inheritParams preview_data
#' @param title title of the viewer tab (default: the file name)
#'
#' @returns the previewed tibble (invisibly), or a named list of them for a
#' `.parquetlist`
#' @export
#'
#' @examples
#'   data_small <- tibble::tibble(a = 1:100, b = rnorm(100)) %>%
#'     write_data(file = "data_small", 
#'                dir = tempdir(), 
#'                type = "parquet")
#'
#'   view_data(data_small, n = 5)
view_data <- function(file, 
                      n = 10, 
                      fallback, 
                      recursive = T, 
                      credit = 10, 
                      title, 
                      silent = F, 
                      ...) {

  # the preview is built quietly so that the viewer opens before the message
  if (hasArg(fallback))
    preview <- preview_data(file = file, 
                            n = n, 
                            fallback = fallback, 
                            recursive = recursive, 
                            credit = credit, 
                            silent = T, 
                            ...)

  else
    preview <- preview_data(file = file, 
                            n = n, 
                            recursive = recursive, 
                            credit = credit, 
                            silent = T, 
                            ...)

  # <file> can be data rather than a path, which has no file name to show
  if (!hasArg(title)) {

    if (is.character(file) && length(file) == 1)
      title <- basename(file)

    else if (hasArg(fallback) && is.character(fallback) && length(fallback) == 1)
      title <- basename(fallback)

    else
      title <- "preview"

  }

  if (interactive()) {

    # RStudio masks View() on the search path with its own data viewer. A
    # package namespace does not see that mask, and utils::View() is base R's
    # separate window, which formats every column to text. Looking the function
    # up from the global environment finds RStudio's viewer when there is one
    # and falls back to utils::View() everywhere else.
    view_fun <- get("View", envir = globalenv())

    if (inherits(preview, "data.frame"))
      view_fun(preview, title)

    else
      purrr::iwalk(preview, 
                   \(x, i) view_fun(x, paste(title, i, sep = ": ")))

  }

  if (!silent) .preview_message(preview, 
                                n_rows = .preview_n(n)$rows, 
                                total_sets = attr(preview, "total_sets"))

  return(invisible(preview))

}

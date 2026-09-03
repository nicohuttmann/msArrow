#' Collects the first rows of one opened object
#'
#' @param x an opened connection or a data frame
#' @param n number of rows to read
#' @param silent Should the message reporting the row counts be suppressed?
#' @param label name printed in front of the message, used for list elements
#'
#' @returns a tibble of the first <n> rows carrying a `total_rows` attribute
#' @keywords internal
.preview_one <- function(x, 
                         n, 
                         silent = F, 
                         label = NULL) {
  
  # nrow() reads the metadata only, so this stays free even for a huge dataset 
  total_rows <- tryCatch(nrow(x), error = function(e) NA_integer_)
  
  if (is.null(total_rows) || length(total_rows) != 1) total_rows <- NA_integer_
  
  preview <- x %>% 
    dplyr::slice_head(n = n) %>% 
    dplyr::collect()
  
  attr(preview, "total_rows") <- total_rows
  
  if (!silent) 
    cat(paste0(if (is.null(label)) "" else paste0(label, ": "), 
               "Showing ", 
               format(nrow(preview), big.mark = ","), 
               " of ", 
               if (is.na(total_rows)) "?" else format(total_rows, big.mark = ","), 
               " rows\n"))
  
  return(preview)
  
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
#' A `.parquetlist` folder holds a list rather than a single table, so a named
#' list of previews is returned, one per element.
#'
#' @inheritParams open_data
#' @param n number of rows to read
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
  if (class(data_object)[1] == "list") 
    return(purrr::imap(data_object, 
                       \(x, i) .preview_one(x = x, 
                                            n = n, 
                                            silent = silent, 
                                            label = i)))
  
  return(.preview_one(x = data_object, 
                      n = n, 
                      silent = silent))
  
}


#' Opens the first rows of a file in the data viewer
#'
#' `view_data()` is [preview_data()] followed by `utils::View()`: the first <n>
#' rows are collected without reading the rest of the file and handed to the
#' viewer. The viewer is only called in an interactive session, and the preview
#' is returned invisibly either way, so the same call is safe inside a script.
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
  
  if (hasArg(fallback)) 
    preview <- preview_data(file = file, 
                            n = n, 
                            fallback = fallback, 
                            recursive = recursive, 
                            credit = credit, 
                            silent = silent, 
                            ...)
  
  else 
    preview <- preview_data(file = file, 
                            n = n, 
                            recursive = recursive, 
                            credit = credit, 
                            silent = silent, 
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
    
    if (class(preview)[1] == "list") 
      purrr::iwalk(preview, 
                   \(x, i) utils::View(x, title = paste(title, i, sep = ": ")))
    
    else 
      utils::View(preview, title = title)
    
  }
  
  return(invisible(preview))
  
}

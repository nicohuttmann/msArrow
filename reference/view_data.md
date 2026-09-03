# Opens the first rows of a file in the data viewer

`view_data()` is
[`preview_data()`](https://nicohuttmann.github.io/msArrow/reference/preview_data.md)
followed by [`View()`](https://rdrr.io/r/utils/View.html): the first
rows are collected without reading the rest of the file and handed to
the viewer. The viewer is only called in an interactive session, and the
preview is returned invisibly either way, so the same call is safe
inside a script.

## Usage

``` r
view_data(
  file,
  n = 10,
  fallback,
  recursive = T,
  credit = 10,
  title,
  silent = F,
  ...
)
```

## Arguments

- file:

  file name

- n:

  number of rows to read, or `c(rows, datasets)` for a `.parquetlist`,
  where the second number defaults to 1

- fallback:

  other file name or alternative way to provide the input - useful if
  file is an R object and fallback is a hardcoded string

- recursive:

  Should data be recursively loaded?

- credit:

  how many recursive steps are allowed

- title:

  title of the viewer tab (default: the file name)

- silent:

  Should the message reporting the row counts be suppressed?

- ...:

  additional arguments for the opening function

## Value

the previewed tibble (invisibly), or a named list of them for a
`.parquetlist`

## Details

[`View()`](https://rdrr.io/r/utils/View.html) is looked up on the search
path rather than called as
[`utils::View()`](https://rdrr.io/r/utils/View.html), so an RStudio
session gets RStudio's data viewer instead of base R's separate window.

The viewer is opened before the row counts are reported, so the message
is the last thing in the console once the data is up. For a
`.parquetlist`, takes a second number giving how many datasets to open,
one by default.

## Examples

``` r
  data_small <- tibble::tibble(a = 1:100, b = rnorm(100)) %>%
    write_data(file = "data_small", 
               dir = tempdir(), 
               type = "parquet")
#> Saving file "/tmp/Rtmp5Bs4pJ/data_small.parquet". Done!

  view_data(data_small, n = 5)
#> Showing 5 of 100 rows
```

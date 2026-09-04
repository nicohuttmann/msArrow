# Reads the first rows of a file without loading the rest

`preview_data()` opens lazily with
[`open_data()`](https://nicohuttmann.github.io/msArrow/reference/open_data.md)
and collects only the first rows, so a multi-million-row file can be
inspected without reading it. It takes everything
[`open_data()`](https://nicohuttmann.github.io/msArrow/reference/open_data.md)
takes: .parquet, .tsv, .csv, .txt, .Rds, a partitioned dataset
directory, or data that is already in the session.

## Usage

``` r
preview_data(
  file,
  n = 10,
  fallback,
  recursive = T,
  credit = 10,
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

- silent:

  Should the message reporting the row counts be suppressed?

- ...:

  additional arguments for the opening function

## Value

a tibble of the first rows carrying a `total_rows` attribute, or a named
list of such tibbles for a `.parquetlist`

## Details

The total number of rows comes from the file metadata, which costs
nothing, and is attached to the result as the attribute `total_rows`;
unless is TRUE a line reporting `Showing <n> of <total_rows> rows` is
printed. The result is otherwise a plain tibble.

A `.parquetlist` folder holds several datasets rather than a single
table. Only the first is shown by default; give a second number to ask
for more, so `n = c(10, 5)` takes ten rows of each of the first five
datasets. The message then says how many were left out and what to pass
to see them.

## Examples

``` r
  data_small <- tibble::tibble(a = 1:100, b = rnorm(100)) %>%
    write_data(file = "data_small", 
               dir = tempdir(), 
               type = "parquet")
#> Saving file "/tmp/Rtmp5EiRY9/data_small.parquet". Done!

  preview_data(data_small, n = 5)
#> Showing 5 of 100 rows
#> # A tibble: 5 × 2
#>       a        b
#>   <int>    <dbl>
#> 1     1 -1.40   
#> 2     2  0.255  
#> 3     3 -2.44   
#> 4     4 -0.00557
#> 5     5  0.622  
```

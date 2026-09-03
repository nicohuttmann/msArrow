# Collects the first rows of one opened object

Collects the first rows of one opened object

## Usage

``` r
.preview_one(x, n, silent = F, label = NULL)
```

## Arguments

- x:

  an opened connection or a data frame

- n:

  number of rows to read

- silent:

  Should the message reporting the row counts be suppressed?

- label:

  name printed in front of the message, used for list elements

## Value

a tibble of the first rows carrying a `total_rows` attribute

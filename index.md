# msArrow

<!-- badges: start -->
[![R-CMD-check](https://github.com/nicohuttmann/msArrow/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/nicohuttmann/msArrow/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/nicohuttmann/msArrow/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/nicohuttmann/msArrow/actions/workflows/pkgdown.yaml)
<!-- badges: end -->

A minimal library to handle large (proteomics) data with R.

In essence, this package contains convenient wrappers around some `arrow` and `nanoparquet` functions and an opinionated way to organize datasets. Latter aspect is not required to be used.

# Installation
You can install this package from GitHub via:

```r
devtools::install_github("nicohuttmann/msArrow")
```

or the new, fast way: 

```r
pak::pkg_install("nicohuttmann/msArrow")
```

# Usage 

Instead of storing your data in the R Environment which can be RAM-intensive, simply write your data to a temporary file, and load them each use. 


```r
library(msArrow)

data <- tibble::tibble(
    a = rnorm(1e8), 
    b = rnorm(1e8), 
    c = rnorm(1e8)) %>% 
  write_data()

get_data(data)
```

The variable holds a **path**, not the data. `write_data()` returns the file it
wrote and every downstream step re-reads it, so nothing large sits in the
session.

## Choosing the file type

By default `write_data()` picks the format itself: parquet for anything Arrow
can represent, a `.parquetlist` folder for a list, `.Rds` otherwise. Give
`type` to override that. It takes one or several of `parquet`, `rds`, `tsv`,
`csv` and `txt`:

```r
data_main <- get_data(data_raw) %>% 
  summarise(mean_int = mean(Intensity), .by = Protein.Group) %>% 
  write_data(file = "data_main", dir = "Data", type = c("parquet", "tsv"))
```

All requested files are written, but **only the path of the first type is
returned** — so the pipeline stays on the parquet file while a readable `.tsv`
lands next to it for collaborators. For a small table, `type = "tsv"` alone is
usually what you want.

If a series of data exports, say 100s of datasets, the `clean_memory = T` 
argument applies a cleanup of the RAM via the internal `cleanMem()` function. 
It's worth using it if you feel your R session is becoming slower. 

`get_data()` and `open_data()` read every one of these formats back, lazily
too:

```r
open_data("Data/data_main.tsv") %>% 
  filter(mean_int > 10) %>% 
  collect()
```

## Reading

| | returns | use for |
|---|---|---|
| `get_data(x)` | a materialised tibble | you want the data now |
| `open_data(x)` | a lazy Arrow connection | filter/aggregate before collecting |

`get_data()` already collects, so it never needs a trailing `collect()`.

## Looking at a file

`preview_data()` reads only the first rows, so a file with millions of them can
be inspected without loading it. The total comes from the file metadata, which
costs nothing, so it can tell you what fraction you are looking at:

```r
preview_data("Data/data_curves.parquet", n = 5)
#> Showing 5 of 2,981,204 rows
```

`view_data()` does the same and hands the result to the data viewer — RStudio's
own, so you keep column types, sorting and filtering — and reports afterwards,
so the counts stay in the console once the pane is up:

```r
view_data("Data/data_curves.parquet")
```

Both return the preview, `view_data()` invisibly, and both attach the total as
a `total_rows` attribute. Neither reads past the rows it shows: previewing a
2.5 M-row file takes about 0.1 s.

A `.parquetlist` holds several datasets rather than one table. Only the first
is shown by default; `n` takes a second number, `c(rows, datasets)`, and the
message says what to pass to see the rest:

```r
view_data("Data/by_group.parquetlist")
#> controls: Showing 10 of 40 rows
#>   3 more datasets not shown - use n = c(10, 4) to show all

view_data("Data/by_group.parquetlist", n = c(5, 4))
```

See `vignette("msArrow")` for the whole workflow.

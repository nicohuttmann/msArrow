# msArrow

A minimal library to handle large (proteomics) data with R.

In essence, this package contains convenient wrappers around some
`arrow` and `nanoparquet` functions and an opinionated way to organize
datasets. Latter aspect is not required to be used.

# Installation

You can install this package from GitHub via:

``` r

devtools::install_github("nicohuttmann/msArrow")
```

or the new, fast way:

``` r

pak::pkg_install("nicohuttmann/msArrow")
```

# Usage

Instead of storing your data in the R Environment which can be
RAM-intensive, simply write your data to a temporary file, and load them
each use.

``` r

library(msArrow)

data <- tibble::tibble(
    a = rnorm(1e8), 
    b = rnorm(1e8), 
    c = rnorm(1e8)) %>% 
  write_data()

get_data(data)
```

The variable holds a **path**, not the data.
[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
returns the file it wrote and every downstream step re-reads it, so
nothing large sits in the session.

## Choosing the file type

By default
[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
picks the format itself: parquet for anything Arrow can represent, a
`.parquetlist` folder for a list, `.Rds` otherwise. Give `type` to
override that. It takes one or several of `parquet`, `rds`, `tsv`, `csv`
and `txt`:

``` r

data_main <- get_data(data_raw) %>% 
  summarise(mean_int = mean(Intensity), .by = Protein.Group) %>% 
  write_data(file = "data_main", dir = "Data", type = c("parquet", "tsv"))
```

All requested files are written, but **only the path of the first type
is returned** — so the pipeline stays on the parquet file while a
readable `.tsv` lands next to it for collaborators. For a small table,
`type = "tsv"` alone is usually what you want.

[`get_data()`](https://nicohuttmann.github.io/msArrow/reference/get_data.md)
and
[`open_data()`](https://nicohuttmann.github.io/msArrow/reference/open_data.md)
read every one of these formats back, lazily too:

``` r

open_data("Data/data_main.tsv") %>% 
  filter(mean_int > 10) %>% 
  collect()
```

## Reading

|                | returns                 | use for                            |
|----------------|-------------------------|------------------------------------|
| `get_data(x)`  | a materialised tibble   | you want the data now              |
| `open_data(x)` | a lazy Arrow connection | filter/aggregate before collecting |

[`get_data()`](https://nicohuttmann.github.io/msArrow/reference/get_data.md)
already collects, so it never needs a trailing `collect()`.

## Looking at a file

[`preview_data()`](https://nicohuttmann.github.io/msArrow/reference/preview_data.md)
reads only the first rows, so a file with millions of them can be
inspected without loading it. The total comes from the file metadata,
which costs nothing, so it can tell you what fraction you are looking
at:

``` r

preview_data("Data/data_curves.parquet", n = 5)
#> Showing 5 of 2,981,204 rows
```

[`view_data()`](https://nicohuttmann.github.io/msArrow/reference/view_data.md)
does the same and hands the result to the data viewer — RStudio’s own,
so you keep column types, sorting and filtering — and reports
afterwards, so the counts stay in the console once the pane is up:

``` r

view_data("Data/data_curves.parquet")
```

Both return the preview,
[`view_data()`](https://nicohuttmann.github.io/msArrow/reference/view_data.md)
invisibly, and both attach the total as a `total_rows` attribute.
Neither reads past the rows it shows: previewing a 2.5 M-row file takes
about 0.1 s.

A `.parquetlist` holds several datasets rather than one table. Only the
first is shown by default; `n` takes a second number,
`c(rows, datasets)`, and the message says what to pass to see the rest:

``` r

view_data("Data/by_group.parquetlist")
#> controls: Showing 10 of 40 rows
#>   3 more datasets not shown - use n = c(10, 4) to show all

view_data("Data/by_group.parquetlist", n = c(5, 4))
```

## Organising datasets

The dataset store — variables (precursors, peptides, proteins),
observations (runs, samples) and long `observations x variables` data
frames kept together on disk — lives in
[msTools](https://nicohuttmann.github.io/msTools/), which is built on
top of this package:

``` r

library(msTools)

import_diann("report.parquet", name = "Precursors", save_dir = "Data/RData")

get_data_frame("Precursor.Normalised", dataset = "Precursors")
get_variables_data("Genes", dataset = "Precursors")
```

msArrow itself stays deliberately small: it is the on-disk I/O layer and
nothing else.

See
[`vignette("msArrow")`](https://nicohuttmann.github.io/msArrow/articles/msArrow.md)
for the whole workflow.

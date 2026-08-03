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

## Organising datasets

The optional dataset store keeps variables (precursors, peptides, proteins),
observations (runs, samples) and long `observations x variables` data frames
together on disk:

```r
import_diann("report.parquet", name = "Precursors", save_dir = "Data/RData")

get_data_frame("Precursor.Normalised", dataset = "Precursors")
get_variables_data("Genes", dataset = "Precursors")
```

See `vignette("msArrow")` for the whole workflow.

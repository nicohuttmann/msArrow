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

See `vignette("msArrow")` or visit the package's [website](nicohuttmann.github.io/msArrow/) for the whole workflow.

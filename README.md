# msArrow

A minimal library to handle large (proteomics) data with R.

In essence, this package contains convenient wrappers around some `arrow` and `nanoparquet` functions and an opinionated way to organize datasets. Latter aspect is not required to be used.

# Installation
You can install this package from GitHub via:

```
devtools::install_github("nicohuttmann/msArrow")
```

or the new, fast way: 

```
pak::pkg_install("nicohuttmann/msArrow")
```

# Usage 

Instead of storing your data in the R Environment which can be RAM-intensive, simply write your data to a temporary file, and load them each use. 


```
library(msArrow)

data <- tibble::tibble(
    a = rnorm(1e8), 
    b = rnorm(1e8), 
    c = rnorm(1e8)) %>% 
  write_data()

get_data(data)
```
# msArrow

A minimal library to handle large (proteomics) data with R.

(Nothing but a convenient wrapper around some `arrow` and `nanoparquet` functions and an opinionated way to organize datasets. Latter aspect is not requi)

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

data <- tibble::tibble(a = rnorm(1e8), 
b = rnorm(1e8), 
c = rnorm(1e8)) %>% 
write_data()

get_data(data)
```
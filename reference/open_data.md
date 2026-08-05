# Loads data from a file name

Loads data from a file name

## Usage

``` r
open_data(file, fallback, recursive = T, credit = 10, ...)
```

## Arguments

- file:

  file name

- fallback:

  other file name or alternative way to provide the input - useful if
  file is an R object and fallback is a hardcoded string

- recursive:

  Should data be recursively loaded?

- credit:

  how many recursive steps are allowed

- ...:

  additional arguments for the opening function

## Value

an Arrow dataset connection, or the object itself if is not a file path

## Examples

``` r
  data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
    write_data(file = "data_small", 
               dir = tempdir(), 
               type = "tsv")
#> Overwriting file "/tmp/RtmpGA8NXj/data_small.tsv". Done!

  open_data(data_small) %>%
    dplyr::filter(a > 1) %>%
    dplyr::collect()
#> # A tibble: 2 × 2
#>       a b    
#>   <int> <chr>
#> 1     2 b    
#> 2     3 c    
```

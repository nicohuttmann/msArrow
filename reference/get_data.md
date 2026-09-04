# Loads data from a file name or returns if already in R

Loads data from a file name or returns if already in R

## Usage

``` r
get_data(file, fallback, recursive = T, credit = 10, as_arrow_table = F, ...)
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

- as_arrow_table:

  return a tibble or an Arrow connection

- ...:

  additional arguments

## Value

a tibble, or an Arrow dataset connection if as_arrow_table = T; the
object itself if is not a file path

## Examples

``` r
  data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
    write_data(file = "data_small", 
               dir = tempdir(), 
               type = "tsv")
#> Saving file "/tmp/Rtmp5EiRY9/data_small.tsv". Done!

  get_data(data_small)
#> # A tibble: 3 × 2
#>       a b    
#>   <int> <chr>
#> 1     1 a    
#> 2     2 b    
#> 3     3 c    
```

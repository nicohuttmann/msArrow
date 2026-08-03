# Combines a get_data(), map() and bind_rows()

Combines a get_data(), map() and bind_rows()

## Usage

``` r
get_data_m(file, recursive = T, credit = 0, .id = NULL)
```

## Arguments

- file:

  file name

- recursive:

  Should data be recursively loaded?

- credit:

  how many recursive steps are allowed

- .id:

  name of an optional identifier column (see dplyr::bind_rows())

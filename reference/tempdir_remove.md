# Removing all temporary files saved by write_data() when

Removing all temporary files saved by write_data() when

## Usage

``` r
tempdir_remove(
  dir = tempdir(),
  all.paths = F,
  pattern = ".Rds|.parquet|.tsv|.csv|.txt|.pdf"
)
```

## Arguments

- dir:

  location of temporary files

- all.paths:

  find all temporary folders

- pattern:

  pattern/s for files to be removed

## Examples

``` r
  tempdir_remove()
#> [1] TRUE TRUE
```

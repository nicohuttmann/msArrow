# List size of all temporary files saved by write_data() when

List size of all temporary files saved by write_data() when

## Usage

``` r
tempdir_size(
  dir = tempdir(),
  all.paths = F,
  pattern = ".Rds|.parquet|.tsv|.csv|.txt",
  units = "auto_si"
)
```

## Arguments

- dir:

  location of temporary files

- all.paths:

  find all temporary folders

- pattern:

  pattern/s for files to be removed

- units:

  unit/s to use to represent file size

## Examples

``` r
  tempdir_list()
#> character(0)
```

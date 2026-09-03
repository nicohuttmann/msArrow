# List all temporary files saved by write_data() when

List all temporary files saved by write_data() when

## Usage

``` r
tempdir_list(
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
  tempdir_list()
#> [1] "/tmp/RtmpaYkuwS/data_small.parquet" "/tmp/RtmpaYkuwS/data_small.tsv"    
```
